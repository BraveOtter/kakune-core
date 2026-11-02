//! Two-phase, data-only plugin installation.
//!
//! Preparing a package may download or copy bytes, but it never runs package
//! code, package-manager lifecycle hooks, or dependency installers. Commit is
//! deliberately separate and accepts only the digest returned by prepare.

use std::{
    collections::BTreeMap,
    fs,
    io::Read,
    path::{Component, Path, PathBuf},
    time::Duration,
};

use flate2::read::GzDecoder;
use serde_json::{Value, json};
use sha2::{Digest, Sha256, Sha512};
use tar::Archive;
use tokio::{
    io::AsyncReadExt,
    process::Command,
    sync::{Semaphore, SemaphorePermit},
    time::timeout,
};
use uuid::Uuid;

use crate::{
    InstalledPluginRecord, PreparedPluginInstallRecord, Store, plugin_process::PluginManifest,
};

pub const PLUGIN_MANIFEST_NAMES: &[&str] = &["kakune.plugin.json", "kakune-plugin.json"];
pub const MAX_PLUGIN_PACKAGE_BYTES: u64 = 256 * 1024 * 1024;
pub const MAX_PLUGIN_FILES: usize = 10_000;
pub const MAX_PLUGIN_PROCESS_OUTPUT: usize = 1024 * 1024;
pub const PLUGIN_ADMIN_TIMEOUT: Duration = Duration::from_secs(30);
static PREPARATION_SLOTS: Semaphore = Semaphore::const_new(4);

#[derive(Clone, Debug)]
pub enum PluginSource {
    Local(PathBuf),
    Npm {
        package: String,
        version: Option<String>,
    },
    Git {
        url: String,
        reference: Option<String>,
    },
}

impl PluginSource {
    pub fn parse(value: &str) -> Result<Self, String> {
        if value.is_empty() || value.len() > 4096 || value.contains(['\r', '\n']) {
            return Err("plugin source must be non-empty and at most 4,096 characters".to_owned());
        }
        if let Some(spec) = value.strip_prefix("npm:") {
            let (package, version) = npm_spec(spec)?;
            return Ok(Self::Npm { package, version });
        }
        if let Some(spec) = value.strip_prefix("git:") {
            let (url, reference) = spec
                .rsplit_once('#')
                .map_or((spec, None), |(url, reference)| (url, Some(reference)));
            if url.is_empty() || reference.is_some_and(str::is_empty) {
                return Err("git source must be git:<https-or-ssh-url>[#ref]".to_string());
            }
            validate_git_url(url)?;
            return Ok(Self::Git {
                url: url.to_string(),
                reference: reference.map(str::to_string),
            });
        }
        Ok(Self::Local(PathBuf::from(value)))
    }

    pub fn request_text(&self) -> String {
        match self {
            Self::Local(path) => path.display().to_string(),
            Self::Npm { package, version } => match version {
                Some(version) => format!("npm:{package}@{version}"),
                None => format!("npm:{package}"),
            },
            Self::Git { url, reference } => match reference {
                Some(reference) => format!("git:{url}#{reference}"),
                None => format!("git:{url}"),
            },
        }
    }
}

#[derive(Clone, Debug)]
pub struct PreparedPluginInstall {
    pub record: PreparedPluginInstallRecord,
}

/// Stages and inspects a plugin without starting a process from its package.
pub async fn prepare(store: &Store, source: PluginSource) -> Result<PreparedPluginInstall, String> {
    let _slot = acquire_preparation_slot(&PREPARATION_SLOTS, PLUGIN_ADMIN_TIMEOUT).await?;
    let id = Uuid::new_v4().to_string();
    let staging = store.data_dir().join("plugin-staging").join(&id);
    fs::create_dir_all(&staging)
        .map_err(|error| format!("cannot create plugin staging directory: {error}"))?;
    let requested_source = match &source {
        PluginSource::Local(path) => fs::canonicalize(path)
            .unwrap_or_else(|_| path.clone())
            .display()
            .to_string(),
        _ => source.request_text(),
    };
    let result = stage_source(&staging, source).await.and_then(|provenance| {
        let manifest_path = find_manifest(&staging)?;
        let manifest = PluginManifest::load(&manifest_path).map_err(|error| error.to_string())?;
        // Register static definitions now, still without starting a plugin.
        let mut registry = crate::PluginRegistry::default();
        registry
            .register_manifest(manifest.clone())
            .map_err(|error| error.to_string())?;
        let digest = directory_digest(&staging)?;
        let policy = serde_json::to_value(manifest.permissions.as_ref())
            .map_err(|error| error.to_string())?;
        let policy = if policy.is_null() { json!({}) } else { policy };
        let resolved_lock = json!({
            "formatVersion": 1,
            "plugin": { "id": manifest.id, "version": manifest.version },
            "contentDigest": digest,
            "source": provenance,
            "requestedSource": requested_source,
        });
        Ok(PreparedPluginInstall {
            record: PreparedPluginInstallRecord {
                id,
                plugin_id: manifest.id,
                plugin_name: manifest.name,
                version: manifest.version,
                digest,
                staged_path: staging.display().to_string(),
                prepared_at: now()?,
                policy,
                resolved_lock,
                provenance,
            },
        })
    });
    match result {
        Ok(prepared) => {
            store.save_prepared_plugin_install(&prepared.record)?;
            Ok(prepared)
        }
        Err(error) => {
            let _ = fs::remove_dir_all(&staging);
            Err(error)
        }
    }
}

/// Commits exactly the bytes that were reviewed by [`prepare`].
pub fn commit(
    store: &Store,
    prepared_id: &str,
    expected_digest: &str,
) -> Result<InstalledPluginRecord, String> {
    let prepared = store
        .prepared_plugin_install(prepared_id)?
        .ok_or_else(|| format!("prepared plugin installation {prepared_id} was not found"))?;
    if !constant_time_eq(&prepared.digest, expected_digest) {
        return Err("the approved digest does not match the prepared installation".to_string());
    }
    let staged = PathBuf::from(&prepared.staged_path);
    let actual = directory_digest(&staged)?;
    if !constant_time_eq(&prepared.digest, &actual) {
        return Err(
            "prepared plugin content changed after inspection; prepare it again".to_string(),
        );
    }
    let manifest =
        PluginManifest::load(find_manifest(&staged)?).map_err(|error| error.to_string())?;
    if manifest.id != prepared.plugin_id || manifest.version != prepared.version {
        return Err(
            "prepared plugin manifest changed after inspection; prepare it again".to_string(),
        );
    }
    let source = prepared
        .resolved_lock
        .get("requestedSource")
        .and_then(Value::as_str)
        .ok_or_else(|| "prepared plugin has no update source".to_owned())?
        .to_owned();
    let destination = store
        .data_dir()
        .join("plugins")
        .join(&manifest.id)
        .join(format!("{}-{}", manifest.version, &prepared.digest[7..19]));
    let already_staged = destination.exists();
    if already_staged {
        let existing_digest = directory_digest(&destination)?;
        if !constant_time_eq(&existing_digest, &prepared.digest) {
            return Err("plugin activation destination contains different content".to_owned());
        }
    }

    validate_prospective_catalog(store, &manifest)?;
    let parent = destination
        .parent()
        .ok_or_else(|| "plugin destination has no parent".to_string())?;
    fs::create_dir_all(parent)
        .map_err(|error| format!("cannot create plugin installation directory: {error}"))?;
    if !already_staged {
        fs::rename(&staged, &destination)
            .map_err(|error| format!("cannot commit plugin package: {error}"))?;
    }
    let manifest_path = find_manifest(&destination)?;
    let record = InstalledPluginRecord {
        id: manifest.id,
        name: manifest.name,
        version: manifest.version,
        manifest_path: manifest_path.display().to_string(),
        installed_at: now()?,
        enabled: true,
        policy: prepared.policy,
        digest: prepared.digest,
        resolved_lock: prepared.resolved_lock,
        provenance: prepared.provenance,
    };
    if let Err(error) = store.upsert_plugin_install_with_source(&record, Some(&source)) {
        if !already_staged {
            let _ = fs::rename(&destination, &staged);
        }
        return Err(error);
    }
    store.remove_prepared_plugin_install(prepared_id)?;
    if already_staged {
        let _ = fs::remove_dir_all(&staged);
    }
    Ok(record)
}

/// Performs the one-command install flow after an explicit digest approval.
pub async fn install(
    store: &Store,
    source: PluginSource,
    approved_digest: &str,
) -> Result<InstalledPluginRecord, String> {
    let prepared = prepare(store, source).await?;
    if !constant_time_eq(&prepared.record.digest, approved_digest) {
        discard_prepared(store, &prepared.record)?;
        return Err("the approved digest does not match the reviewed package".to_owned());
    }
    commit(store, &prepared.record.id, approved_digest)
}

/// Prepares an update from an explicit source or the source recorded at the
/// last successful activation. Updating never silently changes plugin identity.
pub async fn update(
    store: &Store,
    plugin_id: &str,
    source: Option<PluginSource>,
    approved_digest: &str,
) -> Result<InstalledPluginRecord, String> {
    let prepared = prepare_update(store, plugin_id, source).await?;
    if !constant_time_eq(&prepared.record.digest, approved_digest) {
        discard_prepared(store, &prepared.record)?;
        return Err("the approved digest does not match the reviewed package".to_owned());
    }
    commit(store, &prepared.record.id, approved_digest)
}

/// Reviews an update without activating it. Approval remains a separate,
/// digest-specific commit operation.
pub async fn prepare_update(
    store: &Store,
    plugin_id: &str,
    source: Option<PluginSource>,
) -> Result<PreparedPluginInstall, String> {
    let installed = store
        .list_installed_plugins()?
        .into_iter()
        .find(|plugin| plugin.id == plugin_id)
        .ok_or_else(|| format!("plugin {plugin_id} is not installed"))?;
    let lifecycle = store
        .plugin_removal_state(plugin_id)?
        .ok_or_else(|| "plugin lifecycle record is missing".to_owned())?;
    if lifecycle.status != "active" {
        return Err(format!(
            "plugin {plugin_id} cannot be updated while {}",
            lifecycle.status
        ));
    }
    if store.plugin_execution_pin_count(plugin_id)? > 0 {
        return Err("plugin cannot be updated while an admitted execution uses it".to_owned());
    }
    let source =
        match source {
            Some(source) => source,
            None => PluginSource::parse(lifecycle.source.as_deref().ok_or_else(|| {
                "plugin has no stored update source; provide --source".to_owned()
            })?)?,
        };
    let prepared = prepare(store, source).await?;
    if prepared.record.plugin_id != installed.id {
        discard_prepared(store, &prepared.record)?;
        return Err("update source resolves to a different plugin identity".to_owned());
    }
    Ok(prepared)
}

fn discard_prepared(store: &Store, prepared: &PreparedPluginInstallRecord) -> Result<(), String> {
    let path = PathBuf::from(&prepared.staged_path);
    if path.starts_with(store.data_dir().join("plugin-staging")) && path.exists() {
        fs::remove_dir_all(path)
            .map_err(|_| "cannot discard rejected plugin staging".to_owned())?;
    }
    store.remove_prepared_plugin_install(&prepared.id)?;
    Ok(())
}

/// Discards an explicitly rejected staged package without touching the active
/// installation.
pub fn discard_prepared_id(store: &Store, prepared_id: &str) -> Result<bool, String> {
    let Some(prepared) = store.prepared_plugin_install(prepared_id)? else {
        return Ok(false);
    };
    discard_prepared(store, &prepared)?;
    Ok(true)
}

fn validate_prospective_catalog(store: &Store, candidate: &PluginManifest) -> Result<(), String> {
    let installed = store.list_installed_plugins()?;
    if installed.len() > 10_000 {
        return Err("installed plugin catalog exceeds its validation limit".to_owned());
    }
    let mut registry = crate::PluginRegistry::default();
    for plugin in installed
        .into_iter()
        .filter(|plugin| plugin.enabled && plugin.id != candidate.id)
    {
        registry
            .register_manifest_path(&plugin.manifest_path)
            .map_err(|error| format!("installed plugin catalog is not valid: {error}"))?;
    }
    registry
        .register_manifest(candidate.clone())
        .map_err(|error| format!("prospective plugin catalog is not valid: {error}"))
}

async fn stage_source(staging: &Path, source: PluginSource) -> Result<Value, String> {
    match source {
        PluginSource::Local(path) => {
            let root = if path.is_file() {
                path.parent()
                    .ok_or_else(|| "plugin manifest has no parent directory".to_string())?
                    .to_path_buf()
            } else {
                path
            };
            let root = fs::canonicalize(root)
                .map_err(|error| format!("cannot access local plugin source: {error}"))?;
            copy_tree(&root, staging)?;
            Ok(json!({ "kind": "local", "path": root.display().to_string() }))
        }
        PluginSource::Npm { package, version } => {
            stage_npm(staging, &package, version.as_deref()).await
        }
        PluginSource::Git { url, reference } => {
            validate_git_url(&url)?;
            stage_git(staging, &url, reference.as_deref()).await
        }
    }
}

async fn stage_npm(
    staging: &Path,
    package: &str,
    requested_version: Option<&str>,
) -> Result<Value, String> {
    let client = plugin_download_client()?;
    stage_npm_with_client(
        staging,
        package,
        requested_version,
        "https://registry.npmjs.org",
        &client,
    )
    .await
}

async fn stage_npm_with_client(
    staging: &Path,
    package: &str,
    requested_version: Option<&str>,
    registry_base: &str,
    client: &reqwest::Client,
) -> Result<Value, String> {
    let encoded = package.replace('/', "%2f");
    let metadata_url = format!("{}/{encoded}", registry_base.trim_end_matches('/'));
    let metadata_bytes =
        fetch_bytes_with_client(client, &metadata_url, 8 * 1024 * 1024, "npm metadata").await?;
    let metadata: Value = serde_json::from_slice(&metadata_bytes)
        .map_err(|_| "npm metadata is not valid JSON".to_owned())?;
    let version = requested_version
        .map(str::to_string)
        .or_else(|| {
            metadata
                .pointer("/dist-tags/latest")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .ok_or_else(|| format!("npm package {package} has no requested or latest version"))?;
    let release = metadata
        .pointer(&format!("/versions/{}", json_pointer(&version)))
        .ok_or_else(|| format!("npm package {package}@{version} was not found"))?;
    let tarball = release
        .pointer("/dist/tarball")
        .and_then(Value::as_str)
        .ok_or_else(|| "npm package metadata has no dist.tarball".to_string())?;
    let tarball_url = reqwest::Url::parse(tarball)
        .map_err(|_| "npm package metadata has an invalid tarball URL".to_owned())?;
    if tarball_url.scheme() != "https"
        || !tarball_url.username().is_empty()
        || tarball_url.password().is_some()
    {
        return Err("npm package tarball must use HTTPS without credentials".to_owned());
    }
    let integrity = release
        .pointer("/dist/integrity")
        .and_then(Value::as_str)
        .ok_or_else(|| "npm package metadata has no dist.integrity".to_string())?;
    let bytes = fetch_bytes_with_client(
        client,
        tarball,
        MAX_PLUGIN_PACKAGE_BYTES as usize,
        "npm tarball",
    )
    .await?;
    verify_npm_integrity(&bytes, integrity)?;
    extract_npm_tarball(&bytes, staging)?;
    Ok(
        json!({ "kind": "npm", "package": package, "version": version, "tarball": tarball, "integrity": integrity }),
    )
}

async fn stage_git(staging: &Path, url: &str, reference: Option<&str>) -> Result<Value, String> {
    let reference = reference.unwrap_or("HEAD");
    let mut resolve = Command::new("git");
    resolve.args(["ls-remote", "--", url, reference]);
    let (resolve_status, resolve_stdout) =
        run_bounded_command(resolve, "Git reference lookup").await?;
    if !resolve_status {
        return Err("cannot resolve Git reference".to_string());
    }
    let commit = String::from_utf8_lossy(&resolve_stdout)
        .split_whitespace()
        .next()
        .filter(|value| value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(|| "Git reference did not resolve to a commit".to_string())?
        .to_string();
    let checkout = staging.join(".checkout");
    let mut clone = Command::new("git");
    clone
        .args([
            "-c",
            &format!("core.hooksPath={}", disabled_hooks_path()),
            "-c",
            "submodule.recurse=false",
            "clone",
            "--no-checkout",
            "--no-recurse-submodules",
            url,
            &checkout.display().to_string(),
        ])
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", disabled_hooks_path())
        .env("GIT_LFS_SKIP_SMUDGE", "1");
    if !run_bounded_command(clone, "Git clone").await?.0 {
        return Err("cannot stage Git source without executing hooks".to_string());
    }
    let mut checkout_command = Command::new("git");
    checkout_command
        .current_dir(&checkout)
        .args([
            "-c",
            &format!("core.hooksPath={}", disabled_hooks_path()),
            "-c",
            "submodule.recurse=false",
            "checkout",
            "--no-recurse-submodules",
            &commit,
        ])
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", disabled_hooks_path())
        .env("GIT_LFS_SKIP_SMUDGE", "1");
    if !run_bounded_command(checkout_command, "Git checkout")
        .await?
        .0
    {
        return Err("cannot checkout resolved Git commit".to_string());
    }
    copy_tree(&checkout, staging)?;
    fs::remove_dir_all(checkout)
        .map_err(|error| format!("cannot remove Git metadata from staging: {error}"))?;
    Ok(json!({ "kind": "git", "url": url, "reference": reference, "commit": commit }))
}

fn plugin_download_client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .timeout(PLUGIN_ADMIN_TIMEOUT)
        .redirect(reqwest::redirect::Policy::limited(5))
        .build()
        .map_err(|_| "cannot configure bounded plugin download".to_owned())
}

async fn fetch_bytes_with_client(
    client: &reqwest::Client,
    url: &str,
    limit: usize,
    label: &str,
) -> Result<Vec<u8>, String> {
    let mut response = client
        .get(url)
        .send()
        .await
        .map_err(|_| format!("cannot download {label} within the configured limit"))?
        .error_for_status()
        .map_err(|_| format!("cannot download {label}: remote server rejected the request"))?;
    if response.url().scheme() != "https" {
        return Err(format!(
            "{label} download redirected to an unsupported scheme"
        ));
    }
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(format!("{label} exceeds the configured byte limit"));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| format!("cannot read {label} response"))?
    {
        if bytes.len().saturating_add(chunk.len()) > limit {
            return Err(format!("{label} exceeds the configured byte limit"));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

async fn run_bounded_command(command: Command, label: &str) -> Result<(bool, Vec<u8>), String> {
    run_bounded_command_with_timeout(command, label, PLUGIN_ADMIN_TIMEOUT).await
}

async fn run_bounded_command_with_timeout(
    mut command: Command,
    label: &str,
    command_timeout: Duration,
) -> Result<(bool, Vec<u8>), String> {
    use std::process::Stdio;

    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .map_err(|_| format!("cannot start bounded {label}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| format!("cannot read bounded {label} output"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| format!("cannot read bounded {label} diagnostics"))?;
    let stdout_task = tokio::spawn(read_process_pipe(stdout));
    let stderr_task = tokio::spawn(read_process_pipe(stderr));
    let status = match timeout(command_timeout, child.wait()).await {
        Ok(Ok(status)) => status,
        Ok(Err(_)) => {
            let _ = stdout_task.await;
            let _ = stderr_task.await;
            return Err(format!("cannot wait for bounded {label}"));
        }
        Err(_) => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            let _ = stdout_task.await;
            let _ = stderr_task.await;
            return Err(format!("{label} exceeded the configured duration limit"));
        }
    };
    let stdout = stdout_task
        .await
        .map_err(|_| format!("{label} output reader failed"))??;
    let _stderr = stderr_task
        .await
        .map_err(|_| format!("{label} diagnostic reader failed"))??;
    Ok((status.success(), stdout))
}

async fn read_process_pipe<R>(reader: R) -> Result<Vec<u8>, String>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut output = Vec::new();
    reader
        .take((MAX_PLUGIN_PROCESS_OUTPUT + 1) as u64)
        .read_to_end(&mut output)
        .await
        .map_err(|_| "cannot read bounded plugin process output".to_owned())?;
    if output.len() > MAX_PLUGIN_PROCESS_OUTPUT {
        return Err("plugin process output exceeds the configured byte limit".to_owned());
    }
    Ok(output)
}

const fn disabled_hooks_path() -> &'static str {
    if cfg!(windows) { "NUL" } else { "/dev/null" }
}

fn npm_spec(spec: &str) -> Result<(String, Option<String>), String> {
    if spec.is_empty() {
        return Err("npm source must be npm:<package>[@version]".to_string());
    }
    let split = if let Some(scoped) = spec.strip_prefix('@') {
        scoped.rfind('@').map(|index| index + 1)
    } else {
        spec.rfind('@')
    };
    let (package, version) = split.map_or((spec, None), |index| {
        (&spec[..index], Some(&spec[index + 1..]))
    });
    let valid_segment = |value: &str| {
        !value.is_empty()
            && !value.starts_with(['.', '-'])
            && value.bytes().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'-' | b'_' | b'.')
            })
    };
    let valid_package = if let Some(scoped) = package.strip_prefix('@') {
        scoped.split_once('/').is_some_and(|(scope, name)| {
            valid_segment(scope) && valid_segment(name) && !name.contains('/')
        })
    } else {
        valid_segment(package)
    };
    if !valid_package
        || version.is_some_and(str::is_empty)
        || version.is_some_and(|value| {
            value != "latest"
                && value.bytes().any(|byte| {
                    !(byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'+' | b'-'))
                })
        })
    {
        return Err("npm source must be npm:<package>[@version]".to_string());
    }
    Ok((package.to_string(), version.map(str::to_string)))
}

fn validate_git_url(value: &str) -> Result<(), String> {
    let parsed = reqwest::Url::parse(value)
        .map_err(|_| "git source must use a valid HTTPS or SSH URL".to_owned())?;
    if !matches!(parsed.scheme(), "https" | "ssh")
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(
            "git source must not contain credentials, query parameters, or unsupported schemes"
                .to_string(),
        );
    }
    Ok(())
}

fn find_manifest(root: &Path) -> Result<PathBuf, String> {
    PLUGIN_MANIFEST_NAMES
        .iter()
        .map(|name| root.join(name))
        .find(|path| path.is_file())
        .ok_or_else(|| {
            format!(
                "plugin source must contain one of {}",
                PLUGIN_MANIFEST_NAMES.join(", ")
            )
        })
}

fn copy_tree(source: &Path, destination: &Path) -> Result<(), String> {
    copy_tree_with_limits(
        source,
        destination,
        MAX_PLUGIN_FILES,
        MAX_PLUGIN_PACKAGE_BYTES,
    )
}

fn copy_tree_with_limits(
    source: &Path,
    destination: &Path,
    max_files: usize,
    max_bytes: u64,
) -> Result<(), String> {
    let mut budget = (0_usize, 0_u64);
    copy_tree_bounded(source, destination, &mut budget, 0, max_files, max_bytes)
}

fn copy_tree_bounded(
    source: &Path,
    destination: &Path,
    budget: &mut (usize, u64),
    depth: usize,
    max_files: usize,
    max_bytes: u64,
) -> Result<(), String> {
    if depth > 32 {
        return Err("plugin package exceeds the depth limit".to_owned());
    }
    for entry in
        fs::read_dir(source).map_err(|error| format!("cannot read plugin source: {error}"))?
    {
        let entry = entry.map_err(|error| format!("cannot read plugin source entry: {error}"))?;
        let name = entry.file_name();
        if name == ".git" {
            continue;
        }
        let target = destination.join(&name);
        let kind = entry
            .file_type()
            .map_err(|error| format!("cannot inspect plugin source entry: {error}"))?;
        if kind.is_symlink() {
            return Err(format!(
                "plugin source contains unsupported symlink {}",
                entry.path().display()
            ));
        }
        budget.0 = budget.0.saturating_add(1);
        if budget.0 > max_files {
            return Err("plugin package exceeds the file-count limit".to_owned());
        }
        if kind.is_dir() {
            fs::create_dir_all(&target)
                .map_err(|error| format!("cannot create staged directory: {error}"))?;
            copy_tree_bounded(
                &entry.path(),
                &target,
                budget,
                depth + 1,
                max_files,
                max_bytes,
            )?;
        } else if kind.is_file() {
            let remaining = max_bytes.saturating_sub(budget.1);
            let mut input = fs::File::open(entry.path())
                .map_err(|error| format!("cannot read plugin source file: {error}"))?;
            let mut output = fs::File::create(&target)
                .map_err(|error| format!("cannot create staged plugin file: {error}"))?;
            let copied = std::io::copy(
                &mut Read::by_ref(&mut input).take(remaining + 1),
                &mut output,
            )
            .map_err(|error| format!("cannot stage plugin file: {error}"))?;
            budget.1 = budget.1.saturating_add(copied);
            if budget.1 > max_bytes {
                return Err("plugin package exceeds the aggregate byte limit".to_owned());
            }
        } else {
            return Err(format!(
                "plugin source contains unsupported entry {}",
                entry.path().display()
            ));
        }
    }
    Ok(())
}

fn extract_npm_tarball(bytes: &[u8], destination: &Path) -> Result<(), String> {
    extract_npm_tarball_with_limits(
        bytes,
        destination,
        MAX_PLUGIN_FILES,
        MAX_PLUGIN_PACKAGE_BYTES,
    )
}

fn extract_npm_tarball_with_limits(
    bytes: &[u8],
    destination: &Path,
    max_files: usize,
    max_bytes: u64,
) -> Result<(), String> {
    let mut archive = Archive::new(GzDecoder::new(bytes));
    let mut budget = (0_usize, 0_u64);
    for entry in archive
        .entries()
        .map_err(|error| format!("cannot inspect npm tarball: {error}"))?
    {
        let entry = entry.map_err(|error| format!("cannot read npm tarball entry: {error}"))?;
        budget.0 = budget.0.saturating_add(1);
        if budget.0 > max_files {
            return Err("npm package exceeds the file-count limit".to_owned());
        }
        if entry.header().entry_type().is_dir() {
            continue;
        }
        if !entry.header().entry_type().is_file() {
            return Err("npm package contains a non-regular file".to_owned());
        }
        let path = entry
            .path()
            .map_err(|error| format!("npm tarball has invalid path: {error}"))?;
        let relative = npm_relative_path(&path)?;
        let target = destination.join(relative);
        let advertised_size = entry
            .header()
            .size()
            .map_err(|_| "npm package has invalid file metadata".to_owned())?;
        budget.1 = budget.1.saturating_add(advertised_size);
        if budget.1 > max_bytes {
            return Err("npm package exceeds the aggregate byte limit".to_owned());
        }
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("cannot stage npm directory: {error}"))?;
        }
        let mut output =
            fs::File::create(&target).map_err(|error| format!("cannot stage npm file: {error}"))?;
        let copied = std::io::copy(&mut entry.take(advertised_size + 1), &mut output)
            .map_err(|error| format!("cannot extract npm file: {error}"))?;
        if copied != advertised_size {
            return Err("npm package file length does not match its metadata".to_owned());
        }
    }
    Ok(())
}

fn npm_relative_path(path: &Path) -> Result<&Path, String> {
    let relative = path
        .strip_prefix("package")
        .map_err(|_| "npm tarball entry is outside package/".to_string())?;
    if relative.as_os_str().is_empty()
        || relative.components().any(|part| {
            matches!(
                part,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err("npm tarball contains unsafe path".to_string());
    }
    Ok(relative)
}

fn verify_npm_integrity(bytes: &[u8], integrity: &str) -> Result<(), String> {
    let encoded = integrity
        .strip_prefix("sha512-")
        .ok_or_else(|| "npm integrity must use sha512".to_string())?;
    let expected = base64_decode(encoded)?;
    let actual = Sha512::digest(bytes);
    if expected.as_slice() == actual.as_slice() {
        Ok(())
    } else {
        Err("npm tarball integrity check failed".to_string())
    }
}

fn base64_decode(value: &str) -> Result<Vec<u8>, String> {
    const ALPHABET: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = Vec::new();
    let mut buffer = 0_u32;
    let mut bits = 0_u8;
    for byte in value.bytes().filter(|byte| !byte.is_ascii_whitespace()) {
        if byte == b'=' {
            break;
        }
        let value = ALPHABET
            .bytes()
            .position(|candidate| candidate == byte)
            .ok_or_else(|| "npm integrity is not valid base64".to_string())?
            as u32;
        buffer = (buffer << 6) | value;
        bits += 6;
        while bits >= 8 {
            bits -= 8;
            output.push(((buffer >> bits) & 0xff) as u8);
        }
    }
    Ok(output)
}

fn directory_digest(root: &Path) -> Result<String, String> {
    package_digest(root)
}

/// Calculates the bounded, deterministic digest used for plugin approval.
pub fn package_digest(root: &Path) -> Result<String, String> {
    package_digest_with_limits(root, MAX_PLUGIN_FILES, MAX_PLUGIN_PACKAGE_BYTES)
}

fn package_digest_with_limits(
    root: &Path,
    max_files: usize,
    max_bytes: u64,
) -> Result<String, String> {
    let metadata = fs::symlink_metadata(root)
        .map_err(|error| format!("cannot inspect plugin package root: {error}"))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err("plugin package root must be a regular directory".to_owned());
    }
    let mut files = BTreeMap::new();
    let mut budget = (0_usize, 0_u64);
    collect_files(root, root, &mut files, &mut budget, 0, max_files, max_bytes)?;
    let mut digest = Sha256::new();
    for (path, contents) in files {
        digest.update(path.as_bytes());
        digest.update([0]);
        digest.update((contents.len() as u64).to_be_bytes());
        digest.update(contents);
    }
    Ok(format!("sha256:{:x}", digest.finalize()))
}

fn collect_files(
    root: &Path,
    current: &Path,
    files: &mut BTreeMap<String, Vec<u8>>,
    budget: &mut (usize, u64),
    depth: usize,
    max_files: usize,
    max_bytes: u64,
) -> Result<(), String> {
    if depth > 32 {
        return Err("plugin package exceeds the depth limit".to_owned());
    }
    for entry in
        fs::read_dir(current).map_err(|error| format!("cannot read staged plugin: {error}"))?
    {
        let entry = entry.map_err(|error| format!("cannot inspect staged plugin: {error}"))?;
        budget.0 += 1;
        if budget.0 > max_files {
            return Err("plugin package exceeds the file-count limit".to_owned());
        }
        let kind = entry
            .file_type()
            .map_err(|error| format!("cannot inspect staged plugin entry: {error}"))?;
        if kind.is_symlink() {
            return Err("staged plugin contains a symlink".to_string());
        }
        if kind.is_dir() {
            collect_files(
                root,
                &entry.path(),
                files,
                budget,
                depth + 1,
                max_files,
                max_bytes,
            )?;
        } else if kind.is_file() {
            let relative = entry
                .path()
                .strip_prefix(root)
                .map_err(|_| "cannot resolve staged plugin path".to_string())?
                .to_string_lossy()
                .replace('\\', "/");
            let remaining = max_bytes.saturating_sub(budget.1);
            let file = fs::File::open(entry.path())
                .map_err(|error| format!("cannot read staged plugin file: {error}"))?;
            let mut bytes = Vec::new();
            file.take(remaining + 1)
                .read_to_end(&mut bytes)
                .map_err(|error| format!("cannot read staged plugin file: {error}"))?;
            budget.1 += bytes.len() as u64;
            if budget.1 > max_bytes {
                return Err("plugin package exceeds the aggregate byte limit".to_owned());
            }
            files.insert(relative, bytes);
        } else {
            return Err("staged plugin contains an unsupported entry".to_string());
        }
    }
    Ok(())
}

async fn acquire_preparation_slot(
    semaphore: &Semaphore,
    wait_limit: Duration,
) -> Result<SemaphorePermit<'_>, String> {
    timeout(wait_limit, semaphore.acquire())
        .await
        .map_err(|_| "plugin preparation concurrency wait exceeded its limit".to_owned())?
        .map_err(|_| "plugin preparation is shutting down".to_owned())
}

fn json_pointer(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}
fn constant_time_eq(left: &str, right: &str) -> bool {
    left.len() == right.len()
        && left
            .bytes()
            .zip(right.bytes())
            .fold(0_u8, |diff, (a, b)| diff | (a ^ b))
            == 0
}
fn now() -> Result<String, String> {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        fs,
        io::Write,
        path::PathBuf,
        time::{Duration, Instant},
    };

    use axum::{Json, Router, body::Body, response::Response, routing::get};
    use flate2::{Compression, write::GzEncoder};
    use sha2::Digest;
    use tokio::{io::AsyncWriteExt, sync::oneshot};
    use uuid::Uuid;

    use super::*;

    fn temp_dir(label: &str) -> PathBuf {
        let root = std::env::temp_dir().join("opencode");
        fs::create_dir_all(&root).expect("approved temporary root exists");
        let path = root.join(format!("kakune-plugin-{label}-{}", Uuid::new_v4()));
        fs::create_dir(&path).expect("isolated fixture directory is created");
        path
    }

    #[test]
    fn subprocess_timeout_child_fixture() {
        let (Some(starts), Some(marker)) = (
            std::env::var_os("KAKUNE_PLUGIN_TIMEOUT_STARTS"),
            std::env::var_os("KAKUNE_PLUGIN_TIMEOUT_MARKER"),
        ) else {
            return;
        };
        fs::write(starts, "start").expect("child start is recorded");
        std::thread::sleep(Duration::from_secs(5));
        fs::write(marker, "finished").expect("child completion is recorded");
    }

    fn test_tls_config() -> crate::config::ApiConfig {
        crate::config::ApiConfig {
            tls_cert: Some(
                concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/tests/fixtures/provider-test-cert.pem"
                )
                .into(),
            ),
            tls_key: Some(
                concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/tests/fixtures/provider-test-leaf-key.pem"
                )
                .into(),
            ),
            ..Default::default()
        }
    }

    fn test_download_client() -> reqwest::Client {
        reqwest::Client::builder()
            // These tests exercise HTTPS-only download logic, not certificate
            // validation; other provider tests cover certificate trust.
            .danger_accept_invalid_certs(true)
            // The fixture deadline only guards the test harness. It stays wide
            // enough that a loaded runner does not turn a local handshake into
            // a spurious transport failure.
            .timeout(Duration::from_secs(30))
            .build()
            .expect("bounded test client builds")
    }

    fn append_tar_file<W: Write>(archive: &mut tar::Builder<W>, path: &str, contents: &[u8]) {
        let mut header = tar::Header::new_gnu();
        header.set_size(contents.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        archive
            .append_data(&mut header, path, contents)
            .expect("package fixture entry is added");
    }

    fn npm_fixture_tarball() -> Vec<u8> {
        let compressed = GzEncoder::new(Vec::new(), Compression::default());
        let mut archive = tar::Builder::new(compressed);
        append_tar_file(
            &mut archive,
            "package/kakune.plugin.json",
            br#"{"manifestVersion":"1.0","id":"org.example.npm-fixture","name":"Npm fixture","version":"1.2.3","pluginProtocol":">=1.0.0 <2.0.0","runtime":{"kind":"process","command":"never-run","args":[]},"contributes":{"nodes":["nodes/hello.json"]}}"#,
        );
        append_tar_file(
            &mut archive,
            "package/nodes/hello.json",
            br#"{"apiVersion":"kakune.dev/v1","kind":"NodeDefinition","type":"org.example.npm-fixture.hello@1","name":"Hello","inputSchema":{"type":"object"},"outputSchema":{"type":"object"}}"#,
        );
        archive
            .into_inner()
            .expect("npm tarball is finalized")
            .finish()
            .expect("npm gzip stream is finalized")
    }

    fn encode_base64(bytes: &[u8]) -> String {
        const ALPHABET: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut output = String::new();
        for chunk in bytes.chunks(3) {
            let first = u32::from(chunk[0]);
            let second = chunk.get(1).copied().map(u32::from);
            let third = chunk.get(2).copied().map(u32::from);
            let combined =
                (first << 16) | (second.unwrap_or_default() << 8) | third.unwrap_or_default();
            output.push(char::from(ALPHABET[((combined >> 18) & 63) as usize]));
            output.push(char::from(ALPHABET[((combined >> 12) & 63) as usize]));
            output.push(second.map_or('=', |_| {
                char::from(ALPHABET[((combined >> 6) & 63) as usize])
            }));
            output.push(third.map_or('=', |_| char::from(ALPHABET[(combined & 63) as usize])));
        }
        output
    }

    fn git(repository: &std::path::Path, args: &[&str]) {
        let output = std::process::Command::new("git")
            .current_dir(repository)
            .args(args)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", disabled_hooks_path())
            .env("GIT_AUTHOR_NAME", "Kakune fixture")
            .env("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
            .env("GIT_COMMITTER_NAME", "Kakune fixture")
            .env("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
            .output()
            .expect("Git fixture command starts");
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn package_bytes_are_bounded_during_copy_and_digest_walks() {
        let root = temp_dir("byte-bound");
        let source = root.join("source");
        let destination = root.join("destination");
        fs::create_dir(&source).expect("source directory is created");
        fs::create_dir(&destination).expect("destination directory is created");
        fs::write(source.join("payload.bin"), b"123456789").expect("payload is written");

        assert!(
            package_digest_with_limits(&source, 10, 8)
                .unwrap_err()
                .contains("aggregate byte limit")
        );
        assert!(
            copy_tree_with_limits(&source, &destination, 10, 8)
                .unwrap_err()
                .contains("aggregate byte limit")
        );
        assert_eq!(
            fs::metadata(destination.join("payload.bin")).unwrap().len(),
            9
        );
        fs::remove_dir_all(root).expect("fixture is cleaned");
    }

    #[test]
    fn npm_archive_paths_cannot_escape_the_package_root() {
        let error = npm_relative_path(Path::new("package/../escape.txt"))
            .expect_err("traversal is rejected");
        assert!(error.contains("unsafe path"));
        assert!(npm_relative_path(Path::new("package/nodes/hello.json")).is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn npm_source_uses_bounded_local_registry_and_verifies_integrity() {
        let root = temp_dir("npm-source");
        let package_tarball = npm_fixture_tarball();
        let integrity = format!(
            "sha512-{}",
            encode_base64(&Sha512::digest(&package_tarball))
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("TLS registry fixture binds");
        let port = listener.local_addr().unwrap().port();
        let metadata = serde_json::json!({
            "dist-tags":{"latest":"1.2.3"},
            "versions":{"1.2.3":{"dist":{
                "tarball":format!("https://127.0.0.1:{port}/package.tgz"),
                "integrity":integrity
            }}}
        });
        let metadata_response = metadata.clone();
        let tarball_response = package_tarball.clone();
        let app = Router::new()
            .route(
                "/fixture-plugin",
                get(move || {
                    let metadata = metadata_response.clone();
                    async move { Json(metadata) }
                }),
            )
            .route(
                "/package.tgz",
                get(move || {
                    let bytes = tarball_response.clone();
                    async move { Body::from(bytes) }
                }),
            );
        let tls = test_tls_config();
        let (stop, stopped) = oneshot::channel();
        let server = tokio::spawn(async move {
            crate::server::serve(listener, app, &tls, async {
                let _ = stopped.await;
            })
            .await
            .expect("TLS registry fixture exits cleanly");
        });
        let client = test_download_client();
        let staging = root.join("staging");
        fs::create_dir(&staging).expect("staging directory is created");
        let provenance = stage_npm_with_client(
            &staging,
            "fixture-plugin",
            None,
            &format!("https://127.0.0.1:{port}"),
            &client,
        )
        .await
        .expect("npm package metadata and integrity-checked archive stage");
        assert_eq!(provenance["version"], "1.2.3");
        assert_eq!(
            PluginManifest::load(staging.join("kakune.plugin.json"))
                .unwrap()
                .id,
            "org.example.npm-fixture"
        );
        stop.send(())
            .expect("registry server shutdown is signalled");
        server.await.expect("registry server task joins");
        fs::remove_dir_all(root).expect("fixture is cleaned");
    }

    #[tokio::test]
    async fn git_source_resolves_a_local_fixture_without_running_checkout_hooks() {
        let root = temp_dir("git-source");
        let repository = root.join("repository");
        let staging = root.join("staging");
        fs::create_dir(&repository).expect("repository directory is created");
        fs::create_dir(&staging).expect("staging directory is created");
        fs::create_dir(repository.join("nodes")).expect("node directory is created");
        fs::write(
            repository.join("kakune.plugin.json"),
            serde_json::to_vec(&serde_json::json!({
                "manifestVersion":"1.0", "id":"org.example.git-fixture", "name":"Git fixture",
                "version":"1.0.0", "pluginProtocol":">=1.0.0 <2.0.0",
                "runtime":{"kind":"process","command":"never-run","args":[]},
                "contributes":{"nodes":["nodes/hello.json"]}
            }))
            .unwrap(),
        )
        .expect("plugin manifest is written");
        fs::write(
            repository.join("nodes/hello.json"),
            serde_json::to_vec(&serde_json::json!({
                "apiVersion":"kakune.dev/v1", "kind":"NodeDefinition",
                "type":"org.example.git-fixture.hello@1", "name":"Hello",
                "inputSchema":{"type":"object"}, "outputSchema":{"type":"object"}
            }))
            .unwrap(),
        )
        .expect("node definition is written");
        git(&repository, &["init", "--quiet"]);
        git(
            &repository,
            &["config", "user.email", "fixture@example.invalid"],
        );
        git(&repository, &["config", "user.name", "Kakune fixture"]);
        git(&repository, &["add", "."]);
        git(&repository, &["commit", "--quiet", "-m", "fixture package"]);

        let url =
            reqwest::Url::from_file_path(&repository).expect("fixture path becomes a file URL");
        let provenance = stage_git(&staging, url.as_str(), Some("HEAD"))
            .await
            .expect("bounded git lookup, clone, and checkout work on local fixture");
        assert_eq!(provenance["kind"], "git");
        assert_eq!(provenance["commit"].as_str().unwrap().len(), 40);
        assert!(staging.join("kakune.plugin.json").is_file());
        assert!(!staging.join(".git").exists());
        assert!(
            validate_git_url(url.as_str()).is_err(),
            "file URLs are not accepted as operator sources"
        );
        fs::remove_dir_all(root).expect("fixture is cleaned");
    }

    #[tokio::test]
    async fn preparation_slot_wait_is_bounded_and_releases_for_retry() {
        let semaphore = Semaphore::new(1);
        let held = semaphore.acquire().await.expect("one slot is acquired");
        let waiting = acquire_preparation_slot(&semaphore, Duration::from_millis(10)).await;
        assert!(
            waiting
                .unwrap_err()
                .contains("concurrency wait exceeded its limit")
        );

        drop(held);
        let retry = acquire_preparation_slot(&semaphore, Duration::from_millis(100)).await;
        assert!(
            retry.is_ok(),
            "a later operation can acquire the released slot"
        );
    }

    #[tokio::test]
    async fn process_output_reader_rejects_bytes_beyond_its_limit() {
        let (mut writer, reader) = tokio::io::duplex(MAX_PLUGIN_PROCESS_OUTPUT + 1);
        let writer_task = tokio::spawn(async move {
            writer
                .write_all(&vec![b'x'; MAX_PLUGIN_PROCESS_OUTPUT + 1])
                .await
                .expect("test output is written");
        });
        let error = read_process_pipe(reader)
            .await
            .expect_err("output over the cap fails");
        assert!(error.contains("output exceeds the configured byte limit"));
        writer_task.await.expect("writer task completes");
    }

    #[tokio::test]
    async fn timed_out_subprocess_is_killed_and_reaped_without_retry() {
        let root = temp_dir("subprocess-timeout");
        let marker = root.join("should-not-be-written.txt");
        let starts = root.join("starts.txt");
        let mut command = tokio::process::Command::new(
            std::env::current_exe().expect("test executable path is available"),
        );
        command
            .args([
                "--exact",
                "plugin_install::tests::subprocess_timeout_child_fixture",
                "--nocapture",
            ])
            .env("KAKUNE_PLUGIN_TIMEOUT_STARTS", &starts)
            .env("KAKUNE_PLUGIN_TIMEOUT_MARKER", &marker);

        let started = Instant::now();
        let result =
            run_bounded_command_with_timeout(command, "test subprocess", Duration::from_secs(1))
                .await;
        assert!(result.unwrap_err().contains("duration limit"));
        assert!(started.elapsed() < Duration::from_secs(4));
        assert!(
            !marker.exists(),
            "the terminated child cannot finish its side effect"
        );
        assert_eq!(fs::read_to_string(starts).unwrap(), "start");
        fs::remove_dir_all(root).expect("fixture is cleaned");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn npm_download_rejects_oversized_streaming_response() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("TLS test listener binds");
        let port = listener.local_addr().unwrap().port();
        let app = Router::new()
            .route(
                "/oversized",
                get(|| async {
                    let chunks = futures_util::stream::iter([
                        Ok::<_, Infallible>(vec![b'a'; 5]),
                        Ok::<_, Infallible>(vec![b'b'; 5]),
                    ]);
                    Response::new(Body::from_stream(chunks))
                }),
            )
            .route(
                "/slow",
                get(|| async {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    "late response"
                }),
            );
        let tls = test_tls_config();
        let (stop, stopped) = oneshot::channel();
        let server = tokio::spawn(async move {
            crate::server::serve(listener, app, &tls, async {
                let _ = stopped.await;
            })
            .await
            .expect("TLS test server exits cleanly");
        });
        let client = test_download_client();
        let error = fetch_bytes_with_client(
            &client,
            &format!("https://127.0.0.1:{port}/oversized"),
            8,
            "test npm response",
        )
        .await
        .expect_err("streaming response exceeds the byte cap");
        assert!(error.contains("exceeds the configured byte limit"));

        let short_timeout_client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .timeout(Duration::from_millis(50))
            .build()
            .expect("short-timeout test client builds");
        let started = Instant::now();
        let error = fetch_bytes_with_client(
            &short_timeout_client,
            &format!("https://127.0.0.1:{port}/slow"),
            100,
            "test timeout response",
        )
        .await
        .expect_err("slow response exceeds the client deadline");
        assert!(error.contains("within the configured limit"));
        assert!(started.elapsed() < Duration::from_millis(750));
        stop.send(()).expect("TLS server shutdown is signalled");
        server.await.expect("TLS server task joins");
    }
}
