//! Two-phase, data-only plugin installation.
//!
//! Preparing a package may download or copy bytes, but it never runs package
//! code, package-manager lifecycle hooks, or dependency installers. Commit is
//! deliberately separate and accepts only the digest returned by prepare.

use std::{
    collections::BTreeMap,
    fs,
    path::{Component, Path, PathBuf},
};

use flate2::read::GzDecoder;
use serde_json::{Value, json};
use sha2::{Digest, Sha256, Sha512};
use tar::Archive;
use tokio::process::Command;
use uuid::Uuid;

use crate::{
    InstalledPluginRecord, PreparedPluginInstallRecord, Store, plugin_process::PluginManifest,
};

pub const PLUGIN_MANIFEST_NAMES: &[&str] = &["kakune.plugin.json", "kakune-plugin.json"];

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
            return Ok(Self::Git {
                url: url.to_string(),
                reference: reference.map(str::to_string),
            });
        }
        Ok(Self::Local(PathBuf::from(value)))
    }
}

#[derive(Clone, Debug)]
pub struct PreparedPluginInstall {
    pub record: PreparedPluginInstallRecord,
}

/// Stages and inspects a plugin without starting a process from its package.
pub async fn prepare(store: &Store, source: PluginSource) -> Result<PreparedPluginInstall, String> {
    let id = Uuid::new_v4().to_string();
    let staging = store.data_dir().join("plugin-staging").join(&id);
    fs::create_dir_all(&staging)
        .map_err(|error| format!("cannot create plugin staging directory: {error}"))?;
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
    let destination = store
        .data_dir()
        .join("plugins")
        .join(&manifest.id)
        .join(&manifest.version);
    if destination.exists() {
        return Err(format!(
            "plugin {} version {} is already installed",
            manifest.id, manifest.version
        ));
    }
    let parent = destination
        .parent()
        .ok_or_else(|| "plugin destination has no parent".to_string())?;
    fs::create_dir_all(parent)
        .map_err(|error| format!("cannot create plugin installation directory: {error}"))?;
    fs::rename(&staged, &destination)
        .map_err(|error| format!("cannot commit plugin package: {error}"))?;
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
    if let Err(error) = store.upsert_plugin_install(&record) {
        let _ = fs::rename(&destination, &staged);
        return Err(error);
    }
    store.remove_prepared_plugin_install(prepared_id)?;
    Ok(record)
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
            stage_git(staging, &url, reference.as_deref()).await
        }
    }
}

async fn stage_npm(
    staging: &Path,
    package: &str,
    requested_version: Option<&str>,
) -> Result<Value, String> {
    let encoded = package.replace('/', "%2f");
    let metadata_url = format!("https://registry.npmjs.org/{encoded}");
    let metadata: Value = reqwest::get(&metadata_url)
        .await
        .map_err(|error| format!("cannot resolve npm package {package}: {error}"))?
        .error_for_status()
        .map_err(|error| format!("cannot resolve npm package {package}: {error}"))?
        .json()
        .await
        .map_err(|error| format!("npm metadata is not valid JSON: {error}"))?;
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
    let integrity = release
        .pointer("/dist/integrity")
        .and_then(Value::as_str)
        .ok_or_else(|| "npm package metadata has no dist.integrity".to_string())?;
    let bytes = reqwest::get(tarball)
        .await
        .map_err(|error| format!("cannot download npm tarball: {error}"))?
        .error_for_status()
        .map_err(|error| format!("cannot download npm tarball: {error}"))?
        .bytes()
        .await
        .map_err(|error| format!("cannot read npm tarball: {error}"))?;
    verify_npm_integrity(&bytes, integrity)?;
    extract_npm_tarball(&bytes, staging)?;
    Ok(
        json!({ "kind": "npm", "package": package, "version": version, "tarball": tarball, "integrity": integrity }),
    )
}

async fn stage_git(staging: &Path, url: &str, reference: Option<&str>) -> Result<Value, String> {
    let reference = reference.unwrap_or("HEAD");
    let output = Command::new("git")
        .args(["ls-remote", url, reference])
        .output()
        .await
        .map_err(|error| format!("cannot resolve Git source: {error}"))?;
    if !output.status.success() {
        return Err("cannot resolve Git reference".to_string());
    }
    let commit = String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .next()
        .filter(|value| value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(|| "Git reference did not resolve to a commit".to_string())?
        .to_string();
    let checkout = staging.join(".checkout");
    let status = Command::new("git")
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "submodule.recurse=false",
            "clone",
            "--no-checkout",
            "--no-recurse-submodules",
            url,
            &checkout.display().to_string(),
        ])
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .await
        .map_err(|error| format!("cannot stage Git source: {error}"))?;
    if !status.status.success() {
        return Err("cannot stage Git source without executing hooks".to_string());
    }
    let status = Command::new("git")
        .current_dir(&checkout)
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "submodule.recurse=false",
            "checkout",
            "--no-recurse-submodules",
            &commit,
        ])
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .await
        .map_err(|error| format!("cannot checkout Git commit: {error}"))?;
    if !status.status.success() {
        return Err("cannot checkout resolved Git commit".to_string());
    }
    copy_tree(&checkout, staging)?;
    fs::remove_dir_all(checkout)
        .map_err(|error| format!("cannot remove Git metadata from staging: {error}"))?;
    Ok(json!({ "kind": "git", "url": url, "reference": reference, "commit": commit }))
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
    if package.is_empty() || version.is_some_and(str::is_empty) {
        return Err("npm source must be npm:<package>[@version]".to_string());
    }
    Ok((package.to_string(), version.map(str::to_string)))
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
        if kind.is_dir() {
            fs::create_dir_all(&target)
                .map_err(|error| format!("cannot create staged directory: {error}"))?;
            copy_tree(&entry.path(), &target)?;
        } else if kind.is_file() {
            fs::copy(entry.path(), target)
                .map_err(|error| format!("cannot stage plugin file: {error}"))?;
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
    let mut archive = Archive::new(GzDecoder::new(bytes));
    for entry in archive
        .entries()
        .map_err(|error| format!("cannot inspect npm tarball: {error}"))?
    {
        let mut entry = entry.map_err(|error| format!("cannot read npm tarball entry: {error}"))?;
        if !entry.header().entry_type().is_file() {
            continue;
        }
        let path = entry
            .path()
            .map_err(|error| format!("npm tarball has invalid path: {error}"))?;
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
        let target = destination.join(relative);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("cannot stage npm directory: {error}"))?;
        }
        let mut output =
            fs::File::create(&target).map_err(|error| format!("cannot stage npm file: {error}"))?;
        std::io::copy(&mut entry, &mut output)
            .map_err(|error| format!("cannot extract npm file: {error}"))?;
    }
    Ok(())
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
    let mut files = BTreeMap::new();
    collect_files(root, root, &mut files)?;
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
) -> Result<(), String> {
    for entry in
        fs::read_dir(current).map_err(|error| format!("cannot read staged plugin: {error}"))?
    {
        let entry = entry.map_err(|error| format!("cannot inspect staged plugin: {error}"))?;
        let kind = entry
            .file_type()
            .map_err(|error| format!("cannot inspect staged plugin entry: {error}"))?;
        if kind.is_symlink() {
            return Err("staged plugin contains a symlink".to_string());
        }
        if kind.is_dir() {
            collect_files(root, &entry.path(), files)?;
        } else if kind.is_file() {
            let relative = entry
                .path()
                .strip_prefix(root)
                .map_err(|_| "cannot resolve staged plugin path".to_string())?
                .to_string_lossy()
                .replace('\\', "/");
            files.insert(
                relative,
                fs::read(entry.path())
                    .map_err(|error| format!("cannot read staged plugin file: {error}"))?,
            );
        } else {
            return Err("staged plugin contains an unsupported entry".to_string());
        }
    }
    Ok(())
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
