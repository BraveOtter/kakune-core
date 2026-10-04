use std::{
    fmt,
    fs::{self, File, OpenOptions},
    io,
    net::SocketAddr,
    path::{Component, Path, PathBuf},
};

use crate::{AuthScope, Store, client_config::ContextFile, config::CoreConfig};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Resolved local paths selected by the CLI for one initialization attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InitializationPaths {
    pub data_dir: PathBuf,
    pub config_file: PathBuf,
    pub context_file: PathBuf,
}

/// The fixed set of independently reported initialization components.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InitializationComponent {
    Configuration,
    Storage,
    Identity,
    ClientAccess,
    LocalContext,
}

/// Readiness of one required initialization component.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ComponentState {
    /// Work for this component has not been attempted.
    NotAttempted,
    /// This initialization created and prepared the component.
    Created,
    /// A valid existing component was reused.
    Reused,
    /// The component is not ready; consult the associated typed error.
    Incomplete,
}

impl ComponentState {
    fn is_ready(self) -> bool {
        matches!(self, Self::Created | Self::Reused)
    }
}

/// Secret-free outcome data for a local initialization attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InitializationReport {
    pub paths: InitializationPaths,
    pub configuration: ComponentState,
    pub storage: ComponentState,
    pub identity: ComponentState,
    pub client_access: ComponentState,
    pub local_context: ComponentState,
}

impl InitializationReport {
    /// Creates a report with every required component still unattempted.
    pub fn new(paths: InitializationPaths) -> Self {
        Self {
            paths,
            configuration: ComponentState::NotAttempted,
            storage: ComponentState::NotAttempted,
            identity: ComponentState::NotAttempted,
            client_access: ComponentState::NotAttempted,
            local_context: ComponentState::NotAttempted,
        }
    }

    /// Returns true only when every required component is ready.
    pub fn is_complete(&self) -> bool {
        self.configuration.is_ready()
            && self.storage.is_ready()
            && self.identity.is_ready()
            && self.client_access.is_ready()
            && self.local_context.is_ready()
    }

    /// Returns the state associated with a fixed initialization component.
    pub fn component_state(&self, component: InitializationComponent) -> ComponentState {
        match component {
            InitializationComponent::Configuration => self.configuration,
            InitializationComponent::Storage => self.storage,
            InitializationComponent::Identity => self.identity,
            InitializationComponent::ClientAccess => self.client_access,
            InitializationComponent::LocalContext => self.local_context,
        }
    }
}

/// Safe category for an initialization failure; it never carries backend text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InitializationFailureCategory {
    InvalidResource,
    ConflictingResource,
    InaccessibleResource,
    UnsupportedStorage,
    StorageUpgradeFailed,
    StorageFailed,
    ClientAccessUnavailable,
    ClientAccessInvalid,
    SecureCredentialWriteFailed,
    CredentialReadbackFailed,
    SetupContended,
}

impl fmt::Display for InitializationFailureCategory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidResource => "invalid resource",
            Self::ConflictingResource => "conflicting resource",
            Self::InaccessibleResource => "inaccessible resource",
            Self::UnsupportedStorage => "unsupported storage format",
            Self::StorageUpgradeFailed => "storage upgrade failed",
            Self::StorageFailed => "storage operation failed",
            Self::ClientAccessUnavailable => "client access unavailable",
            Self::ClientAccessInvalid => "client access is invalid",
            Self::SecureCredentialWriteFailed => "secure credential save failed",
            Self::CredentialReadbackFailed => "credential readback failed",
            Self::SetupContended => "initialization resource is busy",
        })
    }
}

/// Resource affected by an initialization failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InitializationResource {
    Configuration,
    Storage,
    InstallationIdentity,
    ClientAccess,
    LocalContext,
}

impl fmt::Display for InitializationResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Configuration => "configuration",
            Self::Storage => "storage",
            Self::InstallationIdentity => "installation identity",
            Self::ClientAccess => "client access",
            Self::LocalContext => "local context",
        })
    }
}

/// A safe next action for the CLI to render with the selected local paths.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InitializationCorrection {
    InspectResource,
    RepairOrRestoreStorage,
    RunExplicitAuthRecovery,
    RetryAfterContention,
}

impl fmt::Display for InitializationCorrection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InspectResource => "inspect or correct the affected local resource",
            Self::RepairOrRestoreStorage => "repair the storage or restore its pre-upgrade backup",
            Self::RunExplicitAuthRecovery => {
                "run `kakune auth recover` explicitly for the selected local installation"
            }
            Self::RetryAfterContention => {
                "retry initialization after the other setup operation has finished"
            }
        })
    }
}

/// Typed, secret-free initialization failure and its durable partial result.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("initialization failed for {resource}: {category}; {correction}")]
pub struct InitializationError {
    pub report: InitializationReport,
    pub category: InitializationFailureCategory,
    pub resource: InitializationResource,
    pub correction: InitializationCorrection,
}

/// Portable credential reference, not token material; follows existing
/// environment/keyring resolver semantics.
#[derive(Clone, Eq, PartialEq)]
pub struct CredentialReference(String);

impl CredentialReference {
    /// Creates a reference after rejecting an empty reference string.
    pub fn new(reference: impl Into<String>) -> Result<Self, CredentialAccessError> {
        let reference = reference.into();
        if reference.is_empty() {
            return Err(CredentialAccessError::InvalidReference);
        }
        Ok(Self(reference))
    }

    /// Returns the portable reference for metadata and credential resolution.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for CredentialReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CredentialReference([REDACTED])")
    }
}

/// A transient secret value, excluded from result/error/debug/log/JSON output.
/// The value is held in memory only while accessing the secure local credential
/// facility and is zeroized when dropped.
pub struct CredentialSecret(String);

impl CredentialSecret {
    /// Wraps transient credential material for the per-instance access boundary.
    pub fn new(secret: String) -> Self {
        Self(secret)
    }

    /// Exposes the secret only to the credential adapter or local authorizer.
    pub fn expose_secret(&self) -> &str {
        &self.0
    }

    /// Compares two secret values without formatting either one.
    pub fn matches(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Drop for CredentialSecret {
    fn drop(&mut self) {
        use zeroize::Zeroize;

        self.0.zeroize();
    }
}

impl fmt::Debug for CredentialSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CredentialSecret([REDACTED])")
    }
}

/// Safe error category from reference resolution or secure credential access.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CredentialAccessError {
    #[error("credential reference is invalid")]
    InvalidReference,
    #[error("credential is unavailable")]
    Unavailable,
    #[error("secure credential write failed")]
    SecureWriteFailed,
    #[error("secure credential read failed")]
    ReadFailed,
}

/// Per-initialization credential access; implementations are instance-scoped
/// and must not expose secret-bearing backend errors.
pub trait CredentialAccess: Send + Sync {
    /// Resolves a portable reference. `None` retains the `KAKUNE_TOKEN`
    /// environment fallback used by existing local-context resolution.
    fn resolve_reference(
        &self,
        reference: Option<&str>,
    ) -> Result<CredentialReference, CredentialAccessError>;

    /// Stores a secret in the secure local credential facility.
    fn write_secure(
        &self,
        reference: &CredentialReference,
        secret: &CredentialSecret,
    ) -> Result<(), CredentialAccessError>;

    /// Reads a secret from the selected credential source.
    fn read(
        &self,
        reference: &CredentialReference,
    ) -> Result<CredentialSecret, CredentialAccessError>;
}

struct InitializationLocks {
    _files: Vec<File>,
}

impl InitializationLocks {
    fn acquire(paths: &InitializationPaths) -> Result<Self, InitializationError> {
        let report = InitializationReport::new(paths.clone());
        let mut resources = vec![
            (
                paths.data_dir.join("kakune.sqlite3"),
                InitializationResource::Storage,
            ),
            (
                paths.config_file.clone(),
                InitializationResource::Configuration,
            ),
            (
                paths.context_file.clone(),
                InitializationResource::LocalContext,
            ),
        ];

        let mut normalized = Vec::with_capacity(resources.len());
        for (path, resource) in resources.drain(..) {
            let key = normalize_resource_path(&path).map_err(|_| InitializationError {
                report: report.clone(),
                category: InitializationFailureCategory::InaccessibleResource,
                resource,
                correction: InitializationCorrection::InspectResource,
            })?;
            if !normalized.iter().any(|(existing, _)| existing == &key) {
                normalized.push((key, resource));
            }
        }
        normalized.sort_by(|left, right| left.0.cmp(&right.0));

        let lock_dir = std::env::temp_dir()
            .join("opencode")
            .join("kakune-setup-locks");
        let mut directory = fs::DirBuilder::new();
        directory.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            directory.mode(0o700);
        }
        directory
            .create(&lock_dir)
            .or_else(|error| {
                if error.kind() == io::ErrorKind::AlreadyExists {
                    Ok(())
                } else {
                    Err(error)
                }
            })
            .map_err(|_| InitializationError {
                report: report.clone(),
                category: InitializationFailureCategory::InaccessibleResource,
                resource: InitializationResource::Storage,
                correction: InitializationCorrection::InspectResource,
            })?;

        let mut files = Vec::with_capacity(normalized.len());
        for (key, resource) in normalized {
            let digest = Sha256::digest(key.as_bytes());
            let lock_name = digest
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            let lock_path = lock_dir.join(format!("{lock_name}.lock"));
            let file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(lock_path)
                .map_err(|_| InitializationError {
                    report: report.clone(),
                    category: InitializationFailureCategory::InaccessibleResource,
                    resource,
                    correction: InitializationCorrection::InspectResource,
                })?;
            match file.try_lock() {
                Ok(()) => files.push(file),
                Err(std::fs::TryLockError::WouldBlock) => {
                    return Err(InitializationError {
                        report,
                        category: InitializationFailureCategory::SetupContended,
                        resource,
                        correction: InitializationCorrection::RetryAfterContention,
                    });
                }
                Err(_) => {
                    return Err(InitializationError {
                        report,
                        category: InitializationFailureCategory::InaccessibleResource,
                        resource,
                        correction: InitializationCorrection::InspectResource,
                    });
                }
            }
        }
        Ok(Self { _files: files })
    }
}

fn normalize_resource_path(path: &Path) -> io::Result<String> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };

    let mut unresolved = Vec::new();
    let mut existing = absolute.as_path();
    while !existing.exists() {
        let Some(name) = existing.file_name() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "resource path has no existing ancestor",
            ));
        };
        unresolved.push(name.to_os_string());
        existing = existing.parent().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "resource path has no parent")
        })?;
    }

    let mut normalized = existing.canonicalize()?;
    for component in unresolved.into_iter().rev() {
        normalized.push(component);
    }
    let normalized = lexical_normalize(&normalized);
    let key = normalized.to_string_lossy().into_owned();
    #[cfg(windows)]
    let key = key.to_lowercase();
    Ok(key)
}

fn lexical_normalize(path: &Path) -> PathBuf {
    let mut result = PathBuf::new();
    let mut parents = 0usize;
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !result.pop() {
                    parents += 1;
                }
            }
            other => result.push(other.as_os_str()),
        }
    }
    for _ in 0..parents {
        result.push("..");
    }
    result
}

pub(crate) fn publish_new_file(path: &Path, contents: &[u8]) -> io::Result<()> {
    publish_file(path, contents, false)
}

pub(crate) fn replace_file(path: &Path, contents: &[u8]) -> io::Result<()> {
    publish_file(path, contents, true)
}

fn publish_file(path: &Path, contents: &[u8], replace: bool) -> io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "resource path has no parent")
    })?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(".kakune-{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        use std::io::Write;
        file.write_all(contents)?;
        file.sync_all()?;
        drop(file);

        if replace {
            fs::rename(&temporary, path)
        } else {
            fs::hard_link(&temporary, path)
        }
    })();
    let _ = fs::remove_file(&temporary);
    result
}

/// Prepares one local installation without starting the Core or contacting a
/// remote service.
pub fn initialize(
    paths: InitializationPaths,
    credentials: &dyn CredentialAccess,
) -> Result<InitializationReport, InitializationError> {
    let mut report = InitializationReport::new(paths);

    let _locks = InitializationLocks::acquire(&report.paths)?;
    let (loaded, config_created) =
        match CoreConfig::load_or_create_for_initialization(&report.paths.config_file) {
            Ok(loaded) => loaded,
            Err(_) => {
                report.configuration = ComponentState::Incomplete;
                let category = if report.paths.config_file.exists() {
                    InitializationFailureCategory::InvalidResource
                } else {
                    InitializationFailureCategory::InaccessibleResource
                };
                return Err(failure(
                    report,
                    category,
                    InitializationResource::Configuration,
                    InitializationCorrection::InspectResource,
                ));
            }
        };
    report.configuration = if config_created {
        ComponentState::Created
    } else {
        ComponentState::Reused
    };

    let (store, new_installation) =
        match Store::open_for_initialization(report.paths.data_dir.clone()) {
            Ok(opened) => opened,
            Err(error) => {
                report.storage = ComponentState::Incomplete;
                let category = if error.contains("supported Kakune schema")
                    || error.contains("core identity")
                    || error.contains("empty or is not a regular file")
                    || error.contains("corrupt")
                    || error.contains("newer than this Core supports")
                {
                    InitializationFailureCategory::UnsupportedStorage
                } else if error.contains("backup") || error.contains("migration") {
                    InitializationFailureCategory::StorageUpgradeFailed
                } else if error.contains("cannot create Kakune data directory")
                    || error.contains("cannot create Kakune database")
                    || error.contains("cannot inspect Kakune database")
                    || error.contains("cannot open Kakune database")
                    || error.contains("cannot configure Kakune database")
                {
                    InitializationFailureCategory::InaccessibleResource
                } else {
                    InitializationFailureCategory::StorageFailed
                };
                let correction = if matches!(
                    category,
                    InitializationFailureCategory::StorageUpgradeFailed
                ) {
                    InitializationCorrection::RepairOrRestoreStorage
                } else {
                    InitializationCorrection::InspectResource
                };
                return Err(failure(
                    report,
                    category,
                    InitializationResource::Storage,
                    correction,
                ));
            }
        };
    report.storage = if new_installation {
        ComponentState::Created
    } else {
        ComponentState::Reused
    };
    report.identity = report.storage;

    let core_id = match store.core_id() {
        Ok(core_id) => core_id,
        Err(_) => {
            report.identity = ComponentState::Incomplete;
            return Err(failure(
                report,
                InitializationFailureCategory::StorageFailed,
                InitializationResource::InstallationIdentity,
                InitializationCorrection::RepairOrRestoreStorage,
            ));
        }
    };

    let contexts = match ContextFile::load(&report.paths.context_file) {
        Ok(contexts) => contexts,
        Err(_) => {
            report.local_context = ComponentState::Incomplete;
            return Err(failure(
                report,
                InitializationFailureCategory::InvalidResource,
                InitializationResource::LocalContext,
                InitializationCorrection::InspectResource,
            ));
        }
    };
    let local_context = contexts
        .contexts
        .iter()
        .find(|context| context.id == "local");
    let has_local_context = local_context.is_some();
    if local_context.is_some_and(|context| {
        context
            .expected_core_id
            .as_deref()
            .is_some_and(|expected| expected != core_id)
    }) {
        report.local_context = ComponentState::Incomplete;
        return Err(failure(
            report,
            InitializationFailureCategory::ConflictingResource,
            InitializationResource::LocalContext,
            InitializationCorrection::InspectResource,
        ));
    }

    let canonical_reference = format!("keychain:kakune/core/{core_id}");
    let reference_value = match (new_installation, local_context) {
        (true, Some(context)) => context.credential_ref.as_deref(),
        (false, Some(context)) => context.credential_ref.as_deref(),
        (_, None) => Some(canonical_reference.as_str()),
    };
    let reference = match credentials.resolve_reference(reference_value) {
        Ok(reference) => reference,
        Err(error) => {
            mark_access_failure(&mut report, has_local_context);
            return Err(credential_error(report, error, false));
        }
    };

    let access_secret = if new_installation {
        let token = match store.issue_initial_admin_credential() {
            Ok(token) => CredentialSecret::new(token),
            Err(_) => {
                mark_access_failure(&mut report, has_local_context);
                return Err(failure(
                    report,
                    InitializationFailureCategory::ClientAccessInvalid,
                    InitializationResource::ClientAccess,
                    InitializationCorrection::RunExplicitAuthRecovery,
                ));
            }
        };
        if credentials.write_secure(&reference, &token).is_err() {
            mark_access_failure(&mut report, has_local_context);
            return Err(failure(
                report,
                InitializationFailureCategory::SecureCredentialWriteFailed,
                InitializationResource::ClientAccess,
                InitializationCorrection::RunExplicitAuthRecovery,
            ));
        }
        let readback = match credentials.read(&reference) {
            Ok(readback) => readback,
            Err(error) => {
                mark_access_failure(&mut report, has_local_context);
                return Err(credential_error(report, error, true));
            }
        };
        if !token.matches(&readback) {
            mark_access_failure(&mut report, has_local_context);
            return Err(failure(
                report,
                InitializationFailureCategory::CredentialReadbackFailed,
                InitializationResource::ClientAccess,
                InitializationCorrection::RunExplicitAuthRecovery,
            ));
        }
        readback
    } else {
        match credentials.read(&reference) {
            Ok(secret) => secret,
            Err(error) => {
                mark_access_failure(&mut report, has_local_context);
                return Err(credential_error(report, error, false));
            }
        }
    };

    let authorized = store
        .authorize_scope(access_secret.expose_secret(), AuthScope::Admin)
        .unwrap_or_default();
    if !authorized {
        mark_access_failure(&mut report, has_local_context);
        return Err(failure(
            report,
            InitializationFailureCategory::ClientAccessInvalid,
            InitializationResource::ClientAccess,
            InitializationCorrection::RunExplicitAuthRecovery,
        ));
    }
    report.client_access = if new_installation {
        ComponentState::Created
    } else {
        ComponentState::Reused
    };

    let listen = match loaded.config.listen_addr() {
        Ok(listen) => listen,
        Err(_) => {
            report.configuration = ComponentState::Incomplete;
            return Err(failure(
                report,
                InitializationFailureCategory::InvalidResource,
                InitializationResource::Configuration,
                InitializationCorrection::InspectResource,
            ));
        }
    };
    let endpoint = local_endpoint(&loaded.config.api, listen);
    match ContextFile::ensure_local_context(
        &report.paths.context_file,
        &core_id,
        &endpoint,
        reference.as_str(),
    ) {
        Ok(true) => report.local_context = ComponentState::Created,
        Ok(false) => report.local_context = ComponentState::Reused,
        Err(error) => {
            report.local_context = ComponentState::Incomplete;
            let category = if error.contains("different installation") {
                InitializationFailureCategory::ConflictingResource
            } else if error.contains("publish") || error.contains("write") {
                InitializationFailureCategory::InaccessibleResource
            } else {
                InitializationFailureCategory::InvalidResource
            };
            return Err(failure(
                report,
                category,
                InitializationResource::LocalContext,
                InitializationCorrection::InspectResource,
            ));
        }
    }

    if report.is_complete() {
        Ok(report)
    } else {
        Err(failure(
            report,
            InitializationFailureCategory::StorageFailed,
            InitializationResource::Storage,
            InitializationCorrection::InspectResource,
        ))
    }
}

fn mark_access_failure(report: &mut InitializationReport, has_local_context: bool) {
    report.client_access = ComponentState::Incomplete;
    if has_local_context {
        report.local_context = ComponentState::Incomplete;
    }
}

fn credential_error(
    report: InitializationReport,
    error: CredentialAccessError,
    readback: bool,
) -> InitializationError {
    let category = match error {
        CredentialAccessError::InvalidReference => {
            InitializationFailureCategory::ClientAccessInvalid
        }
        CredentialAccessError::SecureWriteFailed => {
            InitializationFailureCategory::SecureCredentialWriteFailed
        }
        CredentialAccessError::Unavailable | CredentialAccessError::ReadFailed if readback => {
            InitializationFailureCategory::CredentialReadbackFailed
        }
        CredentialAccessError::Unavailable | CredentialAccessError::ReadFailed => {
            InitializationFailureCategory::ClientAccessUnavailable
        }
    };
    let correction = if matches!(
        category,
        InitializationFailureCategory::ClientAccessUnavailable
            | InitializationFailureCategory::ClientAccessInvalid
            | InitializationFailureCategory::SecureCredentialWriteFailed
            | InitializationFailureCategory::CredentialReadbackFailed
    ) {
        InitializationCorrection::RunExplicitAuthRecovery
    } else {
        InitializationCorrection::InspectResource
    };
    failure(
        report,
        category,
        InitializationResource::ClientAccess,
        correction,
    )
}

fn failure(
    report: InitializationReport,
    category: InitializationFailureCategory,
    resource: InitializationResource,
    correction: InitializationCorrection,
) -> InitializationError {
    InitializationError {
        report,
        category,
        resource,
        correction,
    }
}

fn local_endpoint(api: &crate::config::ApiConfig, listen: SocketAddr) -> String {
    let listen = if listen.ip().is_unspecified() {
        let loopback = if listen.is_ipv4() { "127.0.0.1" } else { "::1" };
        SocketAddr::new(
            loopback.parse().expect("valid loopback address"),
            listen.port(),
        )
    } else {
        listen
    };
    let scheme = if api.tls_cert.is_some() {
        "https"
    } else {
        "http"
    };
    format!("{scheme}://{listen}")
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use super::{
        ComponentState, InitializationComponent, InitializationPaths, InitializationReport,
        publish_new_file, replace_file,
    };

    fn paths() -> InitializationPaths {
        InitializationPaths {
            data_dir: PathBuf::from("data"),
            config_file: PathBuf::from("config.yaml"),
            context_file: PathBuf::from("contexts.json"),
        }
    }

    #[test]
    fn report_starts_with_every_component_unattempted() {
        let report = InitializationReport::new(paths());

        for component in [
            InitializationComponent::Configuration,
            InitializationComponent::Storage,
            InitializationComponent::Identity,
            InitializationComponent::ClientAccess,
            InitializationComponent::LocalContext,
        ] {
            assert_eq!(
                report.component_state(component),
                ComponentState::NotAttempted
            );
        }
        assert!(!report.is_complete());
    }

    #[test]
    fn report_is_complete_only_when_all_components_are_ready() {
        let mut report = InitializationReport::new(paths());
        report.configuration = ComponentState::Created;
        report.storage = ComponentState::Reused;
        report.identity = ComponentState::Reused;
        report.client_access = ComponentState::Created;
        report.local_context = ComponentState::Reused;
        assert!(report.is_complete());

        report.local_context = ComponentState::Incomplete;
        assert!(!report.is_complete());
        report.local_context = ComponentState::NotAttempted;
        assert!(!report.is_complete());
    }

    #[test]
    fn failed_atomic_publications_preserve_destinations_and_clean_owned_temporary_files() {
        let parent = std::env::temp_dir()
            .join("opencode")
            .join(format!("kakune-publication-test-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&parent).expect("isolated publication fixture should be created");

        let existing_file = parent.join("existing.json");
        fs::write(&existing_file, b"preserve original bytes")
            .expect("existing destination should be written");
        assert!(publish_new_file(&existing_file, b"replacement").is_err());
        assert_eq!(
            fs::read(&existing_file).unwrap(),
            b"preserve original bytes",
            "no-replace publication must not overwrite an existing destination"
        );

        let existing_directory = parent.join("existing-directory");
        fs::create_dir(&existing_directory).expect("directory destination should be created");
        fs::write(existing_directory.join("marker.txt"), b"preserve directory")
            .expect("directory marker should be written");
        assert!(replace_file(&existing_directory, b"replacement").is_err());
        assert_eq!(
            fs::read(existing_directory.join("marker.txt")).unwrap(),
            b"preserve directory",
            "failed replacement must leave the existing directory intact"
        );

        let temporary_remains =
            fs::read_dir(&parent)
                .unwrap()
                .filter_map(Result::ok)
                .any(|entry| {
                    let name = entry.file_name().to_string_lossy().into_owned();
                    name.starts_with(".kakune-") && name.ends_with(".tmp")
                });
        assert!(
            !temporary_remains,
            "failed publication must remove only its owned temporary files"
        );
        fs::remove_dir_all(parent).expect("isolated publication fixture should be removed");
    }
}
