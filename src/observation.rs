//! Restricted, noninitializing local configuration and catalog observation.

use std::{
    fs::File,
    io::{Read, Take},
    path::{Path, PathBuf},
    time::Duration,
};

use serde::{Deserialize, Serialize};

use crate::{
    CoreConfig,
    store::{self, ReadOnlyDatabaseStatus},
};

pub const MAX_CONFIGURATION_BYTES: u64 = 1_048_576;
/// Newest schema version supported by this Core.
pub const SUPPORTED_SCHEMA_VERSION: u32 = store::LATEST_SCHEMA_VERSION;
pub const DOCTOR_SHARED_BUDGET: Duration = Duration::from_secs(30);
pub const DOCTOR_LIFECYCLE_STATUS_BUDGET: Duration = Duration::from_secs(10);

/// Lists plugin identity metadata from a supported existing database without migration.
pub fn observe_plugins(data_dir: &Path) -> Result<Vec<(String, String, String)>, String> {
    store::observe_plugins_read_only(&data_dir.join("kakune.sqlite3"))
}

/// Bounded, read-only plugin catalog metadata for an explicitly selected local store.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginCatalogEntry {
    pub id: String,
    pub name: String,
    pub version: String,
    pub enabled: bool,
    pub status: String,
    pub failure_details: Option<String>,
    pub active_executions: u64,
}

/// Lists plugin identities and durable lifecycle state without migrating storage.
pub fn observe_plugin_catalog(data_dir: &Path) -> Result<Vec<PluginCatalogEntry>, String> {
    store::observe_plugin_catalog_read_only(&data_dir.join("kakune.sqlite3"))
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum DoctorArea {
    Configuration,
    Storage,
    Database,
    CredentialStore,
    Core,
    Service,
    Compatibility,
    Platform,
}

impl DoctorArea {
    pub const ALL: [Self; 8] = [
        Self::Configuration,
        Self::Storage,
        Self::Database,
        Self::CredentialStore,
        Self::Core,
        Self::Service,
        Self::Compatibility,
        Self::Platform,
    ];

    pub const fn required(self) -> bool {
        !matches!(self, Self::Core | Self::Service)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DoctorStatus {
    Healthy,
    Warning,
    Failed,
    Unknown,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DoctorFinding {
    pub area: DoctorArea,
    pub status: DoctorStatus,
    pub required: bool,
    pub applicability_reason: String,
    pub evidence_timestamp: String,
    pub message: String,
    pub remediation: String,
}

impl DoctorFinding {
    pub fn new(
        area: DoctorArea,
        status: DoctorStatus,
        applicability_reason: impl Into<String>,
        message: impl Into<String>,
        remediation: impl Into<String>,
    ) -> Self {
        Self {
            area,
            status,
            required: area.required(),
            applicability_reason: applicability_reason.into(),
            evidence_timestamp: evidence_timestamp(),
            message: message.into(),
            remediation: remediation.into(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DoctorReport {
    pub findings: Vec<DoctorFinding>,
    pub exit_code: u8,
}

impl DoctorReport {
    /// Applies the fixed requiredness matrix and returns findings in its
    /// contract order. An incomplete or duplicate report is rejected.
    pub fn from_findings(mut findings: Vec<DoctorFinding>) -> Result<Self, String> {
        if findings.len() != DoctorArea::ALL.len() {
            return Err("doctor report must contain all eight diagnostic areas".to_owned());
        }
        for area in DoctorArea::ALL {
            if findings
                .iter()
                .filter(|finding| finding.area == area)
                .count()
                != 1
            {
                return Err(format!(
                    "doctor report must contain exactly one {area:?} finding"
                ));
            }
        }
        for finding in &findings {
            if finding.required != finding.area.required() {
                return Err(
                    "doctor finding requiredness does not match the fixed matrix".to_owned(),
                );
            }
        }
        findings.sort_by_key(|finding| {
            DoctorArea::ALL
                .iter()
                .position(|area| area == &finding.area)
                .unwrap_or(usize::MAX)
        });
        let exit_code = u8::from(findings.iter().any(|finding| {
            finding.status == DoctorStatus::Failed
                || (finding.required && finding.status == DoctorStatus::Unknown)
        }));
        Ok(Self {
            findings,
            exit_code,
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ObservationStatus {
    Healthy,
    Absent,
    Legacy,
    Incompatible,
    Corrupt,
    Unavailable,
}

#[derive(Clone, Debug)]
pub struct ConfigurationObservation {
    pub path: PathBuf,
    pub status: ObservationStatus,
    pub config: Option<CoreConfig>,
    pub guidance: String,
}

#[derive(Clone, Debug)]
pub struct DatabaseObservation {
    pub path: PathBuf,
    pub status: ObservationStatus,
    pub schema_version: Option<u32>,
    pub workflow_count: Option<u64>,
    pub execution_count: Option<u64>,
    pub guidance: String,
}

/// Trusted adapter for non-mutating database observation.
///
/// Implementations are responsible for preserving the database and its
/// sidecars, and for enforcing their own process, output and time limits.
pub trait DatabaseObservationExecutor {
    fn observe_database(&self, path: &Path) -> DatabaseObservation;
}

/// Uses the built-in observation implementation, which fails closed where a
/// safe local snapshot is not available.
pub struct DefaultDatabaseObservationExecutor;

impl DatabaseObservationExecutor for DefaultDatabaseObservationExecutor {
    fn observe_database(&self, path: &Path) -> DatabaseObservation {
        observe_database(path)
    }
}

/// Executes an explicitly trusted observation helper with bounded output
/// and a five-second process budget. It is never selected implicitly.
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub struct IsolatedDatabaseObservationExecutor {
    executable: PathBuf,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl IsolatedDatabaseObservationExecutor {
    pub fn new(executable: PathBuf) -> Result<Self, String> {
        if !executable.is_absolute() {
            return Err("observation helper path must be absolute".to_owned());
        }
        Ok(Self { executable })
    }

    fn run(&self, path: &Path, mode: Option<&str>) -> Option<Vec<u8>> {
        use crate::lifecycle::{
            Deadline, SystemMonotonicClock, service::native::run_owned_command,
        };
        use std::process::{Command, Stdio};
        let mut command = Command::new(&self.executable);
        if let Some(mode) = mode {
            command.arg(mode);
        }
        command.arg(path).env_clear().stdin(Stdio::null());
        let clock = SystemMonotonicClock::default();
        let output = run_owned_command(
            &mut command,
            Deadline::after(&clock, Duration::from_secs(5)),
            &clock,
        )
        .ok()?;
        if !output.status_success || output.stdout.was_truncated() || output.stderr.was_truncated()
        {
            return None;
        }
        Some(output.stdout.as_bytes().to_vec())
    }

    /// Reads plugin identity metadata through the explicitly trusted helper.
    pub fn observe_plugins(
        &self,
        data_dir: &Path,
    ) -> Result<Vec<(String, String, String)>, String> {
        let bytes = self
            .run(&data_dir.join("kakune.sqlite3"), Some("--plugins"))
            .ok_or_else(|| "isolated plugin catalog observation unavailable".to_owned())?;
        let response: IsolatedPluginResponse = serde_json::from_slice(&bytes)
            .map_err(|_| "isolated plugin catalog observation unavailable".to_owned())?;
        if response.status != ObservationStatus::Healthy
            || response.schema_version != SUPPORTED_SCHEMA_VERSION
            || response.items.len() > 10_000
        {
            return Err("isolated plugin catalog observation unavailable".to_owned());
        }
        let mut total = 0_usize;
        for (id, version, name) in &response.items {
            for item in [id, version, name] {
                if item.len() > 4096 {
                    return Err("plugin metadata exceeds the observation limit".to_owned());
                }
                total = total.saturating_add(item.len());
            }
        }
        if total > 1_048_576 {
            return Err("plugin catalog exceeds the observation limit".to_owned());
        }
        Ok(response.items)
    }

    /// Reads the bounded plugin catalog through the explicitly trusted helper.
    ///
    /// The in-process observer cannot open the database on these platforms
    /// without releasing the Core's own SQLite locks, so the catalog is read
    /// by the separate worker and returned as bounded metadata.
    pub fn observe_plugin_catalog(
        &self,
        data_dir: &Path,
    ) -> Result<Vec<PluginCatalogEntry>, String> {
        let bytes = self
            .run(&data_dir.join("kakune.sqlite3"), Some("--catalog"))
            .ok_or_else(|| "isolated plugin catalog observation unavailable".to_owned())?;
        let response: IsolatedPluginCatalogResponse = serde_json::from_slice(&bytes)
            .map_err(|_| "isolated plugin catalog observation unavailable".to_owned())?;
        if response.status != ObservationStatus::Healthy
            || response.schema_version != SUPPORTED_SCHEMA_VERSION
            || response.items.len() > 10_000
        {
            return Err("isolated plugin catalog observation unavailable".to_owned());
        }
        let mut total = 0_usize;
        for item in &response.items {
            for value in [
                Some(item.id.len()),
                Some(item.name.len()),
                Some(item.version.len()),
                Some(item.status.len()),
                item.failure_details.as_ref().map(|details| details.len()),
            ]
            .into_iter()
            .flatten()
            {
                if value > 4096 {
                    return Err("plugin metadata exceeds the observation limit".to_owned());
                }
                total = total.saturating_add(value);
            }
            if !matches!(
                item.status.as_str(),
                "active" | "pending removal" | "removal failed"
            ) || item
                .failure_details
                .as_ref()
                .is_some_and(|details| details.len() > 256)
            {
                return Err("plugin catalog contains invalid lifecycle metadata".to_owned());
            }
        }
        if total > 1_048_576 {
            return Err("plugin catalog exceeds the observation limit".to_owned());
        }
        Ok(response.items)
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IsolatedPluginResponse {
    status: ObservationStatus,
    schema_version: u32,
    items: Vec<(String, String, String)>,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IsolatedPluginCatalogResponse {
    status: ObservationStatus,
    schema_version: u32,
    items: Vec<PluginCatalogEntry>,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IsolatedObservationResponse {
    status: ObservationStatus,
    schema_version: Option<u32>,
    workflow_count: Option<u64>,
    execution_count: Option<u64>,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl DatabaseObservationExecutor for IsolatedDatabaseObservationExecutor {
    fn observe_database(&self, path: &Path) -> DatabaseObservation {
        let unavailable = || DatabaseObservation {
            path: path.to_path_buf(),
            status: ObservationStatus::Unavailable,
            schema_version: None,
            workflow_count: None,
            execution_count: None,
            guidance:
                "The isolated database observation was unavailable; inspect the helper and retry."
                    .to_owned(),
        };
        match std::fs::symlink_metadata(path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let mut observation = unavailable();
                observation.status = ObservationStatus::Absent;
                observation.guidance =
                    "Initialize Kakune explicitly to create local storage.".to_owned();
                return observation;
            }
            Err(_) => return unavailable(),
            Ok(_) => {}
        }
        let Some(output) = self.run(path, None) else {
            return unavailable();
        };
        let Ok(response) = serde_json::from_slice::<IsolatedObservationResponse>(&output) else {
            return unavailable();
        };
        let valid = match response.status {
            ObservationStatus::Healthy => {
                response.schema_version == Some(SUPPORTED_SCHEMA_VERSION)
                    && response.workflow_count.is_some()
                    && response.execution_count.is_some()
            }
            ObservationStatus::Legacy => response
                .schema_version
                .is_some_and(|v| v < SUPPORTED_SCHEMA_VERSION),
            ObservationStatus::Incompatible => response
                .schema_version
                .is_some_and(|v| v > SUPPORTED_SCHEMA_VERSION),
            ObservationStatus::Corrupt | ObservationStatus::Unavailable => {
                response.schema_version.is_none()
                    && response.workflow_count.is_none()
                    && response.execution_count.is_none()
            }
            ObservationStatus::Absent => false,
        };
        if !valid {
            return unavailable();
        }
        DatabaseObservation {
            path: path.to_path_buf(), status: response.status, schema_version: response.schema_version,
            workflow_count: response.workflow_count, execution_count: response.execution_count,
            guidance: match response.status {
                ObservationStatus::Healthy => "Database schema is supported; observation did not initialize or migrate it.",
                ObservationStatus::Legacy => "The database requires a supported backup-first upgrade before normal Core use.",
                ObservationStatus::Incompatible => "The database schema is newer than this Core supports; use a compatible Core.",
                ObservationStatus::Corrupt => "The database schema is not recognized; preserve it and investigate.",
                ObservationStatus::Absent | ObservationStatus::Unavailable => "The isolated database observation was unavailable; retry when the database is idle.",
            }.to_owned(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct InstallationObservation {
    pub data_dir: PathBuf,
    pub data_directory_exists: bool,
    pub configuration: ConfigurationObservation,
    pub database: DatabaseObservation,
}

#[derive(Clone, Debug)]
pub struct StorageObservation {
    pub path: PathBuf,
    pub status: ObservationStatus,
    pub guidance: String,
}

/// Observes directory presence/readability without creating it or traversing
/// arbitrary descendants.
pub fn observe_storage(data_dir: impl AsRef<Path>) -> StorageObservation {
    let path = data_dir.as_ref().to_path_buf();
    match std::fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            StorageObservation {
                path,
                status: ObservationStatus::Corrupt,
                guidance: "The selected storage path is redirected or is not a directory; preserve it and investigate.".to_owned(),
            }
        }
        Ok(_) => match std::fs::read_dir(&path) {
            Ok(_) => StorageObservation {
                path,
                status: ObservationStatus::Healthy,
                guidance: "The existing storage directory is readable; no entries were changed.".to_owned(),
            },
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => StorageObservation {
                path,
                status: ObservationStatus::Unavailable,
                guidance: "Storage access is denied; review directory permissions without changing the installation.".to_owned(),
            },
            Err(_) => StorageObservation {
                path,
                status: ObservationStatus::Unavailable,
                guidance: "Storage readability could not be established without mutation.".to_owned(),
            },
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => StorageObservation {
            path,
            status: ObservationStatus::Absent,
            guidance: "No storage directory exists; initialize explicitly if this is a new installation.".to_owned(),
        },
        Err(_) => StorageObservation {
            path,
            status: ObservationStatus::Unavailable,
            guidance: "Storage metadata could not be read; review permissions and filesystem health.".to_owned(),
        },
    }
}

fn evidence_timestamp() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "unavailable".to_owned())
}

/// Loads only a pre-existing config file. Missing files and errors never cause
/// directory creation, default publication, credential access, or migration.
pub fn observe_configuration(path: impl AsRef<Path>) -> ConfigurationObservation {
    let path = path.as_ref().to_path_buf();
    let unavailable = |guidance: &str| ConfigurationObservation {
        path: path.clone(),
        status: ObservationStatus::Unavailable,
        config: None,
        guidance: guidance.to_owned(),
    };
    let metadata = match std::fs::metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return ConfigurationObservation {
                path,
                status: ObservationStatus::Absent,
                config: None,
                guidance:
                    "No configuration exists; initialize explicitly if this is a new installation."
                        .to_owned(),
            };
        }
        Err(_) => return unavailable("Configuration access could not be verified."),
    };
    if !metadata.is_file() {
        return ConfigurationObservation {
            path,
            status: ObservationStatus::Corrupt,
            config: None,
            guidance: "The configuration path is not a regular file; preserve it and investigate."
                .to_owned(),
        };
    }
    if metadata.len() > MAX_CONFIGURATION_BYTES {
        return ConfigurationObservation {
            path,
            status: ObservationStatus::Corrupt,
            config: None,
            guidance: "The configuration exceeds the 1 MiB observation limit.".to_owned(),
        };
    }

    let source = match read_capped(File::open(&path), MAX_CONFIGURATION_BYTES) {
        Ok(source) => source,
        Err(_) => {
            return unavailable("Configuration could not be read within the observation limit.");
        }
    };
    let config = match serde_yaml::from_slice::<CoreConfig>(&source) {
        Ok(config) => config,
        Err(_) => {
            return ConfigurationObservation {
                path,
                status: ObservationStatus::Corrupt,
                config: None,
                guidance: "Configuration syntax or schema is invalid; preserve it and review the documented format.".to_owned(),
            };
        }
    };
    if config.validate().is_err() {
        return ConfigurationObservation {
            path,
            status: ObservationStatus::Corrupt,
            config: None,
            guidance:
                "Configuration values are invalid; preserve it and review the documented format."
                    .to_owned(),
        };
    }
    ConfigurationObservation {
        path,
        status: ObservationStatus::Healthy,
        config: Some(config),
        guidance: "Existing configuration was read without creating or replacing files.".to_owned(),
    }
}

fn read_capped<R: Read>(
    reader: Result<R, std::io::Error>,
    limit: u64,
) -> Result<Vec<u8>, std::io::Error> {
    let reader = reader?;
    let mut capped: Take<R> = reader.take(limit.saturating_add(1));
    let mut bytes = Vec::new();
    capped.read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "input exceeds observation limit",
        ));
    }
    Ok(bytes)
}

pub fn observe_database(path: impl AsRef<Path>) -> DatabaseObservation {
    let path = path.as_ref().to_path_buf();
    let observed = store::observe_database_read_only(&path);
    let status = match observed.status {
        ReadOnlyDatabaseStatus::Absent => ObservationStatus::Absent,
        ReadOnlyDatabaseStatus::Current => ObservationStatus::Healthy,
        ReadOnlyDatabaseStatus::Legacy => ObservationStatus::Legacy,
        ReadOnlyDatabaseStatus::Corrupt => ObservationStatus::Corrupt,
        ReadOnlyDatabaseStatus::Newer => ObservationStatus::Incompatible,
        ReadOnlyDatabaseStatus::Unavailable => ObservationStatus::Unavailable,
    };
    DatabaseObservation {
        path,
        status,
        schema_version: observed.schema_version,
        workflow_count: observed.workflow_count,
        execution_count: observed.execution_count,
        guidance: observed.guidance,
    }
}

/// Observes the explicit paths without using any initialization or writable
/// Store APIs. The catalog path is `<data_dir>/kakune.sqlite3`.
pub fn observe_installation(
    data_dir: impl AsRef<Path>,
    configuration_path: Option<&Path>,
) -> InstallationObservation {
    observe_installation_with_executor(
        data_dir,
        configuration_path,
        &DefaultDatabaseObservationExecutor,
    )
}

/// Observes an installation using a caller-supplied trusted database adapter.
pub fn observe_installation_with_executor(
    data_dir: impl AsRef<Path>,
    configuration_path: Option<&Path>,
    executor: &dyn DatabaseObservationExecutor,
) -> InstallationObservation {
    let data_dir = data_dir.as_ref().to_path_buf();
    let configuration_path = configuration_path
        .map(Path::to_path_buf)
        .unwrap_or_else(|| data_dir.join("kakune.yaml"));
    let data_directory_exists =
        std::fs::metadata(&data_dir).is_ok_and(|metadata| metadata.is_dir());
    InstallationObservation {
        configuration: observe_configuration(configuration_path),
        database: executor.observe_database(&data_dir.join("kakune.sqlite3")),
        data_dir,
        data_directory_exists,
    }
}

/// Serializable, non-mutating observation of one Core installation.
///
/// The report carries only status and guidance. It never includes the
/// configuration body, credential references or secret values.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InstallationObservationReport {
    pub data_directory: String,
    pub data_directory_exists: bool,
    pub configuration: ObservedComponent,
    pub storage: ObservedComponent,
    pub database: ObservedDatabase,
}

/// Status of one observed file or directory.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ObservedComponent {
    pub path: String,
    pub status: ObservationStatus,
    pub guidance: String,
}

/// Status of one observed database.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ObservedDatabase {
    pub path: String,
    pub status: ObservationStatus,
    pub schema_version: Option<u32>,
    pub workflow_count: Option<u64>,
    pub execution_count: Option<u64>,
    pub guidance: String,
}

/// Observes an installation for callers that publish the shared read-only
/// observation service, such as the Core API.
pub fn observe_installation_report(
    data_dir: impl AsRef<Path>,
    configuration_path: Option<&Path>,
) -> InstallationObservationReport {
    let data_dir = data_dir.as_ref();
    let installation = observe_installation(data_dir, configuration_path);
    let storage = observe_storage(data_dir);
    InstallationObservationReport {
        data_directory: installation.data_dir.to_string_lossy().into_owned(),
        data_directory_exists: installation.data_directory_exists,
        configuration: ObservedComponent {
            path: installation
                .configuration
                .path
                .to_string_lossy()
                .into_owned(),
            status: installation.configuration.status,
            guidance: installation.configuration.guidance,
        },
        storage: ObservedComponent {
            path: storage.path.to_string_lossy().into_owned(),
            status: storage.status,
            guidance: storage.guidance,
        },
        database: ObservedDatabase {
            path: installation.database.path.to_string_lossy().into_owned(),
            status: installation.database.status,
            schema_version: installation.database.schema_version,
            workflow_count: installation.database.workflow_count,
            execution_count: installation.database.execution_count,
            guidance: installation.database.guidance,
        },
    }
}
