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
pub const DOCTOR_SHARED_BUDGET: Duration = Duration::from_secs(30);
pub const DOCTOR_LIFECYCLE_STATUS_BUDGET: Duration = Duration::from_secs(10);

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
    let data_dir = data_dir.as_ref().to_path_buf();
    let configuration_path = configuration_path
        .map(Path::to_path_buf)
        .unwrap_or_else(|| data_dir.join("kakune.yaml"));
    let data_directory_exists =
        std::fs::metadata(&data_dir).is_ok_and(|metadata| metadata.is_dir());
    InstallationObservation {
        configuration: observe_configuration(configuration_path),
        database: observe_database(data_dir.join("kakune.sqlite3")),
        data_dir,
        data_directory_exists,
    }
}
