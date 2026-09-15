use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use time::{Duration, OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::{
    ArtifactRef, WorkflowDocument, artifacts,
    workflow::{ConcurrencyPolicy, OverflowPolicy},
};

#[derive(Clone)]
pub struct Store {
    connection: Arc<Mutex<Connection>>,
    data_dir: Arc<PathBuf>,
}

#[derive(Clone, Debug)]
pub struct RetentionPolicy {
    pub execution_days: i64,
    pub event_days: i64,
    pub artifact_limit_bytes: u64,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            execution_days: 90,
            event_days: 90,
            artifact_limit_bytes: 5 * 1024 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RetentionReport {
    pub executions_deleted: u64,
    pub events_deleted: u64,
    pub artifacts_deleted: u64,
    pub artifact_bytes_deleted: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupSummary {
    pub path: String,
    pub workflows: u64,
    pub executions: u64,
    pub artifacts: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationalMetrics {
    pub queued_executions: u64,
    pub active_executions: u64,
    pub retained_events: u64,
    pub artifacts: u64,
    pub artifact_bytes: u64,
    pub database_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SecretBackend {
    Native,
    Vault,
    NativeWithVaultBackup,
}

impl SecretBackend {
    fn current() -> Result<Self, String> {
        match std::env::var("KAKUNE_SECRET_STORE").as_deref() {
            Ok("native") | Err(std::env::VarError::NotPresent) => Ok(Self::Native),
            Ok("vault") => Ok(Self::Vault),
            Ok("native-with-vault-backup") => Ok(Self::NativeWithVaultBackup),
            Ok(value) => Err(format!(
                "KAKUNE_SECRET_STORE must be native, vault, or native-with-vault-backup; got {value}"
            )),
            Err(error) => Err(format!("cannot read KAKUNE_SECRET_STORE: {error}")),
        }
    }

    fn database_value(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Vault => "vault",
            Self::NativeWithVaultBackup => "native-with-vault-backup",
        }
    }
}

pub struct ProviderInvocation<'a> {
    pub execution_id: &'a str,
    pub node_id: &'a str,
    pub provider: &'a str,
    pub model_requested: &'a str,
    pub model_reported: Option<&'a str>,
    pub usage: Option<&'a Value>,
    pub raw: &'a Value,
    pub span_id: Option<&'a str>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceSpanRecord {
    pub id: String,
    pub parent_span_id: Option<String>,
    pub kind: String,
    pub node_id: Option<String>,
    pub name: String,
    pub attempt: Option<u32>,
    pub status: String,
    pub started_at: String,
    pub completed_at: Option<String>,
    pub error: Option<String>,
    pub attributes: Value,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderInvocationRecord {
    pub span_id: Option<String>,
    pub node_id: String,
    pub provider: String,
    pub model_requested: String,
    pub model_reported: Option<String>,
    pub usage: Option<Value>,
    pub usage_certainty: String,
    pub cost_certainty: String,
    pub created_at: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderUsageSummary {
    pub provider: String,
    pub model: String,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
    pub usage_certainty: String,
    pub cost_certainty: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceArtifactLink {
    pub node_id: String,
    pub artifact_id: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionTraceRecord {
    pub execution: ExecutionRecord,
    pub workflow_revision: String,
    pub workflow_source: String,
    pub plan_hash: String,
    pub spans: Vec<TraceSpanRecord>,
    pub provider_invocations: Vec<ProviderInvocationRecord>,
    pub provider_usage: Vec<ProviderUsageSummary>,
    pub artifacts: Vec<TraceArtifactLink>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowRevisionRecord {
    pub id: String,
    pub created_at: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowRevisionSource {
    pub id: String,
    pub created_at: String,
    pub source: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowRevisionComparison {
    pub base: WorkflowRevisionSource,
    pub head: WorkflowRevisionSource,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowRecord {
    pub id: String,
    pub name: String,
    pub status: String,
    pub revision: String,
    pub updated_at: String,
}

#[derive(Clone, Debug)]
pub struct WorkflowSourceRecord {
    pub source: String,
    pub revision: String,
}

/// A compare-and-swap result for workflow source updates.
pub enum WorkflowSourceUpdate {
    Updated(WorkflowRecord),
    NotFound,
    Conflict { current_revision: String },
}

#[derive(Clone, Debug)]
pub(crate) struct ScheduledTriggerRecord {
    pub next_run_at: OffsetDateTime,
    pub completed_at: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionRecord {
    pub id: String,
    pub workflow_name: String,
    pub status: String,
    pub created_at: String,
    pub completed_at: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeRunRecord {
    pub node_id: String,
    pub status: String,
    pub message: Option<String>,
    pub result: Option<Value>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EventRecord {
    pub event_version: String,
    pub core_id: String,
    pub event_id: String,
    pub sequence: u64,
    pub timestamp: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub resource_id: String,
    pub execution_id: Option<String>,
    pub payload: Value,
}

/// API permissions are intentionally small and composable. `admin` includes every scope.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AuthScope {
    Read,
    Run,
    Manage,
    Admin,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthTokenRecord {
    pub id: String,
    pub name: String,
    pub scopes: Vec<AuthScope>,
    pub created_at: String,
    pub expires_at: Option<String>,
    pub revoked_at: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreatedAuthToken {
    pub token: String,
    #[serde(flatten)]
    pub record: AuthTokenRecord,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretRecord {
    pub name: String,
    pub updated_at: String,
}

/// The supported provider implementations in the profile catalog.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ProviderType {
    MiniMax,
    Codex,
}

/// Authentication configuration retained by a provider profile.
///
/// This deliberately contains a secret *reference*, not the secret itself.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ProviderAuth {
    #[serde(rename = "apiKey")]
    ApiKeySecret { secret_ref: String },
    #[serde(rename = "oauthSecret")]
    OAuthSecret { secret_ref: String },
}

/// The availability reported by the most recent provider diagnosis.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ProviderProfileStatus {
    Unknown,
    Available,
    Unavailable,
}

/// Non-secret result of checking a provider profile.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProviderProfileDiagnostic {
    pub status: ProviderProfileStatus,
    pub checked_at: Option<String>,
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

/// The mutable, non-secret configuration used to create or update a provider profile.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProviderProfileUpsert {
    pub id: String,
    pub display_name: String,
    pub provider_type: ProviderType,
    pub default_model: String,
    pub allowed_models: Vec<String>,
    pub capabilities: Vec<String>,
    pub auth: ProviderAuth,
    pub config: Value,
}

/// A persisted provider profile. It never includes credentials or CLI session data.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProviderProfile {
    pub id: String,
    pub display_name: String,
    pub provider_type: ProviderType,
    pub default_model: String,
    pub allowed_models: Vec<String>,
    pub capabilities: Vec<String>,
    pub auth: ProviderAuth,
    pub config: Value,
    pub diagnostic: ProviderProfileDiagnostic,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InstalledPluginRecord {
    pub id: String,
    pub name: String,
    pub version: String,
    pub manifest_path: String,
    pub installed_at: String,
    pub enabled: bool,
    pub policy: Value,
    pub digest: String,
    pub resolved_lock: Value,
    pub provenance: Value,
}

/// A staged plugin inspection. Its content is never activated until its digest
/// is confirmed by a separate commit operation.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PreparedPluginInstallRecord {
    pub id: String,
    pub plugin_id: String,
    pub plugin_name: String,
    pub version: String,
    pub digest: String,
    pub staged_path: String,
    pub prepared_at: String,
    pub policy: Value,
    pub resolved_lock: Value,
    pub provenance: Value,
}

impl Store {
    pub fn open(data_dir: PathBuf) -> Result<Self, String> {
        fs::create_dir_all(&data_dir)
            .map_err(|error| format!("cannot create Kakune data directory: {error}"))?;
        let database_path = data_dir.join("kakune.sqlite3");
        let database_existed = database_path.exists()
            && fs::metadata(&database_path)
                .map_err(|error| format!("cannot inspect Kakune database: {error}"))?
                .len()
                > 0;
        let mut connection = Connection::open(&database_path)
            .map_err(|error| format!("cannot open Kakune database: {error}"))?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(|error| format!("cannot configure Kakune database: {error}"))?;
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .map_err(|error| format!("cannot configure Kakune database: {error}"))?;
        migrate(&mut connection, &data_dir, database_existed)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            data_dir: Arc::new(data_dir),
        })
    }

    pub fn data_dir(&self) -> &Path {
        self.data_dir.as_ref().as_path()
    }

    pub fn workspace_dir(&self) -> Result<PathBuf, String> {
        let path = self.data_dir().join("workspace");
        fs::create_dir_all(&path).map_err(|error| format!("cannot create workspace: {error}"))?;
        Ok(path)
    }

    /// Creates a consistent, portable data backup. The SQLite backup API
    /// produces a checkpointed database while writers remain serialized. The
    /// backup intentionally removes secret metadata and ciphertext: native
    /// credentials are machine/user-bound and vault recovery is a separate,
    /// explicitly documented operation.
    pub fn backup_to(&self, destination: &Path) -> Result<BackupSummary, String> {
        if destination.exists() {
            return Err(format!(
                "backup destination {} already exists",
                destination.display()
            ));
        }
        let parent = destination
            .parent()
            .ok_or_else(|| "backup destination has no parent directory".to_string())?;
        fs::create_dir_all(parent)
            .map_err(|error| format!("cannot create backup directory: {error}"))?;
        let snapshot = self
            .data_dir()
            .join(format!(".backup-{}.sqlite3", Uuid::new_v4()));
        let counts = self.with_connection(|connection| {
            let mut copy = Connection::open(&snapshot)
                .map_err(|error| format!("cannot create backup snapshot: {error}"))?;
            let backup =
                rusqlite::backup::Backup::new(connection, &mut copy).map_err(database_error)?;
            backup
                .run_to_completion(128, std::time::Duration::from_millis(5), None)
                .map_err(database_error)?;
            drop(backup);
            copy.execute("DELETE FROM secrets", [])
                .map_err(database_error)?;
            let workflows = count_rows(&copy, "workflows")?;
            let executions = count_rows(&copy, "executions")?;
            let artifacts = count_rows(&copy, "artifacts")?;
            Ok((workflows, executions, artifacts))
        })?;

        let result: Result<(), String> = (|| {
            let file = fs::File::create(destination).map_err(|error| {
                format!("cannot create backup {}: {error}", destination.display())
            })?;
            let encoder = GzEncoder::new(file, Compression::default());
            let mut archive = tar::Builder::new(encoder);
            archive
                .append_path_with_name(&snapshot, "kakune.sqlite3")
                .map_err(|error| format!("cannot add database to backup: {error}"))?;
            for name in ["artifacts", "workspace"] {
                let source = self.data_dir().join(name);
                if source.exists() {
                    archive
                        .append_dir_all(name, source)
                        .map_err(|error| format!("cannot add {name} to backup: {error}"))?;
                }
            }
            let config = self.data_dir().join("kakune.yaml");
            if config.is_file() {
                archive
                    .append_path_with_name(&config, "kakune.yaml")
                    .map_err(|error| format!("cannot add configuration to backup: {error}"))?;
            }
            let manifest = serde_json::json!({
                "format": "kakune-backup/v1",
                "createdAt": now()?,
                "secretsIncluded": false,
                "workflows": counts.0,
                "executions": counts.1,
                "artifacts": counts.2,
            });
            let bytes = serde_json::to_vec_pretty(&manifest)
                .map_err(|error| format!("cannot serialize backup manifest: {error}"))?;
            let mut header = tar::Header::new_gnu();
            header.set_size(bytes.len() as u64);
            header.set_mode(0o600);
            header.set_cksum();
            archive
                .append_data(&mut header, "manifest.json", bytes.as_slice())
                .map_err(|error| format!("cannot add backup manifest: {error}"))?;
            archive
                .finish()
                .map_err(|error| format!("cannot finalize backup: {error}"))?;
            Ok(())
        })();
        let _ = fs::remove_file(&snapshot);
        result?;
        Ok(BackupSummary {
            path: destination.display().to_string(),
            workflows: counts.0,
            executions: counts.1,
            artifacts: counts.2,
        })
    }

    /// Restores a backup only into an empty directory. Archive entry paths are
    /// validated before extraction to prevent a backup from writing outside its
    /// selected destination.
    pub fn restore_backup(source: &Path, destination: &Path) -> Result<BackupSummary, String> {
        if !source.is_file() {
            return Err(format!("backup {} does not exist", source.display()));
        }
        if destination.exists()
            && fs::read_dir(destination)
                .map_err(|error| format!("cannot inspect restore destination: {error}"))?
                .next()
                .is_some()
        {
            return Err("restore destination must be empty".to_string());
        }
        fs::create_dir_all(destination)
            .map_err(|error| format!("cannot create restore destination: {error}"))?;
        let file = fs::File::open(source)
            .map_err(|error| format!("cannot open backup {}: {error}", source.display()))?;
        let decoder = GzDecoder::new(file);
        let mut archive = tar::Archive::new(decoder);
        let mut saw_database = false;
        for entry in archive
            .entries()
            .map_err(|error| format!("cannot read backup: {error}"))?
        {
            let mut entry = entry.map_err(|error| format!("cannot read backup entry: {error}"))?;
            let path = entry
                .path()
                .map_err(|error| format!("cannot read backup entry path: {error}"))?;
            let path = PathBuf::from(path.as_ref());
            if path.is_absolute()
                || path
                    .components()
                    .any(|component| !matches!(component, std::path::Component::Normal(_)))
            {
                return Err("backup contains an unsafe path".to_string());
            }
            if !matches!(
                path.components()
                    .next()
                    .map(|component| component.as_os_str().to_str()),
                Some(Some(
                    "kakune.sqlite3" | "artifacts" | "workspace" | "kakune.yaml" | "manifest.json"
                ))
            ) {
                return Err(format!(
                    "backup contains unsupported entry {}",
                    path.display()
                ));
            }
            if path == Path::new("kakune.sqlite3") {
                saw_database = true;
            }
            let output = destination.join(path);
            if entry.header().entry_type().is_dir() {
                fs::create_dir_all(&output)
                    .map_err(|error| format!("cannot create restored directory: {error}"))?;
            } else if entry.header().entry_type().is_file() {
                let parent = output
                    .parent()
                    .ok_or_else(|| "backup entry has no parent".to_string())?;
                fs::create_dir_all(parent)
                    .map_err(|error| format!("cannot create restored directory: {error}"))?;
                let mut target = fs::File::create(&output)
                    .map_err(|error| format!("cannot restore {}: {error}", output.display()))?;
                io::copy(&mut entry, &mut target)
                    .map_err(|error| format!("cannot restore {}: {error}", output.display()))?;
            } else {
                return Err("backup contains an unsupported link or special file".to_string());
            }
        }
        if !saw_database {
            return Err("backup does not contain kakune.sqlite3".to_string());
        }
        let restored = Store::open(destination.to_path_buf())?;
        let summary = restored.backup_summary()?;
        drop(restored);
        Ok(BackupSummary {
            path: destination.display().to_string(),
            ..summary
        })
    }

    pub fn compact(&self) -> Result<(), String> {
        self.with_connection(|connection| {
            connection
                .execute_batch("PRAGMA wal_checkpoint(TRUNCATE); VACUUM;")
                .map_err(database_error)
        })
    }

    pub fn apply_retention(&self, policy: &RetentionPolicy) -> Result<RetentionReport, String> {
        if policy.execution_days < 1 || policy.event_days < 1 || policy.artifact_limit_bytes == 0 {
            return Err("retention days and artifact limit must be positive".to_string());
        }
        let execution_before = (OffsetDateTime::now_utc() - Duration::days(policy.execution_days))
            .format(&Rfc3339)
            .map_err(|error| error.to_string())?;
        let event_before = (OffsetDateTime::now_utc() - Duration::days(policy.event_days))
            .format(&Rfc3339)
            .map_err(|error| error.to_string())?;
        let mut report = self.with_connection(|connection| {
            let transaction = connection.transaction().map_err(database_error)?;
            let executions_deleted = transaction.execute(
                "DELETE FROM executions WHERE completed_at IS NOT NULL AND completed_at < ?1 AND pinned = 0",
                [&execution_before],
            ).map_err(database_error)? as u64;
            let events_deleted = transaction.execute("DELETE FROM events WHERE timestamp < ?1", [&event_before])
                .map_err(database_error)? as u64;
            transaction.commit().map_err(database_error)?;
            Ok(RetentionReport { executions_deleted, events_deleted, ..RetentionReport::default() })
        })?;
        let artifacts = self.unreferenced_artifacts_oldest()?;
        let mut total = self.artifact_bytes()?;
        for artifact in artifacts {
            if total <= policy.artifact_limit_bytes {
                break;
            }
            let path = artifacts::artifact_path(self.data_dir(), &artifact.1);
            if path.exists() {
                fs::remove_file(&path)
                    .map_err(|error| format!("cannot delete retained artifact: {error}"))?;
            }
            self.with_connection(|connection| {
                connection
                    .execute("DELETE FROM artifacts WHERE id = ?1", [&artifact.0])
                    .map_err(database_error)
            })?;
            total = total.saturating_sub(artifact.2);
            report.artifacts_deleted += 1;
            report.artifact_bytes_deleted += artifact.2;
        }
        self.compact()?;
        Ok(report)
    }

    pub fn backup_summary(&self) -> Result<BackupSummary, String> {
        self.with_connection(|connection| {
            let workflows = count_rows(connection, "workflows")?;
            let executions = count_rows(connection, "executions")?;
            let artifacts = count_rows(connection, "artifacts")?;
            Ok(BackupSummary {
                path: String::new(),
                workflows,
                executions,
                artifacts,
            })
        })
    }

    pub fn operational_metrics(&self) -> Result<OperationalMetrics, String> {
        let database_bytes = fs::metadata(self.data_dir().join("kakune.sqlite3"))
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        self.with_connection(|connection| {
            let queued_executions = count_where(connection, "executions", "status = 'queued'")?;
            let active_executions = count_where(
                connection,
                "executions",
                "status IN ('running', 'cancelling')",
            )?;
            let retained_events = count_rows(connection, "events")?;
            let artifacts = count_rows(connection, "artifacts")?;
            let artifact_bytes: i64 = connection
                .query_row(
                    "SELECT COALESCE(SUM(size_bytes), 0) FROM artifacts",
                    [],
                    |row| row.get(0),
                )
                .map_err(database_error)?;
            let artifact_bytes = u64::try_from(artifact_bytes)
                .map_err(|_| "artifact size is invalid".to_string())?;
            Ok(OperationalMetrics {
                queued_executions,
                active_executions,
                retained_events,
                artifacts,
                artifact_bytes,
                database_bytes,
            })
        })
    }

    pub fn set_execution_pinned(&self, id: &str, pinned: bool) -> Result<bool, String> {
        self.with_connection(|connection| {
            Ok(connection
                .execute(
                    "UPDATE executions SET pinned = ?1 WHERE id = ?2",
                    params![pinned, id],
                )
                .map_err(database_error)?
                == 1)
        })
    }

    fn artifact_bytes(&self) -> Result<u64, String> {
        self.with_connection(|connection| {
            let bytes: i64 = connection
                .query_row(
                    "SELECT COALESCE(SUM(size_bytes), 0) FROM artifacts",
                    [],
                    |row| row.get(0),
                )
                .map_err(database_error)?;
            u64::try_from(bytes).map_err(|_| "artifact size is invalid".to_string())
        })
    }

    fn unreferenced_artifacts_oldest(&self) -> Result<Vec<(String, String, u64)>, String> {
        self.with_connection(|connection| {
            let mut statement = connection
                .prepare(
                    "SELECT id, sha256, size_bytes FROM artifacts
                     WHERE NOT EXISTS (SELECT 1 FROM execution_artifacts WHERE artifact_id = artifacts.id)
                     ORDER BY created_at ASC, id ASC",
                )
                .map_err(database_error)?;
            statement
                .query_map([], |row| {
                    let bytes: i64 = row.get(2)?;
                    let bytes = u64::try_from(bytes)
                        .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(2, bytes))?;
                    Ok((row.get(0)?, row.get(1)?, bytes))
                })
                .map_err(database_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(database_error)
        })
    }

    /// Creates the initial local API credential only when no active credential exists.
    /// The raw token is returned once and only its SHA-256 digest is persisted.
    pub fn ensure_bootstrap_token(&self) -> Result<Option<String>, String> {
        self.with_connection(|connection| {
            let has_token: bool = connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM auth_tokens WHERE revoked_at IS NULL)",
                    [],
                    |row| row.get(0),
                )
                .map_err(database_error)?;
            if has_token {
                return Ok(None);
            }
            let token = format!("kakune_{}", Uuid::new_v4().simple());
            connection
                .execute(
                    "INSERT INTO auth_tokens (id, token_hash, name, scopes, created_at)
                     VALUES (?1, ?2, 'Bootstrap token', '[\"admin\"]', ?3)",
                    params![Uuid::new_v4().to_string(), token_hash(&token), now()?],
                )
                .map_err(database_error)?;
            Ok(Some(token))
        })
    }

    pub fn authorize(&self, token: &str) -> Result<bool, String> {
        self.authorize_scope(token, AuthScope::Read)
    }

    pub fn authorize_scope(&self, token: &str, required: AuthScope) -> Result<bool, String> {
        let expected = token_hash(token);
        self.with_connection(|connection| {
            let mut statement = connection
                .prepare(
                    "SELECT token_hash, scopes, expires_at
                     FROM auth_tokens WHERE revoked_at IS NULL",
                )
                .map_err(database_error)?;
            let hashes = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                })
                .map_err(database_error)?;
            for hash in hashes {
                let (hash, scopes, expires_at) = hash.map_err(database_error)?;
                if hash.as_bytes().ct_eq(expected.as_bytes()).into() {
                    if is_expired(expires_at.as_deref())? {
                        return Ok(false);
                    }
                    let scopes = parse_scopes(&scopes)?;
                    return Ok(scopes.contains(&AuthScope::Admin) || scopes.contains(&required));
                }
            }
            Ok(false)
        })
    }

    pub fn create_auth_token(
        &self,
        name: String,
        scopes: Vec<AuthScope>,
        expires_at: Option<String>,
    ) -> Result<CreatedAuthToken, String> {
        self.with_connection(|connection| create_auth_token(connection, name, scopes, expires_at))
    }

    pub fn list_auth_tokens(&self) -> Result<Vec<AuthTokenRecord>, String> {
        self.with_connection(|connection| {
            let mut statement = connection
                .prepare(
                    "SELECT id, name, scopes, created_at, expires_at, revoked_at
                     FROM auth_tokens ORDER BY created_at DESC",
                )
                .map_err(database_error)?;
            statement
                .query_map([], auth_token_from_row)
                .map_err(database_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(database_error)
        })
    }

    pub fn revoke_auth_token(&self, id: &str) -> Result<bool, String> {
        self.with_connection(|connection| {
            let changed = connection
                .execute(
                    "UPDATE auth_tokens SET revoked_at = ?1 WHERE id = ?2 AND revoked_at IS NULL",
                    params![now()?, id],
                )
                .map_err(database_error)?;
            Ok(changed == 1)
        })
    }

    pub fn set_secret(&self, name: &str, value: &str) -> Result<SecretRecord, String> {
        validate_secret_name(name)?;
        if value.is_empty() {
            return Err("secret value must not be empty".to_string());
        }
        let backend = SecretBackend::current()?;
        if matches!(
            backend,
            SecretBackend::Native | SecretBackend::NativeWithVaultBackup
        ) {
            self.native_secret_entry(name)?
                .set_secret(value.as_bytes())
                .map_err(|error| format!("cannot write native secret {name}: {error}"))?;
        }
        let encrypted = if matches!(
            backend,
            SecretBackend::Vault | SecretBackend::NativeWithVaultBackup
        ) {
            crate::vault::encrypt(self.data_dir(), value.as_bytes())?
        } else {
            crate::vault::Ciphertext {
                nonce: Vec::new(),
                ciphertext: Vec::new(),
            }
        };
        let updated_at = now()?;
        self.with_connection(|connection| {
            connection
                .execute(
                    "INSERT INTO secrets (name, nonce, ciphertext, updated_at, backend) VALUES (?1, ?2, ?3, ?4, ?5)
                     ON CONFLICT(name) DO UPDATE SET nonce = excluded.nonce, ciphertext = excluded.ciphertext, updated_at = excluded.updated_at, backend = excluded.backend",
                    params![name, encrypted.nonce, encrypted.ciphertext, updated_at, backend.database_value()],
                )
                .map_err(database_error)?;
            Ok(SecretRecord { name: name.to_string(), updated_at })
        })
    }

    pub fn list_secrets(&self) -> Result<Vec<SecretRecord>, String> {
        self.with_connection(|connection| {
            let mut statement = connection
                .prepare("SELECT name, updated_at FROM secrets ORDER BY name")
                .map_err(database_error)?;
            statement
                .query_map([], |row| {
                    Ok(SecretRecord {
                        name: row.get(0)?,
                        updated_at: row.get(1)?,
                    })
                })
                .map_err(database_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(database_error)
        })
    }

    pub fn delete_secret(&self, name: &str) -> Result<bool, String> {
        let backend = self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT backend FROM secrets WHERE name = ?1",
                    [name],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(database_error)
        })?;
        let Some(backend) = backend else {
            return Ok(false);
        };
        if backend == "native" || backend == "native-with-vault-backup" {
            self.native_secret_entry(name)?
                .delete_credential()
                .map_err(|error| format!("cannot delete native secret {name}: {error}"))?;
        }
        self.with_connection(|connection| {
            Ok(connection
                .execute("DELETE FROM secrets WHERE name = ?1", [name])
                .map_err(database_error)?
                == 1)
        })
    }

    pub fn resolve_secret(&self, name: &str) -> Result<String, String> {
        validate_secret_name(name)?;
        let (nonce, ciphertext, backend) = self
            .with_connection(|connection| {
                connection
                    .query_row(
                        "SELECT nonce, ciphertext, backend FROM secrets WHERE name = ?1",
                        [name],
                        |row| {
                            Ok((
                                row.get::<_, Vec<u8>>(0)?,
                                row.get::<_, Vec<u8>>(1)?,
                                row.get::<_, String>(2)?,
                            ))
                        },
                    )
                    .optional()
                    .map_err(database_error)
            })?
            .ok_or_else(|| format!("secret {name} was not found"))?;
        if backend == "native" || backend == "native-with-vault-backup" {
            let value = self
                .native_secret_entry(name)?
                .get_secret()
                .map_err(|error| format!("cannot read native secret {name}: {error}"))?;
            return String::from_utf8(value)
                .map_err(|_| format!("secret {name} is not valid UTF-8"));
        }
        String::from_utf8(crate::vault::decrypt(self.data_dir(), &nonce, &ciphertext)?)
            .map_err(|_| format!("secret {name} is not valid UTF-8"))
    }

    fn native_secret_entry(&self, name: &str) -> Result<keyring::Entry, String> {
        let core_id: String = self.with_connection(|connection| {
            connection
                .query_row("SELECT core_id FROM core_state WHERE id = 1", [], |row| {
                    row.get(0)
                })
                .map_err(database_error)
        })?;
        keyring::Entry::new(&format!("dev.kakune.core.{core_id}"), name)
            .map_err(|error| format!("native secret store is unavailable: {error}"))
    }

    /// Creates a provider profile. The requested ID must not already exist.
    pub fn create_provider_profile(
        &self,
        profile: ProviderProfileUpsert,
    ) -> Result<ProviderProfile, String> {
        let profile = normalize_provider_profile(profile)?;
        let timestamp = now()?;
        let (allowed_models, capabilities, auth_mode, secret_ref, config) =
            provider_profile_values(&profile)?;
        self.with_connection(|connection| {
            connection
                .execute(
                    "INSERT INTO provider_profiles (
                        id, display_name, provider_type, default_model, allowed_models_json,
                        capabilities_json, auth_mode, secret_ref, config_json, diagnostic_status,
                        diagnostic_checked_at, diagnostic_message, diagnostic_details_json, created_at, updated_at
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'unknown', NULL, NULL, NULL, ?10, ?10)",
                    params![
                        profile.id,
                        profile.display_name,
                        provider_type_name(&profile.provider_type),
                        profile.default_model,
                        allowed_models,
                        capabilities,
                        auth_mode,
                        secret_ref,
                        config,
                        timestamp,
                    ],
                )
                .map_err(database_error)?;
            get_provider_profile(connection, &profile.id)?.ok_or_else(|| {
                "provider profile was created but could not be read back".to_string()
            })
        })
    }

    /// Creates or updates a provider profile. Updating configuration clears the
    /// previous diagnosis because it no longer describes the active settings.
    pub fn upsert_provider_profile(
        &self,
        profile: ProviderProfileUpsert,
    ) -> Result<ProviderProfile, String> {
        let profile = normalize_provider_profile(profile)?;
        let timestamp = now()?;
        let (allowed_models, capabilities, auth_mode, secret_ref, config) =
            provider_profile_values(&profile)?;
        self.with_connection(|connection| {
            connection
                .execute(
                    "INSERT INTO provider_profiles (
                        id, display_name, provider_type, default_model, allowed_models_json,
                        capabilities_json, auth_mode, secret_ref, config_json, diagnostic_status,
                        diagnostic_checked_at, diagnostic_message, diagnostic_details_json, created_at, updated_at
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'unknown', NULL, NULL, NULL, ?10, ?10)
                     ON CONFLICT(id) DO UPDATE SET
                        display_name = excluded.display_name,
                        provider_type = excluded.provider_type,
                        default_model = excluded.default_model,
                        allowed_models_json = excluded.allowed_models_json,
                        capabilities_json = excluded.capabilities_json,
                        auth_mode = excluded.auth_mode,
                        secret_ref = excluded.secret_ref,
                        config_json = excluded.config_json,
                        diagnostic_status = 'unknown',
                        diagnostic_checked_at = NULL,
                        diagnostic_message = NULL,
                        diagnostic_details_json = NULL,
                        updated_at = excluded.updated_at",
                    params![
                        profile.id,
                        profile.display_name,
                        provider_type_name(&profile.provider_type),
                        profile.default_model,
                        allowed_models,
                        capabilities,
                        auth_mode,
                        secret_ref,
                        config,
                        timestamp,
                    ],
                )
                .map_err(database_error)?;
            get_provider_profile(connection, &profile.id)?.ok_or_else(|| {
                "provider profile was updated but could not be read back".to_string()
            })
        })
    }

    pub fn list_provider_profiles(&self) -> Result<Vec<ProviderProfile>, String> {
        self.with_connection(|connection| {
            let mut statement = connection
                .prepare(
                    "SELECT id, display_name, provider_type, default_model, allowed_models_json,
                            capabilities_json, auth_mode, secret_ref, config_json, diagnostic_status,
                            diagnostic_checked_at, diagnostic_message, diagnostic_details_json, created_at, updated_at
                     FROM provider_profiles ORDER BY id",
                )
                .map_err(database_error)?;
            statement
                .query_map([], provider_profile_from_row)
                .map_err(database_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(database_error)
        })
    }

    pub fn get_provider_profile(&self, id: &str) -> Result<Option<ProviderProfile>, String> {
        self.with_connection(|connection| get_provider_profile(connection, id))
    }

    /// Removes a profile without removing its referenced secret, which may be shared.
    pub fn delete_provider_profile(&self, id: &str) -> Result<Option<ProviderProfile>, String> {
        self.with_connection(|connection| {
            let profile = get_provider_profile(connection, id)?;
            if profile.is_some() {
                connection
                    .execute("DELETE FROM provider_profiles WHERE id = ?1", [id])
                    .map_err(database_error)?;
            }
            Ok(profile)
        })
    }

    /// Persists the non-secret result of a provider health or configuration check.
    pub fn diagnose_provider_profile(
        &self,
        id: &str,
        diagnostic: ProviderProfileDiagnostic,
    ) -> Result<Option<ProviderProfile>, String> {
        validate_provider_diagnostic(&diagnostic)?;
        let checked_at = now()?;
        self.with_connection(|connection| {
            let changed = connection
                .execute(
                    "UPDATE provider_profiles SET diagnostic_status = ?1, diagnostic_checked_at = ?2,
                         diagnostic_message = ?3, diagnostic_details_json = ?4, updated_at = ?2
                     WHERE id = ?5",
                    params![
                        provider_status_name(&diagnostic.status),
                        checked_at,
                        diagnostic.message,
                        diagnostic
                            .details
                            .map(|details| serde_json::to_string(&details))
                            .transpose()
                            .map_err(|error| format!("cannot serialize provider diagnosis: {error}"))?,
                        id,
                    ],
                )
                .map_err(database_error)?;
            if changed == 0 {
                return Ok(None);
            }
            get_provider_profile(connection, id)
        })
    }

    pub fn upsert_plugin_install(&self, plugin: &InstalledPluginRecord) -> Result<(), String> {
        self.with_connection(|connection| {
            connection.execute(
                "INSERT INTO installed_plugins (id, name, version, manifest_path, installed_at, enabled, policy_json, digest, lock_json, provenance_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10) ON CONFLICT(id) DO UPDATE SET name=excluded.name, version=excluded.version, manifest_path=excluded.manifest_path, installed_at=excluded.installed_at, enabled=excluded.enabled, policy_json=excluded.policy_json, digest=excluded.digest, lock_json=excluded.lock_json, provenance_json=excluded.provenance_json",
                params![plugin.id, plugin.name, plugin.version, plugin.manifest_path, plugin.installed_at, plugin.enabled, serde_json::to_string(&plugin.policy).map_err(|error| error.to_string())?, plugin.digest, serde_json::to_string(&plugin.resolved_lock).map_err(|error| error.to_string())?, serde_json::to_string(&plugin.provenance).map_err(|error| error.to_string())?],
            ).map_err(database_error)?;
            Ok(())
        })
    }

    pub fn set_plugin_enabled(&self, id: &str, enabled: bool) -> Result<bool, String> {
        self.with_connection(|connection| {
            Ok(connection
                .execute(
                    "UPDATE installed_plugins SET enabled = ?1 WHERE id = ?2",
                    params![enabled, id],
                )
                .map_err(database_error)?
                == 1)
        })
    }

    pub fn list_installed_plugins(&self) -> Result<Vec<InstalledPluginRecord>, String> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare("SELECT id, name, version, manifest_path, installed_at, enabled, policy_json, digest, lock_json, provenance_json FROM installed_plugins ORDER BY id").map_err(database_error)?;
            statement.query_map([], installed_plugin_from_row).map_err(database_error)?.collect::<Result<Vec<_>, _>>().map_err(database_error)
        })
    }

    pub fn remove_plugin_install(&self, id: &str) -> Result<Option<InstalledPluginRecord>, String> {
        self.with_connection(|connection| {
            let record = connection.query_row("SELECT id, name, version, manifest_path, installed_at, enabled, policy_json, digest, lock_json, provenance_json FROM installed_plugins WHERE id = ?1", [id], installed_plugin_from_row).optional().map_err(database_error)?;
            if record.is_some() { connection.execute("DELETE FROM installed_plugins WHERE id = ?1", [id]).map_err(database_error)?; }
            Ok(record)
        })
    }

    pub fn save_prepared_plugin_install(
        &self,
        prepared: &PreparedPluginInstallRecord,
    ) -> Result<(), String> {
        self.with_connection(|connection| {
            connection.execute(
                "INSERT INTO prepared_plugin_installs (id, plugin_id, plugin_name, version, digest, staged_path, prepared_at, policy_json, lock_json, provenance_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![prepared.id, prepared.plugin_id, prepared.plugin_name, prepared.version, prepared.digest, prepared.staged_path, prepared.prepared_at, serde_json::to_string(&prepared.policy).map_err(|error| error.to_string())?, serde_json::to_string(&prepared.resolved_lock).map_err(|error| error.to_string())?, serde_json::to_string(&prepared.provenance).map_err(|error| error.to_string())?],
            ).map_err(database_error)?;
            Ok(())
        })
    }

    pub fn prepared_plugin_install(
        &self,
        id: &str,
    ) -> Result<Option<PreparedPluginInstallRecord>, String> {
        self.with_connection(|connection| connection.query_row(
            "SELECT id, plugin_id, plugin_name, version, digest, staged_path, prepared_at, policy_json, lock_json, provenance_json FROM prepared_plugin_installs WHERE id = ?1", [id], prepared_plugin_install_from_row
        ).optional().map_err(database_error))
    }

    pub fn remove_prepared_plugin_install(
        &self,
        id: &str,
    ) -> Result<Option<PreparedPluginInstallRecord>, String> {
        self.with_connection(|connection| {
            let record = connection.query_row(
                "SELECT id, plugin_id, plugin_name, version, digest, staged_path, prepared_at, policy_json, lock_json, provenance_json FROM prepared_plugin_installs WHERE id = ?1", [id], prepared_plugin_install_from_row
            ).optional().map_err(database_error)?;
            if record.is_some() { connection.execute("DELETE FROM prepared_plugin_installs WHERE id = ?1", [id]).map_err(database_error)?; }
            Ok(record)
        })
    }

    pub fn core_id(&self) -> Result<String, String> {
        self.with_connection(|connection| {
            connection
                .query_row("SELECT core_id FROM core_state WHERE id = 1", [], |row| {
                    row.get(0)
                })
                .map_err(database_error)
        })
    }

    pub fn schema_version(&self) -> Result<u32, String> {
        self.with_connection(|connection| schema_version(connection))
    }

    pub fn upsert_workflow(
        &self,
        workflow: &WorkflowDocument,
        source: &str,
        status: &str,
    ) -> Result<WorkflowRecord, String> {
        if status == "enabled" {
            crate::scheduler::validate_enabled_triggers(workflow, &self.workspace_dir()?)?;
        }
        let now = now()?;
        let id = workflow.metadata.id.clone();
        let revision = Uuid::new_v4().to_string();
        self.with_connection(|connection| {
            let transaction = connection.transaction().map_err(database_error)?;
            let previous_status: Option<String> = transaction
                .query_row("SELECT status FROM workflows WHERE id = ?1", [&id], |row| {
                    row.get(0)
                })
                .optional()
                .map_err(database_error)?;
            transaction
                .execute(
                    "INSERT INTO workflows (id, name, source, status, revision, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                     ON CONFLICT(id) DO UPDATE SET
                       source = excluded.source,
                       name = excluded.name,
                       status = excluded.status,
                       revision = excluded.revision,
                       updated_at = excluded.updated_at",
                    params![id, workflow.metadata.name, source, status, revision, now],
                )
                .map_err(database_error)?;
            transaction
                .execute(
                    "INSERT INTO workflow_revisions (id, workflow_id, source, created_at)
                     VALUES (?1, ?2, ?3, ?4)",
                    params![revision, workflow.metadata.id, source, now],
                )
                .map_err(database_error)?;
            let record = WorkflowRecord {
                id: workflow.metadata.id.clone(),
                name: workflow.metadata.name.clone(),
                status: status.to_owned(),
                revision,
                updated_at: now,
            };
            append_event(
                &transaction,
                if previous_status.is_some() {
                    "workflow.updated"
                } else {
                    "workflow.created"
                },
                &record.id,
                None,
                serde_json::json!({
                    "name": record.name,
                    "status": record.status,
                    "revision": record.revision,
                }),
                &record.updated_at,
            )?;
            if previous_status
                .as_deref()
                .is_some_and(|current| current != status)
            {
                append_event(
                    &transaction,
                    "workflow.status_changed",
                    &record.id,
                    None,
                    serde_json::json!({ "status": status }),
                    &record.updated_at,
                )?;
            }
            transaction.commit().map_err(database_error)?;
            Ok(record)
        })
    }

    pub fn update_workflow_if_revision(
        &self,
        workflow: &WorkflowDocument,
        source: &str,
        expected_revision: &str,
    ) -> Result<WorkflowSourceUpdate, String> {
        let now = now()?;
        let id = workflow.metadata.id.clone();
        self.with_connection(|connection| {
            let transaction = connection.transaction().map_err(database_error)?;
            let current: Option<(String, String)> = transaction
                .query_row(
                    "SELECT status, revision FROM workflows WHERE id = ?1",
                    [&id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(database_error)?;
            let Some((status, current_revision)) = current else {
                return Ok(WorkflowSourceUpdate::NotFound);
            };
            if current_revision != expected_revision {
                return Ok(WorkflowSourceUpdate::Conflict { current_revision });
            }
            if status == "enabled" {
                crate::scheduler::validate_enabled_triggers(workflow, &self.workspace_dir()?)?;
            }
            let revision = Uuid::new_v4().to_string();
            transaction.execute(
                "UPDATE workflows SET source = ?2, name = ?3, revision = ?4, updated_at = ?5 WHERE id = ?1",
                params![id, source, workflow.metadata.name, revision, now],
            ).map_err(database_error)?;
            transaction.execute(
                "INSERT INTO workflow_revisions (id, workflow_id, source, created_at) VALUES (?1, ?2, ?3, ?4)",
                params![revision, workflow.metadata.id, source, now],
            ).map_err(database_error)?;
            let record = WorkflowRecord {
                id: workflow.metadata.id.clone(),
                name: workflow.metadata.name.clone(),
                status,
                revision,
                updated_at: now,
            };
            append_event(
                &transaction,
                "workflow.updated",
                &record.id,
                None,
                serde_json::json!({
                    "name": record.name,
                    "status": record.status,
                    "revision": record.revision,
                }),
                &record.updated_at,
            )?;
            transaction.commit().map_err(database_error)?;
            Ok(WorkflowSourceUpdate::Updated(record))
        })
    }

    pub fn list_workflows(&self) -> Result<Vec<WorkflowRecord>, String> {
        self.with_connection(|connection| {
            let mut statement = connection
                .prepare(
                    "SELECT id, name, status, revision, updated_at FROM workflows ORDER BY name",
                )
                .map_err(database_error)?;
            let rows = statement
                .query_map([], |row| {
                    Ok(WorkflowRecord {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        status: row.get(2)?,
                        revision: row.get(3)?,
                        updated_at: row.get(4)?,
                    })
                })
                .map_err(database_error)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(database_error)
        })
    }

    pub fn list_enabled_workflow_sources(&self) -> Result<Vec<(String, String)>, String> {
        self.with_connection(|connection| {
            let mut statement = connection
                .prepare("SELECT id, source FROM workflows WHERE status = 'enabled' ORDER BY id")
                .map_err(database_error)?;
            let rows = statement
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .map_err(database_error)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(database_error)
        })
    }

    pub(crate) fn scheduled_trigger(
        &self,
        workflow_id: &str,
        trigger_id: &str,
    ) -> Result<Option<ScheduledTriggerRecord>, String> {
        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT next_run_at, completed_at FROM scheduled_triggers
                     WHERE workflow_id = ?1 AND trigger_id = ?2",
                    params![workflow_id, trigger_id],
                    |row| {
                        let next_run_at = row.get::<_, String>(0)?;
                        let next_run_at =
                            OffsetDateTime::parse(&next_run_at, &Rfc3339).map_err(|error| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    0,
                                    rusqlite::types::Type::Text,
                                    Box::new(error),
                                )
                            })?;
                        Ok(ScheduledTriggerRecord {
                            next_run_at,
                            completed_at: row.get(1)?,
                        })
                    },
                )
                .optional()
                .map_err(database_error)
        })
    }

    /// Initializes a durable schedule without claiming a run. Existing deadlines
    /// remain authoritative across restarts while the persisted policy is updated.
    pub(crate) fn ensure_scheduled_trigger(
        &self,
        workflow_id: &str,
        trigger_id: &str,
        next_run_at: OffsetDateTime,
        misfire_policy: &str,
        now_time: OffsetDateTime,
    ) -> Result<(), String> {
        let next_run_at = next_run_at
            .format(&Rfc3339)
            .map_err(|error| format!("cannot format trigger deadline: {error}"))?;
        let now_time = now_time
            .format(&Rfc3339)
            .map_err(|error| format!("cannot format current time: {error}"))?;
        self.with_connection(|connection| {
            connection.execute(
                "INSERT INTO scheduled_triggers (workflow_id, trigger_id, next_run_at, misfire_policy, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(workflow_id, trigger_id) DO UPDATE SET
                   misfire_policy = excluded.misfire_policy,
                   updated_at = excluded.updated_at",
                params![workflow_id, trigger_id, next_run_at, misfire_policy, now_time],
            ).map_err(database_error)?;
            Ok(())
        })
    }

    /// Atomically records a claimed deadline and its replacement. `completed` is
    /// used by one-shot date/time triggers so they cannot fire again after restart.
    pub(crate) fn advance_scheduled_trigger(
        &self,
        workflow_id: &str,
        trigger_id: &str,
        next_run_at: OffsetDateTime,
        completed: bool,
        now_time: OffsetDateTime,
    ) -> Result<(), String> {
        let next_run_at = next_run_at
            .format(&Rfc3339)
            .map_err(|error| format!("cannot format trigger deadline: {error}"))?;
        let now_time = now_time
            .format(&Rfc3339)
            .map_err(|error| format!("cannot format current time: {error}"))?;
        self.with_connection(|connection| {
            connection
                .execute(
                    "UPDATE scheduled_triggers
                 SET next_run_at = ?3, last_run_at = ?4,
                     completed_at = CASE WHEN ?5 THEN ?4 ELSE NULL END,
                     updated_at = ?4
                 WHERE workflow_id = ?1 AND trigger_id = ?2",
                    params![workflow_id, trigger_id, next_run_at, now_time, completed],
                )
                .map_err(database_error)?;
            Ok(())
        })
    }

    /// Records scheduler conditions in the same durable event stream as runtime
    /// events, notably filesystem watcher overflow.
    pub(crate) fn record_scheduler_event(
        &self,
        event_type: &str,
        resource_id: &str,
        payload: Value,
    ) -> Result<(), String> {
        let timestamp = now()?;
        self.with_connection(|connection| {
            let transaction = connection.transaction().map_err(database_error)?;
            append_event(
                &transaction,
                event_type,
                resource_id,
                None,
                payload,
                &timestamp,
            )?;
            transaction.commit().map_err(database_error)
        })
    }

    /// Atomically advances an interval trigger when its persisted deadline has elapsed.
    pub fn claim_interval_trigger(
        &self,
        workflow_id: &str,
        trigger_id: &str,
        every_seconds: u64,
    ) -> Result<bool, String> {
        let now_time = OffsetDateTime::now_utc();
        let now_text = now_time
            .format(&Rfc3339)
            .map_err(|error| format!("cannot format current time: {error}"))?;
        let seconds =
            i64::try_from(every_seconds).map_err(|_| "interval is too large".to_string())?;
        let next_text = (now_time + Duration::seconds(seconds))
            .format(&Rfc3339)
            .map_err(|error| format!("cannot format next trigger time: {error}"))?;
        self.with_connection(|connection| {
            let due_at: Option<String> = connection
                .query_row(
                    "SELECT next_run_at FROM scheduled_triggers WHERE workflow_id = ?1 AND trigger_id = ?2",
                    params![workflow_id, trigger_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(database_error)?;
            let due = due_at
                .as_deref()
                .map(|value| OffsetDateTime::parse(value, &Rfc3339).map(|time| time <= now_time))
                .transpose()
                .map_err(|error| format!("cannot parse persisted trigger deadline: {error}"))?
                .unwrap_or(true);
            if !due {
                return Ok(false);
            }
            connection
                .execute(
                    "INSERT INTO scheduled_triggers (workflow_id, trigger_id, next_run_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT(workflow_id, trigger_id) DO UPDATE SET
                       next_run_at = excluded.next_run_at,
                       updated_at = excluded.updated_at",
                    params![workflow_id, trigger_id, next_text, now_text],
                )
                .map_err(database_error)?;
            Ok(true)
        })
    }

    /// Persists a calendar trigger's next deadline, or atomically claims one elapsed deadline.
    /// A caller supplies the next future deadline after a claim, so missed schedules run once.
    #[allow(dead_code)]
    pub(crate) fn claim_scheduled_trigger(
        &self,
        workflow_id: &str,
        trigger_id: &str,
        now_time: OffsetDateTime,
        initial_next: OffsetDateTime,
        next_after_claim: OffsetDateTime,
    ) -> Result<bool, String> {
        let now_text = now_time
            .format(&Rfc3339)
            .map_err(|error| format!("cannot format current time: {error}"))?;
        let initial_text = initial_next
            .format(&Rfc3339)
            .map_err(|error| format!("cannot format initial trigger time: {error}"))?;
        let next_text = next_after_claim
            .format(&Rfc3339)
            .map_err(|error| format!("cannot format next trigger time: {error}"))?;
        self.with_connection(|connection| {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(database_error)?;
            let due_at: Option<String> = transaction
                .query_row(
                    "SELECT next_run_at FROM scheduled_triggers WHERE workflow_id = ?1 AND trigger_id = ?2",
                    params![workflow_id, trigger_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(database_error)?;
            let Some(due_at) = due_at else {
                transaction
                    .execute(
                        "INSERT INTO scheduled_triggers (workflow_id, trigger_id, next_run_at, updated_at)
                         VALUES (?1, ?2, ?3, ?4)",
                        params![workflow_id, trigger_id, initial_text, now_text],
                    )
                    .map_err(database_error)?;
                transaction.commit().map_err(database_error)?;
                return Ok(false);
            };
            let due_at = OffsetDateTime::parse(&due_at, &Rfc3339)
                .map_err(|error| format!("cannot parse persisted trigger deadline: {error}"))?;
            if due_at > now_time {
                transaction.commit().map_err(database_error)?;
                return Ok(false);
            }
            transaction
                .execute(
                    "UPDATE scheduled_triggers SET next_run_at = ?3, updated_at = ?4
                     WHERE workflow_id = ?1 AND trigger_id = ?2",
                    params![workflow_id, trigger_id, next_text, now_text],
                )
                .map_err(database_error)?;
            transaction.commit().map_err(database_error)?;
            Ok(true)
        })
    }

    pub fn get_workflow_source(&self, id: &str) -> Result<Option<WorkflowSourceRecord>, String> {
        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT source, revision FROM workflows WHERE id = ?1",
                    [id],
                    |row| {
                        Ok(WorkflowSourceRecord {
                            source: row.get(0)?,
                            revision: row.get(1)?,
                        })
                    },
                )
                .optional()
                .map_err(database_error)
        })
    }

    pub fn set_workflow_status(&self, id: &str, status: &str) -> Result<(), String> {
        let now = now()?;
        self.with_connection(|connection| {
            let transaction = connection.transaction().map_err(database_error)?;
            let affected = transaction
                .execute(
                    "UPDATE workflows SET status = ?1, updated_at = ?2 WHERE id = ?3",
                    params![status, now, id],
                )
                .map_err(database_error)?;
            if affected == 0 {
                return Err(format!("workflow {id} was not found"));
            }
            append_event(
                &transaction,
                "workflow.status_changed",
                id,
                None,
                serde_json::json!({ "status": status }),
                &now,
            )?;
            transaction.commit().map_err(database_error)?;
            Ok(())
        })
    }

    pub fn create_execution(&self, workflow_name: &str) -> Result<ExecutionRecord, String> {
        let execution = ExecutionRecord {
            id: Uuid::new_v4().to_string(),
            workflow_name: workflow_name.to_owned(),
            status: "running".to_string(),
            created_at: now()?,
            completed_at: None,
            error: None,
        };
        self.with_connection(|connection| {
            let transaction = connection.transaction().map_err(database_error)?;
            transaction
                .execute(
                    "INSERT INTO executions (id, workflow_name, status, created_at) VALUES (?1, ?2, ?3, ?4)",
                    params![execution.id, execution.workflow_name, execution.status, execution.created_at],
                )
                .map_err(database_error)?;
            append_event(
                &transaction,
                "execution.started",
                workflow_name,
                Some(&execution.id),
                serde_json::json!({
                    "workflowId": workflow_name,
                    "status": execution.status,
                }),
                &execution.created_at,
            )?;
            transaction.commit().map_err(database_error)?;
            Ok(execution)
        })
    }

    /// Stores the immutable workflow revision and private plan that an execution
    /// will use. This is deliberately separate from the mutable workflow row.
    pub fn find_idempotent_execution(
        &self,
        key: &str,
        hash: &str,
    ) -> Result<Option<ExecutionRecord>, String> {
        self.with_connection(|db| idempotent_execution(db, key, hash))
    }

    pub fn create_execution_with_plan(
        &self,
        workflow_name: &str,
        workflow_revision: &str,
        plan: &Value,
    ) -> Result<ExecutionRecord, String> {
        self.create_execution_with_plan_request(workflow_name, workflow_revision, plan, None)
            .map(|(execution, _)| execution)
    }

    pub fn create_execution_with_plan_request(
        &self,
        workflow_name: &str,
        workflow_revision: &str,
        plan: &Value,
        identity: Option<(&str, &str)>,
    ) -> Result<(ExecutionRecord, bool), String> {
        let policy = serde_json::from_value::<crate::workflow::WorkflowPolicy>(
            plan.get("policy").cloned().unwrap_or(Value::Null),
        )
        .map_err(|error| format!("cannot decode execution policy: {error}"))?;
        let execution_id = Uuid::new_v4().to_string();
        let created_at = now()?;
        let plan_json = serde_json::to_string(plan)
            .map_err(|error| format!("cannot serialize execution plan: {error}"))?;
        let plan_hash = artifacts::artifact_id(plan_json.as_bytes());
        self.with_connection(|connection| {
            let transaction = connection.transaction().map_err(database_error)?;
            if let Some((key, hash)) = identity {
                if let Some(record) = idempotent_execution(&transaction, key, hash)? { return Ok((record, false)); }
            }
            let status = execution_admission(&transaction, workflow_name, policy.concurrency.as_ref())?;
            transaction
                .execute(
                    "INSERT INTO executions (id, workflow_name, status, created_at) VALUES (?1, ?2, ?3, ?4)",
                    params![execution_id, workflow_name, status, created_at],
                )
                .map_err(database_error)?;
            transaction
                .execute(
                    "INSERT INTO execution_plans (execution_id, workflow_revision, plan_json, plan_hash, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![execution_id, workflow_revision, plan_json, plan_hash, created_at],
                )
                .map_err(database_error)?;
            if let Some((key, hash)) = identity {
                transaction.execute("INSERT INTO execution_idempotency(request_key,request_hash,execution_id) VALUES(?1,?2,?3)",params![key,hash,execution_id]).map_err(database_error)?;
            }
            append_event(
                &transaction,
                if status == "queued" { "execution.queued" } else { "execution.started" },
                workflow_name,
                Some(&execution_id),
                serde_json::json!({
                    "workflowId": workflow_name,
                    "status": status,
                }),
                &created_at,
            )?;
            transaction.commit().map_err(database_error)?;
            Ok((ExecutionRecord {
            id: execution_id,
            workflow_name: workflow_name.to_owned(),
            status,
            created_at,
            completed_at: None,
            error: None,
        }, true))
        })
    }

    /// Atomically transitions a queued execution to running once its workflow
    /// has capacity. Callers may poll this bounded, durable queue safely.
    pub fn claim_execution_slot(&self, id: &str, max_runs: Option<u32>) -> Result<bool, String> {
        self.with_connection(|connection| {
            let transaction = connection.transaction().map_err(database_error)?;
            let workflow_id: Option<String> = transaction
                .query_row("SELECT workflow_name FROM executions WHERE id = ?1 AND status = 'queued'", [id], |row| row.get(0))
                .optional()
                .map_err(database_error)?;
            let Some(workflow_id) = workflow_id else { return Ok(false); };
            let allowed = if let Some(max_runs) = max_runs {
                let running: u32 = transaction.query_row(
                    "SELECT COUNT(*) FROM executions WHERE workflow_name = ?1 AND status IN ('running', 'cancelling')",
                    [&workflow_id], |row| row.get(0),
                ).map_err(database_error)?;
                running < max_runs
            } else {
                true
            };
            if !allowed { return Ok(false); }
            let started_at = now()?;
            transaction.execute("UPDATE executions SET status = 'running' WHERE id = ?1 AND status = 'queued'", [id]).map_err(database_error)?;
            append_event(&transaction, "execution.started", &workflow_id, Some(id), serde_json::json!({ "workflowId": workflow_id, "status": "running" }), &started_at)?;
            transaction.commit().map_err(database_error)?;
            Ok(true)
        })
    }

    pub fn set_execution_result(&self, id: &str, result: &Value) -> Result<(), String> {
        let result = serde_json::to_string(result)
            .map_err(|error| format!("cannot serialize execution result: {error}"))?;
        self.with_connection(|connection| {
            connection.execute(
                "INSERT INTO execution_results (execution_id, result_json, updated_at) VALUES (?1, ?2, ?3)
                 ON CONFLICT(execution_id) DO UPDATE SET result_json = excluded.result_json, updated_at = excluded.updated_at",
                params![id, result, now()?],
            ).map_err(database_error)?;
            Ok(())
        })
    }

    pub fn execution_result(&self, id: &str) -> Result<Option<Value>, String> {
        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT result_json FROM execution_results WHERE execution_id = ?1",
                    [id],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(database_error)?
                .map(|value| {
                    serde_json::from_str(&value)
                        .map_err(|error| format!("cannot decode execution result: {error}"))
                })
                .transpose()
        })
    }

    pub fn execution_plan(&self, id: &str) -> Result<Option<Value>, String> {
        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT plan_json FROM execution_plans WHERE execution_id = ?1",
                    [id],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(database_error)?
                .map(|value| {
                    serde_json::from_str(&value)
                        .map_err(|error| format!("cannot decode execution plan: {error}"))
                })
                .transpose()
        })
    }

    pub fn put_artifact(
        &self,
        bytes: &[u8],
        media_type: &str,
        name: Option<&str>,
    ) -> Result<ArtifactRef, String> {
        if media_type.trim().is_empty() || media_type.len() > 200 {
            return Err("artifact media type must be 1-200 characters".to_string());
        }
        let sha256 = artifacts::artifact_id(bytes);
        let record = ArtifactRef {
            id: sha256.clone(),
            sha256: sha256.clone(),
            size_bytes: bytes.len() as u64,
            media_type: media_type.to_string(),
            name: name.map(str::to_string),
        };
        artifacts::write_atomically(&artifacts::artifact_path(self.data_dir(), &sha256), bytes)?;
        self.with_connection(|connection| {
            connection.execute(
                "INSERT OR IGNORE INTO artifacts (id, sha256, size_bytes, media_type, name, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![record.id, record.sha256, i64::try_from(record.size_bytes).map_err(|_| "artifact is too large to persist".to_string())?, record.media_type, record.name, now()?],
            ).map_err(database_error)?;
            Ok(record)
        })
    }

    /// Returns an artifact only when it is registered in the database.  The
    /// path is derived from its content hash after validating that hash, so a
    /// caller cannot use this API to read arbitrary workspace files.
    pub fn read_artifact(&self, id: &str) -> Result<Option<(ArtifactRef, Vec<u8>)>, String> {
        if !is_artifact_id(id) {
            return Err(
                "artifact id must be a 64-character lowercase SHA-256 hex digest".to_string(),
            );
        }
        let artifact = self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT id, sha256, size_bytes, media_type, name FROM artifacts WHERE id = ?1",
                    [id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, Option<String>>(4)?,
                        ))
                    },
                )
                .optional()
                .map_err(database_error)
        })?;
        let Some((record_id, sha256, size_bytes, media_type, name)) = artifact else {
            return Ok(None);
        };
        let artifact = ArtifactRef {
            id: record_id,
            sha256,
            size_bytes: u64::try_from(size_bytes)
                .map_err(|_| format!("artifact {id} has an invalid persisted size"))?,
            media_type,
            name,
        };
        let bytes = fs::read(artifacts::artifact_path(self.data_dir(), &artifact.sha256))
            .map_err(|error| format!("cannot read artifact {id}: {error}"))?;
        if bytes.len() as u64 != artifact.size_bytes
            || artifacts::artifact_id(&bytes) != artifact.sha256
        {
            return Err(format!(
                "artifact {id} does not match its persisted content hash"
            ));
        }
        Ok(Some((artifact, bytes)))
    }

    pub fn finish_execution(
        &self,
        id: &str,
        status: &str,
        error: Option<&str>,
    ) -> Result<(), String> {
        let completed_at = now()?;
        self.with_connection(|connection| {
            let transaction = connection.transaction().map_err(database_error)?;
            let workflow_id: String = transaction
                .query_row(
                    "SELECT workflow_name FROM executions WHERE id = ?1",
                    [id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(database_error)?
                .ok_or_else(|| format!("execution {id} was not found"))?;
            transaction
                .execute(
                    "UPDATE executions SET status = ?1, completed_at = ?2, error = ?3 WHERE id = ?4",
                    params![status, completed_at, error, id],
                )
                .map_err(database_error)?;
            transaction.execute("UPDATE execution_trace_spans SET status=?2,completed_at=?3,error=COALESCE(?4,'Execution ended before this span was closed') WHERE execution_id=?1 AND status='running'", params![id,if status == "failed" { "failed" } else { "cancelled" },completed_at,error]).map_err(database_error)?;
            append_event(
                &transaction,
                "execution.finished",
                &workflow_id,
                Some(id),
                serde_json::json!({ "status": status, "error": error }),
                &completed_at,
            )?;
            transaction.commit().map_err(database_error)?;
            Ok(())
        })
    }

    /// Runtime checkpoints are keyed by the full activation scope. A checkpoint
    /// marked running is an uncertain external effect and is never replayed.
    pub(crate) fn checkpoint_result(
        &self,
        execution: &str,
        node: &str,
    ) -> Result<Option<Value>, String> {
        self.with_connection(|db| {
            let value: Option<String> = db.query_row("SELECT result_json FROM node_checkpoints WHERE execution_id=?1 AND node_id=?2 AND status='completed'", params![execution,node], |row| row.get(0)).optional().map_err(database_error)?.flatten();
            value.map(|json| serde_json::from_str(&json).map_err(|error| error.to_string())).transpose()
        })
    }

    pub(crate) fn begin_checkpoint(
        &self,
        execution: &str,
        node: &str,
        status: &str,
    ) -> Result<(), String> {
        self.with_connection(|db| { db.execute("INSERT INTO node_checkpoints(execution_id,node_id,status) VALUES(?1,?2,?3) ON CONFLICT(execution_id,node_id) DO UPDATE SET status=excluded.status",params![execution,node,status]).map_err(database_error)?; Ok(()) })
    }

    pub(crate) fn complete_checkpoint(
        &self,
        execution: &str,
        node: &str,
        result: Option<&Value>,
    ) -> Result<(), String> {
        let json = result
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| error.to_string())?;
        self.with_connection(|db| { db.execute("UPDATE node_checkpoints SET status=?3,result_json=?4 WHERE execution_id=?1 AND node_id=?2", params![execution,node,if json.is_some() { "completed" } else { "unrecoverable" },json]).map_err(database_error)?; Ok(()) })
    }

    pub(crate) fn durable_delay_remaining(
        &self,
        execution: &str,
        node: &str,
        duration: std::time::Duration,
    ) -> Result<std::time::Duration, String> {
        let now = OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;
        let deadline = i64::try_from(now + duration.as_millis() as i128)
            .map_err(|_| "delay deadline overflow")?;
        self.with_connection(|db| {
            db.execute("UPDATE node_checkpoints SET deadline_ms=COALESCE(deadline_ms,?3),status='waiting' WHERE execution_id=?1 AND node_id=?2",params![execution,node,deadline]).map_err(database_error)?;
            let deadline: i64 = db.query_row("SELECT deadline_ms FROM node_checkpoints WHERE execution_id=?1 AND node_id=?2",params![execution,node],|row|row.get(0)).map_err(database_error)?;
            Ok(std::time::Duration::from_millis((i128::from(deadline)-now).max(0) as u64))
        })
    }

    pub(crate) fn execution_time_remaining(
        &self,
        execution: &str,
        duration: std::time::Duration,
    ) -> Result<std::time::Duration, String> {
        let now = OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;
        let deadline = i64::try_from(now + duration.as_millis() as i128)
            .map_err(|_| "execution deadline overflow")?;
        self.with_connection(|db| {
            db.execute(
                "INSERT OR IGNORE INTO execution_deadlines(execution_id,deadline_ms) VALUES(?1,?2)",
                params![execution, deadline],
            )
            .map_err(database_error)?;
            let deadline: i64 = db
                .query_row(
                    "SELECT deadline_ms FROM execution_deadlines WHERE execution_id=?1",
                    [execution],
                    |row| row.get(0),
                )
                .map_err(database_error)?;
            Ok(std::time::Duration::from_millis(
                (i128::from(deadline) - now).max(0) as u64,
            ))
        })
    }

    pub fn queued_executions(&self) -> Result<Vec<ExecutionRecord>, String> {
        self.with_connection(|db| {
            let mut query = db.prepare("SELECT id,workflow_name,status,created_at,completed_at,error FROM executions WHERE status='queued' ORDER BY created_at,id").map_err(database_error)?;
            query.query_map([],execution_from_row).map_err(database_error)?.collect::<Result<Vec<_>,_>>().map_err(database_error)
        })
    }

    /// Reconciles work which cannot safely continue after the Core process exits.
    /// Completed checkpoints and waiting delays resume; uncertain effects stop.
    pub fn recover_incomplete_executions(&self) -> Result<u64, String> {
        self.with_connection(|connection| {
            let transaction = connection.transaction().map_err(database_error)?;
            let mut statement = transaction
                .prepare("SELECT id, workflow_name, status FROM executions WHERE status IN ('running', 'cancelling')")
                .map_err(database_error)?;
            let interrupted = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })
                .map_err(database_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(database_error)?;
            drop(statement);

            let completed_at = now()?;
            for (id, workflow_name, status) in &interrupted {
                let resumable: bool = transaction.query_row("SELECT EXISTS(SELECT 1 FROM execution_plans WHERE execution_id=?1) AND NOT EXISTS(SELECT 1 FROM node_checkpoints WHERE execution_id=?1 AND status IN ('running','unrecoverable'))", [id], |row|row.get(0)).map_err(database_error)?;
                let (next_status, error) = if status == "cancelling" {
                    ("cancelled", None)
                } else if resumable {
                    ("queued", None)
                } else {
                    ("interrupted", Some("Core restarted during an operation with uncertain effects"))
                };
                transaction
                    .execute(
                        "UPDATE executions SET status = ?1, completed_at = ?2, error = ?3 WHERE id = ?4",
                        params![next_status, if next_status == "queued" { None } else { Some(&completed_at) }, error, id],
                    )
                    .map_err(database_error)?;
                transaction.execute("UPDATE execution_trace_spans SET status='cancelled', completed_at=?2, error='Core restarted' WHERE execution_id=?1 AND status='running'", params![id, completed_at]).map_err(database_error)?;
                append_event(
                    &transaction,
                    "execution.recovered",
                    workflow_name,
                    Some(id),
                    serde_json::json!({ "status": next_status, "previousStatus": status }),
                    &completed_at,
                )?;
            }
            transaction.commit().map_err(database_error)?;
            Ok(interrupted.len() as u64)
        })
    }

    pub fn request_execution_cancel(&self, id: &str) -> Result<bool, String> {
        self.with_connection(|connection| {
            Ok(connection
                .execute(
                    "UPDATE executions SET status = CASE WHEN status = 'queued' THEN 'cancelled' ELSE 'cancelling' END WHERE id = ?1 AND status IN ('queued', 'running')",
                    [id],
                )
                .map_err(database_error)?
                == 1)
        })
    }

    pub fn cancel_queued_execution(&self, id: &str) -> Result<bool, String> {
        let completed_at = now()?;
        self.with_connection(|connection| {
            Ok(connection.execute(
                "UPDATE executions SET status = 'cancelled', completed_at = ?2 WHERE id = ?1 AND status = 'queued'",
                params![id, completed_at],
            ).map_err(database_error)? == 1)
        })
    }

    pub fn execution_cancel_requested(&self, id: &str) -> Result<bool, String> {
        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT status IN ('cancelling', 'cancelled') FROM executions WHERE id = ?1",
                    [id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(database_error)
                .map(|value| value.unwrap_or(false))
        })
    }

    pub fn record_node_run(
        &self,
        execution_id: &str,
        node_id: &str,
        status: &str,
        message: Option<&str>,
    ) -> Result<(), String> {
        self.record_node_outcome(execution_id, node_id, status, message, None, None)
    }

    /// Reserves output-token capacity atomically before an AI provider request.
    /// The reservation intentionally does not claim to be a billed monetary cost.
    pub fn reserve_provider_tokens(
        &self,
        execution_id: &str,
        provider: &str,
        tokens: u64,
        budget: Option<u64>,
    ) -> Result<(), String> {
        let tokens = i64::try_from(tokens)
            .map_err(|_| "AI token reservation exceeds SQLite integer range".to_string())?;
        self.with_connection(|connection| {
            let transaction = connection.transaction().map_err(database_error)?;
            let used: i64 = transaction
                .query_row(
                    "SELECT COALESCE(SUM(reserved_tokens), 0) FROM provider_token_reservations WHERE execution_id = ?1 AND provider = ?2",
                    params![execution_id, provider],
                    |row| row.get(0),
                )
                .map_err(database_error)?;
            if budget.is_some_and(|limit| {
                used.saturating_add(tokens) > i64::try_from(limit).unwrap_or(i64::MAX)
            }) {
                return Err(format!("AI token budget for provider {provider} would be exceeded before the provider request"));
            }
            transaction.execute(
                "INSERT INTO provider_token_reservations (execution_id, provider, reserved_tokens, created_at) VALUES (?1, ?2, ?3, ?4)",
                params![execution_id, provider, tokens, now()?],
            ).map_err(database_error)?;
            transaction.commit().map_err(database_error)
        })
    }

    /// Persists provider facts separately from node output so requested and
    /// reported model names, usage and provider-defined fields stay traceable.
    pub fn record_provider_invocation(
        &self,
        invocation: ProviderInvocation<'_>,
    ) -> Result<(), String> {
        self.with_connection(|connection| {
            connection.execute(
                "INSERT INTO provider_invocations (execution_id, node_id, provider, model_requested, model_reported, usage_json, raw_json, created_at, span_id, usage_certainty) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![invocation.execution_id, invocation.node_id, invocation.provider, invocation.model_requested, invocation.model_reported, invocation.usage.map(serde_json::to_string).transpose().map_err(|error| error.to_string())?, serde_json::to_string(invocation.raw).map_err(|error| error.to_string())?, now()?, invocation.span_id, if invocation.usage.is_some() { "reported" } else { "notReported" }],
            ).map_err(database_error)?;
            Ok(())
        })
    }

    pub fn start_trace_span(
        &self,
        execution_id: &str,
        parent_span_id: Option<&str>,
        label: (&str, &str),
        node_id: Option<&str>,
        attempt: Option<u32>,
        attributes: &Value,
    ) -> Result<String, String> {
        let (kind, name) = label;
        let id = Uuid::new_v4().to_string();
        self.with_connection(|connection| {
            connection.execute("INSERT INTO execution_trace_spans (id, execution_id, parent_span_id, kind, node_id, name, attempt, status, started_at, attributes_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'running', ?8, ?9)", params![id, execution_id, parent_span_id, kind, node_id, name, attempt, now()?, serde_json::to_string(attributes).map_err(|error| error.to_string())?]).map_err(database_error)?;
            Ok(())
        })?;
        Ok(id)
    }

    pub fn finish_trace_span(
        &self,
        id: &str,
        status: &str,
        error: Option<&str>,
    ) -> Result<(), String> {
        self.with_connection(|connection| { connection.execute("UPDATE execution_trace_spans SET status = ?2, completed_at = ?3, error = ?4 WHERE id = ?1", params![id, status, now()?, error]).map_err(database_error)?; Ok(()) })
    }

    pub fn execution_trace(
        &self,
        execution_id: &str,
    ) -> Result<Option<ExecutionTraceRecord>, String> {
        let Some(execution) = self.get_execution(execution_id)? else {
            return Ok(None);
        };
        self.with_connection(|connection| {
            let (workflow_revision, plan_hash): (String, String) = connection.query_row("SELECT workflow_revision, plan_hash FROM execution_plans WHERE execution_id = ?1", [execution_id], |row| Ok((row.get(0)?, row.get(1)?))).map_err(database_error)?;
            let workflow_source: String = connection.query_row("SELECT source FROM workflow_revisions WHERE id = ?1", [&workflow_revision], |row| row.get(0)).map_err(database_error)?;
            let mut statement = connection.prepare("SELECT id, parent_span_id, kind, node_id, name, attempt, status, started_at, completed_at, error, attributes_json FROM execution_trace_spans WHERE execution_id = ?1 ORDER BY started_at, id").map_err(database_error)?;
            let spans = statement.query_map([execution_id], |row| Ok(TraceSpanRecord { id: row.get(0)?, parent_span_id: row.get(1)?, kind: row.get(2)?, node_id: row.get(3)?, name: row.get(4)?, attempt: row.get(5)?, status: row.get(6)?, started_at: row.get(7)?, completed_at: row.get(8)?, error: row.get(9)?, attributes: serde_json::from_str(&row.get::<_, String>(10)?).unwrap_or(Value::Object(Default::default())) })).map_err(database_error)?.collect::<Result<Vec<_>, _>>().map_err(database_error)?;
            let mut statement = connection.prepare("SELECT span_id, node_id, provider, model_requested, model_reported, usage_json, usage_certainty, created_at FROM provider_invocations WHERE execution_id = ?1 ORDER BY id").map_err(database_error)?;
            let provider_invocations = statement.query_map([execution_id], |row| Ok(ProviderInvocationRecord { span_id: row.get(0)?, node_id: row.get(1)?, provider: row.get(2)?, model_requested: row.get(3)?, model_reported: row.get(4)?, usage: row.get::<_, Option<String>>(5)?.and_then(|json| serde_json::from_str(&json).ok()), usage_certainty: row.get(6)?, cost_certainty: "unknown".to_string(), created_at: row.get(7)? })).map_err(database_error)?.collect::<Result<Vec<_>, _>>().map_err(database_error)?;
            let mut statement = connection.prepare("SELECT node_id, artifact_id FROM execution_artifacts WHERE execution_id = ?1 ORDER BY node_id, artifact_id").map_err(database_error)?;
            let artifacts = statement.query_map([execution_id], |row| Ok(TraceArtifactLink { node_id: row.get(0)?, artifact_id: row.get(1)? })).map_err(database_error)?.collect::<Result<Vec<_>, _>>().map_err(database_error)?;
            Ok(Some(ExecutionTraceRecord { execution, workflow_revision, workflow_source, plan_hash, spans, provider_usage: summarize_provider_usage(&provider_invocations), provider_invocations, artifacts }))
        })
    }

    pub fn list_workflow_revisions(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<WorkflowRevisionRecord>, String> {
        self.with_connection(|connection| { let mut statement = connection.prepare("SELECT id, created_at FROM workflow_revisions WHERE workflow_id = ?1 ORDER BY created_at DESC").map_err(database_error)?; statement.query_map([workflow_id], |row| Ok(WorkflowRevisionRecord { id: row.get(0)?, created_at: row.get(1)? })).map_err(database_error)?.collect::<Result<Vec<_>, _>>().map_err(database_error) })
    }

    pub fn workflow_revision(
        &self,
        workflow_id: &str,
        revision_id: &str,
    ) -> Result<Option<WorkflowRevisionSource>, String> {
        self.with_connection(|connection| connection.query_row("SELECT id, created_at, source FROM workflow_revisions WHERE workflow_id = ?1 AND id = ?2", params![workflow_id, revision_id], |row| Ok(WorkflowRevisionSource { id: row.get(0)?, created_at: row.get(1)?, source: row.get(2)? })).optional().map_err(database_error))
    }

    pub fn compare_workflow_revisions(
        &self,
        workflow_id: &str,
        base: &str,
        head: &str,
    ) -> Result<Option<WorkflowRevisionComparison>, String> {
        let Some(base) = self.workflow_revision(workflow_id, base)? else {
            return Ok(None);
        };
        let Some(head) = self.workflow_revision(workflow_id, head)? else {
            return Ok(None);
        };
        Ok(Some(WorkflowRevisionComparison { base, head }))
    }

    /// Persists the normalized result for a succeeded node or its error for a failed node.
    pub fn record_node_outcome(
        &self,
        execution_id: &str,
        node_id: &str,
        status: &str,
        message: Option<&str>,
        result: Option<&Value>,
        error: Option<&str>,
    ) -> Result<(), String> {
        let event_result = result.cloned();
        let artifact_ids = result.map(artifact_ids_in_value).unwrap_or_default();
        let result =
            result
                .map(serde_json::to_string)
                .transpose()
                .map_err(|serialization_error| {
                    format!("cannot serialize node result: {serialization_error}")
                })?;
        self.with_connection(|connection| {
            let transaction = connection.transaction().map_err(database_error)?;
            let exists: bool = transaction.query_row("SELECT EXISTS(SELECT 1 FROM node_runs WHERE execution_id=?1 AND node_id=?2 AND status=?3 AND result IS ?4 AND error IS ?5)",params![execution_id,node_id,status,result,error],|row|row.get(0)).map_err(database_error)?;
            if exists { return Ok(()); }
            transaction
                .execute(
                    "INSERT INTO node_runs (execution_id, node_id, status, message, result, error, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![execution_id, node_id, status, message, result, error, now()?],
                )
                .map_err(database_error)?;
            for artifact_id in &artifact_ids {
                transaction.execute(
                    "INSERT OR IGNORE INTO execution_artifacts (execution_id, node_id, artifact_id) VALUES (?1, ?2, ?3)",
                    params![execution_id, node_id, artifact_id],
                ).map_err(database_error)?;
            }
            append_event(
                &transaction,
                "execution.node.outcome",
                node_id,
                Some(execution_id),
                serde_json::json!({
                    "nodeId": node_id,
                    "status": status,
                    "message": message,
                    "result": event_result,
                    "error": error,
                }),
                &now()?,
            )?;
            transaction.commit().map_err(database_error)?;
            Ok(())
        })
    }

    /// Returns the retained durable events strictly after `cursor` in sequence order.
    pub fn list_events_after(&self, cursor: Option<&str>) -> Result<Vec<EventRecord>, String> {
        self.list_events_after_limited(cursor, 10_000)
    }

    /// Returns a bounded page of retained events strictly after `cursor`.
    /// SSE uses this primitive for both catch-up and live polling, preventing an
    /// old cursor from materializing an unbounded history in one response.
    pub fn list_events_after_limited(
        &self,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<EventRecord>, String> {
        if limit == 0 || limit > 10_000 {
            return Err("event page limit must be between 1 and 10000".to_string());
        }
        self.with_connection(|connection| {
            let sequence = cursor
                .filter(|cursor| !cursor.is_empty())
                .map(|cursor| {
                    connection
                        .query_row(
                            "SELECT sequence FROM events WHERE event_id = ?1",
                            [cursor],
                            |row| row.get::<_, i64>(0),
                        )
                        .optional()
                        .map_err(database_error)?
                        .ok_or_else(|| format!("event cursor {cursor} is not retained"))
                })
                .transpose()?;
            let mut statement = connection
                .prepare(
                    "SELECT CASE event_version WHEN 1 THEN '1.0' ELSE CAST(event_version AS TEXT) END, core_id, event_id, sequence, timestamp, type, resource_id,
                            execution_id, payload
                     FROM events
                     WHERE sequence > COALESCE(?1, 0)
                     ORDER BY sequence ASC
                     LIMIT ?2",
                )
                .map_err(database_error)?;
            let rows = statement
                .query_map(params![sequence, limit as i64], event_from_row)
                .map_err(database_error)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(database_error)
        })
    }

    pub fn list_executions(&self) -> Result<Vec<ExecutionRecord>, String> {
        self.with_connection(|connection| {
            let mut statement = connection
                .prepare(
                    "SELECT id, workflow_name, status, created_at, completed_at, error
                     FROM executions ORDER BY created_at DESC",
                )
                .map_err(database_error)?;
            let rows = statement
                .query_map([], execution_from_row)
                .map_err(database_error)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(database_error)
        })
    }

    pub fn get_execution(&self, id: &str) -> Result<Option<ExecutionRecord>, String> {
        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT id, workflow_name, status, created_at, completed_at, error FROM executions WHERE id = ?1",
                    [id],
                    execution_from_row,
                )
                .optional()
                .map_err(database_error)
        })
    }

    pub fn list_node_runs(&self, execution_id: &str) -> Result<Vec<NodeRunRecord>, String> {
        self.with_connection(|connection| {
            let mut statement = connection
                .prepare("SELECT node_id, status, message, result, error FROM node_runs WHERE execution_id = ?1 ORDER BY id")
                .map_err(database_error)?;
            let rows = statement
                .query_map([execution_id], |row| {
                    Ok(NodeRunRecord {
                        node_id: row.get(0)?,
                        status: row.get(1)?,
                        message: row.get(2)?,
                        result: row
                            .get::<_, Option<String>>(3)?
                            .map(|value| serde_json::from_str(&value))
                            .transpose()
                            .map_err(|error| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    3,
                                    rusqlite::types::Type::Text,
                                    Box::new(error),
                                )
                            })?,
                        error: row.get(4)?,
                    })
                })
                .map_err(database_error)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(database_error)
        })
    }

    fn with_connection<T>(
        &self,
        operation: impl FnOnce(&mut Connection) -> Result<T, String>,
    ) -> Result<T, String> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| "Kakune database lock was poisoned".to_string())?;
        operation(&mut connection)
    }
}

fn execution_admission(
    transaction: &rusqlite::Transaction<'_>,
    workflow_name: &str,
    concurrency: Option<&ConcurrencyPolicy>,
) -> Result<String, String> {
    let Some(concurrency) = concurrency else {
        return Ok("running".to_string());
    };
    let running: u32 = transaction.query_row(
        "SELECT COUNT(*) FROM executions WHERE workflow_name = ?1 AND status IN ('running', 'cancelling')",
        [workflow_name], |row| row.get(0),
    ).map_err(database_error)?;
    if running < concurrency.max_runs.unwrap_or(u32::MAX) {
        return Ok("running".to_string());
    }
    if !matches!(concurrency.overflow, Some(OverflowPolicy::Queue)) {
        return Err(format!("workflow {workflow_name} is at its maxRuns limit"));
    }
    let queued: u32 = transaction
        .query_row(
            "SELECT COUNT(*) FROM executions WHERE workflow_name = ?1 AND status = 'queued'",
            [workflow_name],
            |row| row.get(0),
        )
        .map_err(database_error)?;
    if queued >= concurrency.max_queued.unwrap_or(0) {
        return Err(format!("workflow {workflow_name} queue is full"));
    }
    Ok("queued".to_string())
}

const LATEST_SCHEMA_VERSION: u32 = 14;

fn migrate(
    connection: &mut Connection,
    data_dir: &Path,
    database_existed: bool,
) -> Result<(), String> {
    connection
        .execute_batch("CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY)")
        .map_err(database_error)?;
    let current = schema_version(connection)?;
    if current > LATEST_SCHEMA_VERSION {
        return Err(format!(
            "Kakune database schema version {current} is newer than this Core supports ({LATEST_SCHEMA_VERSION})"
        ));
    }
    if current < LATEST_SCHEMA_VERSION && database_existed {
        backup_before_migration(connection, data_dir, current + 1)?;
    }
    for version in current + 1..=LATEST_SCHEMA_VERSION {
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error)?;
        match version {
            1 => create_initial_schema(&transaction)?,
            2 => upgrade_legacy_schema(&transaction)?,
            3 => add_execution_materials(&transaction)?,
            4 => add_execution_artifact_links(&transaction)?,
            5 => add_scheduled_trigger_state(&transaction)?,
            6 => add_plugin_installation_materials(&transaction)?,
            7 => add_ai_accounting(&transaction)?,
            8 => add_provider_profiles(&transaction)?,
            9 => migrate_codex_oauth_profiles(&transaction)?,
            10 => add_execution_traces(&transaction)?,
            11 => add_retention_fields(&transaction)?,
            12 => add_secret_backend(&transaction)?,
            14 => transaction.execute_batch("CREATE TABLE execution_idempotency (request_key TEXT PRIMARY KEY, request_hash TEXT NOT NULL, execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE CASCADE);").map_err(database_error)?,
            13 => transaction
                .execute_batch(
                    "CREATE TABLE node_checkpoints (
                execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
                node_id TEXT NOT NULL, status TEXT NOT NULL, result_json TEXT, deadline_ms INTEGER,
                PRIMARY KEY(execution_id, node_id));
                CREATE TABLE execution_deadlines (
                execution_id TEXT PRIMARY KEY REFERENCES executions(id) ON DELETE CASCADE,
                deadline_ms INTEGER NOT NULL);",
                )
                .map_err(database_error)?,
            _ => unreachable!("schema version range is bounded by LATEST_SCHEMA_VERSION"),
        }
        transaction
            .execute(
                "INSERT INTO schema_migrations (version) VALUES (?1)",
                [version],
            )
            .map_err(database_error)?;
        transaction.commit().map_err(database_error)?;
    }
    Ok(())
}

fn schema_version(connection: &Connection) -> Result<u32, String> {
    let version: i64 = connection
        .query_row(
            "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
            [],
            |row| row.get(0),
        )
        .map_err(database_error)?;
    u32::try_from(version).map_err(|_| "database schema version is invalid".to_string())
}

fn count_rows(connection: &Connection, table: &str) -> Result<u64, String> {
    let value: i64 = connection
        .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .map_err(database_error)?;
    u64::try_from(value).map_err(|_| format!("{table} count is invalid"))
}

fn count_where(connection: &Connection, table: &str, predicate: &str) -> Result<u64, String> {
    let value: i64 = connection
        .query_row(
            &format!("SELECT COUNT(*) FROM {table} WHERE {predicate}"),
            [],
            |row| row.get(0),
        )
        .map_err(database_error)?;
    u64::try_from(value).map_err(|_| format!("{table} count is invalid"))
}

fn backup_before_migration(
    connection: &Connection,
    data_dir: &Path,
    next_version: u32,
) -> Result<(), String> {
    let backup_dir = data_dir.join("backups");
    fs::create_dir_all(&backup_dir)
        .map_err(|error| format!("cannot create migration backup directory: {error}"))?;
    let path = backup_dir.join(format!(
        "kakune.sqlite3.before-migration-v{next_version}.sqlite3"
    ));
    let mut destination = Connection::open(&path)
        .map_err(|error| format!("cannot create migration backup {}: {error}", path.display()))?;
    let backup =
        rusqlite::backup::Backup::new(connection, &mut destination).map_err(database_error)?;
    backup
        .run_to_completion(128, std::time::Duration::from_millis(5), None)
        .map_err(database_error)
}

fn create_initial_schema(transaction: &rusqlite::Transaction<'_>) -> Result<(), String> {
    transaction
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS workflows (
               id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, source TEXT NOT NULL,
               status TEXT NOT NULL CHECK(status IN ('enabled', 'disabled')),
               revision TEXT NOT NULL, updated_at TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS workflow_revisions (
               id TEXT PRIMARY KEY, workflow_id TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
               source TEXT NOT NULL, created_at TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS workflow_revisions_workflow_id ON workflow_revisions(workflow_id, created_at DESC);
             CREATE TABLE IF NOT EXISTS executions (
               id TEXT PRIMARY KEY, workflow_name TEXT NOT NULL, status TEXT NOT NULL,
               created_at TEXT NOT NULL, completed_at TEXT, error TEXT
             );
             CREATE INDEX IF NOT EXISTS executions_created_at ON executions(created_at DESC);
             CREATE TABLE IF NOT EXISTS node_runs (
               id INTEGER PRIMARY KEY, execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
               node_id TEXT NOT NULL, status TEXT NOT NULL, message TEXT, result TEXT, error TEXT,
               created_at TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS node_runs_execution_id ON node_runs(execution_id, id);
             CREATE TABLE IF NOT EXISTS auth_tokens (
               id TEXT PRIMARY KEY, token_hash TEXT NOT NULL UNIQUE,
               name TEXT NOT NULL DEFAULT 'Bootstrap token', scopes TEXT NOT NULL DEFAULT '[\"admin\"]',
               created_at TEXT NOT NULL, expires_at TEXT, revoked_at TEXT
             );
             CREATE TABLE IF NOT EXISTS secrets (
               name TEXT PRIMARY KEY, nonce BLOB NOT NULL, ciphertext BLOB NOT NULL, updated_at TEXT NOT NULL
             );
              CREATE TABLE IF NOT EXISTS installed_plugins (
                id TEXT PRIMARY KEY, name TEXT NOT NULL, version TEXT NOT NULL, manifest_path TEXT NOT NULL,
                installed_at TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1,
                policy_json TEXT NOT NULL DEFAULT '{}', digest TEXT NOT NULL DEFAULT '',
                lock_json TEXT NOT NULL DEFAULT '{}', provenance_json TEXT NOT NULL DEFAULT '{}'
              );
              CREATE TABLE IF NOT EXISTS prepared_plugin_installs (
                id TEXT PRIMARY KEY, plugin_id TEXT NOT NULL, plugin_name TEXT NOT NULL, version TEXT NOT NULL,
                digest TEXT NOT NULL, staged_path TEXT NOT NULL, prepared_at TEXT NOT NULL,
                policy_json TEXT NOT NULL, lock_json TEXT NOT NULL, provenance_json TEXT NOT NULL
              );
             CREATE TABLE IF NOT EXISTS scheduled_triggers (
               workflow_id TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE, trigger_id TEXT NOT NULL,
               next_run_at TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY (workflow_id, trigger_id)
             );
             CREATE TABLE IF NOT EXISTS core_state (
               id INTEGER PRIMARY KEY CHECK(id = 1), core_id TEXT NOT NULL UNIQUE,
               event_sequence INTEGER NOT NULL DEFAULT 0 CHECK(event_sequence >= 0)
             );
              CREATE TABLE IF NOT EXISTS events (
               event_id TEXT PRIMARY KEY, event_version TEXT NOT NULL,
               core_id TEXT NOT NULL REFERENCES core_state(core_id), sequence INTEGER NOT NULL UNIQUE CHECK(sequence > 0),
               timestamp TEXT NOT NULL, type TEXT NOT NULL, resource_id TEXT NOT NULL,
                execution_id TEXT, payload TEXT NOT NULL
              );
              CREATE INDEX IF NOT EXISTS events_sequence ON events(sequence);
              CREATE TABLE IF NOT EXISTS execution_plans (
                execution_id TEXT PRIMARY KEY REFERENCES executions(id) ON DELETE CASCADE,
                workflow_revision TEXT NOT NULL, plan_json TEXT NOT NULL, plan_hash TEXT NOT NULL,
                created_at TEXT NOT NULL
              );
              CREATE TABLE IF NOT EXISTS execution_results (
                execution_id TEXT PRIMARY KEY REFERENCES executions(id) ON DELETE CASCADE,
                result_json TEXT NOT NULL, updated_at TEXT NOT NULL
              );
              CREATE TABLE IF NOT EXISTS artifacts (
                id TEXT PRIMARY KEY, sha256 TEXT NOT NULL UNIQUE, size_bytes INTEGER NOT NULL,
                media_type TEXT NOT NULL, name TEXT, created_at TEXT NOT NULL
              );
              CREATE TABLE IF NOT EXISTS execution_artifacts (
                execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
                node_id TEXT NOT NULL, artifact_id TEXT NOT NULL REFERENCES artifacts(id),
                PRIMARY KEY (execution_id, node_id, artifact_id)
              );",
        )
        .map_err(database_error)?;
    transaction
        .execute(
            "INSERT OR IGNORE INTO core_state (id, core_id, event_sequence) VALUES (1, ?1, 0)",
            [Uuid::new_v4().to_string()],
        )
        .map_err(database_error)?;
    Ok(())
}

fn upgrade_legacy_schema(transaction: &rusqlite::Transaction<'_>) -> Result<(), String> {
    ensure_workflow_revision_column(transaction)?;
    ensure_node_run_columns(transaction)?;
    migrate_auth_tokens(transaction)?;
    ensure_installed_plugin_columns(transaction)
}

fn add_execution_materials(transaction: &rusqlite::Transaction<'_>) -> Result<(), String> {
    transaction
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS execution_plans (
                execution_id TEXT PRIMARY KEY REFERENCES executions(id) ON DELETE CASCADE,
                workflow_revision TEXT NOT NULL, plan_json TEXT NOT NULL, plan_hash TEXT NOT NULL,
                created_at TEXT NOT NULL
              );
              CREATE TABLE IF NOT EXISTS execution_results (
                execution_id TEXT PRIMARY KEY REFERENCES executions(id) ON DELETE CASCADE,
                result_json TEXT NOT NULL, updated_at TEXT NOT NULL
              );
              CREATE TABLE IF NOT EXISTS artifacts (
                id TEXT PRIMARY KEY, sha256 TEXT NOT NULL UNIQUE, size_bytes INTEGER NOT NULL,
                media_type TEXT NOT NULL, name TEXT, created_at TEXT NOT NULL
              );",
        )
        .map_err(database_error)
}

fn add_execution_artifact_links(transaction: &rusqlite::Transaction<'_>) -> Result<(), String> {
    transaction
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS execution_artifacts (
            execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
            node_id TEXT NOT NULL, artifact_id TEXT NOT NULL REFERENCES artifacts(id),
            PRIMARY KEY (execution_id, node_id, artifact_id)
          );",
        )
        .map_err(database_error)
}

fn add_ai_accounting(transaction: &rusqlite::Transaction<'_>) -> Result<(), String> {
    transaction.execute_batch(
        "CREATE TABLE IF NOT EXISTS provider_token_reservations (
            id INTEGER PRIMARY KEY, execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
            provider TEXT NOT NULL, reserved_tokens INTEGER NOT NULL CHECK(reserved_tokens > 0), created_at TEXT NOT NULL
          );
          CREATE INDEX IF NOT EXISTS provider_token_reservations_execution ON provider_token_reservations(execution_id, provider);
          CREATE TABLE IF NOT EXISTS provider_invocations (
            id INTEGER PRIMARY KEY, execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
            node_id TEXT NOT NULL, provider TEXT NOT NULL, model_requested TEXT NOT NULL,
            model_reported TEXT, usage_json TEXT, raw_json TEXT NOT NULL, created_at TEXT NOT NULL
          );
          CREATE INDEX IF NOT EXISTS provider_invocations_execution ON provider_invocations(execution_id, id);",
    ).map_err(database_error)
}

fn add_execution_traces(transaction: &rusqlite::Transaction<'_>) -> Result<(), String> {
    transaction.execute_batch(
        "CREATE TABLE execution_trace_spans (
            id TEXT PRIMARY KEY, execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
            parent_span_id TEXT REFERENCES execution_trace_spans(id) ON DELETE CASCADE,
            kind TEXT NOT NULL CHECK(kind IN ('node', 'provider', 'tool')),
            node_id TEXT, name TEXT NOT NULL, attempt INTEGER,
            status TEXT NOT NULL CHECK(status IN ('running', 'succeeded', 'failed', 'cancelled')),
            started_at TEXT NOT NULL, completed_at TEXT, error TEXT,
            attributes_json TEXT NOT NULL DEFAULT '{}'
          );
          CREATE INDEX execution_trace_spans_execution_started ON execution_trace_spans(execution_id, started_at, id);
          ALTER TABLE provider_invocations ADD COLUMN span_id TEXT REFERENCES execution_trace_spans(id) ON DELETE SET NULL;
          ALTER TABLE provider_invocations ADD COLUMN usage_certainty TEXT NOT NULL DEFAULT 'notReported';",
    ).map_err(database_error)
}

fn add_retention_fields(transaction: &rusqlite::Transaction<'_>) -> Result<(), String> {
    let has_executions: bool = transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'executions')",
            [],
            |row| row.get(0),
        )
        .map_err(database_error)?;
    if !has_executions {
        return Ok(());
    }
    transaction
        .execute(
            "ALTER TABLE executions ADD COLUMN pinned INTEGER NOT NULL DEFAULT 0",
            [],
        )
        .map(|_| ())
        .map_err(database_error)
}

fn add_secret_backend(transaction: &rusqlite::Transaction<'_>) -> Result<(), String> {
    let has_secrets: bool = transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'secrets')",
            [],
            |row| row.get(0),
        )
        .map_err(database_error)?;
    if !has_secrets {
        return Ok(());
    }
    transaction
        .execute(
            "ALTER TABLE secrets ADD COLUMN backend TEXT NOT NULL DEFAULT 'vault' CHECK(backend IN ('native', 'vault', 'native-with-vault-backup'))",
            [],
        )
        .map(|_| ())
        .map_err(database_error)
}

fn summarize_provider_usage(invocations: &[ProviderInvocationRecord]) -> Vec<ProviderUsageSummary> {
    use std::collections::BTreeMap;
    let mut summaries: BTreeMap<(String, String), (u64, u64, u64, bool)> = BTreeMap::new();
    for invocation in invocations {
        let Some(usage) = invocation.usage.as_ref() else {
            continue;
        };
        let entry = summaries
            .entry((
                invocation.provider.clone(),
                invocation
                    .model_reported
                    .clone()
                    .unwrap_or_else(|| invocation.model_requested.clone()),
            ))
            .or_default();
        entry.0 = entry.0.saturating_add(usage_number(usage, "inputTokens"));
        entry.1 = entry.1.saturating_add(usage_number(usage, "outputTokens"));
        entry.2 = entry.2.saturating_add(usage_number(usage, "totalTokens"));
        entry.3 = true;
    }
    summaries
        .into_iter()
        .map(
            |((provider, model), (input, output, total, reported))| ProviderUsageSummary {
                provider,
                model,
                input_tokens: reported.then_some(input),
                output_tokens: reported.then_some(output),
                total_tokens: reported.then_some(total),
                usage_certainty: if reported { "reported" } else { "notReported" }.to_string(),
                cost_certainty: "unknown".to_string(),
            },
        )
        .collect()
}

fn usage_number(value: &Value, key: &str) -> u64 {
    match value {
        Value::Array(values) => values.iter().map(|item| usage_number(item, key)).sum(),
        Value::Object(_) => value.get(key).and_then(Value::as_u64).unwrap_or(0),
        _ => 0,
    }
}

fn add_provider_profiles(transaction: &rusqlite::Transaction<'_>) -> Result<(), String> {
    transaction
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS provider_profiles (
                id TEXT PRIMARY KEY,
                display_name TEXT NOT NULL,
                provider_type TEXT NOT NULL CHECK(provider_type IN ('minimax', 'codex')),
                default_model TEXT NOT NULL,
                allowed_models_json TEXT NOT NULL,
                capabilities_json TEXT NOT NULL,
                auth_mode TEXT NOT NULL CHECK(auth_mode IN ('api_key_secret', 'oauth_secret')),
                secret_ref TEXT,
                config_json TEXT NOT NULL,
                diagnostic_status TEXT NOT NULL CHECK(diagnostic_status IN ('unknown', 'available', 'unavailable')),
                diagnostic_checked_at TEXT,
                diagnostic_message TEXT,
                diagnostic_details_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                CHECK((auth_mode IN ('api_key_secret', 'oauth_secret') AND secret_ref IS NOT NULL))
             );
             CREATE INDEX IF NOT EXISTS provider_profiles_type ON provider_profiles(provider_type);",
        )
        .map_err(database_error)
}

fn migrate_codex_oauth_profiles(transaction: &rusqlite::Transaction<'_>) -> Result<(), String> {
    // SQLite CHECK constraints require rebuilding the table to retire the CLI mode.
    transaction.execute_batch(
        "CREATE TABLE provider_profiles_oauth (
            id TEXT PRIMARY KEY, display_name TEXT NOT NULL,
            provider_type TEXT NOT NULL CHECK(provider_type IN ('minimax', 'codex')),
            default_model TEXT NOT NULL, allowed_models_json TEXT NOT NULL,
            capabilities_json TEXT NOT NULL,
            auth_mode TEXT NOT NULL CHECK(auth_mode IN ('api_key_secret', 'oauth_secret')),
            secret_ref TEXT NOT NULL, config_json TEXT NOT NULL,
            diagnostic_status TEXT NOT NULL CHECK(diagnostic_status IN ('unknown', 'available', 'unavailable')),
            diagnostic_checked_at TEXT, diagnostic_message TEXT, diagnostic_details_json TEXT,
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL
          );
          INSERT INTO provider_profiles_oauth
          SELECT id, display_name, provider_type, default_model, allowed_models_json, capabilities_json,
                 CASE WHEN provider_type = 'codex' THEN 'oauth_secret' ELSE auth_mode END,
                 CASE WHEN provider_type = 'codex' THEN 'provider.' || id || '.oauth' ELSE secret_ref END,
                 config_json, diagnostic_status, diagnostic_checked_at, diagnostic_message,
                 diagnostic_details_json, created_at, updated_at
          FROM provider_profiles;
          DROP TABLE provider_profiles;
          ALTER TABLE provider_profiles_oauth RENAME TO provider_profiles;
          CREATE INDEX provider_profiles_type ON provider_profiles(provider_type);"
    ).map_err(database_error)
}

fn add_scheduled_trigger_state(transaction: &rusqlite::Transaction<'_>) -> Result<(), String> {
    transaction
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS scheduled_triggers (
               workflow_id TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE, trigger_id TEXT NOT NULL,
               next_run_at TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY (workflow_id, trigger_id)
             );",
        )
        .map_err(database_error)?;
    let mut statement = transaction
        .prepare("PRAGMA table_info(scheduled_triggers)")
        .map_err(database_error)?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(database_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(database_error)?;
    for (column, definition) in [
        ("misfire_policy", "TEXT NOT NULL DEFAULT 'runOnce'"),
        ("last_run_at", "TEXT"),
        ("completed_at", "TEXT"),
    ] {
        if !columns.iter().any(|existing| existing == column) {
            transaction
                .execute(
                    &format!("ALTER TABLE scheduled_triggers ADD COLUMN {column} {definition}"),
                    [],
                )
                .map_err(database_error)?;
        }
    }
    Ok(())
}

fn add_plugin_installation_materials(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), String> {
    ensure_installed_plugin_columns(transaction)?;
    transaction.execute_batch(
        "CREATE TABLE IF NOT EXISTS prepared_plugin_installs (
           id TEXT PRIMARY KEY, plugin_id TEXT NOT NULL, plugin_name TEXT NOT NULL, version TEXT NOT NULL,
           digest TEXT NOT NULL, staged_path TEXT NOT NULL, prepared_at TEXT NOT NULL,
           policy_json TEXT NOT NULL, lock_json TEXT NOT NULL, provenance_json TEXT NOT NULL
         );",
    ).map_err(database_error)
}

fn append_event(
    connection: &Connection,
    event_type: &str,
    resource_id: &str,
    execution_id: Option<&str>,
    payload: Value,
    timestamp: &str,
) -> Result<(), String> {
    connection
        .execute(
            "UPDATE core_state SET event_sequence = event_sequence + 1 WHERE id = 1",
            [],
        )
        .map_err(database_error)?;
    let (core_id, sequence): (String, i64) = connection
        .query_row(
            "SELECT core_id, event_sequence FROM core_state WHERE id = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(database_error)?;
    connection
        .execute(
            "INSERT INTO events (event_id, event_version, core_id, sequence, timestamp, type, resource_id,
                                 execution_id, payload)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                Uuid::new_v4().to_string(),
                "1.0",
                core_id,
                sequence,
                timestamp,
                event_type,
                resource_id,
                execution_id,
                serde_json::to_string(&payload)
                    .map_err(|error| format!("cannot serialize event payload: {error}"))?,
            ],
        )
        .map_err(database_error)?;
    Ok(())
}

fn ensure_node_run_columns(connection: &rusqlite::Transaction<'_>) -> Result<(), String> {
    let mut statement = connection
        .prepare("PRAGMA table_info(node_runs)")
        .map_err(database_error)?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(database_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(database_error)?;
    for column in ["result", "error"] {
        if !columns.iter().any(|existing| existing == column) {
            connection
                .execute(
                    &format!("ALTER TABLE node_runs ADD COLUMN {column} TEXT"),
                    [],
                )
                .map_err(database_error)?;
        }
    }
    Ok(())
}

fn ensure_workflow_revision_column(connection: &rusqlite::Transaction<'_>) -> Result<(), String> {
    let mut statement = connection
        .prepare("PRAGMA table_info(workflows)")
        .map_err(database_error)?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(database_error)?;
    let has_revision = columns
        .collect::<Result<Vec<_>, _>>()
        .map_err(database_error)?
        .iter()
        .any(|column| column == "revision");
    if !has_revision {
        connection
            .execute(
                "ALTER TABLE workflows ADD COLUMN revision TEXT NOT NULL DEFAULT ''",
                [],
            )
            .map_err(database_error)?;
        connection
            .execute(
                "UPDATE workflows SET revision = lower(hex(randomblob(16))) WHERE revision = ''",
                [],
            )
            .map_err(database_error)?;
    }
    Ok(())
}

fn migrate_auth_tokens(connection: &rusqlite::Transaction<'_>) -> Result<(), String> {
    let mut statement = connection
        .prepare("PRAGMA table_info(auth_tokens)")
        .map_err(database_error)?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(database_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(database_error)?;
    if !columns.iter().any(|column| column == "name") {
        connection
            .execute(
                "ALTER TABLE auth_tokens ADD COLUMN name TEXT NOT NULL DEFAULT 'Bootstrap token'",
                [],
            )
            .map_err(database_error)?;
    }
    if !columns.iter().any(|column| column == "scopes") {
        connection
            .execute(
                "ALTER TABLE auth_tokens ADD COLUMN scopes TEXT NOT NULL DEFAULT '[\"admin\"]'",
                [],
            )
            .map_err(database_error)?;
    }
    if !columns.iter().any(|column| column == "expires_at") {
        connection
            .execute("ALTER TABLE auth_tokens ADD COLUMN expires_at TEXT", [])
            .map_err(database_error)?;
    }
    Ok(())
}

fn create_auth_token(
    connection: &Connection,
    name: String,
    scopes: Vec<AuthScope>,
    expires_at: Option<String>,
) -> Result<CreatedAuthToken, String> {
    let name = name.trim();
    if name.is_empty() {
        return Err("token name must not be empty".to_string());
    }
    if name.len() > 120 {
        return Err("token name must be at most 120 characters".to_string());
    }
    let scopes = unique_scopes(scopes)?;
    let expires_at = normalize_expiration(expires_at)?;
    let created_at = now()?;
    let id = Uuid::new_v4().to_string();
    let token = format!("kakune_{}", Uuid::new_v4().simple());
    connection
        .execute(
            "INSERT INTO auth_tokens (id, token_hash, name, scopes, created_at, expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                id,
                token_hash(&token),
                name,
                serde_json::to_string(&scopes)
                    .map_err(|error| format!("cannot serialize token scopes: {error}"))?,
                created_at,
                expires_at,
            ],
        )
        .map_err(database_error)?;
    Ok(CreatedAuthToken {
        token,
        record: AuthTokenRecord {
            id,
            name: name.to_string(),
            scopes,
            created_at,
            expires_at,
            revoked_at: None,
        },
    })
}

fn unique_scopes(scopes: Vec<AuthScope>) -> Result<Vec<AuthScope>, String> {
    if scopes.is_empty() {
        return Err("at least one token scope is required".to_string());
    }
    let mut unique = Vec::with_capacity(scopes.len());
    for scope in scopes {
        if !unique.contains(&scope) {
            unique.push(scope);
        }
    }
    Ok(unique)
}

fn parse_scopes(scopes: &str) -> Result<Vec<AuthScope>, String> {
    let scopes = serde_json::from_str(scopes)
        .map_err(|error| format!("cannot decode persisted token scopes: {error}"))?;
    unique_scopes(scopes)
}

fn normalize_expiration(expires_at: Option<String>) -> Result<Option<String>, String> {
    let Some(expires_at) = expires_at else {
        return Ok(None);
    };
    let expires_at = OffsetDateTime::parse(&expires_at, &Rfc3339)
        .map_err(|_| "expiresAt must be an RFC 3339 timestamp".to_string())?;
    if expires_at <= OffsetDateTime::now_utc() {
        return Err("expiresAt must be in the future".to_string());
    }
    expires_at
        .format(&Rfc3339)
        .map(Some)
        .map_err(|error| format!("cannot format token expiration: {error}"))
}

fn is_expired(expires_at: Option<&str>) -> Result<bool, String> {
    let Some(expires_at) = expires_at else {
        return Ok(false);
    };
    let expires_at = OffsetDateTime::parse(expires_at, &Rfc3339)
        .map_err(|_| "cannot decode persisted token expiration".to_string())?;
    Ok(expires_at <= OffsetDateTime::now_utc())
}

fn auth_token_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<AuthTokenRecord> {
    let scopes = row.get::<_, String>(2)?;
    let scopes = parse_scopes(&scopes).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            2,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, error)),
        )
    })?;
    Ok(AuthTokenRecord {
        id: row.get(0)?,
        name: row.get(1)?,
        scopes,
        created_at: row.get(3)?,
        expires_at: row.get(4)?,
        revoked_at: row.get(5)?,
    })
}

fn token_hash(token: &str) -> String {
    let digest = Sha256::digest(token.as_bytes());
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn get_provider_profile(
    connection: &Connection,
    id: &str,
) -> Result<Option<ProviderProfile>, String> {
    connection
        .query_row(
            "SELECT id, display_name, provider_type, default_model, allowed_models_json,
                    capabilities_json, auth_mode, secret_ref, config_json, diagnostic_status,
                    diagnostic_checked_at, diagnostic_message, diagnostic_details_json, created_at, updated_at
             FROM provider_profiles WHERE id = ?1",
            [id],
            provider_profile_from_row,
        )
        .optional()
        .map_err(database_error)
}

fn provider_profile_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ProviderProfile> {
    let provider_type = match row.get::<_, String>(2)?.as_str() {
        "minimax" => ProviderType::MiniMax,
        "codex" => ProviderType::Codex,
        value => {
            return Err(invalid_persisted_value(
                2,
                format!("unknown provider type {value}"),
            ));
        }
    };
    let secret_ref = row.get::<_, Option<String>>(7)?;
    let auth = match row.get::<_, String>(6)?.as_str() {
        "api_key_secret" => ProviderAuth::ApiKeySecret {
            secret_ref: secret_ref.ok_or_else(|| {
                invalid_persisted_value(
                    7,
                    "API-key provider profile is missing its secret reference",
                )
            })?,
        },
        "oauth_secret" => ProviderAuth::OAuthSecret {
            secret_ref: secret_ref.ok_or_else(|| {
                invalid_persisted_value(7, "OAuth provider profile is missing its secret reference")
            })?,
        },
        "managed_codex_cli_login" => {
            return Err(invalid_persisted_value(
                6,
                "managed Codex CLI login is retired; reopen the database to run migration 9",
            ));
        }
        value => {
            return Err(invalid_persisted_value(
                6,
                format!("unknown provider auth mode {value}"),
            ));
        }
    };
    let status = match row.get::<_, String>(9)?.as_str() {
        "unknown" => ProviderProfileStatus::Unknown,
        "available" => ProviderProfileStatus::Available,
        "unavailable" => ProviderProfileStatus::Unavailable,
        value => {
            return Err(invalid_persisted_value(
                9,
                format!("unknown provider diagnostic status {value}"),
            ));
        }
    };
    Ok(ProviderProfile {
        id: row.get(0)?,
        display_name: row.get(1)?,
        provider_type,
        default_model: row.get(3)?,
        allowed_models: string_list_from_column(row, 4)?,
        capabilities: string_list_from_column(row, 5)?,
        auth,
        config: json_from_column(row, 8)?,
        diagnostic: ProviderProfileDiagnostic {
            status,
            checked_at: row.get(10)?,
            message: row.get(11)?,
            details: optional_json_from_column(row, 12)?,
        },
        created_at: row.get(13)?,
        updated_at: row.get(14)?,
    })
}

fn provider_profile_values(
    profile: &ProviderProfileUpsert,
) -> Result<(String, String, String, Option<String>, String), String> {
    let (auth_mode, secret_ref) = match &profile.auth {
        ProviderAuth::ApiKeySecret { secret_ref } => ("api_key_secret", Some(secret_ref.clone())),
        ProviderAuth::OAuthSecret { secret_ref } => ("oauth_secret", Some(secret_ref.clone())),
    };
    Ok((
        serde_json::to_string(&profile.allowed_models)
            .map_err(|error| format!("cannot serialize provider allowed models: {error}"))?,
        serde_json::to_string(&profile.capabilities)
            .map_err(|error| format!("cannot serialize provider capabilities: {error}"))?,
        auth_mode.to_string(),
        secret_ref,
        serde_json::to_string(&profile.config)
            .map_err(|error| format!("cannot serialize provider configuration: {error}"))?,
    ))
}

fn provider_type_name(provider_type: &ProviderType) -> &'static str {
    match provider_type {
        ProviderType::MiniMax => "minimax",
        ProviderType::Codex => "codex",
    }
}

fn provider_status_name(status: &ProviderProfileStatus) -> &'static str {
    match status {
        ProviderProfileStatus::Unknown => "unknown",
        ProviderProfileStatus::Available => "available",
        ProviderProfileStatus::Unavailable => "unavailable",
    }
}

fn normalize_provider_profile(
    mut profile: ProviderProfileUpsert,
) -> Result<ProviderProfileUpsert, String> {
    validate_profile_id(&profile.id)?;
    profile.display_name = normalize_text(profile.display_name, "provider display name", 120)?;
    profile.default_model = normalize_text(profile.default_model, "provider default model", 200)?;
    if profile.allowed_models.is_empty() {
        return Err("provider allowed models must not be empty".to_string());
    }
    normalize_string_list(&mut profile.allowed_models, "provider allowed model", 200)?;
    if !profile
        .allowed_models
        .iter()
        .any(|model| model == &profile.default_model)
    {
        return Err("provider default model must be included in allowed models".to_string());
    }
    if profile.capabilities.is_empty() {
        return Err("provider capabilities must not be empty".to_string());
    }
    normalize_string_list(&mut profile.capabilities, "provider capability", 120)?;
    if !profile.config.is_object() {
        return Err("provider configuration must be a JSON object".to_string());
    }
    validate_non_secret_json(&profile.config, "provider configuration")?;
    match (&profile.provider_type, &profile.auth) {
        (ProviderType::MiniMax, ProviderAuth::ApiKeySecret { secret_ref }) => {
            validate_secret_name(secret_ref)?;
        }
        (ProviderType::MiniMax, ProviderAuth::OAuthSecret { .. }) => {
            return Err("MiniMax profiles require an API-key secret reference".to_string());
        }
        (ProviderType::Codex, ProviderAuth::ApiKeySecret { .. }) => {
            return Err("Codex profiles require a ChatGPT OAuth secret reference".to_string());
        }
        (ProviderType::Codex, ProviderAuth::OAuthSecret { secret_ref }) => {
            validate_secret_name(secret_ref)?
        }
    }
    Ok(profile)
}

fn validate_provider_diagnostic(diagnostic: &ProviderProfileDiagnostic) -> Result<(), String> {
    if diagnostic
        .message
        .as_deref()
        .is_some_and(|message| message.trim().is_empty() || message.len() > 4096)
    {
        return Err(
            "provider diagnostic message must be 1-4096 characters when present".to_string(),
        );
    }
    if let Some(details) = &diagnostic.details {
        validate_non_secret_json(details, "provider diagnostic details")?;
    }
    Ok(())
}

fn validate_profile_id(id: &str) -> Result<(), String> {
    if id.is_empty()
        || id.len() > 120
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(
            "provider profile ID must be 1-120 ASCII letters, digits, '.', '_' or '-'".to_string(),
        );
    }
    Ok(())
}

fn normalize_text(value: String, field: &str, maximum_length: usize) -> Result<String, String> {
    let value = value.trim();
    if value.is_empty() || value.len() > maximum_length {
        return Err(format!("{field} must be 1-{maximum_length} characters"));
    }
    Ok(value.to_string())
}

fn normalize_string_list(
    values: &mut [String],
    field: &str,
    maximum_length: usize,
) -> Result<(), String> {
    for value in values.iter_mut() {
        *value = normalize_text(std::mem::take(value), field, maximum_length)?;
    }
    let mut unique = values.to_owned();
    unique.sort();
    unique.dedup();
    if unique.len() != values.len() {
        return Err(format!("{field}s must not contain duplicates"));
    }
    Ok(())
}

fn validate_non_secret_json(value: &Value, field: &str) -> Result<(), String> {
    fn contains_sensitive_field(value: &Value) -> bool {
        match value {
            Value::Array(values) => values.iter().any(contains_sensitive_field),
            Value::Object(values) => values.iter().any(|(name, value)| {
                let normalized = name
                    .bytes()
                    .filter(u8::is_ascii_alphanumeric)
                    .map(char::from)
                    .collect::<String>()
                    .to_ascii_lowercase();
                matches!(
                    normalized.as_str(),
                    "apikey"
                        | "authorization"
                        | "password"
                        | "secret"
                        | "session"
                        | "cookie"
                        | "credential"
                        | "credentials"
                        | "accesstoken"
                        | "refreshtoken"
                ) || contains_sensitive_field(value)
            }),
            _ => false,
        }
    }
    if contains_sensitive_field(value) {
        return Err(format!(
            "{field} must not contain credentials or session material"
        ));
    }
    Ok(())
}

fn string_list_from_column(
    row: &rusqlite::Row<'_>,
    column: usize,
) -> rusqlite::Result<Vec<String>> {
    serde_json::from_str(&row.get::<_, String>(column)?).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}

fn optional_json_from_column(
    row: &rusqlite::Row<'_>,
    column: usize,
) -> rusqlite::Result<Option<Value>> {
    row.get::<_, Option<String>>(column)?
        .map_or(Ok(None), |value| {
            serde_json::from_str(&value).map(Some).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    column,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })
        })
}

fn invalid_persisted_value(column: usize, message: impl Into<String>) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        column,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            message.into(),
        )),
    )
}

fn installed_plugin_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<InstalledPluginRecord> {
    Ok(InstalledPluginRecord {
        id: row.get(0)?,
        name: row.get(1)?,
        version: row.get(2)?,
        manifest_path: row.get(3)?,
        installed_at: row.get(4)?,
        enabled: row.get(5)?,
        policy: serde_json::from_str(&row.get::<_, String>(6)?).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                6,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        digest: row.get(7)?,
        resolved_lock: json_from_column(row, 8)?,
        provenance: json_from_column(row, 9)?,
    })
}

fn prepared_plugin_install_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<PreparedPluginInstallRecord> {
    Ok(PreparedPluginInstallRecord {
        id: row.get(0)?,
        plugin_id: row.get(1)?,
        plugin_name: row.get(2)?,
        version: row.get(3)?,
        digest: row.get(4)?,
        staged_path: row.get(5)?,
        prepared_at: row.get(6)?,
        policy: json_from_column(row, 7)?,
        resolved_lock: json_from_column(row, 8)?,
        provenance: json_from_column(row, 9)?,
    })
}

fn json_from_column(row: &rusqlite::Row<'_>, column: usize) -> rusqlite::Result<Value> {
    serde_json::from_str(&row.get::<_, String>(column)?).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}

fn ensure_installed_plugin_columns(connection: &rusqlite::Transaction<'_>) -> Result<(), String> {
    let mut statement = connection
        .prepare("PRAGMA table_info(installed_plugins)")
        .map_err(database_error)?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(database_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(database_error)?;
    if !columns.iter().any(|column| column == "enabled") {
        connection
            .execute(
                "ALTER TABLE installed_plugins ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1",
                [],
            )
            .map_err(database_error)?;
    }
    if !columns.iter().any(|column| column == "policy_json") {
        connection
            .execute(
                "ALTER TABLE installed_plugins ADD COLUMN policy_json TEXT NOT NULL DEFAULT '{}'",
                [],
            )
            .map_err(database_error)?;
    }
    for (column, definition) in [
        ("digest", "TEXT NOT NULL DEFAULT ''"),
        ("lock_json", "TEXT NOT NULL DEFAULT '{}'"),
        ("provenance_json", "TEXT NOT NULL DEFAULT '{}'"),
    ] {
        if !columns.iter().any(|existing| existing == column) {
            connection
                .execute(
                    &format!("ALTER TABLE installed_plugins ADD COLUMN {column} {definition}"),
                    [],
                )
                .map_err(database_error)?;
        }
    }
    Ok(())
}

fn validate_secret_name(name: &str) -> Result<(), String> {
    if name.is_empty()
        || name.len() > 120
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err("secret name must be 1-120 ASCII letters, digits, '.', '_' or '-'".to_string());
    }
    Ok(())
}

fn execution_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ExecutionRecord> {
    Ok(ExecutionRecord {
        id: row.get(0)?,
        workflow_name: row.get(1)?,
        status: row.get(2)?,
        created_at: row.get(3)?,
        completed_at: row.get(4)?,
        error: row.get(5)?,
    })
}

fn event_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<EventRecord> {
    Ok(EventRecord {
        event_version: row.get(0)?,
        core_id: row.get(1)?,
        event_id: row.get(2)?,
        sequence: row.get::<_, i64>(3)? as u64,
        timestamp: row.get(4)?,
        event_type: row.get(5)?,
        resource_id: row.get(6)?,
        execution_id: row.get(7)?,
        payload: serde_json::from_str(&row.get::<_, String>(8)?).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                8,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
    })
}

fn now() -> Result<String, String> {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .map_err(|error| format!("cannot format current time: {error}"))
}

fn database_error(error: rusqlite::Error) -> String {
    format!("Kakune database error: {error}")
}

fn is_artifact_id(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn artifact_ids_in_value(value: &Value) -> Vec<String> {
    fn visit(value: &Value, ids: &mut Vec<String>) {
        match value {
            Value::Array(values) => values.iter().for_each(|value| visit(value, ids)),
            Value::Object(values) => {
                if values.contains_key("sha256")
                    && values.contains_key("sizeBytes")
                    && let Some(id) = values.get("id").and_then(Value::as_str)
                {
                    ids.push(id.to_string());
                }
                values.values().for_each(|value| visit(value, ids));
            }
            _ => {}
        }
    }
    let mut ids = Vec::new();
    visit(value, &mut ids);
    ids.sort();
    ids.dedup();
    ids
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::WorkflowDocument;

    use super::{
        ProviderAuth, ProviderInvocation, ProviderProfileDiagnostic, ProviderProfileStatus,
        ProviderProfileUpsert, ProviderType, Store, WorkflowSourceUpdate,
    };

    #[test]
    fn persists_workflows_and_executions() {
        let directory =
            std::env::temp_dir().join(format!("kakune-store-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(directory.clone()).expect("store should open");
        let core_id = store.core_id().expect("core ID should load");
        let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: sample\n  name: Sample\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: hello\nnodes:\n  - id: hello\n    type: kakune.log@1\n    inputs:\n      message: { literal: hello }\n";
        let workflow = WorkflowDocument::parse(source).expect("workflow should parse");
        store
            .upsert_workflow(&workflow, source, "enabled")
            .expect("workflow should save");
        let first_revision = store.list_workflows().expect("workflows should list")[0]
            .revision
            .clone();
        let updated = store
            .upsert_workflow(&workflow, source, "enabled")
            .expect("workflow revision should save");
        assert_ne!(first_revision, updated.revision);
        let execution = store
            .create_execution("sample")
            .expect("execution should save");
        store
            .record_node_outcome(
                &execution.id,
                "hello",
                "succeeded",
                Some("hello"),
                Some(&serde_json::json!({ "message": "hello" })),
                None,
            )
            .expect("node outcome should save");
        store
            .set_workflow_status("sample", "disabled")
            .expect("workflow status should save");
        store
            .finish_execution(&execution.id, "succeeded", None)
            .expect("execution should finish");
        assert_eq!(
            store.list_workflows().expect("workflows should list").len(),
            1
        );
        assert_eq!(
            store.list_executions().expect("executions should list")[0].status,
            "succeeded"
        );
        let events = store.list_events_after(None).expect("events should list");
        assert_eq!(
            events
                .iter()
                .map(|event| event.event_type.as_str())
                .collect::<Vec<_>>(),
            [
                "workflow.created",
                "workflow.updated",
                "execution.started",
                "execution.node.outcome",
                "workflow.status_changed",
                "execution.finished",
            ]
        );
        assert_eq!(events[0].event_version, "1.0");
        assert_eq!(events[0].core_id, core_id);
        assert!(
            events
                .windows(2)
                .all(|events| events[0].sequence < events[1].sequence)
        );
        assert_eq!(
            store
                .list_events_after(Some(&events[1].event_id))
                .expect("events after cursor should list")
                .len(),
            events.len() - 2
        );
        drop(store);
        let reopened = Store::open(directory.clone()).expect("store should reopen");
        assert_eq!(reopened.core_id().expect("core ID should load"), core_id);
        assert_eq!(
            reopened
                .list_events_after(None)
                .expect("events should persist")
                .last()
                .expect("events should exist")
                .sequence,
            events.last().expect("events should exist").sequence
        );
        drop(reopened);
        fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    #[test]
    fn trace_uses_provider_invocations_once_and_keeps_retry_attempts() {
        let directory =
            std::env::temp_dir().join(format!("kakune-trace-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(directory.clone()).expect("store should open");
        let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: traced\n  name: Traced\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: call\nnodes:\n  - id: call\n    type: kakune.log@1\n    inputs:\n      message: { literal: hello }\n";
        let workflow = WorkflowDocument::parse(source).expect("workflow should parse");
        let record = store
            .upsert_workflow(&workflow, source, "enabled")
            .expect("workflow should save");
        let execution = store
            .create_execution_with_plan(
                "traced",
                &record.revision,
                &serde_json::json!({ "policy": {} }),
            )
            .expect("execution should save");
        let first = store
            .start_trace_span(
                &execution.id,
                None,
                ("node", "kakune.ai.prompt@1"),
                Some("call"),
                Some(1),
                &serde_json::json!({}),
            )
            .expect("first attempt should start");
        store
            .finish_trace_span(&first, "failed", Some("temporary provider error"))
            .expect("first attempt should finish");
        let second = store
            .start_trace_span(
                &execution.id,
                None,
                ("node", "kakune.ai.prompt@1"),
                Some("call"),
                Some(2),
                &serde_json::json!({}),
            )
            .expect("second attempt should start");
        let provider = store
            .start_trace_span(
                &execution.id,
                Some(&second),
                ("provider", "minimax"),
                Some("call"),
                None,
                &serde_json::json!({ "providerId": "minimax-personal" }),
            )
            .expect("provider should start");
        let usage = serde_json::json!({ "inputTokens": 3, "outputTokens": 5, "totalTokens": 8 });
        store
            .record_provider_invocation(ProviderInvocation {
                execution_id: &execution.id,
                node_id: "call",
                provider: "minimax-personal",
                model_requested: "MiniMax-M3",
                model_reported: Some("MiniMax-M3"),
                usage: Some(&usage),
                raw: &serde_json::json!({ "private": true }),
                span_id: Some(&provider),
            })
            .expect("provider invocation should save");
        store
            .finish_trace_span(&provider, "succeeded", None)
            .expect("provider should finish");
        store
            .finish_trace_span(&second, "succeeded", None)
            .expect("second attempt should finish");
        let trace = store
            .execution_trace(&execution.id)
            .expect("trace should load")
            .expect("trace should exist");
        assert_eq!(trace.spans.len(), 3);
        assert_eq!(trace.provider_invocations.len(), 1);
        assert_eq!(trace.provider_usage[0].total_tokens, Some(8));
        assert_eq!(trace.provider_usage[0].cost_certainty, "unknown");
        assert_eq!(
            trace.provider_invocations[0].span_id.as_deref(),
            Some(provider.as_str())
        );
        assert!(
            !serde_json::to_string(&trace)
                .expect("trace should serialize")
                .contains("private")
        );
        drop(trace);
        drop(store);
        fs::remove_dir_all(directory).expect("test directory should remove");
    }

    #[test]
    fn updates_workflow_source_with_an_atomic_revision_guard() {
        let directory =
            std::env::temp_dir().join(format!("kakune-store-cas-{}", uuid::Uuid::new_v4()));
        let store = Store::open(directory.clone()).expect("store should open");
        let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: guarded\n  name: Guarded\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: hello\nnodes:\n  - id: hello\n    type: kakune.log@1\n    inputs:\n      message: { literal: hello }\n";
        let workflow = WorkflowDocument::parse(source).expect("workflow should parse");
        let initial = store
            .upsert_workflow(&workflow, source, "enabled")
            .expect("workflow should save");
        store
            .set_workflow_status("guarded", "disabled")
            .expect("workflow should disable");
        let updated = store
            .update_workflow_if_revision(&workflow, source, &initial.revision)
            .expect("update should complete");
        let WorkflowSourceUpdate::Updated(updated) = updated else {
            panic!("current revision must update");
        };
        assert_eq!(updated.status, "disabled");
        let conflict = store
            .update_workflow_if_revision(&workflow, source, &initial.revision)
            .expect("stale update should complete");
        assert!(matches!(conflict, WorkflowSourceUpdate::Conflict { .. }));
        drop(store);
        fs::remove_dir_all(directory).expect("test directory should remove");
    }

    #[test]
    fn persists_and_claims_one_overdue_calendar_trigger() {
        let directory = std::env::temp_dir().join(format!(
            "kakune-calendar-trigger-test-{}",
            uuid::Uuid::new_v4()
        ));
        let store = Store::open(directory.clone()).expect("store should open");
        let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: sample\n  name: Sample\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: hello\nnodes:\n  - id: hello\n    type: kakune.log@1\n    inputs:\n      message: { literal: hello }\n";
        let workflow = WorkflowDocument::parse(source).expect("workflow should parse");
        store
            .upsert_workflow(&workflow, source, "enabled")
            .expect("workflow should save");
        let now = time::macros::datetime!(2026-09-09 09:00 UTC);
        let first_due = time::macros::datetime!(2026-09-09 09:05 UTC);
        let second_due = time::macros::datetime!(2026-09-09 09:10 UTC);
        assert!(
            !store
                .claim_scheduled_trigger("sample", "morning", now, first_due, first_due)
                .expect("initial deadline should persist without running")
        );
        drop(store);

        let reopened = Store::open(directory.clone()).expect("store should reopen");
        assert!(
            reopened
                .claim_scheduled_trigger(
                    "sample",
                    "morning",
                    time::macros::datetime!(2026-09-09 09:09 UTC),
                    second_due,
                    second_due,
                )
                .expect("overdue deadline should claim once")
        );
        assert!(
            !reopened
                .claim_scheduled_trigger(
                    "sample",
                    "morning",
                    time::macros::datetime!(2026-09-09 09:09 UTC),
                    second_due,
                    second_due,
                )
                .expect("advanced deadline should not claim twice")
        );
        drop(reopened);
        fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    #[test]
    fn scopes_and_revocation_limit_api_tokens() {
        let directory =
            std::env::temp_dir().join(format!("kakune-token-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(directory.clone()).expect("store should open");
        let created = store
            .create_auth_token("runner".to_string(), vec![super::AuthScope::Run], None)
            .expect("token should be created");
        assert!(
            store
                .authorize_scope(&created.token, super::AuthScope::Run)
                .expect("run scope should authorize")
        );
        assert!(
            !store
                .authorize_scope(&created.token, super::AuthScope::Read)
                .expect("read scope should not authorize")
        );
        assert!(
            store
                .revoke_auth_token(&created.record.id)
                .expect("token should revoke")
        );
        assert!(
            !store
                .authorize_scope(&created.token, super::AuthScope::Run)
                .expect("revoked token should not authorize")
        );
        assert!(
            !store
                .authorize_scope("kakune_invalid", super::AuthScope::Read)
                .expect("unknown token should not authorize")
        );
        let expired = "kakune_expired";
        store
            .with_connection(|connection| {
                connection
                    .execute(
                        "INSERT INTO auth_tokens (id, token_hash, name, scopes, created_at, expires_at)
                         VALUES ('expired', ?1, 'expired', '[\"read\"]', '2020-01-01T00:00:00Z', '2020-01-01T00:00:00Z')",
                        [super::token_hash(expired)],
                    )
                    .map_err(super::database_error)?;
                Ok(())
            })
            .expect("expired token should persist for the authorization test");
        assert!(
            !store
                .authorize_scope(expired, super::AuthScope::Read)
                .expect("expired token should not authorize")
        );
        drop(store);
        fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    #[test]
    fn cancellation_request_is_durable_and_only_applies_to_running_executions() {
        let directory =
            std::env::temp_dir().join(format!("kakune-cancellation-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(directory.clone()).expect("store should open");
        let execution = store
            .create_execution("cancellable")
            .expect("execution should start");
        assert!(
            store
                .request_execution_cancel(&execution.id)
                .expect("cancellation should be requested")
        );
        assert!(
            store
                .execution_cancel_requested(&execution.id)
                .expect("cancellation should persist")
        );
        store
            .finish_execution(&execution.id, "cancelled", None)
            .expect("execution should finish");
        assert!(
            !store
                .request_execution_cancel(&execution.id)
                .expect("finished execution cannot be cancelled again")
        );
        drop(store);
        fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    #[test]
    fn recovery_terminalizes_incomplete_executions_after_a_restart() {
        let directory =
            std::env::temp_dir().join(format!("kakune-recovery-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(directory.clone()).expect("store should open");
        let running = store
            .create_execution("running")
            .expect("execution should start");
        let cancelling = store
            .create_execution("cancelling")
            .expect("execution should start");
        assert!(
            store
                .request_execution_cancel(&cancelling.id)
                .expect("cancellation should be requested")
        );

        assert_eq!(
            store
                .recover_incomplete_executions()
                .expect("recovery should run"),
            2
        );
        assert_eq!(
            store
                .get_execution(&running.id)
                .expect("execution should load")
                .expect("execution exists")
                .status,
            "interrupted"
        );
        assert_eq!(
            store
                .get_execution(&cancelling.id)
                .expect("execution should load")
                .expect("execution exists")
                .status,
            "cancelled"
        );
        assert_eq!(
            store
                .recover_incomplete_executions()
                .expect("recovery should be idempotent"),
            0
        );
        let events = store.list_events_after(None).expect("events should list");
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event_type == "execution.recovered")
                .count(),
            2
        );
        drop(store);
        fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    #[test]
    fn workflow_concurrency_queues_bounded_work_and_rejects_overflow() {
        let directory =
            std::env::temp_dir().join(format!("kakune-concurrency-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(directory.clone()).expect("store should open");
        let queued_policy = serde_json::json!({ "policy": { "concurrency": { "maxRuns": 1, "overflow": "queue", "maxQueued": 1 } } });
        let reject_policy = serde_json::json!({ "policy": { "concurrency": { "maxRuns": 1, "overflow": "reject" } } });
        let first = store
            .create_execution_with_plan("limited", "test", &queued_policy)
            .expect("first execution should start");
        let second = store
            .create_execution_with_plan("limited", "test", &queued_policy)
            .expect("second execution should queue");
        assert_eq!(first.status, "running");
        assert_eq!(second.status, "queued");
        assert!(
            store
                .create_execution_with_plan("limited", "test", &queued_policy)
                .is_err()
        );
        assert!(
            store
                .create_execution_with_plan("limited", "test", &reject_policy)
                .is_err()
        );
        store
            .finish_execution(&first.id, "succeeded", None)
            .expect("first execution should finish");
        assert!(
            store
                .claim_execution_slot(&second.id, Some(1))
                .expect("queued execution should claim a slot")
        );
        assert_eq!(
            store
                .get_execution(&second.id)
                .expect("execution should load")
                .expect("execution exists")
                .status,
            "running"
        );
        drop(store);
        fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    #[test]
    fn artifacts_are_read_only_through_registered_content_hashes() {
        let directory = std::env::temp_dir().join(format!(
            "kakune-artifact-read-test-{}",
            uuid::Uuid::new_v4()
        ));
        let store = Store::open(directory.clone()).expect("store should open");
        let artifact = store
            .put_artifact(b"verified content", "text/plain", Some("note.txt"))
            .expect("artifact should persist");
        let (record, bytes) = store
            .read_artifact(&artifact.id)
            .expect("artifact should load")
            .expect("artifact exists");
        assert_eq!(record.id, artifact.id);
        assert_eq!(bytes, b"verified content");
        assert!(store.read_artifact("../not-an-artifact").is_err());
        assert!(
            store
                .read_artifact(&"0".repeat(64))
                .expect("well-formed missing ID should be safe")
                .is_none()
        );
        drop(store);
        fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    #[test]
    fn provider_profiles_persist_multiple_auth_modes_without_credentials() {
        let directory = std::env::temp_dir().join(format!(
            "kakune-provider-profile-test-{}",
            uuid::Uuid::new_v4()
        ));
        let store = Store::open(directory.clone()).expect("store should open");
        let minimax = store
            .create_provider_profile(ProviderProfileUpsert {
                id: "minimax-personal".to_string(),
                display_name: "MiniMax Personal".to_string(),
                provider_type: ProviderType::MiniMax,
                default_model: "MiniMax-M2.5".to_string(),
                allowed_models: vec!["MiniMax-M2.5".to_string(), "MiniMax-M2.1".to_string()],
                capabilities: vec!["text".to_string(), "streaming".to_string()],
                auth: ProviderAuth::ApiKeySecret {
                    secret_ref: "minimax-key".to_string(),
                },
                config: serde_json::json!({ "baseUrl": "https://api.minimax.io/anthropic" }),
            })
            .expect("MiniMax profile should persist");
        store
            .create_provider_profile(ProviderProfileUpsert {
                id: "codex-personal".to_string(),
                display_name: "Codex Personal".to_string(),
                provider_type: ProviderType::Codex,
                default_model: "gpt-5.6-terra".to_string(),
                allowed_models: vec!["gpt-5.6-terra".to_string()],
                capabilities: vec!["text".to_string(), "agent".to_string()],
                auth: ProviderAuth::OAuthSecret {
                    secret_ref: "codex-personal-oauth".to_string(),
                },
                config: serde_json::json!({}),
            })
            .expect("Codex profile should persist");
        assert_eq!(
            store
                .list_provider_profiles()
                .expect("profiles should list")
                .iter()
                .map(|profile| profile.id.as_str())
                .collect::<Vec<_>>(),
            ["codex-personal", "minimax-personal"]
        );
        let serialized = serde_json::to_string(&minimax).expect("profile should serialize");
        assert!(serialized.contains("minimax-key"));
        assert!(matches!(
            minimax.auth,
            ProviderAuth::ApiKeySecret { ref secret_ref } if secret_ref == "minimax-key"
        ));
        assert!(
            store
                .delete_provider_profile("minimax-personal")
                .expect("profile should delete")
                .is_some()
        );
        assert!(
            store
                .list_secrets()
                .expect("secret metadata should list")
                .is_empty()
        );
        drop(store);
        fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    #[test]
    fn provider_profile_diagnosis_is_durable_and_upsert_resets_it() {
        let directory = std::env::temp_dir().join(format!(
            "kakune-provider-diagnosis-test-{}",
            uuid::Uuid::new_v4()
        ));
        let store = Store::open(directory.clone()).expect("store should open");
        let profile = ProviderProfileUpsert {
            id: "codex-api".to_string(),
            display_name: "Codex API".to_string(),
            provider_type: ProviderType::Codex,
            default_model: "gpt-5.6-terra".to_string(),
            allowed_models: vec!["gpt-5.6-terra".to_string()],
            capabilities: vec!["text".to_string()],
            auth: ProviderAuth::OAuthSecret {
                secret_ref: "codex-oauth".to_string(),
            },
            config: serde_json::json!({}),
        };
        store
            .create_provider_profile(profile.clone())
            .expect("profile should persist");
        let diagnosed = store
            .diagnose_provider_profile(
                "codex-api",
                ProviderProfileDiagnostic {
                    status: ProviderProfileStatus::Unavailable,
                    checked_at: None,
                    message: Some("ChatGPT OAuth credentials were unavailable".to_string()),
                    details: Some(serde_json::json!({ "check": "chatgptOAuth" })),
                },
            )
            .expect("diagnosis should persist")
            .expect("profile exists");
        assert_eq!(
            diagnosed.diagnostic.status,
            ProviderProfileStatus::Unavailable
        );
        assert!(diagnosed.diagnostic.checked_at.is_some());
        assert_eq!(
            diagnosed.diagnostic.message.as_deref(),
            Some("ChatGPT OAuth credentials were unavailable")
        );
        let updated = store
            .upsert_provider_profile(profile)
            .expect("profile should update");
        assert_eq!(updated.diagnostic.status, ProviderProfileStatus::Unknown);
        assert!(updated.diagnostic.checked_at.is_none());
        assert!(updated.diagnostic.message.is_none());
        assert!(
            store
                .create_provider_profile(ProviderProfileUpsert {
                    id: "invalid".to_string(),
                    display_name: "Invalid".to_string(),
                    provider_type: ProviderType::MiniMax,
                    default_model: "MiniMax-M2.5".to_string(),
                    allowed_models: vec!["MiniMax-M2.5".to_string()],
                    capabilities: vec![],
                    auth: ProviderAuth::OAuthSecret {
                        secret_ref: "invalid-oauth".to_string()
                    },
                    config: serde_json::json!({ "apiKey": "must-not-persist" }),
                })
                .is_err()
        );
        drop(store);
        fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    #[test]
    fn upgrades_legacy_databases_transactionally_with_a_backup() {
        let directory =
            std::env::temp_dir().join(format!("kakune-migration-test-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&directory).expect("temporary directory should create");
        let database = directory.join("kakune.sqlite3");
        let legacy = rusqlite::Connection::open(&database).expect("legacy database should create");
        legacy
            .execute_batch(
                "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY);
                 CREATE TABLE workflows (
                   id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, source TEXT NOT NULL,
                   status TEXT NOT NULL, updated_at TEXT NOT NULL
                 );
                 CREATE TABLE node_runs (
                   id INTEGER PRIMARY KEY, execution_id TEXT NOT NULL, node_id TEXT NOT NULL,
                   status TEXT NOT NULL, message TEXT, created_at TEXT NOT NULL
                 );
                 CREATE TABLE auth_tokens (
                   id TEXT PRIMARY KEY, token_hash TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL,
                   revoked_at TEXT
                 );
                 CREATE TABLE installed_plugins (
                   id TEXT PRIMARY KEY, name TEXT NOT NULL, version TEXT NOT NULL,
                   manifest_path TEXT NOT NULL, installed_at TEXT NOT NULL
                 );
                 INSERT INTO schema_migrations (version) VALUES (1);",
            )
            .expect("legacy schema should create");
        drop(legacy);

        let store = Store::open(directory.clone()).expect("legacy database should migrate");
        assert_eq!(
            store.schema_version().expect("schema version should load"),
            super::LATEST_SCHEMA_VERSION
        );
        assert!(
            directory
                .join("backups")
                .join("kakune.sqlite3.before-migration-v2.sqlite3")
                .exists()
        );
        drop(store);
        fs::remove_dir_all(directory).expect("temporary data should be removed");
    }
}

fn idempotent_execution(
    db: &Connection,
    key: &str,
    hash: &str,
) -> Result<Option<ExecutionRecord>, String> {
    let stored: Option<(String, String)> = db
        .query_row(
            "SELECT request_hash,execution_id FROM execution_idempotency WHERE request_key=?1",
            [key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(database_error)?;
    let Some((previous, id)) = stored else {
        return Ok(None);
    };
    if previous != hash {
        return Err("idempotency key already used for a different request".into());
    }
    db.query_row(
        "SELECT id,workflow_name,status,created_at,completed_at,error FROM executions WHERE id=?1",
        [id],
        execution_from_row,
    )
    .optional()
    .map_err(database_error)
}
