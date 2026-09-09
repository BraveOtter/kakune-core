use std::{
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::WorkflowDocument;

#[derive(Clone)]
pub struct Store {
    connection: Arc<Mutex<Connection>>,
    data_dir: Arc<PathBuf>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowRecord {
    pub id: String,
    pub name: String,
    pub status: String,
    pub updated_at: String,
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

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StepRunRecord {
    pub step_id: String,
    pub status: String,
    pub message: Option<String>,
}

impl Store {
    pub fn open(data_dir: PathBuf) -> Result<Self, String> {
        fs::create_dir_all(&data_dir)
            .map_err(|error| format!("cannot create Kakune data directory: {error}"))?;
        let connection = Connection::open(data_dir.join("kakune.sqlite3"))
            .map_err(|error| format!("cannot open Kakune database: {error}"))?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(|error| format!("cannot configure Kakune database: {error}"))?;
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .map_err(|error| format!("cannot configure Kakune database: {error}"))?;
        migrate(&connection)?;
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

    pub fn upsert_workflow(
        &self,
        workflow: &WorkflowDocument,
        source: &str,
        status: &str,
    ) -> Result<WorkflowRecord, String> {
        let now = now()?;
        let id = workflow.metadata.name.clone();
        self.with_connection(|connection| {
            connection
                .execute(
                    "INSERT INTO workflows (id, name, source, status, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)
                     ON CONFLICT(name) DO UPDATE SET
                       source = excluded.source,
                       status = excluded.status,
                       updated_at = excluded.updated_at",
                    params![id, workflow.metadata.name, source, status, now],
                )
                .map_err(database_error)?;
            Ok(WorkflowRecord {
                id: workflow.metadata.name.clone(),
                name: workflow.metadata.name.clone(),
                status: status.to_owned(),
                updated_at: now,
            })
        })
    }

    pub fn list_workflows(&self) -> Result<Vec<WorkflowRecord>, String> {
        self.with_connection(|connection| {
            let mut statement = connection
                .prepare("SELECT id, name, status, updated_at FROM workflows ORDER BY name")
                .map_err(database_error)?;
            let rows = statement
                .query_map([], |row| {
                    Ok(WorkflowRecord {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        status: row.get(2)?,
                        updated_at: row.get(3)?,
                    })
                })
                .map_err(database_error)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(database_error)
        })
    }

    pub fn get_workflow_source(&self, name: &str) -> Result<Option<String>, String> {
        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT source FROM workflows WHERE name = ?1",
                    [name],
                    |row| row.get(0),
                )
                .optional()
                .map_err(database_error)
        })
    }

    pub fn set_workflow_status(&self, name: &str, status: &str) -> Result<(), String> {
        let now = now()?;
        self.with_connection(|connection| {
            let affected = connection
                .execute(
                    "UPDATE workflows SET status = ?1, updated_at = ?2 WHERE name = ?3",
                    params![status, now, name],
                )
                .map_err(database_error)?;
            if affected == 0 {
                return Err(format!("workflow {name} was not found"));
            }
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
            connection
                .execute(
                    "INSERT INTO executions (id, workflow_name, status, created_at) VALUES (?1, ?2, ?3, ?4)",
                    params![execution.id, execution.workflow_name, execution.status, execution.created_at],
                )
                .map_err(database_error)?;
            Ok(execution)
        })
    }

    pub fn finish_execution(
        &self,
        id: &str,
        status: &str,
        error: Option<&str>,
    ) -> Result<(), String> {
        let completed_at = now()?;
        self.with_connection(|connection| {
            connection
                .execute(
                    "UPDATE executions SET status = ?1, completed_at = ?2, error = ?3 WHERE id = ?4",
                    params![status, completed_at, error, id],
                )
                .map_err(database_error)?;
            Ok(())
        })
    }

    pub fn record_step_run(
        &self,
        execution_id: &str,
        step_id: &str,
        status: &str,
        message: Option<&str>,
    ) -> Result<(), String> {
        self.with_connection(|connection| {
            connection
                .execute(
                    "INSERT INTO step_runs (execution_id, step_id, status, message, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![execution_id, step_id, status, message, now()?],
                )
                .map_err(database_error)?;
            Ok(())
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

    pub fn list_step_runs(&self, execution_id: &str) -> Result<Vec<StepRunRecord>, String> {
        self.with_connection(|connection| {
            let mut statement = connection
                .prepare("SELECT step_id, status, message FROM step_runs WHERE execution_id = ?1 ORDER BY id")
                .map_err(database_error)?;
            let rows = statement
                .query_map([execution_id], |row| {
                    Ok(StepRunRecord {
                        step_id: row.get(0)?,
                        status: row.get(1)?,
                        message: row.get(2)?,
                    })
                })
                .map_err(database_error)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(database_error)
        })
    }

    fn with_connection<T>(
        &self,
        operation: impl FnOnce(&Connection) -> Result<T, String>,
    ) -> Result<T, String> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| "Kakune database lock was poisoned".to_string())?;
        operation(&connection)
    }
}

fn migrate(connection: &Connection) -> Result<(), String> {
    connection
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY);
             CREATE TABLE IF NOT EXISTS workflows (
               id TEXT PRIMARY KEY,
               name TEXT NOT NULL UNIQUE,
               source TEXT NOT NULL,
               status TEXT NOT NULL CHECK(status IN ('enabled', 'disabled')),
               updated_at TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS executions (
               id TEXT PRIMARY KEY,
               workflow_name TEXT NOT NULL,
               status TEXT NOT NULL,
               created_at TEXT NOT NULL,
               completed_at TEXT,
               error TEXT
             );
             CREATE INDEX IF NOT EXISTS executions_created_at ON executions(created_at DESC);
             CREATE TABLE IF NOT EXISTS step_runs (
               id INTEGER PRIMARY KEY,
               execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
               step_id TEXT NOT NULL,
               status TEXT NOT NULL,
               message TEXT,
               created_at TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS step_runs_execution_id ON step_runs(execution_id, id);",
        )
        .map_err(database_error)
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

fn now() -> Result<String, String> {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .map_err(|error| format!("cannot format current time: {error}"))
}

fn database_error(error: rusqlite::Error) -> String {
    format!("Kakune database error: {error}")
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::WorkflowDocument;

    use super::Store;

    #[test]
    fn persists_workflows_and_executions() {
        let directory =
            std::env::temp_dir().join(format!("kakune-store-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(directory.clone()).expect("store should open");
        let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  name: sample\nspec:\n  triggers:\n    manual: {}\n  steps:\n    - id: hello\n      plugin: '@kakune/core'\n";
        let workflow = WorkflowDocument::parse(source).expect("workflow should parse");
        store
            .upsert_workflow(&workflow, source, "enabled")
            .expect("workflow should save");
        let execution = store
            .create_execution("sample")
            .expect("execution should save");
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
        drop(store);
        fs::remove_dir_all(directory).expect("temporary data should be removed");
    }
}
