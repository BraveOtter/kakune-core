pub mod api;
mod runtime;
mod store;
mod workflow;

use std::path::PathBuf;

pub use runtime::run_workflow;
pub use store::{ExecutionRecord, StepRunRecord, Store, WorkflowRecord};
pub use workflow::WorkflowDocument;

pub const API_VERSION: &str = "1.0";

pub fn default_data_dir() -> PathBuf {
    if let Some(path) = std::env::var_os("KAKUNE_DATA_DIR") {
        return PathBuf::from(path);
    }
    if let Some(path) = std::env::var_os("APPDATA") {
        return PathBuf::from(path).join("Kakune");
    }
    if let Some(path) = std::env::var_os("XDG_DATA_HOME") {
        return PathBuf::from(path).join("kakune");
    }
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".local")
        .join("share")
        .join("kakune")
}
