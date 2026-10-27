pub mod api;
mod artifacts;
pub mod client_config;
pub mod codex;
pub mod config;
pub mod diagnostic;
pub mod initialization;
pub mod lifecycle;
pub mod management;
pub mod mcp;
pub mod minimax;
pub mod observation;
mod planner;
pub mod plugin_install;
pub mod plugin_process;
pub mod remote;
pub mod reporting;
mod runtime;
mod scheduler;
pub mod server;
mod store;
mod vault;
mod workflow;

use std::path::PathBuf;

pub use artifacts::ArtifactRef;
pub use client_config::{ConnectionContext, ContextFile, default_contexts_path};
pub use config::CoreConfig;
pub use diagnostic::{Diagnostic, DiagnosticSeverity, SourcePosition, SourceRange};
pub use initialization::{
    ComponentState, CredentialAccess, CredentialAccessError, CredentialReference, CredentialSecret,
    InitializationComponent, InitializationCorrection, InitializationError,
    InitializationFailureCategory, InitializationPaths, InitializationReport,
    InitializationResource,
};
pub use planner::{PlanBoundsView, PlanNodeView, PlanScopeView, PlanView, WorkflowAnalysis};
pub use plugin_process::PluginRegistry;
pub use reporting::{CoreDiagnostic, CoreDiagnosticSource, DiagnosticReporter};
pub use runtime::{
    execute_prepared_workflow_with_plugins, execute_prepared_workflow_with_plugins_and_shutdown,
    prepare_canonical_execution_with_context_request, prepare_execution_with_context,
    prepare_execution_with_plugins, run_standalone_workflow, run_workflow,
    run_workflow_with_context, run_workflow_with_context_and_shutdown, run_workflow_with_plugins,
};
pub use store::{
    AuthPairingState, AuthPairingStatus, AuthScope, AuthTokenRecord, BackupSummary,
    CreatedAuthPairingCode, CreatedAuthToken, ExecutionRecord, ExecutionTraceRecord,
    InstalledPluginRecord, McpConnectionTransport, NamedMcpConnection, NativeCredentialBackend,
    NewNamedMcpConnection, NodeRunRecord, OperationalMetrics, PreparedPluginInstallRecord,
    ProviderAuth, ProviderInvocationRecord, ProviderProfile, ProviderProfileDiagnostic,
    ProviderProfileStatus, ProviderProfileUpsert, ProviderType, ProviderUsageSummary,
    RetentionPolicy, RetentionReport, SecretBackendKind, SecretRecord, SecretRemoveOutcome,
    SecretSetOutcome, Store, TraceArtifactLink, TraceSpanRecord, WorkflowRecord,
    WorkflowRevisionComparison, WorkflowRevisionRecord, WorkflowRevisionSource,
    WorkflowSourceRecord, WorkflowSourceUpdate, WorkflowStatusUpdate,
};
pub use workflow::WorkflowDocument;

/// Starts the durable trigger scheduler for an already-open Core store.
pub fn start_scheduler(store: Store) {
    scheduler::start(store);
}

/// Starts the durable trigger scheduler with an injected diagnostics sink.
pub fn start_scheduler_with_reporter(store: Store, reporter: DiagnosticReporter) {
    scheduler::start_with_reporter(store, reporter);
}

/// Starts a scheduler owned by one managed Core instance. Shutdown closes
/// trigger admission through the supplied lifecycle and returns a join handle
/// so filesystem watchers cannot outlive the Core lease.
pub fn start_managed_scheduler(
    store: Store,
    lifecycle: lifecycle::CoreLifecycle,
) -> scheduler::SchedulerHandle {
    scheduler::start_managed(store, lifecycle, DiagnosticReporter::default())
}

/// Starts a managed scheduler that reports diagnostics through `reporter`.
pub fn start_managed_scheduler_with_reporter(
    store: Store,
    lifecycle: lifecycle::CoreLifecycle,
    reporter: DiagnosticReporter,
) -> scheduler::SchedulerHandle {
    scheduler::start_managed(store, lifecycle, reporter)
}

pub use scheduler::SchedulerHandle;

pub const API_VERSION: &str = "1.0";

/// Analyzes YAML without executing it. Invalid documents return diagnostics
/// rather than requiring callers to parse human-readable error strings.
pub fn analyze_workflow(source: &str) -> WorkflowAnalysis {
    analyze_workflow_with_plugins(source, &PluginRegistry::default())
}

/// Analyzes YAML without executing it using an explicitly supplied plugin catalog.
pub fn analyze_workflow_with_plugins(source: &str, plugins: &PluginRegistry) -> WorkflowAnalysis {
    planner::analyze(source, plugins)
}

/// Rebuilds the explicit process-plugin registry from locally installed manifests.
pub fn load_installed_plugin_registry(store: &Store) -> Result<PluginRegistry, String> {
    let mut registry = PluginRegistry::default();
    for plugin in store
        .list_installed_plugins()?
        .into_iter()
        .filter(|plugin| plugin.enabled)
    {
        registry
            .register_manifest_path(&plugin.manifest_path)
            .map_err(|error| format!("cannot load installed plugin {}: {error}", plugin.id))?;
    }
    Ok(registry)
}

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
pub mod codex_adapter;

pub mod process_supervisor;

mod plugin_services;
