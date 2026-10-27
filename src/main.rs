use std::{
    fs,
    io::{self, IsTerminal, Read, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::ExitCode,
    sync::Arc,
    time::{Duration, Instant},
};

use clap::{Args, CommandFactory, Parser, Subcommand};
use zeroize::{Zeroize, Zeroizing};

#[cfg(windows)]
mod windows_service_host;
use kakune_core::{
    AuthPairingState, AuthScope, ConnectionContext, ContextFile, CoreConfig, PluginRegistry,
    ProviderAuth, ProviderProfile, ProviderProfileDiagnostic, ProviderProfileStatus,
    ProviderProfileUpsert, ProviderType, RetentionPolicy, Store, analyze_workflow_with_plugins,
    api,
    client_config::ContextRepository,
    client_config::SystemCredentialAccess,
    default_contexts_path, default_data_dir,
    initialization::{
        ComponentState, CredentialAccess, CredentialAccessError, InitializationCorrection,
        InitializationError, InitializationPaths, InitializationReport, initialize,
    },
    lifecycle::{
        Deadline, SystemMonotonicClock,
        daemon::{self, DaemonObservation},
        host::{HostAdapter, HostEvent, HostOptions},
        service::{
            NativeServiceObservation, ServiceController, ServiceDefinition, ServiceManagerKind,
            ServiceOperationResult,
        },
    },
    load_installed_plugin_registry,
    plugin_install::{self, PluginSource},
    run_workflow_with_plugins,
};

#[derive(Debug, Parser)]
#[command(name = "kakune", version, about = "Kakune local automation runtime")]
struct Cli {
    #[arg(
        long,
        global = true,
        help = "Select a configured Core; there is no implicit fallback."
    )]
    context: Option<String>,
    #[arg(
        long,
        global = true,
        help = "Use this connection collection (and its active Core unless --context is set)."
    )]
    context_file: Option<PathBuf>,
    #[arg(long, global = true, help = "CA bundle for a TLS Core connection.")]
    ca: Option<PathBuf>,
    /// Operate directly on local data instead of calling a Core API.
    #[arg(
        long,
        global = true,
        conflicts_with = "context",
        help = "Use local execution only where the command documents standalone support."
    )]
    standalone: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Safely prepares or checks the selected local installation.
    #[command(
        long_about = "Local-only: does not accept --context and never starts the Core or contacts remote endpoints.\n--standalone and --ca are compatibility no-ops.\nSafe to repeat: preserves existing data, configuration, credential history, and custom connection metadata; does not rotate access.\nLost access requires explicit `kakune auth recover`."
    )]
    Init {
        #[arg(long)]
        data_dir: Option<PathBuf>,
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Reports this executable's local build version.
    #[command(
        long_about = "Reports the build version of this local executable. It does not access installation files or a remote Core."
    )]
    Version,
    /// Runs and manages the Core HTTP API for the current user.
    #[command(
        long_about = "Runs the Core in the foreground or manages one current-user background daemon. `daemon stop` never controls a foreground Core or the machine-wide system service; use `service` for OS-managed system scope. Status is read-only and does not load configuration or credentials. Startup readiness and Core graceful shutdown each have a 30-second application-controlled budget; ownership-lock acquisition is bounded to 10 seconds. A stop timeout is failure with retained ownership, never a forced-stop success. Performance targets are measured, not per-call deadlines."
    )]
    Daemon(DaemonArgs),
    /// Backs up, restores, retains, and compacts Core data.
    #[command(
        long_about = "Local-only storage maintenance; selecting a remote context does not redirect these operations. The legacy `backup`, `restore`, `retain`, `compact`, and `pin` commands remain distinct from workflow import/export and execution observation."
    )]
    Storage {
        #[command(subcommand)]
        command: StorageCommand,
    },
    /// Installs and operates the machine-wide native service.
    #[command(
        long_about = "Controls the local machine-wide Kakune service through Windows SCM, Linux systemd, or macOS launchd. These commands are separate from the current-user daemon commands and do not operate through remote contexts. Installation registers automatic boot but leaves the service stopped; start succeeds only after the Core is observed ready; stop confirms Core exit before uninstall removes only the registration. Data and credentials are retained.\n\nApplication-controlled phase budgets are 10 seconds for lock acquisition, status, preflight/reconciliation, helpers, and stop dispatch; 30 seconds for readiness, Core shutdown grace, install, and removal. Accumulated application-wait ceilings are 50/50/60/90 seconds for install/start/stop/uninstall. These are not wall-clock guarantees for platform calls: a timed-out native client may already have submitted work, so inspect the service journal and native manager before retrying. Two- and ten-second performance goals are measured targets, not enforced limits.\n\nMachine journal roots: Windows ProgramData\\Kakune\\lifecycle\\services; Linux /var/lib/kakune/lifecycle/services; macOS /Library/Application Support/Kakune/lifecycle/services. macOS support remains theoretical and unverified. Native service-manager acceptance has not been completed on every platform. Local daemon control proves an owner-scoped capability and checks verified ownership; loopback does not establish the TCP peer's OS account."
    )]
    Service {
        #[command(subcommand)]
        command: ServiceCommand,
    },
    /// Shows local configuration, storage, and idle-runtime diagnostics.
    #[command(
        long_about = "Local-only and read-only diagnostics. Reports configuration, storage, database, credential-store, Core, service, compatibility, and platform findings; it never initializes, repairs, migrates, or writes credential state. Required unknown/failed findings exit 1, invalid arguments exit 2, and optional warnings/unknown findings alone exit 0. Use --json for the fixed machine-readable report."
    )]
    Doctor {
        #[arg(long)]
        data_dir: Option<PathBuf>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    /// Manages portable connection metadata without persisting token values.
    #[command(
        long_about = "Local-only context collection management. Add/list/use/inspect/remove/import/export operate on `--contexts-file` (or the configured default) and never resolve credential values. `context export <name> --output <file> [--replace]` writes exactly one context; import validates the complete collection and merges without replacement."
    )]
    Context {
        #[command(subcommand)]
        command: ContextCommand,
    },
    /// Recovers local API access and manages local authentication.
    #[command(
        long_about = "Local Core API access operations: recover, pair, list Core token metadata, and revoke a Core token. These commands remain local even when a Core context is selected; they are distinct from `provider auth`, `mcp auth`, and integration `secret` operations."
    )]
    Auth {
        #[command(subcommand)]
        command: AuthCommand,
    },
    /// Authors workflow files and manages stored workflows through a selected Core.
    #[command(
        long_about = "Workflow create and `validate|plan --file` are local. Stored workflow import/export/list/run/enable/disable use the selected Core by default; an explicit `--context` selects another Core and never falls back. Use `--json` for machine-readable outcomes and `--limit`/`--cursor` for one-page lists. File destinations require `--output` where shown; existing files require explicit `--replace`. Invalid arguments exit 2; validation, authorization, and operation failures exit 1. `--data-dir` does not imply standalone execution."
    )]
    Workflow {
        #[command(subcommand)]
        command: WorkflowCommand,
    },
    /// Observes and cooperatively controls executions in the selected Core.
    #[command(
        long_about = "Execution list/inspect/cancel/logs use the selected Core and return one bounded page where pagination applies. Pass `--json` for typed output; use `--limit` and `--cursor` to continue. Cancellation reports request outcome separately from observed state; logs are finite snapshots, not follow mode. Unknown IDs and operation failures exit 1; invalid arguments exit 2."
    )]
    Execution {
        #[command(subcommand)]
        command: ExecutionCommand,
    },
    /// Observes registered trigger definitions without firing or reconciling them.
    #[command(
        long_about = "Read-only trigger observations use the selected Core. `trigger list` returns one page (`--limit`, `--cursor`); `trigger inspect` accepts the composite workflow-id/trigger-id emitted by list. Inspection does not fire or reconcile schedules. Use `--json` for machine output."
    )]
    Trigger {
        #[command(subcommand)]
        command: TriggerCommand,
    },
    /// Installs, lists, or removes explicitly trusted local process plugins.
    Plugin {
        #[command(subcommand)]
        command: PluginCommand,
    },
    /// Manages supported provider profiles and authentication.
    #[command(
        long_about = "Canonical `add`, `list`, `auth`, `test`, and `models` use the selected Core. Output is human-readable by default; pass `--json` to any canonical provider command for machine-readable output. Add is create-only; it does not replace a profile. MiniMax accepts hidden terminal input or explicit `auth --stdin`; Codex uses its supported browser flow. Model discovery is unsupported by the current providers and is reported as such, separately from configured/static model information. Lists support `--limit`/`--cursor`. The legacy `upsert`, `status`, `remove`, and `login` commands remain separate with their existing routing, output, and side effects; see README migration guidance."
    )]
    Provider {
        #[command(subcommand)]
        command: ProviderCommand,
    },
    /// Manages securely stored integration credentials by name.
    #[command(
        long_about = "Secret operations use the selected Core and never print secret values. Output is human-readable by default; pass `--json` to any canonical secret command for machine-readable output. `secret set <name>` prompts through hidden terminal input; use `--stdin` for bounded automation. Secret values are not accepted as positional arguments or flags. List and success output contain metadata only; use `--limit` and `--cursor` to page."
    )]
    Secret {
        #[command(subcommand)]
        command: SecretCommand,
    },
    /// Manages named MCP connections and invokes tools without an AI workflow.
    #[command(
        long_about = "Canonical named `add`, `list`, `inspect`, `auth`, and `tool list|call` operations use the selected Core. Output is human-readable by default; pass `--json` to any canonical named MCP command for machine-readable output. Tool calls take strict JSON from `--input`, validate the discovered schema before invoking one named tool, and return bounded results. `mcp auth --secret` binds an existing secret reference; it does not accept a value or stdin. The legacy one-shot `mcp call <file>` remains distinct and retains its file-driven routing and response contract. Current transports are the existing stdio and HTTP transports; unsupported authentication is reported explicitly."
    )]
    Mcp {
        #[command(subcommand)]
        command: McpCommand,
    },
    /// Runs a workflow YAML file once and prints its execution record.
    #[command(
        long_about = "Legacy extra; retained separately from `workflow run`. This file-driven entry point keeps its legacy TSV local-standalone output and JSON selected-Core response. Legacy execution registers/updates the definition as enabled before submitting it; canonical import creates disabled and canonical manual run does not change eligibility. `--data-dir` alone is not standalone; pass global `--standalone` for direct local execution. See README for an explicit migration example and side-effect differences."
    )]
    Run {
        workflow: PathBuf,
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    /// Lists persisted execution summaries.
    #[command(
        long_about = "Legacy extra; retained separately from `execution list`. It defaults to the local Store and prints its four-column TSV; explicit context uses the legacy JSON response. Canonical `execution list` targets the selected Core and has bounded pagination and typed output. `--data-dir` on this legacy command continues to select the local installation when no remote context is requested."
    )]
    Executions {
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    /// Inspects a persisted execution and its node runs.
    #[command(
        long_about = "Legacy extra; retained separately from `execution inspect`. It defaults to local storage and prints the legacy TSV summary/node rows; explicit context returns the legacy JSON shape. Canonical inspection targets the selected Core and exposes bounded details and availability states. Migration changes both the default target and output schema."
    )]
    Inspect {
        execution_id: String,
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DispatchCategory {
    CanonicalLocal,
    CanonicalCore,
    Legacy,
}

fn dispatch_category(command: &Command) -> DispatchCategory {
    match command {
        Command::Init { .. }
        | Command::Daemon(_)
        | Command::Storage { .. }
        | Command::Service { .. }
        | Command::Doctor { .. }
        | Command::Context { .. } => DispatchCategory::CanonicalLocal,
        Command::Workflow { command } => match command {
            WorkflowCommand::Create { .. }
            | WorkflowCommand::Validate { file: Some(_), .. }
            | WorkflowCommand::Plan { file: Some(_), .. }
            | WorkflowCommand::Run { file: Some(_), .. } => DispatchCategory::CanonicalLocal,
            _ => DispatchCategory::CanonicalCore,
        },
        Command::Execution { .. } | Command::Trigger { .. } => DispatchCategory::CanonicalCore,
        Command::Provider {
            command:
                ProviderCommand::Add { .. }
                | ProviderCommand::List { .. }
                | ProviderCommand::Auth { .. }
                | ProviderCommand::Test { .. }
                | ProviderCommand::Models { .. },
        }
        | Command::Mcp {
            command:
                McpCommand::Add { .. }
                | McpCommand::List { .. }
                | McpCommand::Inspect { .. }
                | McpCommand::Auth { .. }
                | McpCommand::Tool { .. },
        } => DispatchCategory::CanonicalCore,
        Command::Provider { .. } | Command::Mcp { .. } => DispatchCategory::Legacy,
        Command::Secret { .. } => DispatchCategory::CanonicalCore,
        _ => DispatchCategory::Legacy,
    }
}

fn validate_dispatch_arguments(cli: &Cli) -> Result<(), String> {
    if cli.context.is_some() && dispatch_category(&cli.command) == DispatchCategory::CanonicalLocal
    {
        let message = match &cli.command {
            Command::Init { .. } => "init is local-only and does not accept --context",
            Command::Daemon(_) => "daemon lifecycle commands are local and do not accept --context",
            Command::Service { .. } => {
                "service lifecycle commands are local and do not accept --context"
            }
            Command::Storage { .. } => {
                "this command operates on the local installation; omit --context"
            }
            _ => "local-only commands do not accept --context",
        };
        return Err(message.to_owned());
    }
    if cli.context_file.is_some()
        && let Command::Context { command } = &cli.command
        && context_command_has_file_override(command)
    {
        return Err(
            "--context-file and --contexts-file cannot be combined; choose one path".to_owned(),
        );
    }
    if let Command::Workflow { command } = &cli.command {
        match command {
            WorkflowCommand::Validate { workflow, file, .. }
            | WorkflowCommand::Plan { workflow, file, .. } => {
                if workflow.is_some() == file.is_some() {
                    return Err("specify exactly one workflow selector or --file".to_owned());
                }
            }
            WorkflowCommand::Run {
                workflow,
                file,
                timeout_seconds,
                ..
            } => {
                if workflow.is_some() == file.is_some() {
                    return Err("specify exactly one workflow selector or --file".to_owned());
                }
                if file.is_some() && !cli.standalone {
                    return Err("local workflow files require --standalone for run".to_owned());
                }
                if file.is_none() && cli.standalone {
                    return Err("--standalone requires workflow run --file".to_owned());
                }
                if timeout_seconds.is_some_and(|seconds| !(1..=3_600).contains(&seconds)) {
                    return Err("--timeout-seconds must be from 1 to 3,600".to_owned());
                }
            }
            WorkflowCommand::Import {
                if_revision: Some(_),
                replace: false,
                ..
            } => return Err("--if-revision requires --replace".to_owned()),
            _ => {}
        }
    }
    match &cli.command {
        Command::Execution { command } => match command {
            ExecutionCommand::Cancel {
                wait_seconds: Some(seconds),
                ..
            } if *seconds > 30 => {
                return Err("--wait-seconds must be from 0 to 30".to_owned());
            }
            ExecutionCommand::List {
                limit: Some(limit), ..
            }
            | ExecutionCommand::Inspect {
                limit: Some(limit), ..
            }
            | ExecutionCommand::Logs {
                limit: Some(limit), ..
            } if !(1..=1_000).contains(limit) => {
                return Err("--limit must be from 1 to 1,000".to_owned());
            }
            _ => {}
        },
        Command::Trigger { command } => match command {
            TriggerCommand::List {
                limit: Some(limit), ..
            } if !(1..=1_000).contains(limit) => {
                return Err("--limit must be from 1 to 1,000".to_owned());
            }
            TriggerCommand::Inspect { trigger_id, .. } => {
                parse_composite_trigger_id(trigger_id)?;
            }
            _ => {}
        },
        _ => {}
    }
    Ok(())
}

fn context_command_has_file_override(command: &ContextCommand) -> bool {
    match command {
        ContextCommand::Add { contexts_file, .. }
        | ContextCommand::List { contexts_file }
        | ContextCommand::Use { contexts_file, .. }
        | ContextCommand::Inspect { contexts_file, .. }
        | ContextCommand::Remove { contexts_file, .. }
        | ContextCommand::Import { contexts_file, .. }
        | ContextCommand::Export { contexts_file, .. } => contexts_file.is_some(),
    }
}

fn selected_management_context<'a>(
    contexts: &'a ContextFile,
    explicit: Option<&str>,
) -> Result<&'a ConnectionContext, String> {
    let id = explicit
        .or(contexts.active_context_id.as_deref())
        .ok_or_else(|| {
            "no Core context is selected; add a context and select it explicitly".to_owned()
        })?;
    contexts
        .contexts
        .iter()
        .find(|context| context.id == id)
        .ok_or_else(|| format!("selected Core context {id} was not found"))
}

#[derive(Debug, Args)]
struct DaemonArgs {
    #[arg(long, global = true)]
    tls_cert: Option<PathBuf>,
    #[arg(long, global = true)]
    tls_key: Option<PathBuf>,
    #[arg(long, global = true)]
    allowed_host: Vec<String>,
    #[arg(long, hide = true)]
    internal_daemon_child: bool,
    #[arg(long, hide = true)]
    internal_log: Option<PathBuf>,
    #[command(subcommand)]
    action: Option<DaemonAction>,
    #[arg(long, global = true)]
    listen: Option<SocketAddr>,
    #[arg(long, global = true)]
    data_dir: Option<PathBuf>,
    #[arg(long, global = true)]
    config: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
enum DaemonAction {
    /// Starts the daemon in the background.
    Start,
    /// Stops a background daemon started by this CLI.
    Stop,
    /// Reports whether the background daemon is running.
    Status,
}

#[derive(Debug, Subcommand)]
enum StorageCommand {
    /// Creates a consistent .tar.gz backup without machine-bound secrets.
    Backup {
        destination: PathBuf,
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    /// Restores a backup into an empty, different data directory.
    Restore {
        source: PathBuf,
        destination: PathBuf,
    },
    /// Applies retention and enforces the artifact disk limit.
    Retain {
        #[arg(long, default_value_t = 90)]
        execution_days: i64,
        #[arg(long, default_value_t = 90)]
        event_days: i64,
        #[arg(long, default_value_t = 5 * 1024 * 1024 * 1024)]
        artifact_limit_bytes: u64,
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    /// Checkpoints WAL and compacts the SQLite database.
    Compact {
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    /// Pins or unpins an execution so retention will preserve it.
    Pin {
        execution_id: String,
        #[arg(long)]
        value: bool,
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
}

#[derive(Debug, Subcommand)]
enum ServiceCommand {
    /// Registers the automatic-boot service without starting it.
    #[command(
        long_about = "Installs the machine-wide service definition and leaves it stopped. Use an explicit absolute --data-dir accessible to the installed OS account. Equivalent definitions are no-ops; conflicting definitions are not rewritten. Requires the platform's administrative permission."
    )]
    Install {
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    /// Stops the service, then removes its native registration only.
    #[command(
        long_about = "Stops the Core and confirms it exited before removing the service definition. A failed or uncertain stop leaves the registration in place. Workflows, configuration, credentials, history, logs, and data are not deleted."
    )]
    Uninstall,
    /// Starts the installed service and waits for verified Core readiness.
    #[command(
        long_about = "Starts the verified installed definition and reports success only after its Core is ready. An absent service is an error; a running manager state alone is not proof of Core readiness."
    )]
    Start,
    /// Gracefully stops the service and confirms Core exit.
    #[command(
        long_about = "Requests service shutdown and confirms both Core exit and native stopped state. A timeout is not a claim that manager work was cancelled or that the Core exited."
    )]
    Stop,
    /// Reports read-only native installation and Core observations.
    #[command(
        long_about = "Observes registration, manager state, account, Core readiness, PID, data path, and journal location without creating or repairing service state. Unknown or inaccessible observations are errors, not absence."
    )]
    Status,
    #[command(hide = true)]
    Run {
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
}

#[derive(Debug, Subcommand)]
enum ContextCommand {
    Add {
        name: String,
        #[arg(long)]
        endpoint: String,
        #[arg(long = "name")]
        display_name: Option<String>,
        #[arg(long)]
        expected_core_id: Option<String>,
        #[arg(long)]
        color: Option<String>,
        #[arg(long)]
        credential_ref: Option<String>,
        #[arg(long)]
        contexts_file: Option<PathBuf>,
    },
    List {
        #[arg(long)]
        contexts_file: Option<PathBuf>,
    },
    Use {
        id: String,
        #[arg(long)]
        contexts_file: Option<PathBuf>,
    },
    Inspect {
        id: String,
        #[arg(long)]
        contexts_file: Option<PathBuf>,
    },
    Remove {
        id: String,
        #[arg(long)]
        contexts_file: Option<PathBuf>,
    },
    Import {
        source: PathBuf,
        #[arg(long)]
        contexts_file: Option<PathBuf>,
    },
    Export {
        name: String,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        replace: bool,
        #[arg(long)]
        contexts_file: Option<PathBuf>,
    },
}

#[derive(Debug, Subcommand)]
enum AuthCommand {
    /// Explicitly recovers lost local access by replacing active API tokens.
    Recover {
        #[arg(long)]
        data_dir: Option<PathBuf>,
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Starts a temporary QR pairing invitation and waits for local approval.
    Pair {
        #[arg(long)]
        data_dir: Option<PathBuf>,
        #[arg(long)]
        config: Option<PathBuf>,
        /// Reachable HTTPS endpoint for a remote Core; defaults to the local listener.
        #[arg(long)]
        endpoint: Option<String>,
        /// Allow the paired device to administer tokens and plugins.
        #[arg(long)]
        admin: bool,
        /// Invitation lifetime, from 30 to 600 seconds.
        #[arg(long, default_value_t = 300)]
        ttl_seconds: u64,
    },
    /// Lists device and API credentials without exposing token values.
    Tokens {
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    /// Revokes an API credential by its ID.
    Revoke {
        id: String,
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
}

#[derive(Debug, Subcommand)]
enum PluginCommand {
    /// Copies/downloads and statically inspects a plugin without executing it.
    Prepare {
        /// A local path, npm:<package>[@version], or git:<url>[#ref].
        source: String,
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    /// Activates exactly the staged content previously inspected by prepare.
    Commit {
        prepared_id: String,
        #[arg(long)]
        digest: String,
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    List {
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    Remove {
        id: String,
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
}

#[derive(Debug, Subcommand)]
enum ProviderCommand {
    /// Creates a provider profile from non-secret JSON configuration.
    Add {
        id: String,
        #[arg(long)]
        file: PathBuf,
        /// Print machine-readable JSON instead of the human-readable summary.
        #[arg(long)]
        json: bool,
    },
    /// Lists provider profiles without configuration or credential material.
    List {
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long)]
        cursor: Option<String>,
        /// Print machine-readable JSON instead of a human-readable table.
        #[arg(long)]
        json: bool,
    },
    /// Configures supported authentication without exposing values in argv.
    Auth {
        id: String,
        #[arg(long)]
        stdin: bool,
        /// Print machine-readable JSON instead of the human-readable summary.
        #[arg(long)]
        json: bool,
    },
    /// Reports bounded connectivity, authentication, and capability findings.
    Test {
        id: String,
        /// Print machine-readable JSON instead of human-readable findings.
        #[arg(long)]
        json: bool,
    },
    /// Shows configured models and whether live discovery is supported.
    Models {
        id: String,
        /// Print machine-readable JSON instead of a human-readable summary.
        #[arg(long)]
        json: bool,
    },
    /// Creates or updates a profile from a non-secret ProviderProfileUpsert JSON file.
    Upsert {
        json_file: PathBuf,
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    /// Shows the last persisted, non-secret diagnostic status for a profile.
    Status {
        id: String,
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    /// Removes a profile without removing its referenced secret.
    Remove {
        id: String,
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    /// Opens a browser to authorize ChatGPT Plus/Pro for a Codex profile.
    Login {
        id: String,
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
}

#[derive(Debug, Subcommand)]
enum McpCommand {
    /// Adds a named MCP connection from a non-secret JSON file.
    Add {
        name: String,
        #[arg(long)]
        file: PathBuf,
        /// Print machine-readable JSON instead of the human-readable summary.
        #[arg(long)]
        json: bool,
    },
    /// Lists named MCP connection metadata.
    List {
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long)]
        cursor: Option<String>,
        /// Print machine-readable JSON instead of a human-readable table.
        #[arg(long)]
        json: bool,
    },
    /// Inspects one named MCP connection without connecting to it.
    Inspect {
        name: String,
        /// Print machine-readable JSON instead of a human-readable summary.
        #[arg(long)]
        json: bool,
    },
    /// Checks configured MCP credentials or binds an existing HTTP secret reference.
    Auth {
        name: String,
        #[arg(long)]
        secret: Option<String>,
        /// Print machine-readable JSON instead of a human-readable outcome.
        #[arg(long)]
        json: bool,
    },
    /// Discovers or invokes one named tool.
    Tool {
        #[command(subcommand)]
        command: McpToolCommand,
    },
    /// Calls one MCP tool from a JSON configuration file using store secret references.
    Call {
        json_file: PathBuf,
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
}

#[derive(Debug, Subcommand)]
enum McpToolCommand {
    /// Discovers tools from a named connection.
    List {
        name: String,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long)]
        cursor: Option<String>,
        /// Print machine-readable JSON instead of a human-readable tool list.
        #[arg(long)]
        json: bool,
    },
    /// Validates and calls one named tool with strict JSON object input.
    Call {
        name: String,
        tool: String,
        #[arg(long)]
        input: PathBuf,
        /// Print machine-readable JSON instead of human-readable tool results.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
enum SecretCommand {
    /// Sets a secret from hidden terminal input or explicit bounded stdin.
    Set {
        name: String,
        #[arg(long)]
        stdin: bool,
        /// Print machine-readable JSON instead of the human-readable summary.
        #[arg(long)]
        json: bool,
    },
    /// Lists secret metadata without values.
    List {
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long)]
        cursor: Option<String>,
        /// Print machine-readable JSON instead of a human-readable table.
        #[arg(long)]
        json: bool,
    },
    /// Removes a secret reference without revealing its value.
    Remove {
        name: String,
        /// Print machine-readable JSON instead of the human-readable summary.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
enum WorkflowCommand {
    /// Creates a starter YAML definition without registering it.
    Create {
        name: String,
        #[arg(long)]
        output: Option<PathBuf>,
        #[arg(long)]
        replace: bool,
        #[arg(long)]
        json: bool,
    },
    /// Imports a validated YAML definition into the selected Core.
    #[command(
        long_about = "Imports a validated local YAML file into the selected Core. Imports create disabled registrations; replacement requires --replace and uses the currently observed revision unless --if-revision is supplied. Local files are never inferred from workflow names."
    )]
    Import {
        file: PathBuf,
        #[arg(long)]
        replace: bool,
        #[arg(long)]
        if_revision: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Exports one stored workflow's original YAML source.
    Export {
        workflow: String,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        replace: bool,
        #[arg(long)]
        json: bool,
    },
    #[command(
        long_about = "Validates either a stored workflow in the selected Core or an explicitly named local file with --file. Local checks are observational, do not execute nodes, and disclose that installed capability validation may be partial."
    )]
    Validate {
        workflow: Option<String>,
        #[arg(long)]
        file: Option<PathBuf>,
        #[arg(long)]
        data_dir: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    #[command(
        long_about = "Displays the public execution structure for a stored workflow in the selected Core or an explicitly named local file with --file. Planning does not execute nodes, providers, tools, or plugins."
    )]
    Plan {
        workflow: Option<String>,
        #[arg(long)]
        file: Option<PathBuf>,
        #[arg(long)]
        data_dir: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    List {
        #[arg(long)]
        data_dir: Option<PathBuf>,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long)]
        cursor: Option<String>,
        #[arg(long)]
        json: bool,
    },
    #[command(
        long_about = "Manually submits one stored workflow to the selected Core without changing its enabled/disabled trigger eligibility. Use --file together with global --standalone only for an isolated local execution."
    )]
    Run {
        workflow: Option<String>,
        #[arg(long)]
        file: Option<PathBuf>,
        #[arg(long)]
        request_key: Option<String>,
        #[arg(long)]
        timeout_seconds: Option<u64>,
        #[arg(long)]
        data_dir: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    #[command(
        long_about = "Enables trigger eligibility for a stored workflow selected by ID or unique name. This does not create or replace a workflow definition."
    )]
    Enable {
        workflow: String,
        #[arg(long)]
        if_revision: Option<String>,
        #[arg(long)]
        json: bool,
    },
    #[command(
        long_about = "Disables trigger eligibility for a stored workflow selected by ID or unique name. Executions already admitted remain valid."
    )]
    Disable {
        workflow: String,
        #[arg(long)]
        if_revision: Option<String>,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
enum ExecutionCommand {
    /// Lists one page of execution summaries.
    List {
        #[arg(long)]
        workflow_id: Option<String>,
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long)]
        cursor: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Inspects bounded execution details and availability.
    Inspect {
        execution_id: String,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long)]
        cursor: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Requests cooperative cancellation; optional wait observes for at most 30 seconds.
    Cancel {
        execution_id: String,
        #[arg(long)]
        wait_seconds: Option<u64>,
        #[arg(long)]
        json: bool,
    },
    /// Retrieves one finite, high-water execution log snapshot page.
    Logs {
        execution_id: String,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long)]
        cursor: Option<String>,
        #[arg(long)]
        high_water: Option<u64>,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
enum TriggerCommand {
    /// Lists one page of observed trigger projections.
    List {
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long)]
        cursor: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Inspects a composite workflow-id/trigger-id emitted by `trigger list`.
    Inspect {
        trigger_id: String,
        #[arg(long)]
        json: bool,
    },
}

fn main() -> ExitCode {
    let args = std::env::args_os().collect::<Vec<_>>();
    #[cfg(windows)]
    if args
        .get(1)
        .is_some_and(|argument| argument == "--internal-service-query")
    {
        let result = (|| {
            if args.len() != 3 {
                return Err(kakune_core::lifecycle::LifecycleError::UnverifiedProcess);
            }
            let id = args[2]
                .to_str()
                .ok_or(kakune_core::lifecycle::LifecycleError::UnverifiedProcess)?;
            let observation = kakune_core::lifecycle::service::windows::query_native_service(id)?;
            serde_json::to_writer(std::io::stdout().lock(), &observation)
                .map_err(|_| kakune_core::lifecycle::LifecycleError::UnverifiedProcess)
        })();
        return match result {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                use std::io::Write;
                let _ = std::io::stdout().lock().write_all(
                    kakune_core::lifecycle::service::helper::error_frame(&error).as_bytes(),
                );
                eprintln!(
                    "kakune: {}",
                    redact_lifecycle_diagnostic(&error.to_string())
                );
                ExitCode::FAILURE
            }
        };
    }
    if args
        .get(1)
        .is_some_and(|argument| argument == "--internal-service-probe")
    {
        if args.len() != 4 {
            return ExitCode::FAILURE;
        }
        let data_dir = PathBuf::from(&args[2]);
        let Some(instance_id) = args[3].to_str().and_then(|value| value.parse().ok()) else {
            return ExitCode::FAILURE;
        };
        return run_with_runtime(async {
            let clock = SystemMonotonicClock::default();
            let deadline = Deadline::after(&clock, kakune_core::lifecycle::HELPER_BUDGET);
            daemon::probe_system_service(&data_dir, instance_id, deadline, &clock)
                .await
                .map_err(|error| redact_lifecycle_diagnostic(&error.to_string()))
        });
    }
    if args
        .get(1)
        .is_some_and(|argument| argument == "--internal-service-helper")
    {
        let result = (|| {
            if args.len() != 5 {
                return Err("invalid internal service-helper invocation".to_owned());
            }
            let manager = match args[2].to_str() {
                Some("scm") => ServiceManagerKind::WindowsScm,
                Some("systemd") => ServiceManagerKind::LinuxSystemd,
                Some("launchd") => ServiceManagerKind::MacosLaunchd,
                _ => return Err("invalid internal native manager".to_owned()),
            };
            if manager != current_service_manager()? {
                return Err(
                    "internal helper manager does not match this operating system".to_owned(),
                );
            }
            let id = args[3]
                .to_str()
                .ok_or("invalid internal service identifier")?;
            let attempt = args[4]
                .to_str()
                .ok_or("invalid internal attempt identifier")?
                .parse()
                .map_err(|_| "invalid internal attempt identifier")?;
            kakune_core::lifecycle::service::helper::serve(
                manager,
                id,
                attempt,
                std::io::BufReader::new(std::io::stdin()),
                &mut std::io::stdout().lock(),
            )
            .map_err(|error| {
                use std::io::Write;
                let _ = std::io::stdout().lock().write_all(
                    kakune_core::lifecycle::service::helper::error_frame(&error).as_bytes(),
                );
                error.to_string()
            })
        })();
        return match result {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("kakune: {}", redact_lifecycle_diagnostic(&error));
                ExitCode::FAILURE
            }
        };
    }
    #[cfg(unix)]
    let internal_child = args
        .iter()
        .any(|argument| argument == "--internal-daemon-child");
    #[cfg(unix)]
    if internal_child && let Err(error) = kakune_core::lifecycle::process::detach_current_process()
    {
        eprintln!("kakune: cannot detach background daemon session: {error}");
        return ExitCode::FAILURE;
    }
    let Cli {
        context,
        context_file,
        ca,
        standalone,
        command,
    } = Cli::parse_from(args);
    match command {
        Command::Daemon(options) if options.internal_daemon_child => {
            run_with_runtime(serve_background_child(options))
        }
        command => {
            let cli = Cli {
                context,
                context_file,
                ca,
                standalone,
                command,
            };
            if let Err(error) = validate_dispatch_arguments(&cli) {
                eprintln!("kakune: {error}");
                return ExitCode::from(2);
            }
            run_with_runtime(execute(cli))
        }
    }
}

fn run_with_runtime(future: impl std::future::Future<Output = Result<(), String>>) -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("kakune: cannot initialize async runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(future) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) if error == DOCTOR_REPORT_FAILURE => ExitCode::FAILURE,
        Err(error) => {
            eprintln!("kakune: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn execute(cli: Cli) -> Result<(), String> {
    execute_with_credentials(cli, &SystemCredentialAccess).await
}

async fn execute_with_credentials(
    cli: Cli,
    credentials: &dyn CredentialAccess,
) -> Result<(), String> {
    if matches!(&cli.command, Command::Version) {
        print!("{}", Cli::command().render_long_version());
        return Ok(());
    }

    validate_dispatch_arguments(&cli)?;

    if matches!(
        &cli.command,
        Command::Secret {
            command: SecretCommand::Set { stdin: false, .. }
        }
    ) && !io::stdin().is_terminal()
    {
        return Err(
            "interactive terminal input is required; use --stdin for automation".to_owned(),
        );
    }

    let context_file = cli.context_file.clone();
    if cli.context.is_some() && matches!(&cli.command, Command::Init { .. }) {
        return Err("init is local-only and does not accept --context".to_string());
    }
    if cli.context.is_some() && matches!(&cli.command, Command::Daemon(_)) {
        return Err("daemon lifecycle commands are local and do not accept --context".to_string());
    }
    if cli.context.is_some() && matches!(&cli.command, Command::Service { .. }) {
        return Err("service lifecycle commands are local and do not accept --context".to_string());
    }
    if !cli.standalone
        && !matches!(&cli.command, Command::Auth { .. })
        && (cli.context.is_some() || matches!(&cli.command, Command::Run { .. }))
    {
        return execute_remote(&cli).await;
    }
    if !cli.standalone && dispatch_category(&cli.command) == DispatchCategory::CanonicalCore {
        return execute_remote(&cli).await;
    }
    let standalone = cli.standalone;
    match cli.command {
        Command::Init { data_dir, config } => {
            let paths =
                resolve_initialization_paths(data_dir, config, context_file, default_data_dir());
            let report = run_initialization_with(paths, credentials)?;
            println!("{}", render_init_report(&report));
        }
        Command::Version => unreachable!("version returns before setup and remote routing"),
        Command::Daemon(options) => execute_daemon(options, context_file)
            .await
            .map_err(|error| redact_lifecycle_diagnostic(&error))?,
        Command::Storage { command } => execute_storage(command)?,
        Command::Service { command } => execute_service(command).await?,
        Command::Doctor {
            data_dir,
            config,
            json,
        } => {
            let report = collect_doctor_report(
                data_dir.unwrap_or_else(default_data_dir),
                config,
                credentials,
            );
            if json {
                print_json(&report)?;
            } else {
                render_doctor_report(&report);
            }
            if report.exit_code != 0 {
                return Err(DOCTOR_REPORT_FAILURE.to_owned());
            }
        }
        Command::Context { command } => execute_context(command)?,
        Command::Auth { command } => execute_auth(command, context_file)?,
        Command::Workflow { command } => execute_workflow_local(command, standalone)?,
        Command::Execution { .. } | Command::Trigger { .. } => {
            return Err("execution and trigger commands require the selected Core".to_owned());
        }
        Command::Plugin { command } => match command {
            PluginCommand::Prepare { source, data_dir } => {
                let store = Store::open(data_dir.unwrap_or_else(default_data_dir))?;
                let prepared =
                    plugin_install::prepare(&store, PluginSource::parse(&source)?).await?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&prepared.record)
                        .map_err(|error| error.to_string())?
                );
            }
            PluginCommand::Commit {
                prepared_id,
                digest,
                data_dir,
            } => {
                let store = Store::open(data_dir.unwrap_or_else(default_data_dir))?;
                let installed = plugin_install::commit(&store, &prepared_id, &digest)?;
                println!(
                    "Installed {} {} ({})",
                    installed.id, installed.version, installed.digest
                );
            }
            PluginCommand::List { data_dir } => {
                let store = Store::open(data_dir.unwrap_or_else(default_data_dir))?;
                for plugin in store.list_installed_plugins()? {
                    println!("{}\t{}\t{}", plugin.id, plugin.version, plugin.name);
                }
            }
            PluginCommand::Remove { id, data_dir } => {
                let store = Store::open(data_dir.unwrap_or_else(default_data_dir))?;
                let plugin = store
                    .remove_plugin_install(&id)?
                    .ok_or_else(|| format!("plugin {id} is not installed"))?;
                let manifest = PathBuf::from(plugin.manifest_path);
                let root = store.data_dir().join("plugins");
                let directory = manifest
                    .parent()
                    .ok_or_else(|| "installed plugin manifest has no parent".to_string())?;
                if !directory.starts_with(&root) {
                    return Err("installed plugin path is outside the plugin directory".to_string());
                }
                fs::remove_dir_all(directory)
                    .map_err(|error| format!("cannot remove installed plugin: {error}"))?;
                println!("Removed {id}");
            }
        },
        Command::Provider { command } => execute_provider(command).await?,
        Command::Mcp { command } => match command {
            McpCommand::Call {
                json_file,
                data_dir,
            } => {
                let source = fs::read_to_string(&json_file)
                    .map_err(|_| "cannot read MCP call JSON file".to_string())?;
                let request: api::McpCallRequest = serde_json::from_str(&source)
                    .map_err(|_| "invalid MCP call JSON".to_string())?;
                let store = Store::open(data_dir.unwrap_or_else(default_data_dir))?;
                let response = api::call_mcp(&store, request)
                    .await
                    .map_err(|_| "MCP call could not be completed".to_string())?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&response)
                        .map_err(|_| "cannot encode MCP call response".to_string())?
                );
            }
            _ => return Err("named MCP commands require the selected Core".to_owned()),
        },
        Command::Secret { .. } => {
            return Err("secret commands require the selected Core".to_owned());
        }
        Command::Run { workflow, data_dir } => {
            let source = std::fs::read_to_string(&workflow)
                .map_err(|error| format!("cannot read {}: {error}", workflow.display()))?;
            let store = Store::open(data_dir.unwrap_or_else(default_data_dir))?;
            let plugins = load_installed_plugin_registry(&store)?;
            let analysis = analyze_workflow_with_plugins(&source, &plugins);
            if !analysis.is_valid() {
                return Err(analysis
                    .diagnostics
                    .into_iter()
                    .map(|diagnostic| diagnostic.message)
                    .collect::<Vec<_>>()
                    .join("; "));
            }
            let document = analysis
                .workflow
                .expect("a diagnostic-free analysis has a workflow");
            store.upsert_workflow(&document, &source, "enabled")?;
            let execution = run_workflow_with_plugins(&store, &document, &plugins)?;
            println!(
                "{}\t{}\t{}",
                execution.id, execution.workflow_name, execution.status
            );
        }
        Command::Executions { data_dir } => {
            let store = Store::open(data_dir.unwrap_or_else(default_data_dir))?;
            for execution in store.list_executions()? {
                println!(
                    "{}\t{}\t{}\t{}",
                    execution.id, execution.workflow_name, execution.status, execution.created_at
                );
            }
        }
        Command::Inspect {
            execution_id,
            data_dir,
        } => {
            let store = Store::open(data_dir.unwrap_or_else(default_data_dir))?;
            let execution = store
                .get_execution(&execution_id)?
                .ok_or_else(|| format!("execution {execution_id} was not found"))?;
            println!(
                "{}\t{}\t{}",
                execution.id, execution.workflow_name, execution.status
            );
            for node in store.list_node_runs(&execution_id)? {
                println!(
                    "{}\t{}\t{}",
                    node.node_id,
                    node.status,
                    node.message.unwrap_or_default()
                );
            }
        }
    }
    Ok(())
}

fn execute_workflow_local(command: WorkflowCommand, standalone: bool) -> Result<(), String> {
    match command {
        WorkflowCommand::Create {
            name,
            output,
            replace,
            json,
        } => {
            if name.trim().is_empty() {
                return Err("workflow name must not be empty".to_owned());
            }
            let destination = match output {
                Some(path) => path,
                None if safe_workflow_file_name(&name) => {
                    PathBuf::from(format!("{name}.kakune.yaml"))
                }
                None => {
                    return Err(
                        "path-like or unsafe workflow names require an explicit --output"
                            .to_owned(),
                    );
                }
            };
            let id = workflow_id_from_name(&name);
            let quoted_name = serde_json::to_string(&name).map_err(|error| error.to_string())?;
            let source = format!(
                "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: {id}\n  name: {quoted_name}\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: start\nnodes:\n  - id: start\n    type: kakune.flow.pass@1\n"
            );
            kakune_core::WorkflowDocument::parse(&source)?;
            atomic_publish_workflow_file(&destination, source.as_bytes(), replace)?;
            if json {
                print_json(&serde_json::json!({
                    "path": destination,
                    "workflowId": id,
                    "scope": "local-file",
                }))?;
            } else {
                println!(
                    "Created {} (local YAML file; not registered)",
                    destination.display()
                );
            }
            Ok(())
        }
        WorkflowCommand::Validate {
            file: Some(file),
            json,
            ..
        } => {
            let source = read_workflow_source(&file)?;
            let analysis = analyze_workflow_with_plugins(&source, &PluginRegistry::default());
            if !analysis.is_valid() {
                if json {
                    print_json(&analysis)?;
                }
                return Err(format_workflow_diagnostics(&analysis.diagnostics));
            }
            if json {
                print_json(&serde_json::json!({
                    "valid": true,
                    "scope": "local-file",
                    "workflow": analysis.workflow,
                    "diagnostics": analysis.diagnostics,
                }))?;
            } else {
                let name = analysis
                    .workflow
                    .as_ref()
                    .map(|workflow| workflow.metadata.name.as_str())
                    .unwrap_or("workflow");
                println!("{name} is valid (local-file validation)");
            }
            Ok(())
        }
        WorkflowCommand::Plan {
            file: Some(file),
            json,
            ..
        } => {
            let source = read_workflow_source(&file)?;
            let plan =
                kakune_core::management::plan_workflow(&source, None, &PluginRegistry::default());
            if !plan.diagnostics.is_empty() {
                if json {
                    print_json(&plan)?;
                }
                return Err(format_workflow_diagnostics(&plan.diagnostics));
            }
            if json {
                print_json(&serde_json::json!({ "scope": "local-file", "plan": plan }))?;
            } else {
                render_plan_view(&plan);
            }
            Ok(())
        }
        WorkflowCommand::Run {
            file: Some(file),
            timeout_seconds,
            data_dir,
            json,
            ..
        } if standalone => {
            let source = read_workflow_source(&file)?;
            let analysis = analyze_workflow_with_plugins(&source, &PluginRegistry::default());
            if !analysis.is_valid() {
                return Err(format_workflow_diagnostics(&analysis.diagnostics));
            }
            let (data_dir, remove_after) = match data_dir {
                Some(data_dir) => (data_dir, false),
                None => {
                    let temporary_root = std::env::temp_dir().join("opencode");
                    if !temporary_root.is_dir() {
                        return Err(format!(
                            "approved temporary directory {} is unavailable",
                            temporary_root.display()
                        ));
                    }
                    let data_dir =
                        temporary_root.join(format!("kakune-standalone-{}", uuid::Uuid::new_v4()));
                    fs::create_dir(&data_dir).map_err(|error| {
                        format!("cannot create temporary standalone data directory: {error}")
                    })?;
                    (data_dir, true)
                }
            };
            let store = match Store::open(data_dir.clone()) {
                Ok(store) => store,
                Err(error) => {
                    if remove_after && let Err(cleanup_error) = fs::remove_dir_all(&data_dir) {
                        return Err(format!(
                            "standalone store open failed: {error}; temporary cleanup failed at {}: {cleanup_error}",
                            data_dir.display()
                        ));
                    }
                    return Err(error);
                }
            };
            let result = kakune_core::run_standalone_workflow(
                &store,
                &source,
                &PluginRegistry::default(),
                timeout_seconds,
            );
            drop(store);
            let execution = match result {
                Ok(execution) => execution,
                Err(error) => {
                    if remove_after && let Err(cleanup_error) = fs::remove_dir_all(&data_dir) {
                        return Err(format!(
                            "{error}; temporary standalone runtime cleanup failed at {}: {cleanup_error}",
                            data_dir.display()
                        ));
                    }
                    return Err(error);
                }
            };
            if remove_after {
                fs::remove_dir_all(&data_dir).map_err(|error| {
                    format!(
                        "standalone execution {} completed but temporary data cleanup failed at {}: {error}",
                        execution.id,
                        data_dir.display()
                    )
                })?;
            }
            if json {
                print_json(&execution)?;
            } else {
                println!(
                    "{}\t{}\t{} (standalone/local)",
                    execution.id, execution.workflow_name, execution.status
                );
            }
            Ok(())
        }
        WorkflowCommand::Validate {
            workflow: Some(_), ..
        }
        | WorkflowCommand::Plan {
            workflow: Some(_), ..
        }
        | WorkflowCommand::Import { .. }
        | WorkflowCommand::Export { .. }
        | WorkflowCommand::List { .. }
        | WorkflowCommand::Run {
            workflow: Some(_), ..
        }
        | WorkflowCommand::Enable { .. }
        | WorkflowCommand::Disable { .. } => {
            Err("this workflow operation requires the selected Core context".to_owned())
        }
        WorkflowCommand::Validate { .. } | WorkflowCommand::Plan { .. } => {
            Err("local workflow checks require --file".to_owned())
        }
        WorkflowCommand::Run { .. } => {
            Err("standalone workflow run requires --file and --standalone".to_owned())
        }
    }
}

fn read_workflow_source(path: &Path) -> Result<String, String> {
    let mut file = fs::File::open(path)
        .map_err(|error| format!("cannot read workflow file {}: {error}", path.display()))?;
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(1_048_577)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("cannot read workflow file {}: {error}", path.display()))?;
    if bytes.len() > 1_048_576 {
        return Err("workflow source exceeds the 1 MiB limit".to_owned());
    }
    String::from_utf8(bytes).map_err(|_| "workflow source must be valid UTF-8".to_owned())
}

fn safe_workflow_file_name(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn workflow_id_from_name(name: &str) -> String {
    let mut id = String::new();
    let mut previous_dash = false;
    for character in name.chars().flat_map(char::to_lowercase) {
        if character.is_ascii_lowercase() || character.is_ascii_digit() {
            id.push(character);
            previous_dash = false;
        } else if !previous_dash && !id.is_empty() {
            id.push('-');
            previous_dash = true;
        }
        if id.len() >= 63 {
            break;
        }
    }
    while id.ends_with('-') {
        id.pop();
    }
    if id.is_empty() || !id.starts_with(|character: char| character.is_ascii_lowercase()) {
        id.insert_str(0, "workflow-");
    }
    id.truncate(63);
    id
}

fn atomic_publish_workflow_file(
    destination: &Path,
    contents: &[u8],
    replace: bool,
) -> Result<(), String> {
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    if !parent.is_dir() {
        return Err(format!(
            "destination directory {} does not exist",
            parent.display()
        ));
    }
    if !replace && fs::symlink_metadata(destination).is_ok() {
        return Err(format!(
            "destination {} already exists; pass --replace to replace it",
            destination.display()
        ));
    }
    let mut temporary = None;
    for _ in 0..8 {
        let candidate = parent.join(format!(".kakune-workflow-{}.tmp", uuid::Uuid::new_v4()));
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(mut file) => {
                if let Err(error) = file.write_all(contents).and_then(|()| file.sync_all()) {
                    let _ = fs::remove_file(&candidate);
                    return Err(format!("cannot prepare workflow output: {error}"));
                }
                temporary = Some(candidate);
                break;
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(format!("cannot create workflow output temporary: {error}")),
        }
    }
    let temporary =
        temporary.ok_or_else(|| "cannot allocate a workflow output temporary".to_owned())?;
    let publish = if replace {
        fs::rename(&temporary, destination)
    } else {
        fs::hard_link(&temporary, destination)
    };
    if let Err(error) = publish {
        let _ = fs::remove_file(&temporary);
        return Err(format!(
            "cannot atomically publish workflow output {}: {error}",
            destination.display()
        ));
    }
    if !replace {
        let _ = fs::remove_file(&temporary);
    }
    Ok(())
}

fn format_workflow_diagnostics(diagnostics: &[kakune_core::Diagnostic]) -> String {
    diagnostics
        .iter()
        .map(|diagnostic| diagnostic.message.as_str())
        .collect::<Vec<_>>()
        .join("; ")
}

fn render_plan_view(plan: &kakune_core::PlanView) {
    println!(
        "Plan for {} (local; capability validation may be partial)",
        plan.workflow_id.as_deref().unwrap_or("invalid workflow")
    );
    for scope in &plan.scopes {
        println!("Scope {} ({}) entry={}", scope.id, scope.kind, scope.entry);
        for node in &scope.nodes {
            let dependencies = if node.data_dependencies.is_empty() {
                "none".to_owned()
            } else {
                node.data_dependencies.join(",")
            };
            println!(
                "  {} [{}] data-dependencies={dependencies}",
                node.id, node.node_type
            );
            for (route, targets) in &node.routes {
                println!("    route {route} -> {}", targets.join(","));
            }
        }
    }
    println!(
        "Bounds: {} nodes (max {}), {} control edges (max {}), nesting {} (max {})",
        plan.bounds.node_count,
        plan.bounds.maximum_nodes,
        plan.bounds.control_edge_count,
        plan.bounds.maximum_control_edges,
        plan.bounds.nesting_depth,
        plan.bounds.maximum_nesting_depth
    );
}

fn resolve_initialization_paths(
    data_dir: Option<PathBuf>,
    config: Option<PathBuf>,
    context_file: Option<PathBuf>,
    default_data_dir: PathBuf,
) -> InitializationPaths {
    let data_dir = data_dir.unwrap_or(default_data_dir);
    InitializationPaths {
        config_file: config.unwrap_or_else(|| data_dir.join("kakune.yaml")),
        context_file: context_file.unwrap_or_else(|| default_contexts_path(data_dir.clone())),
        data_dir,
    }
}

fn render_plan_json(plan: &serde_json::Value) {
    use serde_json::Value;

    println!(
        "Plan for {} (selected Core)",
        plan.get("workflowId")
            .and_then(Value::as_str)
            .unwrap_or("workflow")
    );
    for scope in plan
        .get("scopes")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        println!(
            "Scope {} ({}) entry={}",
            scope.get("id").and_then(Value::as_str).unwrap_or(""),
            scope.get("kind").and_then(Value::as_str).unwrap_or(""),
            scope.get("entry").and_then(Value::as_str).unwrap_or("")
        );
        for node in scope
            .get("nodes")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let dependencies = node
                .get("dataDependencies")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(",");
            println!(
                "  {} [{}] data-dependencies={}",
                node.get("id").and_then(Value::as_str).unwrap_or(""),
                node.get("type").and_then(Value::as_str).unwrap_or(""),
                if dependencies.is_empty() {
                    "none"
                } else {
                    &dependencies
                }
            );
        }
    }
    if let Some(bounds) = plan.get("bounds") {
        println!(
            "Bounds: nodes={}/{} control-edges={}/{} nesting={}/{}",
            bounds.get("nodeCount").and_then(Value::as_u64).unwrap_or(0),
            bounds
                .get("maximumNodes")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            bounds
                .get("controlEdgeCount")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            bounds
                .get("maximumControlEdges")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            bounds
                .get("nestingDepth")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            bounds
                .get("maximumNestingDepth")
                .and_then(Value::as_u64)
                .unwrap_or(0)
        );
    }
}

fn run_initialization_with(
    paths: InitializationPaths,
    credentials: &dyn CredentialAccess,
) -> Result<InitializationReport, String> {
    initialize(paths, credentials).map_err(render_init_error)
}

fn render_init_error(error: InitializationError) -> String {
    let report = &error.report;
    let mut message = format!(
        "initialization incomplete: {} for {}; {}\nData directory: {} ({})\nConfiguration: {} ({})\nStorage: {}\nInstallation identity: {}\nClient access: {}\nConnection file: {} ({})",
        error.category,
        error.resource,
        error.correction,
        report.paths.data_dir.display(),
        component_state_name(report.storage),
        report.paths.config_file.display(),
        component_state_name(report.configuration),
        component_state_name(report.storage),
        component_state_name(report.identity),
        component_state_name(report.client_access),
        report.paths.context_file.display(),
        component_state_name(report.local_context),
    );
    if error.correction == InitializationCorrection::RunExplicitAuthRecovery {
        message.push_str(&format!(
            "\nRun explicit recovery for these local paths: kakune --context-file \"{}\" auth recover --data-dir \"{}\" --config \"{}\"",
            report.paths.context_file.display(),
            report.paths.data_dir.display(),
            report.paths.config_file.display(),
        ));
    }
    message
}

fn render_init_report(report: &InitializationReport) -> String {
    format!(
        "Kakune local initialization complete.\nData directory: {} ({})\nConfiguration: {} ({})\nStorage: {}\nInstallation identity: {}\nClient access: {}\nLocal connection: {} ({})",
        report.paths.data_dir.display(),
        component_state_name(report.storage),
        report.paths.config_file.display(),
        component_state_name(report.configuration),
        component_state_name(report.storage),
        component_state_name(report.identity),
        component_state_name(report.client_access),
        report.paths.context_file.display(),
        component_state_name(report.local_context),
    )
}

fn component_state_name(state: ComponentState) -> &'static str {
    match state {
        ComponentState::NotAttempted => "not attempted",
        ComponentState::Created => "created",
        ComponentState::Reused => "reused",
        ComponentState::Incomplete => "incomplete",
    }
}

async fn execute_service(command: ServiceCommand) -> Result<(), String> {
    match command {
        ServiceCommand::Run { data_dir } => {
            current_service_manager()?;
            let data_dir = absolute_path(data_dir.unwrap_or_else(default_data_dir))?;
            #[cfg(windows)]
            {
                windows_service_host::run(data_dir)
            }
            #[cfg(not(windows))]
            {
                serve_system_service(data_dir).await
            }
        }
        ServiceCommand::Install { data_dir } => {
            let data_dir = absolute_path(data_dir.unwrap_or_else(default_data_dir))?;
            let definition = ServiceDefinition::for_platform(&data_dir)
                .map_err(|error| format!("cannot define Kakune system service: {error}"))?;
            let controller = system_service_controller()?;
            let manager = definition.manager;
            let service_id = definition.service_id.as_str();
            let result = controller.install(&definition).map_err(|error| {
                render_service_operation_error(&controller, manager, service_id, "install", error)
            })?;
            print_service_result(&result, "install");
            Ok(())
        }
        ServiceCommand::Start => {
            let controller = system_service_controller()?;
            let manager = current_service_manager()?;
            let service_id = manager.platform_service_id();
            let result = controller.start(manager, service_id).map_err(|error| {
                render_service_operation_error(&controller, manager, service_id, "start", error)
            })?;
            print_service_result(&result, "start");
            Ok(())
        }
        ServiceCommand::Stop => {
            let controller = system_service_controller()?;
            let manager = current_service_manager()?;
            let service_id = manager.platform_service_id();
            let result = controller.stop(manager, service_id).map_err(|error| {
                render_service_operation_error(&controller, manager, service_id, "stop", error)
            })?;
            print_service_result(&result, "stop");
            Ok(())
        }
        ServiceCommand::Uninstall => {
            let controller = system_service_controller()?;
            let manager = current_service_manager()?;
            let service_id = manager.platform_service_id();
            let result = controller.uninstall(manager, service_id).map_err(|error| {
                render_service_operation_error(&controller, manager, service_id, "uninstall", error)
            })?;
            print_service_result(&result, "uninstall");
            Ok(())
        }
        ServiceCommand::Status => {
            let controller = system_service_controller()?;
            let manager = current_service_manager()?;
            let service_id = manager.platform_service_id();
            let observation = controller.status(manager, service_id).map_err(|error| {
                render_service_operation_error(&controller, manager, service_id, "status", error)
            })?;
            print_service_observation(&observation);
            if let Some(detail) = observation.core_unknown {
                return Err(format!(
                    "service status is incomplete because Core ownership is unknown: {detail}"
                ));
            }
            if observation.pending_operation {
                return Err(
                    "service status has an unresolved native-manager operation; inspect the manager diagnostic and journal before retrying".to_owned(),
                );
            }
            Ok(())
        }
    }
}

fn current_service_manager() -> Result<ServiceManagerKind, String> {
    if cfg!(windows) {
        Ok(ServiceManagerKind::WindowsScm)
    } else if cfg!(target_os = "linux") {
        Ok(ServiceManagerKind::LinuxSystemd)
    } else if cfg!(target_os = "macos") {
        Ok(ServiceManagerKind::MacosLaunchd)
    } else {
        Err("native system services are unsupported on this operating system".to_owned())
    }
}

fn system_service_controller() -> Result<ServiceController, String> {
    let manager = kakune_core::lifecycle::service::system_manager()
        .map_err(|error| render_service_error("initialize manager", error))?;
    ServiceController::for_system(manager)
        .map_err(|error| render_service_error("initialize service state", error))
}

fn render_service_error(action: &str, error: kakune_core::lifecycle::LifecycleError) -> String {
    let remediation = match &error {
        kakune_core::lifecycle::LifecycleError::AccessDenied(_) => {
            " Run this command from an elevated administrator terminal (Windows) or as root (Unix)."
        }
        kakune_core::lifecycle::LifecycleError::ManagerUnavailable(_) => {
            " Confirm the native service manager is available and running."
        }
        kakune_core::lifecycle::LifecycleError::UnresolvedServiceAttempt(_) => {
            " Inspect the recorded service attempt and native manager state before retrying."
        }
        kakune_core::lifecycle::LifecycleError::ServiceNotInstalled => {
            " Run kakune service install --data-dir <absolute-data-directory> before starting."
        }
        kakune_core::lifecycle::LifecycleError::ConflictingServiceDefinition(_) => {
            " Review the existing definition. For an old installation, stop/uninstall it with its original version or native manager, then reinstall while retaining data and credentials."
        }
        kakune_core::lifecycle::LifecycleError::UnverifiedProcess
        | kakune_core::lifecycle::LifecycleError::WrongOwner
        | kakune_core::lifecycle::LifecycleError::AuthenticationFailed
        | kakune_core::lifecycle::LifecycleError::UnsafeOwnership(_)
        | kakune_core::lifecycle::LifecycleError::UnsupportedSchema(_) => {
            " Preserve ownership evidence and verify the installed account, executable, instance and data access; do not signal a numeric PID or regenerate credentials."
        }
        kakune_core::lifecycle::LifecycleError::ModeNotControllable(_)
        | kakune_core::lifecycle::LifecycleError::OwnershipBusy => {
            " Use the owning foreground terminal, current-user daemon, or native service manager as appropriate; do not remove its locks or data."
        }
        kakune_core::lifecycle::LifecycleError::DeadlineExpired { .. }
        | kakune_core::lifecycle::LifecycleError::ServiceStateUnconfirmed
        | kakune_core::lifecycle::LifecycleError::HelperUnreaped { .. } => {
            " Completion is unconfirmed. Inspect remaining processes, manager jobs and recovery evidence; native termination never proves Core drain."
        }
        _ => "",
    };
    format!(
        "service {action} failed: {}.{remediation}",
        redact_lifecycle_diagnostic(&error.to_string())
    )
}

fn render_service_operation_error(
    controller: &ServiceController,
    manager: ServiceManagerKind,
    service_id: &str,
    action: &str,
    error: kakune_core::lifecycle::LifecycleError,
) -> String {
    let mut message = render_service_error(action, error);
    if let Ok(path) = controller.attempt_path_for(manager, service_id) {
        message.push_str(&format!(
            " Attempt journal (if present): {}. Inspect it and the native manager before retrying; an uncertain request is not reported as cancelled.",
            path.display()
        ));
    }
    // Do not start another status/helper budget after failure. Reading existing
    // protected filesystem evidence does not reconcile or cancel manager work.
    message.push_str(
        " Current resources: unknown; no post-failure manager observation was requested.",
    );
    match controller.read_attempt_for(manager, service_id) {
        Ok(Some(journal)) => {
            message.push_str(&render_service_recovery_snapshot(&journal));
        }
        Ok(None) => message.push_str(
            " No readable attempt snapshot is present; changed/remaining resources are unconfirmed, not empty. Preserve data and inspect the native manager before retrying.",
        ),
        Err(error) => message.push_str(&format!(
            " Recovery evidence is inaccessible or invalid: {}. Changed/remaining resources are unconfirmed; preserve the journal and data for inspection.",
            redact_lifecycle_diagnostic(&error.to_string())
        )),
    }
    message
}

fn render_service_recovery_snapshot(
    journal: &kakune_core::lifecycle::service::ServiceAttemptJournal,
) -> String {
    let observed = journal.completion_observation.as_ref().map_or_else(
        || "no completion observation".to_owned(),
        |observation| {
            format!(
                "installed={:?}, manager={:?}",
                observation.installed, observation.manager_state
            )
        },
    );
    let snapshot = format!(
        " Recorded attempt snapshot (historical, not current state): attempt={}, action={:?}, step={}, status={:?}, data={}, helperPid={:?}. Last recorded completion: {}. Recorded residual resources: {}. Recorded recovery guidance: {}. Changed resources may include accepted manager work; helper exit does not prove cancellation. Do not delete data, stable locks, or unresolved journals.",
        journal.attempt_id,
        journal.action,
        journal.step,
        journal.status,
        journal.expected_definition.data_dir.display(),
        journal.helper_pid,
        observed,
        if journal.residual_resources.is_empty() {
            "unconfirmed".to_owned()
        } else {
            journal.residual_resources.join("; ")
        },
        journal.recovery_guidance.join("; "),
    );
    redact_lifecycle_diagnostic(&snapshot)
}

fn redact_lifecycle_diagnostic(input: &str) -> String {
    // Preserve line boundaries until multiline credentials and complete cookie
    // headers have been removed. Token-by-token filtering cannot protect them.
    let mut inside_private_key = false;
    let mut lines = Vec::new();
    for line in input.lines() {
        if line.contains("-----BEGIN ") && line.contains("PRIVATE KEY-----") {
            inside_private_key = true;
            lines.push("[REDACTED PRIVATE KEY]");
        }
        if inside_private_key {
            if line.contains("-----END ") && line.contains("PRIVATE KEY-----") {
                inside_private_key = false;
            }
            continue;
        }
        if line.split_once(':').is_some_and(|(key, _)| {
            key.trim().eq_ignore_ascii_case("cookie")
                || key.trim().eq_ignore_ascii_case("set-cookie")
        }) {
            lines.push("[REDACTED COOKIE HEADER]");
        } else {
            lines.push(line);
        }
    }
    let filtered = lines.join(" ");
    let words = filtered.split_whitespace().collect::<Vec<_>>();
    let mut safe_words = Vec::with_capacity(words.len());
    let mut index = 0;
    while index < words.len() {
        let word = words[index];
        let normalized = word
            .trim_matches(|character: char| !character.is_ascii_alphanumeric() && character != '-')
            .to_ascii_lowercase();
        if normalized == "bearer" || normalized == "basic" {
            safe_words.push(word.to_owned());
            if index + 1 < words.len() {
                safe_words.push("[REDACTED]".to_owned());
                index += 2;
            } else {
                index += 1;
            }
            continue;
        }

        if let Some(delimiter) = word.find(['=', ':']) {
            let key = &word[..delimiter];
            if is_sensitive_diagnostic_key(key) {
                let value = &word[delimiter + 1..];
                safe_words.push(format!("{key}=[REDACTED]"));
                index += 1;
                if value.is_empty() {
                    if words.get(index).is_some_and(|next| {
                        let next = next.trim_matches(|character: char| {
                            !character.is_ascii_alphanumeric() && character != '-'
                        });
                        next.eq_ignore_ascii_case("bearer") || next.eq_ignore_ascii_case("basic")
                    }) {
                        index += 1;
                    }
                    if index < words.len() {
                        index += 1;
                    }
                } else if (value.eq_ignore_ascii_case("bearer")
                    || value.eq_ignore_ascii_case("basic"))
                    && index < words.len()
                {
                    index += 1;
                }
                continue;
            }
        }

        safe_words.push(word.to_owned());
        index += 1;
    }
    safe_words.join(" ")
}

fn is_sensitive_diagnostic_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase().replace('-', "_");
    [
        "authorization",
        "token",
        "secret",
        "capability",
        "password",
        "credential",
        "private_key",
        "api_key",
    ]
    .iter()
    .any(|sensitive| normalized.contains(sensitive))
}

fn print_service_result(result: &ServiceOperationResult, action: &str) {
    println!("action\t{action}");
    println!("result\t{}", service_outcome_name(result.operation.outcome));
    println!(
        "operationDataDir\t{}",
        result
            .operation
            .data_dir
            .as_deref()
            .map_or_else(|| "unknown".to_owned(), |path| path.display().to_string())
    );
    println!(
        "resourcesChanged\t{}",
        result.operation.changed_resources.join(",")
    );
    println!(
        "resourcesRemaining\t{}",
        result.operation.remaining_resources.join(",")
    );
    println!(
        "recoveryGuidance\t{}",
        result.operation.recovery_guidance.join("; ")
    );
    println!(
        "platformLimitations\t{}",
        result.operation.platform_completion_limitations.join("; ")
    );
    println!(
        "applicationWaitMs\t{}",
        result.operation.elapsed_application_wait.as_millis()
    );
    print_service_observation(&result.observation);
}

fn print_service_observation(observation: &NativeServiceObservation) {
    println!("installed\t{:?}", observation.installed);
    println!("managerState\t{:?}", observation.manager_state);
    println!("bootEnabled\t{}", observation.boot_enabled);
    println!(
        "account\t{}",
        observation.account.as_deref().unwrap_or("unknown")
    );
    println!(
        "coreState\t{}",
        observation
            .core
            .as_ref()
            .map_or("unknown", |core| observed_state_name(core.state))
    );
    println!(
        "pid\t{}",
        observation
            .core
            .as_ref()
            .filter(|core| core.identity_verified)
            .and_then(|core| core.process.as_ref())
            .map_or_else(|| "unknown".to_owned(), |process| process.pid.to_string())
    );
    println!(
        "dataDir\t{}",
        observation.definition.as_ref().map_or_else(
            || "unknown".to_owned(),
            |definition| definition.data_dir.display().to_string()
        )
    );
    println!(
        "diagnostic\t{}",
        observation
            .diagnostic_location
            .as_deref()
            .unwrap_or("unavailable")
    );
    println!("pendingOperation\t{}", observation.pending_operation);
}

fn service_outcome_name(outcome: kakune_core::lifecycle::OperationOutcome) -> &'static str {
    match outcome {
        kakune_core::lifecycle::OperationOutcome::Success => "success",
        kakune_core::lifecycle::OperationOutcome::Noop => "no-op",
        kakune_core::lifecycle::OperationOutcome::Failure => "failure",
    }
}

fn observed_state_name(state: kakune_core::lifecycle::ObservedState) -> &'static str {
    match state {
        kakune_core::lifecycle::ObservedState::Starting => "starting",
        kakune_core::lifecycle::ObservedState::Ready => "ready",
        kakune_core::lifecycle::ObservedState::Stopping => "stopping",
        kakune_core::lifecycle::ObservedState::Stopped => "stopped",
        kakune_core::lifecycle::ObservedState::Failed => "failed",
        kakune_core::lifecycle::ObservedState::Unknown => "unknown",
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ServiceHostState {
    Running,
    Stopping,
    Stopped,
}

type ServiceStateReporter = Arc<dyn Fn(ServiceHostState) -> Result<(), String> + Send + Sync>;

#[cfg(not(windows))]
async fn serve_system_service(data_dir: PathBuf) -> Result<(), String> {
    serve_system_service_with_host(data_dir, None, None).await
}

async fn serve_system_service_with_host(
    data_dir: PathBuf,
    shutdown: Option<tokio::sync::oneshot::Receiver<()>>,
    state_reporter: Option<ServiceStateReporter>,
) -> Result<(), String> {
    let options = DaemonArgs {
        tls_cert: None,
        tls_key: None,
        allowed_host: Vec::new(),
        internal_daemon_child: false,
        internal_log: None,
        action: None,
        listen: None,
        data_dir: Some(data_dir),
        config: None,
    };
    serve_core(options, false, true, shutdown, state_reporter).await
}

fn execute_storage(command: StorageCommand) -> Result<(), String> {
    match command {
        StorageCommand::Backup {
            destination,
            data_dir,
        } => {
            let store = Store::open(data_dir.unwrap_or_else(default_data_dir))?;
            print_json(&store.backup_to(&destination)?)?;
        }
        StorageCommand::Restore {
            source,
            destination,
        } => print_json(&Store::restore_backup(&source, &destination)?)?,
        StorageCommand::Retain {
            execution_days,
            event_days,
            artifact_limit_bytes,
            data_dir,
        } => {
            let store = Store::open(data_dir.unwrap_or_else(default_data_dir))?;
            print_json(&store.apply_retention(&RetentionPolicy {
                execution_days,
                event_days,
                artifact_limit_bytes,
            })?)?;
        }
        StorageCommand::Compact { data_dir } => {
            let store = Store::open(data_dir.unwrap_or_else(default_data_dir))?;
            store.compact()?;
            println!("SQLite compacted");
        }
        StorageCommand::Pin {
            execution_id,
            value,
            data_dir,
        } => {
            let store = Store::open(data_dir.unwrap_or_else(default_data_dir))?;
            if !store.set_execution_pinned(&execution_id, value)? {
                return Err(format!("execution {execution_id} was not found"));
            }
            println!("execution {execution_id} pinned={value}");
        }
    }
    Ok(())
}

fn print_json(value: &impl serde::Serialize) -> Result<(), String> {
    println!(
        "{}",
        serde_json::to_string_pretty(value).map_err(|error| error.to_string())?
    );
    Ok(())
}

async fn execute_provider(command: ProviderCommand) -> Result<(), String> {
    match command {
        ProviderCommand::List { .. } => {
            let store = Store::open(default_data_dir())?;
            for profile in store.list_provider_profiles()? {
                print_provider_summary(&profile);
            }
        }
        ProviderCommand::Add { .. }
        | ProviderCommand::Auth { .. }
        | ProviderCommand::Test { .. }
        | ProviderCommand::Models { .. } => {
            return Err("provider administration commands require the selected Core".to_owned());
        }
        ProviderCommand::Upsert {
            json_file,
            data_dir,
        } => {
            let source = fs::read_to_string(&json_file)
                .map_err(|error| format!("cannot read {}: {error}", json_file.display()))?;
            let profile: ProviderProfileUpsert = serde_json::from_str(&source)
                .map_err(|error| format!("invalid provider profile JSON: {error}"))?;
            let store = Store::open(data_dir.unwrap_or_else(default_data_dir))?;
            let profile = store.upsert_provider_profile(profile)?;
            print_provider_summary(&profile);
        }
        ProviderCommand::Status { id, data_dir } => {
            let store = Store::open(data_dir.unwrap_or_else(default_data_dir))?;
            let profile = store
                .get_provider_profile(&id)?
                .ok_or_else(|| format!("provider profile {id} was not found"))?;
            println!(
                "{}\t{}\t{}",
                profile.id,
                provider_status_name(&profile.diagnostic.status),
                profile.diagnostic.checked_at.as_deref().unwrap_or("never")
            );
        }
        ProviderCommand::Remove { id, data_dir } => {
            let store = Store::open(data_dir.unwrap_or_else(default_data_dir))?;
            store
                .delete_provider_profile(&id)?
                .ok_or_else(|| format!("provider profile {id} was not found"))?;
            println!("Removed {id}");
        }
        ProviderCommand::Login { id, data_dir } => {
            let store = Store::open(data_dir.unwrap_or_else(default_data_dir))?;
            let profile = store
                .get_provider_profile(&id)?
                .ok_or_else(|| format!("provider profile {id} was not found"))?;
            let secret_ref = match (&profile.provider_type, &profile.auth) {
                (ProviderType::Codex, ProviderAuth::OAuthSecret { secret_ref }) => secret_ref,
                _ => {
                    return Err(
                        "provider login requires a Codex profile using oauthSecret".to_string()
                    );
                }
            };
            let mut tokens = kakune_core::codex::login_with_browser()
                .await
                .map_err(|error| error.to_string())?;
            let encoded = tokens.to_secret().map_err(|error| error.to_string());
            tokens.zeroize_secrets();
            let mut encoded = Zeroizing::new(encoded?);
            let saved = store.set_secret(secret_ref, encoded.as_str());
            encoded.zeroize();
            saved?;
            store.diagnose_provider_profile(
                &profile.id,
                ProviderProfileDiagnostic {
                    status: ProviderProfileStatus::Available,
                    checked_at: None,
                    message: Some("ChatGPT OAuth login completed".to_string()),
                    details: Some(serde_json::json!({"check":"chatgptOAuth","available":true})),
                },
            )?;
            println!("ChatGPT OAuth login completed for profile {}", profile.id);
        }
    }
    Ok(())
}

fn print_provider_summary(profile: &ProviderProfile) {
    println!(
        "{}\t{}\t{}\t{}\t{}",
        profile.id,
        profile.display_name,
        provider_type_name(&profile.provider_type),
        provider_auth_name(&profile.auth),
        provider_status_name(&profile.diagnostic.status),
    );
}

fn provider_type_name(provider_type: &ProviderType) -> &'static str {
    match provider_type {
        ProviderType::MiniMax => "minimax",
        ProviderType::Codex => "codex",
    }
}

fn provider_auth_name(auth: &ProviderAuth) -> &'static str {
    match auth {
        ProviderAuth::ApiKeySecret { .. } => "apiKey",
        ProviderAuth::OAuthSecret { .. } => "oauthSecret",
    }
}

fn provider_status_name(status: &ProviderProfileStatus) -> &'static str {
    match status {
        ProviderProfileStatus::Unknown => "unknown",
        ProviderProfileStatus::Available => "available",
        ProviderProfileStatus::Unavailable => "unavailable",
    }
}

async fn execute_daemon(options: DaemonArgs, context_file: Option<PathBuf>) -> Result<(), String> {
    let DaemonArgs {
        tls_cert,
        tls_key,
        allowed_host,
        internal_daemon_child,
        internal_log,
        action,
        listen,
        data_dir,
        config,
    } = options;
    if internal_daemon_child {
        return Err(
            "internal daemon child dispatch must occur before normal CLI routing".to_owned(),
        );
    }
    let data_dir = absolute_path(data_dir.unwrap_or_else(default_data_dir))?;
    match action {
        None => {
            serve_foreground(DaemonArgs {
                tls_cert,
                tls_key,
                allowed_host,
                internal_daemon_child: false,
                internal_log,
                action: None,
                listen,
                data_dir: Some(data_dir),
                config,
            })
            .await
        }
        Some(DaemonAction::Status) => {
            let observation = daemon::observe_controlled(&data_dir)
                .await
                .map_err(|error| format!("cannot observe daemon: {error}"))?;
            println!("{}", render_daemon_observation(&observation));
            if observation.state == kakune_core::lifecycle::ObservedState::Unknown {
                Err(observation
                    .detail
                    .unwrap_or_else(|| "daemon state is unknown".to_owned()))
            } else {
                Ok(())
            }
        }
        Some(DaemonAction::Stop) => {
            let result = daemon::stop(&data_dir)
                .await
                .map_err(|error| format!("daemon stop failed: {error}"))?;
            println!(
                "daemon\tstate=stopped\tresult={}\tdataDir={}",
                if result.outcome == kakune_core::lifecycle::OperationOutcome::Noop {
                    "noop"
                } else {
                    "stopped"
                },
                result.data_dir.as_deref().unwrap_or(&data_dir).display()
            );
            Ok(())
        }
        Some(DaemonAction::Start) => {
            let mut loaded = CoreConfig::load_or_create(&data_dir, config)?;
            if let Some(certificate) = tls_cert {
                loaded.config.api.tls_cert = Some(certificate);
            }
            if let Some(private_key) = tls_key {
                loaded.config.api.tls_key = Some(private_key);
            }
            if !allowed_host.is_empty() {
                loaded.config.api.allowed_hosts = allowed_host;
            }
            loaded.config.validate()?;
            let listen = listen.unwrap_or(loaded.config.listen_addr()?);
            if !listen.ip().is_loopback() && loaded.config.api.tls_cert.is_none() {
                return Err("non-loopback listeners require TLS certificate and key".to_string());
            }
            fs::create_dir_all(&data_dir)
                .map_err(|error| format!("cannot prepare selected data directory: {error}"))?;
            let cert = loaded
                .config
                .api
                .tls_cert
                .as_ref()
                .map(|path| absolute_path(path.clone()))
                .transpose()?;
            let key = loaded
                .config
                .api
                .tls_key
                .as_ref()
                .map(|path| absolute_path(path.clone()))
                .transpose()?;
            let startup = kakune_core::lifecycle::CoreStartupOptions {
                data_dir: data_dir
                    .canonicalize()
                    .map_err(|error| format!("cannot resolve data directory: {error}"))?,
                config_file: Some(absolute_path(loaded.path)?),
                listen_address: Some(listen.to_string()),
                tls_certificate: cert,
                tls_private_key: key,
                allowed_hosts: loaded.config.api.allowed_hosts.clone(),
            };
            let contexts_file = absolute_path(
                context_file.unwrap_or_else(|| default_contexts_path(data_dir.clone())),
            )?;
            start_background_daemon(&startup, &contexts_file).await
        }
    }
}

async fn serve_foreground(options: DaemonArgs) -> Result<(), String> {
    serve_core(options, false, false, None, None).await
}

async fn serve_background_child(mut options: DaemonArgs) -> Result<(), String> {
    options.action = None;
    serve_core(options, true, false, None, None).await
}

async fn serve_core(
    options: DaemonArgs,
    background: bool,
    system_service: bool,
    service_shutdown: Option<tokio::sync::oneshot::Receiver<()>>,
    service_state: Option<ServiceStateReporter>,
) -> Result<(), String> {
    let DaemonArgs {
        tls_cert,
        tls_key,
        allowed_host,
        internal_daemon_child: _,
        internal_log,
        action: _,
        listen,
        data_dir,
        config,
    } = options;
    let host = Arc::new(CliCoreHost {
        background,
        system_service,
        service_state,
    });
    let shutdown = async move {
        tokio::select! {
            () = shutdown_signal() => {},
            () = wait_for_control_stop(service_shutdown) => {},
        }
    };
    let result = kakune_core::lifecycle::host::serve(
        HostOptions {
            data_dir: data_dir.unwrap_or_else(default_data_dir),
            config_file: config,
            listen,
            tls_certificate: tls_cert,
            tls_private_key: tls_key,
            allowed_hosts: allowed_host,
            diagnostic_location: internal_log,
            mode: if system_service {
                kakune_core::lifecycle::CoreMode::SystemService
            } else if background {
                kakune_core::lifecycle::CoreMode::UserDaemon
            } else {
                kakune_core::lifecycle::CoreMode::Foreground
            },
        },
        host,
        shutdown,
    )
    .await;
    if result.is_ok() {
        if background {
            eprintln!("Kakune Core stopped after graceful drain.");
        } else {
            println!("Kakune Core stopped after graceful drain.");
        }
    }
    result.map_err(|error| redact_lifecycle_diagnostic(&error))
}

struct CliCoreHost {
    background: bool,
    system_service: bool,
    service_state: Option<ServiceStateReporter>,
}

impl HostAdapter for CliCoreHost {
    fn report(&self, event: HostEvent) -> Result<(), String> {
        match event {
            HostEvent::Claimed => write_child_protocol("CLAIM").map_err(|error| error.to_string()),
            HostEvent::Prepared(id) => {
                write_child_protocol(&format!("PREPARED\t{id}")).map_err(|error| error.to_string())
            }
            HostEvent::Ready {
                instance_id,
                address,
                ..
            } => {
                if let Some(reporter) = &self.service_state {
                    reporter(ServiceHostState::Running)?;
                }
                if self.system_service {
                    notify_linux_ready()?;
                }
                if self.background {
                    write_child_protocol(&format!("READY\t{instance_id}")).map_err(|error| {
                        format!("cannot publish background Core readiness: {error}")
                    })?;
                    eprintln!("Kakune Core ready at {address}");
                } else {
                    println!("Kakune Core ready at {address}");
                }
                Ok(())
            }
            HostEvent::Stopping => {
                if let Some(reporter) = &self.service_state {
                    reporter(ServiceHostState::Stopping)?;
                }
                if self.system_service {
                    notify_linux_stopping()?;
                }
                Ok(())
            }
            HostEvent::Stopped => {
                if let Some(reporter) = &self.service_state {
                    reporter(ServiceHostState::Stopped)?;
                }
                Ok(())
            }
            HostEvent::Diagnostic(message) => {
                eprintln!("{}", redact_lifecycle_diagnostic(&message));
                Ok(())
            }
        }
    }

    fn parent_commit(
        &self,
        remaining: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool, String>> + Send + '_>>
    {
        Box::pin(wait_for_parent_commit(remaining))
    }
}

async fn start_background_daemon(
    startup: &kakune_core::lifecycle::CoreStartupOptions,
    contexts_file: &Path,
) -> Result<(), String> {
    let result = kakune_core::lifecycle::launch::start_background(
        startup,
        &std::env::current_exe()
            .map_err(|error| format!("cannot locate Kakune executable: {error}"))?,
    )
    .await?;
    update_local_context_endpoint(
        &startup.data_dir,
        contexts_file,
        result.observation.listen_address.as_deref(),
    );
    println!("{}", render_daemon_observation(&result.observation));
    println!("diagnostic\t{}", result.diagnostic_location.display());
    Ok(())
}

fn absolute_path(path: PathBuf) -> Result<PathBuf, String> {
    let absolute = if path.is_absolute() {
        path
    } else {
        std::env::current_dir()
            .map_err(|error| format!("cannot resolve current directory: {error}"))?
            .join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            std::path::Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            std::path::Component::RootDir => normalized.push(component.as_os_str()),
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                let _ = normalized.pop();
            }
            std::path::Component::Normal(value) => normalized.push(value),
        }
    }
    if normalized.is_absolute() {
        Ok(normalized)
    } else {
        Err("could not resolve an absolute filesystem path".to_owned())
    }
}

async fn wait_for_control_stop(receiver: Option<tokio::sync::oneshot::Receiver<()>>) {
    if let Some(receiver) = receiver {
        let _ = receiver.await;
    } else {
        std::future::pending::<()>().await;
    }
}

fn write_child_protocol(message: &str) -> io::Result<()> {
    let mut output = io::stdout().lock();
    writeln!(output, "{message}")?;
    output.flush()
}

async fn wait_for_parent_commit(remaining: Duration) -> Result<bool, String> {
    use tokio::io::AsyncReadExt;

    let read = async {
        let mut input = tokio::io::stdin();
        let mut bytes = Vec::with_capacity(16);
        let mut byte = [0_u8; 1];
        loop {
            match input.read(&mut byte).await {
                Ok(0) if bytes.is_empty() => return Ok(false),
                Ok(0) => break,
                Ok(_) if byte[0] == b'\n' => break,
                Ok(_) => {
                    if bytes.len() >= 32 {
                        return Err("background launch command exceeds its bound".to_owned());
                    }
                    bytes.push(byte[0]);
                }
                Err(error) => return Err(format!("cannot read parent launch commit: {error}")),
            }
        }
        if bytes == b"COMMIT" {
            Ok(true)
        } else {
            Ok(false)
        }
    };
    tokio::time::timeout(remaining, read)
        .await
        .map_err(|_| "background launch commit deadline expired".to_owned())?
}

fn update_local_context_endpoint(data_dir: &Path, contexts_file: &Path, address: Option<&str>) {
    let Some(address) = address else {
        return;
    };
    let result = (|| -> Result<(), String> {
        let store = Store::open(data_dir.to_path_buf())?;
        let core_id = store.core_id()?;
        let contexts = ContextFile::load(contexts_file)?;
        let credential_ref = contexts
            .contexts
            .iter()
            .find(|context| {
                context.id == "local" && context.expected_core_id.as_deref() == Some(&core_id)
            })
            .and_then(|context| context.credential_ref.clone());
        save_local_context(contexts_file, &core_id, address, credential_ref)
    })();
    if let Err(error) = result {
        eprintln!("Kakune daemon is ready, but the local CLI endpoint was not updated: {error}");
    }
}

fn render_daemon_observation(observation: &DaemonObservation) -> String {
    let state = match observation.state {
        kakune_core::lifecycle::ObservedState::Starting => "starting",
        kakune_core::lifecycle::ObservedState::Ready => "ready",
        kakune_core::lifecycle::ObservedState::Stopping => "stopping",
        kakune_core::lifecycle::ObservedState::Stopped => "stopped",
        kakune_core::lifecycle::ObservedState::Failed => "failed",
        kakune_core::lifecycle::ObservedState::Unknown => "unknown",
    };
    let mut fields = vec![format!("daemon\tstate={state}")];
    if let Some(mode) = observation.mode {
        fields.push(format!("mode={mode}"));
    }
    fields.push(format!("dataDir={}", observation.data_dir.display()));
    if let Some(pid) = observation.verified_pid {
        fields.push(format!("pid={pid}"));
    }
    if let Some(address) = &observation.listen_address {
        fields.push(format!("listen={address}"));
    }
    if let Some(log) = &observation.diagnostic_location {
        fields.push(format!("diagnostic={log}"));
    }
    fields.push(format!(
        "identity={}",
        if observation.identity_verified {
            "verified"
        } else {
            "unverified"
        }
    ));
    if let Some(detail) = &observation.detail {
        fields.push(format!("detail={detail}"));
    }
    fields.join("\t")
}

fn collect_doctor_report(
    data_dir: PathBuf,
    config_path: Option<PathBuf>,
    credentials: &dyn CredentialAccess,
) -> kakune_core::observation::DoctorReport {
    use kakune_core::observation::{
        DOCTOR_LIFECYCLE_STATUS_BUDGET, DoctorArea, DoctorFinding, DoctorStatus, ObservationStatus,
    };

    let started = Instant::now();
    let config_path = config_path.unwrap_or_else(|| data_dir.join("kakune.yaml"));
    let config = if doctor_remaining(started) > Duration::ZERO {
        Some(kakune_core::observation::observe_configuration(
            &config_path,
        ))
    } else {
        None
    };
    let config_status = config.as_ref().map(|observation| observation.status);
    let configuration_finding = config.as_ref().map_or_else(
        || doctor_budget_finding(DoctorArea::Configuration),
        |observation| {
            let (status, message, remediation) = match observation.status {
                ObservationStatus::Healthy => (
                    DoctorStatus::Healthy,
                    "Existing configuration is readable and valid.",
                    "No action is required.",
                ),
                ObservationStatus::Absent | ObservationStatus::Corrupt => (
                    DoctorStatus::Failed,
                    "Configuration is absent or invalid.",
                    "Preserve existing files; initialize explicitly or correct the documented configuration format.",
                ),
                _ => (
                    DoctorStatus::Unknown,
                    "Configuration could not be verified.",
                    "Review file permissions and retry without allowing doctor to repair the file.",
                ),
            };
            DoctorFinding::new(
                DoctorArea::Configuration,
                status,
                "Configuration is required for this local installation.",
                message,
                remediation,
            )
        },
    );

    let storage = if doctor_remaining(started) > Duration::ZERO {
        Some(kakune_core::observation::observe_storage(&data_dir))
    } else {
        None
    };
    let storage_finding = storage.as_ref().map_or_else(
        || doctor_budget_finding(DoctorArea::Storage),
        |observation| {
            let (status, message, remediation) = match observation.status {
                ObservationStatus::Healthy => (
                    DoctorStatus::Healthy,
                    "Existing storage directory is readable.",
                    "No action is required.",
                ),
                ObservationStatus::Absent | ObservationStatus::Corrupt => (
                    DoctorStatus::Failed,
                    "Storage directory is absent or is not a safe directory.",
                    "Preserve existing paths; initialize explicitly or investigate the redirected/non-directory path.",
                ),
                _ => (
                    DoctorStatus::Unknown,
                    "Storage access could not be established.",
                    "Review filesystem permissions and health without changing the installation.",
                ),
            };
            DoctorFinding::new(
                DoctorArea::Storage,
                status,
                "Persistent local storage is required.",
                message,
                remediation,
            )
        },
    );

    let database = if doctor_remaining(started) >= Duration::from_secs(5) {
        Some(kakune_core::observation::observe_database(
            data_dir.join("kakune.sqlite3"),
        ))
    } else {
        None
    };
    let database_status = database.as_ref().map(|observation| observation.status);
    let database_finding = database.as_ref().map_or_else(
        || doctor_budget_finding(DoctorArea::Database),
        |observation| {
            let (status, message, remediation) = match observation.status {
                ObservationStatus::Healthy => (
                    DoctorStatus::Healthy,
                    "Database schema is supported and was inspected read-only.",
                    "No action is required.",
                ),
                ObservationStatus::Legacy => (
                    DoctorStatus::Warning,
                    "Database requires a supported backup-first schema upgrade.",
                    "Start a compatible Core to perform its documented backed-up upgrade; do not delete the database.",
                ),
                ObservationStatus::Absent | ObservationStatus::Corrupt => (
                    DoctorStatus::Failed,
                    "Database is absent or corrupt.",
                    "Preserve the database and backups; initialize explicitly only for a new installation.",
                ),
                ObservationStatus::Incompatible => (
                    DoctorStatus::Failed,
                    "Database schema is newer than this Core supports.",
                    "Use a compatible Core version; do not reset or delete the database.",
                ),
                ObservationStatus::Unavailable => (
                    DoctorStatus::Unknown,
                    "Database could not be safely verified without changing SQLite files.",
                    "Close the Core if a WAL is live, then retry; no immutable read or migration was attempted.",
                ),
            };
            DoctorFinding::new(
                DoctorArea::Database,
                status,
                "Database compatibility and presence are required.",
                message,
                remediation,
            )
        },
    );

    let credential_status = if doctor_remaining(started) > Duration::ZERO {
        match check_doctor_credential(&data_dir, credentials) {
            Ok(()) => Some(DoctorStatus::Healthy),
            Err(CredentialAccessError::Unavailable | CredentialAccessError::InvalidReference) => {
                Some(DoctorStatus::Failed)
            }
            Err(_) => Some(DoctorStatus::Unknown),
        }
    } else {
        None
    };
    let credential_finding = credential_status.map_or_else(
        || doctor_budget_finding(DoctorArea::CredentialStore),
        |status| {
            let (message, remediation) = match status {
                DoctorStatus::Healthy => (
                    "Configured local Core credential is available through its secure reference.",
                    "No action is required.",
                ),
                DoctorStatus::Failed => (
                    "The configured local Core credential is invalid or unavailable.",
                    "Restore the configured secure credential or explicitly recover access; doctor never creates or replaces credentials.",
                ),
                _ => (
                    "Credential-store availability could not be established safely.",
                    "Review the configured credential reference and operating-system credential-store access.",
                ),
            };
            DoctorFinding::new(
                DoctorArea::CredentialStore,
                status,
                "Local Core credentials are required; remote context credentials are not resolved by doctor.",
                message,
                remediation,
            )
        },
    );

    let core_finding = if doctor_remaining(started) >= DOCTOR_LIFECYCLE_STATUS_BUDGET {
        match daemon::observe(&data_dir) {
            Ok(observation) => {
                let (status, message, remediation) = match observation.state {
                    kakune_core::lifecycle::ObservedState::Ready
                        if observation.identity_verified =>
                    {
                        (
                            DoctorStatus::Healthy,
                            "A Core instance is ready and its observed identity is verified.",
                            "No action is required.",
                        )
                    }
                    kakune_core::lifecycle::ObservedState::Stopped => (
                        DoctorStatus::Warning,
                        "No user-level background Core is running.",
                        "Start the daemon only if background Core operation is required; foreground execution remains available.",
                    ),
                    kakune_core::lifecycle::ObservedState::Failed => (
                        DoctorStatus::Failed,
                        "The observed Core instance is failed.",
                        "Inspect sanitized Core diagnostics and preserve its data before recovery.",
                    ),
                    _ => (
                        DoctorStatus::Unknown,
                        "Core runtime state or identity could not be verified.",
                        "Inspect the owning process and lifecycle evidence; do not infer that an unknown instance is stopped.",
                    ),
                };
                DoctorFinding::new(
                    DoctorArea::Core,
                    status,
                    "A background Core is optional; foreground and standalone use do not require one.",
                    message,
                    remediation,
                )
            }
            Err(error) => DoctorFinding::new(
                DoctorArea::Core,
                DoctorStatus::Unknown,
                "A background Core is optional; lifecycle evidence may be unavailable.",
                redact_lifecycle_diagnostic(&error.to_string()),
                "Inspect lifecycle ownership evidence without deleting locks or data.",
            ),
        }
    } else {
        doctor_budget_finding(DoctorArea::Core)
    };

    let service_finding = if doctor_remaining(started) >= DOCTOR_LIFECYCLE_STATUS_BUDGET {
        match (current_service_manager(), system_service_controller()) {
            (Ok(manager), Ok(controller)) => {
                match controller.status(manager, manager.platform_service_id()) {
                    Ok(observation) => doctor_service_finding(observation),
                    Err(error) => DoctorFinding::new(
                        DoctorArea::Service,
                        DoctorStatus::Unknown,
                        "The machine-wide service is optional; native status may be denied or unsupported.",
                        redact_lifecycle_diagnostic(&error.to_string()),
                        "Check native service-manager permissions and evidence; service observation never installs or repairs a service.",
                    ),
                }
            }
            (Err(error), _) | (_, Err(error)) => DoctorFinding::new(
                DoctorArea::Service,
                DoctorStatus::Unknown,
                "The machine-wide service is optional; native status may be denied or unsupported.",
                redact_lifecycle_diagnostic(&error),
                "Check whether the current platform supports native service observation.",
            ),
        }
    } else {
        doctor_budget_finding(DoctorArea::Service)
    };

    let compatibility_finding = if doctor_remaining(started) > Duration::ZERO {
        let (status, message, remediation) = match (config_status, database_status) {
            (Some(ObservationStatus::Corrupt), _)
            | (_, Some(ObservationStatus::Corrupt | ObservationStatus::Incompatible)) => (
                DoctorStatus::Failed,
                "Known configuration or database incompatibility was observed.",
                "Use the documented compatible format/version and preserve data before any upgrade.",
            ),
            (_, Some(ObservationStatus::Legacy)) => (
                DoctorStatus::Warning,
                "The installed database is older but has a supported backup-first upgrade path.",
                "Allow a compatible Core to create its recoverable backup before upgrading.",
            ),
            (Some(ObservationStatus::Healthy), Some(ObservationStatus::Healthy)) => (
                DoctorStatus::Healthy,
                "Configuration and database are compatible with this Core.",
                "No action is required.",
            ),
            _ => (
                DoctorStatus::Unknown,
                "Compatibility could not be established from the available local evidence.",
                "Restore readable configuration/storage evidence or use a compatible Core; do not reset data.",
            ),
        };
        DoctorFinding::new(
            DoctorArea::Compatibility,
            status,
            "Binary, configuration, and storage compatibility are required.",
            message,
            remediation,
        )
    } else {
        doctor_budget_finding(DoctorArea::Compatibility)
    };

    let platform_finding = if doctor_remaining(started) > Duration::ZERO {
        let (status, message, remediation) = if cfg!(any(
            target_os = "windows",
            target_os = "linux"
        )) {
            (
                DoctorStatus::Healthy,
                "This Windows/Linux platform is an acceptance target.",
                "No action is required.",
            )
        } else if cfg!(target_os = "macos") {
            (
                DoctorStatus::Warning,
                "macOS support is present but native acceptance remains unverified.",
                "Treat native lifecycle behavior as unverified until tested on a real macOS host.",
            )
        } else {
            (
                DoctorStatus::Failed,
                "This operating system is not a supported Kakune platform.",
                "Use a supported Windows or Linux installation.",
            )
        };
        DoctorFinding::new(
            DoctorArea::Platform,
            status,
            "Platform compatibility is required.",
            message,
            remediation,
        )
    } else {
        doctor_budget_finding(DoctorArea::Platform)
    };

    kakune_core::observation::DoctorReport::from_findings(vec![
        configuration_finding,
        storage_finding,
        database_finding,
        credential_finding,
        core_finding,
        service_finding,
        compatibility_finding,
        platform_finding,
    ])
    .expect("doctor collector must report each fixed diagnostic area exactly once")
}

fn doctor_remaining(started: Instant) -> Duration {
    kakune_core::observation::DOCTOR_SHARED_BUDGET.saturating_sub(started.elapsed())
}

fn doctor_budget_finding(
    area: kakune_core::observation::DoctorArea,
) -> kakune_core::observation::DoctorFinding {
    kakune_core::observation::DoctorFinding::new(
        area,
        kakune_core::observation::DoctorStatus::Unknown,
        "The shared 30-second doctor budget was exhausted before this check started.",
        "This check was not completed; its status is unknown.",
        "Reduce local filesystem or lifecycle contention and rerun doctor.",
    )
}

fn check_doctor_credential(
    data_dir: &Path,
    credentials: &dyn CredentialAccess,
) -> Result<(), CredentialAccessError> {
    let contexts = ContextFile::load(&default_contexts_path(data_dir.to_path_buf()))
        .map_err(|_| CredentialAccessError::ReadFailed)?;
    let reference = contexts
        .contexts
        .iter()
        .find(|context| context.id == "local")
        .and_then(|context| context.credential_ref.as_deref());
    let reference = credentials.resolve_reference(reference)?;
    let secret = credentials.read(&reference)?;
    drop(secret);
    Ok(())
}

fn doctor_service_finding(
    observation: NativeServiceObservation,
) -> kakune_core::observation::DoctorFinding {
    use kakune_core::lifecycle::{InstallationObservation, NativeManagerState, ObservedState};
    use kakune_core::observation::{DoctorArea, DoctorFinding, DoctorStatus};

    let (status, message, remediation) = if observation.pending_operation
        || observation.core_unknown.is_some()
    {
        (
            DoctorStatus::Unknown,
            "Native service completion or Core ownership could not be verified.",
            "Inspect the native manager and service journal; do not infer absence or retry an uncertain mutation.",
        )
    } else if observation.manager_state == NativeManagerState::Failed {
        (
            DoctorStatus::Failed,
            "The native service manager reports a failed service.",
            "Inspect the native service journal and preserve Kakune data before repair.",
        )
    } else {
        match (observation.installed, observation.manager_state) {
            (InstallationObservation::NotInstalled, _) => (
                DoctorStatus::Warning,
                "No machine-wide Kakune service is installed.",
                "Install a system service only if machine-managed background operation is required.",
            ),
            (_, NativeManagerState::Stopped) => (
                DoctorStatus::Warning,
                "The installed machine-wide service is stopped.",
                "Start it only if machine-managed background operation is required.",
            ),
            (InstallationObservation::Installed, NativeManagerState::Running)
                if observation.core.as_ref().is_some_and(|core| {
                    core.state == ObservedState::Ready && core.identity_verified
                }) =>
            {
                (
                    DoctorStatus::Healthy,
                    "The installed service and its Core instance are ready and verified.",
                    "No action is required.",
                )
            }
            (InstallationObservation::Unknown, _) | (_, NativeManagerState::Unknown) => (
                DoctorStatus::Unknown,
                "Native service state could not be verified.",
                "Check native service-manager availability and permissions.",
            ),
            _ => (
                DoctorStatus::Unknown,
                "Native service state or Core readiness is not confirmed.",
                "Inspect the native manager and Core ownership evidence.",
            ),
        }
    };
    DoctorFinding::new(
        DoctorArea::Service,
        status,
        "Machine-wide service installation is optional; unsupported or denied observation is unknown.",
        message,
        remediation,
    )
}

fn render_doctor_report(report: &kakune_core::observation::DoctorReport) {
    println!("Doctor (local-only; no setup, repair, or migration is performed)");
    println!("area\tstatus\trequired\tapplicability\tevidence\tmessage\tremediation");
    for finding in &report.findings {
        println!(
            "{:?}\t{:?}\t{}\t{}\t{}\t{}\t{}",
            finding.area,
            finding.status,
            finding.required,
            finding.applicability_reason,
            finding.evidence_timestamp,
            finding.message,
            finding.remediation
        );
    }
    println!("exitCode\t{}", report.exit_code);
}

fn execute_context(command: ContextCommand) -> Result<(), String> {
    match command {
        ContextCommand::Add {
            name,
            endpoint,
            display_name,
            expected_core_id,
            color,
            credential_ref,
            contexts_file,
        } => {
            let path = contexts_path(contexts_file);
            ContextRepository::new(path).add(ConnectionContext {
                id: name.clone(),
                name: display_name.unwrap_or(name.clone()),
                endpoint,
                expected_core_id,
                color,
                credential_ref,
            })?;
            println!("Added context {name}; select it with `context use {name}`.");
        }
        ContextCommand::List { contexts_file } => {
            let contexts = ContextFile::load(&contexts_path(contexts_file))?;
            for context in contexts.contexts {
                let marker = if contexts.active_context_id.as_deref() == Some(&context.id) {
                    "*"
                } else {
                    " "
                };
                println!(
                    "{marker}\t{}\t{}\t{}",
                    context.id, context.name, context.endpoint
                );
            }
        }
        ContextCommand::Use { id, contexts_file } => {
            ContextRepository::new(contexts_path(contexts_file)).use_context(&id)?;
            println!("Selected context {id}.");
        }
        ContextCommand::Inspect { id, contexts_file } => {
            let contexts = ContextFile::load(&contexts_path(contexts_file))?;
            let context = contexts
                .contexts
                .iter()
                .find(|context| context.id == id)
                .ok_or_else(|| format!("context {id} was not found"))?;
            print_json(context)?;
        }
        ContextCommand::Remove { id, contexts_file } => {
            ContextRepository::new(contexts_path(contexts_file)).remove(&id)?;
            println!("Removed context {id}.");
        }
        ContextCommand::Import {
            source,
            contexts_file,
        } => {
            ContextRepository::new(contexts_path(contexts_file)).import(&source)?;
            println!("Imported contexts from {}.", source.display());
        }
        ContextCommand::Export {
            name,
            output,
            replace,
            contexts_file,
        } => {
            ContextRepository::new(contexts_path(contexts_file)).export(&name, &output, replace)?;
            println!("Exported context {name} to {}.", output.display());
        }
    }
    Ok(())
}

fn execute_auth(command: AuthCommand, context_file: Option<PathBuf>) -> Result<(), String> {
    match command {
        AuthCommand::Recover { data_dir, config } => {
            let data_dir = data_dir.unwrap_or_else(default_data_dir);
            let loaded = CoreConfig::load_or_create(&data_dir, config)?;
            let listen = loaded.config.listen_addr()?;
            let store = Store::open(data_dir.clone())?;
            let contexts_file =
                context_file.unwrap_or_else(|| default_contexts_path(data_dir.clone()));
            recover_local_auth(&store, &contexts_file, &loaded.config.api, listen)?;
        }
        AuthCommand::Pair {
            data_dir,
            config,
            endpoint,
            admin,
            ttl_seconds,
        } => {
            pair_local_gui(data_dir, config, endpoint, admin, ttl_seconds)?;
        }
        AuthCommand::Tokens { data_dir } => {
            let store = Store::open(data_dir.unwrap_or_else(default_data_dir))?;
            for token in store.list_auth_tokens()? {
                let status = if token.revoked_at.is_some() {
                    "revoked"
                } else {
                    "active"
                };
                println!(
                    "{}\t{}\t{}\t{}\t{}",
                    token.id,
                    token.name,
                    auth_scope_names(&token.scopes),
                    status,
                    token.expires_at.as_deref().unwrap_or("never")
                );
            }
        }
        AuthCommand::Revoke { id, data_dir } => {
            let store = Store::open(data_dir.unwrap_or_else(default_data_dir))?;
            if !store.revoke_auth_token(&id)? {
                return Err(format!("active token {id} was not found"));
            }
            println!("Revoked token {id}.");
        }
    }
    Ok(())
}

fn auth_scope_names(scopes: &[AuthScope]) -> String {
    scopes
        .iter()
        .map(|scope| match scope {
            AuthScope::Read => "read",
            AuthScope::Run => "run",
            AuthScope::Manage => "manage",
            AuthScope::Admin => "admin",
        })
        .collect::<Vec<_>>()
        .join(",")
}

const CLI_KEYRING_SERVICE: &str = "dev.kakune.cli";
const DOCTOR_REPORT_FAILURE: &str = "\0doctor-report-exit-1";

fn local_credential_ref(core_id: &str) -> String {
    format!("keychain:kakune/core/{core_id}")
}

fn cli_credential_entry(reference: &str) -> Result<keyring::Entry, String> {
    let username = reference.strip_prefix("keychain:").unwrap_or(reference);
    keyring::Entry::new(CLI_KEYRING_SERVICE, username)
        .map_err(|error| format!("cannot access the OS credential manager: {error}"))
}

#[cfg(test)]
struct LocalConnectionOutcome {
    credential_available: bool,
    fallback_token: Option<String>,
}

#[cfg(test)]
fn configure_local_connection_with_credentials(
    store: &Store,
    contexts_file: &Path,
    api_config: &kakune_core::config::ApiConfig,
    listen: SocketAddr,
    credential_access: impl FnOnce(&str, Option<&str>) -> bool,
) -> Result<LocalConnectionOutcome, String> {
    let token = store.ensure_bootstrap_token()?;
    let core_id = store.core_id()?;
    let reference = local_credential_ref(&core_id);
    let credential_available = credential_access(&reference, token.as_deref());
    let context_credential_ref = if credential_available || token.is_none() {
        Some(reference)
    } else {
        None
    };
    let endpoint = local_endpoint(api_config, listen);
    save_local_context(contexts_file, &core_id, &endpoint, context_credential_ref)?;

    Ok(LocalConnectionOutcome {
        credential_available,
        fallback_token: (!credential_available).then_some(token).flatten(),
    })
}

struct LocalAuthRecoveryOutcome {
    endpoint: String,
    fallback_token: Option<String>,
    revoked_count: u64,
}

fn recover_local_auth(
    store: &Store,
    contexts_file: &Path,
    api_config: &kakune_core::config::ApiConfig,
    listen: SocketAddr,
) -> Result<(), String> {
    let outcome = recover_local_auth_with_credential_writer(
        store,
        contexts_file,
        api_config,
        listen,
        |reference, token| match cli_credential_entry(reference) {
            Ok(entry) => match entry.set_password(token) {
                Ok(()) => true,
                Err(error) => {
                    eprintln!("Kakune could not save the recovered credential securely: {error}");
                    false
                }
            },
            Err(error) => {
                eprintln!("Kakune could not save the recovered credential securely: {error}");
                false
            }
        },
    )?;

    println!(
        "Local API access recovered; revoked {} previous active token(s).",
        outcome.revoked_count
    );
    println!("Local CLI connection configured for {}.", outcome.endpoint);
    if let Some(token) = outcome.fallback_token {
        println!("Recovery token (OS credential manager unavailable; set KAKUNE_TOKEN): {token}");
    }
    Ok(())
}

fn recover_local_auth_with_credential_writer(
    store: &Store,
    contexts_file: &Path,
    api_config: &kakune_core::config::ApiConfig,
    listen: SocketAddr,
    write_secure: impl FnOnce(&str, &str) -> bool,
) -> Result<LocalAuthRecoveryOutcome, String> {
    let created = store.create_auth_token(
        "Local recovery".to_string(),
        vec![kakune_core::AuthScope::Admin],
        None,
    )?;
    let core_id = store.core_id()?;
    let reference = local_credential_ref(&core_id);
    let credential_available = write_secure(&reference, &created.token);
    save_local_context(
        contexts_file,
        &core_id,
        &local_endpoint(api_config, listen),
        credential_available.then_some(reference),
    )?;
    let revoked = store.revoke_other_auth_tokens(&created.record.id)?;
    Ok(LocalAuthRecoveryOutcome {
        endpoint: local_endpoint(api_config, listen),
        fallback_token: (!credential_available).then_some(created.token),
        revoked_count: revoked,
    })
}

fn pair_local_gui(
    data_dir: Option<PathBuf>,
    config: Option<PathBuf>,
    endpoint: Option<String>,
    admin: bool,
    ttl_seconds: u64,
) -> Result<(), String> {
    if !(30..=600).contains(&ttl_seconds) {
        return Err("pairing invitation lifetime must be 30-600 seconds".to_string());
    }
    let data_dir = data_dir.unwrap_or_else(default_data_dir);
    let loaded = CoreConfig::load_or_create(&data_dir, config)?;
    let listen = loaded.config.listen_addr()?;
    let endpoint = validate_pairing_endpoint(
        &endpoint.unwrap_or_else(|| local_endpoint(&loaded.config.api, listen)),
    )?;
    let store = Store::open(data_dir)?;
    let core_id = store.core_id()?;
    let ttl_seconds = i64::try_from(ttl_seconds)
        .map_err(|_| "pairing invitation lifetime is out of range".to_string())?;
    let expires_at = (time::OffsetDateTime::now_utc() + time::Duration::seconds(ttl_seconds))
        .format(&time::format_description::well_known::Rfc3339)
        .map_err(|error| format!("cannot format pairing expiration: {error}"))?;
    let code = format!("kakune_pair_{}", uuid::Uuid::new_v4().simple());
    let scopes = if admin {
        vec![AuthScope::Admin]
    } else {
        vec![AuthScope::Read, AuthScope::Run, AuthScope::Manage]
    };
    let payload = serde_json::to_vec(&serde_json::json!({
        "format": "kakune-pairing/v1",
        "endpoint": endpoint,
        "coreId": core_id,
        "pairingCode": code,
        "expiresAt": expires_at,
        "scopes": auth_scope_names(&scopes).split(',').collect::<Vec<_>>(),
    }))
    .map_err(|error| format!("cannot encode pairing QR payload: {error}"))?;
    let qr = qrcode::QrCode::new(&payload)
        .map_err(|error| format!("cannot create pairing QR code: {error}"))?;
    let invitation = store.create_auth_pairing_code(&code, scopes.clone(), expires_at.clone())?;

    println!("Scan this QR code in Kakune GUI to request a connection:");
    println!(
        "{}",
        qr.render::<qrcode::render::unicode::Dense1x2>().build()
    );
    println!("Core: {endpoint}");
    println!("Permissions: {}", auth_scope_names(&scopes));
    println!("Expires: {expires_at}");
    println!("Waiting for a device request. Confirm it here before access is granted.");

    loop {
        let status = store
            .get_auth_pairing_status(&invitation.id)?
            .ok_or_else(|| "pairing invitation disappeared from the Core store".to_string())?;
        match status.state {
            AuthPairingState::AwaitingClaim | AuthPairingState::Approved => {}
            AuthPairingState::AwaitingApproval => {
                let device = status
                    .requested_device
                    .as_deref()
                    .unwrap_or("unnamed device");
                print!(
                    "Allow {device:?} to connect with [{}] permissions? [y/N] ",
                    auth_scope_names(&scopes)
                );
                io::stdout()
                    .flush()
                    .map_err(|error| format!("cannot flush pairing prompt: {error}"))?;
                let mut answer = String::new();
                io::stdin()
                    .read_line(&mut answer)
                    .map_err(|error| format!("cannot read pairing approval: {error}"))?;
                let approved = matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes");
                if !store.decide_auth_pairing(&invitation.id, approved)? {
                    println!(
                        "Pairing request could not be confirmed; the invitation may have expired."
                    );
                    return Ok(());
                }
                if !approved {
                    println!("Pairing request rejected.");
                    return Ok(());
                }
                println!("Approved. Waiting for the GUI to finish connecting...");
            }
            AuthPairingState::Rejected => {
                println!("Pairing request rejected.");
                return Ok(());
            }
            AuthPairingState::Consumed => {
                println!(
                    "GUI connected successfully. Manage or revoke its token with `kakune auth tokens`."
                );
                return Ok(());
            }
            AuthPairingState::Expired => {
                println!("Pairing QR code expired without completing a connection.");
                return Ok(());
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(300));
    }
}

fn validate_pairing_endpoint(endpoint: &str) -> Result<String, String> {
    let url = reqwest::Url::parse(endpoint).map_err(|_| "invalid pairing endpoint URL")?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("pairing endpoint must not contain credentials, a query, or a fragment".into());
    }
    let local = url.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    if url.scheme() != "https" && !(url.scheme() == "http" && local) {
        return Err("pairing with a non-loopback Core requires an HTTPS endpoint".to_string());
    }
    if url.host_str().is_none() {
        return Err("pairing endpoint must include a host".to_string());
    }
    Ok(url.as_str().trim_end_matches('/').to_string())
}

fn save_local_context(
    contexts_file: &Path,
    core_id: &str,
    endpoint: &str,
    credential_ref: Option<String>,
) -> Result<(), String> {
    let mut contexts = ContextFile::load(contexts_file)?;
    let local = ConnectionContext {
        id: "local".to_string(),
        name: "Local Kakune Core".to_string(),
        endpoint: endpoint.to_string(),
        expected_core_id: Some(core_id.to_string()),
        color: None,
        credential_ref,
    };
    if let Some(existing) = contexts
        .contexts
        .iter_mut()
        .find(|context| context.id == "local")
    {
        *existing = local;
    } else {
        contexts.contexts.push(local);
    }
    if contexts.active_context_id.is_none() {
        contexts.active_context_id = Some("local".to_string());
    }
    contexts.save(contexts_file)
}

fn local_endpoint(api_config: &kakune_core::config::ApiConfig, listen: SocketAddr) -> String {
    let listen = if listen.ip().is_unspecified() {
        let loopback = if listen.is_ipv4() { "127.0.0.1" } else { "::1" };
        SocketAddr::new(
            loopback.parse().expect("loopback address is valid"),
            listen.port(),
        )
    } else {
        listen
    };
    let scheme = if api_config.tls_cert.is_some() {
        "https"
    } else {
        "http"
    };
    format!("{scheme}://{listen}")
}

fn contexts_path(explicit: Option<PathBuf>) -> PathBuf {
    explicit.unwrap_or_else(|| default_contexts_path(default_data_dir()))
}

fn cli_contexts_path(cli: &Cli) -> PathBuf {
    cli.context_file.clone().unwrap_or_else(|| {
        let data_dir = match &cli.command {
            Command::Run {
                data_dir: Some(data_dir),
                ..
            } => data_dir.clone(),
            Command::Workflow {
                command:
                    WorkflowCommand::Run {
                        data_dir: Some(data_dir),
                        ..
                    },
            } => data_dir.clone(),
            _ => default_data_dir(),
        };
        default_contexts_path(data_dir)
    })
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C signal handler");
    };

    #[cfg(windows)]
    let ctrl_break = async {
        tokio::signal::windows::ctrl_break()
            .expect("failed to install Ctrl+Break signal handler")
            .recv()
            .await;
    };

    #[cfg(not(windows))]
    let ctrl_break = std::future::pending::<()>();

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install terminate signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = ctrl_break => {},
        () = terminate => {},
    }
}

fn notify_linux_ready() -> Result<(), String> {
    #[cfg(target_os = "linux")]
    if std::env::var_os("NOTIFY_SOCKET").is_some() {
        sd_notify::notify(&[sd_notify::NotifyState::Ready])
            .map_err(|error| format!("cannot notify systemd that Core is ready: {error}"))?;
    }
    Ok(())
}

fn notify_linux_stopping() -> Result<(), String> {
    #[cfg(target_os = "linux")]
    if std::env::var_os("NOTIFY_SOCKET").is_some() {
        sd_notify::notify(&[sd_notify::NotifyState::Stopping])
            .map_err(|error| format!("cannot notify systemd that Core is stopping: {error}"))?;
    }
    Ok(())
}

async fn execute_remote(cli: &Cli) -> Result<(), String> {
    use reqwest::Method;
    use serde_json::json;
    let contexts = ContextFile::load(&cli_contexts_path(cli))?;
    let selected = cli.context.as_ref().or(contexts.active_context_id.as_ref());
    let local = ConnectionContext {
        id: "local".into(),
        name: "Local".into(),
        endpoint: std::env::var("KAKUNE_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:8787".into()),
        expected_core_id: None,
        color: None,
        credential_ref: None,
    };
    let context = if dispatch_category(&cli.command) == DispatchCategory::CanonicalCore {
        selected_management_context(&contexts, cli.context.as_deref())?
    } else if let Some(id) = selected {
        contexts
            .contexts
            .iter()
            .find(|context| &context.id == id)
            .ok_or_else(|| format!("context {id} was not found"))?
    } else {
        &local
    };
    let token = match &context.credential_ref {
        Some(reference) if reference.starts_with("env:") => std::env::var(&reference[4..])
            .map_err(|_| "context token environment variable is missing".to_string())?,
        Some(reference) => cli_credential_entry(reference)?
            .get_password()
            .map_err(|_| {
                "context credential is unavailable; run `kakune auth recover` on the Core machine"
                    .to_string()
            })?,
        None => std::env::var("KAKUNE_TOKEN").map_err(|_| {
            "set KAKUNE_TOKEN or configure a context credential reference".to_string()
        })?,
    };
    let ca = cli
        .ca
        .as_ref()
        .map(fs::read)
        .transpose()
        .map_err(|error| error.to_string())?;
    if dispatch_category(&cli.command) == DispatchCategory::CanonicalCore {
        let client =
            kakune_core::remote::ManagementClient::connect(context, token, ca.as_deref()).await?;
        return execute_management_command(&client, &cli.command).await;
    }
    let client = kakune_core::remote::RemoteClient::connect(context, token, ca.as_deref()).await?;
    let read = |path: &Path| fs::read_to_string(path).map_err(|error| error.to_string());
    let value = match &cli.command {
        Command::Run { workflow, .. } => {
            let record = client.save_workflow(&read(workflow)?).await?;
            let id = record["id"]
                .as_str()
                .ok_or("Core did not return a workflow ID")?;
            client
                .request(
                    Method::POST,
                    &["executions"],
                    Some(json!({"workflowId":id})),
                    None,
                )
                .await?
        }
        Command::Executions { .. } => {
            client
                .request(Method::GET, &["executions"], None, None)
                .await?
        }
        Command::Inspect { execution_id, .. } => {
            client
                .request(Method::GET, &["executions", execution_id], None, None)
                .await?
        }
        Command::Workflow { .. } => {
            return Err("workflow commands require the canonical management API".to_owned());
        }
        Command::Plugin { command } => match command {
            PluginCommand::Prepare { source, .. } => {
                client
                    .request(
                        Method::POST,
                        &["plugins", "prepare"],
                        Some(json!({"source":source})),
                        None,
                    )
                    .await?
            }
            PluginCommand::Commit {
                prepared_id,
                digest,
                ..
            } => {
                client
                    .request(
                        Method::POST,
                        &["plugin-installations", prepared_id, "commit"],
                        Some(json!({"digest":digest})),
                        None,
                    )
                    .await?
            }
            PluginCommand::List { .. } => {
                client
                    .request(Method::GET, &["plugins"], None, None)
                    .await?
            }
            PluginCommand::Remove { id, .. } => {
                client
                    .request(Method::DELETE, &["plugins", id], None, None)
                    .await?
            }
        },
        Command::Provider { command } => match command {
            ProviderCommand::List { .. } => {
                client
                    .request(Method::GET, &["providers"], None, None)
                    .await?
            }
            ProviderCommand::Upsert { json_file, .. } => {
                client
                    .request(
                        Method::POST,
                        &["providers"],
                        Some(
                            serde_json::from_str(&read(json_file)?)
                                .map_err(|error| error.to_string())?,
                        ),
                        None,
                    )
                    .await?
            }
            ProviderCommand::Status { id, .. } => {
                client
                    .request(Method::GET, &["providers", id], None, None)
                    .await?
            }
            ProviderCommand::Remove { id, .. } => {
                client
                    .request(Method::DELETE, &["providers", id], None, None)
                    .await?
            }
            ProviderCommand::Login { id, .. } => {
                let profile: ProviderProfile = serde_json::from_value(
                    client
                        .request(Method::GET, &["providers", id], None, None)
                        .await?,
                )
                .map_err(|error| error.to_string())?;
                let secret_ref = match (&profile.provider_type, &profile.auth) {
                    (ProviderType::Codex, ProviderAuth::OAuthSecret { secret_ref }) => secret_ref,
                    _ => {
                        return Err(
                            "provider login requires a Codex profile using oauthSecret".to_string()
                        );
                    }
                };
                let tokens = kakune_core::codex::login_with_browser()
                    .await
                    .map_err(|error| error.to_string())?;
                client.request(Method::POST, &["secrets"], Some(serde_json::json!({
                        "name":secret_ref, "value":tokens.to_secret().map_err(|error| error.to_string())?
                    })), None).await?;
                client
                    .request(Method::POST, &["providers", id, "diagnose"], None, None)
                    .await?
            }
            ProviderCommand::Add { .. }
            | ProviderCommand::Auth { .. }
            | ProviderCommand::Test { .. }
            | ProviderCommand::Models { .. } => {
                return Err(
                    "provider administration commands require the canonical management API"
                        .to_owned(),
                );
            }
        },
        Command::Mcp {
            command: McpCommand::Call { json_file, .. },
        } => {
            client
                .request(
                    Method::POST,
                    &["mcp", "call"],
                    Some(
                        serde_json::from_str(&read(json_file)?)
                            .map_err(|error| error.to_string())?,
                    ),
                    None,
                )
                .await?
        }
        Command::Mcp { .. } | Command::Secret { .. } => {
            return Err("integration commands require the canonical management API".to_owned());
        }
        _ => {
            return Err(
                "this command operates on the local installation; omit --context".to_string(),
            );
        }
    };
    print_json(&value)
}

async fn execute_management_command(
    client: &kakune_core::remote::ManagementClient,
    command: &Command,
) -> Result<(), String> {
    match command {
        Command::Workflow { .. } => execute_management_workflow(client, command).await,
        Command::Execution { command } => execute_management_execution(client, command).await,
        Command::Trigger { command } => execute_management_trigger(client, command).await,
        Command::Provider { command } => execute_management_provider(client, command).await,
        Command::Mcp { command } => execute_management_mcp(client, command).await,
        Command::Secret { command } => execute_management_secret(client, command).await,
        _ => Err("this command has no canonical Core management operation".to_owned()),
    }
}

async fn execute_management_provider(
    client: &kakune_core::remote::ManagementClient,
    command: &ProviderCommand,
) -> Result<(), String> {
    use reqwest::Method;
    match command {
        ProviderCommand::Add { id, file, json } => {
            let source = read_capped_text(file, 1_048_576, "provider profile")?;
            let profile: ProviderProfileUpsert = serde_json::from_str(&source)
                .map_err(|_| "provider profile file is not valid JSON".to_owned())?;
            if profile.id != *id {
                return Err("provider ID must match the profile file ID".to_owned());
            }
            let value =
                serde_json::to_value(profile).map_err(|_| "provider profile cannot be encoded")?;
            let response = client
                .request(Method::POST, &["providers"], Some(value), None)
                .await?;
            print_management_value(&response, *json, |value| {
                render_provider_profile(value, "Added")
            })
        }
        ProviderCommand::List {
            limit,
            cursor,
            json,
        } => {
            let query = page_query(*limit, cursor.as_deref());
            let page = client
                .request_with_query(Method::GET, &["providers"], &query)
                .await?;
            print_management_value(&page, *json, render_provider_page)
        }
        ProviderCommand::Auth { id, stdin, json } => {
            let preparation = client
                .request(
                    Method::POST,
                    &["providers", id, "auth", "prepare"],
                    None,
                    None,
                )
                .await?;
            match preparation
                .get("mechanism")
                .and_then(serde_json::Value::as_str)
            {
                Some("secureInput") => {
                    let secret = if *stdin {
                        read_secret_input(true, "")?
                    } else {
                        read_secret_input(false, "MiniMax API key: ")?
                    };
                    let response = client
                        .request(
                            Method::POST,
                            &["providers", id, "auth", "commit"],
                            Some(serde_json::json!({"secretValue": secret.expose()})),
                            None,
                        )
                        .await?;
                    print_management_value(&response, *json, |value| {
                        println!("Authentication committed for provider {id}.");
                        render_cleanup_warning(value);
                        Ok(())
                    })
                }
                Some("browser") => {
                    if *stdin {
                        return Err(
                            "Codex browser authentication does not accept --stdin".to_owned()
                        );
                    }
                    let login = kakune_core::codex::begin_browser_login()
                        .await
                        .map_err(|_| "cannot prepare Codex browser authorization".to_owned())?;
                    let authorize_url = login.authorize_url().to_owned();
                    if *json {
                        eprintln!("Open this URL to authorize ChatGPT Plus/Pro:\n{authorize_url}");
                    } else {
                        println!("Open this URL to authorize ChatGPT Plus/Pro:\n{authorize_url}");
                    }
                    if webbrowser::open(&authorize_url).is_err() {
                        eprintln!(
                            "Browser launch was unavailable. Open the authorization URL above manually."
                        );
                    }
                    let mut tokens = login
                        .complete()
                        .await
                        .map_err(|_| "Codex browser authorization did not complete".to_owned())?;
                    let encoded = tokens
                        .to_secret()
                        .map_err(|_| "Codex authorization result could not be encoded");
                    tokens.zeroize_secrets();
                    let mut bundle = Zeroizing::new(encoded?);
                    let response = client
                        .request(
                            Method::POST,
                            &["providers", id, "auth", "commit"],
                            Some(serde_json::json!({"oauthBundle": bundle.as_str()})),
                            None,
                        )
                        .await;
                    bundle.zeroize();
                    let response = response?;
                    print_management_value(&response, *json, |value| {
                        println!("Authentication committed for provider {id}.");
                        render_cleanup_warning(value);
                        Ok(())
                    })
                }
                _ => Err(
                    "selected Core reported an unsupported provider authentication mechanism"
                        .to_owned(),
                ),
            }
        }
        ProviderCommand::Test { id, json } => {
            let report = client
                .request(Method::POST, &["providers", id, "test"], None, None)
                .await?;
            print_management_value(&report, *json, render_provider_test)
        }
        ProviderCommand::Models { id, json } => {
            let response = client
                .request_preserving_unsupported(Method::POST, &["providers", id, "models"])
                .await?;
            print_management_value(&response, *json, render_provider_models)?;
            if response
                .pointer("/discovery/status")
                .and_then(serde_json::Value::as_str)
                == Some("unsupported")
            {
                return Err("live provider model discovery is unsupported; configured models are shown above".to_owned());
            }
            Ok(())
        }
        ProviderCommand::Upsert { .. }
        | ProviderCommand::Status { .. }
        | ProviderCommand::Remove { .. }
        | ProviderCommand::Login { .. } => {
            Err("legacy provider commands retain their legacy routing".to_owned())
        }
    }
}

async fn execute_management_mcp(
    client: &kakune_core::remote::ManagementClient,
    command: &McpCommand,
) -> Result<(), String> {
    use reqwest::Method;
    match command {
        McpCommand::Add { name, file, json } => {
            let source = read_capped_text(file, 1_048_576, "MCP connection")?;
            let value: serde_json::Value = serde_json::from_str(&source)
                .map_err(|_| "MCP connection file is not valid JSON".to_owned())?;
            if value
                .get("name")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|file_name| file_name != name)
            {
                return Err("MCP connection name must match the requested name".to_owned());
            }
            let transport: kakune_core::McpConnectionTransport = serde_json::from_value(
                value
                    .get("transport")
                    .cloned()
                    .ok_or("MCP connection file must contain transport")?,
            )
            .map_err(|_| "MCP transport configuration is invalid".to_owned())?;
            let connection = kakune_core::NewNamedMcpConnection {
                name: name.clone(),
                transport,
            };
            let response = client
                .request(
                    Method::POST,
                    &["mcp"],
                    Some(
                        serde_json::to_value(connection)
                            .map_err(|_| "MCP connection cannot be encoded")?,
                    ),
                    None,
                )
                .await?;
            print_management_value(&response, *json, |value| {
                render_mcp_connection(value, "Added")
            })
        }
        McpCommand::List {
            limit,
            cursor,
            json,
        } => {
            let query = page_query(*limit, cursor.as_deref());
            let page = client
                .request_with_query(Method::GET, &["mcp"], &query)
                .await?;
            print_management_value(&page, *json, render_mcp_page)
        }
        McpCommand::Inspect { name, json } => {
            let connection = client
                .request(Method::GET, &["mcp", name], None, None)
                .await?;
            print_management_value(&connection, *json, |value| {
                render_mcp_connection(value, "MCP connection")
            })
        }
        McpCommand::Auth { name, secret, json } => {
            let connection = client
                .request(Method::GET, &["mcp", name], None, None)
                .await?;
            if secret.is_some()
                && connection
                    .pointer("/transport/type")
                    .and_then(serde_json::Value::as_str)
                    == Some("stdio")
            {
                return Err("stdio MCP connections do not support --secret; provision named environment references separately".to_owned());
            }
            let revision = connection
                .get("revision")
                .and_then(serde_json::Value::as_str)
                .ok_or("selected Core did not return an MCP connection revision")?;
            let body = serde_json::json!({"secretReference":secret});
            let outcome = client
                .request(
                    Method::POST,
                    &["mcp", name, "auth"],
                    Some(body),
                    secret.as_ref().map(|_| revision),
                )
                .await?;
            print_management_value(&outcome, *json, render_mcp_auth)
        }
        McpCommand::Tool { command } => match command {
            McpToolCommand::List {
                name,
                limit,
                cursor,
                json,
            } => {
                let body = serde_json::json!({"limit":limit,"cursor":cursor});
                let page = client
                    .request(
                        Method::POST,
                        &["mcp", name, "tools", "list"],
                        Some(body),
                        None,
                    )
                    .await?;
                print_management_value(&page, *json, render_mcp_tools)
            }
            McpToolCommand::Call {
                name,
                tool,
                input,
                json,
            } => {
                let source = read_capped_text(input, 1_048_576, "MCP tool input")?;
                let arguments: serde_json::Value = serde_json::from_str(&source)
                    .map_err(|_| "MCP tool input must be strict JSON".to_owned())?;
                if !arguments.is_object() {
                    return Err("MCP tool input must be a JSON object".to_owned());
                }
                let result = client
                    .request(
                        Method::POST,
                        &["mcp", name, "tools", "call"],
                        Some(serde_json::json!({"name":tool,"arguments":arguments})),
                        None,
                    )
                    .await?;
                print_management_value(&result, *json, render_mcp_tool_result)
            }
        },
        McpCommand::Call { .. } => Err("legacy mcp call retains its legacy routing".to_owned()),
    }
}

async fn execute_management_secret(
    client: &kakune_core::remote::ManagementClient,
    command: &SecretCommand,
) -> Result<(), String> {
    use reqwest::Method;
    match command {
        SecretCommand::Set { name, stdin, json } => {
            let secret = read_secret_input(*stdin, "Secret value: ")?;
            let result = client
                .request(
                    Method::PUT,
                    &["secrets", name],
                    Some(serde_json::json!({"value":secret.expose()})),
                    None,
                )
                .await?;
            print_management_value(&result, *json, |value| {
                let secret = &value["secret"];
                println!(
                    "Secret {} committed to {} at {}.",
                    json_string(secret, "name", name),
                    json_string(secret, "backend", "unknown backend"),
                    json_string(secret, "updatedAt", "timestamp unavailable")
                );
                render_cleanup_warning(value);
                Ok(())
            })
        }
        SecretCommand::List {
            limit,
            cursor,
            json,
        } => {
            let query = page_query(*limit, cursor.as_deref());
            let page = client
                .request_with_query(Method::GET, &["secrets"], &query)
                .await?;
            print_management_value(&page, *json, render_secret_page)
        }
        SecretCommand::Remove { name, json } => {
            let outcome = client
                .request(Method::DELETE, &["secrets", name], None, None)
                .await?;
            print_management_value(&outcome, *json, |value| {
                if value["removed"].as_bool() == Some(true) {
                    println!("Secret {name} removed.");
                }
                render_cleanup_warning(value);
                Ok(())
            })
        }
    }
}

fn print_management_value(
    value: &serde_json::Value,
    as_json: bool,
    render_human: impl FnOnce(&serde_json::Value) -> Result<(), String>,
) -> Result<(), String> {
    if as_json {
        print_json(value)
    } else {
        render_human(value)
    }
}

fn json_string<'a>(value: &'a serde_json::Value, key: &str, fallback: &'a str) -> &'a str {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or(fallback)
}

fn render_provider_profile(profile: &serde_json::Value, action: &str) -> Result<(), String> {
    println!(
        "{action} provider {} ({}, {}).",
        json_string(profile, "id", "unknown ID"),
        json_string(profile, "displayName", "unnamed"),
        json_string(profile, "providerType", "unknown type")
    );
    println!(
        "Default model: {}",
        json_string(profile, "defaultModel", "unknown")
    );
    println!(
        "Authentication: {}",
        json_string(&profile["auth"], "type", "unknown")
    );
    println!(
        "Last diagnosis: {}",
        json_string(&profile["diagnostic"], "status", "unknown")
    );
    Ok(())
}

fn render_provider_page(page: &serde_json::Value) -> Result<(), String> {
    println!("ID\tNAME\tTYPE\tDEFAULT MODEL\tSTATUS");
    for profile in page["items"].as_array().into_iter().flatten() {
        println!(
            "{}\t{}\t{}\t{}\t{}",
            json_string(profile, "id", ""),
            json_string(profile, "displayName", ""),
            json_string(profile, "providerType", "unknown"),
            json_string(profile, "defaultModel", ""),
            json_string(&profile["diagnostic"], "status", "unknown")
        );
    }
    render_next_cursor(page);
    Ok(())
}

fn render_provider_test(report: &serde_json::Value) -> Result<(), String> {
    println!("Provider: {}", json_string(report, "profileId", "unknown"));
    println!(
        "Checked: {}",
        json_string(report, "checkedAt", "time unavailable")
    );
    for (label, key) in [
        ("Connectivity", "connectivity"),
        ("Authentication", "authentication"),
    ] {
        println!(
            "{label}: {} — {}",
            json_string(&report[key], "status", "unknown"),
            json_string(&report[key], "message", "no details available")
        );
    }
    if let Some(capabilities) = report["capabilities"].as_array() {
        for capability in capabilities {
            println!(
                "Capability {}: {} — {}",
                json_string(capability, "capability", "unknown"),
                json_string(capability, "status", "unknown"),
                json_string(capability, "message", "no details available")
            );
        }
    }
    Ok(())
}

fn render_provider_models(models: &serde_json::Value) -> Result<(), String> {
    println!("Provider: {}", json_string(models, "providerId", "unknown"));
    println!(
        "Model discovery: {} — {}",
        json_string(&models["discovery"], "status", "unknown"),
        json_string(&models["discovery"], "message", "no details available")
    );
    println!("MODEL\tSOURCE\tDEFAULT");
    for model in models["models"].as_array().into_iter().flatten() {
        println!(
            "{}\t{}\t{}",
            json_string(model, "id", ""),
            json_string(model, "provenance", "unknown"),
            model["isDefault"].as_bool().unwrap_or(false)
        );
    }
    Ok(())
}

fn render_cleanup_warning(value: &serde_json::Value) {
    if let Some(warning) = value
        .get("cleanupWarning")
        .and_then(serde_json::Value::as_str)
    {
        println!("Cleanup warning: {warning}");
    }
}

fn render_mcp_connection(connection: &serde_json::Value, action: &str) -> Result<(), String> {
    let name = json_string(connection, "name", "unknown");
    let transport = &connection["transport"];
    let transport_type = json_string(transport, "type", "unknown");
    println!("{action} MCP connection {name} ({transport_type}).");
    match transport_type {
        "http" => {
            println!(
                "Endpoint: {}",
                json_string(transport, "endpoint", "unavailable")
            );
            println!(
                "Authentication reference: {}",
                if transport["bearerSecretRef"].as_str().is_some() {
                    "configured"
                } else {
                    "none"
                }
            );
        }
        "stdio" => {
            println!(
                "Executable: {}",
                json_string(transport, "command", "unavailable")
            );
            println!(
                "Working directory: {}",
                json_string(transport, "workingDirectory", "unavailable")
            );
            let arguments = transport["args"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(serde_json::Value::as_str)
                .collect::<Vec<_>>()
                .join(" ");
            println!("Arguments: {arguments}");
            println!(
                "Environment credential references: {}",
                if transport["environmentSecretRefs"]
                    .as_object()
                    .is_some_and(|references| !references.is_empty())
                {
                    "configured"
                } else {
                    "none"
                }
            );
        }
        _ => println!("Transport configuration is unavailable."),
    }
    println!(
        "Revision: {}",
        json_string(connection, "revision", "unavailable")
    );
    Ok(())
}

fn render_mcp_page(page: &serde_json::Value) -> Result<(), String> {
    println!("NAME\tTRANSPORT\tENDPOINT / EXECUTABLE\tREVISION");
    for connection in page["items"].as_array().into_iter().flatten() {
        let transport = &connection["transport"];
        let address = match json_string(transport, "type", "") {
            "http" => json_string(transport, "endpoint", ""),
            "stdio" => json_string(transport, "command", ""),
            _ => "unknown",
        };
        println!(
            "{}\t{}\t{}\t{}",
            json_string(connection, "name", ""),
            json_string(transport, "type", "unknown"),
            address,
            json_string(connection, "revision", "")
        );
    }
    render_next_cursor(page);
    Ok(())
}

fn render_mcp_auth(outcome: &serde_json::Value) -> Result<(), String> {
    let status = json_string(outcome, "status", "unknown");
    match status {
        "notRequired" => println!(
            "Authentication not required: {}.",
            json_string(outcome, "message", "no authentication is configured")
        ),
        "available" => {
            println!("Authentication references are available; upstream validity was not tested.")
        }
        "committed" => {
            let connection = &outcome["connection"];
            println!(
                "Authentication reference committed for MCP connection {} (revision {}).",
                json_string(connection, "name", "unknown"),
                json_string(connection, "revision", "unavailable")
            );
        }
        _ => println!("MCP authentication status: {status}."),
    }
    render_cleanup_warning(outcome);
    Ok(())
}

fn render_mcp_tools(page: &serde_json::Value) -> Result<(), String> {
    println!(
        "MCP server: {} {} (protocol {}).",
        json_string(&page["server"], "name", "unknown"),
        json_string(&page["server"], "version", "unknown version"),
        json_string(&page["server"], "protocolVersion", "unknown")
    );
    for tool in page["items"].as_array().into_iter().flatten() {
        println!("Tool: {}", json_string(tool, "name", "unknown"));
        if let Some(description) = tool["description"].as_str() {
            println!("  {description}");
        }
        if !tool["inputSchema"].is_null() {
            println!("  Input schema:");
            println!(
                "{}",
                serde_json::to_string_pretty(&tool["inputSchema"])
                    .map_err(|error| error.to_string())?
            );
        }
    }
    render_next_cursor(page);
    Ok(())
}

fn render_mcp_tool_result(result: &serde_json::Value) -> Result<(), String> {
    let mut printed_content = false;
    for item in result["content"].as_array().into_iter().flatten() {
        match json_string(item, "type", "unknown") {
            "text" => {
                println!("{}", json_string(item, "text", ""));
                printed_content = true;
            }
            "image" | "audio" => {
                println!(
                    "{} content ({}); use --json to include the binary payload.",
                    json_string(item, "type", "media"),
                    json_string(item, "mimeType", "MIME type unavailable")
                );
                printed_content = true;
            }
            _ => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(item).map_err(|error| error.to_string())?
                );
                printed_content = true;
            }
        }
    }
    if !result["structuredContent"].is_null() {
        println!("Structured result:");
        println!(
            "{}",
            serde_json::to_string_pretty(&result["structuredContent"])
                .map_err(|error| error.to_string())?
        );
        printed_content = true;
    }
    if !printed_content {
        println!("Tool completed without content.");
    }
    Ok(())
}

fn render_secret_page(page: &serde_json::Value) -> Result<(), String> {
    println!("NAME\tUPDATED\tBACKEND");
    for secret in page["items"].as_array().into_iter().flatten() {
        println!(
            "{}\t{}\t{}",
            json_string(secret, "name", ""),
            json_string(secret, "updatedAt", ""),
            json_string(secret, "backend", "unknown")
        );
    }
    render_next_cursor(page);
    Ok(())
}

fn render_next_cursor(page: &serde_json::Value) {
    if let Some(cursor) = page.get("nextCursor").and_then(serde_json::Value::as_str) {
        println!("Next page cursor: {cursor}");
    }
}

fn page_query(limit: Option<usize>, cursor: Option<&str>) -> Vec<(String, String)> {
    let mut query = Vec::new();
    if let Some(limit) = limit {
        query.push(("limit".to_owned(), limit.to_string()));
    }
    if let Some(cursor) = cursor {
        query.push(("cursor".to_owned(), cursor.to_owned()));
    }
    query
}

fn read_capped_text(path: &Path, limit: usize, label: &str) -> Result<String, String> {
    let file = fs::File::open(path).map_err(|_| format!("cannot read {label} file"))?;
    let mut bytes = Vec::new();
    file.take(limit.saturating_add(1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| format!("cannot read {label} file"))?;
    if bytes.len() > limit {
        return Err(format!("{label} file exceeds the {limit}-byte limit"));
    }
    String::from_utf8(bytes).map_err(|_| format!("{label} file must be UTF-8"))
}

fn read_secret_input(
    explicit_stdin: bool,
    prompt: &str,
) -> Result<kakune_core::management::SecretValue, String> {
    const MAX_SECRET_BYTES: usize = 64 * 1024;
    if explicit_stdin {
        let stdin = io::stdin();
        let mut input = Zeroizing::new(Vec::new());
        stdin
            .lock()
            .take((MAX_SECRET_BYTES + 3) as u64)
            .read_to_end(&mut input)
            .map_err(|_| "cannot read secret from stdin".to_owned())?;
        if input.len() > MAX_SECRET_BYTES + 2 {
            return Err("secret input exceeds the 64 KiB limit".to_owned());
        }
        if input.ends_with(b"\r\n") {
            let len = input.len() - 2;
            input.truncate(len);
        } else if input.ends_with(b"\n") {
            let len = input.len() - 1;
            input.truncate(len);
        }
        if input.is_empty() {
            return Err("secret input must not be empty".to_owned());
        }
        if input.len() > MAX_SECRET_BYTES {
            return Err("secret input exceeds the 64 KiB limit".to_owned());
        }
        let mut value = Zeroizing::new(
            String::from_utf8(input.to_vec())
                .map_err(|_| "secret input must be UTF-8".to_owned())?,
        );
        let secret = kakune_core::management::SecretValue::new(value.as_str().to_owned());
        value.zeroize();
        return Ok(secret);
    }
    if !io::stdin().is_terminal() {
        return Err(
            "interactive terminal input is required; use --stdin for automation".to_owned(),
        );
    }
    let value = Zeroizing::new(
        rpassword::prompt_password(prompt)
            .map_err(|_| "cannot read hidden secret input".to_owned())?,
    );
    if value.is_empty() {
        return Err("secret input must not be empty".to_owned());
    }
    if value.len() > MAX_SECRET_BYTES {
        return Err("secret input exceeds the 64 KiB limit".to_owned());
    }
    Ok(kakune_core::management::SecretValue::new(
        value.as_str().to_owned(),
    ))
}

async fn execute_management_workflow(
    client: &kakune_core::remote::ManagementClient,
    command: &Command,
) -> Result<(), String> {
    use reqwest::Method;
    use serde_json::{Value, json};

    let Command::Workflow { command } = command else {
        return Err("selected-Core management dispatch requires a workflow command".to_owned());
    };

    async fn resolve(
        client: &kakune_core::remote::ManagementClient,
        selector: &str,
    ) -> Result<Value, String> {
        client
            .request(
                Method::POST,
                &["workflows", "resolve"],
                Some(serde_json::json!({ "selector": selector })),
                None,
            )
            .await
    }

    match command {
        WorkflowCommand::Import {
            file,
            replace,
            if_revision,
            json: as_json,
        } => {
            let source = read_workflow_source(file)?;
            let mut expected_revision = if_revision.clone();
            if *replace
                && expected_revision.is_none()
                && let Ok(workflow) = kakune_core::WorkflowDocument::parse(&source)
            {
                let selector = format!("id:{}", workflow.metadata.id);
                match resolve(client, &selector).await {
                    Ok(record) => {
                        expected_revision = Some(
                            record["revision"]
                                .as_str()
                                .ok_or("Core did not return the workflow revision")?
                                .to_owned(),
                        );
                    }
                    Err(error) if error.contains("HTTP 404") => {}
                    Err(error) => return Err(error),
                }
            }
            let mut body = json!({ "source": source, "replace": replace });
            if let Some(revision) = expected_revision {
                body["expectedRevision"] = json!(revision);
            }
            let record = client
                .request(Method::POST, &["workflows", "import"], Some(body), None)
                .await?;
            if *as_json {
                print_json(&record)?;
            } else {
                println!(
                    "Imported {} ({}, {})",
                    record["id"].as_str().unwrap_or("workflow"),
                    record["status"].as_str().unwrap_or("unknown status"),
                    record["revision"]
                        .as_str()
                        .unwrap_or("revision unavailable")
                );
            }
        }
        WorkflowCommand::Export {
            workflow,
            output,
            replace,
            json: as_json,
        } => {
            let record = resolve(client, workflow).await?;
            let id = record["id"]
                .as_str()
                .ok_or("Core did not return the workflow ID")?;
            let source = client
                .request(Method::GET, &["workflows", id, "source"], None, None)
                .await?;
            let source = source["source"]
                .as_str()
                .ok_or("Core did not return the original workflow source")?;
            atomic_publish_workflow_file(output, source.as_bytes(), *replace)?;
            if *as_json {
                print_json(&json!({ "id": id, "path": output, "scope": "selected-core" }))?;
            } else {
                println!("Exported {id} to {}", output.display());
            }
        }
        WorkflowCommand::Validate {
            workflow: Some(workflow),
            json: as_json,
            ..
        } => {
            let record = resolve(client, workflow).await?;
            let id = record["id"]
                .as_str()
                .ok_or("Core did not return the workflow ID")?;
            let result = client
                .request(Method::POST, &["workflows", id, "validate"], None, None)
                .await?;
            if *as_json {
                print_json(&result)?;
            } else {
                println!(
                    "{} is valid (selected Core)",
                    record["name"].as_str().unwrap_or(id)
                );
            }
        }
        WorkflowCommand::Plan {
            workflow: Some(workflow),
            json: as_json,
            ..
        } => {
            let record = resolve(client, workflow).await?;
            let id = record["id"]
                .as_str()
                .ok_or("Core did not return the workflow ID")?;
            let plan = client
                .request(Method::POST, &["workflows", id, "plan"], None, None)
                .await?;
            if *as_json {
                print_json(&plan)?;
            } else {
                render_plan_json(&plan);
            }
        }
        WorkflowCommand::List {
            limit,
            cursor,
            json: as_json,
            ..
        } => {
            let mut query = Vec::new();
            if let Some(limit) = limit {
                query.push(("limit".to_owned(), limit.to_string()));
            }
            if let Some(cursor) = cursor {
                query.push(("cursor".to_owned(), cursor.clone()));
            }
            let page = client
                .request_with_query(Method::GET, &["workflows"], &query)
                .await?;
            if *as_json {
                print_json(&page)?;
            } else {
                println!("ID\tNAME\tSTATUS\tREVISION");
                for item in page["items"].as_array().into_iter().flatten() {
                    println!(
                        "{}\t{}\t{}\t{}",
                        item["id"].as_str().unwrap_or(""),
                        item["name"].as_str().unwrap_or(""),
                        item["status"].as_str().unwrap_or(""),
                        item["revision"].as_str().unwrap_or("")
                    );
                }
                if let Some(cursor) = page["nextCursor"].as_str() {
                    println!("nextCursor\t{cursor}");
                }
            }
        }
        WorkflowCommand::Run {
            workflow: Some(workflow),
            request_key,
            json: as_json,
            ..
        } => {
            let record = resolve(client, workflow).await?;
            let id = record["id"]
                .as_str()
                .ok_or("Core did not return the workflow ID")?;
            let mut body = json!({});
            if let Some(request_key) = request_key {
                body["requestKey"] = json!(request_key);
            }
            let submission = client
                .request(
                    Method::POST,
                    &["workflows", id, "executions"],
                    Some(body),
                    None,
                )
                .await?;
            let execution_id = submission["executionId"]
                .as_str()
                .ok_or("Core did not return an execution ID")?;
            let result = json!({
                "id": execution_id,
                "executionId": execution_id,
                "status": submission["status"],
                "workflowRevision": submission["workflowRevision"],
            });
            if *as_json {
                print_json(&result)?;
            } else {
                println!(
                    "{execution_id}\t{}",
                    submission["status"].as_str().unwrap_or("accepted")
                );
            }
        }
        WorkflowCommand::Enable {
            workflow,
            if_revision,
            json: as_json,
        }
        | WorkflowCommand::Disable {
            workflow,
            if_revision,
            json: as_json,
        } => {
            let enabled = matches!(command, WorkflowCommand::Enable { .. });
            let record = resolve(client, workflow).await?;
            let id = record["id"]
                .as_str()
                .ok_or("Core did not return the workflow ID")?;
            let revision = if_revision
                .as_deref()
                .or_else(|| record["revision"].as_str())
                .ok_or("Core did not return the workflow revision")?;
            let updated = client
                .request(
                    Method::PUT,
                    &["workflows", id, "eligibility"],
                    Some(json!({ "enabled": enabled })),
                    Some(revision),
                )
                .await?;
            if *as_json {
                print_json(&updated)?;
            } else {
                println!(
                    "{} is {}",
                    updated["id"].as_str().unwrap_or(id),
                    updated["status"].as_str().unwrap_or("unknown")
                );
            }
        }
        WorkflowCommand::Create { .. }
        | WorkflowCommand::Validate { .. }
        | WorkflowCommand::Plan { .. }
        | WorkflowCommand::Run { .. } => {
            return Err("local workflow operations require --file".to_owned());
        }
    }
    Ok(())
}

async fn execute_management_execution(
    client: &kakune_core::remote::ManagementClient,
    command: &ExecutionCommand,
) -> Result<(), String> {
    use reqwest::Method;
    use serde_json::json;

    match command {
        ExecutionCommand::List {
            workflow_id,
            status,
            limit,
            cursor,
            json: as_json,
        } => {
            let mut query = Vec::new();
            if let Some(workflow_id) = workflow_id {
                query.push(("workflowId".to_owned(), workflow_id.clone()));
            }
            if let Some(status) = status {
                query.push(("status".to_owned(), status.clone()));
            }
            if let Some(limit) = limit {
                query.push(("limit".to_owned(), limit.to_string()));
            }
            if let Some(cursor) = cursor {
                query.push(("cursor".to_owned(), cursor.clone()));
            }
            let page = client
                .request_with_query(Method::GET, &["executions"], &query)
                .await?;
            if *as_json {
                print_json(&page)?;
            } else {
                println!("ID\tWORKFLOW\tSTATUS\tCREATED\tCOMPLETED");
                for item in page["items"].as_array().into_iter().flatten() {
                    println!(
                        "{}\t{}\t{}\t{}\t{}",
                        item["id"].as_str().unwrap_or(""),
                        item["workflowName"].as_str().unwrap_or(""),
                        item["status"].as_str().unwrap_or("unknown"),
                        item["createdAt"].as_str().unwrap_or(""),
                        item["completedAt"].as_str().unwrap_or(""),
                    );
                }
                if let Some(next) = page["nextCursor"].as_str() {
                    println!("nextCursor\t{next}");
                }
            }
        }
        ExecutionCommand::Inspect {
            execution_id,
            limit,
            cursor,
            json: as_json,
        } => {
            let mut query = Vec::new();
            if let Some(limit) = limit {
                query.push(("limit".to_owned(), limit.to_string()));
            }
            if let Some(cursor) = cursor {
                query.push(("cursor".to_owned(), cursor.clone()));
            }
            let detail = client
                .request_with_query(Method::GET, &["executions", execution_id], &query)
                .await?;
            if *as_json {
                print_json(&detail)?;
            } else {
                println!("ID\t{}", detail["id"].as_str().unwrap_or(execution_id));
                println!(
                    "WORKFLOW\t{}",
                    detail["workflowName"].as_str().unwrap_or("unknown")
                );
                println!("STATUS\t{}", detail["status"].as_str().unwrap_or("unknown"));
                println!(
                    "CREATED\t{}",
                    detail["createdAt"].as_str().unwrap_or("unknown")
                );
                println!(
                    "STARTED\t{}",
                    detail["startedAt"].as_str().unwrap_or("unavailable")
                );
                println!(
                    "COMPLETED\t{}",
                    detail["completedAt"].as_str().unwrap_or("unavailable")
                );
                println!(
                    "WORKFLOW REVISION\t{}",
                    detail["workflowRevision"].as_str().unwrap_or("unavailable")
                );
                for section in ["error", "result", "trace"] {
                    println!(
                        "{}\t{}\t{}",
                        section.to_ascii_uppercase(),
                        detail[section]["availability"]
                            .as_str()
                            .unwrap_or("unknown"),
                        detail[section]["guidance"].as_str().unwrap_or("")
                    );
                }
                println!(
                    "NODES\t{}",
                    detail["nodes"]["availability"]
                        .as_str()
                        .unwrap_or("unknown")
                );
                for node in detail["nodes"]["items"].as_array().into_iter().flatten() {
                    println!(
                        "{}\t{}\t{}",
                        node["nodeId"].as_str().unwrap_or(""),
                        node["status"].as_str().unwrap_or("unknown"),
                        node["detailsAvailability"]
                            .as_str()
                            .unwrap_or("unavailable")
                    );
                }
                if let Some(next) = detail["nodes"]["nextCursor"].as_str() {
                    println!("nextCursor\t{next}");
                }
            }
        }
        ExecutionCommand::Cancel {
            execution_id,
            wait_seconds,
            json: as_json,
        } => {
            let outcome = client
                .request(
                    Method::POST,
                    &["executions", execution_id, "cancel"],
                    Some(json!({"waitSeconds": wait_seconds.unwrap_or(0)})),
                    None,
                )
                .await?;
            if *as_json {
                print_json(&outcome)?;
            } else {
                println!(
                    "CANCELLATION OUTCOME\t{}",
                    outcome["outcome"].as_str().unwrap_or("unknown")
                );
                println!(
                    "OBSERVED STATUS\t{}",
                    outcome["observedStatus"].as_str().unwrap_or("unknown")
                );
                println!("PENDING\t{}", outcome["pending"].as_bool().unwrap_or(true));
            }
        }
        ExecutionCommand::Logs {
            execution_id,
            limit,
            cursor,
            high_water,
            json: as_json,
        } => {
            let mut query = Vec::new();
            if let Some(limit) = limit {
                query.push(("limit".to_owned(), limit.to_string()));
            }
            if let Some(cursor) = cursor {
                query.push(("cursor".to_owned(), cursor.clone()));
            }
            if let Some(high_water) = high_water {
                query.push(("highWater".to_owned(), high_water.to_string()));
            }
            let page = client
                .request_with_query(Method::GET, &["executions", execution_id, "logs"], &query)
                .await?;
            if *as_json {
                print_json(&page)?;
            } else {
                println!(
                    "HIGH WATER\t{}",
                    page["highWater"].as_u64().unwrap_or_default()
                );
                println!(
                    "RETENTION\t{}",
                    page["retentionAvailability"].as_str().unwrap_or("unknown")
                );
                for event in page["items"].as_array().into_iter().flatten() {
                    println!(
                        "{}\t{}\t{}\t{}",
                        event["sequence"].as_u64().unwrap_or_default(),
                        event["timestamp"].as_str().unwrap_or(""),
                        event["type"].as_str().unwrap_or(""),
                        event["payload"]
                    );
                }
                if let Some(next) = page["nextCursor"].as_str() {
                    println!("nextCursor\t{next}");
                }
                if page["truncated"].as_bool() == Some(true) {
                    println!(
                        "TRUNCATED\tSome event payloads were omitted at the safe section limit."
                    );
                }
            }
        }
    }
    Ok(())
}

async fn execute_management_trigger(
    client: &kakune_core::remote::ManagementClient,
    command: &TriggerCommand,
) -> Result<(), String> {
    use reqwest::Method;

    match command {
        TriggerCommand::List {
            limit,
            cursor,
            json: as_json,
        } => {
            let mut query = Vec::new();
            if let Some(limit) = limit {
                query.push(("limit".to_owned(), limit.to_string()));
            }
            if let Some(cursor) = cursor {
                query.push(("cursor".to_owned(), cursor.clone()));
            }
            let page = client
                .request_with_query(Method::GET, &["triggers"], &query)
                .await?;
            if *as_json {
                print_json(&page)?;
            } else {
                println!("ID\tWORKFLOW\tTRIGGER\tTYPE\tELIGIBILITY\tSTATE\tNEXT RUN");
                for item in page["items"].as_array().into_iter().flatten() {
                    println!(
                        "{}\t{}\t{}\t{}\t{}\t{}\t{}",
                        item["compositeId"].as_str().unwrap_or(""),
                        item["workflowName"].as_str().unwrap_or("unknown"),
                        item["triggerId"].as_str().unwrap_or(""),
                        item["triggerType"].as_str().unwrap_or("unknown"),
                        item["eligibility"].as_str().unwrap_or("unknown"),
                        item["effectiveState"].as_str().unwrap_or("unknown"),
                        item["nextRunAt"].as_str().unwrap_or("unavailable"),
                    );
                }
                if let Some(next) = page["nextCursor"].as_str() {
                    println!("nextCursor\t{next}");
                }
            }
        }
        TriggerCommand::Inspect {
            trigger_id,
            json: as_json,
        } => {
            let (workflow_id, trigger_id) = parse_composite_trigger_id(trigger_id)?;
            let trigger = client
                .request(
                    Method::GET,
                    &["triggers", &workflow_id, &trigger_id],
                    None,
                    None,
                )
                .await?;
            if *as_json {
                print_json(&trigger)?;
            } else {
                println!("ID\t{}", trigger["compositeId"].as_str().unwrap_or(""));
                println!(
                    "WORKFLOW\t{}",
                    trigger["workflowName"].as_str().unwrap_or("unknown")
                );
                println!("TRIGGER\t{}", trigger["triggerId"].as_str().unwrap_or(""));
                println!(
                    "TYPE\t{}",
                    trigger["triggerType"].as_str().unwrap_or("unknown")
                );
                println!(
                    "ELIGIBILITY\t{}",
                    trigger["eligibility"].as_str().unwrap_or("unknown")
                );
                println!(
                    "EFFECTIVE STATE\t{}",
                    trigger["effectiveState"].as_str().unwrap_or("unknown")
                );
                println!(
                    "NEXT RUN\t{}",
                    trigger["nextRunAt"].as_str().unwrap_or("unavailable")
                );
                println!(
                    "LAST RUN\t{}",
                    trigger["lastRunAt"].as_str().unwrap_or("unavailable")
                );
                if let Some(reason) = trigger["unavailableReason"].as_str() {
                    println!("OBSERVATION\t{reason}");
                }
                println!("CONFIGURATION\t{}", trigger["configuration"]);
            }
        }
    }
    Ok(())
}

fn parse_composite_trigger_id(value: &str) -> Result<(String, String), String> {
    let (workflow_id, trigger_id) = value
        .split_once('/')
        .ok_or_else(|| "trigger ID must be a composite workflow-id/trigger-id".to_owned())?;
    let workflow_id = decode_trigger_path_component(workflow_id)?;
    let trigger_id = decode_trigger_path_component(trigger_id)?;
    if workflow_id.is_empty() || trigger_id.is_empty() {
        return Err("composite trigger ID components must not be empty".to_owned());
    }
    Ok((workflow_id, trigger_id))
}

fn decode_trigger_path_component(value: &str) -> Result<String, String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let Some(hex) = bytes.get(index + 1..index + 3) else {
                return Err("trigger composite ID contains invalid percent encoding".to_owned());
            };
            let hex = std::str::from_utf8(hex)
                .map_err(|_| "trigger composite ID contains invalid percent encoding")?;
            decoded.push(
                u8::from_str_radix(hex, 16)
                    .map_err(|_| "trigger composite ID contains invalid percent encoding")?,
            );
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).map_err(|_| "trigger composite ID is not valid UTF-8".to_owned())
}

#[cfg(test)]
mod tests {
    use super::{
        AuthCommand, Cli, Command, ContextFile, DOCTOR_REPORT_FAILURE, ServiceCommand,
        configure_local_connection_with_credentials, execute, execute_with_credentials,
        local_endpoint, recover_local_auth_with_credential_writer, redact_lifecycle_diagnostic,
        render_init_report, render_service_error, render_service_operation_error,
        resolve_initialization_paths, run_initialization_with, save_local_context,
        selected_management_context, validate_dispatch_arguments, validate_pairing_endpoint,
    };
    use clap::Parser;
    use kakune_core::{
        AuthScope, Store,
        config::ApiConfig,
        initialization::{
            CredentialAccess, CredentialAccessError, CredentialReference, CredentialSecret,
            InitializationPaths,
        },
        lifecycle::{
            Deadline, DeadlineWaiter, LifecycleError, MonotonicClock, SystemMonotonicClock,
            launch::read_child_protocol,
            service::{
                NativeServiceObservation, ServiceAction, ServiceController, ServiceManagerAdapter,
                ServiceManagerKind, ServiceStateRoot,
            },
        },
    };
    use std::{
        fs,
        path::{Path, PathBuf},
        sync::Mutex,
        time::Instant,
    };

    struct ForbiddenCredentialAccess;

    impl CredentialAccess for ForbiddenCredentialAccess {
        fn resolve_reference(
            &self,
            _: Option<&str>,
        ) -> Result<CredentialReference, CredentialAccessError> {
            panic!("local dispatch rejection must precede credential access");
        }

        fn write_secure(
            &self,
            _: &CredentialReference,
            _: &CredentialSecret,
        ) -> Result<(), CredentialAccessError> {
            panic!("local dispatch rejection must precede credential access");
        }

        fn read(&self, _: &CredentialReference) -> Result<CredentialSecret, CredentialAccessError> {
            panic!("local dispatch rejection must precede credential access");
        }
    }

    #[tokio::test]
    async fn explicit_remote_selection_for_local_commands_is_rejected_before_io() {
        let cli = Cli::try_parse_from([
            "kakune",
            "--context",
            "remote-core",
            "--context-file",
            "missing-contexts.json",
            "doctor",
        ])
        .expect("CLI arguments should parse");
        let error = execute_with_credentials(cli, &ForbiddenCredentialAccess)
            .await
            .expect_err("local doctor must reject explicit remote selection");
        assert!(error.contains("local-only"));
    }

    #[test]
    fn global_and_context_command_file_flags_cannot_conflict() {
        let cli = Cli::try_parse_from([
            "kakune",
            "--context-file",
            "global.json",
            "context",
            "list",
            "--contexts-file",
            "local.json",
        ])
        .expect("CLI arguments should parse before conflict validation");
        assert!(
            validate_dispatch_arguments(&cli)
                .expect_err("two context files must be rejected")
                .contains("cannot be combined")
        );
    }

    #[test]
    fn canonical_management_context_resolution_never_synthesizes_a_local_core() {
        let contexts = ContextFile::default();
        assert!(
            selected_management_context(&contexts, None)
                .expect_err("no selected context must not fall back to localhost")
                .contains("no Core context is selected")
        );
        assert!(
            selected_management_context(&contexts, Some("missing"))
                .expect_err("unknown selected context must fail")
                .contains("was not found")
        );
    }

    #[test]
    fn data_directory_option_does_not_imply_standalone_execution() {
        let cli = Cli::try_parse_from([
            "kakune",
            "run",
            "workflow.kakune.yaml",
            "--data-dir",
            "isolated-data",
        ])
        .expect("legacy run arguments should parse");
        assert!(!cli.standalone);
    }

    struct CliFixture {
        root: PathBuf,
    }

    #[test]
    fn lifecycle_diagnostics_redact_credentials_capabilities_and_auth_headers() {
        let message = redact_lifecycle_diagnostic(
            "denied Authorization: Bearer live-token capability=private-proof api_key: hidden",
        );

        assert!(!message.contains("live-token"));
        assert!(!message.contains("private-proof"));
        assert!(!message.contains("hidden"));
        assert!(message.contains("[REDACTED]"));
    }

    #[test]
    fn service_error_rendering_preserves_distinct_causes_and_safe_remediation() {
        use kakune_core::lifecycle::LifecycleError;

        let denied = render_service_error(
            "install",
            LifecycleError::AccessDenied("Authorization: Bearer private-token".into()),
        );
        assert!(denied.contains("access to the native service manager was denied"));
        assert!(denied.contains("elevated administrator terminal"));
        assert!(!denied.contains("private-token"));

        let unavailable = render_service_error(
            "status",
            LifecycleError::ManagerUnavailable("systemd is not available".into()),
        );
        assert!(unavailable.contains("native service manager is unavailable"));
        assert!(unavailable.contains("Confirm the native service manager"));

        let conflict = render_service_error(
            "install",
            LifecycleError::ConflictingServiceDefinition("different data path".into()),
        );
        assert!(conflict.contains("service definition conflicts"));
        assert!(conflict.contains("different data path"));
        assert!(!conflict.contains("elevated administrator terminal"));

        let missing = render_service_error("start", LifecycleError::ServiceNotInstalled);
        assert!(missing.contains("requested native service is not installed"));
        assert!(!missing.contains("ManagerUnavailable"));
    }

    #[test]
    fn lifecycle_error_rendering_keeps_timeout_and_uncertain_state_truthful() {
        let timeout = render_service_error(
            "stop",
            LifecycleError::DeadlineExpired {
                phase: "service stop",
            },
        );
        assert!(timeout.contains("deadline expired during service stop"));
        assert!(!timeout.contains("stopped successfully"));

        let unconfirmed = render_service_error("start", LifecycleError::ServiceStateUnconfirmed);
        assert!(unconfirmed.contains("did not reach the requested state"));
        assert!(!unconfirmed.contains("started successfully"));

        let unresolved = render_service_error(
            "uninstall",
            LifecycleError::UnresolvedServiceAttempt("attempt-123".into()),
        );
        assert!(unresolved.contains("unresolved service mutation"));
        assert!(unresolved.contains("Inspect the recorded service attempt"));

        let helper = render_service_error("install", LifecycleError::HelperUnreaped { pid: 4242 });
        assert!(helper.contains("helper process 4242 could not be confirmed reaped"));
        assert!(!helper.contains("graceful"));
    }

    #[test]
    fn service_operation_failure_names_recovery_journal_without_claiming_cancellation() {
        struct NoopServiceManager;

        impl ServiceManagerAdapter for NoopServiceManager {
            fn observe(
                &self,
                _manager: ServiceManagerKind,
                _service_id: &str,
                _deadline: Deadline,
                _clock: &dyn MonotonicClock,
            ) -> Result<NativeServiceObservation, LifecycleError> {
                Ok(NativeServiceObservation::absent())
            }

            fn mutate(
                &self,
                _action: ServiceAction,
                _manager: ServiceManagerKind,
                _service_id: &str,
                _definition: Option<&kakune_core::lifecycle::service::ServiceDefinition>,
                _deadline: Deadline,
                _clock: &dyn MonotonicClock,
            ) -> Result<(), LifecycleError> {
                Ok(())
            }
        }

        struct NoopWaiter;

        impl DeadlineWaiter for NoopWaiter {
            fn wait(&self, _duration: std::time::Duration) {}
        }

        let root = std::env::temp_dir().join(format!("kakune-diagnostic-{}", uuid::Uuid::new_v4()));
        let controller = ServiceController::with_components(
            ServiceStateRoot::isolated(root.clone()).expect("isolated state root should validate"),
            std::sync::Arc::new(NoopServiceManager),
            std::sync::Arc::new(SystemMonotonicClock::default()),
            std::sync::Arc::new(NoopWaiter),
        );
        let rendered = render_service_operation_error(
            &controller,
            ServiceManagerKind::WindowsScm,
            "KakuneCore",
            "stop",
            LifecycleError::DeadlineExpired {
                phase: "service stop",
            },
        );

        let journal_path = controller
            .attempt_path_for(ServiceManagerKind::WindowsScm, "KakuneCore")
            .expect("the validated manager key should resolve its journal path");
        assert!(rendered.contains(&journal_path.display().to_string()));
        assert!(rendered.contains("Inspect it and the native manager before retrying"));
        assert!(rendered.contains("not reported as cancelled"));
        assert!(!rendered.contains("stop completed successfully"));
        assert!(rendered.contains("Current resources: unknown"));
        assert!(rendered.contains("No readable attempt snapshot"));
    }

    #[test]
    fn lifecycle_diagnostic_redaction_covers_sensitive_header_and_value_forms() {
        let message = redact_lifecycle_diagnostic(
            "Authorization: Bearer auth-value X-Access-Token=token-value secret: secret-value password=password-value credential=credential-value private-key=key-value client-secret=client-value API-Key: api-value",
        );

        for secret in [
            "auth-value",
            "token-value",
            "secret-value",
            "password-value",
            "credential-value",
            "key-value",
            "client-value",
            "api-value",
        ] {
            assert!(!message.contains(secret), "diagnostic leaked {secret}");
        }
        assert!(message.contains("[REDACTED]"));
    }

    #[test]
    fn lifecycle_diagnostics_redact_multiline_private_keys_and_cookie_headers() {
        let message = redact_lifecycle_diagnostic(
            "configuration failed\n-----BEGIN PRIVATE KEY-----\nprivate-key-material\n-----END PRIVATE KEY-----\nCookie: session=cookie-value; other=second-value\nSet-Cookie: session=response-value; HttpOnly\ninspect retained configuration",
        );
        for secret in [
            "private-key-material",
            "cookie-value",
            "second-value",
            "response-value",
        ] {
            assert!(
                !message.contains(secret),
                "diagnostic leaked {secret}: {message}"
            );
        }
        assert!(message.contains("configuration failed"));
        assert!(message.contains("inspect retained configuration"));
    }

    #[test]
    fn partial_service_snapshot_reports_residuals_without_claiming_current_state() {
        use kakune_core::lifecycle::service::{
            AttemptStatus, ServiceAttemptJournal, ServiceDefinition,
        };
        let definition =
            ServiceDefinition::for_platform(&std::env::current_dir().unwrap()).unwrap();
        for action in [ServiceAction::Install, ServiceAction::Remove] {
            let journal = ServiceAttemptJournal {
                schema_version: 1,
                attempt_id: uuid::Uuid::new_v4(),
                manager: definition.manager,
                native_service_id: definition.service_id.clone(),
                action,
                step: "registration".into(),
                expected_definition: definition.clone(),
                prior_state: NativeServiceObservation::absent(),
                helper_pid: Some(4242),
                helper_owner: None,
                status: AttemptStatus::OutcomeUnknown,
                completion_observation: None,
                residual_resources: vec![
                    "registration may remain".into(),
                    "capability=hidden-proof".into(),
                ],
                recovery_guidance: vec!["Inspect the native registration before retrying".into()],
            };
            let rendered = super::render_service_recovery_snapshot(&journal);
            for expected in [
                "historical, not current state",
                "registration may remain",
                "OutcomeUnknown",
                "4242",
                "Inspect the native registration",
                "does not prove cancellation",
                "Do not delete data",
            ] {
                assert!(
                    rendered.contains(expected),
                    "missing {expected}: {rendered}"
                );
            }
            assert!(!rendered.contains("hidden-proof"));
            assert!(!rendered.contains("graceful completion"));
        }
    }

    #[test]
    fn identity_account_and_mode_failures_do_not_suggest_generic_elevation() {
        for error in [
            LifecycleError::UnverifiedProcess,
            LifecycleError::WrongOwner,
            LifecycleError::AuthenticationFailed,
            LifecycleError::UnsafeOwnership("installed account cannot access configuration".into()),
            LifecycleError::ModeNotControllable("foreground".into()),
            LifecycleError::OwnershipBusy,
            LifecycleError::UnsupportedSchema(99),
        ] {
            let cause = error.to_string();
            let rendered = render_service_error("start", error);
            assert!(rendered.contains(&cause), "{rendered}");
            assert!(!rendered.contains("elevated administrator terminal"));
            assert!(
                rendered.contains("do not"),
                "missing safe recovery: {rendered}"
            );
        }
    }

    impl CliFixture {
        fn new() -> Self {
            let parent = std::env::temp_dir().join("opencode");
            fs::create_dir_all(&parent).expect("approved temporary parent should exist");
            let root = parent.join(format!("kakune-init-cli-{}", uuid::Uuid::new_v4()));
            fs::create_dir(&root).expect("fixture directory should be created");
            Self { root }
        }

        fn paths(&self) -> InitializationPaths {
            InitializationPaths {
                data_dir: self.root.join("data"),
                config_file: self.root.join("config").join("kakune.yaml"),
                context_file: self.root.join("contexts").join("contexts.json"),
            }
        }
    }

    impl Drop for CliFixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    struct CliCredentialDouble {
        saved: Mutex<Option<String>>,
        write_fails: bool,
    }

    impl CliCredentialDouble {
        fn new(write_fails: bool) -> Self {
            Self {
                saved: Mutex::new(None),
                write_fails,
            }
        }

        fn saved_secret(&self) -> Option<String> {
            self.saved.lock().ok()?.clone()
        }
    }

    impl CredentialAccess for CliCredentialDouble {
        fn resolve_reference(
            &self,
            reference: Option<&str>,
        ) -> Result<CredentialReference, CredentialAccessError> {
            CredentialReference::new(reference.unwrap_or("env:KAKUNE_TOKEN"))
        }

        fn write_secure(
            &self,
            _reference: &CredentialReference,
            secret: &CredentialSecret,
        ) -> Result<(), CredentialAccessError> {
            if self.write_fails {
                return Err(CredentialAccessError::SecureWriteFailed);
            }
            *self
                .saved
                .lock()
                .map_err(|_| CredentialAccessError::SecureWriteFailed)? =
                Some(secret.expose_secret().to_string());
            Ok(())
        }

        fn read(
            &self,
            _reference: &CredentialReference,
        ) -> Result<CredentialSecret, CredentialAccessError> {
            self.saved
                .lock()
                .map_err(|_| CredentialAccessError::Unavailable)?
                .as_ref()
                .map(|secret| CredentialSecret::new(secret.clone()))
                .ok_or(CredentialAccessError::Unavailable)
        }
    }

    fn assert_path_is_under(path: &Path, root: &Path) {
        assert!(
            path.starts_with(root),
            "{} should be under {}",
            path.display(),
            root.display()
        );
    }

    #[test]
    fn init_adapter_resolves_default_and_explicit_paths_without_environment_mutation() {
        let fixture = CliFixture::new();
        let simulated_kakune_data_dir = fixture.root.join("KAKUNE_DATA_DIR");
        let defaults =
            resolve_initialization_paths(None, None, None, simulated_kakune_data_dir.clone());
        assert_eq!(defaults.data_dir, simulated_kakune_data_dir);
        assert_eq!(defaults.config_file, defaults.data_dir.join("kakune.yaml"));
        assert_eq!(
            defaults.context_file,
            defaults.data_dir.join("cli").join("contexts.json")
        );

        let explicit = resolve_initialization_paths(
            Some(fixture.root.join("custom data")),
            Some(fixture.root.join("custom config.yaml")),
            Some(fixture.root.join("custom contexts.json")),
            fixture.root.join("unused default"),
        );
        assert_path_is_under(&explicit.data_dir, &fixture.root);
        assert_eq!(explicit.data_dir, fixture.root.join("custom data"));
        assert_eq!(
            explicit.config_file,
            fixture.root.join("custom config.yaml")
        );
        assert_eq!(
            explicit.context_file,
            fixture.root.join("custom contexts.json")
        );
    }

    #[test]
    fn init_parser_accepts_standalone_and_ca_compatibility_no_ops() {
        let parsed = Cli::try_parse_from([
            "kakune",
            "--standalone",
            "--ca",
            "missing-ca.pem",
            "init",
            "--data-dir",
            "isolated-data",
            "--config",
            "isolated.yaml",
        ])
        .expect("init should accept documented compatibility options");
        assert!(parsed.standalone);
        assert_eq!(parsed.ca, Some(PathBuf::from("missing-ca.pem")));
        assert!(matches!(parsed.command, Command::Init { .. }));
    }

    #[tokio::test]
    async fn service_install_keeps_data_dir_option_and_rejects_remote_scope_before_io() {
        let fixture = CliFixture::new();
        let data_dir = fixture.root.join("uncreated service data");
        let parsed = Cli::try_parse_from([
            "kakune",
            "--context",
            "remote",
            "service",
            "install",
            "--data-dir",
            data_dir.to_str().expect("fixture path is UTF-8"),
        ])
        .expect("service install should accept --data-dir after the subcommand");
        assert!(matches!(
            &parsed.command,
            Command::Service {
                command: ServiceCommand::Install { data_dir: Some(path) }
            } if path == &data_dir
        ));

        let error = execute_with_credentials(parsed, &CliCredentialDouble::new(false))
            .await
            .expect_err("remote service operations must be rejected before native access");
        assert!(error.contains("do not accept --context"));
        assert!(!data_dir.exists());
    }

    #[tokio::test]
    async fn init_uses_injected_local_setup_without_starting_core_or_running_recovery() {
        let fixture = CliFixture::new();
        let paths = fixture.paths();
        let credentials = CliCredentialDouble::new(false);
        let cli = Cli {
            context: None,
            context_file: Some(paths.context_file.clone()),
            ca: None,
            standalone: false,
            command: Command::Init {
                data_dir: Some(paths.data_dir.clone()),
                config: Some(paths.config_file.clone()),
            },
        };

        execute_with_credentials(cli, &credentials)
            .await
            .expect("injected local initialization should complete");

        let secret = credentials
            .saved_secret()
            .expect("initialization should use the injected secure credential adapter");
        let store = Store::open(paths.data_dir.clone()).expect("initialized store should open");
        let tokens = store.list_auth_tokens().expect("token history should load");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].name, "Initial local administrator");
        assert!(
            store
                .authorize_scope(&secret, AuthScope::Admin)
                .expect("local authorization should be checked")
        );
        assert!(paths.context_file.is_file());
        assert!(!paths.data_dir.join("daemon.pid").exists());
        assert!(!paths.data_dir.join("daemon.log").exists());
    }

    #[test]
    fn daemon_connection_preparation_keeps_bootstrap_separate_from_recovery() {
        let fixture = CliFixture::new();
        let paths = fixture.paths();
        let store = Store::open(paths.data_dir.clone()).expect("fixture store should open");
        let api = ApiConfig::default();
        let listen = "127.0.0.1:8787"
            .parse()
            .expect("loopback address should parse");
        let mut first_secret = None;
        let mut first_reference = None;

        let first = configure_local_connection_with_credentials(
            &store,
            &paths.context_file,
            &api,
            listen,
            |reference, secret| {
                first_reference = Some(reference.to_string());
                first_secret = secret.map(str::to_string);
                secret.is_some()
            },
        )
        .expect("daemon preparation should configure bootstrap access");

        assert!(first.credential_available);
        assert!(first.fallback_token.is_none());
        let bootstrap = first_secret.expect("first-use setup should provide bootstrap access");
        let reference = first_reference.expect("local access should use a canonical reference");
        let before_repeat = store
            .list_auth_tokens()
            .expect("bootstrap history should load");
        assert_eq!(before_repeat.len(), 1);

        let repeat = configure_local_connection_with_credentials(
            &store,
            &paths.context_file,
            &api,
            listen,
            |next_reference, secret| {
                assert_eq!(next_reference, reference);
                assert!(
                    secret.is_none(),
                    "repeat startup must reuse existing access"
                );
                true
            },
        )
        .expect("repeat daemon preparation should reuse existing access");

        assert!(repeat.credential_available);
        assert!(repeat.fallback_token.is_none());
        let after_repeat = store
            .list_auth_tokens()
            .expect("repeated history should load");
        assert_eq!(after_repeat.len(), 1, "startup must not become recovery");
        assert_eq!(before_repeat[0].id, after_repeat[0].id);
        assert!(
            store
                .authorize_scope(&bootstrap, AuthScope::Admin)
                .expect("the existing bootstrap credential should remain authorized")
        );
    }

    #[tokio::test]
    async fn doctor_remains_diagnostic_and_does_not_recover_or_configure_local_access() {
        let fixture = CliFixture::new();
        let paths = fixture.paths();
        let store = Store::open(paths.data_dir.clone()).expect("fixture store should open");
        let existing = store
            .create_auth_token(
                "Existing administrative access".to_string(),
                vec![AuthScope::Admin],
                None,
            )
            .expect("fixture credential should be issued");
        drop(store);

        let credentials = CliCredentialDouble::new(false);
        let cli = Cli {
            context: None,
            context_file: Some(paths.context_file.clone()),
            ca: None,
            standalone: true,
            command: Command::Doctor {
                data_dir: Some(paths.data_dir.clone()),
                config: Some(paths.config_file.clone()),
                json: false,
            },
        };
        assert_eq!(
            execute_with_credentials(cli, &credentials)
                .await
                .expect_err("missing required diagnostic evidence must return a failing report"),
            DOCTOR_REPORT_FAILURE
        );

        let after = Store::open(paths.data_dir.clone()).expect("diagnosed store should open");
        let tokens = after
            .list_auth_tokens()
            .expect("diagnosed token history should load");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].id, existing.record.id);
        assert!(tokens[0].revoked_at.is_none());
        assert!(credentials.saved_secret().is_none());
        assert!(!paths.context_file.exists());
    }

    #[test]
    fn explicit_auth_recovery_still_replaces_active_access_with_injected_credentials() {
        let fixture = CliFixture::new();
        let paths = fixture.paths();
        let parsed = Cli::try_parse_from([
            "kakune",
            "--standalone",
            "--context-file",
            paths
                .context_file
                .to_str()
                .expect("fixture path should be UTF-8"),
            "auth",
            "recover",
            "--data-dir",
            paths
                .data_dir
                .to_str()
                .expect("fixture path should be UTF-8"),
        ])
        .expect("explicit auth recover should remain a supported command");
        assert!(matches!(
            parsed.command,
            Command::Auth {
                command: AuthCommand::Recover { .. }
            }
        ));

        let store = Store::open(paths.data_dir.clone()).expect("fixture store should open");
        let previous = store
            .create_auth_token(
                "Existing administrative access".to_string(),
                vec![AuthScope::Admin],
                None,
            )
            .expect("fixture credential should be issued");
        let api = ApiConfig::default();
        let listen = "127.0.0.1:8787"
            .parse()
            .expect("loopback address should parse");
        let mut new_secret = None;
        let mut new_reference = None;

        let outcome = recover_local_auth_with_credential_writer(
            &store,
            &paths.context_file,
            &api,
            listen,
            |reference, secret| {
                new_reference = Some(reference.to_string());
                new_secret = Some(secret.to_string());
                true
            },
        )
        .expect("explicit recovery should persist and authorize replacement access");

        assert_eq!(outcome.revoked_count, 1);
        assert!(outcome.fallback_token.is_none());
        let tokens = store
            .list_auth_tokens()
            .expect("recovered history should load");
        assert_eq!(tokens.len(), 2);
        let old = tokens
            .iter()
            .find(|token| token.id == previous.record.id)
            .expect("the old token should remain in history");
        assert!(old.revoked_at.is_some());
        let new_secret = new_secret.expect("secure writer should receive the new credential");
        assert!(
            store
                .authorize_scope(&new_secret, AuthScope::Admin)
                .expect("replacement access should authorize locally")
        );
        let contexts = ContextFile::load(&paths.context_file)
            .expect("recovery should restore the local context");
        assert_eq!(
            contexts.contexts[0].credential_ref.as_deref(),
            new_reference.as_deref()
        );
    }

    #[tokio::test]
    async fn init_rejects_remote_context_before_filesystem_or_lock_side_effects() {
        let fixture = CliFixture::new();
        let paths = fixture.paths();
        let parsed = Cli::try_parse_from([
            "kakune",
            "--context",
            "remote",
            "--context-file",
            paths.context_file.to_str().expect("fixture path is UTF-8"),
            "init",
            "--data-dir",
            paths.data_dir.to_str().expect("fixture path is UTF-8"),
            "--config",
            paths.config_file.to_str().expect("fixture path is UTF-8"),
        ])
        .expect("the argument parser accepts context so the local adapter can reject it");

        let error = super::execute(parsed)
            .await
            .expect_err("init must reject explicit remote selection");
        assert!(error.contains("local-only"));
        assert!(!paths.data_dir.exists());
        assert!(!paths.config_file.exists());
        assert!(!paths.context_file.exists());
    }

    #[tokio::test]
    async fn daemon_lifecycle_rejects_remote_context_before_local_side_effects() {
        let fixture = CliFixture::new();
        let paths = fixture.paths();
        let parsed = Cli::try_parse_from([
            "kakune",
            "--context",
            "remote",
            "--context-file",
            paths.context_file.to_str().expect("fixture path is UTF-8"),
            "daemon",
            "--data-dir",
            paths.data_dir.to_str().expect("fixture path is UTF-8"),
            "--config",
            paths.config_file.to_str().expect("fixture path is UTF-8"),
            "status",
        ])
        .expect("daemon parser should preserve harmless global options");

        let error = execute(parsed)
            .await
            .expect_err("daemon lifecycle must reject an explicit remote context");
        assert!(error.contains("local"));
        assert!(!paths.data_dir.exists());
        assert!(!paths.config_file.exists());
        assert!(!paths.context_file.exists());
    }

    #[tokio::test]
    async fn standalone_daemon_status_stays_local_and_does_not_initialize_paths() {
        let fixture = CliFixture::new();
        let paths = fixture.paths();
        let parsed = Cli::try_parse_from([
            "kakune",
            "--standalone",
            "--context-file",
            paths.context_file.to_str().expect("fixture path is UTF-8"),
            "daemon",
            "--data-dir",
            paths.data_dir.to_str().expect("fixture path is UTF-8"),
            "--config",
            paths.config_file.to_str().expect("fixture path is UTF-8"),
            "status",
        ])
        .expect("standalone and context-file options should parse for daemon status");
        let credentials = CliCredentialDouble::new(false);

        execute_with_credentials(parsed, &credentials)
            .await
            .expect("standalone must not change local daemon status scope");

        assert!(!paths.data_dir.exists());
        assert!(!paths.config_file.exists());
        assert!(!paths.context_file.exists());
        assert!(credentials.saved_secret().is_none());
    }

    #[tokio::test]
    async fn background_parent_waits_for_delayed_child_readiness() {
        use tokio::io::AsyncWriteExt;

        let (mut writer, mut reader) = tokio::io::duplex(64);
        let clock = SystemMonotonicClock::default();
        let deadline = Deadline::after(&clock, std::time::Duration::from_secs(2));
        let delayed_child = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            writer
                .write_all(b"READY\tprepared-instance\n")
                .await
                .expect("delayed readiness should be sent");
        });
        let started = Instant::now();

        let message = read_child_protocol(&mut reader, deadline, &clock)
            .await
            .expect("delayed protocol message should arrive");
        delayed_child
            .await
            .expect("delayed child task should complete");

        assert_eq!(message.as_deref(), Some("READY\tprepared-instance"));
        assert!(started.elapsed() >= std::time::Duration::from_millis(40));
    }

    #[tokio::test]
    async fn version_dispatch_precedes_remote_and_local_file_access() {
        let fixture = CliFixture::new();
        let context_file = fixture.root.join("poison-missing-contexts.json");
        let ca_file = fixture.root.join("poison-missing-ca.pem");
        let cli = Cli {
            context: Some("unreachable-remote".to_string()),
            context_file: Some(context_file.clone()),
            ca: Some(ca_file.clone()),
            standalone: false,
            command: Command::Version,
        };

        execute(cli)
            .await
            .expect("version must exit before remote routing or local resource access");

        assert!(!context_file.exists());
        assert!(!ca_file.exists());
    }

    #[test]
    fn injected_init_adapter_reports_complete_and_secret_free_partial_results() {
        let fixture = CliFixture::new();
        let paths = fixture.paths();
        let report = run_initialization_with(paths.clone(), &CliCredentialDouble::new(false))
            .expect("injected secure-access success should complete setup");
        let rendered = render_init_report(&report);
        assert!(rendered.contains("Kakune local initialization complete"));
        assert!(rendered.contains("Client access: created"));
        assert!(rendered.contains(paths.data_dir.to_string_lossy().as_ref()));

        let failed_paths = InitializationPaths {
            data_dir: fixture.root.join("failed-data"),
            config_file: fixture.root.join("failed-config").join("kakune.yaml"),
            context_file: fixture.root.join("failed-contexts").join("contexts.json"),
        };
        let error = run_initialization_with(failed_paths.clone(), &CliCredentialDouble::new(true))
            .expect_err("injected secure-save failure must remain partial");
        assert!(error.contains("incomplete"));
        assert!(error.contains("auth recover"));
        assert!(error.contains(failed_paths.data_dir.to_string_lossy().as_ref()));
        assert!(!error.contains("kakune_"));
        assert!(!failed_paths.context_file.exists());
    }

    #[test]
    fn existing_initialization_failures_render_partial_paths_without_secrets_or_success_claims() {
        let fixture = CliFixture::new();
        let paths = fixture.paths();
        let credentials = CliCredentialDouble::new(false);
        run_initialization_with(paths.clone(), &credentials)
            .expect("initial setup should complete before error-rendering cases");
        let original_secret = credentials
            .saved_secret()
            .expect("first-use credential should be available to the test double");

        let unusable = CliCredentialDouble::new(false);
        *unusable.saved.lock().unwrap() = Some("kakune_secret_bearing_fixture_value".to_string());
        let invalid_access = run_initialization_with(paths.clone(), &unusable)
            .expect_err("a non-authorizing existing credential should fail");
        assert!(invalid_access.starts_with("initialization incomplete"));
        assert!(invalid_access.contains("auth recover"));
        assert!(invalid_access.contains(paths.data_dir.to_string_lossy().as_ref()));
        assert!(invalid_access.contains(paths.config_file.to_string_lossy().as_ref()));
        assert!(invalid_access.contains(paths.context_file.to_string_lossy().as_ref()));
        assert!(!invalid_access.contains("Kakune local initialization complete"));
        assert!(!invalid_access.contains(&original_secret));
        assert!(!invalid_access.contains("kakune_secret_bearing_fixture_value"));

        let conflict = serde_json::json!({
            "format": "kakune-contexts/v1",
            "exportedAt": "2026-10-01T12:00:00Z",
            "activeContextId": "local",
            "contexts": [{
                "id": "local",
                "name": "Preserve conflicting local context",
                "endpoint": "https://custom.example.test:9443",
                "expectedCoreId": "another-installation",
                "credentialRef": "keychain:do-not-replace"
            }]
        });
        fs::write(
            &paths.context_file,
            serde_json::to_vec_pretty(&conflict).unwrap(),
        )
        .unwrap();
        let original_conflict = fs::read(&paths.context_file).unwrap();
        let context_conflict = run_initialization_with(paths.clone(), &credentials)
            .expect_err("identity conflict should be rendered as a partial result");
        assert!(context_conflict.starts_with("initialization incomplete"));
        assert!(context_conflict.contains("local context"));
        assert!(context_conflict.contains(paths.context_file.to_string_lossy().as_ref()));
        assert!(!context_conflict.contains("Kakune local initialization complete"));
        assert!(!context_conflict.contains(&original_secret));
        assert_eq!(fs::read(&paths.context_file).unwrap(), original_conflict);

        fs::write(&paths.context_file, b"{malformed json").unwrap();
        let malformed_bytes = fs::read(&paths.context_file).unwrap();
        let malformed_metadata = run_initialization_with(paths.clone(), &credentials)
            .expect_err("malformed existing context metadata should be reported");
        assert!(malformed_metadata.starts_with("initialization incomplete"));
        assert!(malformed_metadata.contains("local context"));
        assert!(malformed_metadata.contains(paths.context_file.to_string_lossy().as_ref()));
        assert!(!malformed_metadata.contains(&original_secret));
        assert_eq!(fs::read(&paths.context_file).unwrap(), malformed_bytes);

        let migration_paths = InitializationPaths {
            data_dir: fixture.root.join("migration-data"),
            config_file: fixture.root.join("migration-config").join("kakune.yaml"),
            context_file: fixture
                .root
                .join("migration-contexts")
                .join("contexts.json"),
        };
        let migration_credentials = CliCredentialDouble::new(false);
        run_initialization_with(migration_paths.clone(), &migration_credentials)
            .expect("migration fixture should initialize before it is marked unsupported");
        let migration_database = migration_paths.data_dir.join("kakune.sqlite3");
        let connection = rusqlite::Connection::open(&migration_database).unwrap();
        connection
            .execute("INSERT INTO schema_migrations (version) VALUES (999)", [])
            .unwrap();
        drop(connection);
        let database_before = fs::read(&migration_database).unwrap();
        let migration_error =
            run_initialization_with(migration_paths.clone(), &migration_credentials)
                .expect_err("unsupported storage should report incomplete setup");
        assert!(migration_error.starts_with("initialization incomplete"));
        assert!(migration_error.contains("unsupported storage format"));
        assert!(migration_error.contains(migration_paths.data_dir.to_string_lossy().as_ref()));
        assert!(!migration_error.contains("Kakune local initialization complete"));
        assert!(!migration_error.contains(&original_secret));
        assert_eq!(fs::read(&migration_database).unwrap(), database_before);

        let retry_paths = InitializationPaths {
            data_dir: fixture.root.join("retry-data"),
            config_file: fixture.root.join("retry-config").join("kakune.yaml"),
            context_file: fixture.root.join("retry-contexts").join("contexts.json"),
        };
        let failed_save = CliCredentialDouble::new(true);
        let secure_save_error = run_initialization_with(retry_paths.clone(), &failed_save)
            .expect_err("secure-save failure should remain visibly incomplete");
        assert!(secure_save_error.starts_with("initialization incomplete"));
        assert!(secure_save_error.contains("auth recover"));
        assert!(secure_save_error.contains(retry_paths.data_dir.to_string_lossy().as_ref()));
        assert!(!secure_save_error.contains("Kakune local initialization complete"));
        assert!(!secure_save_error.contains("kakune_"));
        let before_retry = kakune_core::Store::open(retry_paths.data_dir.clone()).unwrap();
        let token_count = before_retry.list_auth_tokens().unwrap().len();
        assert_eq!(token_count, 1);
        drop(before_retry);

        let retry_error =
            run_initialization_with(retry_paths.clone(), &CliCredentialDouble::new(false))
                .expect_err("retry must not silently replace an unsaved initial credential");
        assert!(retry_error.starts_with("initialization incomplete"));
        assert!(retry_error.contains("auth recover"));
        assert!(retry_error.contains(retry_paths.data_dir.to_string_lossy().as_ref()));
        assert!(!retry_error.contains("Kakune local initialization complete"));
        assert!(!retry_error.contains("kakune_"));
        let after_retry = kakune_core::Store::open(retry_paths.data_dir.clone()).unwrap();
        assert_eq!(after_retry.list_auth_tokens().unwrap().len(), token_count);
    }

    #[test]
    fn local_connection_persists_only_credential_reference_and_selects_local_context() {
        let directory = std::env::temp_dir().join(format!(
            "kakune-local-context-test-{}",
            uuid::Uuid::new_v4()
        ));
        let path = directory.join("cli").join("contexts.json");

        save_local_context(
            &path,
            "core-123",
            "http://127.0.0.1:8787",
            Some("keychain:kakune/core/core-123".to_string()),
        )
        .expect("local context should save");
        let contexts = ContextFile::load(&path).expect("local context should reload");
        let source = fs::read_to_string(&path).expect("context file should be readable");

        assert_eq!(contexts.active_context_id.as_deref(), Some("local"));
        assert_eq!(
            contexts.contexts[0].expected_core_id.as_deref(),
            Some("core-123")
        );
        assert_eq!(
            contexts.contexts[0].credential_ref.as_deref(),
            Some("keychain:kakune/core/core-123")
        );
        assert!(!source.contains("kakune_"));

        fs::remove_dir_all(directory).expect("temporary context directory should be removed");
    }

    #[test]
    fn local_connection_uses_loopback_for_wildcard_listeners() {
        let config = ApiConfig::default();
        assert_eq!(
            local_endpoint(
                &config,
                "0.0.0.0:8787".parse().expect("IPv4 socket is valid")
            ),
            "http://127.0.0.1:8787"
        );
        assert_eq!(
            local_endpoint(&config, "[::]:8787".parse().expect("IPv6 socket is valid")),
            "http://[::1]:8787"
        );
    }

    #[test]
    fn remote_pairing_requires_https_but_loopback_can_use_http() {
        assert_eq!(
            validate_pairing_endpoint("http://127.0.0.1:8787")
                .expect("loopback endpoint should be accepted"),
            "http://127.0.0.1:8787"
        );
        assert!(validate_pairing_endpoint("http://192.168.1.20:8787").is_err());
        assert_eq!(
            validate_pairing_endpoint("https://core.example.com:8787/")
                .expect("HTTPS endpoint should be accepted"),
            "https://core.example.com:8787"
        );
    }
}
