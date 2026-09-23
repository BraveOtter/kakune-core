use std::{
    fs,
    io::{self, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Command as ProcessCommand, ExitCode, Stdio},
};

use clap::{Args, Parser, Subcommand};

#[cfg(windows)]
mod windows_service_host;
use kakune_core::{
    AuthPairingState, AuthScope, ConnectionContext, ContextFile, CoreConfig, PluginRegistry,
    ProviderAuth, ProviderProfile, ProviderProfileDiagnostic, ProviderProfileStatus,
    ProviderProfileUpsert, ProviderType, RetentionPolicy, Store, analyze_workflow_with_plugins,
    api, default_contexts_path, default_data_dir, load_installed_plugin_registry,
    plugin_install::{self, PluginSource},
    run_workflow_with_plugins, start_scheduler,
};

#[derive(Debug, Parser)]
#[command(name = "kakune", version, about = "Kakune local automation runtime")]
struct Cli {
    #[arg(long, global = true)]
    context: Option<String>,
    #[arg(long, global = true)]
    context_file: Option<PathBuf>,
    #[arg(long, global = true)]
    ca: Option<PathBuf>,
    /// Operate directly on local data instead of calling a Core API.
    #[arg(long, global = true, conflicts_with = "context")]
    standalone: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Creates the Kakune data directory and SQLite database.
    Init {
        #[arg(long)]
        data_dir: Option<PathBuf>,
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Runs the Core HTTP API in the foreground.
    Daemon(DaemonArgs),
    /// Backs up, restores, retains, and compacts Core data.
    Storage {
        #[command(subcommand)]
        command: StorageCommand,
    },
    /// Installs and operates the native Windows service.
    Service {
        #[command(subcommand)]
        command: ServiceCommand,
    },
    /// Shows local configuration, storage, and idle-runtime diagnostics.
    Doctor {
        #[arg(long)]
        data_dir: Option<PathBuf>,
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Manages portable connection metadata without persisting token values.
    Context {
        #[command(subcommand)]
        command: ContextCommand,
    },
    /// Recovers local API access and manages local authentication.
    Auth {
        #[command(subcommand)]
        command: AuthCommand,
    },
    /// Validates, enables, disables, and lists workflow YAML documents.
    Workflow {
        #[command(subcommand)]
        command: WorkflowCommand,
    },
    /// Installs, lists, or removes explicitly trusted local process plugins.
    Plugin {
        #[command(subcommand)]
        command: PluginCommand,
    },
    /// Manages non-secret provider profiles and ChatGPT OAuth login state.
    Provider {
        #[command(subcommand)]
        command: ProviderCommand,
    },
    /// Calls one explicitly configured MCP tool without using an AI workflow.
    Mcp {
        #[command(subcommand)]
        command: McpCommand,
    },
    /// Runs a workflow YAML file once and prints its execution record.
    Run {
        workflow: PathBuf,
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    /// Lists persisted execution summaries.
    Executions {
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    /// Inspects a persisted execution and its node runs.
    Inspect {
        execution_id: String,
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
}

#[derive(Debug, Args)]
struct DaemonArgs {
    #[arg(long, global = true)]
    tls_cert: Option<PathBuf>,
    #[arg(long, global = true)]
    tls_key: Option<PathBuf>,
    #[arg(long, global = true)]
    allowed_host: Vec<String>,
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
    Install {
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    Uninstall,
    Start,
    Stop,
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
        id: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        endpoint: String,
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
        destination: PathBuf,
        #[arg(long)]
        contexts_file: Option<PathBuf>,
    },
}

#[derive(Debug, Subcommand)]
enum AuthCommand {
    /// Replaces all active API tokens and restores this CLI's local connection.
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
    /// Lists provider profiles without configuration or credential material.
    List {
        #[arg(long)]
        data_dir: Option<PathBuf>,
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
    /// Calls one MCP tool from a JSON configuration file using store secret references.
    Call {
        json_file: PathBuf,
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
}

#[derive(Debug, Subcommand)]
enum WorkflowCommand {
    Validate {
        workflow: PathBuf,
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    List {
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    Enable {
        workflow: PathBuf,
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    Disable {
        id: String,
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    match execute(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("kakune: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn execute(cli: Cli) -> Result<(), String> {
    let context_file = cli.context_file.clone();
    if !cli.standalone
        && !matches!(&cli.command, Command::Auth { .. })
        && (cli.context.is_some() || matches!(&cli.command, Command::Run { .. }))
    {
        return execute_remote(&cli).await;
    }
    match cli.command {
        Command::Init { data_dir, config } => {
            let data_dir = data_dir.unwrap_or_else(default_data_dir);
            let loaded = CoreConfig::load_or_create(&data_dir, config)?;
            let store = Store::open(data_dir.clone())?;
            println!(
                "Initialized Kakune data directory at {}",
                store.data_dir().display()
            );
            println!("Configuration: {}", loaded.path.display());
            let contexts_file = context_file
                .clone()
                .unwrap_or_else(|| default_contexts_path(data_dir));
            configure_local_connection(
                &store,
                &contexts_file,
                &loaded.config.api,
                loaded.config.listen_addr()?,
            )?;
        }
        Command::Daemon(options) => execute_daemon(options, context_file).await?,
        Command::Storage { command } => execute_storage(command)?,
        Command::Service { command } => execute_service(command)?,
        Command::Doctor { data_dir, config } => {
            let data_dir = data_dir.unwrap_or_else(default_data_dir);
            let loaded = CoreConfig::load_or_create(&data_dir, config)?;
            let store = Store::open(data_dir)?;
            println!("dataDir\t{}", store.data_dir().display());
            println!("config\t{}", loaded.path.display());
            println!("schemaVersion\t{}", store.schema_version()?);
            println!("apiListen\t{}", loaded.config.api.listen);
            println!("aiRuntimes\tnot initialized until an AI workflow node executes");
            println!("backgroundDaemon\t{}", daemon_status(store.data_dir())?);
            print_json(&store.operational_metrics()?)?;
        }
        Command::Context { command } => execute_context(command)?,
        Command::Auth { command } => execute_auth(command, context_file)?,
        Command::Workflow { command } => match command {
            WorkflowCommand::Validate { workflow, data_dir } => {
                let source = std::fs::read_to_string(&workflow)
                    .map_err(|error| format!("cannot read {}: {error}", workflow.display()))?;
                let plugins = match data_dir {
                    Some(data_dir) => {
                        let store = Store::open(data_dir)?;
                        load_installed_plugin_registry(&store)?
                    }
                    None => PluginRegistry::default(),
                };
                let analysis = analyze_workflow_with_plugins(&source, &plugins);
                if !analysis.is_valid() {
                    return Err(analysis
                        .diagnostics
                        .into_iter()
                        .map(|diagnostic| diagnostic.message)
                        .collect::<Vec<_>>()
                        .join("; "));
                }
                let document = analysis.workflow.expect("a valid analysis has a workflow");
                println!("{} is valid", document.metadata.name);
            }
            WorkflowCommand::List { data_dir } => {
                let store = Store::open(data_dir.unwrap_or_else(default_data_dir))?;
                for workflow in store.list_workflows()? {
                    println!("{}\t{}", workflow.name, workflow.status);
                }
            }
            WorkflowCommand::Enable { workflow, data_dir } => {
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
                let record = store.upsert_workflow(&document, &source, "enabled")?;
                println!("Enabled {}", record.name);
            }
            WorkflowCommand::Disable { id, data_dir } => {
                let store = Store::open(data_dir.unwrap_or_else(default_data_dir))?;
                store.set_workflow_status(&id, "disabled")?;
                println!("Disabled {id}");
            }
        },
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
        },
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

fn execute_service(command: ServiceCommand) -> Result<(), String> {
    #[cfg(windows)]
    {
        windows_service_host::execute(command)
    }
    #[cfg(not(windows))]
    {
        let _ = command;
        Err("native service commands are available on Windows; use the packaged systemd or launchd unit on this platform".to_string())
    }
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
        ProviderCommand::List { data_dir } => {
            let store = Store::open(data_dir.unwrap_or_else(default_data_dir))?;
            for profile in store.list_provider_profiles()? {
                print_provider_summary(&profile);
            }
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
            let tokens = kakune_core::codex::login_with_browser()
                .await
                .map_err(|error| error.to_string())?;
            store.set_secret(
                secret_ref,
                &tokens.to_secret().map_err(|error| error.to_string())?,
            )?;
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
    let data_dir = options.data_dir.unwrap_or_else(default_data_dir);
    let mut loaded = CoreConfig::load_or_create(&data_dir, options.config.clone())?;
    if options.tls_cert.is_some() {
        loaded.config.api.tls_cert = options.tls_cert;
    }
    if options.tls_key.is_some() {
        loaded.config.api.tls_key = options.tls_key;
    }
    if !options.allowed_host.is_empty() {
        loaded.config.api.allowed_hosts = options.allowed_host;
    }
    loaded.config.validate()?;
    let listen = options.listen.unwrap_or(loaded.config.listen_addr()?);
    if !listen.ip().is_loopback() && loaded.config.api.tls_cert.is_none() {
        return Err("non-loopback listeners require TLS certificate and key".to_string());
    }
    match options.action {
        Some(DaemonAction::Start) => start_background_daemon(
            &data_dir,
            &loaded.path,
            context_file
                .clone()
                .unwrap_or_else(|| default_contexts_path(data_dir.clone())),
            listen,
            &loaded.config.api,
        ),
        Some(DaemonAction::Stop) => stop_background_daemon(&data_dir),
        Some(DaemonAction::Status) => {
            println!("{}", daemon_status(&data_dir)?);
            Ok(())
        }
        None => {
            let is_managed_child = std::env::var_os("KAKUNE_DAEMON_MANAGED").is_some()
                || std::env::var_os("KAKUNE_SERVICE_HOST").is_some();
            if !is_managed_child {
                let store = Store::open(data_dir.clone())?;
                configure_local_connection(
                    &store,
                    &context_file
                        .clone()
                        .unwrap_or_else(|| default_contexts_path(data_dir.clone())),
                    &loaded.config.api,
                    listen,
                )?;
            }
            serve_daemon(data_dir, loaded.config, listen).await
        }
    }
}

async fn serve_daemon(
    data_dir: PathBuf,
    config: CoreConfig,
    listen: SocketAddr,
) -> Result<(), String> {
    let store = Store::open(data_dir)?;
    let recovered = store.recover_incomplete_executions()?;
    if recovered > 0 {
        eprintln!("Recovered {recovered} incomplete execution(s) after a previous Core shutdown.");
    }
    let _ = store.ensure_bootstrap_token()?;
    let plugins = load_installed_plugin_registry(&store)?;
    for execution in store.queued_executions()? {
        let store = store.clone();
        let plugins = plugins.clone();
        tokio::task::spawn_blocking(move || {
            if let Err(error) =
                kakune_core::execute_prepared_workflow_with_plugins(&store, execution, &plugins)
            {
                eprintln!("Recovered execution stopped: {error}");
            }
        });
    }
    start_scheduler(store.clone());
    let app = api::router_with_plugins_and_config(store, plugins, config.api.clone());
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|error| format!("cannot listen on {listen}: {error}"))?;
    let scheme = if config.api.tls_cert.is_some() {
        "https"
    } else {
        "http"
    };
    println!("Kakune Core listening on {scheme}://{listen}");
    kakune_core::server::serve(listener, app, &config.api, shutdown_signal()).await
}

fn start_background_daemon(
    data_dir: &Path,
    config: &Path,
    contexts_file: PathBuf,
    listen: SocketAddr,
    api_config: &kakune_core::config::ApiConfig,
) -> Result<(), String> {
    let store = Store::open(data_dir.to_path_buf())?;
    if daemon_is_running(data_dir)? {
        return Err("Kakune daemon is already running".to_string());
    }
    configure_local_connection(&store, &contexts_file, api_config, listen)?;
    let pid_path = daemon_pid_path(data_dir);
    if pid_path.exists() {
        fs::remove_file(&pid_path)
            .map_err(|error| format!("cannot remove stale daemon PID file: {error}"))?;
    }
    let log_path = data_dir.join("daemon.log");
    let log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .map_err(|error| format!("cannot open daemon log {}: {error}", log_path.display()))?;
    let mut command = ProcessCommand::new(
        std::env::current_exe()
            .map_err(|error| format!("cannot locate Kakune executable: {error}"))?,
    );
    command
        .arg("daemon")
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--config")
        .arg(config)
        .arg("--listen")
        .arg(listen.to_string())
        .env("KAKUNE_DAEMON_MANAGED", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::from(log.try_clone().map_err(|error| {
            format!("cannot clone daemon log handle: {error}")
        })?))
        .stderr(Stdio::from(log));
    if let Some(cert) = &api_config.tls_cert {
        command.arg("--tls-cert").arg(cert);
    }
    if let Some(key) = &api_config.tls_key {
        command.arg("--tls-key").arg(key);
    }
    for host in &api_config.allowed_hosts {
        command.arg("--allowed-host").arg(host);
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }
    let child = command
        .spawn()
        .map_err(|error| format!("cannot start Kakune daemon: {error}"))?;
    fs::write(&pid_path, child.id().to_string())
        .map_err(|error| format!("cannot write daemon PID file: {error}"))?;
    println!("Kakune daemon started (pid {})", child.id());
    Ok(())
}

fn stop_background_daemon(data_dir: &Path) -> Result<(), String> {
    let pid =
        read_daemon_pid(data_dir)?.ok_or_else(|| "Kakune daemon is not running".to_string())?;
    if !process_is_running(pid)? {
        fs::remove_file(daemon_pid_path(data_dir))
            .map_err(|error| format!("cannot remove stale daemon PID file: {error}"))?;
        return Err("Kakune daemon is not running".to_string());
    }
    #[cfg(windows)]
    let status = ProcessCommand::new("taskkill")
        .args(["/PID", &pid.to_string(), "/T", "/F"])
        .status()
        .map_err(|error| format!("cannot stop Kakune daemon: {error}"))?;
    #[cfg(not(windows))]
    let status = ProcessCommand::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .map_err(|error| format!("cannot stop Kakune daemon: {error}"))?;
    if !status.success() {
        return Err("operating system rejected the daemon stop request".to_string());
    }
    fs::remove_file(daemon_pid_path(data_dir))
        .map_err(|error| format!("cannot remove daemon PID file: {error}"))?;
    println!("Kakune daemon stopped");
    Ok(())
}

fn daemon_status(data_dir: &Path) -> Result<String, String> {
    match read_daemon_pid(data_dir)? {
        Some(pid) if process_is_running(pid)? => Ok(format!("running\tpid={pid}")),
        Some(_) => Ok("stopped\tstale PID file".to_string()),
        None => Ok("stopped".to_string()),
    }
}

fn daemon_is_running(data_dir: &Path) -> Result<bool, String> {
    read_daemon_pid(data_dir)?.map_or(Ok(false), process_is_running)
}

fn daemon_pid_path(data_dir: &Path) -> PathBuf {
    data_dir.join("daemon.pid")
}

fn read_daemon_pid(data_dir: &Path) -> Result<Option<u32>, String> {
    let path = daemon_pid_path(data_dir);
    if !path.exists() {
        return Ok(None);
    }
    fs::read_to_string(&path)
        .map_err(|error| format!("cannot read daemon PID file: {error}"))?
        .trim()
        .parse::<u32>()
        .map(Some)
        .map_err(|_| format!("daemon PID file {} is invalid", path.display()))
}

fn process_is_running(pid: u32) -> Result<bool, String> {
    #[cfg(windows)]
    {
        let output = ProcessCommand::new("tasklist")
            .args(["/FI", &format!("PID eq {pid}"), "/NH"])
            .output()
            .map_err(|error| format!("cannot inspect daemon process: {error}"))?;
        Ok(String::from_utf8_lossy(&output.stdout).contains(&pid.to_string()))
    }
    #[cfg(not(windows))]
    {
        let status = ProcessCommand::new("kill")
            .args(["-0", &pid.to_string()])
            .status()
            .map_err(|error| format!("cannot inspect daemon process: {error}"))?;
        Ok(status.success())
    }
}

fn execute_context(command: ContextCommand) -> Result<(), String> {
    match command {
        ContextCommand::Add {
            id,
            name,
            endpoint,
            expected_core_id,
            color,
            credential_ref,
            contexts_file,
        } => {
            let path = contexts_path(contexts_file);
            let mut contexts = ContextFile::load(&path)?;
            if contexts.contexts.iter().any(|context| context.id == id) {
                return Err(format!("context {id} already exists"));
            }
            contexts.contexts.push(ConnectionContext {
                id: id.clone(),
                name,
                endpoint,
                expected_core_id,
                color,
                credential_ref,
            });
            if contexts.active_context_id.is_none() {
                contexts.active_context_id = Some(id);
            }
            contexts.save(&path)?;
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
            let path = contexts_path(contexts_file);
            let mut contexts = ContextFile::load(&path)?;
            if !contexts.contexts.iter().any(|context| context.id == id) {
                return Err(format!("context {id} was not found"));
            }
            contexts.active_context_id = Some(id);
            contexts.save(&path)?;
        }
        ContextCommand::Inspect { id, contexts_file } => {
            let contexts = ContextFile::load(&contexts_path(contexts_file))?;
            let context = contexts
                .contexts
                .iter()
                .find(|context| context.id == id)
                .ok_or_else(|| format!("context {id} was not found"))?;
            println!(
                "{}",
                serde_json::to_string_pretty(context).map_err(|error| error.to_string())?
            );
        }
        ContextCommand::Remove { id, contexts_file } => {
            let path = contexts_path(contexts_file);
            let mut contexts = ContextFile::load(&path)?;
            let original_len = contexts.contexts.len();
            contexts.contexts.retain(|context| context.id != id);
            if contexts.contexts.len() == original_len {
                return Err(format!("context {id} was not found"));
            }
            if contexts.active_context_id.as_deref() == Some(&id) {
                contexts.active_context_id =
                    contexts.contexts.first().map(|context| context.id.clone());
            }
            contexts.save(&path)?;
        }
        ContextCommand::Import {
            source,
            contexts_file,
        } => {
            let path = contexts_path(contexts_file);
            let mut imported = ContextFile::load(&source)?;
            imported.save(&path)?;
        }
        ContextCommand::Export {
            destination,
            contexts_file,
        } => {
            let mut contexts = ContextFile::load(&contexts_path(contexts_file))?;
            contexts.save(&destination)?;
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

fn local_credential_ref(core_id: &str) -> String {
    format!("keychain:kakune/core/{core_id}")
}

fn cli_credential_entry(reference: &str) -> Result<keyring::Entry, String> {
    let username = reference.strip_prefix("keychain:").unwrap_or(reference);
    keyring::Entry::new(CLI_KEYRING_SERVICE, username)
        .map_err(|error| format!("cannot access the OS credential manager: {error}"))
}

fn configure_local_connection(
    store: &Store,
    contexts_file: &Path,
    api_config: &kakune_core::config::ApiConfig,
    listen: SocketAddr,
) -> Result<(), String> {
    let token = store.ensure_bootstrap_token()?;
    let core_id = store.core_id()?;
    let reference = local_credential_ref(&core_id);
    let credential_available = match cli_credential_entry(&reference) {
        Ok(entry) => match token.as_deref() {
            Some(token) => match entry.set_password(token) {
                Ok(()) => true,
                Err(error) => {
                    eprintln!("Kakune could not save the local credential securely: {error}");
                    false
                }
            },
            None => entry.get_password().is_ok(),
        },
        Err(error) => {
            if token.is_none() {
                eprintln!("Kakune could not read the local credential: {error}");
            } else {
                eprintln!("Kakune could not save the local credential securely: {error}");
            }
            false
        }
    };
    let context_credential_ref = if credential_available || token.is_none() {
        Some(reference)
    } else {
        None
    };
    save_local_context(
        contexts_file,
        &core_id,
        &local_endpoint(api_config, listen),
        context_credential_ref,
    )?;

    if credential_available {
        println!(
            "Local CLI connection configured for {}.",
            local_endpoint(api_config, listen)
        );
    } else if let Some(token) = token {
        println!(
            "Initial API token (OS credential manager unavailable; set KAKUNE_TOKEN to use it): {token}"
        );
    } else {
        println!(
            "No local credential is available. Run `kakune auth recover` on this machine to reconnect."
        );
    }
    Ok(())
}

fn recover_local_auth(
    store: &Store,
    contexts_file: &Path,
    api_config: &kakune_core::config::ApiConfig,
    listen: SocketAddr,
) -> Result<(), String> {
    let created = store.create_auth_token(
        "Local recovery".to_string(),
        vec![kakune_core::AuthScope::Admin],
        None,
    )?;
    let core_id = store.core_id()?;
    let reference = local_credential_ref(&core_id);
    let credential_available = match cli_credential_entry(&reference) {
        Ok(entry) => match entry.set_password(&created.token) {
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
    };
    save_local_context(
        contexts_file,
        &core_id,
        &local_endpoint(api_config, listen),
        credential_available.then_some(reference),
    )?;
    let revoked = store.revoke_other_auth_tokens(&created.record.id)?;
    println!("Local API access recovered; revoked {revoked} previous active token(s).");
    println!(
        "Local CLI connection configured for {}.",
        local_endpoint(api_config, listen)
    );
    if !credential_available {
        println!(
            "Recovery token (OS credential manager unavailable; set KAKUNE_TOKEN): {}",
            created.token
        );
    }
    Ok(())
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
        () = terminate => {},
    }
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
    let context = if let Some(id) = selected {
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
        Command::Workflow { command } => match command {
            WorkflowCommand::Validate { workflow, .. } => {
                client
                    .request(
                        Method::POST,
                        &["workflows", "analyze"],
                        Some(json!({"source":read(workflow)?})),
                        None,
                    )
                    .await?
            }
            WorkflowCommand::List { .. } => {
                client
                    .request(Method::GET, &["workflows"], None, None)
                    .await?
            }
            WorkflowCommand::Enable { workflow, .. } => {
                let record = client.save_workflow(&read(workflow)?).await?;
                client
                    .request(
                        Method::POST,
                        &[
                            "workflows",
                            record["id"].as_str().ok_or("workflow ID is missing")?,
                            "enable",
                        ],
                        None,
                        None,
                    )
                    .await?
            }
            WorkflowCommand::Disable { id, .. } => {
                client
                    .request(Method::POST, &["workflows", id, "disable"], None, None)
                    .await?
            }
        },
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
        _ => {
            return Err(
                "this command operates on the local installation; omit --context".to_string(),
            );
        }
    };
    print_json(&value)
}

#[cfg(test)]
mod tests {
    use super::{ContextFile, local_endpoint, save_local_context, validate_pairing_endpoint};
    use kakune_core::config::ApiConfig;
    use std::fs;

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
