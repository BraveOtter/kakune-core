use std::{
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Command as ProcessCommand, ExitCode, Stdio},
};

use clap::{Args, Parser, Subcommand};

#[cfg(windows)]
mod windows_service_host;
use kakune_core::{
    ConnectionContext, ContextFile, CoreConfig, PluginRegistry, ProviderAuth, ProviderProfile,
    ProviderProfileDiagnostic, ProviderProfileStatus, ProviderProfileUpsert, ProviderType,
    RetentionPolicy, Store, analyze_workflow_with_plugins, api, default_contexts_path,
    default_data_dir, load_installed_plugin_registry,
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
    if !cli.standalone && (cli.context.is_some() || matches!(&cli.command, Command::Run { .. })) {
        return execute_remote(&cli).await;
    }
    match cli.command {
        Command::Init { data_dir, config } => {
            let data_dir = data_dir.unwrap_or_else(default_data_dir);
            let loaded = CoreConfig::load_or_create(&data_dir, config)?;
            let store = Store::open(data_dir)?;
            println!(
                "Initialized Kakune data directory at {}",
                store.data_dir().display()
            );
            println!("Configuration: {}", loaded.path.display());
            if let Some(token) = store.ensure_bootstrap_token()? {
                println!("Initial API token (store it securely; it is shown only once): {token}");
            }
        }
        Command::Daemon(options) => execute_daemon(options).await?,
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

async fn execute_daemon(options: DaemonArgs) -> Result<(), String> {
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
        Some(DaemonAction::Start) => {
            start_background_daemon(&data_dir, &loaded.path, listen, &loaded.config.api)
        }
        Some(DaemonAction::Stop) => stop_background_daemon(&data_dir),
        Some(DaemonAction::Status) => {
            println!("{}", daemon_status(&data_dir)?);
            Ok(())
        }
        None => serve_daemon(data_dir, loaded.config, listen).await,
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
    if let Some(token) = store.ensure_bootstrap_token()? {
        println!("Initial API token (store it securely; it is shown only once): {token}");
    }
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
    listen: SocketAddr,
    api_config: &kakune_core::config::ApiConfig,
) -> Result<(), String> {
    let store = Store::open(data_dir.to_path_buf())?;
    if daemon_is_running(data_dir)? {
        return Err("Kakune daemon is already running".to_string());
    }
    let pid_path = daemon_pid_path(data_dir);
    if pid_path.exists() {
        fs::remove_file(&pid_path)
            .map_err(|error| format!("cannot remove stale daemon PID file: {error}"))?;
    }
    if let Some(token) = store.ensure_bootstrap_token()? {
        println!("Initial API token (store it securely; it is shown only once): {token}");
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

fn contexts_path(explicit: Option<PathBuf>) -> PathBuf {
    explicit.unwrap_or_else(|| default_contexts_path(default_data_dir()))
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
    let contexts = ContextFile::load(
        &cli.context_file
            .clone()
            .unwrap_or_else(|| default_contexts_path(default_data_dir())),
    )?;
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
        Some(reference) => keyring::Entry::new(
            "dev.kakune.cli",
            reference.strip_prefix("keychain:").unwrap_or(reference),
        )
        .map_err(|error| error.to_string())?
        .get_password()
        .map_err(|_| "context token is unavailable in the credential store".to_string())?,
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
