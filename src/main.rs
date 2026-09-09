use std::{net::SocketAddr, path::PathBuf, process::ExitCode};

use clap::{Parser, Subcommand};
use kakune_core::{Store, WorkflowDocument, api, default_data_dir, run_workflow};

#[derive(Debug, Parser)]
#[command(name = "kakune", version, about = "Kakune local automation runtime")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Creates the Kakune data directory and SQLite database.
    Init {
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    /// Runs the Core HTTP API in the foreground.
    Daemon {
        #[arg(long, default_value = "127.0.0.1:8787")]
        listen: SocketAddr,
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    /// Validates a workflow YAML file without saving or executing it.
    Workflow {
        #[command(subcommand)]
        command: WorkflowCommand,
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

#[derive(Debug, Subcommand)]
enum WorkflowCommand {
    Validate {
        workflow: PathBuf,
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
        name: String,
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
    match cli.command {
        Command::Init { data_dir } => {
            let store = Store::open(data_dir.unwrap_or_else(default_data_dir))?;
            println!(
                "Initialized Kakune data directory at {}",
                store.data_dir().display()
            );
        }
        Command::Daemon { listen, data_dir } => {
            let store = Store::open(data_dir.unwrap_or_else(default_data_dir))?;
            let app = api::router(store);
            let listener = tokio::net::TcpListener::bind(listen)
                .await
                .map_err(|error| format!("cannot listen on {listen}: {error}"))?;
            println!("Kakune Core listening on http://{listen}");
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown_signal())
                .await
                .map_err(|error| format!("Core server stopped unexpectedly: {error}"))?;
        }
        Command::Workflow { command } => match command {
            WorkflowCommand::Validate { workflow } => {
                let source = std::fs::read_to_string(&workflow)
                    .map_err(|error| format!("cannot read {}: {error}", workflow.display()))?;
                let document = WorkflowDocument::parse(&source)?;
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
                let document = WorkflowDocument::parse(&source)?;
                let store = Store::open(data_dir.unwrap_or_else(default_data_dir))?;
                let record = store.upsert_workflow(&document, &source, "enabled")?;
                println!("Enabled {}", record.name);
            }
            WorkflowCommand::Disable { name, data_dir } => {
                let store = Store::open(data_dir.unwrap_or_else(default_data_dir))?;
                store.set_workflow_status(&name, "disabled")?;
                println!("Disabled {name}");
            }
        },
        Command::Run { workflow, data_dir } => {
            let source = std::fs::read_to_string(&workflow)
                .map_err(|error| format!("cannot read {}: {error}", workflow.display()))?;
            let document = WorkflowDocument::parse(&source)?;
            let store = Store::open(data_dir.unwrap_or_else(default_data_dir))?;
            store.upsert_workflow(&document, &source, "enabled")?;
            let execution = run_workflow(&store, &document)?;
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
            for step in store.list_step_runs(&execution_id)? {
                println!(
                    "{}\t{}\t{}",
                    step.step_id,
                    step.status,
                    step.message.unwrap_or_default()
                );
            }
        }
    }
    Ok(())
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
