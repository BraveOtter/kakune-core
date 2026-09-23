use std::{
    ffi::OsString,
    path::PathBuf,
    process::{Command, Stdio},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use windows_service::{
    define_windows_service,
    service::{
        ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus,
        ServiceType,
    },
    service_control_handler::{self, ServiceControlHandlerResult},
    service_dispatcher,
};

use crate::{ServiceCommand, default_data_dir};

const SERVICE_NAME: &str = "KakuneCore";
static SERVICE_DATA_DIR: OnceLock<PathBuf> = OnceLock::new();

define_windows_service!(ffi_service_main, service_main);

pub fn execute(command: ServiceCommand) -> Result<(), String> {
    match command {
        ServiceCommand::Install { data_dir } => install(data_dir.unwrap_or_else(default_data_dir)),
        ServiceCommand::Uninstall => run_sc(["delete", SERVICE_NAME]),
        ServiceCommand::Start => run_sc(["start", SERVICE_NAME]),
        ServiceCommand::Stop => run_sc(["stop", SERVICE_NAME]),
        ServiceCommand::Status => run_sc(["query", SERVICE_NAME]),
        ServiceCommand::Run { data_dir } => run(data_dir.unwrap_or_else(default_data_dir)),
    }
}

fn install(data_dir: PathBuf) -> Result<(), String> {
    let executable = std::env::current_exe()
        .map_err(|error| format!("cannot locate Kakune executable: {error}"))?;
    let command_line = format!(
        "\"{}\" service run --data-dir \"{}\"",
        executable.display(),
        data_dir.display()
    );
    run_sc([
        "create",
        SERVICE_NAME,
        "binPath=",
        &command_line,
        "start=",
        "auto",
    ])?;
    let _ = run_sc([
        "description",
        SERVICE_NAME,
        "Kakune Core local automation runtime",
    ]);
    run_sc([
        "failure",
        SERVICE_NAME,
        "reset=",
        "86400",
        "actions=",
        "restart/5000/restart/5000/restart/30000",
    ])?;
    println!(
        "Installed {SERVICE_NAME}; it runs as LocalSystem. Data: {}",
        data_dir.display()
    );
    Ok(())
}

fn run_sc<'a>(arguments: impl IntoIterator<Item = &'a str>) -> Result<(), String> {
    let status = Command::new("sc.exe")
        .args(arguments)
        .status()
        .map_err(|error| format!("cannot invoke Service Control Manager: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err("Service Control Manager rejected the operation; run an elevated terminal".to_string())
    }
}

fn run(data_dir: PathBuf) -> Result<(), String> {
    SERVICE_DATA_DIR
        .set(data_dir)
        .map_err(|_| "Windows service dispatcher was already initialized".to_string())?;
    service_dispatcher::start(SERVICE_NAME, ffi_service_main)
        .map_err(|error| format!("cannot start Windows service dispatcher: {error}"))
}

fn service_main(_arguments: Vec<OsString>) {
    if let Err(error) = run_service() {
        eprintln!("Kakune Windows service failed: {error}");
    }
}

fn run_service() -> Result<(), String> {
    let stopping = Arc::new(AtomicBool::new(false));
    let stop_signal = stopping.clone();
    let status_handle = service_control_handler::register(SERVICE_NAME, move |event| match event {
        ServiceControl::Stop | ServiceControl::Shutdown => {
            stop_signal.store(true, Ordering::SeqCst);
            ServiceControlHandlerResult::NoError
        }
        _ => ServiceControlHandlerResult::NotImplemented,
    })
    .map_err(|error| format!("cannot register Windows service control handler: {error}"))?;
    status_handle
        .set_service_status(status(ServiceState::Running))
        .map_err(|error| format!("cannot report Windows service running: {error}"))?;

    let data_dir = SERVICE_DATA_DIR
        .get()
        .cloned()
        .unwrap_or_else(default_data_dir);
    let executable = std::env::current_exe()
        .map_err(|error| format!("cannot locate Kakune executable: {error}"))?;
    let mut child = Command::new(executable)
        .args(["daemon", "--data-dir"])
        .arg(data_dir)
        .env("KAKUNE_SERVICE_HOST", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| format!("cannot start Core service child: {error}"))?;

    while !stopping.load(Ordering::SeqCst) {
        if child
            .try_wait()
            .map_err(|error| format!("cannot wait for Core service child: {error}"))?
            .is_some()
        {
            return Err("Core service child exited unexpectedly".to_string());
        }
        thread::sleep(Duration::from_millis(100));
    }
    let _ = Command::new("taskkill.exe")
        .args(["/PID", &child.id().to_string(), "/T", "/F"])
        .status();
    let _ = child.wait();
    status_handle
        .set_service_status(status(ServiceState::Stopped))
        .map_err(|error| format!("cannot report Windows service stopped: {error}"))
}

fn status(state: ServiceState) -> ServiceStatus {
    ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: state,
        controls_accepted: ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN,
        exit_code: ServiceExitCode::Win32(0),
        checkpoint: 0,
        wait_hint: Duration::default(),
        process_id: None,
    }
}
