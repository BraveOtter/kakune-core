use std::{
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use tokio::sync::oneshot;
use windows_service::{
    service::{
        ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus,
        ServiceType,
    },
    service_control_handler::{self, ServiceControlHandlerResult},
    service_dispatcher,
};

use crate::{ServiceHostState, default_data_dir};

const SERVICE_NAME: &str = "KakuneCore";
static SERVICE_DATA_DIR: OnceLock<PathBuf> = OnceLock::new();

pub fn run(data_dir: PathBuf) -> Result<(), String> {
    SERVICE_DATA_DIR
        .set(data_dir)
        .map_err(|_| "Windows service dispatcher was already initialized".to_owned())?;
    service_dispatcher::start(SERVICE_NAME, service_main_callback)
        .map_err(|error| format!("cannot start Windows service dispatcher: {error}"))
}

// The dispatcher signature exposes raw SCM arguments. This callback deliberately
// ignores them; the validated data path was selected by the CLI before dispatch.
extern "system" fn service_main_callback(_argument_count: u32, _arguments: *mut *mut u16) {
    service_main();
}

fn service_main() {
    let data_dir = SERVICE_DATA_DIR
        .get()
        .cloned()
        .unwrap_or_else(default_data_dir);
    let (stop_sender, stop_receiver) = oneshot::channel();
    let stop_sender = Arc::new(Mutex::new(Some(stop_sender)));
    let handler_sender = stop_sender.clone();
    let status_handle =
        match service_control_handler::register(SERVICE_NAME, move |event| match event {
            ServiceControl::Stop | ServiceControl::Shutdown => {
                if let Ok(mut sender) = handler_sender.lock()
                    && let Some(sender) = sender.take()
                {
                    let _ = sender.send(());
                }
                ServiceControlHandlerResult::NoError
            }
            ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
            _ => ServiceControlHandlerResult::NotImplemented,
        }) {
            Ok(handle) => handle,
            Err(_) => return,
        };

    if status_handle
        .set_service_status(service_status(ServiceState::StartPending, 0))
        .is_err()
    {
        return;
    }
    let reporter_handle = status_handle;
    let reporter = Arc::new(move |state| {
        let (native_state, exit_code) = match state {
            ServiceHostState::Running => (ServiceState::Running, 0),
            ServiceHostState::Stopping => (ServiceState::StopPending, 0),
            ServiceHostState::Stopped => (ServiceState::Stopped, 0),
        };
        reporter_handle
            .set_service_status(service_status(native_state, exit_code))
            .map_err(|error| format!("cannot update SCM service state: {error}"))
    });
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => {
            let _ = status_handle.set_service_status(service_status(ServiceState::Stopped, 1));
            return;
        }
    };
    let result = runtime.block_on(crate::serve_system_service_with_host(
        data_dir.clone(),
        Some(stop_receiver),
        Some(reporter),
    ));
    if let Err(error) = &result {
        record_failure_diagnostic(&data_dir, error);
    }
    let exit_code = if result.is_ok() { 0 } else { 1 };
    let _ = status_handle.set_service_status(service_status(ServiceState::Stopped, exit_code));
}

fn failure_diagnostic_message(error: &str) -> String {
    crate::redact_lifecycle_diagnostic(error)
        .chars()
        .take(1800)
        .collect()
}

fn record_failure_diagnostic(data_dir: &std::path::Path, error: &str) {
    let message = failure_diagnostic_message(error);
    use kakune_core::lifecycle::{
        Deadline, SystemMonotonicClock,
        process::{ProcessEvidenceProvider, SystemProcessEvidence},
    };
    let clock = SystemMonotonicClock::default();
    let Ok(Some(evidence)) = SystemProcessEvidence::default().observe(
        std::process::id(),
        Deadline::after(&clock, Duration::from_secs(1)),
        &clock,
    ) else {
        return;
    };
    let path = data_dir
        .join(".kakune")
        .join(format!("service-error-{}.log", uuid::Uuid::new_v4()));
    let _ = kakune_core::lifecycle::control::write_service_diagnostic(
        &path,
        message.as_bytes(),
        &evidence.owner,
    );
}

fn service_status(state: ServiceState, exit_code: u32) -> ServiceStatus {
    let (controls_accepted, checkpoint, wait_hint) = match state {
        ServiceState::Running => (
            ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN,
            0,
            Duration::ZERO,
        ),
        ServiceState::StartPending | ServiceState::StopPending => {
            (ServiceControlAccept::empty(), 1, Duration::from_secs(30))
        }
        _ => (ServiceControlAccept::empty(), 0, Duration::ZERO),
    };
    ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: state,
        controls_accepted,
        exit_code: ServiceExitCode::Win32(exit_code),
        checkpoint,
        wait_hint,
        process_id: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_failure_diagnostics_are_sanitized_and_bounded() {
        let error = format!(
            "Authorization: Bearer fixture-secret\n{}",
            "雪".repeat(9000)
        );
        let message = failure_diagnostic_message(&error);
        assert!(!message.contains("fixture-secret"));
        assert!(message.len() <= 8192);
    }

    #[test]
    fn pending_and_terminal_states_never_advertise_running_controls() {
        for state in [
            ServiceState::StartPending,
            ServiceState::StopPending,
            ServiceState::Stopped,
        ] {
            let status = service_status(state, 0);
            assert_eq!(status.current_state, state);
            assert!(status.controls_accepted.is_empty());
            assert_eq!(status.process_id, None);
        }
        let running = service_status(ServiceState::Running, 0);
        assert!(
            running
                .controls_accepted
                .contains(ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN)
        );
        assert_eq!(running.wait_hint, Duration::ZERO);
    }

    #[test]
    fn native_host_failure_preserves_a_nonzero_terminal_exit_code() {
        assert_eq!(
            service_status(ServiceState::Stopped, 1).exit_code,
            ServiceExitCode::Win32(1)
        );
        assert_eq!(
            service_status(ServiceState::StartPending, 0).wait_hint,
            Duration::from_secs(30)
        );
    }
}
