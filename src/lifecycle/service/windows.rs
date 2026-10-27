//! Windows SCM adapter. Read-only queries use safe SCM handles in a bounded
//! child; mutations use retained handles in authorized owned helpers.

use std::{path::Path, process::Command};

use super::{
    LifecycleError, MonotonicClock, NativeMutation, NativeServiceObservation, ServiceAction,
    ServiceDefinition, ServiceManagerAdapter, ServiceManagerKind,
    native::{require_success, run_owned_command},
    observe_service_core,
};
use crate::lifecycle::{Deadline, InstallationObservation};

#[derive(Clone, Copy, Debug, Default)]
pub struct WindowsServiceManager;

impl ServiceManagerAdapter for WindowsServiceManager {
    fn requires_owned_helper(&self) -> bool {
        true
    }
    fn observe(
        &self,
        manager: ServiceManagerKind,
        service_id: &str,
        deadline: Deadline,
        clock: &dyn MonotonicClock,
    ) -> Result<NativeServiceObservation, LifecycleError> {
        if manager != ServiceManagerKind::WindowsScm {
            return Err(LifecycleError::ManagerUnavailable(
                "Windows adapter received a non-SCM request".into(),
            ));
        }
        let query = run_owned_command(
            Command::new(std::env::current_exe()?).args(["--internal-service-query", service_id]),
            deadline,
            clock,
        )?;
        if !query.status_success && !query.stdout.as_bytes().starts_with(b"KAKUNE-HELPER-ERROR ") {
            return require_success(query, "SCM read-only query")
                .map(|_| NativeServiceObservation::unknown("SCM query helper failed"));
        }
        let mut observation = decode_query_output(query.status_success, query.stdout.as_bytes())?;
        if let Some(definition) = &observation.definition {
            let (core, unknown) = observe_service_core(&definition.data_dir, deadline, clock)?;
            observation.core = core;
            observation.core_unknown = unknown;
        }
        Ok(observation)
    }

    fn mutate(
        &self,
        _action: ServiceAction,
        _manager: ServiceManagerKind,
        _service_id: &str,
        _definition: Option<&ServiceDefinition>,
        _deadline: Deadline,
        _clock: &dyn MonotonicClock,
    ) -> Result<(), LifecycleError> {
        Err(LifecycleError::UnsafeOwnership(
            "SCM mutations require a journal-authorized helper".into(),
        ))
    }
}

fn decode_query_output(
    success: bool,
    output: &[u8],
) -> Result<NativeServiceObservation, LifecycleError> {
    if !success {
        return Err(match output {
            b"KAKUNE-HELPER-ERROR access_denied\n" => {
                LifecycleError::AccessDenied("SCM denied read-only query access".into())
            }
            b"KAKUNE-HELPER-ERROR conflict\n" => LifecycleError::ConflictingServiceDefinition(
                "SCM registration has an incompatible definition".into(),
            ),
            b"KAKUNE-HELPER-ERROR identity\n" => {
                LifecycleError::UnsafeOwnership("SCM definition could not be verified".into())
            }
            b"KAKUNE-HELPER-ERROR unconfirmed\n" => LifecycleError::UnresolvedServiceAttempt(
                "SCM query could not establish completion or deletion state; inspect the native manager and retained journal".into(),
            ),
            _ => LifecycleError::ManagerUnavailable(
                "SCM query helper failed; no absence or stopped state is inferred".into(),
            ),
        });
    }
    serde_json::from_slice(output).map_err(|_| {
        LifecycleError::ManagerUnavailable(
            "SCM query helper returned invalid structured data".into(),
        )
    })
}

/// Read-only SCM query for the internal CLI child only. The managing process
/// must call this through its bounded child boundary, never an in-process wait.
pub fn query_native_service(service_id: &str) -> Result<NativeServiceObservation, LifecycleError> {
    use windows_service::{
        service::{ServiceAccess, ServiceStartType, ServiceType},
        service_manager::{ServiceManager, ServiceManagerAccess},
    };

    if !service_id.eq_ignore_ascii_case("KakuneCore") {
        return Err(LifecycleError::UnsafeOwnership(
            "internal SCM query is restricted to KakuneCore".into(),
        ));
    }
    let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)
        .map_err(map_scm_error)?;
    let service = match manager.open_service(
        service_id,
        ServiceAccess::QUERY_CONFIG | ServiceAccess::QUERY_STATUS,
    ) {
        Ok(service) => service,
        Err(error) => match map_scm_error(error) {
            LifecycleError::ServiceNotInstalled => return Ok(NativeServiceObservation::absent()),
            error => return Err(error),
        },
    };
    let config = service.query_config().map_err(map_scm_error)?;
    if config.service_type != ServiceType::OWN_PROCESS {
        return Err(LifecycleError::ConflictingServiceDefinition(
            "SCM registration is not an own-process service".into(),
        ));
    }
    let binary = config
        .executable_path
        .to_str()
        .ok_or(LifecycleError::UnverifiedProcess)?;
    let account = config
        .account_name
        .as_deref()
        .and_then(|account| account.to_str())
        .ok_or(LifecycleError::UnverifiedProcess)?;
    // These labels are our internal representation, never localized sc.exe text.
    let definition = parse_service_config(
        service_id,
        &format!(
            "BINARY_PATH_NAME : {binary}\nSERVICE_START_NAME : {account}\nSTART_TYPE : {}",
            if config.start_type == ServiceStartType::AutoStart {
                "2 AUTO_START"
            } else {
                "incompatible"
            }
        ),
    )?;
    let status = service.query_status().map_err(map_scm_error)?;
    let manager_state = native_manager_state(status.current_state, status.exit_code);
    let diagnostic_location = format!(
        "Windows Event Viewer: Kakune Core service; protected host-error logs, when available: {}",
        definition.data_dir.join(".kakune").display()
    );
    Ok(NativeServiceObservation {
        installed: InstallationObservation::Installed,
        manager_state,
        account: Some(definition.account.clone()),
        definition: Some(definition),
        boot_enabled: true,
        core: None,
        core_unknown: None,
        diagnostic_location: Some(diagnostic_location),
        pending_operation: matches!(
            manager_state,
            crate::lifecycle::NativeManagerState::Starting
                | crate::lifecycle::NativeManagerState::Stopping
        ),
    })
}

fn native_manager_state(
    state: windows_service::service::ServiceState,
    exit: windows_service::service::ServiceExitCode,
) -> crate::lifecycle::NativeManagerState {
    use crate::lifecycle::NativeManagerState;
    use windows_service::service::{ServiceExitCode, ServiceState};
    match state {
        // ERROR_SERVICE_NEVER_STARTED is normal immediately after stopped install.
        ServiceState::Stopped if matches!(exit, ServiceExitCode::Win32(0 | 1077)) => {
            NativeManagerState::Stopped
        }
        ServiceState::Stopped => NativeManagerState::Failed,
        ServiceState::StartPending => NativeManagerState::Starting,
        ServiceState::Running => NativeManagerState::Running,
        ServiceState::StopPending => NativeManagerState::Stopping,
        _ => NativeManagerState::Unknown,
    }
}

pub(super) fn execute_step(
    step: NativeMutation,
    definition: &ServiceDefinition,
    deadline: Deadline,
    clock: &dyn MonotonicClock,
) -> Result<(), LifecycleError> {
    use std::ffi::OsString;
    use windows_service::{
        service::{
            ServiceAccess, ServiceAction as FailureAction, ServiceActionType, ServiceErrorControl,
            ServiceFailureActions, ServiceFailureResetPeriod, ServiceInfo, ServiceStartType,
            ServiceState, ServiceType,
        },
        service_manager::{ServiceManager, ServiceManagerAccess},
    };
    super::ensure_remaining(deadline, clock, "SCM single mutation")?;
    let service_id = definition.service_id.as_str();
    let access = ServiceManagerAccess::CONNECT
        | if step == NativeMutation::Register {
            ServiceManagerAccess::CREATE_SERVICE
        } else {
            ServiceManagerAccess::empty()
        };
    let manager = ServiceManager::local_computer(None::<&str>, access).map_err(map_scm_error)?;
    if step == NativeMutation::Register {
        if definition.account != "LocalSystem" {
            return Err(LifecycleError::ConflictingServiceDefinition(
                "new SCM registrations use LocalSystem; existing compatible accounts are preserved"
                    .into(),
            ));
        }
        let info = ServiceInfo {
            name: service_id.into(),
            display_name: "Kakune Core".into(),
            service_type: ServiceType::OWN_PROCESS,
            start_type: ServiceStartType::AutoStart,
            error_control: ServiceErrorControl::Normal,
            executable_path: definition.executable.clone(),
            launch_arguments: definition.arguments.iter().map(OsString::from).collect(),
            dependencies: Vec::new(),
            account_name: None,
            account_password: None,
        };
        manager
            .create_service(&info, ServiceAccess::QUERY_STATUS)
            .map_err(map_scm_error)?;
        return Ok(());
    }
    let access = mutation_access(step)?;
    let service = manager
        .open_service(service_id, access)
        .map_err(map_scm_error)?;
    let config = service.query_config().map_err(map_scm_error)?;
    let binary = config
        .executable_path
        .to_str()
        .ok_or(LifecycleError::UnverifiedProcess)?;
    let account = config
        .account_name
        .as_deref()
        .and_then(|account| account.to_str())
        .ok_or(LifecycleError::UnverifiedProcess)?;
    let text = format!(
        "BINARY_PATH_NAME : {binary}\nSTART_TYPE : {}\nSERVICE_START_NAME : {account}",
        if config.start_type == ServiceStartType::AutoStart {
            "2 AUTO_START"
        } else {
            "incompatible"
        }
    );
    let actual = parse_service_config(service_id, &text)?;
    if !definition.equivalent_to(&actual) || config.service_type != ServiceType::OWN_PROCESS {
        return Err(LifecycleError::ConflictingServiceDefinition(
            "retained SCM handle belongs to an incompatible definition".into(),
        ));
    }
    super::ensure_remaining(deadline, clock, "SCM mutation revalidation")?;
    match step {
        NativeMutation::Description => {
            service.set_description("Kakune Core local automation runtime")
        }
        NativeMutation::FailurePolicy => service.update_failure_actions(ServiceFailureActions {
            reset_period: ServiceFailureResetPeriod::After(std::time::Duration::from_secs(86400)),
            reboot_msg: Some(OsString::new()),
            command: Some(OsString::new()),
            actions: Some(
                [5, 5, 30]
                    .into_iter()
                    .map(|seconds| FailureAction {
                        action_type: ServiceActionType::Restart,
                        delay: std::time::Duration::from_secs(seconds),
                    })
                    .collect(),
            ),
        }),
        NativeMutation::Start => service.start::<&str>(&[]),
        NativeMutation::Stop => {
            if service.query_status().map_err(map_scm_error)?.current_state == ServiceState::Stopped
            {
                return Ok(());
            }
            service.stop().map(|_| ())
        }
        NativeMutation::Delete => {
            if service.query_status().map_err(map_scm_error)?.current_state != ServiceState::Stopped
            {
                return Err(LifecycleError::ServiceStateUnconfirmed);
            }
            service.delete()
        }
        _ => {
            return Err(LifecycleError::UnsafeOwnership(
                "unsupported SCM mutation step".into(),
            ));
        }
    }
    .map_err(map_scm_error)
}

fn map_scm_error(error: windows_service::Error) -> LifecycleError {
    let code = match &error {
        windows_service::Error::Winapi(error) => error.raw_os_error(),
        _ => None,
    };
    match code {
        Some(5) => {
            LifecycleError::AccessDenied("SCM denied the requested administrative access".into())
        }
        Some(1060) => LifecycleError::ServiceNotInstalled,
        Some(1072) => LifecycleError::UnresolvedServiceAttempt(
            "SCM registration is marked for deletion; it is not confirmed absent".into(),
        ),
        Some(1073) => LifecycleError::ConflictingServiceDefinition(
            "SCM registration appeared before create".into(),
        ),
        _ => LifecycleError::ManagerUnavailable(format!(
            "SCM operation failed (Windows error {code:?}): {error}"
        )),
    }
}

fn mutation_access(
    step: NativeMutation,
) -> Result<windows_service::service::ServiceAccess, LifecycleError> {
    use windows_service::service::ServiceAccess;
    let mutation = match step {
        NativeMutation::Description => ServiceAccess::CHANGE_CONFIG,
        // SCM requires SERVICE_START on the retained handle when restart
        // actions are configured. Granting this access does not start Core.
        NativeMutation::FailurePolicy => ServiceAccess::CHANGE_CONFIG | ServiceAccess::START,
        NativeMutation::Start => ServiceAccess::START,
        NativeMutation::Stop => ServiceAccess::STOP,
        NativeMutation::Delete => ServiceAccess::DELETE,
        _ => {
            return Err(LifecycleError::UnsafeOwnership(
                "unsupported SCM mutation step".into(),
            ));
        }
    };
    Ok(ServiceAccess::QUERY_CONFIG | ServiceAccess::QUERY_STATUS | mutation)
}

fn parse_service_config(
    service_id: &str,
    config: &str,
) -> Result<ServiceDefinition, LifecycleError> {
    let binary = config
        .lines()
        .find_map(|line| value_after_label(line, "BINARY_PATH_NAME"))
        .ok_or_else(|| {
            LifecycleError::UnsafeOwnership("SCM service has no verifiable binary path".into())
        })?;
    let tokens = parse_windows_command_line(binary)?;
    let (executable, arguments) = tokens.split_first().ok_or_else(|| {
        LifecycleError::UnsafeOwnership("SCM service binary path is empty".into())
    })?;
    let data_dir = arguments
        .windows(2)
        .find(|pair| pair[0] == "--data-dir")
        .map(|pair| pair[1].as_str())
        .ok_or_else(|| {
            LifecycleError::UnsafeOwnership("SCM service has no absolute data path".into())
        })?;
    let account_raw = config
        .lines()
        .find_map(|line| value_after_label(line, "SERVICE_START_NAME"))
        .ok_or_else(|| LifecycleError::UnsafeOwnership("SCM service account is unknown".into()))?;
    let account = if account_raw.eq_ignore_ascii_case("LocalSystem")
        || account_raw.eq_ignore_ascii_case("NT AUTHORITY\\SYSTEM")
    {
        "LocalSystem"
    } else {
        account_raw
    };
    let definition = ServiceDefinition::new(
        ServiceManagerKind::WindowsScm,
        service_id,
        Path::new(executable),
        arguments.to_vec(),
        account,
        Path::new(data_dir),
    )?;
    let start_type = config
        .lines()
        .find_map(|line| value_after_label(line, "START_TYPE"))
        .unwrap_or_default();
    if !start_type.contains("AUTO_START") {
        return Err(LifecycleError::ConflictingServiceDefinition(
            "SCM service is not configured for automatic startup".into(),
        ));
    }
    Ok(definition)
}

fn value_after_label<'a>(line: &'a str, label: &str) -> Option<&'a str> {
    let (name, value) = line.split_once(':')?;
    name.trim()
        .eq_ignore_ascii_case(label)
        .then_some(value.trim())
}

/// Parses the Windows CRT command-line quoting rules without invoking shell
/// expansion. SCM `ImagePath` is treated as structured arguments, not a shell.
fn parse_windows_command_line(input: &str) -> Result<Vec<String>, LifecycleError> {
    let mut result = Vec::new();
    let mut characters = input.chars().peekable();
    while characters.peek().is_some() {
        while characters
            .peek()
            .is_some_and(|character| character.is_whitespace())
        {
            characters.next();
        }
        if characters.peek().is_none() {
            break;
        }
        let mut argument = String::new();
        let mut quoted = false;
        while let Some(character) = characters.next() {
            match character {
                '\\' => {
                    let mut slashes = 1;
                    while characters.peek() == Some(&'\\') {
                        characters.next();
                        slashes += 1;
                    }
                    if characters.peek() == Some(&'"') {
                        characters.next();
                        argument.push_str(&"\\".repeat(slashes / 2));
                        if slashes % 2 == 0 {
                            quoted = !quoted;
                        } else {
                            argument.push('"');
                        }
                    } else {
                        argument.push_str(&"\\".repeat(slashes));
                    }
                }
                '"' => quoted = !quoted,
                character if character.is_whitespace() && !quoted => break,
                character => argument.push(character),
            }
        }
        if quoted {
            return Err(LifecycleError::UnsafeOwnership(
                "SCM binary path contains an unterminated quote".into(),
            ));
        }
        result.push(argument);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::service::parse_manager_state;
    use crate::lifecycle::service::render_service_definition;

    #[test]
    fn scm_native_query_maps_never_started_without_localized_labels() {
        use crate::lifecycle::NativeManagerState;
        use windows_service::service::{ServiceExitCode, ServiceState};
        for (state, exit, expected) in [
            (
                ServiceState::Stopped,
                ServiceExitCode::Win32(1077),
                NativeManagerState::Stopped,
            ),
            (
                ServiceState::Stopped,
                ServiceExitCode::Win32(0),
                NativeManagerState::Stopped,
            ),
            (
                ServiceState::Stopped,
                ServiceExitCode::Win32(5),
                NativeManagerState::Failed,
            ),
            (
                ServiceState::Stopped,
                ServiceExitCode::ServiceSpecific(42),
                NativeManagerState::Failed,
            ),
            (
                ServiceState::StartPending,
                ServiceExitCode::Win32(0),
                NativeManagerState::Starting,
            ),
            (
                ServiceState::StopPending,
                ServiceExitCode::Win32(0),
                NativeManagerState::Stopping,
            ),
            (
                ServiceState::Running,
                ServiceExitCode::Win32(0),
                NativeManagerState::Running,
            ),
            (
                ServiceState::Paused,
                ServiceExitCode::Win32(0),
                NativeManagerState::Unknown,
            ),
        ] {
            assert_eq!(native_manager_state(state, exit), expected);
        }
    }

    #[test]
    fn scm_query_protocol_preserves_errors_without_inventing_absence() {
        assert!(matches!(
            decode_query_output(false, b"KAKUNE-HELPER-ERROR access_denied\n"),
            Err(LifecycleError::AccessDenied(_))
        ));
        assert!(
            decode_query_output(true, b"NOMBRE_DE_SERVICIO: KakuneCore\nESTADO: 1 STOPPED")
                .is_err()
        );
        let absent = NativeServiceObservation::absent();
        let encoded = serde_json::to_vec(&absent).unwrap();
        assert_eq!(decode_query_output(true, &encoded).unwrap(), absent);
    }

    #[test]
    fn scm_failure_policy_requests_restart_configuration_rights_without_starting_a_service() {
        use windows_service::service::ServiceAccess;
        let access = mutation_access(NativeMutation::FailurePolicy).unwrap();
        assert!(access.contains(ServiceAccess::START | ServiceAccess::CHANGE_CONFIG));
        assert!(
            !mutation_access(NativeMutation::Description)
                .unwrap()
                .contains(ServiceAccess::START)
        );
        assert!(
            !mutation_access(NativeMutation::Stop)
                .unwrap()
                .contains(ServiceAccess::DELETE)
        );
    }

    #[test]
    fn scm_config_parser_round_trips_spaces_and_non_ascii_arguments() {
        let executable = Path::new(r"C:\Program Files\Kakune 雪\kakune.exe");
        let data = Path::new(r"C:\ProgramData\Kakune Data");
        let definition = ServiceDefinition::new(
            ServiceManagerKind::WindowsScm,
            "KakuneCore",
            executable,
            vec![
                "service".into(),
                "run".into(),
                "--data-dir".into(),
                data.display().to_string(),
            ],
            "LocalSystem",
            data,
        )
        .unwrap();
        let rendered = render_service_definition(&definition).unwrap();
        let config = format!(
            "BINARY_PATH_NAME : {rendered}\nSTART_TYPE : 2 AUTO_START\nSERVICE_START_NAME : LocalSystem"
        );
        let parsed = parse_service_config("KakuneCore", &config).unwrap();
        assert!(definition.equivalent_to(&parsed));
    }

    #[test]
    fn scm_query_unknown_and_pending_states_remain_distinct() {
        assert_eq!(
            parse_manager_state(
                ServiceManagerKind::WindowsScm,
                "SERVICE_NAME: KakuneCore\nSTATE: 2 START_PENDING"
            )
            .unwrap(),
            crate::lifecycle::NativeManagerState::Starting
        );
        assert_eq!(
            parse_manager_state(
                ServiceManagerKind::WindowsScm,
                "SERVICE_NAME: KakuneCore\nSTATE: 7 PAUSED"
            )
            .unwrap(),
            crate::lifecycle::NativeManagerState::Unknown
        );
    }
}
