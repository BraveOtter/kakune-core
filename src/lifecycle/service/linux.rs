//! Root systemd adapter. All `systemctl` calls run as retained child processes
//! under the operation deadline; accepted jobs are reconciled from manager state.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use super::{
    BootPolicy, LifecycleError, MonotonicClock, NativeMutation, NativeServiceObservation,
    ServiceAction, ServiceDefinition, ServiceManagerAdapter, ServiceManagerKind,
    native::{
        remove_native_definition, require_success, run_owned_command, write_native_definition,
    },
    observe_service_core, parse_manager_state, parse_systemd_enabled, render_service_definition,
};
use crate::lifecycle::{Deadline, InstallationObservation};

const SYSTEMD_UNIT_DIRECTORY: &str = "/etc/systemd/system";

#[derive(Clone, Copy, Debug, Default)]
pub struct LinuxServiceManager;

impl ServiceManagerAdapter for LinuxServiceManager {
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
        if manager != ServiceManagerKind::LinuxSystemd {
            return Err(LifecycleError::ManagerUnavailable(
                "Linux adapter received a non-systemd request".into(),
            ));
        }
        let unit = unit_path(service_id)?;
        let query = run_owned_command(
            systemctl_command()?.args([
                "--no-pager",
                "show",
                "--property=LoadState,ActiveState,SubState,MainPID,Job",
                service_id,
            ]),
            deadline,
            clock,
        )?;
        let output = query.text();
        if !query.status_success && output.to_ascii_lowercase().contains("permission denied") {
            return Err(LifecycleError::AccessDenied(output.trim().to_owned()));
        }
        if !query.status_success && output.trim().is_empty() {
            return Err(LifecycleError::ManagerUnavailable(
                "systemctl show failed without a diagnostic".into(),
            ));
        }
        let manager_state = parse_manager_state(ServiceManagerKind::LinuxSystemd, &output)?;
        let load_state = property(&output, "LoadState").unwrap_or_default();
        let file = read_unit_file(&unit)?;
        if file.is_none() && load_state == "not-found" {
            return Ok(NativeServiceObservation::absent());
        }
        let Some(contents) = file else {
            return Ok(NativeServiceObservation::unknown(format!(
                "systemd reports {service_id} loaded without the Kakune-owned unit file"
            )));
        };
        let definition = parse_unit_definition(service_id, &contents)?;
        let enabled = run_owned_command(
            systemctl_command()?.args(["is-enabled", service_id]),
            deadline,
            clock,
        )?;
        let boot_enabled = parse_systemd_enabled(&enabled.text(), enabled.status_success)?;
        let job = property(&output, "Job").unwrap_or_default();
        let pending_operation = !job.is_empty() && job != "0" && job != "-";
        let (core, core_unknown) = observe_service_core(&definition.data_dir, deadline, clock)?;
        Ok(NativeServiceObservation {
            installed: InstallationObservation::Installed,
            manager_state,
            definition: Some(definition.clone()),
            account: Some(definition.account.clone()),
            boot_enabled,
            core,
            core_unknown,
            diagnostic_location: Some(format!("systemd journal: {service_id}")),
            pending_operation,
        })
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
            "systemd mutations require a journal-authorized helper".into(),
        ))
    }
}

pub(super) fn execute_step(
    step: NativeMutation,
    definition: &ServiceDefinition,
    deadline: Deadline,
    clock: &dyn MonotonicClock,
) -> Result<(), LifecycleError> {
    let unit = unit_path(&definition.service_id)?;
    let mut command = systemctl_command()?;
    match step {
        NativeMutation::Publish => {
            return write_native_definition(
                &unit,
                render_service_definition(definition)?.as_bytes(),
            );
        }
        NativeMutation::Delete => {
            return remove_native_definition(
                &unit,
                render_service_definition(definition)?.as_bytes(),
            )
            .map(|_| ());
        }
        NativeMutation::Reload => {
            command.arg("daemon-reload");
        }
        NativeMutation::Enable => {
            command.args(["enable", &definition.service_id]);
        }
        NativeMutation::Disable => {
            command.args(["disable", &definition.service_id]);
        }
        NativeMutation::Start => {
            command.args(["start", "--no-block", &definition.service_id]);
        }
        NativeMutation::Stop => {
            command.args(["stop", "--no-block", &definition.service_id]);
        }
        _ => {
            return Err(LifecycleError::UnsafeOwnership(
                "unsupported systemd mutation step".into(),
            ));
        }
    }
    require_success(
        run_owned_command(&mut command, deadline, clock)?,
        "authorized systemd mutation",
    )
    .map(|_| ())
}

fn systemctl_command() -> Result<Command, LifecycleError> {
    for candidate in ["/usr/bin/systemctl", "/bin/systemctl"] {
        match fs::metadata(candidate) {
            Ok(_) => return Ok(Command::new(candidate)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Err(LifecycleError::ManagerUnavailable(
        "systemctl is unavailable in the system binary directories".into(),
    ))
}

fn unit_path(service_id: &str) -> Result<PathBuf, LifecycleError> {
    if service_id.is_empty()
        || !service_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(LifecycleError::UnsafeOwnership(
            "invalid systemd unit identifier".into(),
        ));
    }
    Ok(Path::new(SYSTEMD_UNIT_DIRECTORY).join(service_id))
}

fn read_unit_file(path: &Path) -> Result<Option<String>, LifecycleError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(LifecycleError::UnsafeOwnership(
            "systemd unit file is redirected or not a regular file".into(),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.uid() != 0 || metadata.permissions().mode() & 0o022 != 0 {
            return Err(LifecycleError::InsecureProtection(
                "systemd unit must be root-owned and not writable by untrusted accounts".into(),
            ));
        }
    }
    Ok(Some(super::native::read_native_definition(path)?))
}

fn property<'a>(output: &'a str, key: &str) -> Option<&'a str> {
    output.lines().find_map(|line| {
        let (name, value) = line.split_once('=')?;
        (name == key).then_some(value)
    })
}

fn parse_unit_definition(
    service_id: &str,
    contents: &str,
) -> Result<ServiceDefinition, LifecycleError> {
    for required in [
        "Type=notify",
        "NotifyAccess=main",
        "Restart=on-failure",
        "KillMode=mixed",
        "SendSIGKILL=no",
        "TimeoutStopSec=30",
        "WantedBy=multi-user.target",
    ] {
        if !contents.lines().any(|line| line == required) {
            return Err(LifecycleError::ConflictingServiceDefinition(format!(
                "systemd unit is missing required setting {required}"
            )));
        }
    }
    let account = contents
        .lines()
        .find_map(|line| line.strip_prefix("User="))
        .ok_or_else(|| {
            LifecycleError::ConflictingServiceDefinition(
                "systemd unit has no explicit service account".into(),
            )
        })?;
    let command = contents
        .lines()
        .find_map(|line| line.strip_prefix("ExecStart="))
        .ok_or_else(|| {
            LifecycleError::ConflictingServiceDefinition("systemd unit has no ExecStart".into())
        })?;
    let arguments = parse_systemd_words(command)?;
    let (executable, arguments) = arguments.split_first().ok_or_else(|| {
        LifecycleError::ConflictingServiceDefinition("systemd ExecStart is empty".into())
    })?;
    let data = contents
        .lines()
        .find_map(|line| line.strip_prefix("ReadWritePaths="))
        .ok_or_else(|| {
            LifecycleError::ConflictingServiceDefinition(
                "systemd unit does not identify the persistent data path".into(),
            )
        })?;
    let definition = ServiceDefinition::new(
        ServiceManagerKind::LinuxSystemd,
        service_id,
        Path::new(executable),
        arguments.to_vec(),
        account,
        Path::new(&decode_systemd_word(data)?),
    )?;
    if render_service_definition(&definition)? != contents {
        return Err(LifecycleError::ConflictingServiceDefinition(
            "systemd unit contains fields outside Kakune's verified definition".into(),
        ));
    }
    if definition.boot_policy != BootPolicy::Automatic {
        return Err(LifecycleError::ConflictingServiceDefinition(
            "systemd boot policy is not automatic".into(),
        ));
    }
    Ok(definition)
}

fn parse_systemd_words(input: &str) -> Result<Vec<String>, LifecycleError> {
    input.split_whitespace().map(decode_systemd_word).collect()
}

fn decode_systemd_word(input: &str) -> Result<String, LifecycleError> {
    let bytes = input.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\\' {
            if bytes.get(index + 1) == Some(&b'\\') {
                decoded.push(b'\\');
                index += 2;
            } else if bytes.get(index + 1) == Some(&b'x') && index + 3 < bytes.len() {
                let hex = std::str::from_utf8(&bytes[index + 2..index + 4]).map_err(|_| {
                    LifecycleError::UnsafeOwnership("invalid systemd hex escape".into())
                })?;
                let byte = u8::from_str_radix(hex, 16).map_err(|_| {
                    LifecycleError::UnsafeOwnership("invalid systemd hex escape".into())
                })?;
                decoded.push(byte);
                index += 4;
            } else {
                return Err(LifecycleError::UnsafeOwnership(
                    "unsupported systemd escape in native definition".into(),
                ));
            }
        } else if bytes[index] == b'$' && bytes.get(index + 1) == Some(&b'$') {
            decoded.push(b'$');
            index += 2;
        } else if bytes[index] == b'%' && bytes.get(index + 1) == Some(&b'%') {
            decoded.push(b'%');
            index += 2;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded)
        .map_err(|_| LifecycleError::UnsafeOwnership("systemd value is not UTF-8".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn systemd_definition_round_trips_escaped_paths() {
        let definition = ServiceDefinition::new(
            ServiceManagerKind::LinuxSystemd,
            "kakune-core.service",
            Path::new("/usr/local/bin/kakune core"),
            vec![
                "service".into(),
                "run".into(),
                "--data-dir".into(),
                "/var/lib/kakune 雪 $state 100%".into(),
            ],
            "root",
            Path::new("/var/lib/kakune 雪 $state 100%"),
        )
        .unwrap();
        let rendered = render_service_definition(&definition).unwrap();
        assert!(rendered.contains("$$state"));
        assert!(rendered.contains("100%%"));
        let parsed = parse_unit_definition("kakune-core.service", &rendered).unwrap();
        assert!(definition.equivalent_to(&parsed));
    }

    #[test]
    fn systemd_pending_jobs_are_not_mapped_to_stopped() {
        let output = "LoadState=loaded\nActiveState=activating\nSubState=start\nJob=42";
        assert_eq!(
            parse_manager_state(ServiceManagerKind::LinuxSystemd, output).unwrap(),
            crate::lifecycle::NativeManagerState::Starting
        );
        assert_eq!(property(output, "Job"), Some("42"));
    }
}
