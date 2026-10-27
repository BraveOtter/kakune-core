//! Theoretical root launchd adapter. The implementation and deterministic
//! definitions are provided, but no real-Mac acceptance evidence is claimed.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use super::{
    LifecycleError, MonotonicClock, NativeMutation, NativeServiceObservation, ServiceAction,
    ServiceDefinition, ServiceManagerAdapter, ServiceManagerKind,
    native::{
        remove_native_definition, require_success, run_owned_command, write_native_definition,
    },
    observe_service_core, parse_manager_state, render_service_definition,
};
use crate::lifecycle::{Deadline, InstallationObservation};

const LAUNCH_DAEMONS: &str = "/Library/LaunchDaemons";

#[derive(Clone, Copy, Debug, Default)]
pub struct MacosServiceManager;

impl ServiceManagerAdapter for MacosServiceManager {
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
        if manager != ServiceManagerKind::MacosLaunchd {
            return Err(LifecycleError::ManagerUnavailable(
                "macOS adapter received a non-launchd request".into(),
            ));
        }
        let plist = plist_path(service_id)?;
        let output = run_owned_command(
            Command::new("/bin/launchctl").args(["print", &format!("system/{service_id}")]),
            deadline,
            clock,
        )?;
        let manager_output = output.text();
        let manager_state = parse_manager_state(ServiceManagerKind::MacosLaunchd, &manager_output)?;
        let contents = read_plist(&plist)?;
        if contents.is_none() {
            if manager_state == crate::lifecycle::NativeManagerState::Stopped {
                return Ok(NativeServiceObservation::absent());
            }
            return Ok(NativeServiceObservation::unknown(format!(
                "launchd reports {service_id} without the Kakune-owned plist"
            )));
        }
        let definition = parse_plist_definition(service_id, contents.as_deref().unwrap())?;
        let (core, core_unknown) = observe_service_core(&definition.data_dir, deadline, clock)?;
        let pending_operation = manager_output
            .to_ascii_lowercase()
            .contains("state = spawn")
            || manager_output
                .to_ascii_lowercase()
                .contains("state = stopping");
        Ok(NativeServiceObservation {
            installed: InstallationObservation::Installed,
            manager_state,
            definition: Some(definition.clone()),
            account: Some(definition.account.clone()),
            boot_enabled: true,
            core,
            core_unknown,
            diagnostic_location: Some(format!("launchd system/{service_id}")),
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
            "launchd mutations require a journal-authorized helper".into(),
        ))
    }
}

pub(super) fn execute_step(
    step: NativeMutation,
    definition: &ServiceDefinition,
    deadline: Deadline,
    clock: &dyn MonotonicClock,
) -> Result<(), LifecycleError> {
    let plist = plist_path(&definition.service_id)?;
    let domain = format!("system/{}", definition.service_id);
    match step {
        NativeMutation::Publish => {
            write_native_definition(&plist, render_service_definition(definition)?.as_bytes())
        }
        NativeMutation::Delete => {
            remove_native_definition(&plist, render_service_definition(definition)?.as_bytes())
                .map(|_| ())
        }
        NativeMutation::Start => {
            let print = run_owned_command(
                Command::new("/bin/launchctl").args(["print", &domain]),
                deadline,
                clock,
            )?;
            if print.status_success {
                return require_success(
                    run_owned_command(
                        Command::new("/bin/launchctl").args(["kickstart", &domain]),
                        deadline,
                        clock,
                    )?,
                    "launchd kickstart",
                )
                .map(|_| ());
            }
            if !print
                .text()
                .to_ascii_lowercase()
                .contains("could not find service")
            {
                return require_success(print, "launchd domain observation").map(|_| ());
            }
            require_success(
                run_owned_command(
                    Command::new("/bin/launchctl")
                        .args(["bootstrap", "system"])
                        .arg(plist),
                    deadline,
                    clock,
                )?,
                "launchd bootstrap",
            )
            .map(|_| ())
        }
        NativeMutation::Stop => require_success(
            run_owned_command(
                Command::new("/bin/launchctl").args(["bootout", &domain]),
                deadline,
                clock,
            )?,
            "launchd bootout",
        )
        .map(|_| ()),
        _ => Err(LifecycleError::UnsafeOwnership(
            "unsupported launchd mutation step".into(),
        )),
    }
}

fn plist_path(service_id: &str) -> Result<PathBuf, LifecycleError> {
    if service_id.is_empty()
        || !service_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(LifecycleError::UnsafeOwnership(
            "invalid launchd label".into(),
        ));
    }
    Ok(Path::new(LAUNCH_DAEMONS).join(format!("{service_id}.plist")))
}

fn read_plist(path: &Path) -> Result<Option<String>, LifecycleError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(LifecycleError::UnsafeOwnership(
            "launchd plist is redirected or not a regular file".into(),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.uid() != 0 || metadata.permissions().mode() & 0o022 != 0 {
            return Err(LifecycleError::InsecureProtection(
                "launchd plist must be root-owned and not writable by untrusted accounts".into(),
            ));
        }
    }
    Ok(Some(super::native::read_native_definition(path)?))
}

fn xml_unescape(input: &str) -> Result<String, LifecycleError> {
    let mut output = input.to_owned();
    for (encoded, decoded) in [
        ("&quot;", "\""),
        ("&apos;", "'"),
        ("&gt;", ">"),
        ("&lt;", "<"),
        ("&amp;", "&"),
    ] {
        output = output.replace(encoded, decoded);
    }
    if input
        .replace("&quot;", "")
        .replace("&apos;", "")
        .replace("&gt;", "")
        .replace("&lt;", "")
        .replace("&amp;", "")
        .contains('&')
    {
        return Err(LifecycleError::UnsafeOwnership(
            "launchd plist contains an unsupported XML entity".into(),
        ));
    }
    Ok(output)
}

fn parse_plist_definition(
    service_id: &str,
    contents: &str,
) -> Result<ServiceDefinition, LifecycleError> {
    if !contents.contains("<key>RunAtLoad</key><true/>")
        || !contents.contains("<key>KeepAlive</key><true/>")
        || !contents.contains("<key>ExitTimeOut</key><integer>0</integer>")
        || contents.contains("/bin/sh")
    {
        return Err(LifecycleError::ConflictingServiceDefinition(
            "launchd plist does not match Kakune's protected system definition".into(),
        ));
    }
    let label = xml_value_after_key(contents, "Label")?;
    let account = xml_value_after_key(contents, "UserName")?;
    if label != service_id {
        return Err(LifecycleError::ConflictingServiceDefinition(
            "launchd plist label does not match its native identifier".into(),
        ));
    }
    let start = contents
        .find("<key>ProgramArguments</key><array>")
        .ok_or_else(|| {
            LifecycleError::ConflictingServiceDefinition(
                "launchd plist has no ProgramArguments array".into(),
            )
        })?
        + "<key>ProgramArguments</key><array>".len();
    let end = contents[start..].find("</array>").ok_or_else(|| {
        LifecycleError::ConflictingServiceDefinition(
            "launchd ProgramArguments array is unterminated".into(),
        )
    })? + start;
    let arguments = contents[start..end]
        .split("<string>")
        .skip(1)
        .map(|value| {
            value
                .split_once("</string>")
                .map(|(value, _)| xml_unescape(value))
                .ok_or_else(|| {
                    LifecycleError::ConflictingServiceDefinition(
                        "launchd argument is malformed".into(),
                    )
                })?
        })
        .collect::<Result<Vec<_>, _>>()?;
    let (executable, arguments) = arguments.split_first().ok_or_else(|| {
        LifecycleError::ConflictingServiceDefinition("launchd ProgramArguments is empty".into())
    })?;
    let data_dir = arguments
        .windows(2)
        .find(|pair| pair[0] == "--data-dir")
        .map(|pair| pair[1].as_str())
        .ok_or_else(|| {
            LifecycleError::ConflictingServiceDefinition(
                "launchd definition does not identify the data directory".into(),
            )
        })?;
    let definition = ServiceDefinition::new(
        ServiceManagerKind::MacosLaunchd,
        service_id,
        Path::new(executable),
        arguments.to_vec(),
        account,
        Path::new(data_dir),
    )?;
    if render_service_definition(&definition)? != contents {
        return Err(LifecycleError::ConflictingServiceDefinition(
            "launchd plist contains fields outside Kakune's verified definition".into(),
        ));
    }
    Ok(definition)
}

fn xml_value_after_key(contents: &str, key: &str) -> Result<String, LifecycleError> {
    let needle = format!("<key>{key}</key><string>");
    let start = contents.find(&needle).ok_or_else(|| {
        LifecycleError::ConflictingServiceDefinition(format!("launchd plist has no {key} field"))
    })? + needle.len();
    let value = contents[start..]
        .split_once("</string>")
        .map(|(value, _)| value)
        .ok_or_else(|| {
            LifecycleError::ConflictingServiceDefinition(format!(
                "launchd {key} field is unterminated"
            ))
        })?;
    xml_unescape(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn launchd_definition_round_trips_xml_escaped_paths() {
        let definition = ServiceDefinition::new(
            ServiceManagerKind::MacosLaunchd,
            "dev.kakune.core",
            Path::new("/usr/local/Kakune 雪/kakune core"),
            vec![
                "service".into(),
                "run".into(),
                "--data-dir".into(),
                "/Library/Kakune & Data".into(),
            ],
            "root",
            Path::new("/Library/Kakune & Data"),
        )
        .unwrap();
        let rendered = render_service_definition(&definition).unwrap();
        let parsed = parse_plist_definition("dev.kakune.core", &rendered).unwrap();
        assert!(definition.equivalent_to(&parsed));
    }
}
