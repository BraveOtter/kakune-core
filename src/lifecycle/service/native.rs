use std::{
    io,
    process::{Child, Command, Stdio},
    sync::{Mutex, OnceLock},
    thread,
    thread::JoinHandle,
    time::Duration,
};

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::io::Write;

use super::{LifecycleError, MonotonicClock};
use crate::lifecycle::{Deadline, MAX_HELPER_DIAGNOSTIC_BYTES, process::BoundedCapture};

pub(super) struct NativeCommandOutput {
    pub status_success: bool,
    pub code: Option<i32>,
    pub stdout: BoundedCapture,
    pub stderr: BoundedCapture,
}

struct RetainedNativeHelper {
    child: Child,
    stdout: JoinHandle<io::Result<BoundedCapture>>,
    stderr: JoinHandle<io::Result<BoundedCapture>>,
}

static RETAINED_HELPERS: OnceLock<Mutex<Vec<RetainedNativeHelper>>> = OnceLock::new();

impl NativeCommandOutput {
    pub fn text(&self) -> String {
        let mut output = self.stdout.as_bytes().to_vec();
        output.extend_from_slice(self.stderr.as_bytes());
        String::from_utf8_lossy(&output).into_owned()
    }
}

/// Runs a native manager client as an owned child, with bounded output and an
/// absolute deadline. Killing this client is not treated as cancellation of a
/// manager request it may already have submitted.
pub(super) fn run_owned_command(
    command: &mut Command,
    deadline: Deadline,
    clock: &dyn MonotonicClock,
) -> Result<NativeCommandOutput, LifecycleError> {
    reap_confirmed_helpers();
    if RETAINED_HELPERS.get().is_some_and(|registry| {
        registry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
            >= 16
    }) {
        return Err(LifecycleError::UnresolvedServiceAttempt(
            "sixteen native helper resources remain unresolved; no additional client was launched"
                .into(),
        ));
    }
    let deadline = deadline.cap(clock, super::super::HELPER_BUDGET);
    let remaining = deadline.remaining(clock);
    if remaining.is_zero() {
        return Err(LifecycleError::DeadlineExpired {
            phase: "native service helper",
        });
    }
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn().map_err(map_spawn_error)?;
    let stdout = child.stdout.take().ok_or_else(|| {
        LifecycleError::ManagerUnavailable("service helper stdout was not captured".to_owned())
    })?;
    let stderr = child.stderr.take().ok_or_else(|| {
        LifecycleError::ManagerUnavailable("service helper stderr was not captured".to_owned())
    })?;
    let half_limit = MAX_HELPER_DIAGNOSTIC_BYTES / 2;
    let stdout_reader = thread::spawn(move || {
        crate::lifecycle::process::capture_reader_with_limit(stdout, half_limit)
    });
    let stderr_reader = thread::spawn(move || {
        crate::lifecycle::process::capture_reader_with_limit(stderr, half_limit)
    });

    let work_deadline = helper_work_deadline(deadline, clock);
    let completion = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Some((status, !work_deadline.is_expired(clock))),
            Ok(None) => {}
            Err(error) => {
                let pid = child.id();
                let _ = child.kill();
                if reap_deadline(&mut child, deadline, clock)
                    && stdout_reader.is_finished()
                    && stderr_reader.is_finished()
                {
                    let _ = join_reader(stdout_reader)?;
                    let _ = join_reader(stderr_reader)?;
                    return Err(error.into());
                }
                retain_helper(child, stdout_reader, stderr_reader);
                return Err(LifecycleError::HelperUnreaped { pid });
            }
        }
        let remaining = work_deadline.remaining(clock);
        if remaining.is_zero() {
            // The retained process handle authorizes termination of this
            // manager client only. The durable attempt journal remains the
            // authority for reconciling an accepted native operation.
            let _ = child.kill();
            let cleanup = reap_deadline(&mut child, deadline, clock);
            if !cleanup {
                let pid = child.id();
                retain_helper(child, stdout_reader, stderr_reader);
                return Err(LifecycleError::HelperUnreaped { pid });
            }
            break None;
        }
        thread::sleep(remaining.min(Duration::from_millis(20)));
    };
    // Process exit does not imply EOF: descendants can inherit capture pipes.
    // Never join a still-running reader outside the same helper budget.
    while (!stdout_reader.is_finished() || !stderr_reader.is_finished())
        && !deadline.is_expired(clock)
    {
        thread::sleep(deadline.remaining(clock).min(Duration::from_millis(10)));
    }
    if !stdout_reader.is_finished() || !stderr_reader.is_finished() {
        let pid = child.id();
        retain_helper(child, stdout_reader, stderr_reader);
        return Err(LifecycleError::HelperUnreaped { pid });
    }
    if completion.is_none() {
        join_reader(stdout_reader)?;
        join_reader(stderr_reader)?;
        return Err(LifecycleError::DeadlineExpired {
            phase: "native service helper",
        });
    }
    let stdout = stdout_reader.join().map_err(|_| {
        LifecycleError::ManagerUnavailable("helper stdout reader panicked".into())
    })??;
    let stderr = stderr_reader.join().map_err(|_| {
        LifecycleError::ManagerUnavailable("helper stderr reader panicked".into())
    })??;
    let (status, completed_in_budget) = completion.expect("completion was checked above");
    if !completed_in_budget {
        return Err(LifecycleError::DeadlineExpired {
            phase: "native service helper",
        });
    }
    debug_assert!(stdout.as_bytes().len() + stderr.as_bytes().len() <= MAX_HELPER_DIAGNOSTIC_BYTES);
    Ok(NativeCommandOutput {
        status_success: status.success(),
        code: status.code(),
        stdout,
        stderr,
    })
}

fn join_reader(
    reader: JoinHandle<io::Result<BoundedCapture>>,
) -> Result<BoundedCapture, LifecycleError> {
    reader
        .join()
        .map_err(|_| LifecycleError::ManagerUnavailable("helper output reader panicked".into()))?
        .map_err(Into::into)
}

fn reap_deadline(child: &mut Child, deadline: Deadline, clock: &dyn MonotonicClock) -> bool {
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return true,
            Ok(None) => {}
            Err(_) => return false,
        }
        let remaining = deadline.remaining(clock);
        if remaining.is_zero() {
            return false;
        }
        thread::sleep(remaining.min(Duration::from_millis(10)));
    }
}

/// Reserves up to the final second of the helper budget for termination and
/// reaping. When less than one second remains, the helper gets no additional
/// work time; otherwise its work deadline leaves exactly one second available
/// for bounded cleanup.
pub(super) fn helper_work_deadline(deadline: Deadline, clock: &dyn MonotonicClock) -> Deadline {
    let cleanup_reserve = deadline.remaining(clock).min(Duration::from_secs(1));
    Deadline::at(deadline.endpoint().saturating_sub(cleanup_reserve))
}

fn retain_helper(
    child: Child,
    stdout: JoinHandle<io::Result<BoundedCapture>>,
    stderr: JoinHandle<io::Result<BoundedCapture>>,
) {
    let helpers = RETAINED_HELPERS.get_or_init(|| Mutex::new(Vec::new()));
    if let Ok(mut helpers) = helpers.lock() {
        helpers.push(RetainedNativeHelper {
            child,
            stdout,
            stderr,
        });
    } else {
        // Keep the handle reachable even after a poisoned registry. Leaking the
        // owned process handle is safer than detaching an unconfirmed helper.
        let _ = Box::leak(Box::new(RetainedNativeHelper {
            child,
            stdout,
            stderr,
        }));
    }
}

fn reap_confirmed_helpers() {
    let Some(helpers) = RETAINED_HELPERS.get() else {
        return;
    };
    let Ok(mut helpers) = helpers.lock() else {
        return;
    };
    let mut pending = Vec::new();
    for mut helper in helpers.drain(..) {
        match helper.child.try_wait() {
            Ok(Some(_)) if helper.stdout.is_finished() && helper.stderr.is_finished() => {
                let _ = helper.stdout.join();
                let _ = helper.stderr.join();
            }
            Ok(Some(_)) | Ok(None) | Err(_) => pending.push(helper),
        }
    }
    *helpers = pending;
}

fn map_spawn_error(error: io::Error) -> LifecycleError {
    if error.kind() == io::ErrorKind::PermissionDenied {
        LifecycleError::AccessDenied(error.to_string())
    } else {
        LifecycleError::ManagerUnavailable(error.to_string())
    }
}

pub(super) fn require_success(
    output: NativeCommandOutput,
    action: &str,
) -> Result<String, LifecycleError> {
    let text = output.text();
    if output.status_success {
        Ok(text)
    } else {
        let lower = text.to_ascii_lowercase();
        if lower.contains("access is denied")
            || lower.contains("permission denied")
            || lower.contains("operation not permitted")
            || lower.contains("not authorized")
        {
            Err(LifecycleError::AccessDenied(text.trim().to_owned()))
        } else {
            Err(LifecycleError::ManagerUnavailable(format!(
                "{action} failed (exit {:?}): {}",
                output.code,
                text.trim()
            )))
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(super) fn write_native_definition(
    path: &std::path::Path,
    contents: &[u8],
) -> Result<(), LifecycleError> {
    if !path.is_absolute() {
        return Err(LifecycleError::UnsafeOwnership(
            "native service definition path must be absolute".to_owned(),
        ));
    }
    let parent = path.parent().ok_or_else(|| {
        LifecycleError::UnsafeOwnership("native definition has no parent directory".to_owned())
    })?;
    validate_native_directory(parent)?;
    match std::fs::symlink_metadata(path) {
        Ok(_) => {
            return Err(LifecycleError::ConflictingServiceDefinition(format!(
                "{} already exists outside this install attempt",
                path.display()
            )));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let temporary = parent.join(format!(".kakune-service-{}.tmp", uuid::Uuid::new_v4()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o644);
    }
    let mut file = options.open(&temporary)?;
    let result = (|| -> Result<(), LifecycleError> {
        file.write_all(contents)?;
        file.sync_all()?;
        drop(file);
        match std::fs::symlink_metadata(path) {
            Ok(_) => {
                return Err(LifecycleError::ConflictingServiceDefinition(format!(
                    "{} appeared during installation",
                    path.display()
                )));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        // Atomic no-clobber publication: rename would overwrite a replacement
        // created by an external administrator between inspection and publish.
        std::fs::hard_link(&temporary, path)?;
        std::fs::remove_file(&temporary)?;
        #[cfg(unix)]
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(super) fn remove_native_definition(
    path: &std::path::Path,
    expected_contents: &[u8],
) -> Result<bool, LifecycleError> {
    validate_native_directory(path.parent().ok_or_else(|| {
        LifecycleError::UnsafeOwnership("native definition has no parent".to_owned())
    })?)?;
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(LifecycleError::UnsafeOwnership(
            "native definition is redirected or is not a regular file".to_owned(),
        ));
    }
    let actual = std::fs::read(path)?;
    if actual != expected_contents {
        return Err(LifecycleError::ConflictingServiceDefinition(
            "native definition changed after ownership was observed".to_owned(),
        ));
    }
    std::fs::remove_file(path)?;
    Ok(true)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(super) fn validate_native_directory(path: &std::path::Path) -> Result<(), LifecycleError> {
    if !path.is_absolute() {
        return Err(LifecycleError::UnsafeOwnership(
            "native service directory must be absolute".to_owned(),
        ));
    }
    for ancestor in path.ancestors() {
        let metadata = std::fs::symlink_metadata(ancestor)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(LifecycleError::UnsafeOwnership(format!(
                "{} is redirected or is not a directory",
                ancestor.display()
            )));
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let metadata = std::fs::symlink_metadata(path)?;
        if metadata.uid() != 0 || metadata.permissions().mode() & 0o022 != 0 {
            return Err(LifecycleError::InsecureProtection(format!(
                "{} is not a protected root-owned native manager directory",
                path.display()
            )));
        }
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(super) fn read_native_definition(path: &std::path::Path) -> Result<String, LifecycleError> {
    use std::io::Read;
    validate_native_directory(path.parent().ok_or_else(|| {
        LifecycleError::UnsafeOwnership("native definition has no parent".into())
    })?)?;
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take((super::super::MAX_CONTROL_MESSAGE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > super::super::MAX_CONTROL_MESSAGE_BYTES {
        return Err(LifecycleError::MessageTooLarge {
            limit: super::super::MAX_CONTROL_MESSAGE_BYTES,
        });
    }
    String::from_utf8(bytes).map_err(|_| {
        LifecycleError::UnsafeOwnership("native service definition is not UTF-8".into())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct FakeClock(Mutex<Duration>);

    impl MonotonicClock for FakeClock {
        fn now(&self) -> Duration {
            *self
                .0
                .lock()
                .expect("fake clock mutex should not be poisoned")
        }
    }

    impl FakeClock {
        fn set(&self, now: Duration) {
            *self
                .0
                .lock()
                .expect("fake clock mutex should not be poisoned") = now;
        }
    }

    #[test]
    fn helper_deadline_reserves_one_second_for_cleanup() {
        let clock = FakeClock(Mutex::new(Duration::ZERO));
        let deadline = Deadline::after(&clock, Duration::from_secs(10));

        let work = helper_work_deadline(deadline, &clock);

        assert_eq!(work.remaining(&clock), Duration::from_secs(9));
        assert_eq!(
            deadline.remaining(&clock) - work.remaining(&clock),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn helper_gets_no_work_budget_when_only_the_cleanup_reserve_remains() {
        let clock = FakeClock(Mutex::new(Duration::ZERO));
        let deadline = Deadline::after(&clock, Duration::from_millis(700));
        clock.set(Duration::from_millis(200));

        let work = helper_work_deadline(deadline, &clock);

        assert_eq!(work.remaining(&clock), Duration::ZERO);
        assert_eq!(deadline.remaining(&clock), Duration::from_millis(500));
    }
}
