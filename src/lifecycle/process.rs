//! Process identity evidence and retained ownership of spawned children.

use std::{
    collections::BTreeMap,
    io,
    path::{Path, PathBuf},
    process::ExitStatus,
    sync::Mutex,
    time::Duration,
};

use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System, UpdateKind};
use tokio::process::Child;
use uuid::Uuid;

#[cfg(windows)]
use std::ops::Deref;

use super::{
    Deadline, LifecycleError, MAX_HELPER_DIAGNOSTIC_BYTES, MonotonicClock,
    ownership::{OwnerIdentity, ProcessIdentity},
};

#[cfg(any(target_os = "linux", test))]
mod linux;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ProcessEvidence {
    pub identity: ProcessIdentity,
    pub owner: OwnerIdentity,
}

pub trait ProcessEvidenceProvider: Send + Sync {
    /// `Ok(None)` means a successful OS observation found no such process.
    /// Missing owner/executable/start evidence is an error, never a partial match.
    fn observe(
        &self,
        pid: u32,
        deadline: Deadline,
        clock: &dyn MonotonicClock,
    ) -> Result<Option<ProcessEvidence>, LifecycleError>;
}

/// A process match is the conjunction of PID, OS start identity, and absolute
/// executable evidence. PID-only observations are never sufficient.
pub fn verify_process_identity(
    expected: &ProcessIdentity,
    observed: &ProcessEvidence,
) -> Result<(), LifecycleError> {
    expected.validate()?;
    observed.identity.validate()?;
    observed.owner.validate()?;
    if expected == &observed.identity {
        Ok(())
    } else {
        Err(LifecycleError::UnverifiedProcess)
    }
}

/// Detaches the current Unix child into a new session. This must be called by
/// the hidden child entry point before constructing Tokio or starting threads.
#[cfg(unix)]
pub fn detach_current_process() -> Result<(), LifecycleError> {
    rustix::process::setsid()
        .map(|_| ())
        .map_err(|error| LifecycleError::Io(error.into()))
}

/// Read-only process observation. sysinfo is evidence only; it is never used to
/// signal a discovered PID or as a substitute for the instance control channel.
#[derive(Default)]
pub struct SystemProcessEvidence {
    system: Mutex<System>,
}

impl ProcessEvidenceProvider for SystemProcessEvidence {
    fn observe(
        &self,
        pid: u32,
        deadline: Deadline,
        clock: &dyn MonotonicClock,
    ) -> Result<Option<ProcessEvidence>, LifecycleError> {
        if deadline.is_expired(clock) {
            return Err(LifecycleError::DeadlineExpired {
                phase: "process observation",
            });
        }
        #[cfg(target_os = "linux")]
        let start_identity = match linux::observe_start_identity(pid)? {
            Some(identity) => identity,
            None => return Ok(None),
        };
        let pid = Pid::from_u32(pid);
        let mut system = self
            .system
            .lock()
            .map_err(|_| LifecycleError::UnverifiedProcess)?;
        system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[pid]),
            true,
            ProcessRefreshKind::nothing()
                .with_user(UpdateKind::Always)
                .with_exe(UpdateKind::Always),
        );
        if deadline.is_expired(clock) {
            return Err(LifecycleError::DeadlineExpired {
                phase: "process observation",
            });
        }
        let Some(process) = system.process(pid) else {
            return Ok(None);
        };
        let executable = process.exe().ok_or(LifecycleError::UnverifiedProcess)?;
        let owner = process.user_id().ok_or(LifecycleError::UnverifiedProcess)?;
        #[cfg(not(target_os = "linux"))]
        let start_identity = {
            let start_time = process.start_time();
            if start_time == 0 {
                return Err(LifecycleError::UnverifiedProcess);
            }
            start_time.to_string()
        };
        if !executable.is_absolute() {
            return Err(LifecycleError::UnverifiedProcess);
        }
        // Bracket owner/executable observation with the kernel start identity so
        // a PID replacement during refresh cannot combine two processes' evidence.
        #[cfg(target_os = "linux")]
        if linux::observe_start_identity(pid.as_u32())?.as_deref() != Some(&start_identity) {
            return Err(LifecycleError::UnverifiedProcess);
        }
        if deadline.is_expired(clock) {
            return Err(LifecycleError::DeadlineExpired {
                phase: "process observation",
            });
        }

        #[cfg(unix)]
        let owner = OwnerIdentity::UnixUid(**owner);
        #[cfg(windows)]
        let owner = OwnerIdentity::WindowsSid(owner.deref().to_string());
        #[cfg(not(any(unix, windows)))]
        let owner = OwnerIdentity::Other(format!("{owner:?}"));

        let evidence = ProcessEvidence {
            identity: ProcessIdentity {
                pid: pid.as_u32(),
                start_identity,
                executable: executable.to_path_buf(),
            },
            owner,
        };
        evidence.identity.validate()?;
        evidence.owner.validate()?;
        Ok(Some(evidence))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChildPurpose {
    /// A Core child that has not yet completed the parent-owned ready/commit step.
    UncommittedCoreLaunch,
    /// A committed Core instance. It must only receive cooperative control.
    CommittedCore,
    /// An owned one-step native administration/ACL helper.
    AdministrativeHelper,
}

/// A child handle remains attached to the lifecycle operation until exit is
/// observed. Destructive termination is deliberately restricted by purpose.
pub struct TrackedChild {
    launch_id: Uuid,
    child: Child,
    purpose: ChildPurpose,
}

impl TrackedChild {
    pub fn new(child: Child, purpose: ChildPurpose) -> Self {
        Self {
            launch_id: Uuid::new_v4(),
            child,
            purpose,
        }
    }

    pub fn launch_id(&self) -> Uuid {
        self.launch_id
    }

    pub fn pid(&self) -> Option<u32> {
        self.child.id()
    }

    pub fn take_stdin(&mut self) -> Option<tokio::process::ChildStdin> {
        self.child.stdin.take()
    }

    pub fn take_stdout(&mut self) -> Option<tokio::process::ChildStdout> {
        self.child.stdout.take()
    }

    pub fn purpose(&self) -> ChildPurpose {
        self.purpose
    }

    pub fn try_wait(&mut self) -> Result<Option<ExitStatus>, LifecycleError> {
        self.child.try_wait().map_err(Into::into)
    }

    pub fn mark_core_committed(&mut self) -> Result<(), LifecycleError> {
        if self.purpose != ChildPurpose::UncommittedCoreLaunch {
            return Err(LifecycleError::UnsafeOwnership(
                "only an uncommitted Core launch can be committed".to_owned(),
            ));
        }
        self.purpose = ChildPurpose::CommittedCore;
        Ok(())
    }

    pub async fn wait_until(
        &mut self,
        deadline: Deadline,
        clock: &dyn MonotonicClock,
    ) -> Result<Option<ExitStatus>, LifecycleError> {
        let remaining = deadline.remaining(clock);
        if remaining.is_zero() {
            return Err(LifecycleError::DeadlineExpired {
                phase: "child observation",
            });
        }
        match tokio::time::timeout(remaining, self.child.wait()).await {
            Ok(Ok(status)) => Ok(Some(status)),
            Ok(Err(error)) => Err(error.into()),
            Err(_) => Err(LifecycleError::DeadlineExpired {
                phase: "child observation",
            }),
        }
    }

    /// Failed-start cleanup is authorized only by the retained, uncommitted
    /// launch handle, never by a PID rediscovered from metadata.
    pub fn terminate_failed_core_launch(&mut self) -> Result<(), LifecycleError> {
        if self.purpose != ChildPurpose::UncommittedCoreLaunch {
            return Err(LifecycleError::UnsafeOwnership(
                "forced cleanup is limited to this retained uncommitted launch handle".to_owned(),
            ));
        }
        self.child.start_kill().map_err(Into::into)
    }

    /// Helpers can be terminated through their retained handle. This does not
    /// imply that an accepted native-manager request was cancelled.
    pub fn terminate_owned_helper(&mut self) -> Result<(), LifecycleError> {
        if self.purpose != ChildPurpose::AdministrativeHelper {
            return Err(LifecycleError::UnsafeOwnership(
                "only an owned administrative helper may be terminated".to_owned(),
            ));
        }
        self.child.start_kill().map_err(Into::into)
    }
}

impl Drop for TrackedChild {
    fn drop(&mut self) {
        if self.purpose == ChildPurpose::UncommittedCoreLaunch {
            // Best-effort fallback for cancellation/error paths. The retained
            // handle, never a PID from metadata, authorizes this failed-start
            // cleanup. Callers still wait/reap explicitly when possible.
            let _ = self.child.start_kill();
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChildOwnershipSnapshot {
    pub launch_id: Uuid,
    pub pid: Option<u32>,
    pub purpose: ChildPurpose,
}

/// Lifecycle-owned child table. Entries can only be removed after the OS reports
/// exit for the exact retained child handle.
#[derive(Default)]
pub struct RetainedChildRegistry {
    children: tokio::sync::Mutex<BTreeMap<Uuid, std::sync::Arc<tokio::sync::Mutex<TrackedChild>>>>,
}

impl RetainedChildRegistry {
    pub async fn retain(&self, child: Child, purpose: ChildPurpose) -> Uuid {
        self.retain_tracked(TrackedChild::new(child, purpose)).await
    }

    pub async fn retain_tracked(&self, child: TrackedChild) -> Uuid {
        let launch_id = child.launch_id();
        self.children.lock().await.insert(
            launch_id,
            std::sync::Arc::new(tokio::sync::Mutex::new(child)),
        );
        launch_id
    }

    /// Transfers a verified committed daemon to its own lifetime lease/control
    /// record. Failed or uncommitted launches cannot use this detach boundary.
    pub async fn handoff_committed(&self, launch_id: Uuid) -> Result<(), LifecycleError> {
        let child = self.child_handle(launch_id).await?;
        let child = child.lock().await;
        if child.purpose() != ChildPurpose::CommittedCore {
            return Err(LifecycleError::UnsafeOwnership(
                "only a verified committed Core may be handed off".into(),
            ));
        }
        self.children.lock().await.remove(&launch_id);
        Ok(())
    }

    pub async fn snapshots(&self) -> Vec<ChildOwnershipSnapshot> {
        let children = self
            .children
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut snapshots = Vec::with_capacity(children.len());
        for child in children {
            let child = child.lock().await;
            snapshots.push(ChildOwnershipSnapshot {
                launch_id: child.launch_id(),
                pid: child.pid(),
                purpose: child.purpose(),
            });
        }
        snapshots
    }

    pub async fn mark_core_committed(&self, launch_id: Uuid) -> Result<(), LifecycleError> {
        self.with_child(launch_id, TrackedChild::mark_core_committed)
            .await
    }

    pub async fn terminate_failed_core_launch(
        &self,
        launch_id: Uuid,
    ) -> Result<(), LifecycleError> {
        self.with_child(launch_id, TrackedChild::terminate_failed_core_launch)
            .await
    }

    pub async fn terminate_owned_helper(&self, launch_id: Uuid) -> Result<(), LifecycleError> {
        self.with_child(launch_id, TrackedChild::terminate_owned_helper)
            .await
    }

    pub async fn wait_until(
        &self,
        launch_id: Uuid,
        deadline: Deadline,
        clock: &dyn MonotonicClock,
    ) -> Result<Option<ExitStatus>, LifecycleError> {
        let child = self.child_handle(launch_id).await?;
        let mut child = child.lock().await;
        child.wait_until(deadline, clock).await
    }

    /// `false` means the exact child is still live or its exit is not observable.
    pub async fn release_after_exit(&self, launch_id: Uuid) -> Result<bool, LifecycleError> {
        let child_handle = self.child_handle(launch_id).await?;
        let exited = {
            let mut child = child_handle.lock().await;
            match child.child.try_wait() {
                Ok(Some(_)) => true,
                Ok(None) => false,
                Err(error) => return Err(error.into()),
            }
        };
        if exited {
            let mut children = self.children.lock().await;
            if children
                .get(&launch_id)
                .is_some_and(|current| std::sync::Arc::ptr_eq(current, &child_handle))
            {
                children.remove(&launch_id);
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn child_handle(
        &self,
        launch_id: Uuid,
    ) -> Result<std::sync::Arc<tokio::sync::Mutex<TrackedChild>>, LifecycleError> {
        self.children
            .lock()
            .await
            .get(&launch_id)
            .cloned()
            .ok_or_else(|| {
                LifecycleError::UnsafeOwnership("retained child handle was not found".to_owned())
            })
    }

    async fn with_child(
        &self,
        launch_id: Uuid,
        operation: impl FnOnce(&mut TrackedChild) -> Result<(), LifecycleError>,
    ) -> Result<(), LifecycleError> {
        let child = self.child_handle(launch_id).await?;
        let mut child = child.lock().await;
        operation(&mut child)
    }
}

/// Bounded capture for helper diagnostics. The boolean return value indicates
/// that the incoming bytes were truncated.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BoundedCapture {
    bytes: Vec<u8>,
    truncated: bool,
}

impl BoundedCapture {
    pub fn append(&mut self, incoming: &[u8]) -> bool {
        let available = MAX_HELPER_DIAGNOSTIC_BYTES.saturating_sub(self.bytes.len());
        let accepted = incoming.len().min(available);
        self.bytes.extend_from_slice(&incoming[..accepted]);
        self.truncated |= accepted < incoming.len();
        self.truncated
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn was_truncated(&self) -> bool {
        self.truncated
    }

    pub fn into_string_lossy(self) -> String {
        String::from_utf8_lossy(&self.bytes).into_owned()
    }
}

/// A reader that cannot retain more than the lifecycle diagnostic cap.
pub fn capture_reader<R: io::Read>(mut reader: R) -> io::Result<BoundedCapture> {
    capture_reader_with_limit(&mut reader, MAX_HELPER_DIAGNOSTIC_BYTES)
}

/// Captures at most `limit` bytes and drains the remainder without retaining it.
pub fn capture_reader_with_limit<R: io::Read>(
    mut reader: R,
    limit: usize,
) -> io::Result<BoundedCapture> {
    let mut capture = BoundedCapture::default();
    let mut buffer = [0_u8; 8192];
    let limit = limit.min(MAX_HELPER_DIAGNOSTIC_BYTES);
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            return Ok(capture);
        }
        let available = limit.saturating_sub(capture.as_bytes().len());
        let accepted = available.min(read);
        capture.append(&buffer[..accepted]);
        if accepted < read || capture.as_bytes().len() == limit {
            // Drain without retaining so a caller can continue to join the helper.
            if io::copy(&mut reader, &mut io::sink())? > 0 {
                capture.truncated = true;
            }
            return Ok(capture);
        }
    }
}

pub fn absolute_executable(path: &Path) -> Result<PathBuf, LifecycleError> {
    if !path.is_absolute() {
        return Err(LifecycleError::UnverifiedProcess);
    }
    Ok(path.to_path_buf())
}

pub fn remaining_or_timeout(
    deadline: Deadline,
    clock: &dyn MonotonicClock,
    phase: &'static str,
) -> Result<Duration, LifecycleError> {
    let remaining = deadline.remaining(clock);
    if remaining.is_zero() {
        Err(LifecycleError::DeadlineExpired { phase })
    } else {
        Ok(remaining)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn helper_capture_is_capped_at_one_mibibyte() {
        let output = vec![b'x'; MAX_HELPER_DIAGNOSTIC_BYTES + 17];
        let capture = capture_reader(Cursor::new(output)).unwrap();
        assert_eq!(capture.as_bytes().len(), MAX_HELPER_DIAGNOSTIC_BYTES);
        assert!(capture.was_truncated());
    }

    #[test]
    fn a_small_helper_capture_is_not_marked_truncated() {
        let capture = capture_reader(Cursor::new(b"bounded".to_vec())).unwrap();
        assert_eq!(capture.as_bytes(), b"bounded");
        assert!(!capture.was_truncated());
    }
}
