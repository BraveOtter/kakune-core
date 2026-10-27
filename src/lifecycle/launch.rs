//! Parent-owned background launch, bounded handoff and retained failed starts.

use std::{
    io,
    path::{Path, PathBuf},
    process::Stdio,
    sync::OnceLock,
    time::Duration,
};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

use super::{
    CoreMode, CoreStartupOptions, Deadline, MonotonicClock, ObservedState, READINESS_BUDGET,
    SystemMonotonicClock,
    daemon::{self, DaemonObservation},
    process::{ChildPurpose, RetainedChildRegistry, SystemProcessEvidence, TrackedChild},
};

static RETAINED_LAUNCHES: OnceLock<RetainedChildRegistry> = OnceLock::new();

/// Structured, secret-free successful handoff. The CLI owns context updates and
/// rendering; a ready Core is not undone if its client-context update fails.
pub struct BackgroundLaunch {
    pub observation: DaemonObservation,
    pub diagnostic_location: PathBuf,
}

/// Injectable executable selection/launch preparation; production arguments and
/// platform detachment remain structured and do not invoke a shell.
pub trait BackgroundLauncher: Send + Sync {
    fn command(
        &self,
        startup: &CoreStartupOptions,
        log: &Path,
    ) -> Result<tokio::process::Command, String>;
}

pub struct ExecutableLauncher(pub PathBuf);

impl BackgroundLauncher for ExecutableLauncher {
    fn command(
        &self,
        startup: &CoreStartupOptions,
        log: &Path,
    ) -> Result<tokio::process::Command, String> {
        if !self.0.is_absolute() {
            return Err("background executable must be absolute".into());
        }
        let mut command = tokio::process::Command::new(&self.0);
        command
            .arg("daemon")
            .arg("--internal-daemon-child")
            .arg("--internal-log")
            .arg(log)
            .arg("--data-dir")
            .arg(&startup.data_dir);
        if let Some(config) = &startup.config_file {
            command.arg("--config").arg(config);
        }
        if let Some(listen) = &startup.listen_address {
            command.arg("--listen").arg(listen);
        }
        if let Some(cert) = &startup.tls_certificate {
            command.arg("--tls-cert").arg(cert);
        }
        if let Some(key) = &startup.tls_private_key {
            command.arg("--tls-key").arg(key);
        }
        for host in &startup.allowed_hosts {
            command.arg("--allowed-host").arg(host);
        }
        command.current_dir(&startup.data_dir);
        #[cfg(windows)]
        command.creation_flags(0x0000_0008 | 0x0000_0200 | 0x0800_0000);
        Ok(command)
    }
}

pub async fn start_background(
    startup: &CoreStartupOptions,
    executable: &Path,
) -> Result<BackgroundLaunch, String> {
    startup.validate().map_err(|error| error.to_string())?;
    let reservation =
        daemon::reserve_start(&startup.data_dir).map_err(|error| error.to_string())?;
    // Acquisition has its own phase; launch timing starts before constructing
    // or spawning the child, never after a potentially slow spawn returns.
    let clock = SystemMonotonicClock::default();
    let deadline = Deadline::after(&clock, READINESS_BUDGET);
    launch_reserved(
        startup,
        &ExecutableLauncher(executable.to_path_buf()),
        Some(reservation),
        deadline,
        &clock,
        RETAINED_LAUNCHES.get_or_init(RetainedChildRegistry::default),
    )
    .await
}

/// Deterministic library boundary for bounded launch tests/embedders. The caller
/// supplies one deadline in `clock`'s domain and owns the residual registry.
pub async fn start_background_with(
    startup: &CoreStartupOptions,
    launcher: &dyn BackgroundLauncher,
    deadline: Deadline,
    clock: &dyn MonotonicClock,
    retained: &RetainedChildRegistry,
) -> Result<BackgroundLaunch, String> {
    startup.validate().map_err(|error| error.to_string())?;
    let reservation =
        daemon::reserve_start(&startup.data_dir).map_err(|error| error.to_string())?;
    launch_reserved(
        startup,
        launcher,
        Some(reservation),
        deadline,
        clock,
        retained,
    )
    .await
}

async fn launch_reserved(
    startup: &CoreStartupOptions,
    launcher: &dyn BackgroundLauncher,
    mut reservation: Option<daemon::DaemonStartReservation>,
    deadline: Deadline,
    clock: &dyn MonotonicClock,
    retained: &RetainedChildRegistry,
) -> Result<BackgroundLaunch, String> {
    for child in retained.snapshots().await {
        retained
            .release_after_exit(child.launch_id)
            .await
            .map_err(|e| e.to_string())?;
    }
    if retained.snapshots().await.len() >= 16 {
        return Err(
            "sixteen launches remain unresolved; inspect their ownership records before retrying"
                .into(),
        );
    }
    let log_path = startup.data_dir.join("daemon.log");
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .map_err(|error| format!("cannot open daemon log {}: {error}", log_path.display()))?;
    ensure_remaining(deadline, clock)?;
    let mut command = launcher.command(startup, &log_path)?;
    ensure_remaining(deadline, clock)?;
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::from(log))
        .kill_on_drop(false);
    let mut child = TrackedChild::new(
        command
            .spawn()
            .map_err(|error| format!("cannot start Kakune daemon child: {error}"))?,
        ChildPurpose::UncommittedCoreLaunch,
    );
    let pid = child.pid().unwrap_or_default();
    let input = child.take_stdin();
    let output = child.take_stdout();
    // Retain before any fallible handoff operation. Cancellation closes the
    // pipes but cannot discard the owning process handle from this registry.
    let launch_id = retained.retain_tracked(child).await;
    let result: Result<DaemonObservation, String> = async {
        let mut input =
            input.ok_or_else(|| "daemon child startup channel was not piped".to_owned())?;
        let mut output =
            output.ok_or_else(|| "daemon child readiness channel was not piped".to_owned())?;
        if read_child_protocol(&mut output, deadline, clock)
            .await?
            .as_deref()
            != Some("CLAIM")
        {
            return Err("child did not claim the runtime lease".into());
        }
        if let Some(reservation) = reservation.take() {
            reservation.release();
        }
        let prepared = read_child_protocol(&mut output, deadline, clock).await?;
        let instance_id = parse_instance(prepared.as_deref(), "PREPARED")
            .ok_or_else(|| "child exited before readiness preparation".to_owned())?;
        let observation = daemon::observe_with(
            &startup.data_dir,
            deadline,
            clock,
            &SystemProcessEvidence::default(),
        )
        .map_err(|error| error.to_string())?;
        verify_child(&observation, instance_id, pid, ObservedState::Starting)?;
        let response = daemon::request_readiness_control(
            &startup.data_dir,
            instance_id,
            b"prepared",
            deadline,
            clock,
        )
        .await
        .map_err(|error| error.to_string())?;
        if response != b"prepared" {
            return Err("child transport did not confirm preparation".into());
        }
        let remaining = ensure_remaining(deadline, clock)?;
        tokio::time::timeout(remaining, async {
            input.write_all(b"COMMIT\n").await?;
            input.flush().await
        })
        .await
        .map_err(|_| "background Core readiness deadline expired".to_owned())?
        .map_err(|error: io::Error| format!("cannot send background launch commit: {error}"))?;
        let ready = read_child_protocol(&mut output, deadline, clock).await?;
        if parse_instance(ready.as_deref(), "READY") != Some(instance_id) {
            return Err("child exited or instance changed before readiness was committed".into());
        }
        let observation = daemon::observe_controlled_with(&startup.data_dir, deadline, clock)
            .await
            .map_err(|error| error.to_string())?;
        verify_child(&observation, instance_id, pid, ObservedState::Ready)?;
        ensure_remaining(deadline, clock)?;
        retained
            .mark_core_committed(launch_id)
            .await
            .map_err(|error| error.to_string())?;
        retained
            .handoff_committed(launch_id)
            .await
            .map_err(|error| error.to_string())?;
        Ok(observation)
    }
    .await;
    match result {
        Ok(observation) => Ok(BackgroundLaunch {
            observation,
            diagnostic_location: log_path,
        }),
        Err(error) => {
            let cleanup = cleanup_failed_launch(retained, launch_id, deadline, clock).await;
            Err(format!(
                "background daemon startup failed: {error}; {cleanup}; see diagnostic log {}{}",
                log_path.display(),
                read_log_excerpt(&log_path)
                    .map(|text| format!("; child diagnostic: {text}"))
                    .unwrap_or_default()
            ))
        }
    }
}

async fn cleanup_failed_launch(
    retained: &RetainedChildRegistry,
    id: Uuid,
    deadline: Deadline,
    clock: &dyn MonotonicClock,
) -> String {
    if retained.release_after_exit(id).await.unwrap_or(false) {
        return "failed-start child exit confirmed".into();
    }
    // Closing parent pipes above lets a healthy uncommitted child stop itself.
    // Both optional waits are caps of the ORIGINAL phase, never new budgets.
    if !deadline.is_expired(clock) {
        let _ = retained
            .wait_until(id, deadline.cap(clock, Duration::from_secs(2)), clock)
            .await;
        if retained.release_after_exit(id).await.unwrap_or(false) {
            return "failed-start child exit confirmed".into();
        }
    }
    if let Err(error) = retained.terminate_failed_core_launch(id).await {
        return format!("uncommitted launch {id} retained; termination request failed: {error}");
    }
    if !deadline.is_expired(clock) {
        let _ = retained
            .wait_until(id, deadline.cap(clock, Duration::from_secs(2)), clock)
            .await;
    }
    if retained.release_after_exit(id).await.unwrap_or(false) {
        "failed-start child exit confirmed".into()
    } else {
        let pid = retained
            .snapshots()
            .await
            .into_iter()
            .find(|child| child.launch_id == id)
            .and_then(|child| child.pid);
        format!(
            "uncommitted child PID {pid:?}, launch {id}, remains unconfirmed and retained; inspect runtime ownership before retrying (not graceful stop)"
        )
    }
}

fn ensure_remaining(deadline: Deadline, clock: &dyn MonotonicClock) -> Result<Duration, String> {
    let remaining = deadline.remaining(clock);
    if remaining.is_zero() {
        Err("background Core readiness deadline expired".into())
    } else {
        Ok(remaining)
    }
}

pub async fn read_child_protocol<R: AsyncRead + Unpin>(
    reader: &mut R,
    deadline: Deadline,
    clock: &dyn MonotonicClock,
) -> Result<Option<String>, String> {
    tokio::time::timeout(ensure_remaining(deadline, clock)?, async {
        let mut bytes = Vec::with_capacity(128);
        let mut byte = [0; 1];
        loop {
            match reader.read(&mut byte).await {
                Ok(0) if bytes.is_empty() => return Ok(None),
                Ok(0) => break,
                Ok(_) if byte[0] == b'\n' => break,
                Ok(_) => {
                    if bytes.len() >= 4096 {
                        return Err("background child protocol line exceeds 4096 bytes".into());
                    }
                    bytes.push(byte[0]);
                }
                Err(error) => {
                    return Err(format!("cannot read background child protocol: {error}"));
                }
            }
        }
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
        String::from_utf8(bytes)
            .map(Some)
            .map_err(|_| "background child protocol is not UTF-8".into())
    })
    .await
    .map_err(|_| "background Core readiness deadline expired".to_owned())?
}

fn parse_instance(message: Option<&str>, expected: &str) -> Option<Uuid> {
    let (kind, value) = message?.split_once('\t')?;
    (kind == expected)
        .then(|| Uuid::parse_str(value).ok())
        .flatten()
}

fn verify_child(
    observation: &DaemonObservation,
    instance: Uuid,
    pid: u32,
    state: ObservedState,
) -> Result<(), String> {
    if observation.state != state
        || observation.mode != Some(CoreMode::UserDaemon)
        || observation.verified_pid != Some(pid)
        || !observation.identity_verified
        || observation.instance_id() != Some(instance)
    {
        Err(format!(
            "cannot verify intended daemon instance {instance}, PID {pid}, state {state:?}; observed {:?}",
            observation.state
        ))
    } else {
        Ok(())
    }
}

fn read_log_excerpt(path: &Path) -> Option<String> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = std::fs::File::open(path).ok()?;
    let length = file.metadata().ok()?.len();
    file.seek(SeekFrom::Start(length.saturating_sub(4096)))
        .ok()?;
    let mut bytes = Vec::new();
    file.take(4096).read_to_end(&mut bytes).ok()?;
    let text = String::from_utf8_lossy(&bytes).trim().to_owned();
    (!text.is_empty()).then_some(text)
}
