//! Read-only observation and cooperative control for current-user daemons.

use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use super::{
    ACQUISITION_BUDGET, CORE_GRACE_BUDGET, CoreMode, CoreObservation, Deadline, LifecycleError,
    LifecycleOperationResult, MonotonicClock, ObservedState, OperationOutcome, STATUS_BUDGET,
    STOP_DISPATCH_BUDGET, SystemMonotonicClock, ThreadWaiter,
    control::{
        self, CapabilityStorage, ControlContext, ControlOperation, ControlOperationRequest,
        LoopbackControlEndpoint, OwnerProtectedFileStorage,
    },
    ownership::{
        self, AdministrativeLease, InstancePhase, OwnerIdentity, OwnershipRead,
        RuntimeOwnershipRecord, cleanup_legacy_if_proven_dead_locked,
        cleanup_record_if_instance_locked, read_legacy_pid_file, read_ownership_record,
        runtime_lease_is_held,
    },
    process::{self, ProcessEvidenceProvider, SystemProcessEvidence},
};

const LEGACY_PID_FILE: &str = "daemon.pid";
const CONTROL_RETRY_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DaemonObservation {
    pub state: ObservedState,
    pub mode: Option<CoreMode>,
    pub verified_pid: Option<u32>,
    pub data_dir: PathBuf,
    pub listen_address: Option<String>,
    pub diagnostic_location: Option<String>,
    pub control_endpoint: Option<String>,
    pub identity_verified: bool,
    pub detail: Option<String>,
    instance_id: Option<uuid::Uuid>,
    owner: Option<OwnerIdentity>,
    process_identity: Option<ownership::ProcessIdentity>,
}

impl DaemonObservation {
    fn stopped(data_dir: &Path) -> Self {
        Self {
            state: ObservedState::Stopped,
            mode: None,
            verified_pid: None,
            data_dir: data_dir.to_path_buf(),
            listen_address: None,
            diagnostic_location: None,
            control_endpoint: None,
            identity_verified: true,
            detail: None,
            instance_id: None,
            owner: None,
            process_identity: None,
        }
    }

    fn unknown(data_dir: &Path, detail: impl Into<String>) -> Self {
        Self {
            state: ObservedState::Unknown,
            mode: None,
            verified_pid: None,
            data_dir: data_dir.to_path_buf(),
            listen_address: None,
            diagnostic_location: None,
            control_endpoint: None,
            identity_verified: false,
            detail: Some(detail.into()),
            instance_id: None,
            owner: None,
            process_identity: None,
        }
    }

    fn from_record(record: &RuntimeOwnershipRecord, verified: bool) -> Self {
        let instance = &record.instance;
        Self {
            state: phase_state(instance.phase),
            mode: Some(instance.mode),
            verified_pid: verified.then_some(instance.process_identity.pid),
            data_dir: instance.data_dir.clone(),
            listen_address: instance.listen_address.clone(),
            diagnostic_location: instance.diagnostic_location.clone(),
            control_endpoint: instance.control_endpoint.clone(),
            identity_verified: verified,
            detail: None,
            instance_id: Some(instance.instance_id),
            owner: Some(instance.owner.clone()),
            process_identity: Some(instance.process_identity.clone()),
        }
    }

    pub fn core_observation(&self) -> Option<CoreObservation> {
        Some(CoreObservation {
            mode: self.mode?,
            state: self.state,
            instance_id: self.instance_id,
            owner: self.owner.clone(),
            process: self.process_identity.clone(),
            data_dir: self.data_dir.clone(),
            listen_address: self.listen_address.clone(),
            diagnostic_location: self.diagnostic_location.clone(),
            identity_verified: self.identity_verified,
        })
    }

    pub fn instance_id(&self) -> Option<uuid::Uuid> {
        self.instance_id
    }
}

/// Holds per-data-directory start serialization until the child has acquired
/// its exclusive runtime lease. Dropping the reservation releases only the
/// administrative lock; stable lock files are retained.
pub struct DaemonStartReservation {
    _admin: AdministrativeLease,
}

impl DaemonStartReservation {
    pub fn release(self) {}
}

/// Serializes competing starts, rejects any live/unknown owner and cleans only
/// positively verified obsolete lifecycle metadata.
pub fn reserve_start(data_dir: &Path) -> Result<DaemonStartReservation, LifecycleError> {
    let clock = SystemMonotonicClock::default();
    let deadline = Deadline::after(&clock, ACQUISITION_BUDGET);
    let admin =
        AdministrativeLease::acquire_for_data_dir(data_dir, deadline, &clock, &ThreadWaiter)?;
    let record_path = admin.ownership_record_path();
    let record = read_ownership_record(&record_path)?;
    let held = runtime_lease_is_held(data_dir)?;
    match record {
        Some(OwnershipRead::Current(record)) => {
            let instance = &record.instance;
            let provider = SystemProcessEvidence::default();
            let evidence = provider.observe(instance.process_identity.pid, deadline, &clock)?;
            if held {
                return Err(if instance.mode == CoreMode::UserDaemon {
                    LifecycleError::OwnershipBusy
                } else {
                    LifecycleError::UnsafeOwnership(format!(
                        "data directory is owned by a {} Core instance",
                        instance.mode
                    ))
                });
            }
            if evidence.as_ref().is_some_and(|evidence| {
                evidence.identity == instance.process_identity && evidence.owner == instance.owner
            }) {
                return Err(LifecycleError::UnsafeOwnership(
                    "verified Core process is live without its required runtime lease".to_owned(),
                ));
            }
            if !cleanup_record_if_instance_locked(&admin, instance.instance_id)? {
                return Err(LifecycleError::UnsafeOwnership(
                    "ownership record changed while stale state was being reconciled".to_owned(),
                ));
            }
            control::remove_capability(
                &ownership::capability_path(data_dir, instance.instance_id),
                &instance.owner,
            )?;
        }
        Some(OwnershipRead::Legacy(legacy)) => {
            if held {
                return Err(LifecycleError::UnsafeOwnership(
                    "a runtime lease exists with untrusted legacy PID-only metadata; stop the old instance using its original installation or terminal".to_owned(),
                ));
            }
            if let Some(pid) = legacy.pid
                && SystemProcessEvidence::default()
                    .observe(pid, deadline, &clock)?
                    .is_some()
            {
                return Err(LifecycleError::UnsafeOwnership(
                    "legacy PID-only state may refer to a live or reused process; it was not signaled or removed".to_owned(),
                ));
            }
            if !cleanup_legacy_if_proven_dead_locked(&admin, |pid| {
                Ok(SystemProcessEvidence::default()
                    .observe(pid, deadline, &clock)?
                    .is_none())
            })? && read_ownership_record(&record_path)?.is_some()
            {
                return Err(LifecycleError::UnsafeOwnership(
                    "legacy ownership state could not be safely reconciled".to_owned(),
                ));
            }
        }
        None => {
            if held {
                return Err(LifecycleError::UnsafeOwnership(
                    "a runtime lease is held but its ownership metadata is missing".to_owned(),
                ));
            }
            let legacy_path = admin.legacy_pid_path();
            if read_legacy_pid_file(&legacy_path)?.is_some() {
                let removed = cleanup_legacy_if_proven_dead_locked(&admin, |pid| {
                    Ok(SystemProcessEvidence::default()
                        .observe(pid, deadline, &clock)?
                        .is_none())
                })?;
                if !removed && legacy_path.exists() {
                    return Err(LifecycleError::UnsafeOwnership(
                        "legacy daemon PID state may refer to a live or reused process; it was retained".to_owned(),
                    ));
                }
            }
        }
    }
    Ok(DaemonStartReservation { _admin: admin })
}

/// Read-only synchronous identity and lease observation. No config, database,
/// credential, directory, or exclusive administrative lock is opened.
pub fn observe(data_dir: &Path) -> Result<DaemonObservation, LifecycleError> {
    let clock = SystemMonotonicClock::default();
    let deadline = Deadline::after(&clock, STATUS_BUDGET);
    observe_with(
        data_dir,
        deadline,
        &clock,
        &SystemProcessEvidence::default(),
    )
}

pub fn observe_with(
    data_dir: &Path,
    deadline: Deadline,
    clock: &dyn MonotonicClock,
    processes: &dyn ProcessEvidenceProvider,
) -> Result<DaemonObservation, LifecycleError> {
    if !data_dir.is_absolute() {
        return Ok(DaemonObservation::unknown(
            data_dir,
            "daemon observation requires an absolute data directory",
        ));
    }
    match fs::symlink_metadata(data_dir) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
        Ok(_) => {
            return Ok(DaemonObservation::unknown(
                data_dir,
                "selected data location is redirected or is not a directory",
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(DaemonObservation::stopped(data_dir));
        }
        Err(error) => {
            return Ok(DaemonObservation::unknown(
                data_dir,
                format!("cannot inspect selected data location: {error}"),
            ));
        }
    }
    let canonical_data_dir = match data_dir.canonicalize() {
        Ok(path) => path,
        Err(error) => {
            return Ok(DaemonObservation::unknown(
                data_dir,
                format!("cannot resolve selected data location: {error}"),
            ));
        }
    };
    let record_path = canonical_data_dir
        .join(ownership::OWNERSHIP_DIRECTORY)
        .join("ownership.json");
    let record = match read_ownership_record(&record_path) {
        Ok(record) => record,
        Err(error) => return Ok(DaemonObservation::unknown(data_dir, error.to_string())),
    };
    let held = match runtime_lease_is_held(&canonical_data_dir) {
        Ok(held) => held,
        Err(error) => return Ok(DaemonObservation::unknown(data_dir, error.to_string())),
    };

    match record {
        Some(OwnershipRead::Current(record)) => {
            let instance = &record.instance;
            if instance.data_dir != canonical_data_dir {
                return Ok(DaemonObservation::unknown(
                    data_dir,
                    "ownership metadata names a different canonical data directory",
                ));
            }
            let observed = match processes.observe(instance.process_identity.pid, deadline, clock) {
                Ok(observed) => observed,
                Err(error) => return Ok(DaemonObservation::unknown(data_dir, error.to_string())),
            };
            let Some(observed) = observed else {
                return if held {
                    Ok(DaemonObservation::unknown(
                        data_dir,
                        "runtime lease is held but recorded process identity is not observable",
                    ))
                } else {
                    Ok(DaemonObservation::stopped(data_dir))
                };
            };
            if process::verify_process_identity(&instance.process_identity, &observed).is_err()
                || observed.owner != instance.owner
            {
                return if held {
                    Ok(DaemonObservation::unknown(
                        data_dir,
                        "runtime lease is held but process identity evidence does not match",
                    ))
                } else {
                    // A reused PID is not the recorded instance and is never
                    // reported as the daemon or used as shutdown authority.
                    Ok(DaemonObservation::stopped(data_dir))
                };
            }
            if !held {
                return Ok(DaemonObservation::unknown(
                    data_dir,
                    "recorded process is live but its runtime lease is not held",
                ));
            }
            Ok(DaemonObservation::from_record(&record, true))
        }
        Some(OwnershipRead::Legacy(legacy)) => {
            observe_legacy(data_dir, legacy.pid, held, deadline, clock, processes)
        }
        None => {
            let legacy_path = canonical_data_dir.join(LEGACY_PID_FILE);
            match read_legacy_pid_file(&legacy_path) {
                Ok(Some(pid)) => {
                    observe_legacy(data_dir, Some(pid), held, deadline, clock, processes)
                }
                Ok(None) if held => Ok(DaemonObservation::unknown(
                    data_dir,
                    "runtime lease is held but ownership metadata is missing",
                )),
                Ok(None) => Ok(DaemonObservation::stopped(data_dir)),
                Err(error) => Ok(DaemonObservation::unknown(data_dir, error.to_string())),
            }
        }
    }
}

fn observe_legacy(
    data_dir: &Path,
    pid: Option<u32>,
    held: bool,
    deadline: Deadline,
    clock: &dyn MonotonicClock,
    processes: &dyn ProcessEvidenceProvider,
) -> Result<DaemonObservation, LifecycleError> {
    if held {
        return Ok(DaemonObservation::unknown(
            data_dir,
            "runtime lease is held with legacy PID-only metadata; legacy PIDs are not shutdown authority",
        ));
    }
    let Some(pid) = pid else {
        return Ok(DaemonObservation::unknown(
            data_dir,
            "legacy ownership metadata has no verifiable process identity",
        ));
    };
    match processes.observe(pid, deadline, clock) {
        Ok(None) => Ok(DaemonObservation {
            detail: Some("obsolete legacy PID-only metadata; no process was signaled".to_owned()),
            ..DaemonObservation::stopped(data_dir)
        }),
        Ok(Some(_)) => Ok(DaemonObservation::unknown(
            data_dir,
            "legacy PID-only metadata may refer to a live or reused process; it was not trusted",
        )),
        Err(error) => Ok(DaemonObservation::unknown(data_dir, error.to_string())),
    }
}

/// Authenticates a user-daemon readiness observation under the same status
/// deadline. Any unavailable handshake with a held lease remains unknown.
pub async fn observe_controlled(data_dir: &Path) -> Result<DaemonObservation, LifecycleError> {
    let clock = SystemMonotonicClock::default();
    let deadline = Deadline::after(&clock, STATUS_BUDGET);
    observe_controlled_with(data_dir, deadline, &clock).await
}

/// Authenticates the exact live system-service instance without granting
/// user-daemon stop authority or provisioning any client credentials.
pub async fn probe_system_service(
    data_dir: &Path,
    instance_id: uuid::Uuid,
    deadline: Deadline,
    clock: &dyn MonotonicClock,
) -> Result<(), LifecycleError> {
    let observation = observe_with(data_dir, deadline, clock, &SystemProcessEvidence::default())?;
    if !observation.identity_verified
        || observation.mode != Some(CoreMode::SystemService)
        || observation.instance_id != Some(instance_id)
        || observation.state != ObservedState::Ready
    {
        return Err(LifecycleError::UnverifiedProcess);
    }
    let owner = observation.owner.ok_or(LifecycleError::UnverifiedProcess)?;
    let endpoint = observation
        .control_endpoint
        .as_deref()
        .ok_or(LifecycleError::UnverifiedProcess)?
        .parse()
        .map_err(|_| LifecycleError::UnverifiedProcess)?;
    let endpoint = LoopbackControlEndpoint::new(endpoint)?;
    let capability = OwnerProtectedFileStorage
        .load(&ownership::capability_path(data_dir, instance_id), &owner)?;
    let context = ControlContext {
        instance_id,
        owner,
        mode: CoreMode::SystemService,
    };
    let operation = ControlOperation::Readiness;
    let response = control::request_control(
        endpoint,
        &context,
        &current_owner(deadline, clock)?,
        &capability,
        &ControlOperationRequest {
            operation: &operation,
            arguments: b"ready",
        },
        deadline,
        clock,
    )
    .await?;
    if response != b"ready" {
        return Err(LifecycleError::ServiceStateUnconfirmed);
    }
    Ok(())
}

pub async fn observe_controlled_with(
    data_dir: &Path,
    deadline: Deadline,
    clock: &dyn MonotonicClock,
) -> Result<DaemonObservation, LifecycleError> {
    let mut observation =
        observe_with(data_dir, deadline, clock, &SystemProcessEvidence::default())?;
    if observation.state != ObservedState::Ready || observation.mode != Some(CoreMode::UserDaemon) {
        return Ok(observation);
    }
    let Some(owner) = observation.owner.clone() else {
        return Ok(DaemonObservation::unknown(
            data_dir,
            "verified daemon owner evidence is unavailable",
        ));
    };
    let Some(instance_id) = observation.instance_id else {
        return Ok(DaemonObservation::unknown(
            data_dir,
            "verified daemon instance ID is unavailable",
        ));
    };
    let Some(endpoint) = observation.control_endpoint.as_deref() else {
        return Ok(DaemonObservation::unknown(
            data_dir,
            "ready daemon has no private control endpoint",
        ));
    };
    let endpoint = match endpoint.parse::<std::net::SocketAddr>() {
        Ok(address) => match LoopbackControlEndpoint::new(address) {
            Ok(endpoint) => endpoint,
            Err(error) => {
                return Ok(DaemonObservation::unknown(data_dir, error.to_string()));
            }
        },
        Err(error) => {
            return Ok(DaemonObservation::unknown(
                data_dir,
                format!("invalid private control endpoint: {error}"),
            ));
        }
    };
    let client_owner = current_owner(deadline, clock)?;
    if let Err(error) = control::verify_client_owner(&client_owner, &owner) {
        return Ok(DaemonObservation::unknown(data_dir, error.to_string()));
    }
    let capability_path = ownership::capability_path(data_dir, instance_id);
    let capability = match OwnerProtectedFileStorage.load(&capability_path, &owner) {
        Ok(capability) => capability,
        Err(error) => return Ok(DaemonObservation::unknown(data_dir, error.to_string())),
    };
    let context = ControlContext {
        instance_id,
        owner,
        mode: CoreMode::UserDaemon,
    };
    let operation = ControlOperation::Readiness;
    let request = ControlOperationRequest {
        operation: &operation,
        arguments: b"ready",
    };
    match control::request_control(
        endpoint,
        &context,
        &client_owner,
        &capability,
        &request,
        deadline,
        clock,
    )
    .await
    {
        Ok(result) if result == b"ready" => Ok(observation),
        Ok(_) => {
            observation.state = ObservedState::Unknown;
            observation.detail = Some("Core readiness handshake did not confirm ready".to_owned());
            observation.verified_pid = None;
            observation.identity_verified = false;
            Ok(observation)
        }
        Err(error) => Ok(DaemonObservation::unknown(data_dir, error.to_string())),
    }
}

/// Performs a one-shot authenticated prepared/ready query for a parent-owned
/// start handoff. The caller supplies its original startup deadline so control
/// retries cannot extend the readiness budget.
pub async fn request_readiness_control(
    data_dir: &Path,
    expected_instance_id: uuid::Uuid,
    readiness: &[u8],
    deadline: Deadline,
    clock: &dyn MonotonicClock,
) -> Result<Vec<u8>, LifecycleError> {
    let observation = observe_with(data_dir, deadline, clock, &SystemProcessEvidence::default())?;
    if !observation.identity_verified
        || observation.mode != Some(CoreMode::UserDaemon)
        || observation.instance_id != Some(expected_instance_id)
    {
        return Err(LifecycleError::UnverifiedProcess);
    }
    let owner = observation.owner.ok_or(LifecycleError::UnverifiedProcess)?;
    let endpoint = observation
        .control_endpoint
        .as_deref()
        .ok_or(LifecycleError::UnverifiedProcess)?
        .parse::<std::net::SocketAddr>()
        .map_err(|_| LifecycleError::UnsafeOwnership("invalid control endpoint".to_owned()))?;
    let endpoint = LoopbackControlEndpoint::new(endpoint)?;
    let client_owner = current_owner(deadline, clock)?;
    control::verify_client_owner(&client_owner, &owner)?;
    let capability = OwnerProtectedFileStorage.load(
        &ownership::capability_path(data_dir, expected_instance_id),
        &owner,
    )?;
    let context = ControlContext {
        instance_id: expected_instance_id,
        owner,
        mode: CoreMode::UserDaemon,
    };
    let operation = ControlOperation::Readiness;
    control::request_control(
        endpoint,
        &context,
        &client_owner,
        &capability,
        &ControlOperationRequest {
            operation: &operation,
            arguments: readiness,
        },
        deadline,
        clock,
    )
    .await
}

/// Requests graceful shutdown only after current-user, mode, instance, process,
/// lease, and capability verification; success additionally requires exit.
pub async fn stop(data_dir: &Path) -> Result<LifecycleOperationResult, LifecycleError> {
    let clock = SystemMonotonicClock::default();
    let dispatch_deadline = Deadline::after(&clock, STOP_DISPATCH_BUDGET);
    let observation = observe_with(
        data_dir,
        dispatch_deadline,
        &clock,
        &SystemProcessEvidence::default(),
    )?;
    if observation.state == ObservedState::Stopped {
        cleanup_obsolete(data_dir, &observation)?;
        return Ok(noop_result(data_dir));
    }
    if observation.state == ObservedState::Unknown {
        return Err(LifecycleError::UnsafeOwnership(
            observation
                .detail
                .unwrap_or_else(|| "daemon ownership is unknown".to_owned()),
        ));
    }
    if observation.mode != Some(CoreMode::UserDaemon) {
        return Err(LifecycleError::ModeNotControllable(
            observation
                .mode
                .map_or_else(|| "unknown".to_owned(), |mode| mode.to_string()),
        ));
    }
    let (Some(instance_id), Some(owner), Some(endpoint), Some(expected_process)) = (
        observation.instance_id,
        observation.owner.clone(),
        observation.control_endpoint.as_deref(),
        observation.process_identity.as_ref(),
    ) else {
        return Err(LifecycleError::UnverifiedProcess);
    };
    let client_owner = current_owner(dispatch_deadline, &clock)?;
    control::verify_client_owner(&client_owner, &owner)?;
    let observed_process = SystemProcessEvidence::default()
        .observe(expected_process.pid, dispatch_deadline, &clock)?
        .ok_or(LifecycleError::UnverifiedProcess)?;
    process::verify_process_identity(expected_process, &observed_process)?;
    if observed_process.owner != owner {
        return Err(LifecycleError::WrongOwner);
    }
    let address = endpoint
        .parse::<std::net::SocketAddr>()
        .map_err(|_| LifecycleError::UnsafeOwnership("invalid control endpoint".to_owned()))?;
    let endpoint = LoopbackControlEndpoint::new(address)?;
    let capability_path = ownership::capability_path(data_dir, instance_id);
    let capability = OwnerProtectedFileStorage.load(&capability_path, &owner)?;
    let context = ControlContext {
        instance_id,
        owner,
        mode: CoreMode::UserDaemon,
    };
    let operation = ControlOperation::GracefulStop;
    let response = control::request_control(
        endpoint,
        &context,
        &client_owner,
        &capability,
        &ControlOperationRequest {
            operation: &operation,
            arguments: b"",
        },
        dispatch_deadline,
        &clock,
    )
    .await?;
    if response != b"accepted" {
        return Err(LifecycleError::UnsafeOwnership(
            "daemon did not acknowledge graceful shutdown".to_owned(),
        ));
    }

    let exit_deadline = Deadline::after(&clock, CORE_GRACE_BUDGET);
    loop {
        let held = runtime_lease_is_held(data_dir)?;
        let current =
            SystemProcessEvidence::default().observe(expected_process.pid, exit_deadline, &clock);
        if !held && process_exit_is_confirmed(current, expected_process, &context.owner)? {
            cleanup_obsolete(data_dir, &observation)?;
            let mut result = LifecycleOperationResult::confirmed(
                "daemon stop",
                "user_daemon",
                ObservedState::Stopped,
                ObservedState::Stopped,
            )?;
            result.verified_pid = Some(expected_process.pid);
            result.data_dir = Some(data_dir.to_path_buf());
            result.diagnostic_location = observation.diagnostic_location;
            return Ok(result);
        }
        let remaining = exit_deadline.remaining(&clock);
        if remaining.is_zero() {
            return Err(LifecycleError::DeadlineExpired {
                phase: "daemon graceful exit observation",
            });
        }
        tokio::time::sleep(remaining.min(CONTROL_RETRY_INTERVAL)).await;
    }
}

fn process_exit_is_confirmed(
    evidence: Result<Option<process::ProcessEvidence>, LifecycleError>,
    expected: &ownership::ProcessIdentity,
    owner: &OwnerIdentity,
) -> Result<bool, LifecycleError> {
    match evidence {
        // Windows can still enumerate an exiting process after its executable or
        // owner becomes unavailable. This is not proof of exit. Keep polling
        // under the existing grace deadline, without weakening pre-stop checks.
        Err(LifecycleError::UnverifiedProcess) => Ok(false),
        other => {
            Ok(!other?
                .is_some_and(|current| current.identity == *expected && current.owner == *owner))
        }
    }
}

fn cleanup_obsolete(data_dir: &Path, expected: &DaemonObservation) -> Result<(), LifecycleError> {
    if !data_dir.is_dir() {
        return Ok(());
    }
    let ownership_dir = data_dir.join(ownership::OWNERSHIP_DIRECTORY);
    let ownership_record = ownership_dir.join("ownership.json");
    let runtime_lock = ownership_dir.join(ownership::RUNTIME_LOCK_FILE);
    let legacy_pid = data_dir.join(LEGACY_PID_FILE);
    if expected.instance_id.is_none()
        && !path_exists(&ownership_record)?
        && !path_exists(&runtime_lock)?
        && !path_exists(&legacy_pid)?
    {
        // A confirmed absence is already a no-op. Do not create lifecycle
        // directories or lock files merely to repeat daemon stop.
        return Ok(());
    }
    let clock = SystemMonotonicClock::default();
    let deadline = Deadline::after(&clock, ACQUISITION_BUDGET);
    let admin =
        AdministrativeLease::acquire_for_data_dir(data_dir, deadline, &clock, &ThreadWaiter)?;
    if runtime_lease_is_held(data_dir)? {
        return Err(LifecycleError::OwnershipBusy);
    }
    if let Some(instance_id) = expected.instance_id {
        let path = admin.ownership_record_path();
        match read_ownership_record(&path)? {
            Some(OwnershipRead::Current(record)) if record.instance.instance_id == instance_id => {
                let provider = SystemProcessEvidence::default();
                let evidence =
                    provider.observe(record.instance.process_identity.pid, deadline, &clock)?;
                if evidence.as_ref().is_some_and(|evidence| {
                    evidence.identity == record.instance.process_identity
                        && evidence.owner == record.instance.owner
                }) {
                    return Err(LifecycleError::UnsafeOwnership(
                        "recorded daemon process remains live; stale metadata was retained"
                            .to_owned(),
                    ));
                }
                cleanup_record_if_instance_locked(&admin, instance_id)?;
                control::remove_capability(
                    &ownership::capability_path(data_dir, instance_id),
                    &record.instance.owner,
                )?;
            }
            Some(OwnershipRead::Legacy(legacy)) => {
                cleanup_legacy_if_proven_dead_locked(&admin, |pid| {
                    Ok(SystemProcessEvidence::default()
                        .observe(pid, deadline, &clock)?
                        .is_none())
                })?;
                if legacy.pid.is_some() && path.exists() {
                    return Err(LifecycleError::UnsafeOwnership(
                        "legacy ownership data was not safely removable".to_owned(),
                    ));
                }
            }
            None => {}
            Some(OwnershipRead::Current(_)) => {
                return Err(LifecycleError::UnsafeOwnership(
                    "ownership metadata was replaced during daemon cleanup".to_owned(),
                ));
            }
        }
    } else {
        let legacy_path = admin.legacy_pid_path();
        if read_legacy_pid_file(&legacy_path)?.is_some() {
            cleanup_legacy_if_proven_dead_locked(&admin, |pid| {
                Ok(SystemProcessEvidence::default()
                    .observe(pid, deadline, &clock)?
                    .is_none())
            })?;
        }
    }
    Ok(())
}

fn path_exists(path: &Path) -> Result<bool, LifecycleError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn current_owner(
    deadline: Deadline,
    clock: &dyn MonotonicClock,
) -> Result<OwnerIdentity, LifecycleError> {
    SystemProcessEvidence::default()
        .observe(std::process::id(), deadline, clock)?
        .map(|evidence| evidence.owner)
        .ok_or(LifecycleError::UnverifiedProcess)
}

fn phase_state(phase: InstancePhase) -> ObservedState {
    match phase {
        InstancePhase::Starting => ObservedState::Starting,
        InstancePhase::Ready => ObservedState::Ready,
        InstancePhase::Stopping => ObservedState::Stopping,
        InstancePhase::Failed => ObservedState::Failed,
    }
}

fn noop_result(data_dir: &Path) -> LifecycleOperationResult {
    LifecycleOperationResult {
        action: "daemon stop".to_owned(),
        scope: "user_daemon".to_owned(),
        outcome: OperationOutcome::Noop,
        observed_state: ObservedState::Stopped,
        verified_pid: None,
        data_dir: Some(data_dir.to_path_buf()),
        diagnostic_location: None,
        changed_resources: Vec::new(),
        remaining_resources: Vec::new(),
        recovery_guidance: Vec::new(),
        elapsed_application_wait: Duration::ZERO,
        platform_completion_limitations: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };

    use super::*;
    use crate::lifecycle::ownership::CoreInstance;

    #[test]
    fn exiting_process_with_incomplete_identity_is_pending_not_success_or_immediate_failure() {
        let expected = ownership::ProcessIdentity {
            pid: 42,
            start_identity: "original-start".into(),
            executable: std::env::current_exe().unwrap(),
        };
        let owner = OwnerIdentity::Other("test-owner".into());
        assert!(
            !process_exit_is_confirmed(Err(LifecycleError::UnverifiedProcess), &expected, &owner)
                .unwrap()
        );
        assert!(process_exit_is_confirmed(Ok(None), &expected, &owner).unwrap());
        assert!(
            process_exit_is_confirmed(
                Err(LifecycleError::DeadlineExpired { phase: "exit" }),
                &expected,
                &owner
            )
            .is_err()
        );
    }

    #[derive(Clone, Default)]
    struct FakeClock(Arc<AtomicU64>);

    impl MonotonicClock for FakeClock {
        fn now(&self) -> Duration {
            Duration::from_nanos(self.0.load(Ordering::SeqCst))
        }
    }

    #[test]
    fn missing_status_path_is_not_created() {
        let path =
            std::env::temp_dir().join(format!("kakune-daemon-status-{}", uuid::Uuid::new_v4()));
        let result = observe(&path).unwrap();
        assert_eq!(result.state, ObservedState::Stopped);
        assert!(!path.exists());
    }

    #[test]
    fn status_deadline_is_passed_through_process_observation() {
        let clock = FakeClock::default();
        let deadline = Deadline::after(&clock, Duration::ZERO);
        let path =
            std::env::temp_dir().join(format!("kakune-daemon-deadline-{}", uuid::Uuid::new_v4()));
        let result =
            observe_with(&path, deadline, &clock, &SystemProcessEvidence::default()).unwrap();
        assert_eq!(result.state, ObservedState::Stopped);
    }

    #[test]
    fn ownership_record_state_does_not_claim_verified_pid_when_identity_is_unverified() {
        let path =
            std::env::temp_dir().join(format!("kakune-daemon-record-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&path).unwrap();
        let instance = CoreInstance {
            instance_id: uuid::Uuid::new_v4(),
            mode: CoreMode::UserDaemon,
            owner: OwnerIdentity::Other("test-owner".to_owned()),
            process_identity: ownership::ProcessIdentity {
                pid: 1,
                start_identity: "old-start".to_owned(),
                executable: std::env::current_exe().unwrap(),
            },
            data_dir: path.canonicalize().unwrap(),
            listen_address: None,
            phase: InstancePhase::Ready,
            diagnostic_location: None,
            control_endpoint: None,
        };
        let record = RuntimeOwnershipRecord::new(instance).unwrap();
        let observation = DaemonObservation::from_record(&record, false);
        assert_eq!(observation.state, ObservedState::Ready);
        assert_eq!(observation.verified_pid, None);
        let _ = fs::remove_dir_all(path);
    }
}
