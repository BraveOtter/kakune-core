mod support;

use std::{
    fs,
    path::PathBuf,
    sync::{
        Arc, Barrier,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::Duration,
};

use kakune_core::lifecycle::{
    ACQUISITION_BUDGET, CORE_GRACE_BUDGET, Deadline, DeadlineBook, DeadlineWaiter, HELPER_BUDGET,
    INSTALL_BUDGET, INSTALL_COMMAND_CEILING, LifecycleBudget, LifecycleError,
    LifecycleOperationResult, MonotonicClock, ObservedState, PREFLIGHT_BUDGET, READINESS_BUDGET,
    REMOVAL_BUDGET, START_COMMAND_CEILING, STATUS_BUDGET, STOP_COMMAND_CEILING,
    STOP_DISPATCH_BUDGET, UNINSTALL_COMMAND_CEILING,
    control::{
        Capability, ChallengeRegistry, ControlChallenge, ControlContext, ControlOperation,
        OwnerProtectedFileStorage, RandomSource, client_request_proof, create_capability,
        server_proof, server_response_proof, verify_client_owner, verify_proof,
    },
    execution::{AdmissionCoordinator, TrackedExecutionState},
    ownership::{
        AdministrativeLease, CoreInstance, InstancePhase, OwnerIdentity, ProcessIdentity,
        RuntimeLease, RuntimeOwnershipRecord, cleanup_legacy_if_proven_dead_locked,
        cleanup_record_if_instance_locked, publish_record_locked, read_legacy_pid_file,
        read_ownership_record,
    },
    process::{BoundedCapture, ProcessEvidenceProvider},
};
#[cfg(windows)]
use kakune_core::lifecycle::{SystemMonotonicClock, process::SystemProcessEvidence};
use support::lifecycle::{
    LifecycleFixture, LifecycleTestAdapters, ProcessEvidence as FixtureProcessEvidence,
    ProcessEvidenceAdapter as FixtureProcessEvidenceAdapter,
};
use uuid::Uuid;

#[derive(Clone, Default)]
struct FakeClock(Arc<AtomicU64>);

impl FakeClock {
    fn advance(&self, duration: Duration) {
        self.0
            .fetch_add(duration.as_nanos() as u64, Ordering::SeqCst);
    }
}

impl MonotonicClock for FakeClock {
    fn now(&self) -> Duration {
        Duration::from_nanos(self.0.load(Ordering::SeqCst))
    }
}

struct AdvancingWaiter(FakeClock);

impl DeadlineWaiter for AdvancingWaiter {
    fn wait(&self, duration: Duration) {
        self.0.advance(duration);
    }
}

#[cfg(unix)]
fn current_owner() -> OwnerIdentity {
    OwnerIdentity::UnixUid(rustix::process::geteuid().as_raw())
}

#[cfg(windows)]
fn current_owner() -> OwnerIdentity {
    let clock = SystemMonotonicClock::default();
    SystemProcessEvidence::default()
        .observe(
            std::process::id(),
            Deadline::after(&clock, Duration::from_secs(5)),
            &clock,
        )
        .expect("the current test process identity should be observable")
        .expect("the current test process should exist")
        .owner
}

#[cfg(not(any(unix, windows)))]
fn current_owner() -> OwnerIdentity {
    OwnerIdentity::Other("test-owner".to_owned())
}

fn make_record(data_dir: &std::path::Path, instance_id: Uuid) -> RuntimeOwnershipRecord {
    RuntimeOwnershipRecord::new(CoreInstance {
        instance_id,
        mode: kakune_core::lifecycle::CoreMode::Foreground,
        owner: current_owner(),
        process_identity: ProcessIdentity {
            pid: 934,
            start_identity: "verified-start-identity".to_owned(),
            executable: if cfg!(windows) {
                PathBuf::from("C:\\Kakune\\kakune.exe")
            } else {
                PathBuf::from("/opt/kakune/kakune")
            },
        },
        data_dir: data_dir.to_path_buf(),
        listen_address: Some("http://127.0.0.1:18702".to_owned()),
        phase: InstancePhase::Starting,
        diagnostic_location: Some("fixture log".to_owned()),
        control_endpoint: None,
    })
    .expect("test ownership record should be valid")
}

#[test]
fn phase_deadlines_share_budgets_and_never_renew_nested_work() {
    assert_eq!(READINESS_BUDGET, Duration::from_secs(30));
    assert_eq!(CORE_GRACE_BUDGET, Duration::from_secs(30));
    assert_eq!(INSTALL_BUDGET, Duration::from_secs(30));
    assert_eq!(REMOVAL_BUDGET, Duration::from_secs(30));
    assert_eq!(ACQUISITION_BUDGET, Duration::from_secs(10));
    assert_eq!(STATUS_BUDGET, Duration::from_secs(10));
    assert_eq!(PREFLIGHT_BUDGET, Duration::from_secs(10));
    assert_eq!(HELPER_BUDGET, Duration::from_secs(10));
    assert_eq!(STOP_DISPATCH_BUDGET, Duration::from_secs(10));
    assert_eq!(INSTALL_COMMAND_CEILING, Duration::from_secs(50));
    assert_eq!(START_COMMAND_CEILING, Duration::from_secs(50));
    assert_eq!(STOP_COMMAND_CEILING, Duration::from_secs(60));
    assert_eq!(UNINSTALL_COMMAND_CEILING, Duration::from_secs(90));
    assert_eq!(
        ACQUISITION_BUDGET + PREFLIGHT_BUDGET + INSTALL_BUDGET,
        INSTALL_COMMAND_CEILING
    );
    assert_eq!(
        ACQUISITION_BUDGET + PREFLIGHT_BUDGET + READINESS_BUDGET,
        START_COMMAND_CEILING
    );
    assert_eq!(
        ACQUISITION_BUDGET + PREFLIGHT_BUDGET + STOP_DISPATCH_BUDGET + CORE_GRACE_BUDGET,
        STOP_COMMAND_CEILING
    );
    assert_eq!(
        ACQUISITION_BUDGET
            + PREFLIGHT_BUDGET
            + STOP_DISPATCH_BUDGET
            + CORE_GRACE_BUDGET
            + REMOVAL_BUDGET,
        UNINSTALL_COMMAND_CEILING
    );

    let clock = FakeClock::default();
    let phase = Deadline::after(&clock, CORE_GRACE_BUDGET);
    clock.advance(Duration::from_secs(7));
    let helper = phase.cap(&clock, HELPER_BUDGET);
    assert_eq!(helper.remaining(&clock), Duration::from_secs(10));
    clock.advance(Duration::from_secs(3));
    assert_eq!(helper.remaining(&clock), Duration::from_secs(7));
    assert_eq!(phase.remaining(&clock), Duration::from_secs(20));

    let command = Deadline::after(&clock, Duration::from_secs(1));
    let mut deadlines = DeadlineBook::default();
    let grace = deadlines.start_once(LifecycleBudget::CoreGrace, &clock, Some(command));
    assert_eq!(grace.remaining(&clock), CORE_GRACE_BUDGET);
    clock.advance(CORE_GRACE_BUDGET);
    assert!(grace.is_expired(&clock));
    assert_eq!(
        deadlines
            .start_once(LifecycleBudget::CoreGrace, &clock, Some(command))
            .endpoint(),
        grace.endpoint(),
        "re-entering a phase must return its first absolute deadline"
    );

    for phase in [
        LifecycleBudget::Status,
        LifecycleBudget::Preflight,
        LifecycleBudget::Helper,
    ] {
        let command = Deadline::after(&clock, Duration::from_secs(50));
        let first = deadlines.start_once(phase, &clock, Some(command));
        let first_endpoint = first.endpoint();
        clock.advance(Duration::from_secs(1));
        let retried = deadlines.start_once(phase, &clock, Some(command));
        assert_eq!(retried.endpoint(), first_endpoint);
    }

    assert!(
        LifecycleOperationResult::confirmed(
            "start",
            "daemon",
            ObservedState::Ready,
            ObservedState::Starting
        )
        .is_err()
    );
    assert!(
        LifecycleOperationResult::confirmed(
            "start",
            "daemon",
            ObservedState::Ready,
            ObservedState::Ready
        )
        .is_ok()
    );
}

#[test]
fn ownership_lease_serializes_aliases_and_does_not_unlink_stable_locks() {
    let root = std::env::temp_dir().join(format!("kakune-foundation-{}", Uuid::new_v4()));
    fs::create_dir_all(&root).unwrap();
    let clock = FakeClock::default();
    let waiter = AdvancingWaiter(clock.clone());
    let owner = RuntimeLease::acquire(&root, Uuid::new_v4(), &clock, &waiter).unwrap();
    let contender = RuntimeLease::acquire(&root.join("."), Uuid::new_v4(), &clock, &waiter);
    assert!(matches!(contender, Err(LifecycleError::OwnershipBusy)));
    let stable_path = root.join(".kakune").join("runtime.lock");
    drop(owner);
    assert!(stable_path.is_file());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn atomic_publication_failure_keeps_replacement_and_owned_temp_files_are_cleaned() {
    let fixture = LifecycleFixture::new().unwrap();
    let clock = FakeClock::default();
    let waiter = AdvancingWaiter(clock.clone());
    let lease =
        RuntimeLease::acquire(&fixture.data_dir(), Uuid::new_v4(), &clock, &waiter).unwrap();
    let admin = AdministrativeLease::acquire_for_data_dir(
        lease.canonical_data_dir(),
        Deadline::after(&clock, ACQUISITION_BUDGET),
        &clock,
        &waiter,
    )
    .unwrap();
    let record_path = admin.ownership_record_path();
    fs::create_dir(&record_path).unwrap();
    let record = make_record(lease.canonical_data_dir(), lease.instance_id());
    assert!(publish_record_locked(&admin, &record).is_err());
    assert!(record_path.is_dir());
    let entries = fs::read_dir(record_path.parent().unwrap())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert!(
        !entries
            .iter()
            .any(|entry| entry.starts_with("ownership-") && entry.ends_with(".tmp"))
    );
    fs::remove_dir(record_path).unwrap();
}

#[test]
fn instance_checked_cleanup_preserves_a_replacement_and_legacy_pids_are_not_authority() {
    let fixture = LifecycleFixture::new().unwrap();
    let clock = FakeClock::default();
    let waiter = AdvancingWaiter(clock.clone());
    let original_id = Uuid::new_v4();
    let lease = RuntimeLease::acquire(&fixture.data_dir(), original_id, &clock, &waiter).unwrap();
    let admin = AdministrativeLease::acquire_for_data_dir(
        lease.canonical_data_dir(),
        Deadline::after(&clock, ACQUISITION_BUDGET),
        &clock,
        &waiter,
    )
    .unwrap();
    let replacement_id = Uuid::new_v4();
    publish_record_locked(
        &admin,
        &make_record(lease.canonical_data_dir(), replacement_id),
    )
    .unwrap();
    assert!(!cleanup_record_if_instance_locked(&admin, original_id).unwrap());
    assert!(matches!(
        read_ownership_record(&admin.ownership_record_path()).unwrap(),
        Some(kakune_core::lifecycle::ownership::OwnershipRead::Current(record))
            if record.instance.instance_id == replacement_id
    ));

    let legacy_path = admin.legacy_pid_path();
    fs::write(&legacy_path, "934\n").unwrap();
    assert_eq!(read_legacy_pid_file(&legacy_path).unwrap(), Some(934));
    let adapters = LifecycleTestAdapters::new(fixture.resources().clone());
    adapters.processes.insert(FixtureProcessEvidence {
        pid: 934,
        owner: Some("some-owner".to_owned()),
        executable: None,
        start_identity: None,
        alive: true,
    });
    assert!(
        !cleanup_legacy_if_proven_dead_locked(&admin, |pid| {
            Ok(adapters.processes.observe(pid).is_none())
        })
        .unwrap()
    );
    assert!(
        legacy_path.exists(),
        "a live or uncertain PID is never cleaned"
    );
    adapters.processes.remove(934);
    assert!(
        cleanup_legacy_if_proven_dead_locked(&admin, |pid| {
            Ok(adapters.processes.observe(pid).is_none())
        })
        .unwrap()
    );
    assert!(!legacy_path.exists());
    assert!(adapters.control.requests().is_empty());
}

#[test]
fn process_evidence_with_missing_owner_executable_or_start_identity_fails_closed() {
    struct InaccessibleProcess;
    impl ProcessEvidenceProvider for InaccessibleProcess {
        fn observe(
            &self,
            _pid: u32,
            _deadline: Deadline,
            _clock: &dyn MonotonicClock,
        ) -> Result<Option<kakune_core::lifecycle::process::ProcessEvidence>, LifecycleError>
        {
            Err(LifecycleError::UnverifiedProcess)
        }
    }
    let clock = FakeClock::default();
    assert!(matches!(
        InaccessibleProcess.observe(934, Deadline::after(&clock, HELPER_BUDGET), &clock),
        Err(LifecycleError::UnverifiedProcess)
    ));
}

#[test]
fn capability_protocol_rejects_wrong_account_spoof_replay_and_transcript_substitution() {
    let owner = current_owner();
    let wrong_owner = match owner.clone() {
        OwnerIdentity::UnixUid(uid) => OwnerIdentity::UnixUid(uid.saturating_add(1)),
        OwnerIdentity::WindowsSid(_) => OwnerIdentity::WindowsSid("S-1-5-19".to_owned()),
        OwnerIdentity::Other(_) => OwnerIdentity::Other("other-account".to_owned()),
    };
    assert!(matches!(
        verify_client_owner(&wrong_owner, &owner),
        Err(LifecycleError::WrongOwner)
    ));

    let capability = Capability::from_bytes([0x5a; 32]);
    let challenge = ControlChallenge {
        protocol_version: 1,
        context: ControlContext {
            instance_id: Uuid::new_v4(),
            owner: owner.clone(),
            mode: kakune_core::lifecycle::CoreMode::UserDaemon,
        },
        client_nonce: [0x11; 32],
        server_nonce: [0x22; 32],
    };
    let operation = ControlOperation::GracefulStop;
    let arguments = b"close admission";
    let valid_request =
        client_request_proof(&capability, &challenge, &operation, arguments).unwrap();

    // A claimed/spoofed owner is not trusted: changing it invalidates the proof.
    let mut spoofed_challenge = challenge.clone();
    spoofed_challenge.context.owner = wrong_owner;
    let spoofed =
        client_request_proof(&capability, &spoofed_challenge, &operation, arguments).unwrap();
    assert!(verify_proof(&valid_request, &spoofed).is_err());

    let fake_server = Capability::from_bytes([0xa5; 32]);
    let trusted_server_proof = server_proof(&capability, &challenge).unwrap();
    let fake_server_proof = server_proof(&fake_server, &challenge).unwrap();
    assert!(verify_proof(&trusted_server_proof, &fake_server_proof).is_err());

    let altered_operation = client_request_proof(
        &capability,
        &challenge,
        &ControlOperation::Readiness,
        arguments,
    )
    .unwrap();
    assert!(verify_proof(&valid_request, &altered_operation).is_err());
    let trusted_response = server_response_proof(
        &capability,
        &challenge,
        &operation,
        arguments,
        b"acknowledged",
    )
    .unwrap();
    let altered_result =
        server_response_proof(&capability, &challenge, &operation, arguments, b"failed").unwrap();
    assert!(verify_proof(&trusted_response, &altered_result).is_err());

    let registry = ChallengeRegistry::default();
    let clock = FakeClock::default();
    let deadline = Deadline::after(&clock, HELPER_BUDGET);
    let mut first_connection = registry
        .open_session(challenge.clone(), deadline, &clock)
        .unwrap();
    first_connection
        .verify_request(&capability, &operation, arguments, &valid_request, &clock)
        .unwrap();
    assert!(matches!(
        first_connection.verify_request(&capability, &operation, arguments, &valid_request, &clock),
        Err(LifecycleError::ChallengeConsumed)
    ));
    assert!(matches!(
        registry.open_session(challenge, deadline, &clock),
        Err(LifecycleError::AuthenticationFailed)
    ));
}

struct FailingRandom;
impl RandomSource for FailingRandom {
    fn fill(&self, _destination: &mut [u8]) -> Result<(), LifecycleError> {
        Err(LifecycleError::RandomnessFailed)
    }
}

#[test]
fn failed_randomness_and_unsafe_capability_publication_never_fall_back() {
    let path = std::env::temp_dir().join(format!("kakune-capability-{}.key", Uuid::new_v4()));
    #[cfg(unix)]
    let (path, insecure_parent) = {
        use std::os::unix::fs::PermissionsExt;

        let parent = path.with_extension("insecure-parent");
        fs::create_dir(&parent).expect("test-owned insecure parent should be created");
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o770))
            .expect("the fixture should explicitly expose unsafe group-write permissions");
        (parent.join("capability.key"), parent)
    };
    let failing_random = FailingRandom;
    // Injected failure does not create a capability file or print/store bytes.
    assert!(matches!(
        create_capability(
            &path,
            &current_owner(),
            &failing_random,
            &OwnerProtectedFileStorage
        ),
        Err(LifecycleError::RandomnessFailed)
    ));
    assert!(!path.exists());

    let capability = Capability::from_bytes([0x7d; 32]);
    let result = kakune_core::lifecycle::control::CapabilityStorage::store_new(
        &OwnerProtectedFileStorage,
        &path,
        &capability,
        &current_owner(),
    );
    #[cfg(windows)]
    assert!(matches!(result, Err(LifecycleError::InsecureProtection(_))));
    #[cfg(unix)]
    assert!(
        result.is_err(),
        "explicitly group-writable fixture ancestors must fail closed regardless of the host umask"
    );
    assert!(!path.exists());
    #[cfg(unix)]
    fs::remove_dir(insecure_parent).expect("empty test-owned insecure parent should be removed");
}

#[test]
fn helper_output_is_capped_and_admission_close_accounts_for_every_accepted_task() {
    let mut capture = BoundedCapture::default();
    let bytes = vec![b'x'; kakune_core::lifecycle::MAX_HELPER_DIAGNOSTIC_BYTES + 1];
    assert!(capture.append(&bytes));
    assert!(capture.was_truncated());
    assert_eq!(
        capture.as_bytes().len(),
        kakune_core::lifecycle::MAX_HELPER_DIAGNOSTIC_BYTES
    );

    let admission = AdmissionCoordinator::new();
    let workers = 12;
    let barrier = Arc::new(Barrier::new(workers + 1));
    let mut joins = Vec::with_capacity(workers);
    for _ in 0..workers {
        let coordinator = admission.clone();
        let barrier = Arc::clone(&barrier);
        joins.push(thread::spawn(move || {
            barrier.wait();
            coordinator.admit(TrackedExecutionState::Preparation)
        }));
    }
    barrier.wait();
    admission.close_admission().unwrap();
    let permits = joins
        .into_iter()
        .map(|join| join.join().unwrap())
        .collect::<Vec<_>>();
    let accepted = permits.iter().filter(|permit| permit.is_ok()).count();
    assert_eq!(admission.snapshot().unwrap().preparation, accepted);
    assert!(admission.admit(TrackedExecutionState::Queued).is_err());
    drop(permits);
    assert!(admission.snapshot().unwrap().is_drained());
}

#[test]
fn test_owned_fixture_adapters_remain_separate_from_real_os_resources() {
    let fixture = LifecycleFixture::new().unwrap();
    let adapters = LifecycleTestAdapters::new(fixture.resources().clone());
    assert!(adapters.processes.observe(77).is_none());
    assert!(adapters.control.requests().is_empty());
    assert!(fixture.resources().is_empty());
}
