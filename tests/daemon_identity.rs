mod support;

use std::{fs, time::Duration};

use kakune_core::lifecycle::{
    CORE_GRACE_BUDGET, CoreMode, Deadline, DeadlineBook, LifecycleBudget, LifecycleError,
    MonotonicClock, READINESS_BUDGET, SystemMonotonicClock,
    control::{
        CONTROL_NONCE_BYTES, CONTROL_PROTOCOL_VERSION, Capability, ChallengeRegistry,
        ControlChallenge, ControlContext, ControlOperation, client_request_proof,
        verify_client_owner,
    },
    ownership::{
        AdministrativeLease, CoreInstance, InstancePhase, OWNERSHIP_SCHEMA_VERSION, OwnerIdentity,
        OwnershipRead, ProcessIdentity, RuntimeLease, RuntimeOwnershipRecord,
        cleanup_legacy_if_proven_dead_locked, cleanup_record_if_instance_locked,
        publish_record_locked, read_ownership_record,
    },
    process::{ProcessEvidence, verify_process_identity},
};
use support::lifecycle::{FakeClock, LifecycleFixture};
use uuid::Uuid;

impl MonotonicClock for FakeClock {
    fn now(&self) -> Duration {
        support::lifecycle::MonotonicClock::now(self)
    }
}

fn fixture() -> LifecycleFixture {
    LifecycleFixture::new().expect("identity fixture should be created")
}

fn observed_owner() -> OwnerIdentity {
    use kakune_core::lifecycle::process::ProcessEvidenceProvider;

    let clock = SystemMonotonicClock::default();
    kakune_core::lifecycle::process::SystemProcessEvidence::default()
        .observe(
            std::process::id(),
            Deadline::after(&clock, Duration::from_secs(5)),
            &clock,
        )
        .expect("the current test process should be inspectable")
        .expect("the current test process should exist")
        .owner
}

fn current_process_identity() -> ProcessIdentity {
    use kakune_core::lifecycle::process::ProcessEvidenceProvider;

    let clock = SystemMonotonicClock::default();
    kakune_core::lifecycle::process::SystemProcessEvidence::default()
        .observe(
            std::process::id(),
            Deadline::after(&clock, Duration::from_secs(5)),
            &clock,
        )
        .expect("the current test process should be inspectable")
        .expect("the current test process should exist")
        .identity
}

#[cfg(target_os = "linux")]
#[test]
fn linux_process_identity_uses_boot_id_and_raw_start_ticks_not_wall_clock() {
    let boot_id = fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .expect("Linux boot ID should be readable");
    let stat =
        fs::read_to_string("/proc/self/stat").expect("current process stat should be readable");
    let (_, fields) = stat.rsplit_once(')').expect("stat should contain comm");
    let start_ticks = fields
        .split_ascii_whitespace()
        .nth(19)
        .expect("stat should contain start ticks");
    assert_eq!(
        current_process_identity().start_identity,
        format!("linux:{}:{start_ticks}", boot_id.trim()),
        "process identity must not include the mutable wall-clock boot timestamp"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn linux_wall_clock_legacy_identity_is_rejected_without_a_tolerance_fallback() {
    let current = current_process_identity();
    let mut legacy = current.clone();
    legacy.start_identity = "1792611499".to_owned();
    let observed = ProcessEvidence {
        identity: current,
        owner: observed_owner(),
    };
    assert!(matches!(
        verify_process_identity(&legacy, &observed),
        Err(LifecycleError::UnverifiedProcess)
    ));
}

fn instance(data_dir: &std::path::Path, mode: CoreMode) -> CoreInstance {
    CoreInstance {
        instance_id: Uuid::new_v4(),
        mode,
        owner: observed_owner(),
        process_identity: current_process_identity(),
        data_dir: data_dir
            .canonicalize()
            .expect("fixture data directory should canonicalize"),
        listen_address: Some("http://127.0.0.1:18702".to_owned()),
        phase: InstancePhase::Ready,
        diagnostic_location: None,
        control_endpoint: Some("127.0.0.1:18703".to_owned()),
    }
}

#[test]
fn reused_pid_with_different_start_or_executable_is_not_same_process_identity() {
    let original = current_process_identity();
    let owner = observed_owner();
    let matching = ProcessEvidence {
        identity: original.clone(),
        owner: owner.clone(),
    };
    assert!(verify_process_identity(&original, &matching).is_ok());

    let mut reused_pid = original.clone();
    reused_pid.start_identity.push_str("-reused");
    assert_eq!(reused_pid.pid, original.pid);
    assert_ne!(reused_pid, original);
    assert!(matches!(
        verify_process_identity(
            &original,
            &ProcessEvidence {
                identity: reused_pid,
                owner: owner.clone(),
            }
        ),
        Err(LifecycleError::UnverifiedProcess)
    ));

    let mut different_executable = original.clone();
    different_executable.executable = original.executable.with_file_name("unrelated-process.exe");
    assert_eq!(different_executable.pid, original.pid);
    assert_ne!(different_executable, original);
    assert!(matches!(
        verify_process_identity(
            &original,
            &ProcessEvidence {
                identity: different_executable,
                owner,
            }
        ),
        Err(LifecycleError::UnverifiedProcess)
    ));
}

#[test]
fn held_runtime_lease_without_ownership_metadata_is_unknown_not_stopped() {
    let fixture = fixture();
    let data_dir = fixture.data_dir();
    fs::create_dir_all(&data_dir).expect("isolated data directory should be created");
    let clock = SystemMonotonicClock::default();
    let waiter = kakune_core::lifecycle::ThreadWaiter;
    let lease = RuntimeLease::acquire(&data_dir, Uuid::new_v4(), &clock, &waiter)
        .expect("test should hold the runtime lease");

    let observation = kakune_core::lifecycle::daemon::observe(&data_dir)
        .expect("read-only daemon status should return an observation");
    assert_eq!(
        observation.state,
        kakune_core::lifecycle::ObservedState::Unknown
    );
    assert!(!observation.identity_verified);
    drop(lease);
}

#[test]
fn daemon_stop_requires_verified_owner_and_user_daemon_mode() {
    let fixture = fixture();
    fs::create_dir_all(fixture.data_dir()).unwrap();
    let target = instance(&fixture.data_dir(), CoreMode::UserDaemon);
    let other_owner = match target.owner.clone() {
        OwnerIdentity::WindowsSid(_) => OwnerIdentity::WindowsSid("S-1-5-18".to_owned()),
        OwnerIdentity::UnixUid(uid) => OwnerIdentity::UnixUid(uid.saturating_add(1)),
        OwnerIdentity::Other(_) => OwnerIdentity::Other("different-user".to_owned()),
    };
    assert!(matches!(
        verify_client_owner(&other_owner, &target.owner),
        Err(LifecycleError::WrongOwner)
    ));

    for mode in [CoreMode::Foreground, CoreMode::SystemService] {
        let non_daemon = instance(&fixture.data_dir(), mode);
        let challenge = ControlChallenge {
            protocol_version: CONTROL_PROTOCOL_VERSION,
            context: ControlContext {
                instance_id: non_daemon.instance_id,
                owner: non_daemon.owner,
                mode,
            },
            client_nonce: [3; CONTROL_NONCE_BYTES],
            server_nonce: [4; CONTROL_NONCE_BYTES],
        };
        let capability = Capability::from_bytes([0x51; 32]);
        let proof = client_request_proof(
            &capability,
            &challenge,
            &ControlOperation::GracefulStop,
            b"",
        )
        .expect("proof is valid for the control protocol");
        let registry = ChallengeRegistry::default();
        let clock = FakeClock::default();
        let session = registry.open_session(
            challenge,
            Deadline::after(&clock, Duration::from_secs(10)),
            &clock,
        );
        let mut session = session.expect("challenge should be accepted");
        assert!(matches!(
            session.verify_request(
                &capability,
                &ControlOperation::GracefulStop,
                b"",
                &proof,
                &clock,
            ),
            Err(LifecycleError::ModeNotControllable(_))
        ));
    }
}

#[test]
fn control_proofs_bind_instance_owner_operation_and_mode() {
    let fixture = fixture();
    fs::create_dir_all(fixture.data_dir()).unwrap();
    let target = instance(&fixture.data_dir(), CoreMode::UserDaemon);
    let capability = Capability::from_bytes([0x27; 32]);
    let challenge = ControlChallenge {
        protocol_version: CONTROL_PROTOCOL_VERSION,
        context: ControlContext {
            instance_id: target.instance_id,
            owner: target.owner.clone(),
            mode: target.mode,
        },
        client_nonce: [8; CONTROL_NONCE_BYTES],
        server_nonce: [9; CONTROL_NONCE_BYTES],
    };
    let proof = client_request_proof(
        &capability,
        &challenge,
        &ControlOperation::GracefulStop,
        b"stop-instance",
    )
    .unwrap();

    let mut substituted_instance = challenge.clone();
    substituted_instance.context.instance_id = Uuid::new_v4();
    let changed_instance = client_request_proof(
        &capability,
        &substituted_instance,
        &ControlOperation::GracefulStop,
        b"stop-instance",
    )
    .unwrap();
    assert_ne!(proof, changed_instance);

    let changed_operation = client_request_proof(
        &capability,
        &challenge,
        &ControlOperation::Readiness,
        b"stop-instance",
    )
    .unwrap();
    assert_ne!(proof, changed_operation);

    let mut substituted_owner = challenge.clone();
    substituted_owner.context.owner = match substituted_owner.context.owner {
        OwnerIdentity::WindowsSid(_) => OwnerIdentity::WindowsSid("S-1-5-18".to_owned()),
        OwnerIdentity::UnixUid(uid) => OwnerIdentity::UnixUid(uid.saturating_add(1)),
        OwnerIdentity::Other(_) => OwnerIdentity::Other("different-user".to_owned()),
    };
    let changed_owner = client_request_proof(
        &capability,
        &substituted_owner,
        &ControlOperation::GracefulStop,
        b"stop-instance",
    )
    .unwrap();
    assert_ne!(proof, changed_owner);

    let clock = FakeClock::default();
    let registry = ChallengeRegistry::default();
    let deadline = Deadline::after(&clock, Duration::from_secs(10));
    let mut instance_session = registry
        .open_session(challenge.clone(), deadline, &clock)
        .expect("original challenge should open a session");
    assert!(matches!(
        instance_session.verify_request(
            &capability,
            &ControlOperation::GracefulStop,
            b"stop-instance",
            &changed_instance,
            &clock,
        ),
        Err(LifecycleError::AuthenticationFailed)
    ));

    let mut wrong_operation_challenge = challenge.clone();
    wrong_operation_challenge.server_nonce = [10; CONTROL_NONCE_BYTES];
    let mut wrong_operation_session = registry
        .open_session(wrong_operation_challenge.clone(), deadline, &clock)
        .expect("fresh challenge should open a session");
    let stop_proof = client_request_proof(
        &capability,
        &wrong_operation_challenge,
        &ControlOperation::GracefulStop,
        b"stop-instance",
    )
    .unwrap();
    assert!(matches!(
        wrong_operation_session.verify_request(
            &capability,
            &ControlOperation::Readiness,
            b"stop-instance",
            &stop_proof,
            &clock,
        ),
        Err(LifecycleError::AuthenticationFailed)
    ));

    let mut wrong_capability_challenge = challenge;
    wrong_capability_challenge.server_nonce = [11; CONTROL_NONCE_BYTES];
    let mut wrong_capability_session = registry
        .open_session(wrong_capability_challenge.clone(), deadline, &clock)
        .expect("another fresh challenge should open a session");
    let valid_stop_proof = client_request_proof(
        &capability,
        &wrong_capability_challenge,
        &ControlOperation::GracefulStop,
        b"stop-instance",
    )
    .unwrap();
    let wrong_capability = Capability::from_bytes([0x28; 32]);
    assert!(matches!(
        wrong_capability_session.verify_request(
            &wrong_capability,
            &ControlOperation::GracefulStop,
            b"stop-instance",
            &valid_stop_proof,
            &clock,
        ),
        Err(LifecycleError::AuthenticationFailed)
    ));
}

#[test]
fn malformed_unreadable_and_unsupported_ownership_records_fail_closed() {
    let fixture = fixture();
    let record = fixture.root().join("ownership.json");
    fs::write(&record, b"not-json").unwrap();
    assert!(read_ownership_record(&record).is_err());

    fs::write(&record, br#"{"schema_version":999,"instance":{}}"#).unwrap();
    assert!(matches!(
        read_ownership_record(&record),
        Err(LifecycleError::UnsupportedSchema(999))
    ));

    fs::remove_file(&record).unwrap();
    fs::create_dir(&record).unwrap();
    assert!(read_ownership_record(&record).is_err());
}

#[test]
fn legacy_pid_is_never_removed_when_process_observer_reports_it_live() {
    let fixture = fixture();
    let data_dir = fixture.data_dir();
    fs::create_dir_all(&data_dir).unwrap();
    let clock = kakune_core::lifecycle::SystemMonotonicClock::default();
    let waiter = kakune_core::lifecycle::ThreadWaiter;
    let admin = AdministrativeLease::acquire_for_data_dir(
        &data_dir,
        Deadline::after(&clock, Duration::from_secs(10)),
        &clock,
        &waiter,
    )
    .unwrap();
    let legacy = admin.ownership_record_path();
    fs::write(&legacy, br#"{"pid":12345}"#).unwrap();

    let removed = cleanup_legacy_if_proven_dead_locked(&admin, |pid| {
        assert_eq!(pid, 12345);
        Ok(false)
    })
    .unwrap();
    assert!(!removed);
    assert!(legacy.is_file(), "live legacy PID state must be retained");
}

#[test]
fn replacement_record_cleanup_requires_the_original_instance_id() {
    let fixture = fixture();
    let data_dir = fixture.data_dir();
    fs::create_dir_all(&data_dir).unwrap();
    let clock = kakune_core::lifecycle::SystemMonotonicClock::default();
    let waiter = kakune_core::lifecycle::ThreadWaiter;
    let old_id = Uuid::new_v4();
    let lease = RuntimeLease::acquire(&data_dir, old_id, &clock, &waiter).unwrap();
    let new_id = Uuid::new_v4();
    let replacement = RuntimeOwnershipRecord::new(instance(&data_dir, CoreMode::UserDaemon))
        .map(|mut record| {
            record.instance.instance_id = new_id;
            record
        })
        .expect("replacement ownership record should validate");
    let admin = AdministrativeLease::acquire_for_data_dir(
        &data_dir,
        Deadline::after(&clock, Duration::from_secs(10)),
        &clock,
        &waiter,
    )
    .unwrap();
    publish_record_locked(&admin, &replacement).unwrap();
    assert!(!cleanup_record_if_instance_locked(&admin, old_id).unwrap());
    assert_eq!(
        read_ownership_record(&admin.ownership_record_path()).unwrap(),
        Some(OwnershipRead::Current(Box::new(replacement)))
    );
    drop(admin);
    drop(lease);
}

#[test]
fn deadline_book_does_not_renew_status_readiness_or_core_grace() {
    let clock = FakeClock::default();
    let mut deadlines = DeadlineBook::default();
    let status = deadlines.start_once(LifecycleBudget::Status, &clock, None);
    clock.advance(Duration::from_secs(4));
    assert_eq!(
        deadlines.start_once(LifecycleBudget::Status, &clock, None),
        status,
        "a repeated observation must reuse its original budget"
    );

    let readiness = Deadline::after(&clock, READINESS_BUDGET);
    let grace = Deadline::after(&clock, CORE_GRACE_BUDGET);
    clock.advance(Duration::from_secs(5));
    assert_eq!(readiness.remaining(&clock), Duration::from_secs(25));
    assert_eq!(grace.remaining(&clock), Duration::from_secs(25));
    assert_eq!(LifecycleBudget::Status.duration(), Duration::from_secs(10));
    assert_eq!(
        LifecycleBudget::CoreGrace.duration(),
        Duration::from_secs(30)
    );
    assert_eq!(
        LifecycleBudget::StopDispatch.duration(),
        Duration::from_secs(10)
    );
    assert_eq!(OWNERSHIP_SCHEMA_VERSION, 1);
}
