mod support;

use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use kakune_core::lifecycle::{
    CoreMode, CoreObservation, Deadline, DeadlineWaiter, InstallationObservation, LifecycleError,
    MonotonicClock, NativeManagerState, ObservedState,
    service::{
        AttemptStatus, NativeMutation, NativeServiceObservation, ServiceAction,
        ServiceAttemptJournal, ServiceController, ServiceDefinition, ServiceManagerAdapter,
        ServiceManagerKind, ServiceStateRoot,
    },
};
use support::lifecycle::LifecycleFixture;

#[derive(Clone, Default)]
struct FakeClock(Arc<Mutex<Duration>>);

impl MonotonicClock for FakeClock {
    fn now(&self) -> Duration {
        *self.0.lock().expect("clock mutex should not be poisoned")
    }
}

struct AuthorizingOutput {
    path: PathBuf,
    change_definition: bool,
    bytes: Vec<u8>,
}

impl std::io::Write for AuthorizingOutput {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.bytes.extend_from_slice(bytes);
        if self.bytes.ends_with(b"KAKUNE-HELPER-READY\n") {
            let mut journal: ServiceAttemptJournal =
                serde_json::from_slice(&std::fs::read(&self.path)?)?;
            journal.status = AttemptStatus::Authorized;
            if self.change_definition {
                journal.expected_definition.account = "replacement-account".into();
                journal.expected_definition.definition_fingerprint =
                    Some(journal.expected_definition.fingerprint().unwrap());
            }
            std::fs::write(&self.path, serde_json::to_vec(&journal)?)?;
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn prepared_native_helper(
    controller: &ServiceController,
    definition: &ServiceDefinition,
    action: ServiceAction,
    index: usize,
) -> ServiceAttemptJournal {
    use kakune_core::lifecycle::process::{ProcessEvidenceProvider, SystemProcessEvidence};
    let clock = FakeClock::default();
    let evidence = SystemProcessEvidence::default()
        .observe(
            std::process::id(),
            Deadline::after(&clock, Duration::from_secs(10)),
            &clock,
        )
        .unwrap()
        .unwrap();
    let step = NativeMutation::steps(definition.manager, action)[index];
    let journal = ServiceAttemptJournal {
        schema_version: 1,
        attempt_id: uuid::Uuid::new_v4(),
        manager: definition.manager,
        native_service_id: definition.service_id.clone(),
        action,
        step: format!("{index}:{}", serde_json::to_string(&step).unwrap()),
        expected_definition: definition.clone(),
        prior_state: NativeServiceObservation::absent(),
        helper_pid: Some(std::process::id()),
        helper_owner: Some(serde_json::to_string(&evidence).unwrap()),
        status: AttemptStatus::Prepared,
        completion_observation: None,
        residual_resources: Vec::new(),
        recovery_guidance: vec!["Inspect this isolated attempt.".into()],
    };
    let path = controller.attempt_path(definition);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, serde_json::to_vec(&journal).unwrap()).unwrap();
    journal
}

#[test]
fn parent_loss_and_missing_durable_authorization_never_dispatch_a_helper_mutation() {
    for parent_loss in [true, false] {
        let (fixture, controller, _, definition) = setup();
        let journal = prepared_native_helper(&controller, &definition, ServiceAction::Install, 0);
        let input = if parent_loss {
            "PREPARE\n".into()
        } else {
            format!("PREPARE\nAUTHORIZE {}\n", journal.attempt_id)
        };
        let calls = Mutex::new(Vec::new());
        let result = kakune_core::lifecycle::service::helper::serve_with(
            ServiceStateRoot::isolated(fixture.root().join("machine-services")).unwrap(),
            definition.manager,
            &definition.service_id,
            journal.attempt_id,
            &mut std::io::Cursor::new(input),
            &mut Vec::new(),
            &FakeClock::default(),
            &|step, _, _| {
                calls.lock().unwrap().push(step);
                Ok(())
            },
        );
        assert!(result.is_err());
        assert!(calls.lock().unwrap().is_empty());
        assert_eq!(
            controller
                .read_attempt(&definition)
                .unwrap()
                .unwrap()
                .status,
            AttemptStatus::Prepared
        );
    }
}

#[test]
fn every_native_boundary_requires_one_authorization_and_retains_helper_serialization() {
    for manager in [
        ServiceManagerKind::WindowsScm,
        ServiceManagerKind::LinuxSystemd,
        ServiceManagerKind::MacosLaunchd,
    ] {
        for action in [
            ServiceAction::Install,
            ServiceAction::Start,
            ServiceAction::Stop,
            ServiceAction::Remove,
        ] {
            for (index, step) in NativeMutation::steps(manager, action)
                .iter()
                .copied()
                .enumerate()
            {
                let (fixture, controller, backend, mut definition) = setup();
                definition.manager = manager;
                definition.service_id = manager.platform_service_id().into();
                definition.account = manager.default_account().into();
                definition.definition_fingerprint = Some(definition.fingerprint().unwrap());
                let journal = prepared_native_helper(&controller, &definition, action, index);
                let mut output = AuthorizingOutput {
                    path: controller.attempt_path(&definition),
                    change_definition: false,
                    bytes: Vec::new(),
                };
                let input = format!("PREPARE\nAUTHORIZE {}\n", journal.attempt_id);
                let calls = Mutex::new(Vec::new());
                let result = kakune_core::lifecycle::service::helper::serve_with(
                    ServiceStateRoot::isolated(fixture.root().join("machine-services")).unwrap(),
                    manager,
                    &definition.service_id,
                    journal.attempt_id,
                    &mut std::io::Cursor::new(&input),
                    &mut output,
                    &FakeClock::default(),
                    &|actual, target, _| {
                        assert_eq!(actual, step);
                        assert!(target.equivalent_to(&definition));
                        let lock = std::fs::OpenOptions::new()
                            .read(true)
                            .write(true)
                            .open(
                                controller
                                    .attempt_path(&definition)
                                    .with_file_name("helper.lock"),
                            )
                            .unwrap();
                        assert!(matches!(
                            lock.try_lock(),
                            Err(std::fs::TryLockError::WouldBlock)
                        ));
                        // A later administrator cannot cross the helper barrier,
                        // even after the parent-owned admin.lock was released.
                        assert!(matches!(
                            controller.install(&definition),
                            Err(LifecycleError::DeadlineExpired { .. })
                        ));
                        calls.lock().unwrap().push(actual);
                        Ok(())
                    },
                );
                assert!(
                    result.is_ok(),
                    "{manager:?} {action:?}/{step:?}: {result:?}"
                );
                assert_eq!(calls.lock().unwrap().as_slice(), &[step]);
                assert!(backend.calls.lock().unwrap().is_empty());
                assert!(output.bytes.ends_with(b"KAKUNE-HELPER-DONE\n"));
                // Replaying authorization after the first invocation cannot
                // promote an already-authorized attempt to another mutation.
                assert!(
                    kakune_core::lifecycle::service::helper::serve_with(
                        ServiceStateRoot::isolated(fixture.root().join("machine-services"))
                            .unwrap(),
                        manager,
                        &definition.service_id,
                        journal.attempt_id,
                        &mut std::io::Cursor::new(&input),
                        &mut Vec::new(),
                        &FakeClock::default(),
                        &|_, _, _| panic!("replayed helper must not mutate"),
                    )
                    .is_err()
                );
            }
        }
    }
}

#[test]
fn helper_rejects_replacement_authorization_and_oversized_or_spoofed_frames() {
    for case in ["replacement", "oversized", "wrong_attempt", "wrong_pid"] {
        let (fixture, controller, _, definition) = setup();
        let mut journal =
            prepared_native_helper(&controller, &definition, ServiceAction::Install, 0);
        if case == "wrong_pid" {
            journal.helper_pid = Some(std::process::id().wrapping_add(1));
            std::fs::write(
                controller.attempt_path(&definition),
                serde_json::to_vec(&journal).unwrap(),
            )
            .unwrap();
        }
        let input = if case == "oversized" {
            format!("{}\n", "x".repeat(257))
        } else {
            format!("PREPARE\nAUTHORIZE {}\n", journal.attempt_id)
        };
        let mut output = AuthorizingOutput {
            path: controller.attempt_path(&definition),
            change_definition: case == "replacement",
            bytes: Vec::new(),
        };
        let result = kakune_core::lifecycle::service::helper::serve_with(
            ServiceStateRoot::isolated(fixture.root().join("machine-services")).unwrap(),
            definition.manager,
            &definition.service_id,
            if case == "wrong_attempt" {
                uuid::Uuid::new_v4()
            } else {
                journal.attempt_id
            },
            &mut std::io::Cursor::new(input),
            &mut output,
            &FakeClock::default(),
            &|_, _, _| panic!("invalid authorization must not mutate"),
        );
        assert!(result.is_err(), "case {case}");
    }
}

struct FixtureNativeManager(PathBuf);
impl ServiceManagerAdapter for FixtureNativeManager {
    fn requires_owned_helper(&self) -> bool {
        true
    }
    fn observe(
        &self,
        _: ServiceManagerKind,
        _: &str,
        _: Deadline,
        _: &dyn MonotonicClock,
    ) -> Result<NativeServiceObservation, LifecycleError> {
        match std::fs::read(self.0.join("native-observation.json")) {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(NativeServiceObservation::absent())
            }
            Err(error) => Err(error.into()),
        }
    }
    fn mutate(
        &self,
        _: ServiceAction,
        _: ServiceManagerKind,
        _: &str,
        _: Option<&ServiceDefinition>,
        _: Deadline,
        _: &dyn MonotonicClock,
    ) -> Result<(), LifecycleError> {
        panic!("a native operation must never bypass its owned helper")
    }
}

struct AcceleratedClock(std::time::Instant);
impl MonotonicClock for AcceleratedClock {
    fn now(&self) -> Duration {
        self.0.elapsed() * 10
    }
}

#[test]
fn legacy_native_attempts_cannot_be_reconciled_from_a_matching_definition() {
    let (fixture, previous, _, definition) = setup();
    let mut journal = prepared_native_helper(&previous, &definition, ServiceAction::Install, 0);
    journal.step = "service install".into();
    journal.status = AttemptStatus::OutcomeUnknown;
    journal.helper_pid = None;
    let path = previous.attempt_path(&definition);
    let before = serde_json::to_vec(&journal).unwrap();
    std::fs::write(&path, &before).unwrap();
    let root = fixture.root().join("machine-services");
    std::fs::write(
        root.join("native-observation.json"),
        serde_json::to_vec(&installed_observation(
            &definition,
            NativeManagerState::Stopped,
            None,
        ))
        .unwrap(),
    )
    .unwrap();
    let controller = ServiceController::with_adapters(
        root.clone(),
        Arc::new(FixtureNativeManager(root)),
        Arc::new(FakeClock::default()),
        Arc::new(AdvancingWaiter(FakeClock::default())),
    );
    assert!(matches!(
        controller.install(&definition),
        Err(LifecycleError::UnresolvedServiceAttempt(_))
    ));
    assert_eq!(std::fs::read(path).unwrap(), before);
}

impl FakeClock {
    fn advance(&self, duration: Duration) {
        *self.0.lock().expect("clock mutex should not be poisoned") += duration;
    }
}

struct AdvancingWaiter(FakeClock);

impl DeadlineWaiter for AdvancingWaiter {
    fn wait(&self, duration: Duration) {
        *self.0.0.lock().expect("clock mutex should not be poisoned") += duration;
    }
}

#[derive(Default)]
struct FailingManager {
    calls: Mutex<Vec<ServiceAction>>,
}

struct ObservedFailureManager {
    observation: NativeServiceObservation,
    calls: Mutex<Vec<ServiceAction>>,
}

impl ServiceManagerAdapter for FailingManager {
    fn observe(
        &self,
        _manager: ServiceManagerKind,
        _service_id: &str,
        _deadline: Deadline,
        _clock: &dyn MonotonicClock,
    ) -> Result<NativeServiceObservation, LifecycleError> {
        Ok(NativeServiceObservation::absent())
    }

    fn mutate(
        &self,
        action: ServiceAction,
        _manager: ServiceManagerKind,
        _service_id: &str,
        _definition: Option<&ServiceDefinition>,
        _deadline: Deadline,
        _clock: &dyn MonotonicClock,
    ) -> Result<(), LifecycleError> {
        self.calls
            .lock()
            .expect("call log mutex should not be poisoned")
            .push(action);
        Err(LifecycleError::ManagerUnavailable(
            "injected helper crash after authorization".into(),
        ))
    }
}

impl ServiceManagerAdapter for ObservedFailureManager {
    fn observe(
        &self,
        _manager: ServiceManagerKind,
        _service_id: &str,
        _deadline: Deadline,
        _clock: &dyn MonotonicClock,
    ) -> Result<NativeServiceObservation, LifecycleError> {
        Ok(self.observation.clone())
    }

    fn mutate(
        &self,
        action: ServiceAction,
        _manager: ServiceManagerKind,
        _service_id: &str,
        _definition: Option<&ServiceDefinition>,
        _deadline: Deadline,
        _clock: &dyn MonotonicClock,
    ) -> Result<(), LifecycleError> {
        self.calls.lock().unwrap().push(action);
        Err(LifecycleError::ManagerUnavailable(
            "injected native operation failure after authorization".into(),
        ))
    }
}

fn installed_observation(
    definition: &ServiceDefinition,
    manager_state: NativeManagerState,
    core: Option<(CoreMode, ObservedState)>,
) -> NativeServiceObservation {
    NativeServiceObservation {
        installed: InstallationObservation::Installed,
        manager_state,
        definition: Some(definition.clone()),
        account: Some(definition.account.clone()),
        boot_enabled: true,
        core: core.map(|(mode, state)| CoreObservation {
            mode,
            state,
            instance_id: Some(uuid::Uuid::new_v4()),
            owner: None,
            process: None,
            data_dir: definition.data_dir.clone(),
            listen_address: Some("http://127.0.0.1:18702".into()),
            diagnostic_location: None,
            identity_verified: true,
        }),
        core_unknown: None,
        diagnostic_location: Some("fake-manager:injected-failure".into()),
        pending_operation: false,
    }
}

struct BudgetManager {
    clock: FakeClock,
    observations: Mutex<Vec<Duration>>,
    mutations: Mutex<Vec<Duration>>,
}

struct SlowObservationManager {
    clock: FakeClock,
    advance_by: Duration,
    calls: Mutex<Vec<ServiceAction>>,
}

struct SequencedBudgetManager {
    clock: FakeClock,
    observations: Mutex<VecDeque<NativeServiceObservation>>,
    observation_budgets: Mutex<Vec<Duration>>,
    mutations: Mutex<Vec<(ServiceAction, Duration)>>,
    stop_work: Duration,
}

impl ServiceManagerAdapter for SlowObservationManager {
    fn observe(
        &self,
        _manager: ServiceManagerKind,
        _service_id: &str,
        _deadline: Deadline,
        _clock: &dyn MonotonicClock,
    ) -> Result<NativeServiceObservation, LifecycleError> {
        self.clock.advance(self.advance_by);
        Ok(NativeServiceObservation::absent())
    }

    fn mutate(
        &self,
        action: ServiceAction,
        _manager: ServiceManagerKind,
        _service_id: &str,
        _definition: Option<&ServiceDefinition>,
        _deadline: Deadline,
        _clock: &dyn MonotonicClock,
    ) -> Result<(), LifecycleError> {
        self.calls.lock().unwrap().push(action);
        Ok(())
    }
}

impl ServiceManagerAdapter for SequencedBudgetManager {
    fn observe(
        &self,
        _manager: ServiceManagerKind,
        _service_id: &str,
        deadline: Deadline,
        clock: &dyn MonotonicClock,
    ) -> Result<NativeServiceObservation, LifecycleError> {
        self.observation_budgets
            .lock()
            .unwrap()
            .push(deadline.remaining(clock));
        self.observations
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| {
                LifecycleError::ManagerUnavailable("observation script exhausted".into())
            })
    }

    fn mutate(
        &self,
        action: ServiceAction,
        _manager: ServiceManagerKind,
        _service_id: &str,
        _definition: Option<&ServiceDefinition>,
        deadline: Deadline,
        clock: &dyn MonotonicClock,
    ) -> Result<(), LifecycleError> {
        self.mutations
            .lock()
            .unwrap()
            .push((action, deadline.remaining(clock)));
        if action == ServiceAction::Stop {
            self.clock.advance(self.stop_work);
        }
        Ok(())
    }
}

impl ServiceManagerAdapter for BudgetManager {
    fn observe(
        &self,
        _manager: ServiceManagerKind,
        _service_id: &str,
        deadline: Deadline,
        clock: &dyn MonotonicClock,
    ) -> Result<NativeServiceObservation, LifecycleError> {
        self.observations
            .lock()
            .unwrap()
            .push(deadline.remaining(clock));
        if self.observations.lock().unwrap().len() == 1 {
            self.clock.advance(Duration::from_secs(4));
        }
        Ok(NativeServiceObservation::absent())
    }

    fn mutate(
        &self,
        _action: ServiceAction,
        _manager: ServiceManagerKind,
        _service_id: &str,
        _definition: Option<&ServiceDefinition>,
        deadline: Deadline,
        clock: &dyn MonotonicClock,
    ) -> Result<(), LifecycleError> {
        self.mutations
            .lock()
            .unwrap()
            .push(deadline.remaining(clock));
        self.clock.advance(Duration::from_secs(9));
        Ok(())
    }
}

fn setup() -> (
    LifecycleFixture,
    ServiceController,
    Arc<FailingManager>,
    ServiceDefinition,
) {
    let fixture = LifecycleFixture::new().expect("isolated lifecycle fixture");
    let clock = FakeClock::default();
    let backend = Arc::new(FailingManager::default());
    let root = ServiceStateRoot::isolated(fixture.root().join("machine-services"))
        .expect("isolated service root");
    let controller = ServiceController::with_components(
        root,
        backend.clone(),
        Arc::new(clock.clone()),
        Arc::new(AdvancingWaiter(clock)),
    );
    let definition = definition_for(fixture.data_dir());
    (fixture, controller, backend, definition)
}

fn definition_for(data_dir: PathBuf) -> ServiceDefinition {
    ServiceDefinition::new(
        ServiceManagerKind::LinuxSystemd,
        "kakune-core.service",
        std::env::current_exe()
            .expect("test executable path")
            .as_path(),
        vec![
            "service".into(),
            "run".into(),
            "--data-dir".into(),
            data_dir.display().to_string(),
        ],
        "root",
        data_dir.as_path(),
    )
    .expect("service definition")
}

#[test]
fn install_observation_and_mutation_share_the_preflight_and_install_budgets() {
    let fixture = LifecycleFixture::new().expect("isolated lifecycle fixture");
    let clock = FakeClock::default();
    let manager = Arc::new(BudgetManager {
        clock: clock.clone(),
        observations: Mutex::new(Vec::new()),
        mutations: Mutex::new(Vec::new()),
    });
    let root = ServiceStateRoot::isolated(fixture.root().join("machine-services"))
        .expect("isolated service root");
    let controller = ServiceController::with_components(
        root,
        manager.clone(),
        Arc::new(clock.clone()),
        Arc::new(AdvancingWaiter(clock)),
    );
    let data_dir = fixture.data_dir();
    let definition = ServiceDefinition::new(
        ServiceManagerKind::LinuxSystemd,
        "kakune-core.service",
        std::env::current_exe()
            .expect("test executable path")
            .as_path(),
        vec![
            "service".into(),
            "run".into(),
            "--data-dir".into(),
            data_dir.display().to_string(),
        ],
        "root",
        data_dir.as_path(),
    )
    .expect("service definition");

    assert!(matches!(
        controller.install(&definition),
        Err(LifecycleError::ServiceStateUnconfirmed)
    ));
    assert_eq!(
        manager.observations.lock().unwrap().as_slice(),
        &[Duration::from_secs(10), Duration::from_secs(21)],
        "the verification observation must consume the original install phase"
    );
    assert_eq!(
        manager.mutations.lock().unwrap().as_slice(),
        &[Duration::from_secs(10)],
        "a native helper must be capped at ten seconds even inside the install phase"
    );
}

#[test]
fn status_observation_is_capped_by_its_single_ten_second_budget() {
    let fixture = LifecycleFixture::new().expect("isolated lifecycle fixture");
    let clock = FakeClock::default();
    let manager = Arc::new(SlowObservationManager {
        clock: clock.clone(),
        advance_by: Duration::from_secs(11),
        calls: Mutex::new(Vec::new()),
    });
    let root = ServiceStateRoot::isolated(fixture.root().join("machine-services"))
        .expect("isolated service root");
    let controller = ServiceController::with_components(
        root,
        manager.clone(),
        Arc::new(clock.clone()),
        Arc::new(AdvancingWaiter(clock.clone())),
    );

    assert!(matches!(
        controller.status(ServiceManagerKind::LinuxSystemd, "kakune-core.service"),
        Err(LifecycleError::DeadlineExpired {
            phase: "service observation"
        })
    ));
    assert_eq!(clock.now(), Duration::from_secs(11));
    assert!(manager.calls.lock().unwrap().is_empty());
}

#[test]
fn expired_preflight_never_authorizes_or_dispatches_a_native_mutation() {
    let fixture = LifecycleFixture::new().expect("isolated lifecycle fixture");
    let clock = FakeClock::default();
    let manager = Arc::new(SlowObservationManager {
        clock: clock.clone(),
        advance_by: Duration::from_secs(11),
        calls: Mutex::new(Vec::new()),
    });
    let root = ServiceStateRoot::isolated(fixture.root().join("machine-services"))
        .expect("isolated service root");
    let controller = ServiceController::with_components(
        root,
        manager.clone(),
        Arc::new(clock),
        Arc::new(AdvancingWaiter(FakeClock::default())),
    );
    let data_dir = fixture.data_dir();
    let definition = ServiceDefinition::new(
        ServiceManagerKind::LinuxSystemd,
        "kakune-core.service",
        std::env::current_exe()
            .expect("test executable path")
            .as_path(),
        vec![
            "service".into(),
            "run".into(),
            "--data-dir".into(),
            data_dir.display().to_string(),
        ],
        "root",
        data_dir.as_path(),
    )
    .expect("service definition");

    assert!(matches!(
        controller.install(&definition),
        Err(LifecycleError::DeadlineExpired {
            phase: "service observation"
        })
    ));
    assert!(manager.calls.lock().unwrap().is_empty());
    assert!(
        controller.read_attempt(&definition).unwrap().is_none(),
        "preflight failure must not publish an authorized attempt"
    );
}

#[test]
fn authorized_unknown_mutation_leaves_a_durable_barrier() {
    let (_fixture, controller, manager, definition) = setup();
    assert!(matches!(
        controller.install(&definition),
        Err(LifecycleError::ManagerUnavailable(_))
    ));
    assert_eq!(
        manager.calls.lock().unwrap().as_slice(),
        &[ServiceAction::Install]
    );

    let journal = controller
        .read_attempt(&definition)
        .expect("authorized attempt should remain inspectable")
        .expect("attempt journal should be retained");
    assert_eq!(journal.status, AttemptStatus::OutcomeUnknown);
    assert!(journal.expected_definition.definition_fingerprint.is_some());
    let journal_path = controller.attempt_path(&definition);
    let before_status = std::fs::read(&journal_path).expect("journal should be readable");
    assert!(
        controller
            .status(definition.manager, &definition.service_id)
            .expect("status should observe the existing journal read-only")
            .pending_operation
    );
    assert_eq!(
        std::fs::read(&journal_path).expect("journal should remain readable"),
        before_status,
        "status must not rewrite the service attempt"
    );

    assert!(matches!(
        controller.install(&definition),
        Err(LifecycleError::UnresolvedServiceAttempt(_))
    ));
    assert!(matches!(
        controller.stop(definition.manager, &definition.service_id),
        Err(LifecycleError::UnresolvedServiceAttempt(_))
    ));
    assert!(matches!(
        controller.uninstall(definition.manager, &definition.service_id),
        Err(LifecycleError::UnresolvedServiceAttempt(_))
    ));
    assert_eq!(manager.calls.lock().unwrap().len(), 1);
}

#[test]
fn each_logical_mutation_action_retains_unknown_journal_after_failure() {
    for action in [
        ServiceAction::Install,
        ServiceAction::Start,
        ServiceAction::Stop,
        ServiceAction::Remove,
    ] {
        let (fixture, _unused_controller, _unused_manager, definition) = setup();
        let observation = match action {
            ServiceAction::Install => NativeServiceObservation::absent(),
            ServiceAction::Start | ServiceAction::Remove => {
                installed_observation(&definition, NativeManagerState::Stopped, None)
            }
            ServiceAction::Stop => installed_observation(
                &definition,
                NativeManagerState::Running,
                Some((CoreMode::SystemService, ObservedState::Ready)),
            ),
        };
        let manager = Arc::new(ObservedFailureManager {
            observation,
            calls: Mutex::new(Vec::new()),
        });
        let clock = FakeClock::default();
        let controller = ServiceController::with_adapters(
            fixture.root().join("injected-manager-root"),
            manager.clone(),
            Arc::new(clock.clone()),
            Arc::new(AdvancingWaiter(clock)),
        );
        let invoke = || match action {
            ServiceAction::Install => controller.install(&definition),
            ServiceAction::Start => controller.start(definition.manager, &definition.service_id),
            ServiceAction::Stop => controller.stop(definition.manager, &definition.service_id),
            ServiceAction::Remove => {
                controller.uninstall(definition.manager, &definition.service_id)
            }
        };

        assert!(matches!(
            invoke(),
            Err(LifecycleError::ManagerUnavailable(_))
        ));
        assert_eq!(manager.calls.lock().unwrap().as_slice(), &[action]);
        let journal = controller
            .read_attempt(&definition)
            .expect("failed mutation journal should be readable")
            .expect("failed mutation must retain an attempt journal");
        assert_eq!(journal.action, action);
        assert_eq!(journal.status, AttemptStatus::OutcomeUnknown);

        let journal_path = controller.attempt_path(&definition);
        let before_status = std::fs::read(&journal_path).expect("journal snapshot should exist");
        assert!(
            controller
                .status(definition.manager, &definition.service_id)
                .expect("status should remain read-only")
                .pending_operation
        );
        assert_eq!(
            std::fs::read(&journal_path).expect("status must preserve the journal bytes"),
            before_status,
            "status must not rewrite the {action:?} attempt"
        );
        assert!(matches!(
            invoke(),
            Err(LifecycleError::UnresolvedServiceAttempt(_))
        ));
        assert_eq!(
            manager.calls.lock().unwrap().as_slice(),
            &[action],
            "an unresolved {action:?} effect must block a second native mutation"
        );
    }
}

#[test]
fn an_interrupted_native_helper_keeps_its_barrier_even_when_the_manager_looks_settled() {
    let fixture = LifecycleFixture::new().unwrap();
    let root = fixture.root().join("interrupted-native-helper");
    std::fs::create_dir_all(&root).unwrap();
    let definition = definition_for(fixture.data_dir());
    let observation = NativeServiceObservation {
        installed: InstallationObservation::Installed,
        manager_state: NativeManagerState::Stopped,
        definition: Some(definition.clone()),
        account: Some(definition.account.clone()),
        boot_enabled: true,
        core: None,
        core_unknown: None,
        diagnostic_location: Some("fake-manager:interrupted-helper".into()),
        pending_operation: false,
    };
    std::fs::write(
        root.join("native-observation.json"),
        serde_json::to_vec(&observation).unwrap(),
    )
    .unwrap();
    let controller = ServiceController::with_components(
        ServiceStateRoot::isolated(root.clone()).unwrap(),
        Arc::new(FixtureNativeManager(root)),
        Arc::new(AcceleratedClock(std::time::Instant::now())),
        Arc::new(kakune_core::lifecycle::ThreadWaiter),
    );
    // An interrupted helper may have applied only part of the native step.
    let journal = ServiceAttemptJournal {
        schema_version: 1,
        attempt_id: uuid::Uuid::new_v4(),
        manager: definition.manager,
        native_service_id: definition.service_id.clone(),
        action: ServiceAction::Start,
        step: "0:\"start\"".into(),
        expected_definition: definition.clone(),
        prior_state: NativeServiceObservation::absent(),
        helper_pid: None,
        helper_owner: None,
        status: AttemptStatus::OutcomeUnknown,
        completion_observation: None,
        residual_resources: vec!["native step is unconfirmed".into()],
        recovery_guidance: vec!["Inspect the native manager before retrying.".into()],
    };
    let path = controller.attempt_path(&definition);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let before = serde_json::to_vec(&journal).unwrap();
    std::fs::write(&path, &before).unwrap();

    assert!(matches!(
        controller.stop(definition.manager, &definition.service_id),
        Err(LifecycleError::UnresolvedServiceAttempt(_))
    ));
    assert_eq!(
        std::fs::read(&path).unwrap(),
        before,
        "an unconfirmed native effect must keep its durable barrier"
    );
}

#[test]
fn an_unsettled_native_failure_keeps_its_durable_barrier() {
    let fixture = LifecycleFixture::new().unwrap();
    let root = fixture.root().join("unsettled-native-failure");
    std::fs::create_dir_all(&root).unwrap();
    let definition = definition_for(fixture.data_dir());
    // The manager still reports pending work, so nothing is settled.
    let observation = NativeServiceObservation {
        installed: InstallationObservation::Installed,
        manager_state: NativeManagerState::Failed,
        definition: Some(definition.clone()),
        account: Some(definition.account.clone()),
        boot_enabled: true,
        core: None,
        core_unknown: None,
        diagnostic_location: Some("fake-manager:pending-work".into()),
        pending_operation: true,
    };
    std::fs::write(
        root.join("native-observation.json"),
        serde_json::to_vec(&observation).unwrap(),
    )
    .unwrap();
    let controller = ServiceController::with_components(
        ServiceStateRoot::isolated(root.clone()).unwrap(),
        Arc::new(FixtureNativeManager(root)),
        Arc::new(AcceleratedClock(std::time::Instant::now())),
        Arc::new(kakune_core::lifecycle::ThreadWaiter),
    );
    let journal = ServiceAttemptJournal {
        schema_version: 1,
        attempt_id: uuid::Uuid::new_v4(),
        manager: definition.manager,
        native_service_id: definition.service_id.clone(),
        action: ServiceAction::Start,
        step: "0:\"start\"".into(),
        expected_definition: definition.clone(),
        prior_state: NativeServiceObservation::absent(),
        helper_pid: None,
        helper_owner: None,
        status: AttemptStatus::OutcomeUnknown,
        completion_observation: None,
        residual_resources: vec!["native manager work is still pending".into()],
        recovery_guidance: vec!["Inspect the native manager before retrying.".into()],
    };
    let path = controller.attempt_path(&definition);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let before = serde_json::to_vec(&journal).unwrap();
    std::fs::write(&path, &before).unwrap();

    assert!(matches!(
        controller.stop(definition.manager, &definition.service_id),
        Err(LifecycleError::UnresolvedServiceAttempt(_))
    ));
    assert_eq!(
        std::fs::read(&path).unwrap(),
        before,
        "an unsettled native effect must keep its durable barrier"
    );
}

#[test]
fn status_reads_pending_journal_without_repairing_or_removing_it() {
    let (_fixture, controller, _manager, definition) = setup();
    let attempt_path = controller.attempt_path(&definition);
    assert!(!attempt_path.exists());
    assert!(
        controller
            .status(definition.manager, &definition.service_id)
            .is_ok()
    );
    assert!(!attempt_path.exists());
}

#[test]
fn corrupt_or_redirected_attempt_storage_fails_closed_before_manager_mutation() {
    let (_fixture, controller, manager, definition) = setup();
    let attempt_path = controller.attempt_path(&definition);
    std::fs::create_dir_all(attempt_path.parent().unwrap()).unwrap();
    std::fs::create_dir(&attempt_path).unwrap();

    assert!(controller.install(&definition).is_err());
    assert!(manager.calls.lock().unwrap().is_empty());
}

#[test]
fn journal_publication_failure_never_reaches_the_native_manager() {
    let (_fixture, controller, manager, definition) = setup();
    let attempt_path = controller.attempt_path(&definition);
    std::fs::create_dir_all(attempt_path.parent().unwrap()).unwrap();
    std::fs::create_dir(&attempt_path).unwrap();

    assert!(controller.install(&definition).is_err());
    assert!(
        manager.calls.lock().unwrap().is_empty(),
        "native work must not begin unless its durable attempt record was published"
    );
}

#[test]
fn prepared_attempt_with_a_spawned_helper_blocks_mutation_after_parent_loss() {
    let (_fixture, controller, manager, definition) = setup();
    let journal = ServiceAttemptJournal {
        schema_version: 1,
        attempt_id: uuid::Uuid::new_v4(),
        manager: definition.manager,
        native_service_id: definition.service_id.clone(),
        action: ServiceAction::Install,
        step: "service install".into(),
        expected_definition: definition.clone(),
        prior_state: NativeServiceObservation::absent(),
        helper_pid: Some(42_424),
        helper_owner: Some("test-owner".into()),
        status: AttemptStatus::Prepared,
        completion_observation: None,
        residual_resources: Vec::new(),
        recovery_guidance: vec!["Inspect the isolated helper before retrying.".into()],
    };
    let attempt_path = controller.attempt_path(&definition);
    std::fs::create_dir_all(attempt_path.parent().unwrap()).unwrap();
    std::fs::write(&attempt_path, serde_json::to_vec(&journal).unwrap()).unwrap();

    assert!(matches!(
        controller.install(&definition),
        Err(LifecycleError::UnresolvedServiceAttempt(_))
    ));
    assert!(
        manager.calls.lock().unwrap().is_empty(),
        "an un-authorized helper record must remain a barrier after its parent disappears"
    );
    assert_eq!(
        controller
            .read_attempt(&definition)
            .unwrap()
            .unwrap()
            .status,
        AttemptStatus::Prepared,
        "read-only recovery must not promote an un-authorized helper"
    );
}

#[test]
fn journal_identity_must_match_the_service_key_where_it_is_discovered() {
    let (_fixture, controller, manager, definition) = setup();
    let other_id = "kakune-other.service";
    let other_data_dir = definition.data_dir.with_file_name("other-service-data");
    let other_definition = ServiceDefinition::new(
        definition.manager,
        other_id,
        &definition.executable,
        vec![
            "service".into(),
            "run".into(),
            "--data-dir".into(),
            other_data_dir.display().to_string(),
        ],
        &definition.account,
        &other_data_dir,
    )
    .expect("other service definition");
    let journal = ServiceAttemptJournal {
        schema_version: 1,
        attempt_id: uuid::Uuid::new_v4(),
        manager: definition.manager,
        native_service_id: definition.service_id.clone(),
        action: ServiceAction::Install,
        step: "service install".into(),
        expected_definition: definition.clone(),
        prior_state: NativeServiceObservation::absent(),
        helper_pid: None,
        helper_owner: None,
        status: AttemptStatus::Prepared,
        completion_observation: None,
        residual_resources: Vec::new(),
        recovery_guidance: vec!["Inspect the original service attempt.".into()],
    };
    let other_attempt_path = controller.attempt_path(&other_definition);
    std::fs::create_dir_all(other_attempt_path.parent().unwrap()).unwrap();
    std::fs::write(&other_attempt_path, serde_json::to_vec(&journal).unwrap()).unwrap();

    assert!(matches!(
        controller.read_attempt(&other_definition),
        Err(LifecycleError::UnsafeOwnership(_))
    ));
    assert!(matches!(
        controller.install(&other_definition),
        Err(LifecycleError::UnsafeOwnership(_))
    ));
    assert!(
        manager.calls.lock().unwrap().is_empty(),
        "a journal discovered under a different native key must block manager mutation"
    );
}

#[test]
fn canonical_service_keys_are_independent_of_data_paths_and_reject_traversal() {
    let (_fixture, _controller, _manager, definition) = setup();
    let other_data_dir = definition.data_dir.with_file_name("other-data");
    let same_key_other_path = ServiceDefinition::new(
        definition.manager,
        &definition.service_id,
        &definition.executable,
        vec![
            "service".into(),
            "run".into(),
            "--data-dir".into(),
            other_data_dir.display().to_string(),
        ],
        &definition.account,
        &other_data_dir,
    )
    .expect("absolute alternate data path is valid for key comparison");
    assert_eq!(
        definition.service_key().expect("service key"),
        same_key_other_path.service_key().expect("service key")
    );
    assert!(
        ServiceDefinition::new(
            definition.manager,
            "../kakune-core.service",
            &definition.executable,
            definition.arguments.clone(),
            &definition.account,
            &definition.data_dir,
        )
        .is_err()
    );
}

#[test]
fn every_service_command_budget_is_explicit_and_helper_deadlines_cannot_renew() {
    use kakune_core::lifecycle::{
        ACQUISITION_BUDGET, CORE_GRACE_BUDGET, HELPER_BUDGET, INSTALL_BUDGET,
        INSTALL_COMMAND_CEILING, PREFLIGHT_BUDGET, READINESS_BUDGET, REMOVAL_BUDGET,
        START_COMMAND_CEILING, STATUS_BUDGET, STOP_COMMAND_CEILING, STOP_DISPATCH_BUDGET,
        UNINSTALL_COMMAND_CEILING,
    };

    assert_eq!(ACQUISITION_BUDGET, Duration::from_secs(10));
    assert_eq!(PREFLIGHT_BUDGET, Duration::from_secs(10));
    assert_eq!(HELPER_BUDGET, Duration::from_secs(10));
    assert_eq!(STATUS_BUDGET, Duration::from_secs(10));
    assert_eq!(INSTALL_BUDGET, Duration::from_secs(30));
    assert_eq!(READINESS_BUDGET, Duration::from_secs(30));
    assert_eq!(STOP_DISPATCH_BUDGET, Duration::from_secs(10));
    assert_eq!(REMOVAL_BUDGET, Duration::from_secs(30));
    assert_eq!(
        INSTALL_COMMAND_CEILING,
        ACQUISITION_BUDGET + PREFLIGHT_BUDGET + INSTALL_BUDGET
    );
    assert_eq!(
        START_COMMAND_CEILING,
        ACQUISITION_BUDGET + PREFLIGHT_BUDGET + READINESS_BUDGET
    );
    assert_eq!(
        STOP_COMMAND_CEILING,
        ACQUISITION_BUDGET + PREFLIGHT_BUDGET + STOP_DISPATCH_BUDGET + CORE_GRACE_BUDGET
    );
    assert_eq!(
        UNINSTALL_COMMAND_CEILING,
        ACQUISITION_BUDGET
            + PREFLIGHT_BUDGET
            + STOP_DISPATCH_BUDGET
            + CORE_GRACE_BUDGET
            + REMOVAL_BUDGET
    );
}

#[test]
fn core_grace_and_uninstall_removal_keep_fresh_budgets_after_helper_work() {
    for uninstall in [false, true] {
        let fixture = LifecycleFixture::new().expect("isolated lifecycle fixture");
        let clock = FakeClock::default();
        let definition = definition_for(fixture.data_dir());
        let mut observations = VecDeque::from([
            installed_observation(
                &definition,
                NativeManagerState::Running,
                Some((CoreMode::SystemService, ObservedState::Ready)),
            ),
            installed_observation(&definition, NativeManagerState::Stopped, None),
        ]);
        if uninstall {
            observations.push_back(NativeServiceObservation::absent());
        }
        let manager = Arc::new(SequencedBudgetManager {
            clock: clock.clone(),
            observations: Mutex::new(observations),
            observation_budgets: Mutex::new(Vec::new()),
            mutations: Mutex::new(Vec::new()),
            // This fits within the helper's nine-second work budget, while
            // consuming almost all of the ten-second stop-dispatch phase.
            stop_work: Duration::from_millis(8_500),
        });
        let controller = ServiceController::with_components(
            ServiceStateRoot::isolated(fixture.root().join("machine-services"))
                .expect("isolated service root"),
            manager.clone(),
            Arc::new(clock),
            Arc::new(AdvancingWaiter(FakeClock::default())),
        );

        let result = if uninstall {
            controller.uninstall(definition.manager, &definition.service_id)
        } else {
            controller.stop(definition.manager, &definition.service_id)
        };
        assert!(result.is_ok(), "operation failed: {result:?}");

        let expected_observation_budgets = if uninstall {
            vec![
                Duration::from_secs(10),
                Duration::from_secs(30),
                Duration::from_secs(30),
            ]
        } else {
            vec![Duration::from_secs(10), Duration::from_secs(30)]
        };
        assert_eq!(
            manager.observation_budgets.lock().unwrap().as_slice(),
            expected_observation_budgets.as_slice(),
            "the Core grace and removal observations must receive fresh phase budgets"
        );
        let expected_mutations = if uninstall {
            vec![
                (ServiceAction::Stop, Duration::from_secs(10)),
                (ServiceAction::Remove, Duration::from_secs(10)),
            ]
        } else {
            vec![(ServiceAction::Stop, Duration::from_secs(10))]
        };
        assert_eq!(
            manager.mutations.lock().unwrap().as_slice(),
            expected_mutations.as_slice(),
            "each native helper stays within its own ten-second cap"
        );
    }
}

#[test]
fn journal_layout_uses_manager_and_canonical_service_key_not_data_location() {
    let (_fixture, controller, _manager, definition) = setup();
    let path = controller.attempt_path(&definition);
    assert!(path.ends_with("systemd/kakune-core.service/attempt.json"));
    assert!(!path.to_string_lossy().contains("data"));
}

fn scripted_controller(
    fixture: &LifecycleFixture,
    observations: impl IntoIterator<Item = NativeServiceObservation>,
    stop_work: Duration,
) -> (ServiceController, Arc<SequencedBudgetManager>) {
    let clock = FakeClock::default();
    let manager = Arc::new(SequencedBudgetManager {
        clock: clock.clone(),
        observations: Mutex::new(observations.into_iter().collect()),
        observation_budgets: Mutex::new(Vec::new()),
        mutations: Mutex::new(Vec::new()),
        stop_work,
    });
    let controller = ServiceController::with_components(
        ServiceStateRoot::isolated(fixture.root().join("machine-services")).unwrap(),
        manager.clone(),
        Arc::new(clock.clone()),
        Arc::new(AdvancingWaiter(clock)),
    );
    (controller, manager)
}

#[test]
fn replacement_during_readiness_never_completes_the_original_start() {
    let fixture = LifecycleFixture::new().unwrap();
    let definition = definition_for(fixture.data_dir());
    let replacement = definition_for(fixture.root().join("replacement-data"));
    let (controller, _) = scripted_controller(
        &fixture,
        [
            installed_observation(&definition, NativeManagerState::Stopped, None),
            installed_observation(
                &replacement,
                NativeManagerState::Running,
                Some((CoreMode::SystemService, ObservedState::Ready)),
            ),
        ],
        Duration::ZERO,
    );

    assert!(matches!(
        controller.start(definition.manager, &definition.service_id),
        Err(LifecycleError::ConflictingServiceDefinition(_))
    ));
    assert!(controller.read_attempt(&definition).unwrap().is_some());
}

#[test]
fn replacement_during_stop_prevents_uninstall_removal() {
    let fixture = LifecycleFixture::new().unwrap();
    let definition = definition_for(fixture.data_dir());
    let replacement = definition_for(fixture.root().join("replacement-data"));
    let (controller, manager) = scripted_controller(
        &fixture,
        [
            installed_observation(
                &definition,
                NativeManagerState::Running,
                Some((CoreMode::SystemService, ObservedState::Ready)),
            ),
            installed_observation(&replacement, NativeManagerState::Stopped, None),
            NativeServiceObservation::absent(),
        ],
        Duration::ZERO,
    );

    assert!(matches!(
        controller.uninstall(definition.manager, &definition.service_id),
        Err(LifecycleError::ConflictingServiceDefinition(_))
    ));
    assert_eq!(
        manager.mutations.lock().unwrap().as_slice(),
        [(ServiceAction::Stop, Duration::from_secs(10))]
    );
    assert!(controller.read_attempt(&definition).unwrap().is_some());
}

#[test]
fn late_stop_dispatch_acknowledgment_does_not_begin_a_fresh_core_grace() {
    let fixture = LifecycleFixture::new().unwrap();
    let definition = definition_for(fixture.data_dir());
    let (controller, manager) = scripted_controller(
        &fixture,
        [
            installed_observation(
                &definition,
                NativeManagerState::Running,
                Some((CoreMode::SystemService, ObservedState::Ready)),
            ),
            installed_observation(&definition, NativeManagerState::Stopped, None),
        ],
        Duration::from_secs(10),
    );

    assert!(matches!(
        controller.stop(definition.manager, &definition.service_id),
        Err(LifecycleError::DeadlineExpired { .. })
    ));
    assert_eq!(manager.observation_budgets.lock().unwrap().len(), 1);
    assert_eq!(
        controller
            .read_attempt(&definition)
            .unwrap()
            .unwrap()
            .status,
        AttemptStatus::OutcomeUnknown
    );
}

#[test]
fn failed_core_record_is_not_proof_of_process_exit() {
    let fixture = LifecycleFixture::new().unwrap();
    let definition = definition_for(fixture.data_dir());
    let failed = installed_observation(
        &definition,
        NativeManagerState::Stopped,
        Some((CoreMode::SystemService, ObservedState::Failed)),
    );
    let (controller, manager) =
        scripted_controller(&fixture, [failed.clone(), failed], Duration::ZERO);

    assert!(
        controller
            .uninstall(definition.manager, &definition.service_id)
            .is_err()
    );
    assert!(
        manager
            .mutations
            .lock()
            .unwrap()
            .iter()
            .all(|(action, _)| *action != ServiceAction::Remove),
        "a failed Core can retain its lease while storage workers finish"
    );
}

#[test]
fn installed_observation_for_another_native_key_is_rejected_before_mutation() {
    let fixture = LifecycleFixture::new().unwrap();
    let definition = definition_for(fixture.data_dir());
    let mut foreign = definition.clone();
    foreign.service_id = "foreign.service".into();
    foreign.definition_fingerprint = Some(foreign.fingerprint().unwrap());
    let (controller, manager) = scripted_controller(
        &fixture,
        [installed_observation(
            &foreign,
            NativeManagerState::Stopped,
            None,
        )],
        Duration::ZERO,
    );

    assert!(
        controller
            .start(definition.manager, &definition.service_id)
            .is_err()
    );
    assert!(manager.mutations.lock().unwrap().is_empty());
}

#[test]
fn stopped_install_cannot_succeed_with_a_live_foreground_owner() {
    let fixture = LifecycleFixture::new().unwrap();
    let definition = definition_for(fixture.data_dir());
    let (controller, _) = scripted_controller(
        &fixture,
        [
            NativeServiceObservation::absent(),
            installed_observation(
                &definition,
                NativeManagerState::Stopped,
                Some((CoreMode::Foreground, ObservedState::Ready)),
            ),
        ],
        Duration::ZERO,
    );

    assert!(controller.install(&definition).is_err());
    assert!(controller.read_attempt(&definition).unwrap().is_some());
}

#[test]
fn unresolved_helper_cannot_be_reconciled_from_a_matching_registration_alone() {
    let fixture = LifecycleFixture::new().unwrap();
    let definition = definition_for(fixture.data_dir());
    let (controller, manager) = scripted_controller(
        &fixture,
        [installed_observation(
            &definition,
            NativeManagerState::Stopped,
            None,
        )],
        Duration::ZERO,
    );
    let journal = ServiceAttemptJournal {
        schema_version: 1,
        attempt_id: uuid::Uuid::new_v4(),
        manager: definition.manager,
        native_service_id: definition.service_id.clone(),
        action: ServiceAction::Install,
        step: "service install".into(),
        expected_definition: definition.clone(),
        prior_state: NativeServiceObservation::absent(),
        helper_pid: Some(42_424),
        helper_owner: Some("test-owner".into()),
        status: AttemptStatus::OutcomeUnknown,
        completion_observation: None,
        residual_resources: vec!["helper exit remains unconfirmed".into()],
        recovery_guidance: vec!["Verify the retained helper and pending manager work.".into()],
    };
    let path = controller.attempt_path(&definition);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let before = serde_json::to_vec(&journal).unwrap();
    std::fs::write(&path, &before).unwrap();

    assert!(matches!(
        controller.install(&definition),
        Err(LifecycleError::UnresolvedServiceAttempt(_))
    ));
    assert!(manager.mutations.lock().unwrap().is_empty());
    assert_eq!(std::fs::read(path).unwrap(), before);
}

#[test]
fn recovery_does_not_reconcile_a_replacement_service_as_the_original_stop() {
    let fixture = LifecycleFixture::new().unwrap();
    let definition = definition_for(fixture.data_dir());
    let replacement = definition_for(fixture.root().join("replacement-data"));
    let (controller, manager) = scripted_controller(
        &fixture,
        [installed_observation(
            &replacement,
            NativeManagerState::Stopped,
            None,
        )],
        Duration::ZERO,
    );
    let journal = ServiceAttemptJournal {
        schema_version: 1,
        attempt_id: uuid::Uuid::new_v4(),
        manager: definition.manager,
        native_service_id: definition.service_id.clone(),
        action: ServiceAction::Stop,
        step: "service stop".into(),
        expected_definition: definition.clone(),
        prior_state: installed_observation(
            &definition,
            NativeManagerState::Running,
            Some((CoreMode::SystemService, ObservedState::Ready)),
        ),
        helper_pid: None,
        helper_owner: None,
        status: AttemptStatus::OutcomeUnknown,
        completion_observation: None,
        residual_resources: vec!["original Core exit remains unconfirmed".into()],
        recovery_guidance: vec!["Inspect the original definition and its Core.".into()],
    };
    let path = controller.attempt_path(&definition);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, serde_json::to_vec(&journal).unwrap()).unwrap();

    assert!(matches!(
        controller.uninstall(definition.manager, &definition.service_id),
        Err(LifecycleError::UnresolvedServiceAttempt(_))
    ));
    assert!(manager.mutations.lock().unwrap().is_empty());
    assert!(controller.read_attempt(&definition).unwrap().is_some());
}
