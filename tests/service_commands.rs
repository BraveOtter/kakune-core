mod support;

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use kakune_core::lifecycle::{
    CoreMode, CoreObservation, Deadline, DeadlineWaiter, InstallationObservation, LifecycleError,
    MonotonicClock, NativeManagerState, ObservedState, OperationOutcome,
    service::{
        NativeServiceObservation, ServiceAction, ServiceController, ServiceDefinition,
        ServiceManagerAdapter, ServiceManagerKind,
    },
};
use support::lifecycle::LifecycleFixture;

#[derive(Clone, Default)]
struct TestClock(Arc<Mutex<Duration>>);

impl MonotonicClock for TestClock {
    fn now(&self) -> Duration {
        *self.0.lock().expect("clock mutex should not be poisoned")
    }
}

struct AdvancingWaiter(TestClock);

impl DeadlineWaiter for AdvancingWaiter {
    fn wait(&self, duration: Duration) {
        thread::sleep(Duration::from_millis(1));
        *self.0.0.lock().expect("clock mutex should not be poisoned") += duration;
    }
}

#[derive(Clone)]
struct FakeManager {
    state: Arc<Mutex<FakeState>>,
}

#[derive(Default)]
struct FakeState {
    registrations: BTreeMap<(ServiceManagerKind, String), FakeRegistration>,
    failures: BTreeMap<ServiceAction, Failure>,
    active_mutations: usize,
    overlapping_mutations: bool,
    mutation_count: usize,
    stop_dispatched: bool,
    stop_unknown_observations: usize,
}

#[derive(Clone)]
struct FakeRegistration {
    definition: ServiceDefinition,
    manager_state: NativeManagerState,
    core: Option<CoreObservation>,
    core_unknown: Option<String>,
    pending: bool,
}

#[derive(Clone, Copy)]
enum Failure {
    PermissionDenied,
    ManagerUnavailable,
    StopTimeout,
    StartNotReady,
}

impl Default for FakeManager {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeState::default())),
        }
    }
}

impl FakeManager {
    fn fail(&self, action: ServiceAction, failure: Failure) {
        self.state
            .lock()
            .expect("fake manager mutex should not be poisoned")
            .failures
            .insert(action, failure);
    }

    fn manager_state(&self, definition: &ServiceDefinition) -> NativeManagerState {
        self.state
            .lock()
            .expect("fake manager mutex should not be poisoned")
            .registrations
            .get(&(definition.manager, definition.service_id.clone()))
            .map_or(NativeManagerState::Unknown, |entry| entry.manager_state)
    }

    fn set_core_state(&self, definition: &ServiceDefinition, mode: CoreMode, state: ObservedState) {
        let mut manager = self
            .state
            .lock()
            .expect("fake manager mutex should not be poisoned");
        if let Some(registration) = manager
            .registrations
            .get_mut(&(definition.manager, definition.service_id.clone()))
        {
            registration.core = Some(core_observation(definition, mode, state));
        }
    }

    fn set_core_unknown(&self, definition: &ServiceDefinition, detail: &str) {
        let mut manager = self
            .state
            .lock()
            .expect("fake manager mutex should not be poisoned");
        if let Some(registration) = manager
            .registrations
            .get_mut(&(definition.manager, definition.service_id.clone()))
        {
            registration.core = None;
            registration.core_unknown = Some(detail.to_owned());
        }
    }

    fn set_manager_state(&self, definition: &ServiceDefinition, state: NativeManagerState) {
        let mut manager = self
            .state
            .lock()
            .expect("fake manager mutex should not be poisoned");
        if let Some(registration) = manager
            .registrations
            .get_mut(&(definition.manager, definition.service_id.clone()))
        {
            registration.manager_state = state;
        }
    }

    fn set_pending(&self, definition: &ServiceDefinition, pending: bool) {
        let mut manager = self
            .state
            .lock()
            .expect("fake manager mutex should not be poisoned");
        if let Some(registration) = manager
            .registrations
            .get_mut(&(definition.manager, definition.service_id.clone()))
        {
            registration.pending = pending;
        }
    }

    fn metrics(&self) -> (usize, bool) {
        let state = self
            .state
            .lock()
            .expect("fake manager mutex should not be poisoned");
        (state.mutation_count, state.overlapping_mutations)
    }
}

impl ServiceManagerAdapter for FakeManager {
    fn observe(
        &self,
        manager: ServiceManagerKind,
        service_id: &str,
        _deadline: Deadline,
        _clock: &dyn MonotonicClock,
    ) -> Result<NativeServiceObservation, LifecycleError> {
        let mut state = self
            .state
            .lock()
            .expect("fake manager mutex should not be poisoned");
        let Some(registration) = state
            .registrations
            .get(&(manager, service_id.to_owned()))
            .cloned()
        else {
            return Ok(NativeServiceObservation::absent());
        };
        let mut observation = NativeServiceObservation {
            installed: InstallationObservation::Installed,
            manager_state: registration.manager_state,
            definition: Some(registration.definition.clone()),
            account: Some(registration.definition.account.clone()),
            boot_enabled: true,
            core: registration.core.clone(),
            core_unknown: registration.core_unknown.clone(),
            diagnostic_location: Some("fake-manager:test-log".to_owned()),
            pending_operation: registration.pending,
        };
        if state.stop_dispatched && state.stop_unknown_observations > 0 {
            state.stop_unknown_observations -= 1;
            observation.core = None;
            observation.core_unknown =
                Some("runtime lease is held but ownership metadata is missing".into());
        }
        Ok(observation)
    }

    fn mutate(
        &self,
        action: ServiceAction,
        manager: ServiceManagerKind,
        service_id: &str,
        definition: Option<&ServiceDefinition>,
        _deadline: Deadline,
        _clock: &dyn MonotonicClock,
    ) -> Result<(), LifecycleError> {
        let mut state = self
            .state
            .lock()
            .expect("fake manager mutex should not be poisoned");
        state.overlapping_mutations |= state.active_mutations != 0;
        state.active_mutations += 1;
        state.mutation_count += 1;
        let failure = state.failures.get(&action).copied();
        if let Some(failure) = failure {
            state.active_mutations -= 1;
            return Err(match failure {
                Failure::PermissionDenied => LifecycleError::AccessDenied("fake manager".into()),
                Failure::ManagerUnavailable => {
                    LifecycleError::ManagerUnavailable("fake manager unavailable".into())
                }
                Failure::StopTimeout => LifecycleError::DeadlineExpired {
                    phase: "service stop dispatch",
                },
                Failure::StartNotReady => LifecycleError::UnsafeOwnership(
                    "installed account cannot access configured data or credentials".into(),
                ),
            });
        }

        let key = (manager, service_id.to_owned());
        match action {
            ServiceAction::Install => {
                let definition = definition.ok_or_else(|| {
                    LifecycleError::UnsafeOwnership("install requires a definition".into())
                })?;
                state
                    .registrations
                    .entry(key)
                    .or_insert_with(|| FakeRegistration {
                        definition: definition.clone(),
                        manager_state: NativeManagerState::Stopped,
                        core: None,
                        core_unknown: None,
                        pending: false,
                    });
            }
            ServiceAction::Start => {
                let entry = state
                    .registrations
                    .get_mut(&key)
                    .ok_or(LifecycleError::ServiceNotInstalled)?;
                entry.manager_state = NativeManagerState::Running;
                entry.core = Some(core_observation(
                    &entry.definition,
                    CoreMode::SystemService,
                    ObservedState::Ready,
                ));
            }
            ServiceAction::Stop => {
                state.stop_dispatched = true;
                let entry = state
                    .registrations
                    .get_mut(&key)
                    .ok_or(LifecycleError::ServiceNotInstalled)?;
                entry.manager_state = NativeManagerState::Stopped;
                entry.core = None;
            }
            ServiceAction::Remove => {
                let entry = state
                    .registrations
                    .get(&key)
                    .ok_or(LifecycleError::ServiceNotInstalled)?;
                if entry.manager_state != NativeManagerState::Stopped {
                    state.active_mutations -= 1;
                    return Err(LifecycleError::UnsafeOwnership(
                        "service removal requires confirmed stop".into(),
                    ));
                }
                state.registrations.remove(&key);
            }
        }
        state.active_mutations -= 1;
        Ok(())
    }
}

fn core_observation(
    definition: &ServiceDefinition,
    mode: CoreMode,
    state: ObservedState,
) -> CoreObservation {
    CoreObservation {
        mode,
        state,
        instance_id: Some(uuid::Uuid::new_v4()),
        owner: None,
        process: None,
        data_dir: definition.data_dir.clone(),
        listen_address: Some("http://127.0.0.1:18702".into()),
        diagnostic_location: None,
        identity_verified: true,
    }
}

struct Fixture {
    _lifecycle: LifecycleFixture,
    manager: FakeManager,
    controller: ServiceController,
    definition: ServiceDefinition,
    clock: TestClock,
}

impl Fixture {
    fn new() -> Self {
        let lifecycle = LifecycleFixture::new().expect("isolated lifecycle fixture");
        let clock = TestClock::default();
        let manager = FakeManager::default();
        let controller = ServiceController::with_adapters(
            lifecycle.root().join("machine-services"),
            Arc::new(manager.clone()),
            Arc::new(clock.clone()),
            Arc::new(AdvancingWaiter(clock.clone())),
        );
        let data_dir = lifecycle.data_dir();
        let definition = ServiceDefinition::new(
            ServiceManagerKind::WindowsScm,
            "KakuneCore",
            std::env::current_exe()
                .expect("test executable path")
                .as_path(),
            vec![
                "service".into(),
                "run".into(),
                "--data-dir".into(),
                data_dir.display().to_string(),
            ],
            "LocalSystem",
            data_dir.as_path(),
        )
        .expect("test service definition");
        Self {
            _lifecycle: lifecycle,
            manager,
            controller,
            definition,
            clock,
        }
    }
}

#[test]
fn stop_waits_for_a_transient_metadata_gap_without_redispatching() {
    let fixture = Fixture::new();
    fixture.controller.install(&fixture.definition).unwrap();
    fixture
        .controller
        .start(fixture.definition.manager, &fixture.definition.service_id)
        .unwrap();
    fixture
        .manager
        .state
        .lock()
        .unwrap()
        .stop_unknown_observations = 1;
    let before = fixture.manager.metrics().0;
    let stopped = fixture
        .controller
        .stop(fixture.definition.manager, &fixture.definition.service_id)
        .unwrap();
    assert_eq!(
        stopped.observation.manager_state,
        NativeManagerState::Stopped
    );
    assert!(fixture.clock.now() > Duration::ZERO);
    assert_eq!(fixture.manager.metrics().0, before + 1);
}

#[test]
fn persistent_unknown_exit_expires_the_original_grace_and_preserves_registration() {
    let fixture = Fixture::new();
    fixture.controller.install(&fixture.definition).unwrap();
    fixture
        .controller
        .start(fixture.definition.manager, &fixture.definition.service_id)
        .unwrap();
    fixture
        .manager
        .state
        .lock()
        .unwrap()
        .stop_unknown_observations = usize::MAX;
    let before = fixture.manager.metrics().0;
    let error = fixture
        .controller
        .stop(fixture.definition.manager, &fixture.definition.service_id)
        .unwrap_err();
    assert!(matches!(error, LifecycleError::DeadlineExpired { .. }));
    assert_eq!(
        fixture.clock.now(),
        kakune_core::lifecycle::CORE_GRACE_BUDGET
    );
    assert!(
        fixture
            .controller
            .uninstall(fixture.definition.manager, &fixture.definition.service_id)
            .is_err()
    );
    assert_eq!(fixture.manager.metrics().0, before + 1);
    assert_eq!(
        fixture.manager.manager_state(&fixture.definition),
        NativeManagerState::Stopped
    );
}

#[test]
fn all_five_service_commands_cover_install_start_status_stop_and_uninstall() {
    let fixture = Fixture::new();
    let installed = fixture
        .controller
        .install(&fixture.definition)
        .expect("install should register an automatic stopped service");
    assert_eq!(installed.operation.outcome, OperationOutcome::Success);
    assert_eq!(
        installed.observation.manager_state,
        NativeManagerState::Stopped
    );

    let started = fixture
        .controller
        .start(fixture.definition.manager, &fixture.definition.service_id)
        .expect("start should wait until Core is ready");
    assert_eq!(
        started.observation.manager_state,
        NativeManagerState::Running
    );
    assert_eq!(
        started.observation.core.as_ref().map(|core| core.state),
        Some(ObservedState::Ready)
    );

    let status = fixture
        .controller
        .status(fixture.definition.manager, &fixture.definition.service_id)
        .expect("status should report native and Core observations separately");
    assert_eq!(status.installed, InstallationObservation::Installed);
    assert_eq!(status.manager_state, NativeManagerState::Running);
    assert_eq!(status.account.as_deref(), Some("LocalSystem"));
    assert_eq!(
        status.core.as_ref().map(|core| core.state),
        Some(ObservedState::Ready)
    );

    fixture
        .controller
        .stop(fixture.definition.manager, &fixture.definition.service_id)
        .expect("stop should confirm Core exit and native stopped state");
    let removed = fixture
        .controller
        .uninstall(fixture.definition.manager, &fixture.definition.service_id)
        .expect("uninstall should remove only after stop");
    assert_eq!(
        removed.observation.installed,
        InstallationObservation::NotInstalled
    );
}

#[test]
fn equivalent_install_and_repeated_no_ops_do_not_mutate_the_registration() {
    let fixture = Fixture::new();
    fixture.controller.install(&fixture.definition).unwrap();
    let mutations_after_install = fixture.manager.metrics().0;

    let repeated = fixture.controller.install(&fixture.definition).unwrap();
    assert_eq!(repeated.operation.outcome, OperationOutcome::Noop);
    assert_eq!(fixture.manager.metrics().0, mutations_after_install);

    let absent = Fixture::new();
    assert_eq!(
        absent
            .controller
            .stop(absent.definition.manager, &absent.definition.service_id)
            .unwrap()
            .operation
            .outcome,
        OperationOutcome::Noop
    );
    assert_eq!(
        absent
            .controller
            .uninstall(absent.definition.manager, &absent.definition.service_id)
            .unwrap()
            .operation
            .outcome,
        OperationOutcome::Noop
    );
}

#[test]
fn conflicting_definition_is_not_rewritten_and_absent_start_is_an_error() {
    let fixture = Fixture::new();
    fixture.controller.install(&fixture.definition).unwrap();
    let different_path = fixture._lifecycle.root().join("different-service-data");
    let conflicting = ServiceDefinition::new(
        fixture.definition.manager,
        &fixture.definition.service_id,
        &fixture.definition.executable,
        vec![
            "service".into(),
            "run".into(),
            "--data-dir".into(),
            different_path.display().to_string(),
        ],
        &fixture.definition.account,
        &different_path,
    )
    .unwrap();
    let before = fixture.manager.metrics().0;
    assert!(matches!(
        fixture.controller.install(&conflicting),
        Err(LifecycleError::ConflictingServiceDefinition(_))
    ));
    assert_eq!(fixture.manager.metrics().0, before);

    let absent = Fixture::new();
    assert!(matches!(
        absent
            .controller
            .start(absent.definition.manager, &absent.definition.service_id),
        Err(LifecycleError::ServiceNotInstalled)
    ));
}

#[test]
fn manager_running_is_not_reported_as_core_ready() {
    let fixture = Fixture::new();
    fixture.controller.install(&fixture.definition).unwrap();
    fixture.manager.set_core_state(
        &fixture.definition,
        CoreMode::SystemService,
        ObservedState::Starting,
    );
    let status = fixture
        .controller
        .status(fixture.definition.manager, &fixture.definition.service_id)
        .unwrap();
    assert_eq!(status.manager_state, NativeManagerState::Stopped);
    assert_eq!(
        status.core.as_ref().map(|core| core.state),
        Some(ObservedState::Starting)
    );
}

#[test]
fn status_is_read_only_even_when_the_service_journal_root_is_absent() {
    let fixture = Fixture::new();
    let root = fixture.controller.state_root().to_path_buf();
    assert!(!root.exists());
    let status = fixture
        .controller
        .status(fixture.definition.manager, &fixture.definition.service_id)
        .unwrap();
    assert_eq!(status.installed, InstallationObservation::NotInstalled);
    assert!(!root.exists());
}

#[test]
fn installed_account_data_or_credential_failure_keeps_service_stopped() {
    let fixture = Fixture::new();
    fixture.controller.install(&fixture.definition).unwrap();
    fixture
        .manager
        .fail(ServiceAction::Start, Failure::StartNotReady);
    assert!(matches!(
        fixture
            .controller
            .start(fixture.definition.manager, &fixture.definition.service_id),
        Err(LifecycleError::UnsafeOwnership(_))
    ));
    assert_eq!(
        fixture.manager.manager_state(&fixture.definition),
        NativeManagerState::Stopped
    );
}

#[test]
fn permission_and_unavailable_manager_errors_remain_distinct() {
    let fixture = Fixture::new();
    fixture
        .manager
        .fail(ServiceAction::Install, Failure::PermissionDenied);
    assert!(matches!(
        fixture.controller.install(&fixture.definition),
        Err(LifecycleError::AccessDenied(_))
    ));

    let unavailable = Fixture::new();
    unavailable
        .manager
        .fail(ServiceAction::Install, Failure::ManagerUnavailable);
    assert!(matches!(
        unavailable.controller.install(&unavailable.definition),
        Err(LifecycleError::ManagerUnavailable(_))
    ));
}

#[test]
fn a_user_daemon_owning_the_service_data_directory_is_a_cross_mode_conflict() {
    let fixture = Fixture::new();
    fixture.controller.install(&fixture.definition).unwrap();
    fixture.manager.set_core_state(
        &fixture.definition,
        CoreMode::UserDaemon,
        ObservedState::Ready,
    );
    assert!(matches!(
        fixture
            .controller
            .start(fixture.definition.manager, &fixture.definition.service_id),
        Err(LifecycleError::UnsafeOwnership(_))
    ));
}

#[test]
fn stop_and_uninstall_do_not_mutate_another_core_mode() {
    let fixture = Fixture::new();
    fixture.controller.install(&fixture.definition).unwrap();
    fixture.manager.set_core_state(
        &fixture.definition,
        CoreMode::UserDaemon,
        ObservedState::Ready,
    );
    let before = fixture.manager.metrics().0;

    assert!(matches!(
        fixture
            .controller
            .stop(fixture.definition.manager, &fixture.definition.service_id),
        Err(LifecycleError::UnsafeOwnership(_))
    ));
    assert!(matches!(
        fixture
            .controller
            .uninstall(fixture.definition.manager, &fixture.definition.service_id),
        Err(LifecycleError::UnsafeOwnership(_))
    ));
    assert_eq!(fixture.manager.metrics().0, before);
}

#[test]
fn a_failed_stop_never_removes_the_registration() {
    let fixture = Fixture::new();
    fixture.controller.install(&fixture.definition).unwrap();
    fixture
        .controller
        .start(fixture.definition.manager, &fixture.definition.service_id)
        .unwrap();
    fixture
        .manager
        .fail(ServiceAction::Stop, Failure::StopTimeout);
    assert!(
        fixture
            .controller
            .uninstall(fixture.definition.manager, &fixture.definition.service_id)
            .is_err()
    );
    assert_eq!(
        fixture
            .controller
            .status(fixture.definition.manager, &fixture.definition.service_id)
            .unwrap()
            .installed,
        InstallationObservation::Installed
    );
}

#[test]
fn unknown_core_ownership_is_not_treated_as_absent_for_uninstall() {
    let fixture = Fixture::new();
    fixture.controller.install(&fixture.definition).unwrap();
    fixture
        .manager
        .set_core_unknown(&fixture.definition, "data directory is inaccessible");
    let before = fixture.manager.metrics().0;

    assert!(matches!(
        fixture
            .controller
            .uninstall(fixture.definition.manager, &fixture.definition.service_id),
        Err(LifecycleError::UnsafeOwnership(_))
    ));
    assert_eq!(fixture.manager.metrics().0, before);
    assert_eq!(
        fixture
            .controller
            .status(fixture.definition.manager, &fixture.definition.service_id)
            .unwrap()
            .core_unknown
            .as_deref(),
        Some("data directory is inaccessible")
    );
}

#[test]
fn an_already_running_but_unverified_core_is_not_started_again() {
    let fixture = Fixture::new();
    fixture.controller.install(&fixture.definition).unwrap();
    fixture
        .manager
        .set_manager_state(&fixture.definition, NativeManagerState::Running);
    fixture.manager.set_core_state(
        &fixture.definition,
        CoreMode::SystemService,
        ObservedState::Starting,
    );
    let before = fixture.manager.metrics().0;

    assert!(matches!(
        fixture
            .controller
            .start(fixture.definition.manager, &fixture.definition.service_id),
        Err(LifecycleError::ServiceStateUnconfirmed)
    ));
    assert_eq!(fixture.manager.metrics().0, before);
}

#[test]
fn pending_native_manager_work_blocks_noops_and_duplicate_mutations() {
    let fixture = Fixture::new();
    fixture.controller.install(&fixture.definition).unwrap();
    fixture.manager.set_pending(&fixture.definition, true);
    let before = fixture.manager.metrics().0;

    let status = fixture
        .controller
        .status(fixture.definition.manager, &fixture.definition.service_id)
        .unwrap();
    assert!(status.pending_operation);
    assert!(matches!(
        fixture.controller.install(&fixture.definition),
        Err(LifecycleError::ServiceStateUnconfirmed)
    ));
    assert!(matches!(
        fixture
            .controller
            .start(fixture.definition.manager, &fixture.definition.service_id),
        Err(LifecycleError::ServiceStateUnconfirmed)
    ));
    assert!(matches!(
        fixture
            .controller
            .stop(fixture.definition.manager, &fixture.definition.service_id),
        Err(LifecycleError::ServiceStateUnconfirmed)
    ));
    assert!(matches!(
        fixture
            .controller
            .uninstall(fixture.definition.manager, &fixture.definition.service_id),
        Err(LifecycleError::ServiceStateUnconfirmed)
    ));
    assert_eq!(fixture.manager.metrics().0, before);
}

#[test]
fn competing_installs_for_one_service_key_do_not_ignore_different_data_paths() {
    let fixture = Fixture::new();
    let other_path = fixture._lifecycle.root().join("different-service-data");
    let conflicting = ServiceDefinition::new(
        fixture.definition.manager,
        &fixture.definition.service_id,
        &fixture.definition.executable,
        vec![
            "service".into(),
            "run".into(),
            "--data-dir".into(),
            other_path.display().to_string(),
        ],
        &fixture.definition.account,
        &other_path,
    )
    .unwrap();
    let controller = Arc::new(fixture.controller);
    let first_controller = controller.clone();
    let first_definition = fixture.definition.clone();
    let first = thread::spawn(move || first_controller.install(&first_definition));
    let second_controller = controller.clone();
    let second = thread::spawn(move || second_controller.install(&conflicting));

    let outcomes = [first.join().unwrap(), second.join().unwrap()];
    assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| matches!(result, Err(LifecycleError::ConflictingServiceDefinition(_))))
            .count(),
        1
    );
}

#[test]
fn uninstall_preserves_data_and_execution_files() {
    let fixture = Fixture::new();
    fixture.controller.install(&fixture.definition).unwrap();
    let retained = fixture.definition.data_dir.join("retained-execution.json");
    std::fs::create_dir_all(&fixture.definition.data_dir).unwrap();
    std::fs::write(&retained, b"execution-history").unwrap();

    fixture
        .controller
        .uninstall(fixture.definition.manager, &fixture.definition.service_id)
        .unwrap();

    assert_eq!(std::fs::read(retained).unwrap(), b"execution-history");
}

#[test]
fn simultaneous_equivalent_installs_share_the_service_wide_lock() {
    let fixture = Fixture::new();
    let controller = Arc::new(fixture.controller);
    let definition = Arc::new(fixture.definition);
    let first_controller = controller.clone();
    let first_definition = definition.clone();
    let first = thread::spawn(move || first_controller.install(&first_definition));
    let second_controller = controller.clone();
    let second_definition = definition.clone();
    let second = thread::spawn(move || second_controller.install(&second_definition));

    let results = [
        first.join().unwrap().unwrap(),
        second.join().unwrap().unwrap(),
    ];
    assert_eq!(
        results
            .iter()
            .filter(|result| result.operation.outcome == OperationOutcome::Success)
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|result| result.operation.outcome == OperationOutcome::Noop)
            .count(),
        1
    );
}

#[test]
fn concurrent_stop_and_uninstall_remain_serialized_and_truthful() {
    let fixture = Fixture::new();
    fixture.controller.install(&fixture.definition).unwrap();
    fixture
        .controller
        .start(fixture.definition.manager, &fixture.definition.service_id)
        .unwrap();
    let controller = Arc::new(fixture.controller);
    let definition = Arc::new(fixture.definition);
    let barrier = Arc::new(std::sync::Barrier::new(3));

    let stop_controller = controller.clone();
    let stop_definition = definition.clone();
    let stop_barrier = barrier.clone();
    let stop = thread::spawn(move || {
        stop_barrier.wait();
        stop_controller.stop(stop_definition.manager, &stop_definition.service_id)
    });
    let remove_controller = controller.clone();
    let remove_definition = definition.clone();
    let remove_barrier = barrier.clone();
    let uninstall = thread::spawn(move || {
        remove_barrier.wait();
        remove_controller.uninstall(remove_definition.manager, &remove_definition.service_id)
    });
    barrier.wait();

    assert!(stop.join().unwrap().is_ok());
    assert!(uninstall.join().unwrap().is_ok());
    assert!(
        !fixture.manager.metrics().1,
        "native mutations must not overlap"
    );
    assert_eq!(
        controller
            .status(definition.manager, &definition.service_id)
            .unwrap()
            .installed,
        InstallationObservation::NotInstalled
    );
}

#[test]
fn concurrent_install_and_stop_are_serialized_without_losing_the_registration() {
    let fixture = Fixture::new();
    let controller = Arc::new(fixture.controller);
    let definition = Arc::new(fixture.definition);
    let barrier = Arc::new(std::sync::Barrier::new(3));

    let install_controller = controller.clone();
    let install_definition = definition.clone();
    let install_barrier = barrier.clone();
    let install = thread::spawn(move || {
        install_barrier.wait();
        install_controller.install(&install_definition)
    });
    let stop_controller = controller.clone();
    let stop_definition = definition.clone();
    let stop_barrier = barrier.clone();
    let stop = thread::spawn(move || {
        stop_barrier.wait();
        stop_controller.stop(stop_definition.manager, &stop_definition.service_id)
    });
    barrier.wait();

    assert!(install.join().unwrap().is_ok());
    assert!(stop.join().unwrap().is_ok());
    assert!(
        !fixture.manager.metrics().1,
        "native mutations must not overlap"
    );
    assert_eq!(
        controller
            .status(definition.manager, &definition.service_id)
            .unwrap()
            .installed,
        InstallationObservation::Installed
    );
}

#[allow(dead_code)]
fn _assert_paths_are_absolute(path: &Path) {
    let _: &PathBuf = &path.to_path_buf();
    assert!(path.is_absolute());
}
