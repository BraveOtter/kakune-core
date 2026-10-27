//! Real helper processes are kept separate from instantaneous fake-clock tests.
mod support;

use std::{
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use kakune_core::lifecycle::{
    Deadline, InstallationObservation, LifecycleError, MonotonicClock, NativeManagerState,
    SystemMonotonicClock,
    service::{
        AttemptStatus, NativeMutation, NativeServiceObservation, ServiceAction,
        ServiceAttemptJournal, ServiceController, ServiceDefinition, ServiceManagerAdapter,
        ServiceManagerKind, ServiceStateRoot,
        helper::{HelperLauncher, retained_helper_count},
    },
};
use support::lifecycle::LifecycleFixture;

// The retained-handle registry is process-global. Only these process tests own
// it; serialization prevents another test's helper becoming our cleanup target.
static PROCESS_FIXTURES: Mutex<()> = Mutex::new(());

struct ProcessFixture {
    fixture: Option<LifecycleFixture>,
    _guard: MutexGuard<'static, ()>,
}

impl ProcessFixture {
    fn new() -> Self {
        let guard = PROCESS_FIXTURES
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(retained_helper_count(), 0, "previous fixture left a helper");
        Self {
            fixture: Some(LifecycleFixture::new().expect("isolated process fixture")),
            _guard: guard,
        }
    }

    fn fixture(&self) -> &LifecycleFixture {
        self.fixture.as_ref().expect("fixture remains owned")
    }
}

impl Drop for ProcessFixture {
    fn drop(&mut self) {
        // This is test teardown, not a renewed production helper/Core budget.
        // The deliberately unreapable fixture naturally finishes after 5 s.
        let deadline = Instant::now() + Duration::from_secs(10);
        while retained_helper_count() != 0 {
            if Instant::now() >= deadline {
                // Never let fixture destruction delete an uncertain worker's data.
                std::mem::forget(self.fixture.take());
                if !std::thread::panicking() {
                    panic!("test-owned helper exit unconfirmed; fixture preserved");
                }
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
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
        panic!("a native operation must never bypass its owned fixture helper")
    }
}

struct FixtureHelperLauncher {
    mode: &'static str,
    fail_cleanup: bool,
}
impl HelperLauncher for FixtureHelperLauncher {
    fn command(
        &self,
        journal: &ServiceAttemptJournal,
        root: &std::path::Path,
    ) -> Result<std::process::Command, LifecycleError> {
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_kakune-process-fixture"));
        command.arg("service-helper-fixture").arg(root).args([
            journal.manager.key(),
            &journal.native_service_id,
            &journal.attempt_id.to_string(),
            self.mode,
        ]);
        Ok(command)
    }
    fn terminate(&self, child: &mut dyn process_wrap::std::ChildWrapper) -> std::io::Result<()> {
        if self.fail_cleanup {
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "injected failed retained-handle cleanup",
            ))
        } else {
            child.kill()
        }
    }
}

struct AcceleratedClock(Instant);
impl MonotonicClock for AcceleratedClock {
    fn now(&self) -> Duration {
        self.0.elapsed() * 10
    }
}

fn definition_for(data: PathBuf) -> ServiceDefinition {
    ServiceDefinition::new(
        ServiceManagerKind::LinuxSystemd,
        "kakune-core.service",
        &std::env::current_exe().unwrap(),
        vec![
            "service".into(),
            "run".into(),
            "--data-dir".into(),
            data.display().to_string(),
        ],
        "root",
        &data,
    )
    .unwrap()
}

fn installed(
    definition: &ServiceDefinition,
    state: NativeManagerState,
) -> NativeServiceObservation {
    NativeServiceObservation {
        installed: InstallationObservation::Installed,
        manager_state: state,
        definition: Some(definition.clone()),
        account: Some(definition.account.clone()),
        boot_enabled: true,
        core: None,
        core_unknown: None,
        diagnostic_location: Some("fake-manager:process-fixture".into()),
        pending_operation: false,
    }
}

#[test]
fn real_fixture_helpers_journal_each_native_step_without_contacting_managers() {
    let owner = ProcessFixture::new();
    for mode in ["complete", "output-cap", "crash", "hang"] {
        let root = owner.fixture().root().join(mode);
        let clock: Arc<dyn MonotonicClock> = if mode == "hang" {
            Arc::new(AcceleratedClock(Instant::now()))
        } else {
            Arc::new(SystemMonotonicClock::default())
        };
        let controller = ServiceController::with_components(
            ServiceStateRoot::isolated(root.clone()).unwrap(),
            Arc::new(FixtureNativeManager(root.clone())),
            clock,
            Arc::new(kakune_core::lifecycle::ThreadWaiter),
        )
        .with_helper_launcher(Arc::new(FixtureHelperLauncher {
            mode,
            fail_cleanup: false,
        }));
        let definition = definition_for(owner.fixture().data_dir().join(mode));
        let result = controller.install(&definition);
        assert!(
            root.join("authorized-step").exists(),
            "mode {mode}: {result:?}"
        );
        if matches!(mode, "complete" | "output-cap") {
            assert!(result.is_ok(), "mode {mode}: {result:?}");
            assert!(controller.read_attempt(&definition).unwrap().is_none());
            let marker = definition.data_dir.join("retained-workflow.yaml");
            std::fs::create_dir_all(&definition.data_dir).unwrap();
            std::fs::write(&marker, b"retained workflow").unwrap();
            controller
                .uninstall(definition.manager, &definition.service_id)
                .unwrap();
            assert_eq!(std::fs::read(marker).unwrap(), b"retained workflow");
        } else {
            assert!(result.is_err());
            let journal = controller.read_attempt(&definition).unwrap().unwrap();
            assert_eq!(journal.status, AttemptStatus::OutcomeUnknown);
            // A 100 ms scaled cleanup reserve does not guarantee OS exit/reap.
            // If exit was not confirmed, the returned retained handle and the
            // journal must agree; fixture ownership lasts until actual reaping.
            if let Some(pid) = journal.helper_pid {
                assert!(
                    matches!(result, Err(LifecycleError::HelperUnreaped { pid: observed }) if observed == pid)
                );
            }
            let before = std::fs::read(controller.attempt_path(&definition)).unwrap();
            controller
                .status(definition.manager, &definition.service_id)
                .unwrap();
            assert_eq!(
                std::fs::read(controller.attempt_path(&definition)).unwrap(),
                before
            );
            assert!(matches!(
                controller.install(&definition),
                Err(LifecycleError::UnresolvedServiceAttempt(_))
            ));
        }
    }
}

#[test]
fn failed_helper_reap_retains_ownership_and_a_durable_mutation_barrier() {
    let owner = ProcessFixture::new();
    let root = owner.fixture().root().join("unreaped-helper");
    let controller = ServiceController::with_components(
        ServiceStateRoot::isolated(root.clone()).unwrap(),
        Arc::new(FixtureNativeManager(root)),
        Arc::new(AcceleratedClock(Instant::now())),
        Arc::new(kakune_core::lifecycle::ThreadWaiter),
    )
    .with_helper_launcher(Arc::new(FixtureHelperLauncher {
        mode: "hang",
        fail_cleanup: true,
    }));
    let definition = definition_for(owner.fixture().data_dir());
    assert!(matches!(
        controller.install(&definition),
        Err(LifecycleError::HelperUnreaped { .. })
    ));
    let journal = controller.read_attempt(&definition).unwrap().unwrap();
    assert!(journal.helper_pid.is_some());
    assert_eq!(journal.status, AttemptStatus::OutcomeUnknown);
    assert!(retained_helper_count() > 0);
    // ProcessFixture retains the data and serial guard until observed exit.
}

#[test]
fn partial_native_install_and_removal_failures_never_rollback_or_erase_the_journal() {
    let owner = ProcessFixture::new();
    for (manager, action, modes) in [
        (
            ServiceManagerKind::WindowsScm,
            ServiceAction::Install,
            &["crash:Register", "crash:Description", "crash:FailurePolicy"][..],
        ),
        (
            ServiceManagerKind::LinuxSystemd,
            ServiceAction::Install,
            &["crash:Publish", "crash:Reload", "crash:Enable"][..],
        ),
        (
            ServiceManagerKind::LinuxSystemd,
            ServiceAction::Remove,
            &["crash:Disable", "crash:Delete", "crash:Reload"][..],
        ),
        (
            ServiceManagerKind::MacosLaunchd,
            ServiceAction::Install,
            &["crash:Publish"][..],
        ),
    ] {
        for &mode in modes {
            let root = owner
                .fixture()
                .root()
                .join(format!("{}-{action:?}-{mode}", manager.key()).replace(':', "-"));
            std::fs::create_dir_all(&root).unwrap();
            let mut definition = definition_for(owner.fixture().data_dir());
            definition.manager = manager;
            definition.service_id = manager.platform_service_id().into();
            definition.account = manager.default_account().into();
            definition.definition_fingerprint = Some(definition.fingerprint().unwrap());
            if action == ServiceAction::Remove {
                std::fs::write(
                    root.join("native-observation.json"),
                    serde_json::to_vec(&installed(&definition, NativeManagerState::Stopped))
                        .unwrap(),
                )
                .unwrap();
            }
            let controller = ServiceController::with_components(
                ServiceStateRoot::isolated(root.clone()).unwrap(),
                Arc::new(FixtureNativeManager(root)),
                Arc::new(SystemMonotonicClock::default()),
                Arc::new(kakune_core::lifecycle::ThreadWaiter),
            )
            .with_helper_launcher(Arc::new(FixtureHelperLauncher {
                mode,
                fail_cleanup: false,
            }));
            let result = if action == ServiceAction::Install {
                controller.install(&definition)
            } else {
                controller.uninstall(manager, &definition.service_id)
            };
            assert!(result.is_err(), "{manager:?} {mode}");
            let journal = controller.read_attempt(&definition).unwrap().unwrap();
            assert_eq!(journal.status, AttemptStatus::OutcomeUnknown);
            assert!(
                journal.step.contains(
                    mode.strip_prefix("crash:")
                        .unwrap()
                        .to_ascii_lowercase()
                        .as_str()
                ) || (mode == "crash:FailurePolicy" && journal.step.contains("failure_policy"))
            );
            let before = std::fs::read(controller.attempt_path(&definition)).unwrap();
            assert!(
                controller
                    .uninstall(manager, &definition.service_id)
                    .is_err()
            );
            assert_eq!(
                std::fs::read(controller.attempt_path(&definition)).unwrap(),
                before,
                "partial native effects must retain their barrier"
            );
        }
    }
}

#[test]
fn a_settled_native_failure_releases_the_barrier_for_a_later_command() {
    let owner = ProcessFixture::new();
    let root = owner.fixture().root().join("settled-native-failure");
    std::fs::create_dir_all(&root).unwrap();
    let definition = definition_for(owner.fixture().data_dir());
    std::fs::write(
        root.join("native-observation.json"),
        serde_json::to_vec(&installed(&definition, NativeManagerState::Failed)).unwrap(),
    )
    .unwrap();
    let controller = ServiceController::with_components(
        ServiceStateRoot::isolated(root.clone()).unwrap(),
        Arc::new(FixtureNativeManager(root.clone())),
        Arc::new(AcceleratedClock(Instant::now())),
        Arc::new(kakune_core::lifecycle::ThreadWaiter),
    )
    .with_helper_launcher(Arc::new(FixtureHelperLauncher {
        mode: "complete",
        fail_cleanup: false,
    }));
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
        status: AttemptStatus::Authorized,
        completion_observation: None,
        residual_resources: vec!["native manager refused the start".into()],
        recovery_guidance: vec!["Inspect the native manager before retrying.".into()],
    };
    let path = controller.attempt_path(&definition);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, serde_json::to_vec(&journal).unwrap()).unwrap();
    let result = controller.stop(definition.manager, &definition.service_id);
    assert!(
        !matches!(result, Err(LifecycleError::UnresolvedServiceAttempt(_))),
        "settled failure must permit dispatch of the later fixture command: {result:?}"
    );
    assert_eq!(
        std::fs::read(root.join("authorized-step")).unwrap(),
        serde_json::to_vec(&NativeMutation::Stop).unwrap()
    );
}
