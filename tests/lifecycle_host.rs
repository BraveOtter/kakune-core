mod support;

use std::{
    future::Future,
    net::SocketAddr,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

use kakune_core::{
    Store,
    lifecycle::{
        CoreMode, CoreStartupOptions, Deadline, SystemMonotonicClock, ThreadWaiter,
        host::{self, HostAdapter, HostEvent, HostOptions},
        launch::{self, BackgroundLauncher},
        ownership::{self, OwnershipRead, RuntimeLease},
        process::RetainedChildRegistry,
    },
};
use support::lifecycle::{FakeClock, LifecycleFixture};

impl kakune_core::lifecycle::MonotonicClock for FakeClock {
    fn now(&self) -> Duration {
        support::lifecycle::MonotonicClock::now(self)
    }
}

fn options(data_dir: PathBuf, mode: CoreMode) -> HostOptions {
    HostOptions {
        data_dir,
        mode,
        config_file: None,
        listen: Some("127.0.0.1:0".parse().unwrap()),
        tls_certificate: None,
        tls_private_key: None,
        allowed_hosts: vec![],
        diagnostic_location: None,
    }
}

#[derive(Default)]
struct Gate(Arc<(Mutex<bool>, Condvar)>);
impl Gate {
    fn release(&self) {
        let (lock, changed) = &*self.0;
        *lock.lock().unwrap() = true;
        changed.notify_all();
    }
}
impl Drop for Gate {
    fn drop(&mut self) {
        self.release();
    }
}

struct RecordingHost {
    data_dir: PathBuf,
    fail: &'static str,
    gate: Option<Arc<(Mutex<bool>, Condvar)>>,
    ready: tokio::sync::Notify,
    stopping: tokio::sync::Notify,
    endpoints: Mutex<Vec<SocketAddr>>,
    events: Mutex<Vec<&'static str>>,
}

impl RecordingHost {
    fn new(data_dir: PathBuf, fail: &'static str) -> Self {
        Self {
            data_dir,
            fail,
            gate: None,
            ready: tokio::sync::Notify::new(),
            stopping: tokio::sync::Notify::new(),
            endpoints: Mutex::new(vec![]),
            events: Mutex::new(vec![]),
        }
    }
    fn capture_endpoints(&self) {
        let record =
            ownership::read_ownership_record(&self.data_dir.join(".kakune/ownership.json"))
                .unwrap();
        if let Some(OwnershipRead::Current(record)) = record {
            let mut addresses = self.endpoints.lock().unwrap();
            if let Some(address) = record.instance.listen_address {
                addresses.push(address.trim_start_matches("http://").parse().unwrap());
            }
            if let Some(address) = record.instance.control_endpoint {
                addresses.push(address.parse().unwrap());
            }
        }
    }
}

impl HostAdapter for RecordingHost {
    fn report(&self, event: HostEvent) -> Result<(), String> {
        let name = match event {
            HostEvent::Claimed => "claimed",
            HostEvent::Prepared(_) => {
                self.capture_endpoints();
                "prepared"
            }
            HostEvent::Ready { lifecycle, .. } => {
                self.capture_endpoints();
                if let Some(gate) = self.gate.clone() {
                    let permit = lifecycle
                        .admission()
                        .admit(kakune_core::lifecycle::execution::TrackedExecutionState::Active)
                        .unwrap();
                    let store = Store::open(self.data_dir.clone()).unwrap();
                    tokio::task::spawn_blocking(move || {
                        let (lock, changed) = &*gate;
                        let open = lock.lock().unwrap();
                        let _open = changed.wait_while(open, |open| !*open).unwrap();
                        let source = include_str!("../examples/write-note.kakune.yaml");
                        let workflow = kakune_core::WorkflowDocument::parse(source).unwrap();
                        assert!(
                            store
                                .create_workflow(&workflow, source, "disabled")
                                .unwrap()
                                .is_some()
                        );
                        std::fs::write(
                            store.data_dir().join("worker-finished"),
                            b"persisted before lease release",
                        )
                        .unwrap();
                        drop(permit);
                    });
                }
                self.ready.notify_one();
                "ready"
            }
            HostEvent::Stopping => {
                self.stopping.notify_one();
                "stopping"
            }
            HostEvent::Stopped => "stopped",
            HostEvent::Diagnostic(_) => "diagnostic",
        };
        self.events.lock().unwrap().push(name);
        if self.fail == name {
            Err(format!(
                "injected {name} publication failure (broken parent pipe)"
            ))
        } else {
            Ok(())
        }
    }
    fn parent_commit(
        &self,
        _: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + '_>> {
        Box::pin(async move {
            match self.fail {
                "parent-eof" => Ok(false),
                "parent-error" => Err("injected parent read failure".into()),
                _ => Ok(true),
            }
        })
    }
}

async fn assert_released(host: &RecordingHost) {
    let endpoints = host.endpoints.lock().unwrap().clone();
    for address in endpoints {
        assert!(
            tokio::net::TcpStream::connect(address).await.is_err(),
            "listener {address} escaped cleanup"
        );
    }
    let clock = SystemMonotonicClock::default();
    let lease = RuntimeLease::acquire_with_deadline(
        &host.data_dir,
        uuid::Uuid::new_v4(),
        Deadline::after(&clock, Duration::ZERO),
        &clock,
        &ThreadWaiter,
    )
    .unwrap();
    assert!(!host.data_dir.join(".kakune/ownership.json").exists());
    drop(lease);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ready_publication_failure_drains_workers_before_releasing_ownership() {
    let fixture = LifecycleFixture::new().unwrap();
    let gate = Gate::default();
    let mut adapter = RecordingHost::new(fixture.data_dir(), "ready");
    adapter.gate = Some(gate.0.clone());
    let adapter = Arc::new(adapter);
    let core_adapter = adapter.clone();
    let config = options(fixture.data_dir(), CoreMode::UserDaemon);
    let mut core = tokio::spawn(host::serve(config, core_adapter, std::future::pending()));
    tokio::time::timeout(Duration::from_secs(5), adapter.stopping.notified())
        .await
        .unwrap();
    let clock = SystemMonotonicClock::default();
    let competing = RuntimeLease::acquire_with_deadline(
        &fixture.data_dir(),
        uuid::Uuid::new_v4(),
        Deadline::after(&clock, Duration::ZERO),
        &clock,
        &ThreadWaiter,
    );
    assert!(
        competing.is_err(),
        "broken READY publication released a live worker's lease"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut core)
            .await
            .is_err()
    );
    assert!(!adapter.events.lock().unwrap().contains(&"stopped"));
    gate.release();
    let error = tokio::time::timeout(Duration::from_secs(5), core)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(
        error.contains("injected ready publication failure"),
        "{error}"
    );
    assert!(fixture.data_dir().join("worker-finished").is_file());
    assert_eq!(
        Store::open(fixture.data_dir())
            .unwrap()
            .list_workflows()
            .unwrap()
            .len(),
        1
    );
    assert_released(&adapter).await;
}

#[tokio::test]
async fn prepared_and_parent_commit_failures_join_transport_and_control() {
    for failure in ["prepared", "parent-eof", "parent-error"] {
        let fixture = LifecycleFixture::new().unwrap();
        let adapter = Arc::new(RecordingHost::new(fixture.data_dir(), failure));
        let error = tokio::time::timeout(
            Duration::from_secs(5),
            host::serve(
                options(fixture.data_dir(), CoreMode::UserDaemon),
                adapter.clone(),
                std::future::pending(),
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(!error.is_empty());
        assert!(!adapter.events.lock().unwrap().contains(&"ready"));
        assert!(adapter.events.lock().unwrap().contains(&"stopped"));
        assert_released(&adapter).await;
    }
}

#[tokio::test]
async fn library_foreground_and_service_hosts_use_injected_shutdown_without_a_terminal() {
    for mode in [CoreMode::Foreground, CoreMode::SystemService] {
        let fixture = LifecycleFixture::new().unwrap();
        let adapter = Arc::new(RecordingHost::new(fixture.data_dir(), ""));
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let core = tokio::spawn(host::serve(
            options(fixture.data_dir(), mode),
            adapter.clone(),
            async {
                let _ = stopped.await;
            },
        ));
        tokio::time::timeout(Duration::from_secs(5), adapter.ready.notified())
            .await
            .unwrap();
        let address = adapter.endpoints.lock().unwrap()[0];
        let response = reqwest::get(format!("http://{address}/health/ready"))
            .await
            .unwrap();
        assert!(response.status().is_success());
        drop(response);
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), core)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            &adapter.events.lock().unwrap()[..],
            ["ready", "stopping", "stopped"]
        );
        assert_released(&adapter).await;
    }
}

fn startup(data_dir: PathBuf) -> CoreStartupOptions {
    std::fs::create_dir_all(&data_dir).unwrap();
    CoreStartupOptions {
        config_file: Some(data_dir.join("config.yaml")),
        data_dir,
        listen_address: Some("127.0.0.1:0".into()),
        tls_certificate: None,
        tls_private_key: None,
        allowed_hosts: vec![],
    }
}

struct SlowPreparation(FakeClock);
impl BackgroundLauncher for SlowPreparation {
    fn command(&self, _: &CoreStartupOptions, _: &Path) -> Result<tokio::process::Command, String> {
        self.0.advance(Duration::from_secs(30));
        Ok(tokio::process::Command::new("must-not-be-launched"))
    }
}

#[tokio::test]
async fn launch_preparation_consumes_the_original_readiness_deadline_before_spawn() {
    let fixture = LifecycleFixture::new().unwrap();
    let clock = FakeClock::default();
    let deadline = Deadline::after(&clock, Duration::from_secs(30));
    let retained = RetainedChildRegistry::default();
    let error = match launch::start_background_with(
        &startup(fixture.data_dir()),
        &SlowPreparation(clock.clone()),
        deadline,
        &clock,
        &retained,
    )
    .await
    {
        Ok(_) => panic!("expired launch must fail"),
        Err(error) => error,
    };
    assert!(error.contains("readiness deadline expired"), "{error}");
    assert!(retained.snapshots().await.is_empty());
}

struct StalledChild;
impl BackgroundLauncher for StalledChild {
    fn command(&self, _: &CoreStartupOptions, _: &Path) -> Result<tokio::process::Command, String> {
        let mut command =
            tokio::process::Command::new(env!("CARGO_BIN_EXE_kakune-process-fixture"));
        command.arg("lifecycle-launch-stall");
        Ok(command)
    }
}

#[tokio::test]
async fn readiness_expiry_does_not_renew_cleanup_and_retains_unconfirmed_child_handles() {
    let fixture = LifecycleFixture::new().unwrap();
    let clock = SystemMonotonicClock::default();
    let retained = RetainedChildRegistry::default();
    let start = Instant::now();
    let result = launch::start_background_with(
        &startup(fixture.data_dir()),
        &StalledChild,
        Deadline::after(&clock, Duration::from_millis(100)),
        &clock,
        &retained,
    )
    .await;
    let elapsed = start.elapsed();
    let residuals = retained.snapshots().await;
    // Test-owned teardown gets its own budget; the operation above must not.
    for child in &residuals {
        let teardown = Deadline::after(&clock, Duration::from_secs(5));
        retained
            .wait_until(child.launch_id, teardown, &clock)
            .await
            .unwrap();
        assert!(retained.release_after_exit(child.launch_id).await.unwrap());
    }
    let error = match result {
        Ok(_) => panic!("stalled child cannot become ready"),
        Err(error) => error,
    };
    assert!(error.contains("readiness deadline expired"), "{error}");
    assert!(
        elapsed < Duration::from_secs(1),
        "cleanup renewed the expired phase: {elapsed:?}"
    );
    if !residuals.is_empty() {
        assert!(error.contains("retained"), "{error}");
    }
    assert!(retained.snapshots().await.is_empty());
}
