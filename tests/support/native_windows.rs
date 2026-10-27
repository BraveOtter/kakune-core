//! Windows-only, opt-in failure acceptance against the real CLI and OS.

use super::*;
use kakune_core::lifecycle::{
    Deadline, SystemMonotonicClock, ThreadWaiter,
    ownership::{AdministrativeLease, RuntimeOwnershipRecord, runtime_lease_is_held},
    process::{ProcessEvidenceProvider, SystemProcessEvidence},
};

fn start_daemon(fixture: &NativeFixture, data: &Path, address: &str) -> DaemonGuard {
    let mut guard = DaemonGuard::new(
        fixture.root.clone(),
        data.to_path_buf(),
        fixture.preserve.clone(),
    );
    guard.active = true;
    let output = run_cli(
        &fixture.root,
        &[
            "daemon",
            "--data-dir",
            data.to_str().unwrap(),
            "--config",
            data.join("kakune.yaml").to_str().unwrap(),
            "--listen",
            address,
            "start",
        ],
        Duration::from_secs(40),
    )
    .expect("native daemon start should return");
    assert!(output.status.success(), "{}", cli_text(&output));
    guard
}

fn assert_elapsed(label: &str, elapsed: Duration, seconds: u64) {
    println!("native-failure-timing\t{label}\t{}ms", elapsed.as_millis());
    // These checks distinguish actual expiry from immediate rejection. The
    // small upper tolerance is harness/process scheduling, not a renewed budget.
    assert!(
        elapsed >= Duration::from_secs(seconds),
        "{label}: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(seconds + 6),
        "{label}: {elapsed:?}"
    );
}

fn observed_process(pid: u32) -> Option<kakune_core::lifecycle::process::ProcessEvidence> {
    let clock = SystemMonotonicClock::default();
    SystemProcessEvidence::default()
        .observe(pid, Deadline::after(&clock, Duration::from_secs(5)), &clock)
        .expect("test-owned process identity should be observable")
}

/// Restores test-tampered metadata before the daemon cleanup guard runs.
struct RecordRestore {
    path: PathBuf,
    original: Vec<u8>,
    preserve: Arc<AtomicBool>,
}

impl Drop for RecordRestore {
    fn drop(&mut self) {
        if let Err(error) = fs::write(&self.path, &self.original) {
            self.preserve.store(true, Ordering::SeqCst);
            eprintln!(
                "Cannot restore test-owned record {}: {error}; retain fixture for recovery",
                self.path.display()
            );
        }
    }
}

struct Sentinel {
    child: Child,
    root: PathBuf,
    preserve: Arc<AtomicBool>,
}

impl Sentinel {
    fn spawn(fixture: &NativeFixture) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_kakune-process-fixture"))
            .arg("lifecycle-sentinel")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("owned unrelated-process sentinel should start");
        Self {
            child,
            root: fixture.root.clone(),
            preserve: fixture.preserve.clone(),
        }
    }

    fn finish(&mut self) {
        drop(self.child.stdin.take());
        if let Err(error) = wait_foreground_exit(&mut self.child, Duration::from_secs(5)) {
            self.preserve.store(true, Ordering::SeqCst);
            let _ = fs::write(
                self.root.join("sentinel-recovery.md"),
                format!(
                    "Retained sentinel PID {}: {error}. No Core process was killed.\n",
                    self.child.id()
                ),
            );
        }
    }
}

impl Drop for Sentinel {
    fn drop(&mut self) {
        self.finish();
    }
}

#[test]
#[ignore = "requires explicit native-acceptance opt-in; test-owned Windows processes only"]
fn windows_native_identity_failures_preserve_daemon_and_unrelated_process() {
    require_native_approval(false);
    let fixture = NativeFixture::new().unwrap();
    let address = available_address();
    let (data, _token) = fixture_data(&fixture, "identity-data", &address);
    let mut daemon = start_daemon(&fixture, &data, &address);
    let mut sentinel = Sentinel::spawn(&fixture);
    let sentinel_identity = observed_process(sentinel.child.id()).unwrap();
    let path = data.join(".kakune/ownership.json");
    let original = fs::read(&path).unwrap();
    let record: RuntimeOwnershipRecord = serde_json::from_slice(&original).unwrap();
    let daemon_identity = observed_process(record.instance.process_identity.pid).unwrap();
    assert_ne!(
        record.instance.owner,
        kakune_core::lifecycle::ownership::OwnerIdentity::WindowsSid("S-1-5-18".into()),
        "this administrator-free test requires an ordinary user, not LocalSystem"
    );
    let restore = RecordRestore {
        path: path.clone(),
        original: original.clone(),
        preserve: fixture.preserve.clone(),
    };
    for case in [
        "pid",
        "start",
        "executable",
        "owner",
        "instance",
        "mode",
        "schema",
        "corrupt",
    ] {
        let mut changed = record.clone();
        match case {
            "pid" => changed.instance.process_identity.pid = sentinel.child.id(),
            "start" => changed
                .instance
                .process_identity
                .start_identity
                .push_str("-mismatch"),
            "executable" => {
                changed.instance.process_identity.executable =
                    PathBuf::from(env!("CARGO_BIN_EXE_kakune-process-fixture"))
            }
            "owner" => {
                changed.instance.owner =
                    kakune_core::lifecycle::ownership::OwnerIdentity::WindowsSid("S-1-5-18".into())
            }
            "instance" => changed.instance.instance_id = uuid::Uuid::new_v4(),
            "mode" => changed.instance.mode = kakune_core::lifecycle::CoreMode::Foreground,
            "schema" => changed.schema_version += 1,
            "corrupt" => {}
            _ => unreachable!(),
        }
        let bytes = if case == "corrupt" {
            b"{invalid".to_vec()
        } else {
            serde_json::to_vec(&changed).unwrap()
        };
        fs::write(&path, &bytes).unwrap();
        let output = run_cli(
            &fixture.root,
            &["daemon", "--data-dir", data.to_str().unwrap(), "stop"],
            Duration::from_secs(12),
        )
        .unwrap();
        assert!(!output.status.success(), "{case}: {}", cli_text(&output));
        assert_eq!(
            fs::read(&path).unwrap(),
            bytes,
            "{case}: rejection modified evidence"
        );
        assert_eq!(
            observed_process(record.instance.process_identity.pid)
                .unwrap()
                .identity,
            daemon_identity.identity,
            "{case}: daemon must stay alive"
        );
        assert_eq!(
            observed_process(sentinel.child.id()).unwrap().identity,
            sentinel_identity.identity,
            "{case}: unrelated sentinel must stay alive"
        );
        assert!(runtime_lease_is_held(&data).unwrap());
        println!(
            "native-identity-rejection\t{case}\tnonzero; both processes unchanged; lease retained"
        );
        fs::write(&path, &original).unwrap();
    }
    drop(restore);
    let status = daemon_status(&fixture.root, &data).unwrap();
    assert!(status.status.success() && cli_text(&status).contains("state=ready"));
    daemon.stop().unwrap();
    sentinel.finish();
    assert!(sentinel.child.try_wait().unwrap().is_some());
    println!(
        "native-identity-recovery\tready restored; daemon and sentinel exited; lease released"
    );
}

#[test]
#[ignore = "requires explicit native-acceptance opt-in; waits for status and stop-dispatch expiry"]
fn windows_native_control_deadlines_preserve_live_daemon() {
    require_native_approval(false);
    let fixture = NativeFixture::new().unwrap();
    let address = available_address();
    let (data, _token) = fixture_data(&fixture, "control-deadline-data", &address);
    let mut daemon = start_daemon(&fixture, &data, &address);
    let path = data.join(".kakune/ownership.json");
    let original = fs::read(&path).unwrap();
    let mut record: RuntimeOwnershipRecord = serde_json::from_slice(&original).unwrap();
    let identity = observed_process(record.instance.process_identity.pid).unwrap();
    let restore = RecordRestore {
        path: path.clone(),
        original,
        preserve: fixture.preserve.clone(),
    };
    // The test-owned listener accepts the TCP backlog but sends no proof. Both
    // real commands must exhaust their original budget without stopping Core.
    let stalled = TcpListener::bind("127.0.0.1:0").unwrap();
    record.instance.control_endpoint = Some(stalled.local_addr().unwrap().to_string());
    let bytes = serde_json::to_vec(&record).unwrap();
    fs::write(&path, &bytes).unwrap();
    for action in ["status", "stop"] {
        let started = Instant::now();
        let output = run_cli(
            &fixture.root,
            &["daemon", "--data-dir", data.to_str().unwrap(), action],
            Duration::from_secs(16),
        )
        .unwrap();
        assert_elapsed(action, started.elapsed(), 10);
        assert!(!output.status.success(), "{}", cli_text(&output));
        assert!(
            cli_text(&output).contains("deadline"),
            "{}",
            cli_text(&output)
        );
        if action == "status" {
            assert!(
                cli_text(&output).contains("state=unknown"),
                "{}",
                cli_text(&output)
            );
        }
        assert_eq!(fs::read(&path).unwrap(), bytes);
        assert_eq!(
            observed_process(record.instance.process_identity.pid)
                .unwrap()
                .identity,
            identity.identity
        );
        assert!(runtime_lease_is_held(&data).unwrap());
    }
    drop(stalled);
    drop(restore);
    let status = daemon_status(&fixture.root, &data).unwrap();
    assert!(status.status.success() && cli_text(&status).contains("state=ready"));
    daemon.stop().unwrap();
    println!(
        "native-control-recovery\toriginal endpoint restored; ready verified; normal stop releases lease"
    );
}

#[test]
#[ignore = "requires explicit native-acceptance opt-in; waits for the real ten-second lock expiry"]
fn windows_native_lock_deadline_and_recovery() {
    require_native_approval(false);
    let fixture = NativeFixture::new().unwrap();
    let address = available_address();
    let (data, _token) = fixture_data(&fixture, "lock-data", &address);
    let config_before = fs::read(data.join("kakune.yaml")).unwrap();
    let clock = SystemMonotonicClock::default();
    let lease = AdministrativeLease::acquire_for_data_dir(
        &data,
        Deadline::after(&clock, Duration::from_secs(10)),
        &clock,
        &ThreadWaiter,
    )
    .unwrap();
    let started = Instant::now();
    let output = run_cli(
        &fixture.root,
        &["daemon", "--data-dir", data.to_str().unwrap(), "start"],
        Duration::from_secs(17),
    )
    .unwrap();
    assert_elapsed("administrative-lock", started.elapsed(), 10);
    assert!(!output.status.success(), "{}", cli_text(&output));
    assert!(
        cli_text(&output).contains("deadline"),
        "{}",
        cli_text(&output)
    );
    assert!(!runtime_lease_is_held(&data).unwrap());
    assert_eq!(fs::read(data.join("kakune.yaml")).unwrap(), config_before);
    drop(lease);
    let mut daemon = start_daemon(&fixture, &data, &address);
    daemon.stop().unwrap();
    println!("native-lock-recovery\tstart/stop succeeds after releasing only the owned lock");
}

#[tokio::test]
#[ignore = "requires explicit native-acceptance opt-in; waits for real thirty-second readiness expiry"]
async fn windows_native_readiness_deadline_reaps_only_uncommitted_child() {
    use tokio::net::windows::named_pipe::ServerOptions;

    require_native_approval(false);
    let fixture = NativeFixture::new().unwrap();
    let _inventory = ReadinessInventory(&fixture);
    let address = available_address();
    let (data, _token) = fixture_data(&fixture, "readiness-data", &address);
    let certificate = fixture.root.join("localhost-cert.pem");
    fs::write(
        &certificate,
        include_bytes!("../fixtures/localhost-cert.pem"),
    )
    .unwrap();
    let pipe_name = format!(r"\\.\pipe\kakune-t043-{}", fixture.id);
    // No key bytes are sent. The OS pipe stalls the child's synchronous TLS
    // read while its real parent must still enforce the readiness budget.
    let pipe = ServerOptions::new()
        .first_pipe_instance(true)
        .reject_remote_clients(true)
        .create(&pipe_name)
        .unwrap();
    let root = fixture.root.clone();
    let command_data = data.clone();
    fixture.preserve.store(true, Ordering::SeqCst);
    let started = Instant::now();
    let command = tokio::task::spawn_blocking(move || {
        run_cli(
            &root,
            &[
                "daemon",
                "--data-dir",
                command_data.to_str().unwrap(),
                "--config",
                command_data.join("kakune.yaml").to_str().unwrap(),
                "--listen",
                &address,
                "--tls-cert",
                certificate.to_str().unwrap(),
                "--tls-key",
                &pipe_name,
                "start",
            ],
            Duration::from_secs(40),
        )
    });
    let connected = tokio::time::timeout(Duration::from_secs(8), pipe.connect()).await;
    if !matches!(connected, Ok(Ok(()))) {
        drop(pipe);
        let output = command.await.unwrap();
        panic!(
            "TLS stall pipe was not reached: {}",
            describe_cli_result(output)
        );
    }
    let path = data.join(".kakune/ownership.json");
    let record: RuntimeOwnershipRecord = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    let child_identity = observed_process(record.instance.process_identity.pid).unwrap();
    assert!(runtime_lease_is_held(&data).unwrap());
    let output = command
        .await
        .unwrap()
        .expect("parent must return before harness timeout");
    assert_elapsed(
        "readiness-including-failed-start-cleanup",
        started.elapsed(),
        30,
    );
    assert!(!output.status.success(), "{}", cli_text(&output));
    assert!(
        cli_text(&output).contains("readiness deadline expired"),
        "{}",
        cli_text(&output)
    );
    assert!(observed_process(child_identity.identity.pid).is_none());
    verify_runtime_lease_released(&data).unwrap();
    drop(pipe);
    // A failed-start record may remain; only normal CLI reconciliation is used.
    let mut daemon = start_daemon(&fixture, &data, &available_address());
    daemon.stop().unwrap();
    fixture.preserve.store(false, Ordering::SeqCst);
    println!(
        "native-readiness-recovery\tuncommitted child exit confirmed; normal restart/stop succeeds"
    );
}

struct ReadinessInventory<'a>(&'a NativeFixture);

impl Drop for ReadinessInventory<'_> {
    fn drop(&mut self) {
        if self.0.preserve.load(Ordering::SeqCst) {
            let _ = fs::write(
                self.0.root.join("recovery-inventory.md"),
                "# T043 readiness recovery inventory\n\nStartup/teardown was not fully confirmed. Retain this marker-owned fixture and all CLI logs. Inspect readiness-data/.kakune/ownership.json, its recorded process identity, and runtime.lock read-only. Do not delete data until actual child exit and lease release are confirmed. The named pipe closes with its owning test; no secret key bytes were sent.\n",
            );
        }
    }
}

struct PluginRelease(PathBuf);

#[tokio::test]
async fn slow_plugin_fixture_uses_explicit_gates_and_exits_after_shutdown() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let fixture = NativeFixture::new().unwrap();
    let release = PluginRelease(fixture.root.clone());
    drop(release);
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_kakune-process-fixture"));
    command
        .args(["lifecycle-slow-plugin", fixture.root.to_str().unwrap()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let mut child = kakune_core::process_supervisor::spawn_async(command).unwrap();
    let mut input = child.stdin().take().unwrap();
    let mut output = BufReader::new(child.stdout().take().unwrap()).lines();
    for (id, method) in [(1, "node/execute"), (2, "shutdown")] {
        input
            .write_all(
                format!("{}\n", json!({"jsonrpc":"2.0", "id":id, "method":method})).as_bytes(),
            )
            .await
            .unwrap();
        let line = tokio::time::timeout(Duration::from_secs(3), output.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let reply: serde_json::Value = serde_json::from_str(&line)
            .expect("fixture must speak JSON-RPC, not spawn descendants");
        assert_eq!(reply["id"], id);
    }
    assert!(
        tokio::time::timeout(Duration::from_secs(3), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    assert!(fixture.root.join("plugin-entered.json").exists());
}

impl Drop for PluginRelease {
    fn drop(&mut self) {
        let _ = fs::write(self.0.join("release-node"), b"release");
        let _ = fs::write(self.0.join("release-shutdown"), b"release");
    }
}

fn install_slow_plugin(fixture: &NativeFixture, data: &Path) {
    let plugin = fixture.root.join("slow-plugin");
    fs::create_dir(&plugin).unwrap();
    let manifest = plugin.join("kakune-plugin.json");
    fs::write(
        plugin.join("slow.json"),
        json!({
            "apiVersion":"kakune.dev/v1", "kind":"NodeDefinition",
            "type":"org.example.native-slow@1", "name":"Native slow fixture",
            "inputSchema":{"type":"object"}, "outputSchema":{"type":"object"}
        })
        .to_string(),
    )
    .unwrap();
    fs::write(
        &manifest,
        json!({
            "manifestVersion":"1.0", "id":"org.example.native-slow", "name":"Native slow fixture",
            "version":"0.1.0", "pluginProtocol":">=1.0.0 <2.0.0",
            "runtime":{"kind":"process", "command":env!("CARGO_BIN_EXE_kakune-process-fixture"),
                "args":["lifecycle-slow-plugin", fixture.root]},
            "contributes":{"nodes":["slow.json"]}
        })
        .to_string(),
    )
    .unwrap();
    let store = Store::open(data.to_path_buf()).unwrap();
    store
        .upsert_plugin_install(&kakune_core::InstalledPluginRecord {
            id: "org.example.native-slow".into(),
            name: "Native slow fixture".into(),
            version: "0.1.0".into(),
            manifest_path: manifest.to_str().unwrap().into(),
            installed_at: chrono::Utc::now().to_rfc3339(),
            enabled: true,
            policy: json!({}),
            digest: "native-test-only".into(),
            resolved_lock: json!({}),
            provenance: json!({}),
        })
        .unwrap();
    let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata: { id: native-slow, name: Native slow }\ntriggers: [{ id: manual, type: kakune.trigger.manual@1 }]\nentry: slow\nnodes:\n  - id: slow\n    type: org.example.native-slow@1\n";
    store
        .upsert_workflow(&WorkflowDocument::parse(source).unwrap(), source, "enabled")
        .unwrap();
}

#[tokio::test]
#[ignore = "requires explicit native-acceptance opt-in; waits for real thirty-second Core grace expiry"]
async fn windows_native_grace_expiry_retains_ownership_until_worker_finishes() {
    require_native_approval(false);
    let fixture = NativeFixture::new().unwrap();
    let address = available_address();
    let (data, token) = fixture_data(&fixture, "grace-data", &address);
    install_slow_plugin(&fixture, &data);
    let mut daemon = start_daemon(&fixture, &data, &address);
    // This guard releases both waits before DaemonGuard attempts cleanup during
    // unwinding. No Core process is killed even if an assertion fails.
    let release = PluginRelease(fixture.root.clone());
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let response = client
        .post(format!("http://{address}/api/v1/executions"))
        .bearer_auth(&token)
        .json(&json!({"workflowId":"native-slow"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    let execution_id = response.json::<serde_json::Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let entered = fixture.root.join("plugin-entered.json");
    let entered_deadline = Instant::now() + Duration::from_secs(8);
    while !entered.exists() {
        assert!(
            Instant::now() < entered_deadline,
            "slow fixture did not enter node/execute"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let plugin_pid: u32 = serde_json::from_slice(&fs::read(&entered).unwrap()).unwrap();
    let record: RuntimeOwnershipRecord =
        serde_json::from_slice(&fs::read(data.join(".kakune/ownership.json")).unwrap()).unwrap();
    let original_identity = observed_process(record.instance.process_identity.pid).unwrap();
    let root = fixture.root.clone();
    let stop_data = data.clone();
    let started = Instant::now();
    let stop = tokio::task::spawn_blocking(move || {
        run_cli(
            &root,
            &["daemon", "--data-dir", stop_data.to_str().unwrap(), "stop"],
            Duration::from_secs(38),
        )
    });
    tokio::time::sleep(Duration::from_secs(20)).await;
    fs::write(fixture.root.join("release-node"), b"release").unwrap();
    let output = stop.await.unwrap().unwrap();
    assert_elapsed("grace-expiry", started.elapsed(), 30);
    assert!(!output.status.success(), "{}", cli_text(&output));
    assert!(
        cli_text(&output).contains("deadline expired"),
        "{}",
        cli_text(&output)
    );
    assert_eq!(
        observed_process(record.instance.process_identity.pid)
            .unwrap()
            .identity,
        original_identity.identity
    );
    assert!(runtime_lease_is_held(&data).unwrap());
    let status = daemon_status(&fixture.root, &data).unwrap();
    assert!(
        status.status.success() && cli_text(&status).contains("state=stopping"),
        "{}",
        cli_text(&status)
    );
    assert!(observed_process(plugin_pid).is_some());
    assert!(
        !Store::open(data.clone())
            .unwrap()
            .list_executions()
            .unwrap()
            .iter()
            .any(|execution| execution.id == execution_id && execution.status == "succeeded")
    );
    drop(release);
    let exit_deadline = Instant::now() + Duration::from_secs(12);
    while runtime_lease_is_held(&data).unwrap()
        || observed_process(record.instance.process_identity.pid).is_some()
        || observed_process(plugin_pid).is_some()
    {
        assert!(
            Instant::now() < exit_deadline,
            "released worker/Core exit remains unconfirmed"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    daemon.stop().unwrap();
    let mut restarted = start_daemon(&fixture, &data, &available_address());
    restarted.stop().unwrap();
    assert!(
        Store::open(data)
            .unwrap()
            .list_executions()
            .unwrap()
            .iter()
            .any(|execution| execution.id == execution_id)
    );
    assert_eq!(
        fs::read_to_string(fixture.root.join("plugin-calls.txt"))
            .unwrap()
            .lines()
            .count(),
        1,
        "restart must not rerun the completed plugin node"
    );
    println!(
        "native-grace-recovery\tstopping+lease retained after expiry; worker/Core exited after release; execution retained after restart"
    );
}
