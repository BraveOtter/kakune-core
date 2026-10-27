mod support;

use std::{
    collections::BTreeMap,
    fs,
    io::{BufRead, BufReader},
    net::TcpListener,
    path::PathBuf,
    process::{Child, Command, Output, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    sync::mpsc,
    time::{Duration, Instant},
};

use kakune_core::{ContextFile, Store, WorkflowDocument};
use serde_json::json;
use support::lifecycle::LifecycleFixture;

static NEXT_START_CAPTURE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, PartialEq, Eq)]
enum FileSnapshot {
    Bytes(Vec<u8>),
    LockedMetadata {
        length: u64,
        modified: Option<std::time::SystemTime>,
    },
}

fn kakune() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_kakune"))
}

fn fixture() -> LifecycleFixture {
    LifecycleFixture::new().expect("isolated lifecycle fixture should be created")
}

struct DaemonCleanup(PathBuf);

impl Drop for DaemonCleanup {
    fn drop(&mut self) {
        let _ = Command::new(kakune())
            .arg("daemon")
            .arg("--data-dir")
            .arg(&self.0)
            .arg("stop")
            .stdin(Stdio::null())
            .output();
    }
}

fn daemon_start_output(
    data_dir: &std::path::Path,
    config: &std::path::Path,
    listen: &str,
) -> Output {
    daemon_start_output_with_context(data_dir, config, listen, None)
}

fn daemon_start_output_with_context(
    data_dir: &std::path::Path,
    config: &std::path::Path,
    listen: &str,
    context_file: Option<&std::path::Path>,
) -> Output {
    let capture_id = NEXT_START_CAPTURE.fetch_add(1, Ordering::Relaxed);
    let capture_stem = format!(".daemon-start-{}-{capture_id}", std::process::id());
    let stdout_path = data_dir.join(format!("{capture_stem}.stdout"));
    let stderr_path = data_dir.join(format!("{capture_stem}.stderr"));
    let stdout = fs::File::create(&stdout_path).expect("start stdout capture should be created");
    let stderr = fs::File::create(&stderr_path).expect("start stderr capture should be created");
    let mut command = Command::new(kakune());
    if let Some(context_file) = context_file {
        command.arg("--context-file").arg(context_file);
    }
    let mut child = command
        .arg("daemon")
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--config")
        .arg(config)
        .arg("--listen")
        .arg(listen)
        .arg("--allowed-host")
        .arg("127.0.0.1")
        .arg("start")
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .expect("background start should launch");
    let deadline = Instant::now() + Duration::from_secs(35);
    let status = loop {
        if let Some(status) = child
            .try_wait()
            .expect("start command should be observable")
        {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("background start command did not exit within its bounded readiness window");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let output = Output {
        status,
        stdout: fs::read(&stdout_path).expect("captured start stdout should be readable"),
        stderr: fs::read(&stderr_path).expect("captured start stderr should be readable"),
    };
    let _ = fs::remove_file(stdout_path);
    let _ = fs::remove_file(stderr_path);
    output
}

fn snapshot_files(root: &std::path::Path) -> BTreeMap<PathBuf, FileSnapshot> {
    fn visit(
        root: &std::path::Path,
        directory: &std::path::Path,
        snapshot: &mut BTreeMap<PathBuf, FileSnapshot>,
    ) {
        for entry in fs::read_dir(directory).expect("snapshot directory should be readable") {
            let entry = entry.expect("snapshot entry should be readable");
            let path = entry.path();
            let file_type = entry
                .file_type()
                .expect("snapshot entry type should be readable");
            if file_type.is_dir() {
                visit(root, &path, snapshot);
            } else if file_type.is_file() {
                let value = match fs::read(&path) {
                    Ok(bytes) => FileSnapshot::Bytes(bytes),
                    Err(error)
                        if cfg!(windows)
                            && error
                                .raw_os_error()
                                .is_some_and(|code| matches!(code, 32 | 33)) =>
                    {
                        // The active Windows runtime lock cannot be read while the daemon holds
                        // its byte-range lock, so retain its observable filesystem metadata.
                        let metadata = fs::metadata(&path)
                            .expect("locked snapshot file metadata should remain readable");
                        FileSnapshot::LockedMetadata {
                            length: metadata.len(),
                            modified: metadata.modified().ok(),
                        }
                    }
                    Err(error) => panic!(
                        "snapshot file {} should be readable: {error}",
                        path.display()
                    ),
                };
                snapshot.insert(
                    path.strip_prefix(root)
                        .expect("snapshot path should remain under its root")
                        .to_path_buf(),
                    value,
                );
            }
        }
    }

    let mut snapshot = BTreeMap::new();
    visit(root, root, &mut snapshot);
    snapshot
}

#[test]
fn missing_directory_status_and_stop_are_read_only_successful_noops() {
    let fixture = fixture();
    let data_dir = fixture.root().join("not-created");
    let config = fixture.root().join("invalid config.yaml");
    fs::write(&config, "api: [invalid yaml\n").expect("invalid config should be written");

    for action in ["status", "stop", "stop"] {
        let output = Command::new(kakune())
            .arg("daemon")
            .arg("--data-dir")
            .arg(&data_dir)
            .arg("--config")
            .arg(&config)
            .arg(action)
            .stdin(Stdio::null())
            .output()
            .expect("read-only daemon command should launch");
        let transcript = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            output.status.success(),
            "{action} should be a no-op: {transcript}"
        );
        assert!(
            !data_dir.exists(),
            "{action} must not create a missing data directory"
        );
        assert!(
            !transcript.contains("invalid yaml"),
            "{action} must not load unrelated startup configuration"
        );
    }
}

#[test]
fn status_and_stop_ignore_invalid_startup_configuration_without_changing_files() {
    let fixture = fixture();
    let data_dir = fixture.data_dir();
    fs::create_dir_all(&data_dir).expect("isolated data directory should be created");
    let config = fixture.root().join("broken.yaml");
    fs::write(&config, "api: [invalid yaml\n").expect("invalid config should be written");
    let before = snapshot_files(fixture.root());

    for action in ["status", "stop"] {
        let output = Command::new(kakune())
            .arg("daemon")
            .arg("--data-dir")
            .arg(&data_dir)
            .arg("--config")
            .arg(&config)
            .arg(action)
            .stdin(Stdio::null())
            .output()
            .expect("daemon observation/control should launch");
        assert!(
            output.status.success(),
            "{action} should not depend on invalid startup config: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    assert_eq!(
        snapshot_files(fixture.root()),
        before,
        "status/stop must leave every fixture file byte-for-byte unchanged"
    );
}

#[test]
fn failed_background_start_does_not_claim_readiness_or_leave_a_live_child() {
    let fixture = fixture();
    let data_dir = fixture.data_dir();
    let config = fixture.root().join("bad-tls.yaml");
    let certificate = fixture.root().join("broken certificate.pem");
    let private_key = fixture.root().join("broken private key.pem");
    fs::write(&config, "api:\n  listen: 127.0.0.1:8787\n").unwrap();
    fs::write(&certificate, "not a certificate").unwrap();
    fs::write(&private_key, "not a key").unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
    let address = listener.local_addr().unwrap();
    drop(listener);

    let output = Command::new(kakune())
        .arg("daemon")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--config")
        .arg(&config)
        .arg("--listen")
        .arg(address.to_string())
        .arg("--tls-cert")
        .arg(&certificate)
        .arg("--tls-key")
        .arg(&private_key)
        .arg("start")
        .stdin(Stdio::null())
        .output()
        .expect("background start should launch");
    let transcript = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !output.status.success(),
        "invalid TLS must fail start: {transcript}"
    );
    assert!(
        !transcript.contains("readiness: ready") && !transcript.contains("started (pid"),
        "failed startup must not report readiness: {transcript}"
    );
    assert!(
        !daemon_ownership_is_live(&data_dir),
        "a failed startup must not leave a live owned daemon"
    );
    let lifecycle_dir = data_dir.join(".kakune");
    assert!(
        !lifecycle_dir.join("ownership.json").exists(),
        "failed startup should remove its uncommitted ownership record"
    );
    if lifecycle_dir.exists() {
        assert!(
            fs::read_dir(&lifecycle_dir)
                .expect("failed-start lifecycle directory should be readable")
                .all(|entry| {
                    entry.is_ok_and(|entry| {
                        !entry.file_name().to_string_lossy().starts_with("control-")
                    })
                }),
            "failed startup should not leave an instance capability behind"
        );
    }
}

#[test]
fn background_child_releases_uncommitted_state_when_parent_channel_is_lost() {
    let fixture = fixture();
    let data_dir = fixture.root().join("parent-loss data ñ");
    fs::create_dir_all(&data_dir).expect("isolated data directory should be created");
    let config = fixture.root().join("parent-loss config.yaml");
    fs::write(&config, "api:\n  listen: 127.0.0.1:8787\n")
        .expect("valid configuration should be written");
    let reservation = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
    let address = reservation
        .local_addr()
        .expect("test listener should be inspectable");
    drop(reservation);
    let log_path = fixture.root().join("parent-loss daemon.log");
    let log = fs::File::create(&log_path).expect("child diagnostic should be captured");
    let child = Command::new(kakune())
        .arg("daemon")
        .arg("--internal-daemon-child")
        .arg("--internal-log")
        .arg(&log_path)
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--config")
        .arg(&config)
        .arg("--listen")
        .arg(address.to_string())
        .arg("--allowed-host")
        .arg("127.0.0.1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::from(log))
        .spawn()
        .expect("hidden daemon child should launch");
    let mut child = OwnedChild(child);
    let parent_channel = child
        .0
        .stdin
        .take()
        .expect("parent commit channel should be piped");
    let child_protocol = child
        .0
        .stdout
        .take()
        .expect("child protocol channel should be piped");
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let mut reader = BufReader::new(child_protocol);
        let mut messages = Vec::new();
        for _ in 0..2 {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => break,
                Ok(_) => messages.push(line.trim_end().to_owned()),
            }
        }
        let _ = sender.send(messages);
    });
    let messages = receiver
        .recv_timeout(Duration::from_secs(20))
        .expect("child should reach its prepared state within the startup budget");
    assert_eq!(messages.first().map(String::as_str), Some("CLAIM"));
    assert!(
        messages
            .get(1)
            .is_some_and(|message| message.starts_with("PREPARED\t"))
    );

    drop(parent_channel);
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = child.0.try_wait().expect("child exit should be observable") {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "parent loss should shut the child down promptly"
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    assert!(
        !status.success(),
        "a child without parent commit must not claim startup success"
    );
    assert!(
        fs::read_to_string(&log_path)
            .expect("child diagnostic should be readable")
            .contains("parent channel closed before launch commit")
    );
    assert!(
        !data_dir.join(".kakune").join("ownership.json").exists(),
        "parent loss should clean the uncommitted ownership record"
    );
    assert!(!daemon_ownership_is_live(&data_dir));
}

#[tokio::test]
async fn background_start_waits_for_instance_readiness_and_supports_workflow_stop() {
    let fixture = fixture();
    let data_dir = fixture.root().join("datos con espacios 東京");
    let config = fixture.root().join("config de API.yaml");
    let context_file = fixture.root().join("custom contexts/contexts.json");
    let store = Store::open(data_dir.clone()).expect("isolated store should open");
    let token = store
        .ensure_bootstrap_token()
        .expect("test bootstrap token should be available")
        .expect("isolated store should create its initial test token");
    let source = r#"apiVersion: kakune/v1
kind: Workflow
metadata: { id: daemon-smoke, name: Background daemon smoke }
triggers: [{ id: manual, type: kakune.trigger.manual@1 }]
entry: finish
nodes:
  - id: finish
    type: kakune.flow.pass@1
"#;
    let workflow = WorkflowDocument::parse(source).expect("workflow should parse");
    store
        .upsert_workflow(&workflow, source, "enabled")
        .expect("workflow fixture should be saved");
    drop(store);
    fs::write(&config, "api:\n  listen: 127.0.0.1:8787\n")
        .expect("valid configuration should be written");
    let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    let cleanup = DaemonCleanup(data_dir.clone());

    let output = daemon_start_output_with_context(
        &data_dir,
        &config,
        &address.to_string(),
        Some(&context_file),
    );
    let transcript = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.status.success(),
        "start must wait for readiness: {transcript}"
    );
    assert!(
        transcript.contains("state=ready"),
        "ready state is required: {transcript}"
    );
    assert!(
        transcript.contains("identity=verified"),
        "identity must be verified: {transcript}"
    );
    assert!(
        transcript.contains("diagnostic"),
        "log location must be reported: {transcript}"
    );

    let canonical_data_dir = data_dir
        .canonicalize()
        .expect("test data path should resolve");
    let canonical_text = canonical_data_dir.to_string_lossy();
    let display_text = canonical_text
        .strip_prefix(r"\\?\")
        .unwrap_or(&canonical_text);
    assert!(
        transcript.contains(canonical_text.as_ref()) || transcript.contains(display_text),
        "ready output should preserve the selected absolute data path: {transcript}"
    );
    let contexts = ContextFile::load(&context_file).expect("explicit context path should load");
    let expected_endpoint = format!("http://{address}");
    assert_eq!(
        contexts
            .contexts
            .iter()
            .find(|context| context.id == "local")
            .map(|context| context.endpoint.as_str()),
        Some(expected_endpoint.as_str()),
        "effective --context-file and --listen options should be preserved"
    );
    assert!(
        !data_dir.join("cli").join("contexts.json").exists(),
        "an explicit context path must not be silently replaced by the default"
    );
    assert!(daemon_ownership_is_live(&data_dir));

    let repeated_start = daemon_start_output(&data_dir, &config, &address.to_string());
    assert!(
        !repeated_start.status.success(),
        "a repeated start must not claim a second owner: {}",
        String::from_utf8_lossy(&repeated_start.stderr)
    );
    assert!(daemon_ownership_is_live(&data_dir));

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .expect("API test client should build");
    let response = client
        .post(format!("http://{address}/api/v1/executions"))
        .bearer_auth(&token)
        .json(&json!({ "workflowId": "daemon-smoke" }))
        .send()
        .await
        .expect("ready daemon should accept workflow submissions");
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    let execution_id = response
        .json::<serde_json::Value>()
        .await
        .expect("execution response should be JSON")["id"]
        .as_str()
        .expect("execution ID should be returned")
        .to_owned();
    let status_url = format!("http://{address}/api/v1/executions/{execution_id}");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let response = client
            .get(&status_url)
            .bearer_auth(&token)
            .send()
            .await
            .expect("execution status should be available");
        let body = response
            .json::<serde_json::Value>()
            .await
            .expect("execution status should be JSON");
        if body["execution"]["status"] == "succeeded" {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "workflow did not finish: {body}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let lifecycle_state = data_dir.join(".kakune");
    let before_status = snapshot_files(&lifecycle_state);

    let status = Command::new(kakune())
        .arg("daemon")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--config")
        .arg(
            fixture
                .root()
                .join("invalid config that must not load.yaml"),
        )
        .arg("status")
        .output()
        .expect("daemon status should launch");
    assert!(
        status.status.success(),
        "ready status should succeed: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    assert!(String::from_utf8_lossy(&status.stdout).contains("state=ready"));
    assert_eq!(
        snapshot_files(&lifecycle_state),
        before_status,
        "status must not change lifecycle ownership, lock, or capability bytes"
    );

    let stop = Command::new(kakune())
        .arg("daemon")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--config")
        .arg(
            fixture
                .root()
                .join("invalid config that must not load.yaml"),
        )
        .arg("stop")
        .output()
        .expect("daemon stop should launch");
    assert!(
        stop.status.success(),
        "verified graceful stop should succeed: {}",
        String::from_utf8_lossy(&stop.stderr)
    );
    let status = Command::new(kakune())
        .arg("daemon")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("status")
        .output()
        .expect("stopped status should launch");
    assert!(status.status.success());
    assert!(String::from_utf8_lossy(&status.stdout).contains("state=stopped"));
    let repeated_stop = Command::new(kakune())
        .arg("daemon")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("stop")
        .output()
        .expect("repeated stop should launch");
    assert!(repeated_stop.status.success(), "repeated stop is a no-op");
    drop(cleanup);
}

#[test]
fn ten_competing_background_starts_produce_exactly_one_ready_owner() {
    let fixture = fixture();
    let data_dir = fixture.root().join("concurrent data ñ");
    let config = fixture.root().join("concurrent config.yaml");
    fs::create_dir_all(&data_dir).unwrap();
    fs::write(&config, "api:\n  listen: 127.0.0.1:8787\n").unwrap();
    let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reservation.local_addr().unwrap().to_string();
    drop(reservation);
    let cleanup = DaemonCleanup(data_dir.clone());

    let outputs = std::thread::scope(|scope| {
        (0..10)
            .map(|_| {
                let data_dir = data_dir.clone();
                let config = config.clone();
                let address = address.clone();
                scope.spawn(move || daemon_start_output(&data_dir, &config, &address))
            })
            .map(|thread| thread.join().expect("start worker should complete"))
            .collect::<Vec<_>>()
    });
    let ready_count = outputs
        .iter()
        .filter(|output| {
            output.status.success()
                && String::from_utf8_lossy(&output.stdout).contains("state=ready")
        })
        .count();
    assert_eq!(
        ready_count, 1,
        "exactly one competing start may become ready"
    );
    assert!(daemon_ownership_is_live(&data_dir));
    let stop = Command::new(kakune())
        .arg("daemon")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("stop")
        .output()
        .expect("test-owned daemon should stop");
    assert!(
        stop.status.success(),
        "test-owned daemon cleanup should succeed"
    );
    drop(cleanup);
}

#[test]
fn concurrent_stops_leave_only_the_verified_daemon_stopped() {
    let fixture = fixture();
    let data_dir = fixture.root().join("concurrent-stop data ñ");
    let config = fixture.root().join("concurrent-stop config.yaml");
    fs::create_dir_all(&data_dir).expect("isolated data directory should be created");
    fs::write(&config, "api:\n  listen: 127.0.0.1:8787\n")
        .expect("valid configuration should be written");
    let reservation = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
    let address = reservation
        .local_addr()
        .expect("test listener should be inspectable");
    drop(reservation);
    let cleanup = DaemonCleanup(data_dir.clone());
    let start = daemon_start_output(&data_dir, &config, &address.to_string());
    assert!(
        start.status.success(),
        "test daemon should become ready: {}",
        String::from_utf8_lossy(&start.stderr)
    );

    let outputs = std::thread::scope(|scope| {
        (0..2)
            .map(|_| {
                let data_dir = data_dir.clone();
                scope.spawn(move || {
                    Command::new(kakune())
                        .arg("daemon")
                        .arg("--data-dir")
                        .arg(data_dir)
                        .arg("stop")
                        .stdin(Stdio::null())
                        .output()
                        .expect("concurrent cooperative stop should launch")
                })
            })
            .map(|worker| worker.join().expect("stop worker should complete"))
            .collect::<Vec<_>>()
    });
    assert!(
        outputs.iter().any(|output| output.status.success()),
        "at least one concurrent stop must confirm the daemon exit: {:?}",
        outputs
            .iter()
            .map(|output| String::from_utf8_lossy(&output.stderr).into_owned())
            .collect::<Vec<_>>()
    );
    let status = Command::new(kakune())
        .arg("daemon")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("status")
        .output()
        .expect("post-stop status should launch");
    assert!(status.status.success());
    assert!(String::from_utf8_lossy(&status.stdout).contains("state=stopped"));
    assert!(!daemon_ownership_is_live(&data_dir));
    drop(cleanup);
}

fn daemon_ownership_is_live(data_dir: &std::path::Path) -> bool {
    use kakune_core::lifecycle::{
        Deadline, SystemMonotonicClock,
        ownership::{OWNERSHIP_SCHEMA_VERSION, OwnershipRead, read_ownership_record},
        process::{ProcessEvidenceProvider, SystemProcessEvidence},
    };

    let path = data_dir.join(".kakune").join("ownership.json");
    let Ok(Some(OwnershipRead::Current(record))) = read_ownership_record(&path) else {
        return false;
    };
    if record.schema_version != OWNERSHIP_SCHEMA_VERSION {
        return false;
    }
    let clock = SystemMonotonicClock::default();
    SystemProcessEvidence::default()
        .observe(
            record.instance.process_identity.pid,
            Deadline::after(&clock, Duration::from_secs(2)),
            &clock,
        )
        .is_ok_and(|evidence| {
            evidence.is_some_and(|evidence| {
                evidence.identity == record.instance.process_identity
                    && evidence.owner == record.instance.owner
            })
        })
}

struct OwnedChild(Child);

impl Drop for OwnedChild {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

#[test]
fn invalid_tls_never_announces_foreground_readiness_or_provisions_a_client_context() {
    let fixture = fixture();
    let data_dir = fixture.root().join("datos ñ 東京");
    let config_file = fixture.root().join("config with spaces.yaml");
    let context_file = fixture.root().join("client context.json");
    let certificate = fixture.root().join("invalid certificate.pem");
    let private_key = fixture.root().join("invalid private key.pem");
    fs::write(&config_file, "api:\n  listen: 127.0.0.1:8787\n")
        .expect("configuration fixture should be written");
    fs::write(&certificate, "not a certificate").expect("certificate fixture should be written");
    fs::write(&private_key, "not a private key").expect("key fixture should be written");
    let listener = TcpListener::bind("127.0.0.1:0").expect("test port should be available");
    let address = listener.local_addr().expect("test listener has an address");
    drop(listener);

    let output = Command::new(kakune())
        .arg("--context-file")
        .arg(&context_file)
        .arg("daemon")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--config")
        .arg(&config_file)
        .arg("--listen")
        .arg(address.to_string())
        .arg("--tls-cert")
        .arg(certificate)
        .arg("--tls-key")
        .arg(private_key)
        // Prevent this regression test from touching the host credential manager
        // when run against the pre-fix implementation.
        .env("KAKUNE_DAEMON_MANAGED", "1")
        .stdin(Stdio::null())
        .output()
        .expect("foreground command should launch");

    let transcript = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !output.status.success(),
        "invalid TLS must fail: {transcript}"
    );
    assert!(
        transcript.contains("TLS") || transcript.contains("certificate"),
        "failure should identify TLS configuration: {transcript}"
    );
    assert!(
        !transcript.contains("Kakune Core listening") && !transcript.contains("readiness: ready"),
        "invalid TLS must not be reported ready: {transcript}"
    );
    assert!(
        !context_file.exists(),
        "Core startup must not provision a client context"
    );
}

#[test]
fn invalid_configuration_does_not_announce_readiness() {
    let fixture = fixture();
    let data_dir = fixture.root().join("配置 with spaces");
    let config_file = fixture.root().join("broken configuration.yaml");
    fs::write(&config_file, "api: [not valid yaml\n")
        .expect("malformed configuration fixture should be written");
    let listener = TcpListener::bind("127.0.0.1:0").expect("test port should be available");
    let address = listener.local_addr().expect("test listener has an address");
    drop(listener);

    let output = Command::new(kakune())
        .arg("daemon")
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--config")
        .arg(config_file)
        .arg("--listen")
        .arg(address.to_string())
        .env("KAKUNE_DAEMON_MANAGED", "1")
        .stdin(Stdio::null())
        .output()
        .expect("foreground command should launch");
    let transcript = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !output.status.success(),
        "invalid configuration must fail: {transcript}"
    );
    assert!(
        !transcript.contains("Kakune Core ready") && !transcript.contains("Kakune Core listening"),
        "invalid configuration must not be announced ready: {transcript}"
    );
}

#[test]
fn inaccessible_data_path_fails_before_readiness() {
    let fixture = fixture();
    let data_path = fixture.root().join("not a directory");
    fs::write(&data_path, "fixture file blocks directory creation")
        .expect("blocking file fixture should be written");
    let listener = TcpListener::bind("127.0.0.1:0").expect("test port should be available");
    let address = listener.local_addr().expect("test listener has an address");
    drop(listener);

    let output = Command::new(kakune())
        .arg("daemon")
        .arg("--data-dir")
        .arg(data_path)
        .arg("--listen")
        .arg(address.to_string())
        .env("KAKUNE_DAEMON_MANAGED", "1")
        .stdin(Stdio::null())
        .output()
        .expect("foreground command should launch");
    let transcript = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !output.status.success(),
        "inaccessible data path must fail: {transcript}"
    );
    assert!(
        !transcript.contains("Kakune Core ready") && !transcript.contains("Kakune Core listening"),
        "inaccessible storage must not be announced ready: {transcript}"
    );
}

#[test]
fn occupied_listener_fails_without_a_foreground_readiness_announcement() {
    let fixture = fixture();
    let data_dir = fixture.data_dir();
    let config_file = fixture.config_file();
    fs::create_dir_all(config_file.parent().expect("config has a parent"))
        .expect("configuration directory should be created");
    fs::write(&config_file, "api:\n  listen: 127.0.0.1:8787\n")
        .expect("configuration fixture should be written");
    let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
    let address = listener.local_addr().expect("test listener has an address");

    let output = Command::new(kakune())
        .arg("--context-file")
        .arg(fixture.root().join("contexts.json"))
        .arg("daemon")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--config")
        .arg(&config_file)
        .arg("--listen")
        .arg(address.to_string())
        .env("KAKUNE_DAEMON_MANAGED", "1")
        .stdin(Stdio::null())
        .output()
        .expect("foreground command should launch");

    let transcript = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !output.status.success(),
        "occupied listener must fail: {transcript}"
    );
    assert!(
        !transcript.contains("Kakune Core listening") && !transcript.contains("readiness: ready"),
        "a failed bind must not be announced as ready: {transcript}"
    );
}

#[tokio::test]
async fn foreground_stays_attached_and_accepts_an_api_workflow_before_readiness_is_reported() {
    let fixture = fixture();
    let data_dir = fixture.data_dir();
    let context_file = fixture.root().join("unused client context.json");
    let store = Store::open(data_dir.clone()).expect("isolated store should open");
    let token = store
        .ensure_bootstrap_token()
        .expect("test token should be created")
        .expect("new isolated store should need a token");
    let source = r#"apiVersion: kakune/v1
kind: Workflow
metadata: { id: foreground-smoke, name: Foreground smoke }
triggers: [{ id: manual, type: kakune.trigger.manual@1 }]
entry: finish
nodes:
  - id: finish
    type: kakune.flow.pass@1
"#;
    let workflow = WorkflowDocument::parse(source).expect("workflow should parse");
    store
        .upsert_workflow(&workflow, source, "enabled")
        .expect("workflow should be saved in the isolated store");
    drop(store);

    let reservation = TcpListener::bind("127.0.0.1:0").expect("test port should be available");
    let address = reservation
        .local_addr()
        .expect("test listener has an address");
    drop(reservation);
    let child = Command::new(kakune())
        .arg("daemon")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--listen")
        .arg(address.to_string())
        .arg("--allowed-host")
        .arg("127.0.0.1")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("foreground Core should launch");
    let mut child = OwnedChild(child);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .expect("HTTP test client should build");
    let ready_url = format!("http://{address}/health/ready");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(response) = client.get(&ready_url).send().await
            && response.status().is_success()
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "foreground Core did not become ready"
        );
        assert!(
            child
                .0
                .try_wait()
                .expect("test-owned Core should be inspectable")
                .is_none(),
            "foreground Core exited before readiness"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    assert!(
        child
            .0
            .try_wait()
            .expect("test-owned Core should be inspectable")
            .is_none(),
        "foreground command must stay attached while the Core is ready"
    );
    let response = client
        .post(format!("http://{address}/api/v1/executions"))
        .bearer_auth(&token)
        .json(&json!({ "workflowId": "foreground-smoke" }))
        .send()
        .await
        .expect("API should accept a workflow execution");
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    let execution_id = response
        .json::<serde_json::Value>()
        .await
        .expect("execution response should be JSON")["id"]
        .as_str()
        .expect("execution id should be present")
        .to_string();
    let execution_url = format!("http://{address}/api/v1/executions/{execution_id}");
    let status_deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let status = client
            .get(&execution_url)
            .bearer_auth(&token)
            .send()
            .await
            .expect("execution status should be queryable");
        let body = status
            .json::<serde_json::Value>()
            .await
            .expect("execution status should be JSON");
        if body["execution"]["status"] == "succeeded" {
            break;
        }
        assert!(
            std::time::Instant::now() < status_deadline,
            "API-backed workflow did not finish: {body}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        !context_file.exists(),
        "Core startup must not create a client context"
    );
}
