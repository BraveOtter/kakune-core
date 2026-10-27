//! Explicit, opt-in native acceptance. Default test runs never mutate a native
//! service or create detached Core processes.

use std::{
    fs, io,
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Output, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use kakune_core::{Store, WorkflowDocument};
use serde_json::json;

const FIXTURE_MARKER: &str = ".kakune-native-acceptance-fixture";
#[cfg(target_os = "linux")]
const NATIVE_SERVICE_CYCLE_DELAY: Duration = Duration::from_secs(4);
#[cfg(any(windows, target_os = "linux"))]
const APPROVAL_VARIABLE: &str = "KAKUNE_RUN_NATIVE_ACCEPTANCE";
static NEXT_OUTPUT: AtomicU64 = AtomicU64::new(0);

#[cfg(windows)]
#[path = "support/native_windows.rs"]
mod native_windows;

#[cfg(windows)]
#[path = "support/native_windows_service.rs"]
mod native_windows_service;

fn require_native_approval(service_mutation: bool) {
    #[cfg(not(any(windows, target_os = "linux")))]
    {
        let _ = service_mutation;
        panic!(
            "native acceptance is available only on Windows and Linux; macOS remains theoretical"
        );
    }

    #[cfg(any(windows, target_os = "linux"))]
    {
        assert!(
            std::env::var(APPROVAL_VARIABLE).is_ok_and(|value| value == "1"),
            "native acceptance is ignored by default; set {APPROVAL_VARIABLE}=1 only for a disposable fixture"
        );
        if service_mutation {
            #[cfg(windows)]
            let approval = "DISPOSABLE-VM-AND-KAKUNECORE-ABSENT";
            #[cfg(target_os = "linux")]
            let approval = "DISPOSABLE-VM-AND-KAKUNE-CORE-SERVICE-ABSENT";
            assert!(
                std::env::var("KAKUNE_NATIVE_SERVICE_APPROVAL")
                    .is_ok_and(|value| value == approval),
                "service acceptance requires explicit approval for a disposable VM and an absent native registration"
            );
            #[cfg(target_os = "linux")]
            assert_eq!(
                rustix::process::geteuid().as_raw(),
                0,
                "systemd service acceptance requires root inside the disposable VM"
            );
            assert!(
                std::env::var("KAKUNE_NATIVE_SERVICE_ACCOUNT_READY")
                    .is_ok_and(|value| value == "1"),
                "provision and verify a disposable service-account fixture without copying user credentials"
            );
        }
    }
}

struct NativeFixture {
    root: PathBuf,
    id: String,
    preserve: Arc<AtomicBool>,
}

impl NativeFixture {
    fn new() -> io::Result<Self> {
        let root = approved_native_root()?;
        fs::create_dir_all(&root)?;
        if fs::symlink_metadata(&root)?.file_type().is_symlink() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "native acceptance root must not be redirected",
            ));
        }
        let id = uuid::Uuid::new_v4().to_string();
        let fixture = root.join(format!("kakune-native-{id}"));
        fs::create_dir(&fixture)?;
        fs::write(fixture.join(FIXTURE_MARKER), &id)?;
        Ok(Self {
            root: fixture,
            id,
            preserve: Arc::new(AtomicBool::new(false)),
        })
    }
}

impl Drop for NativeFixture {
    fn drop(&mut self) {
        // A failed acceptance keeps its data/logs even when the registration
        // guard can safely stop/remove the service during assertion unwinding.
        if thread::panicking() {
            self.preserve.store(true, Ordering::SeqCst);
        }
        if self.preserve.load(Ordering::SeqCst) {
            return;
        }
        let Ok(base) = approved_native_root().and_then(|path| path.canonicalize()) else {
            return;
        };
        let Ok(root) = self.root.canonicalize() else {
            return;
        };
        if root.parent() == Some(base.as_path())
            && root
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("kakune-native-"))
            && fs::read_to_string(root.join(FIXTURE_MARKER))
                .ok()
                .as_deref()
                == Some(self.id.as_str())
        {
            let _ = fs::remove_dir_all(root);
        }
    }
}

fn approved_native_root() -> io::Result<PathBuf> {
    #[cfg(windows)]
    {
        let local_app_data = std::env::var_os("LOCALAPPDATA").ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "LOCALAPPDATA is unavailable")
        })?;
        Ok(PathBuf::from(local_app_data).join("Kakune-Spec002-NativeAcceptance"))
    }
    #[cfg(target_os = "linux")]
    {
        let home = std::env::var_os("HOME")
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "HOME is unavailable"))?;
        Ok(PathBuf::from(home).join(".local/share/Kakune-Spec002-NativeAcceptance"))
    }
    #[cfg(not(any(windows, target_os = "linux")))]
    {
        Ok(std::env::temp_dir().join("opencode"))
    }
}

fn resolve_native_cli_executable(
    selected: Option<&Path>,
    approved: bool,
) -> Result<PathBuf, String> {
    let Some(path) = selected else {
        return Ok(PathBuf::from(env!("CARGO_BIN_EXE_kakune")));
    };
    if !approved {
        return Err(
            "a selected native test executable requires explicit acceptance approval".into(),
        );
    }
    if !path.is_absolute() {
        return Err("the native test executable must be an absolute path".into());
    }
    let metadata = fs::symlink_metadata(path).map_err(|error| error.to_string())?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || path.canonicalize().map_err(|error| error.to_string())? != path
    {
        return Err("the native test executable must be a non-redirected regular file".into());
    }
    Ok(path.to_owned())
}

fn native_cli_executable() -> Result<PathBuf, String> {
    let selected = std::env::var_os("KAKUNE_NATIVE_CLI_EXECUTABLE").map(PathBuf::from);
    // Test-only selection lets the approved root harness invoke the separately
    // hash-verified, root-owned binary rather than a user-writable Cargo target.
    resolve_native_cli_executable(
        selected.as_deref(),
        std::env::var("KAKUNE_RUN_NATIVE_ACCEPTANCE").is_ok_and(|value| value == "1"),
    )
}

fn run_cli(root: &Path, arguments: &[&str], timeout: Duration) -> Result<Output, String> {
    let output_id = NEXT_OUTPUT.fetch_add(1, Ordering::Relaxed);
    let stdout_path = root.join(format!("cli-{output_id}.stdout"));
    let stderr_path = root.join(format!("cli-{output_id}.stderr"));
    let stdout = fs::File::create(&stdout_path).map_err(|error| error.to_string())?;
    let stderr = fs::File::create(&stderr_path).map_err(|error| error.to_string())?;
    let mut child = Command::new(native_cli_executable()?)
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .map_err(|error| format!("cannot start Kakune acceptance command: {error}"))?;
    let status = wait_bounded(&mut child, timeout)?;
    Ok(Output {
        status,
        stdout: fs::read(stdout_path).map_err(|error| error.to_string())?,
        stderr: fs::read(stderr_path).map_err(|error| error.to_string())?,
    })
}

fn wait_bounded(child: &mut Child, timeout: Duration) -> Result<std::process::ExitStatus, String> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("cannot observe owned acceptance command: {error}"))?
        {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err("owned Kakune command exceeded its harness wait bound".to_owned());
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn cli_text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn fixture_data(fixture: &NativeFixture, name: &str, listen: &str) -> (PathBuf, String) {
    let data = fixture.root.join(name);
    fs::create_dir_all(&data).expect("test-owned data directory should be created");
    let config = format!("api:\n  listen: {listen}\n");
    fs::write(data.join("kakune.yaml"), config).expect("isolated config should be written");
    let store = Store::open(data.clone()).expect("test-owned store should open");
    let token = store
        .ensure_bootstrap_token()
        .expect("test bootstrap token should be available")
        .expect("new isolated store should create its test-only token");
    let workflow_source = r#"apiVersion: kakune/v1
kind: Workflow
metadata: { id: native-acceptance, name: Native acceptance }
triggers: [{ id: manual, type: kakune.trigger.manual@1 }]
entry: finish
nodes:
  - id: finish
    type: kakune.flow.pass@1
"#;
    let workflow =
        WorkflowDocument::parse(workflow_source).expect("native acceptance workflow should parse");
    store
        .upsert_workflow(&workflow, workflow_source, "enabled")
        .expect("native acceptance workflow should be stored");
    (data, token)
}

fn available_address() -> String {
    let reservation = TcpListener::bind("127.0.0.1:0").expect("free local port should bind");
    reservation
        .local_addr()
        .expect("test listener should expose its address")
        .to_string()
}

struct DaemonGuard {
    root: PathBuf,
    data: PathBuf,
    active: bool,
    preserve: Arc<AtomicBool>,
}

impl DaemonGuard {
    fn new(root: PathBuf, data: PathBuf, preserve: Arc<AtomicBool>) -> Self {
        Self {
            root,
            data,
            active: false,
            preserve,
        }
    }

    fn stop(&mut self) -> Result<(), String> {
        let Some(data_arg) = self.data.to_str() else {
            let details = format!(
                "Cannot safely request daemon cleanup because the fixture path is not UTF-8.\n\nData directory: `{}`\n",
                self.data.display()
            );
            self.preserve.store(true, Ordering::SeqCst);
            self.active = false;
            let _ = fs::write(
                self.root.join("recovery-inventory.md"),
                format!("# Kakune native daemon recovery inventory\n\n{details}"),
            );
            return Err(details);
        };
        let stop = run_cli(
            &self.root,
            &["daemon", "--data-dir", data_arg, "stop"],
            Duration::from_secs(40),
        );
        let status = run_cli(
            &self.root,
            &["daemon", "--data-dir", data_arg, "status"],
            Duration::from_secs(12),
        );
        let lease = verify_runtime_lease_released(&self.data);
        let status_confirms_stopped = status.as_ref().is_ok_and(|output| {
            output.status.success() && cli_text(output).contains("state=stopped")
        });
        let stop_succeeded = stop.as_ref().is_ok_and(|output| output.status.success());
        if stop_succeeded && status_confirms_stopped && lease.is_ok() {
            self.active = false;
            return Ok(());
        }

        let details = format!(
            "Daemon teardown remains unconfirmed.\n\nData directory: `{}`\nStop result: `{}`\nRead-only status: `{}`\nRuntime lease: `{}`\n",
            self.data.display(),
            describe_cli_result(stop),
            describe_cli_result(status),
            lease
                .as_ref()
                .map_or_else(|error| error.clone(), |_| "released".to_owned())
        );
        self.preserve.store(true, Ordering::SeqCst);
        self.active = false;
        let _ = fs::write(
            self.root.join("recovery-inventory.md"),
            format!("# Kakune native daemon recovery inventory\n\n{details}"),
        );
        Err(details)
    }
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        if self.active {
            let _ = self.stop();
        }
    }
}

struct ForegroundGuard {
    root: PathBuf,
    data: PathBuf,
    child: Child,
    pid: u32,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    active: bool,
    preserve: Arc<AtomicBool>,
}

impl ForegroundGuard {
    fn stop(&mut self) -> Result<(), String> {
        let process = match self.child.try_wait() {
            Ok(Some(status)) => Ok(status),
            Ok(None) => request_foreground_shutdown(&mut self.child)
                .and_then(|()| wait_foreground_exit(&mut self.child, Duration::from_secs(40))),
            Err(error) => Err(format!(
                "cannot inspect foreground child {}: {error}",
                self.pid
            )),
        };
        let status = daemon_status(&self.root, &self.data);
        let lease = verify_runtime_lease_released(&self.data);
        let status_confirms_stopped = status.as_ref().is_ok_and(|output| {
            output.status.success() && cli_text(output).contains("state=stopped")
        });
        let resources_released = process.is_ok() && status_confirms_stopped && lease.is_ok();

        if resources_released {
            self.active = false;
        }
        if resources_released && process.as_ref().is_ok_and(ExitStatus::success) {
            return Ok(());
        }

        let details = format!(
            "Foreground Core teardown remains unconfirmed.\n\nPID from retained child handle: {}\nData directory: `{}`\nProcess exit: `{}`\nRead-only status: `{}`\nRuntime lease: `{}`\nstdout log: `{}`\nstderr log: `{}`\n",
            self.pid,
            self.data.display(),
            process.as_ref().map_or_else(
                |error| error.clone(),
                |status| format!("{:?}", status.code())
            ),
            describe_cli_result(status),
            lease
                .as_ref()
                .map_or_else(|error| error.clone(), |_| "released".to_owned()),
            self.stdout_path.display(),
            self.stderr_path.display()
        );
        if !resources_released {
            self.preserve.store(true, Ordering::SeqCst);
            self.active = false;
            let _ = fs::write(
                self.root.join("recovery-inventory.md"),
                format!("# Kakune native foreground recovery inventory\n\n{details}"),
            );
        }
        Err(details)
    }
}

impl Drop for ForegroundGuard {
    fn drop(&mut self) {
        if self.active {
            let _ = self.stop();
        }
    }
}

fn daemon_status(root: &Path, data: &Path) -> Result<Output, String> {
    let data_arg = data
        .to_str()
        .ok_or_else(|| "daemon data path is not UTF-8".to_owned())?;
    run_cli(
        root,
        &["daemon", "--data-dir", data_arg, "status"],
        Duration::from_secs(12),
    )
}

fn spawn_foreground(
    root: &Path,
    data: PathBuf,
    address: &str,
    preserve: Arc<AtomicBool>,
) -> Result<ForegroundGuard, String> {
    let id = NEXT_OUTPUT.fetch_add(1, Ordering::Relaxed);
    let stdout_path = root.join(format!("foreground-{id}.stdout"));
    let stderr_path = root.join(format!("foreground-{id}.stderr"));
    let stdout = fs::File::create(&stdout_path).map_err(|error| error.to_string())?;
    let stderr = fs::File::create(&stderr_path).map_err(|error| error.to_string())?;
    let data_arg = data
        .to_str()
        .ok_or_else(|| "foreground data path is not UTF-8".to_owned())?;
    let config = data.join("kakune.yaml");
    let config_arg = config
        .to_str()
        .ok_or_else(|| "foreground config path is not UTF-8".to_owned())?;
    let mut command = Command::new(native_cli_executable()?);
    command
        .args([
            "daemon",
            "--data-dir",
            data_arg,
            "--config",
            config_arg,
            "--listen",
            address,
            "--allowed-host",
            "127.0.0.1",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;

        // Keep the child attached to this console while assigning it a group
        // that can receive a targeted Ctrl+Break event from the test harness.
        command.creation_flags(0x0000_0200);
    }
    let child = command
        .spawn()
        .map_err(|error| format!("cannot start foreground Core: {error}"))?;
    let pid = child.id();
    Ok(ForegroundGuard {
        root: root.to_path_buf(),
        data,
        child,
        pid,
        stdout_path,
        stderr_path,
        active: true,
        preserve,
    })
}

fn wait_foreground_exit(child: &mut Child, timeout: Duration) -> Result<ExitStatus, String> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("cannot observe owned foreground Core: {error}"))?
        {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            return Err(
                "foreground Core did not exit after a graceful signal; process was not killed"
                    .to_owned(),
            );
        }
        thread::sleep(Duration::from_millis(20));
    }
}

#[cfg(unix)]
fn request_foreground_shutdown(child: &mut Child) -> Result<(), String> {
    rustix::process::kill_process(
        rustix::process::Pid::from_child(child),
        rustix::process::Signal::TERM,
    )
    .map_err(|error| format!("cannot send SIGTERM to retained foreground child: {error}"))
}

#[cfg(windows)]
fn request_foreground_shutdown(child: &mut Child) -> Result<(), String> {
    let script = format!(
        "$source = 'using System; using System.Runtime.InteropServices; public static class KakuneCtrlBreak {{ [DllImport(\"kernel32.dll\", SetLastError = true)] public static extern bool GenerateConsoleCtrlEvent(uint eventType, uint processGroupId); }}'; Add-Type -TypeDefinition $source; if (-not [KakuneCtrlBreak]::GenerateConsoleCtrlEvent(1, [uint32]{})) {{ [Console]::Error.WriteLine([Runtime.InteropServices.Marshal]::GetLastWin32Error()); exit 1 }}",
        child.id()
    );
    let output = Command::new("powershell.exe")
        .args(["-NoProfile", "-NonInteractive", "-Command", &script])
        .output()
        .map_err(|error| {
            format!("cannot request targeted Ctrl+Break for foreground child: {error}")
        })?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "targeted Ctrl+Break was not accepted: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

#[cfg(not(any(unix, windows)))]
fn request_foreground_shutdown(_child: &mut Child) -> Result<(), String> {
    Err("foreground signal acceptance is unsupported on this platform".to_owned())
}

async fn wait_for_foreground_ready(
    child: &mut Child,
    client: &reqwest::Client,
    address: &str,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("cannot observe foreground child: {error}"))?
        {
            return Err(format!(
                "foreground Core exited before readiness with {status:?}"
            ));
        }
        if let Ok(response) = client
            .get(format!("http://{address}/health/ready"))
            .send()
            .await
            && response.status().is_success()
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(
                "foreground Core did not become ready within the harness wait bound".to_owned(),
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn run_foreground_acceptance(fixture: &NativeFixture) {
    let address = available_address();
    let (data, token) = fixture_data(fixture, "foreground-data", &address);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .expect("foreground API client should build");
    let mut foreground = spawn_foreground(
        &fixture.root,
        data.clone(),
        &address,
        fixture.preserve.clone(),
    )
    .expect("foreground CLI should start as an owned child");
    wait_for_foreground_ready(
        &mut foreground.child,
        &client,
        &address,
        Duration::from_secs(35),
    )
    .await
    .unwrap_or_else(|error| panic!("foreground CLI did not reach readiness: {error}"));

    let response = client
        .post(format!("http://{address}/api/v1/executions"))
        .bearer_auth(&token)
        .json(&json!({ "workflowId": "native-acceptance" }))
        .send()
        .await
        .expect("foreground Core should accept a local workflow operation");
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    let execution_id = response
        .json::<serde_json::Value>()
        .await
        .expect("foreground execution response should be JSON")["id"]
        .as_str()
        .expect("foreground execution ID should be present")
        .to_owned();
    let execution_url = format!("http://{address}/api/v1/executions/{execution_id}");
    let execution_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let response = client
            .get(&execution_url)
            .bearer_auth(&token)
            .send()
            .await
            .expect("foreground execution state should be queryable");
        let body = response
            .json::<serde_json::Value>()
            .await
            .expect("foreground execution state should be JSON");
        if body["execution"]["status"] == "succeeded" {
            break;
        }
        assert!(
            Instant::now() < execution_deadline,
            "foreground workflow did not finish: {body}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    foreground
        .stop()
        .unwrap_or_else(|error| panic!("foreground signal shutdown failed: {error}"));
    assert_eq!(
        Store::open(data)
            .expect("foreground store should reopen after shutdown")
            .list_executions()
            .expect("foreground execution should remain persisted")
            .iter()
            .filter(|record| record.status == "succeeded")
            .count(),
        1
    );
}

fn record_latency(samples: &mut Vec<Duration>, label: &str, elapsed: Duration) {
    samples.push(elapsed);
    println!("native-timing\t{label}\t{}ms", elapsed.as_millis());
}

fn report_p95(label: &str, samples: &mut [Duration]) {
    samples.sort_unstable();
    let index = samples
        .len()
        .saturating_mul(95)
        .div_ceil(100)
        .saturating_sub(1);
    println!(
        "native-p95\t{label}\t{}ms\tsamples={}",
        samples[index].as_millis(),
        samples.len()
    );
}

fn verify_runtime_lease_released(data_dir: &Path) -> Result<(), String> {
    let lock_path = data_dir.join(".kakune").join("runtime.lock");
    if !lock_path.exists() {
        return Ok(());
    }
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(|error| format!("stopped Core runtime lock cannot be inspected: {error}"))?;
    lock.try_lock()
        .map_err(|error| format!("runtime lease release is unconfirmed: {error}"))?;
    Ok(())
}

#[tokio::test]
#[cfg(windows)]
#[ignore = "requires explicit native-acceptance opt-in; never runs in the default suite"]
async fn windows_native_user_daemon_cycles_concurrency_and_timing() {
    native_user_daemon_cycles_concurrency_and_timing().await;
}

#[tokio::test]
#[cfg(target_os = "linux")]
#[ignore = "requires explicit native-acceptance opt-in; never runs in the default suite"]
async fn linux_native_user_daemon_cycles_concurrency_and_timing() {
    native_user_daemon_cycles_concurrency_and_timing().await;
}

#[cfg(any(windows, target_os = "linux"))]
async fn native_user_daemon_cycles_concurrency_and_timing() {
    require_native_approval(false);
    let fixture = NativeFixture::new().expect("approved native fixture should be created");
    run_foreground_acceptance(&fixture).await;
    let mut start_samples = Vec::new();
    let mut status_samples = Vec::new();
    let mut stop_samples = Vec::new();
    let initial_address = available_address();
    let (data, token) = fixture_data(&fixture, "daemon-data", &initial_address);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .expect("acceptance API client should build");

    for cycle in 0..20 {
        let address = available_address();
        let mut guard =
            DaemonGuard::new(fixture.root.clone(), data.clone(), fixture.preserve.clone());
        guard.active = true;
        let listen = address.as_str();
        let data_arg = data.to_str().expect("fixture data path should be UTF-8");
        let started = Instant::now();
        let start = run_cli(
            &fixture.root,
            &[
                "daemon",
                "--data-dir",
                data.to_str().expect("fixture data path should be UTF-8"),
                "--config",
                data.join("kakune.yaml")
                    .to_str()
                    .expect("config path should be UTF-8"),
                "--listen",
                listen,
                "--allowed-host",
                "127.0.0.1",
                "start",
            ],
            Duration::from_secs(35),
        )
        .unwrap_or_else(|error| panic!("cycle {cycle} start failed: {error}"));
        record_latency(&mut start_samples, "daemon-start", started.elapsed());
        assert!(
            start.status.success(),
            "cycle {cycle} start must reach readiness: {}",
            cli_text(&start)
        );

        let status_started = Instant::now();
        let status = run_cli(
            &fixture.root,
            &["daemon", "--data-dir", data_arg, "status"],
            Duration::from_secs(10),
        )
        .expect("daemon status should remain bounded");
        record_latency(
            &mut status_samples,
            "daemon-status",
            status_started.elapsed(),
        );
        assert!(status.status.success());
        assert!(cli_text(&status).contains("state=ready"));

        let response = client
            .post(format!("http://{address}/api/v1/executions"))
            .bearer_auth(&token)
            .json(&json!({"workflowId":"native-acceptance"}))
            .send()
            .await
            .expect("ready Core should accept a local workflow operation");
        assert_eq!(response.status(), reqwest::StatusCode::CREATED);
        let execution_id = response
            .json::<serde_json::Value>()
            .await
            .expect("execution response should be JSON")["id"]
            .as_str()
            .expect("execution ID should be present")
            .to_owned();
        let execution_url = format!("http://{address}/api/v1/executions/{execution_id}");
        let execution_deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let response = client
                .get(&execution_url)
                .bearer_auth(&token)
                .send()
                .await
                .expect("execution state should remain queryable");
            let body = response
                .json::<serde_json::Value>()
                .await
                .expect("execution state should be JSON");
            if body["execution"]["status"] == "succeeded" {
                break;
            }
            assert!(
                Instant::now() < execution_deadline,
                "workflow did not finish: {body}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        let stopped_at = Instant::now();
        guard
            .stop()
            .unwrap_or_else(|error| panic!("cycle {cycle} stop failed: {error}"));
        record_latency(&mut stop_samples, "daemon-stop", stopped_at.elapsed());
        let stopped = run_cli(
            &fixture.root,
            &["daemon", "--data-dir", data_arg, "status"],
            Duration::from_secs(10),
        )
        .expect("post-stop status should remain bounded");
        assert!(stopped.status.success());
        assert!(cli_text(&stopped).contains("state=stopped"));
        verify_runtime_lease_released(&data)
            .expect("stopped daemon must release its runtime lease");
        assert_eq!(
            Store::open(data.clone())
                .expect("retained store should reopen")
                .list_executions()
                .expect("completed execution should remain persisted")
                .iter()
                .filter(|record| record.status == "succeeded")
                .count(),
            cycle + 1
        );
        drop(guard);
    }

    report_p95("daemon-start", &mut start_samples);
    report_p95("daemon-status", &mut status_samples);
    report_p95("daemon-stop", &mut stop_samples);

    let concurrent_data = fixture.root.join("concurrent-daemon-data");
    let concurrent_address = available_address();
    let (_data, _token) = fixture_data(&fixture, "concurrent-daemon-data", &concurrent_address);
    let data_arg = concurrent_data
        .to_str()
        .expect("concurrent fixture path should be UTF-8")
        .to_owned();
    let config_arg = concurrent_data.join("kakune.yaml");
    let address_arg = concurrent_address.clone();
    let preserve = fixture.preserve.clone();
    let mut guard = DaemonGuard::new(fixture.root.clone(), concurrent_data, preserve);
    guard.active = true;
    let outputs = thread::scope(|scope| {
        (0..10)
            .map(|_| {
                let root = fixture.root.clone();
                let data = data_arg.clone();
                let config = config_arg.clone();
                let address = address_arg.clone();
                scope.spawn(move || {
                    run_cli(
                        &root,
                        &[
                            "daemon",
                            "--data-dir",
                            &data,
                            "--config",
                            config.to_str().expect("config path should be UTF-8"),
                            "--listen",
                            &address,
                            "--allowed-host",
                            "127.0.0.1",
                            "start",
                        ],
                        Duration::from_secs(35),
                    )
                })
            })
            .map(|worker| worker.join().expect("competing start should finish"))
            .collect::<Vec<_>>()
    });
    let ready_count = outputs
        .iter()
        .filter(|output| {
            output.as_ref().is_ok_and(|output| {
                output.status.success() && cli_text(output).contains("state=ready")
            })
        })
        .count();
    assert_eq!(
        ready_count, 1,
        "exactly one of ten starts must become ready"
    );
    guard
        .stop()
        .expect("the one test-owned daemon should stop cleanly");
}

struct ServiceRegistrationGuard {
    root: PathBuf,
    fixture_root: PathBuf,
    retention_sentinel: PathBuf,
    preserve: Arc<AtomicBool>,
    armed: bool,
}

impl ServiceRegistrationGuard {
    fn cleanup(&mut self) -> Result<(), String> {
        let fixture_root = self.fixture_root.clone();
        self.cleanup_with(move |arguments, timeout| run_cli(&fixture_root, arguments, timeout))
    }

    fn cleanup_with(
        &mut self,
        mut run: impl FnMut(&[&str], Duration) -> Result<Output, String>,
    ) -> Result<(), String> {
        if !self.armed {
            return Ok(());
        }
        let stop = run(&["service", "stop"], Duration::from_secs(65));
        let after_stop = run(&["service", "status"], Duration::from_secs(12));
        let stop_succeeded = stop.as_ref().is_ok_and(|output| output.status.success());
        let stopped_state_confirmed = after_stop.as_ref().is_ok_and(|output| {
            output.status.success() && service_observation_is_stopped(&cli_text(output))
        });
        let lease_after_stop = verify_runtime_lease_released(&self.root);
        let sentinel_after_stop = verify_retention_sentinel(&self.retention_sentinel);

        if !stop_succeeded
            || !stopped_state_confirmed
            || lease_after_stop.is_err()
            || sentinel_after_stop.is_err()
        {
            let absence_confirmed = after_stop.as_ref().is_ok_and(|output| {
                output.status.success() && service_observation_is_uninstalled(&cli_text(output))
            }) && lease_after_stop.is_ok()
                && sentinel_after_stop.is_ok();
            let details = format!(
                "Stop must be confirmed before uninstall; uninstall was not attempted.\n\nService ID: `{}`\nData directory: `{}`\nStop result: `{}`\nRead-only status after stop: `{}`\nRuntime lease: `{}`\nData sentinel: `{}`\n",
                native_service_id(),
                self.root.display(),
                describe_cli_result(stop),
                describe_cli_result(after_stop),
                describe_check(lease_after_stop),
                describe_check(sentinel_after_stop)
            );
            if absence_confirmed {
                self.armed = false;
                return Err(details);
            }
            return self.retain_cleanup_failure(details);
        }

        let uninstall = run(&["service", "uninstall"], Duration::from_secs(95));
        let final_status = run(&["service", "status"], Duration::from_secs(12));
        let lease_after_uninstall = verify_runtime_lease_released(&self.root);
        let sentinel_after_uninstall = verify_retention_sentinel(&self.retention_sentinel);
        let removal_confirmed = final_status.as_ref().is_ok_and(|output| {
            output.status.success() && service_observation_is_uninstalled(&cli_text(output))
        });

        if uninstall
            .as_ref()
            .is_ok_and(|output| output.status.success())
            && removal_confirmed
            && lease_after_uninstall.is_ok()
            && sentinel_after_uninstall.is_ok()
        {
            self.armed = false;
            return Ok(());
        }

        self.retain_cleanup_failure(format!(
            "Native service cleanup did not confirm registration removal and data retention.\n\nService ID: `{}`\nData directory: `{}`\nStop result: `{}`\nRead-only status after stop: `{}`\nUninstall result: `{}`\nFinal read-only status: `{}`\nRuntime lease: `{}`\nData sentinel: `{}`\n",
            native_service_id(),
            self.root.display(),
            describe_cli_result(stop),
            describe_cli_result(after_stop),
            describe_cli_result(uninstall),
            describe_cli_result(final_status),
            describe_check(lease_after_uninstall),
            describe_check(sentinel_after_uninstall)
        ))
    }

    fn retain_cleanup_failure(&mut self, details: String) -> Result<(), String> {
        self.preserve.store(true, Ordering::SeqCst);
        self.armed = false;
        let inventory = format!(
            "# Kakune service recovery inventory\n\n{details}\nRecovery: preserve this fixture and inspect the native registration, process identity, journal, and data directory read-only before any further mutation. Do not remove the registration unless stop and process exit are confirmed.\n"
        );
        let _ = fs::write(self.fixture_root.join("recovery-inventory.md"), inventory);
        Err(details)
    }
}

fn native_service_id() -> &'static str {
    if cfg!(target_os = "linux") {
        "kakune-core.service"
    } else {
        "KakuneCore"
    }
}

fn service_observation_is_stopped(text: &str) -> bool {
    text.contains("installed\tInstalled")
        && text.contains("managerState\tStopped")
        && text.contains("coreState\tstopped")
        && !text.contains("pendingOperation\ttrue")
}

fn service_observation_is_uninstalled(text: &str) -> bool {
    text.contains("installed\tNotInstalled")
        && text.contains("managerState\tStopped")
        && !text.contains("pendingOperation\ttrue")
}

fn verify_retention_sentinel(path: &Path) -> Result<(), String> {
    match fs::read(path) {
        Ok(contents) if contents == b"test-owned persistence sentinel" => Ok(()),
        Ok(_) => Err(format!(
            "data retention sentinel changed: {}",
            path.display()
        )),
        Err(error) => Err(format!(
            "data retention sentinel is unavailable at {}: {error}",
            path.display()
        )),
    }
}

fn describe_check(result: Result<(), String>) -> String {
    result.map_or_else(|error| error, |_| "confirmed".to_owned())
}

fn describe_cli_result(result: Result<Output, String>) -> String {
    result.map_or_else(
        |error| error,
        |output| format!("exit={:?}: {}", output.status.code(), cli_text(&output)),
    )
}

impl Drop for ServiceRegistrationGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.cleanup();
        }
    }
}

#[tokio::test]
#[cfg(windows)]
#[ignore = "requires explicit approval for a disposable VM and absent KakuneCore registration"]
async fn windows_native_service_lifecycle_and_data_retention() {
    native_service_lifecycle_and_data_retention().await;
}

#[tokio::test]
#[cfg(target_os = "linux")]
#[ignore = "requires explicit approval for a disposable root/systemd VM and absent kakune-core.service"]
async fn linux_native_service_lifecycle_and_data_retention() {
    native_service_lifecycle_and_data_retention().await;
}

#[cfg(any(windows, target_os = "linux"))]
async fn submit_service_fixture_execution(
    client: &reqwest::Client,
    listen: &str,
    token: &str,
) -> String {
    let response = client
        .post(format!("http://{listen}/api/v1/executions"))
        .bearer_auth(token)
        .json(&json!({"workflowId":"native-acceptance"}))
        .send()
        .await
        .expect("installed-account API should accept a fixture workflow");
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    let body = response
        .json::<serde_json::Value>()
        .await
        .expect("fixture submission should return JSON");
    let id = body["id"]
        .as_str()
        .expect("fixture execution ID")
        .to_owned();
    assert_service_fixture_execution(client, listen, token, &id).await;
    id
}

#[cfg(any(windows, target_os = "linux"))]
async fn assert_service_fixture_execution(
    client: &reqwest::Client,
    listen: &str,
    token: &str,
    id: &str,
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let response = client
            .get(format!("http://{listen}/api/v1/executions/{id}"))
            .bearer_auth(token)
            .send()
            .await
            .expect("fixture execution should remain queryable");
        assert!(response.status().is_success());
        let body = response
            .json::<serde_json::Value>()
            .await
            .expect("fixture execution should return JSON");
        if body["execution"]["status"] == "succeeded" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "fixture execution did not succeed before its bounded wait"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[cfg(any(windows, target_os = "linux"))]
fn assert_retained_service_fixture(data: &Path, listen: &str, token: &str, ids: &[String]) {
    let retained = Store::open(data.to_owned()).expect("retained fixture store should reopen");
    for id in ids {
        assert_eq!(
            retained
                .get_execution(id)
                .unwrap()
                .expect("completed execution must survive service removal/reinstall")
                .status,
            "succeeded"
        );
    }
    assert_eq!(ids.len(), 21);
    assert_eq!(retained.list_executions().unwrap().len(), ids.len());
    assert!(
        retained
            .authorize_scope(token, kakune_core::AuthScope::Admin)
            .unwrap()
    );
    assert_eq!(
        fs::read_to_string(data.join("kakune.yaml")).unwrap(),
        format!("api:\n  listen: {listen}\n")
    );
}

#[cfg(any(windows, target_os = "linux"))]
async fn native_service_lifecycle_and_data_retention() {
    require_native_approval(true);
    let fixture = NativeFixture::new().expect("approved service fixture should be created");
    let listen = available_address();
    let (data, token) = fixture_data(&fixture, "service-data", &listen);
    let marker = data.join("retained-by-service-uninstall.txt");
    fs::write(&marker, b"test-owned persistence sentinel")
        .expect("service retention marker should be written");
    let data_arg = data.to_str().expect("service data path should be UTF-8");

    let before = run_cli(
        &fixture.root,
        &["service", "status"],
        Duration::from_secs(12),
    )
    .expect("read-only service preflight should finish");
    assert!(
        before.status.success(),
        "service preflight failed: {}",
        cli_text(&before)
    );
    assert!(
        cli_text(&before).contains("NotInstalled"),
        "the native service must be confirmed absent before mutation: {}",
        cli_text(&before)
    );
    let mut registration = ServiceRegistrationGuard {
        root: data.clone(),
        fixture_root: fixture.root.clone(),
        retention_sentinel: marker.clone(),
        preserve: fixture.preserve.clone(),
        armed: true,
    };

    for arguments in [
        vec!["service", "install", "--data-dir", data_arg],
        vec!["service", "status"],
        vec!["service", "install", "--data-dir", data_arg],
        vec!["service", "start"],
        vec!["service", "status"],
    ] {
        let output = run_cli(&fixture.root, &arguments, Duration::from_secs(55))
            .expect("service command should stay within its harness wait cap");
        assert!(
            output.status.success(),
            "{}: {}",
            arguments.join(" "),
            cli_text(&output)
        );
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        // Each cycle deliberately replaces the server process. Never reuse a
        // socket pooled from a previous instance as readiness evidence.
        .pool_max_idle_per_host(0)
        .build()
        .expect("service API client should build");
    let mut execution_ids = vec![submit_service_fixture_execution(&client, &listen, &token).await];

    // These samples are real installed-account/manager operations, never fake
    // timing evidence. The entire test remains ignored and approval-gated.
    let mut start_samples = Vec::new();
    let mut status_samples = Vec::new();
    let mut stop_samples = Vec::new();
    for cycle in 0..20 {
        // systemd's default start rate limit accepts only a few starts per
        // interval. Pace the cycles instead of weakening the packaged unit's
        // documented restart policy, and keep the wait outside the measured
        // command latency.
        #[cfg(target_os = "linux")]
        tokio::time::sleep(NATIVE_SERVICE_CYCLE_DELAY).await;
        for (command, samples, limit) in [
            ("stop", &mut stop_samples, 65),
            ("start", &mut start_samples, 55),
            ("status", &mut status_samples, 12),
        ] {
            let started = Instant::now();
            let output = run_cli(
                &fixture.root,
                &["service", command],
                Duration::from_secs(limit),
            )
            .expect("approved service timing command should remain bounded");
            record_latency(samples, &format!("service-{command}"), started.elapsed());
            assert!(
                output.status.success(),
                "cycle {cycle} service {command}: {}",
                cli_text(&output)
            );
        }
        let response = client
            .get(format!("http://{listen}/health/ready"))
            .send()
            .await
            .expect("restarted installed-account Core should be reachable");
        assert!(response.status().is_success());
        assert_eq!(
            fs::read(&marker).unwrap(),
            b"test-owned persistence sentinel"
        );
        assert_service_fixture_execution(&client, &listen, &token, &execution_ids[0]).await;
        execution_ids.push(submit_service_fixture_execution(&client, &listen, &token).await);
    }
    report_p95("service-start", &mut start_samples);
    report_p95("service-status", &mut status_samples);
    report_p95("service-stop", &mut stop_samples);

    for command in ["stop", "stop", "uninstall", "uninstall"] {
        let output = run_cli(
            &fixture.root,
            &["service", command],
            Duration::from_secs(if command == "uninstall" { 95 } else { 65 }),
        )
        .expect("service cleanup command should remain bounded");
        assert!(
            output.status.success(),
            "service {command}: {}",
            cli_text(&output)
        );
    }
    assert_eq!(
        fs::read(&marker).unwrap(),
        b"test-owned persistence sentinel"
    );
    assert_retained_service_fixture(&data, &listen, &token, &execution_ids);
    let absent = run_cli(
        &fixture.root,
        &["service", "status"],
        Duration::from_secs(12),
    )
    .expect("post-uninstall status should finish");
    assert!(absent.status.success());
    assert!(service_observation_is_uninstalled(&cli_text(&absent)));
    verify_runtime_lease_released(&data)
        .expect("uninstalled service should release the test data lease");
    verify_retention_sentinel(&marker).expect("uninstall must retain the fixture sentinel");
    registration.armed = false;

    registration.armed = true;
    let reinstall = run_cli(
        &fixture.root,
        &["service", "install", "--data-dir", data_arg],
        Duration::from_secs(55),
    )
    .expect("service reinstall should finish");
    assert!(reinstall.status.success(), "{}", cli_text(&reinstall));
    assert_retained_service_fixture(&data, &listen, &token, &execution_ids);
    assert_eq!(
        fs::read(&marker).unwrap(),
        b"test-owned persistence sentinel"
    );
    #[cfg(windows)]
    if std::env::var("KAKUNE_NATIVE_KEEP_FOR_BOOT").is_ok_and(|value| value == "1") {
        let inventory = fixture.root.join("boot-check.json");
        native_windows_service::write_boot_inventory(&inventory, &fixture, &data, &listen, &token);
        fixture.preserve.store(true, Ordering::SeqCst);
        registration.armed = false;
        println!("native-boot-inventory\t{}", inventory.display());
        return;
    }
    #[cfg(target_os = "linux")]
    if std::env::var("KAKUNE_NATIVE_KEEP_FOR_BOOT").is_ok_and(|value| value == "1") {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;

        // Explicit staged boot acceptance retains a root-private disposable
        // fixture. Never print its test-only bearer token or copy user secrets.
        let inventory = fixture.root.join("boot-check.json");
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&inventory)
            .expect("root-private boot inventory should be created");
        file.write_all(
            &serde_json::to_vec(&json!({
                "fixtureId": fixture.id, "dataDir": data, "listen": listen, "token": token,
                "executable": native_cli_executable().expect("approved test executable"),
                "serviceId": "kakune-core.service", "executionIds": execution_ids
            }))
            .unwrap(),
        )
        .unwrap();
        file.sync_all().unwrap();
        fixture.preserve.store(true, Ordering::SeqCst);
        registration.armed = false;
        println!("native-boot-inventory\t{}", inventory.display());
        return;
    }
    eprintln!(
        "Automatic-boot verification requires a separate approved VM reboot and post-boot API check; this test does not claim boot acceptance."
    );
    registration
        .cleanup()
        .expect("test-owned service registration should be fully removed");
    verify_retention_sentinel(&marker).expect("service cleanup must retain the fixture sentinel");
}

#[cfg(test)]
mod cleanup_guard_tests {
    use super::*;

    #[test]
    fn failed_native_acceptance_marks_its_fixture_for_preservation() {
        let id = uuid::Uuid::new_v4().to_string();
        let root = std::env::temp_dir().join(format!("kakune-preservation-test-{id}"));
        fs::create_dir(&root).unwrap();
        fs::write(root.join(FIXTURE_MARKER), &id).unwrap();
        let preserve = Arc::new(AtomicBool::new(false));
        let fixture = NativeFixture {
            root: root.clone(),
            id,
            preserve: preserve.clone(),
        };
        let failure = std::panic::catch_unwind(move || {
            let _fixture = fixture;
            panic!("injected acceptance failure; no native resources exist");
        });
        assert!(failure.is_err());
        assert!(preserve.load(Ordering::SeqCst));
        assert!(root.join(FIXTURE_MARKER).is_file());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn native_executable_selection_requires_approval_before_path_access() {
        assert!(
            resolve_native_cli_executable(Some(Path::new("missing-relative-file")), false)
                .unwrap_err()
                .contains("approval")
        );
    }

    #[test]
    fn native_executable_selection_rejects_relative_paths() {
        assert!(
            resolve_native_cli_executable(Some(Path::new("relative-kakune")), true)
                .unwrap_err()
                .contains("absolute")
        );
    }

    #[test]
    fn native_executable_selection_accepts_an_existing_canonical_file() {
        let path = std::env::current_exe().unwrap().canonicalize().unwrap();
        assert_eq!(
            resolve_native_cli_executable(Some(&path), true).unwrap(),
            path
        );
    }

    #[test]
    fn default_native_executable_selection_preserves_cargo_path() {
        assert_eq!(
            resolve_native_cli_executable(None, false).unwrap(),
            PathBuf::from(env!("CARGO_BIN_EXE_kakune"))
        );
    }

    #[test]
    fn absent_registration_does_not_require_an_invented_core_identity() {
        assert!(service_observation_is_uninstalled(
            "installed\tNotInstalled\nmanagerState\tStopped\ncoreState\tunknown\npendingOperation\tfalse"
        ));
        assert!(!service_observation_is_uninstalled(
            "installed\tNotInstalled\nmanagerState\tStopped\npendingOperation\ttrue"
        ));
    }

    fn fake_exit_status(success: bool) -> ExitStatus {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;

            ExitStatus::from_raw(if success { 0 } else { 1 << 8 })
        }
        #[cfg(windows)]
        {
            use std::os::windows::process::ExitStatusExt;

            ExitStatus::from_raw(u32::from(!success))
        }
    }

    fn fake_output(success: bool, stdout: &str) -> Output {
        Output {
            status: fake_exit_status(success),
            stdout: stdout.as_bytes().to_vec(),
            stderr: Vec::new(),
        }
    }

    fn test_guard() -> (ServiceRegistrationGuard, String) {
        let id = uuid::Uuid::new_v4().to_string();
        let fixture_root = std::env::temp_dir().join(format!("kakune-guard-test-{id}"));
        fs::create_dir(&fixture_root).expect("unique local guard fixture should be created");
        fs::write(fixture_root.join(FIXTURE_MARKER), &id)
            .expect("guard fixture marker should be written");
        let data = fixture_root.join("data");
        fs::create_dir(&data).expect("guard test data should be created");
        let retention_sentinel = data.join("retained-by-service-uninstall.txt");
        fs::write(&retention_sentinel, b"test-owned persistence sentinel")
            .expect("guard test retention sentinel should be written");
        (
            ServiceRegistrationGuard {
                root: data,
                fixture_root,
                retention_sentinel,
                preserve: Arc::new(AtomicBool::new(false)),
                armed: true,
            },
            id,
        )
    }

    fn remove_test_fixture(guard: &ServiceRegistrationGuard, id: &str) {
        let marker = guard.fixture_root.join(FIXTURE_MARKER);
        if fs::read_to_string(&marker).ok().as_deref() == Some(id) {
            fs::remove_dir_all(&guard.fixture_root)
                .expect("only the uniquely marked local guard fixture should be removed");
        }
    }

    #[test]
    fn failed_stop_never_uninstalls_and_keeps_recovery_inventory() {
        let (mut guard, id) = test_guard();
        let mut calls: Vec<Vec<String>> = Vec::new();
        let result = guard.cleanup_with(|arguments, _timeout| {
            calls.push(arguments.iter().map(|argument| (*argument).to_owned()).collect());
            match arguments.get(1).copied() {
                Some("stop") => Ok(fake_output(false, "stop not confirmed")),
                Some("status") => Ok(fake_output(
                    true,
                    "installed\tInstalled\nmanagerState\tRunning\ncoreState\tready\npendingOperation\tfalse\n",
                )),
                Some("uninstall") => panic!("uninstall must not follow a failed stop"),
                other => panic!("unexpected cleanup command: {other:?}"),
            }
        });

        assert!(result.is_err());
        assert_eq!(
            calls,
            vec![
                vec!["service".to_owned(), "stop".to_owned()],
                vec!["service".to_owned(), "status".to_owned()]
            ]
        );
        assert!(!guard.armed);
        assert!(guard.preserve.load(Ordering::SeqCst));
        assert!(guard.fixture_root.join("recovery-inventory.md").exists());
        assert!(guard.root.exists());
        assert!(guard.retention_sentinel.exists());
        remove_test_fixture(&guard, &id);
    }

    #[test]
    fn confirmed_stop_precedes_uninstall_and_all_teardown_resources_are_checked() {
        let (mut guard, id) = test_guard();
        let mut calls: Vec<Vec<String>> = Vec::new();
        let mut statuses = 0;
        let result = guard.cleanup_with(|arguments, _timeout| {
            calls.push(arguments.iter().map(|argument| (*argument).to_owned()).collect());
            match arguments.get(1).copied() {
                Some("stop") | Some("uninstall") => Ok(fake_output(true, "success\n")),
                Some("status") => {
                    statuses += 1;
                    if statuses == 1 {
                        Ok(fake_output(
                            true,
                            "installed\tInstalled\nmanagerState\tStopped\ncoreState\tstopped\npendingOperation\tfalse\n",
                        ))
                    } else {
                        Ok(fake_output(
                            true,
                            "installed\tNotInstalled\nmanagerState\tStopped\ncoreState\tstopped\npendingOperation\tfalse\n",
                        ))
                    }
                }
                other => panic!("unexpected cleanup command: {other:?}"),
            }
        });

        assert!(result.is_ok());
        assert_eq!(
            calls,
            vec![
                vec!["service".to_owned(), "stop".to_owned()],
                vec!["service".to_owned(), "status".to_owned()],
                vec!["service".to_owned(), "uninstall".to_owned()],
                vec!["service".to_owned(), "status".to_owned()]
            ]
        );
        assert!(!guard.armed);
        assert!(!guard.preserve.load(Ordering::SeqCst));
        assert!(guard.root.exists());
        assert!(guard.retention_sentinel.exists());
        remove_test_fixture(&guard, &id);
    }
}
