use std::{
    collections::BTreeMap,
    fs,
    io::{self, Read},
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

use axum::serve;
use kakune_core::{AuthScope, ProviderAuth, ProviderProfileUpsert, ProviderType, Store, api};
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};

const CHILD_TIMEOUT: Duration = Duration::from_secs(15);
const WORKFLOW_SOURCE: &str = "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: legacy-flow\n  name: Legacy Flow\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: pass\nnodes:\n  - id: pass\n    type: kakune.flow.pass@1\n";

struct Fixture {
    root: PathBuf,
}

impl Fixture {
    fn new() -> io::Result<Self> {
        let approved_root = std::env::temp_dir().join("opencode");
        fs::create_dir_all(&approved_root)?;
        let root = approved_root.join(format!("kakune-legacy-contracts-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root)?;
        Ok(Self { root })
    }

    fn path(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }

    fn text(&self, name: &str) -> String {
        self.path(name).to_string_lossy().into_owned()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

struct Output {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

fn run_cli(fixture: &Fixture, arguments: &[String], environment: &[(&str, &str)]) -> Output {
    let output_id = uuid::Uuid::new_v4();
    let stdout_path = fixture.path(&format!("child-{output_id}.stdout"));
    let stderr_path = fixture.path(&format!("child-{output_id}.stderr"));
    let stdout = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&stdout_path)
        .expect("child stdout capture should be created");
    let stderr = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&stderr_path)
        .expect("child stderr capture should be created");
    let mut command = Command::new(env!("CARGO_BIN_EXE_kakune"));
    command
        .args(arguments)
        .env_remove("KAKUNE_DATA_DIR")
        .env_remove("KAKUNE_ENDPOINT")
        .env_remove("KAKUNE_TOKEN")
        .env_remove("KAKUNE_SECRET_STORE")
        .envs(environment.iter().copied())
        .current_dir(&fixture.root)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    let mut child = command.spawn().expect("Kakune CLI should spawn");
    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().expect("CLI child should be observable") {
            break status;
        }
        if started.elapsed() >= CHILD_TIMEOUT {
            let _ = child.kill();
            let _ = child.wait();
            panic!("Kakune CLI exceeded its {CHILD_TIMEOUT:?} contract-test deadline");
        }
        thread::sleep(Duration::from_millis(10));
    };
    Output {
        status,
        stdout: read_file(&stdout_path),
        stderr: read_file(&stderr_path),
    }
}

fn read_file(path: &Path) -> Vec<u8> {
    let mut bytes = Vec::new();
    fs::File::open(path)
        .and_then(|mut file| file.read_to_end(&mut bytes))
        .expect("captured child output should be readable");
    bytes
}

fn args(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn assert_success(output: &Output, expected_stdout: &str) {
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, expected_stdout.as_bytes());
    assert!(output.stderr.is_empty());
}

fn assert_failure(output: &Output, expected_stderr: &str) {
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert_eq!(output.stderr, expected_stderr.as_bytes());
}

fn snapshot_tree(root: &Path) -> io::Result<BTreeMap<PathBuf, Vec<u8>>> {
    fn visit(
        root: &Path,
        current: &Path,
        entries: &mut BTreeMap<PathBuf, Vec<u8>>,
    ) -> io::Result<()> {
        for entry in fs::read_dir(current)? {
            let entry = entry?;
            let path = entry.path();
            if entry.file_type()?.is_dir() {
                visit(root, &path, entries)?;
            } else if entry.file_type()?.is_file() {
                entries.insert(
                    path.strip_prefix(root).unwrap().to_path_buf(),
                    fs::read(path)?,
                );
            }
        }
        Ok(())
    }

    let mut entries = BTreeMap::new();
    if root.exists() {
        visit(root, root, &mut entries)?;
    }
    Ok(entries)
}

fn provider_profile() -> ProviderProfileUpsert {
    ProviderProfileUpsert {
        id: "legacy-minimax".to_owned(),
        display_name: "Legacy MiniMax".to_owned(),
        provider_type: ProviderType::MiniMax,
        default_model: "MiniMax-M2.5".to_owned(),
        allowed_models: vec!["MiniMax-M2.5".to_owned()],
        capabilities: vec!["text".to_owned()],
        auth: ProviderAuth::ApiKeySecret {
            secret_ref: "legacy-minimax-key".to_owned(),
        },
        config: json!({}),
    }
}

fn write_profile(fixture: &Fixture) -> PathBuf {
    let path = fixture.path("provider profile.json");
    fs::write(
        &path,
        serde_json::to_vec(&provider_profile()).expect("fixture profile should encode"),
    )
    .expect("fixture profile should be written");
    path
}

fn write_workflow(fixture: &Fixture) -> PathBuf {
    let path = fixture.path("legacy flow with spaces.kakune.yaml");
    fs::write(&path, WORKFLOW_SOURCE).expect("fixture workflow should be written");
    path
}

fn write_mcp_call(fixture: &Fixture) -> PathBuf {
    let path = fixture.path("mcp call.json");
    fs::write(
        &path,
        serde_json::to_vec(&json!({
            "transport": {
                "transport": "http",
                "endpoint": "http://127.0.0.1:1/mcp"
            },
            "toolName": "fixture-tool",
            "arguments": {}
        }))
        .expect("MCP fixture should encode"),
    )
    .expect("MCP fixture should be written");
    path
}

fn mcp_helper_command() -> String {
    std::env::var("CARGO_BIN_EXE_mcp-helper").unwrap_or_else(|_| {
        std::env::current_exe()
            .expect("test executable path")
            .parent()
            .and_then(|path| path.parent())
            .expect("test executable should live under target/debug/deps")
            .join(format!("mcp-helper{}", std::env::consts::EXE_SUFFIX))
            .to_string_lossy()
            .into_owned()
    })
}

fn write_successful_mcp_call(fixture: &Fixture) -> PathBuf {
    let path = fixture.path("successful mcp call.json");
    fs::write(
        &path,
        serde_json::to_vec(&json!({
            "transport": {
                "transport": "stdio",
                "command": mcp_helper_command(),
                "args": [],
                "current_dir": fixture.root,
                "environment_secret_refs": {}
            },
            "toolName": "greet",
            "arguments": {"name": "Ada"}
        }))
        .expect("successful MCP call should encode"),
    )
    .expect("successful MCP call fixture should be written");
    path
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_local_extras_keep_golden_argv_output_exit_and_side_effects() {
    let fixture = Fixture::new().expect("isolated fixture should be created");
    let data_dir = fixture.path("local core data");
    let store = Store::open(data_dir.clone()).expect("isolated Store should open");
    let initial_data = snapshot_tree(&data_dir).expect("initial Store files should be snapshotted");
    let workflow = write_workflow(&fixture);
    let workflow_string = workflow.to_string_lossy().into_owned();
    let data_string = data_dir.to_string_lossy().into_owned();

    for version_args in [args(&["version"]), args(&["--version"])] {
        assert_success(
            &run_cli(&fixture, &version_args, &[]),
            &format!("kakune {}\n", env!("CARGO_PKG_VERSION")),
        );
    }
    assert!(!data_dir.join("missing-version-data").exists());
    assert_eq!(
        snapshot_tree(&data_dir).expect("version should leave the Store unchanged"),
        initial_data,
        "both version forms are observational"
    );

    // Top-level `run` is a retained local-only operation only when standalone is explicit.
    let run_args = args(&[
        "--standalone",
        "run",
        &workflow_string,
        "--data-dir",
        &data_string,
    ]);
    let run = run_cli(&fixture, &run_args, &[]);
    assert!(
        run.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&run.stderr)
    );
    assert!(run.stderr.is_empty());
    let run_line = String::from_utf8(run.stdout.clone()).expect("run output should be UTF-8");
    let fields = run_line.trim_end().split('\t').collect::<Vec<_>>();
    assert_eq!(
        fields.len(),
        3,
        "legacy run keeps its three-column TSV: {run_line:?}"
    );
    assert!(!fields[0].is_empty());
    assert_eq!(fields[1], "legacy-flow");
    assert_eq!(fields[2], "succeeded");

    let execution_id = fields[0];
    let execution = store
        .get_execution(execution_id)
        .expect("execution lookup should work")
        .expect("run should persist one execution");
    assert_eq!(execution.workflow_name, "legacy-flow");
    assert_eq!(execution.status, "succeeded");
    assert_eq!(
        store
            .list_workflows()
            .expect("workflow list should work")
            .into_iter()
            .find(|workflow| workflow.id == "legacy-flow")
            .expect("legacy run upserts its workflow")
            .status,
        "enabled",
        "the legacy `run` side effect includes an enabled registration"
    );

    let executions = run_cli(
        &fixture,
        &args(&["executions", "--data-dir", &data_string]),
        &[],
    );
    assert_success(
        &executions,
        &format!(
            "{}\t{}\t{}\t{}\n",
            execution.id, execution.workflow_name, execution.status, execution.created_at
        ),
    );

    let inspect = run_cli(
        &fixture,
        &args(&["inspect", execution_id, "--data-dir", &data_string]),
        &[],
    );
    let mut inspect_golden = format!(
        "{}\t{}\t{}\n",
        execution.id, execution.workflow_name, execution.status
    );
    for node in store
        .list_node_runs(execution_id)
        .expect("node runs should load")
    {
        inspect_golden.push_str(&format!(
            "{}\t{}\t{}\n",
            node.node_id,
            node.status,
            node.message.unwrap_or_default()
        ));
    }
    assert_success(&inspect, &inspect_golden);

    let profile_file = write_profile(&fixture);
    let profile_path = profile_file.to_string_lossy().into_owned();
    let upsert = run_cli(
        &fixture,
        &args(&[
            "provider",
            "upsert",
            &profile_path,
            "--data-dir",
            &data_string,
        ]),
        &[],
    );
    assert_success(
        &upsert,
        "legacy-minimax\tLegacy MiniMax\tminimax\tapiKey\tunknown\n",
    );
    assert!(
        store
            .get_provider_profile("legacy-minimax")
            .expect("profile lookup should work")
            .is_some()
    );

    let mut updated_profile: Value = serde_json::from_slice(
        &fs::read(&profile_file).expect("profile fixture should remain readable"),
    )
    .expect("profile fixture should decode");
    updated_profile["displayName"] = json!("Updated Legacy MiniMax");
    fs::write(
        &profile_file,
        serde_json::to_vec(&updated_profile).expect("updated profile should encode"),
    )
    .expect("updated profile fixture should be written");
    let update = run_cli(
        &fixture,
        &args(&[
            "provider",
            "upsert",
            &profile_path,
            "--data-dir",
            &data_string,
        ]),
        &[],
    );
    assert_success(
        &update,
        "legacy-minimax\tUpdated Legacy MiniMax\tminimax\tapiKey\tunknown\n",
    );
    assert_eq!(
        store
            .get_provider_profile("legacy-minimax")
            .expect("updated profile should remain readable")
            .expect("upsert should retain the profile")
            .display_name,
        "Updated Legacy MiniMax",
        "legacy upsert continues to replace existing profiles"
    );

    let status = run_cli(
        &fixture,
        &args(&[
            "provider",
            "status",
            "legacy-minimax",
            "--data-dir",
            &data_string,
        ]),
        &[],
    );
    assert_success(&status, "legacy-minimax\tunknown\tnever\n");

    let login = run_cli(
        &fixture,
        &args(&[
            "provider",
            "login",
            "legacy-minimax",
            "--data-dir",
            &data_string,
        ]),
        &[],
    );
    assert_failure(
        &login,
        "kakune: provider login requires a Codex profile using oauthSecret\n",
    );
    assert!(
        store
            .get_provider_profile("legacy-minimax")
            .expect("profile should remain readable")
            .is_some()
    );

    let remove = run_cli(
        &fixture,
        &args(&[
            "provider",
            "remove",
            "legacy-minimax",
            "--data-dir",
            &data_string,
        ]),
        &[],
    );
    assert_success(&remove, "Removed legacy-minimax\n");
    assert!(
        store
            .get_provider_profile("legacy-minimax")
            .expect("removed profile lookup should work")
            .is_none()
    );

    // The old one-shot MCP call reports a generic failure and does not run a tool
    // when its explicitly configured local endpoint is unavailable.
    let mcp_file = write_mcp_call(&fixture);
    let mcp_path = mcp_file.to_string_lossy().into_owned();
    let mcp = run_cli(
        &fixture,
        &args(&["mcp", "call", &mcp_path, "--data-dir", &data_string]),
        &[],
    );
    assert_failure(&mcp, "kakune: MCP call could not be completed\n");
    assert!(data_dir.join("kakune.sqlite3").is_file());

    let successful_mcp_file = write_successful_mcp_call(&fixture);
    let successful_mcp_path = successful_mcp_file.to_string_lossy().into_owned();
    let workflow_count_before_mcp = store.list_workflows().unwrap().len();
    let executions_before_mcp = store.list_executions().unwrap().len();
    let successful_mcp = run_cli(
        &fixture,
        &args(&[
            "mcp",
            "call",
            &successful_mcp_path,
            "--data-dir",
            &data_string,
        ]),
        &[],
    );
    assert!(
        successful_mcp.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&successful_mcp.stderr)
    );
    assert!(successful_mcp.stderr.is_empty());
    let successful_mcp_json: Value = serde_json::from_slice(&successful_mcp.stdout)
        .expect("legacy MCP call keeps its JSON response");
    assert_eq!(successful_mcp_json["transport"]["type"], "stdio");
    assert_eq!(
        successful_mcp_json["transport"]["server"]["name"],
        "test-mcp-server"
    );
    assert_eq!(
        successful_mcp_json["result"]["content"][0]["text"],
        "Hello, Ada!"
    );
    assert!(!successful_mcp_json["result"]["isError"].as_bool().unwrap());
    assert_eq!(
        store.list_workflows().unwrap().len(),
        workflow_count_before_mcp
    );
    assert_eq!(
        store.list_executions().unwrap().len(),
        executions_before_mcp
    );

    let token = store
        .create_auth_token(
            "legacy fixture token".to_owned(),
            vec![AuthScope::Admin],
            None,
        )
        .expect("test token should be created without a credential backend");
    let tokens = run_cli(
        &fixture,
        &args(&["auth", "tokens", "--data-dir", &data_string]),
        &[],
    );
    assert_success(
        &tokens,
        &format!(
            "{}\tlegacy fixture token\tadmin\tactive\tnever\n",
            token.record.id
        ),
    );
    let revoke = run_cli(
        &fixture,
        &args(&[
            "auth",
            "revoke",
            &token.record.id,
            "--data-dir",
            &data_string,
        ]),
        &[],
    );
    assert_success(&revoke, &format!("Revoked token {}.\n", token.record.id));
    assert!(
        store
            .list_auth_tokens()
            .expect("token history should remain available")
            .iter()
            .find(|record| record.id == token.record.id)
            .is_some_and(|record| record.revoked_at.is_some())
    );

    // Recovery and pairing are retained but this baseline must not touch the host
    // keyring or wait for an interactive approval. These fail before those effects.
    let invalid_config = fixture.path("invalid existing config.yaml");
    fs::write(&invalid_config, b"not: [valid yaml").expect("invalid config should be written");
    let invalid_config_path = invalid_config.to_string_lossy().into_owned();
    let recovery_data = fixture.path("recovery must remain absent");
    let recovery_data_string = recovery_data.to_string_lossy().into_owned();
    let recover = run_cli(
        &fixture,
        &args(&[
            "auth",
            "recover",
            "--data-dir",
            &recovery_data_string,
            "--config",
            &invalid_config_path,
        ]),
        &[],
    );
    assert!(!recover.status.success());
    assert!(recover.stdout.is_empty());
    assert!(String::from_utf8_lossy(&recover.stderr).starts_with("kakune: "));
    assert!(!recovery_data.exists());
    assert_eq!(fs::read(&invalid_config).unwrap(), b"not: [valid yaml");

    let pair_data = fixture.path("pairing must remain absent");
    let pair_data_string = pair_data.to_string_lossy().into_owned();
    let pair = run_cli(
        &fixture,
        &args(&[
            "auth",
            "pair",
            "--data-dir",
            &pair_data_string,
            "--ttl-seconds",
            "1",
        ]),
        &[],
    );
    assert_failure(
        &pair,
        "kakune: pairing invitation lifetime must be 30-600 seconds\n",
    );
    assert!(!pair_data.exists());

    let backup = fixture.path("legacy backup.sqlite3");
    let backup_path = backup.to_string_lossy().into_owned();
    let backup_output = run_cli(
        &fixture,
        &args(&[
            "storage",
            "backup",
            &backup_path,
            "--data-dir",
            &data_string,
        ]),
        &[],
    );
    assert!(backup_output.status.success());
    assert!(backup_output.stderr.is_empty());
    assert!(backup.is_file());
    let backup_json: Value = serde_json::from_slice(&backup_output.stdout)
        .expect("storage backup keeps its JSON summary");
    assert_eq!(backup_json["path"], backup_path);
    assert_eq!(backup_json["workflows"], 1);
    assert_eq!(backup_json["executions"], 1);

    let restored = fixture.path("restored core");
    let restored_path = restored.to_string_lossy().into_owned();
    let restore = run_cli(
        &fixture,
        &args(&["storage", "restore", &backup_path, &restored_path]),
        &[],
    );
    assert!(
        restore.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&restore.stderr)
    );
    assert!(restore.stderr.is_empty());
    let restore_json: Value =
        serde_json::from_slice(&restore.stdout).expect("storage restore keeps its JSON summary");
    assert_eq!(restore_json["path"], restored_path);
    let restored_store = Store::open(restored.clone()).expect("restored Store should open");
    assert_eq!(restored_store.list_workflows().unwrap().len(), 1);
    assert_eq!(restored_store.list_executions().unwrap().len(), 1);
    drop(restored_store);

    let retain = run_cli(
        &fixture,
        &args(&[
            "storage",
            "retain",
            "--data-dir",
            &data_string,
            "--execution-days",
            "3650",
            "--event-days",
            "3650",
        ]),
        &[],
    );
    assert!(retain.status.success());
    assert!(retain.stderr.is_empty());
    let retain_json: Value =
        serde_json::from_slice(&retain.stdout).expect("storage retain keeps its JSON report");
    assert_eq!(
        retain_json,
        json!({
            "executionsDeleted": 0,
            "eventsDeleted": 0,
            "artifactsDeleted": 0,
            "artifactBytesDeleted": 0
        })
    );

    let pin = run_cli(
        &fixture,
        &args(&[
            "storage",
            "pin",
            execution_id,
            "--value",
            "--data-dir",
            &data_string,
        ]),
        &[],
    );
    assert_success(&pin, &format!("execution {execution_id} pinned=true\n"));
    let pinned: i64 = rusqlite::Connection::open(data_dir.join("kakune.sqlite3"))
        .expect("isolated database should open for side-effect verification")
        .query_row(
            "SELECT pinned FROM executions WHERE id = ?1",
            [execution_id],
            |row| row.get(0),
        )
        .expect("pinned execution should remain in the database");
    assert_eq!(pinned, 1, "the legacy storage pin mutates the execution");

    let compact = run_cli(
        &fixture,
        &args(&["storage", "compact", "--data-dir", &data_string]),
        &[],
    );
    assert_success(&compact, "SQLite compacted\n");
    assert_eq!(store.list_executions().unwrap().len(), 1);
}

struct CoreServer {
    store: Store,
    token: String,
    endpoint: String,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl CoreServer {
    async fn start(data_dir: PathBuf) -> Self {
        let store = Store::open(data_dir).expect("isolated Core Store should open");
        let token = store
            .ensure_bootstrap_token()
            .expect("test Core token should be available")
            .expect("isolated Core should issue one test token");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("loopback Core listener should bind");
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let app = api::router(store.clone());
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
                .expect("isolated Core server should exit cleanly");
        });
        Self {
            store,
            token,
            endpoint,
            stop: Some(stop),
            task: Some(task),
        }
    }

    async fn shutdown(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.await.expect("Core server task should join");
        }
    }
}

fn write_context_file(fixture: &Fixture, remote: &CoreServer) -> PathBuf {
    let path = fixture.path("selected contexts.json");
    fs::write(
        &path,
        serde_json::to_vec(&json!({
            "format": "kakune-contexts/v1",
            "exportedAt": "2026-10-26T00:00:00Z",
            "activeContextId": "remote",
            "contexts": [{
                "id": "remote",
                "name": "Remote fixture",
                "endpoint": remote.endpoint,
                "expectedCoreId": remote.store.core_id().unwrap(),
                "credentialRef": "env:KAKUNE_TOKEN"
            }]
        }))
        .expect("context file should encode"),
    )
    .expect("context file should be written");
    path
}

fn selected_args(context_file: &Path, command: &[String]) -> Vec<String> {
    let mut selected = args(&["--context", "remote", "--context-file"]);
    selected.push(context_file.to_string_lossy().into_owned());
    selected.extend_from_slice(command);
    selected
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_context_legacy_contracts_target_only_the_selected_core() {
    let fixture = Fixture::new().expect("isolated fixture should be created");
    let mut local = CoreServer::start(fixture.path("local Core")).await;
    let mut remote = CoreServer::start(fixture.path("remote Core")).await;
    let context_file = write_context_file(&fixture, &remote);
    let local_data = fixture.path("local Core");
    let local_data_string = local_data.to_string_lossy().into_owned();
    let context_snapshot = fs::read(&context_file).expect("context should be snapshotted");
    let workflow = write_workflow(&fixture);
    let workflow_string = workflow.to_string_lossy().into_owned();
    let environment = [("KAKUNE_TOKEN", remote.token.as_str())];

    // Explicit Core targeting must preserve the old remote response shapes and
    // must not mutate the local Store, even when a --data-dir is also supplied.
    let remote_run = run_cli(
        &fixture,
        &selected_args(
            &context_file,
            &args(&["run", &workflow_string, "--data-dir", &local_data_string]),
        ),
        &environment,
    );
    assert!(
        remote_run.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&remote_run.stderr)
    );
    assert!(remote_run.stderr.is_empty());
    let run_json: Value = serde_json::from_slice(&remote_run.stdout)
        .expect("legacy remote run returns an execution JSON object");
    let remote_execution_id = run_json["id"].as_str().expect("remote run returns an ID");
    assert_eq!(run_json["workflowName"], "legacy-flow");
    assert!(
        local
            .store
            .get_workflow_source("legacy-flow")
            .unwrap()
            .is_none()
    );
    assert!(
        remote
            .store
            .get_workflow_source("legacy-flow")
            .unwrap()
            .is_some()
    );

    let remote_list = run_cli(
        &fixture,
        &selected_args(
            &context_file,
            &args(&["executions", "--data-dir", &local_data_string]),
        ),
        &environment,
    );
    assert!(
        remote_list.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&remote_list.stderr)
    );
    let remote_list_json: Value = serde_json::from_slice(&remote_list.stdout)
        .expect("explicit-context execution list keeps its JSON response");
    assert_eq!(remote_list_json["items"][0]["id"], remote_execution_id);
    assert_eq!(local.store.list_executions().unwrap().len(), 0);

    let remote_inspect = run_cli(
        &fixture,
        &selected_args(
            &context_file,
            &args(&[
                "inspect",
                remote_execution_id,
                "--data-dir",
                &local_data_string,
            ]),
        ),
        &environment,
    );
    assert!(
        remote_inspect.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&remote_inspect.stderr)
    );
    let inspect_json: Value = serde_json::from_slice(&remote_inspect.stdout)
        .expect("explicit-context inspect keeps its JSON response");
    assert_eq!(inspect_json["execution"]["id"], remote_execution_id);

    let profile_file = write_profile(&fixture);
    let profile_path = profile_file.to_string_lossy().into_owned();
    let remote_upsert = run_cli(
        &fixture,
        &selected_args(
            &context_file,
            &args(&[
                "provider",
                "upsert",
                &profile_path,
                "--data-dir",
                &local_data_string,
            ]),
        ),
        &environment,
    );
    assert!(
        remote_upsert.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&remote_upsert.stderr)
    );
    let upsert_json: Value = serde_json::from_slice(&remote_upsert.stdout)
        .expect("remote provider upsert keeps its JSON response");
    assert_eq!(upsert_json["id"], "legacy-minimax");
    assert!(
        remote
            .store
            .get_provider_profile("legacy-minimax")
            .unwrap()
            .is_some()
    );
    assert!(
        local
            .store
            .get_provider_profile("legacy-minimax")
            .unwrap()
            .is_none()
    );

    let mut updated_profile: Value = serde_json::from_slice(
        &fs::read(&profile_file).expect("remote profile fixture should remain readable"),
    )
    .expect("remote profile fixture should decode");
    updated_profile["displayName"] = json!("Remote Updated MiniMax");
    fs::write(
        &profile_file,
        serde_json::to_vec(&updated_profile).expect("remote update should encode"),
    )
    .expect("remote update fixture should be written");
    let remote_update = run_cli(
        &fixture,
        &selected_args(
            &context_file,
            &args(&[
                "provider",
                "upsert",
                &profile_path,
                "--data-dir",
                &local_data_string,
            ]),
        ),
        &environment,
    );
    assert!(
        remote_update.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&remote_update.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&remote_update.stdout).unwrap()["displayName"],
        "Remote Updated MiniMax"
    );
    assert_eq!(
        remote
            .store
            .get_provider_profile("legacy-minimax")
            .unwrap()
            .unwrap()
            .display_name,
        "Remote Updated MiniMax"
    );
    assert!(
        local
            .store
            .get_provider_profile("legacy-minimax")
            .unwrap()
            .is_none(),
        "explicit-context upsert must not update the local Core"
    );

    let remote_status = run_cli(
        &fixture,
        &selected_args(
            &context_file,
            &args(&[
                "provider",
                "status",
                "legacy-minimax",
                "--data-dir",
                &local_data_string,
            ]),
        ),
        &environment,
    );
    assert!(remote_status.status.success());
    let status_json: Value = serde_json::from_slice(&remote_status.stdout)
        .expect("remote provider status keeps its JSON response");
    assert_eq!(status_json["id"], "legacy-minimax");
    assert_eq!(status_json["diagnostic"]["status"], "unknown");

    let remote_login = run_cli(
        &fixture,
        &selected_args(
            &context_file,
            &args(&[
                "provider",
                "login",
                "legacy-minimax",
                "--data-dir",
                &local_data_string,
            ]),
        ),
        &environment,
    );
    assert_failure(
        &remote_login,
        "kakune: provider login requires a Codex profile using oauthSecret\n",
    );

    let remote_remove = run_cli(
        &fixture,
        &selected_args(
            &context_file,
            &args(&[
                "provider",
                "remove",
                "legacy-minimax",
                "--data-dir",
                &local_data_string,
            ]),
        ),
        &environment,
    );
    assert_success(&remote_remove, "null\n");
    assert!(
        remote
            .store
            .get_provider_profile("legacy-minimax")
            .unwrap()
            .is_none()
    );
    assert!(
        local
            .store
            .get_provider_profile("legacy-minimax")
            .unwrap()
            .is_none()
    );

    let mcp_file = write_mcp_call(&fixture);
    let mcp_path = mcp_file.to_string_lossy().into_owned();
    let remote_mcp = run_cli(
        &fixture,
        &selected_args(
            &context_file,
            &args(&["mcp", "call", &mcp_path, "--data-dir", &local_data_string]),
        ),
        &environment,
    );
    assert_failure(
        &remote_mcp,
        "kakune: Core returned 502 Bad Gateway: MCP call could not be completed\n",
    );
    assert!(local.store.list_executions().unwrap().is_empty());

    let successful_mcp_file = write_successful_mcp_call(&fixture);
    let successful_mcp_path = successful_mcp_file.to_string_lossy().into_owned();
    let remote_workflows_before_mcp = remote.store.list_workflows().unwrap().len();
    let remote_executions_before_mcp = remote.store.list_executions().unwrap().len();
    let remote_mcp_success = run_cli(
        &fixture,
        &selected_args(
            &context_file,
            &args(&[
                "mcp",
                "call",
                &successful_mcp_path,
                "--data-dir",
                &local_data_string,
            ]),
        ),
        &environment,
    );
    assert!(
        remote_mcp_success.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&remote_mcp_success.stderr)
    );
    let remote_mcp_json: Value = serde_json::from_slice(&remote_mcp_success.stdout)
        .expect("remote legacy MCP call keeps its JSON response");
    assert_eq!(remote_mcp_json["transport"]["type"], "stdio");
    assert_eq!(
        remote_mcp_json["result"]["content"][0]["text"],
        "Hello, Ada!"
    );
    assert_eq!(
        remote.store.list_workflows().unwrap().len(),
        remote_workflows_before_mcp
    );
    assert_eq!(
        remote.store.list_executions().unwrap().len(),
        remote_executions_before_mcp
    );
    assert!(local.store.list_workflows().unwrap().is_empty());
    assert!(local.store.list_executions().unwrap().is_empty());

    // Storage commands remain local-only. Explicit remote selection reaches the
    // Core envelope but the old adapter rejects the operation without effects.
    let storage_commands = [
        args(&[
            "storage",
            "backup",
            &fixture.text("remote backup.sqlite3"),
            "--data-dir",
            &local_data_string,
        ]),
        args(&[
            "storage",
            "restore",
            &fixture.text("not-a-backup.sqlite3"),
            &fixture.text("not-restored"),
        ]),
        args(&["storage", "retain", "--data-dir", &local_data_string]),
        args(&["storage", "compact", "--data-dir", &local_data_string]),
        args(&[
            "storage",
            "pin",
            "unknown-execution",
            "--value",
            "--data-dir",
            &local_data_string,
        ]),
    ];
    for command in storage_commands {
        let command_description = command.join(" ");
        let output = run_cli(
            &fixture,
            &selected_args(&context_file, &command),
            &environment,
        );
        assert!(
            !output.status.success()
                && output.stdout.is_empty()
                && output.stderr
                    == b"kakune: this command operates on the local installation; omit --context\n",
            "unexpected explicit-context result for `{command_description}`: stdout={:?}, stderr={:?}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert!(!fixture.path("remote backup.sqlite3").exists());
    assert_eq!(fs::read(&context_file).unwrap(), context_snapshot);

    // `auth` intentionally bypasses remote dispatch; its explicit-context form
    // continues to read the requested local data directory and token metadata.
    let local_token = local
        .store
        .create_auth_token(
            "local-only auth fixture".to_owned(),
            vec![AuthScope::Admin],
            None,
        )
        .expect("local token metadata should be created");
    let auth_tokens = run_cli(
        &fixture,
        &selected_args(
            &context_file,
            &args(&["auth", "tokens", "--data-dir", &local_data_string]),
        ),
        &environment,
    );
    let bootstrap_token_id = local
        .store
        .list_auth_tokens()
        .expect("local token metadata should remain available")
        .into_iter()
        .find(|record| record.name == "Bootstrap token")
        .expect("local Core bootstrap token should remain listed")
        .id;
    assert_success(
        &auth_tokens,
        &format!(
            "{}\tlocal-only auth fixture\tadmin\tactive\tnever\n{}\tBootstrap token\tadmin\tactive\tnever\n",
            local_token.record.id, bootstrap_token_id
        ),
    );
    assert_eq!(fs::read(&context_file).unwrap(), context_snapshot);

    // The existing Plugin list remains available both locally and remotely;
    // detailed process-contract preservation stays covered by plugin_process.
    let local_plugins = run_cli(
        &fixture,
        &args(&["plugin", "list", "--data-dir", &local_data_string]),
        &[],
    );
    assert_success(&local_plugins, "");
    let remote_plugins = run_cli(
        &fixture,
        &selected_args(
            &context_file,
            &args(&["plugin", "list", "--data-dir", &local_data_string]),
        ),
        &environment,
    );
    assert_success(&remote_plugins, "{\n  \"items\": []\n}\n");

    local.shutdown().await;
    remote.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn general_lifecycle_and_plugin_entry_points_remain_discoverable() {
    let fixture = Fixture::new().expect("isolated fixture should be created");
    let data_dir = fixture.path("idle daemon data");
    let data_string = data_dir.to_string_lossy().into_owned();
    for command in ["init", "doctor", "daemon", "service", "plugin"] {
        let help = run_cli(&fixture, &args(&[command, "--help"]), &[]);
        assert!(
            help.status.success(),
            "{command} help stderr: {}",
            String::from_utf8_lossy(&help.stderr)
        );
        assert!(help.stderr.is_empty());
        assert!(!help.stdout.is_empty());
    }
    for (command, required) in [
        (
            args(&["daemon", "--help"]),
            &["start", "stop", "status"][..],
        ),
        (
            args(&["service", "--help"]),
            &["install", "uninstall", "start", "stop", "status"][..],
        ),
        (
            args(&["plugin", "--help"]),
            &["prepare", "commit", "list", "remove"][..],
        ),
    ] {
        let help = run_cli(&fixture, &command, &[]);
        let help = String::from_utf8(help.stdout).expect("help should be UTF-8");
        for subcommand in required {
            assert!(
                help.lines()
                    .any(|line| line.split_whitespace().next() == Some(subcommand)),
                "help should retain {subcommand}:\n{help}"
            );
        }
    }

    let daemon_status = run_cli(
        &fixture,
        &args(&["daemon", "--data-dir", &data_string, "status"]),
        &[],
    );
    assert!(daemon_status.status.success());
    assert!(String::from_utf8_lossy(&daemon_status.stdout).contains("state=stopped"));
    assert!(
        !data_dir.exists(),
        "daemon status must remain observational"
    );
}
