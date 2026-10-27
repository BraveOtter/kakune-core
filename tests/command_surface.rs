mod support;

use std::{
    collections::{BTreeSet, HashMap},
    fs,
    io::{self, Read, Write},
    path::PathBuf,
    process::{Command, ExitStatus, Stdio},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use kakune_core::{
    CoreConfig, McpConnectionTransport, NativeCredentialBackend, NewNamedMcpConnection,
    ProviderAuth, ProviderProfileUpsert, ProviderType, SecretBackendKind, Store,
    config::ApiConfig,
    observation::{DoctorArea, DoctorFinding, DoctorReport, DoctorStatus},
};
use rusqlite::Connection;
use serde_json::Value;
use support::management::{InProcessCore, ManagementFixture};

struct Output {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

fn run(fixture: &ManagementFixture, args: &[String], env: &[(&str, &str)]) -> io::Result<Output> {
    run_with_stdin(fixture, args, env, None)
}

fn run_with_stdin(
    fixture: &ManagementFixture,
    args: &[String],
    env: &[(&str, &str)],
    input: Option<&[u8]>,
) -> io::Result<Output> {
    let id = uuid::Uuid::new_v4();
    let stdout_path = fixture.path(format!("child-{id}.stdout"));
    let stderr_path = fixture.path(format!("child-{id}.stderr"));
    let stdout = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&stdout_path)?;
    let stderr = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&stderr_path)?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_kakune"));
    command
        .args(args)
        .env_remove("KAKUNE_TOKEN")
        .envs(env.iter().copied())
        .current_dir(fixture.root())
        .stdin(if input.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    let mut child = command.spawn()?;
    if let Some(input) = input {
        child
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("child stdin was not piped"))?
            .write_all(input)?;
    }
    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if started.elapsed() >= Duration::from_secs(15) {
            let _ = child.kill();
            let _ = child.wait();
            let captured_stdout = fs::read_to_string(&stdout_path).unwrap_or_default();
            let captured_stderr = fs::read_to_string(&stderr_path).unwrap_or_default();
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "kakune command exceeded the 15-second fixture limit; stdout={captured_stdout:?}; stderr={captured_stderr:?}"
                ),
            ));
        }
        thread::sleep(Duration::from_millis(10));
    };
    let mut stdout = Vec::new();
    fs::File::open(stdout_path)?.read_to_end(&mut stdout)?;
    let mut stderr = Vec::new();
    fs::File::open(stderr_path)?.read_to_end(&mut stderr)?;
    Ok(Output {
        status,
        stdout,
        stderr,
    })
}

fn write_selected_context(path: &std::path::Path, core: &InProcessCore) {
    let core_id = core
        .store
        .core_id()
        .expect("Core identity should be readable");
    fs::write(
        path,
        serde_json::to_vec(&serde_json::json!({
            "format":"kakune-contexts/v1",
            "exportedAt":"2026-10-26T00:00:00Z",
            "activeContextId":"selected",
            "revision":0,
            "contexts":[{
                "id":"selected",
                "name":"Selected Core",
                "endpoint":core.endpoint,
                "expectedCoreId":core_id,
                "color":null,
                "credentialRef":null
            }]
        }))
        .expect("context fixture should serialize"),
    )
    .expect("selected Core context should be written");
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn path_arg(path: PathBuf) -> String {
    path.to_string_lossy().into_owned()
}

fn help_commands(fixture: &ManagementFixture, command: &[&str]) -> (String, BTreeSet<String>) {
    let mut arguments = command.to_vec();
    arguments.push("--help");
    let output = run(fixture, &strings(&arguments), &[]).expect("help should complete");
    assert_eq!(
        output.status.code(),
        Some(0),
        "help for {command:?} should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    let help = String::from_utf8(output.stdout).expect("help should be UTF-8");
    let command_listing = help
        .split_once("Commands:")
        .map(|(_, listing)| listing.split("Options:").next().unwrap_or(listing))
        .unwrap_or("");
    let commands = command_listing
        .lines()
        .filter_map(|line| line.split_whitespace().next())
        .filter(|name| *name != "help")
        .map(str::to_owned)
        .collect();
    (help, commands)
}

fn assert_help_inventory(
    fixture: &ManagementFixture,
    parent: &[&str],
    expected: &[&str],
) -> String {
    let (help, actual) = help_commands(fixture, parent);
    let expected = expected
        .iter()
        .map(|name| (*name).to_owned())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        actual, expected,
        "public help inventory for {parent:?} changed"
    );
    help
}

#[test]
fn cli_help_inventory_matches_requirements_and_compatibility_review() {
    let fixture = ManagementFixture::new().expect("isolated fixture should be created");
    let root = assert_help_inventory(
        &fixture,
        &[],
        &[
            "init",
            "version",
            "daemon",
            "storage",
            "service",
            "doctor",
            "context",
            "auth",
            "workflow",
            "execution",
            "trigger",
            "plugin",
            "provider",
            "secret",
            "mcp",
            "run",
            "executions",
            "inspect",
        ],
    );
    assert!(
        !root.to_ascii_lowercase().contains("deprecated"),
        "help must not newly deprecate a command"
    );

    for (parent, expected) in [
        (&["daemon"][..], &["start", "stop", "status"][..]),
        (
            &["service"][..],
            &["install", "uninstall", "start", "stop", "status"][..],
        ),
        (
            &["context"][..],
            &[
                "add", "list", "use", "inspect", "remove", "import", "export",
            ][..],
        ),
        (
            &["workflow"][..],
            &[
                "create", "import", "export", "validate", "plan", "list", "run", "enable",
                "disable",
            ][..],
        ),
        (
            &["execution"][..],
            &["list", "inspect", "cancel", "logs"][..],
        ),
        (&["trigger"][..], &["list", "inspect"][..]),
        (
            &["provider"][..],
            &[
                "add", "list", "auth", "test", "models", "upsert", "status", "remove", "login",
            ][..],
        ),
        (
            &["mcp"][..],
            &["add", "list", "inspect", "auth", "tool", "call"][..],
        ),
        (&["mcp", "tool"][..], &["list", "call"][..]),
        (&["secret"][..], &["set", "list", "remove"][..]),
        (&["auth"][..], &["recover", "pair", "tokens", "revoke"][..]),
        (
            &["storage"][..],
            &["backup", "restore", "retain", "compact", "pin"][..],
        ),
        (
            &["plugin"][..],
            &["prepare", "commit", "list", "remove"][..],
        ),
    ] {
        let help = assert_help_inventory(&fixture, parent, expected);
        assert!(
            !help.to_ascii_lowercase().contains("deprecated"),
            "help for {parent:?} must not newly deprecate a command"
        );
    }

    for (command, required_options) in [
        (&["context", "export"][..], &["--output", "--replace"][..]),
        (&["workflow", "validate"][..], &["--file", "--json"][..]),
        (&["workflow", "plan"][..], &["--file", "--json"][..]),
        (
            &["workflow", "run"][..],
            &["--file", "--standalone", "--request-key", "--json"][..],
        ),
        (
            &["workflow", "import"][..],
            &["--replace", "--if-revision", "--json"][..],
        ),
        (
            &["workflow", "list"][..],
            &["--limit", "--cursor", "--json"][..],
        ),
        (
            &["execution", "cancel"][..],
            &["--wait-seconds", "--json"][..],
        ),
        (
            &["execution", "logs"][..],
            &["--limit", "--cursor", "--json"][..],
        ),
        (
            &["provider", "auth"][..],
            &[
                "--stdin", /* only MiniMax consumes a secure value */
                "--json",
            ][..],
        ),
        (&["provider", "add"][..], &["--file", "--json"][..]),
        (
            &["provider", "list"][..],
            &["--limit", "--cursor", "--json"][..],
        ),
        (&["provider", "test"][..], &["--json"][..]),
        (&["provider", "models"][..], &["--json"][..]),
        (&["mcp", "add"][..], &["--file", "--json"][..]),
        (&["mcp", "list"][..], &["--limit", "--cursor", "--json"][..]),
        (&["mcp", "inspect"][..], &["--json"][..]),
        (&["mcp", "auth"][..], &["--secret", "--json"][..]),
        (
            &["mcp", "tool", "list"][..],
            &["--limit", "--cursor", "--json"][..],
        ),
        (&["mcp", "tool", "call"][..], &["--input", "--json"][..]),
        (&["secret", "set"][..], &["--stdin", "--json"][..]),
        (
            &["secret", "list"][..],
            &["--limit", "--cursor", "--json"][..],
        ),
        (&["secret", "remove"][..], &["--json"][..]),
    ] {
        let (help, _) = help_commands(&fixture, command);
        for option in required_options {
            assert!(
                help.contains(option),
                "{command:?} help must document {option}"
            );
        }
    }

    for arguments in [&["version"][..], &["--version"][..]] {
        let output = run(&fixture, &strings(arguments), &[]).expect("version should complete");
        assert_eq!(output.status.code(), Some(0));
        assert!(output.stderr.is_empty());
        assert_eq!(
            output.stdout,
            format!("kakune {}\n", env!("CARGO_PKG_VERSION")).as_bytes(),
            "both documented version forms must remain equivalent"
        );
    }

    let review = include_str!("../specs/003-complete-command-surface/command-review.md");
    let expected_extras = [
        "run",
        "executions",
        "inspect",
        "provider upsert",
        "provider login",
        "provider status",
        "provider remove",
        "mcp call",
        "auth recover",
        "auth pair",
        "auth tokens",
        "auth revoke",
        "storage backup",
        "storage restore",
        "storage retain",
        "storage compact",
        "storage pin",
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    let mut in_extra_ledger = false;
    let mut ledger_extras = BTreeSet::new();
    for line in review.lines() {
        if line.starts_with("| Extra / disposition |") {
            in_extra_ledger = true;
            continue;
        }
        if in_extra_ledger && line.starts_with("| ---") {
            continue;
        }
        if !in_extra_ledger || !line.starts_with("| `") {
            continue;
        }
        let entry = line
            .split('|')
            .nth(1)
            .and_then(|cell| cell.split('`').nth(1))
            .expect("each extra ledger row should name its invocation");
        let status_cell = line.split('|').nth(1).unwrap_or_default();
        assert!(
            status_cell.contains("candidate") || status_cell.contains("retain"),
            "every extra needs a retain or candidate disposition: {line}"
        );
        assert!(
            !status_cell.to_ascii_lowercase().contains("deprecat")
                && !status_cell.to_ascii_lowercase().contains("removed"),
            "no extra may be newly deprecated or removed: {line}"
        );
        assert!(
            line.contains('→'),
            "each extra needs a migration example: {line}"
        );
        ledger_extras.insert(entry);
    }
    assert_eq!(ledger_extras, expected_extras);
    assert!(review.contains("Spec 001 owns init/version"));
    assert!(review.contains("Spec 002 owns daemon lifecycle"));
    assert!(review.contains("Plugin behavior is preserved and not reimplemented"));
}

fn assert_human_output(output: &Output, expected: &str) {
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        serde_json::from_slice::<Value>(&output.stdout).is_err(),
        "default output should be human-readable, got: {stdout}"
    );
    assert!(
        stdout.contains(expected),
        "human output should contain {expected:?}, got: {stdout}"
    );
}

fn assert_json_output(output: &Output) -> Value {
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    json(&output.stdout)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canonical_provider_mcp_and_secret_commands_support_human_and_json_output() {
    let fixture = ManagementFixture::new().expect("isolated fixture should be created");
    let store = isolated_core_store(&fixture, "integration-output-core");
    let mut core = InProcessCore::start_with_store(store.clone())
        .await
        .expect("isolated Core should start");
    let contexts = fixture.path("integration output contexts.json");
    write_selected_context(&contexts, &core);
    let context_file = path_arg(contexts);
    let env = [("KAKUNE_TOKEN", core.token.as_str())];

    let set_provider_secret = run_with_stdin(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "secret",
            "set",
            "provider-key",
            "--stdin",
            "--json",
        ]),
        &env,
        Some(b"provider-canary"),
    )
    .expect("JSON secret set should finish");
    let secret_record = assert_json_output(&set_provider_secret);
    assert_eq!(secret_record["status"], "committed");
    assert_eq!(secret_record["secret"]["name"], "provider-key");
    assert!(!String::from_utf8_lossy(&set_provider_secret.stdout).contains("provider-canary"));
    assert_eq!(
        store.resolve_secret("provider-key").unwrap(),
        "provider-canary"
    );

    let provider_profile = ProviderProfileUpsert {
        id: "human-provider".into(),
        display_name: "Human Provider".into(),
        provider_type: ProviderType::MiniMax,
        default_model: "MiniMax-M3".into(),
        allowed_models: vec!["MiniMax-M3".into(), "MiniMax-M2".into()],
        capabilities: vec!["text-generation".into()],
        auth: ProviderAuth::ApiKeySecret {
            secret_ref: "provider-key".into(),
        },
        config: serde_json::json!({}),
    };
    let human_provider_file = fixture.path("human provider.json");
    fs::write(
        &human_provider_file,
        serde_json::to_vec(&provider_profile).unwrap(),
    )
    .unwrap();
    let human_provider = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "provider",
            "add",
            "human-provider",
            "--file",
            &path_arg(human_provider_file),
        ]),
        &env,
    )
    .unwrap();
    assert_human_output(&human_provider, "Added provider human-provider");
    assert!(!String::from_utf8_lossy(&human_provider.stdout).contains("provider-key"));

    let mut json_profile = provider_profile.clone();
    json_profile.id = "json-provider".into();
    json_profile.display_name = "JSON Provider".into();
    let json_provider_file = fixture.path("json provider.json");
    fs::write(
        &json_provider_file,
        serde_json::to_vec(&json_profile).unwrap(),
    )
    .unwrap();
    let json_provider = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "provider",
            "add",
            "json-provider",
            "--file",
            &path_arg(json_provider_file),
            "--json",
        ]),
        &env,
    )
    .unwrap();
    assert_eq!(assert_json_output(&json_provider)["id"], "json-provider");

    for (mode, expected) in [("human", "human-provider"), ("json", "json-provider")] {
        let mut args = vec![
            "--context-file".to_owned(),
            context_file.clone(),
            "provider".to_owned(),
            "list".to_owned(),
            "--limit".to_owned(),
            "100".to_owned(),
        ];
        if mode == "json" {
            args.push("--json".to_owned());
        }
        let listed = run(&fixture, &args, &env).unwrap();
        if mode == "json" {
            let page = assert_json_output(&listed);
            assert!(page["items"].as_array().unwrap().len() >= 2);
        } else {
            assert_human_output(&listed, "human-provider");
            assert!(!String::from_utf8_lossy(&listed.stdout).contains("provider-key"));
        }
        assert!(String::from_utf8_lossy(&listed.stdout).contains(expected));
    }

    let human_test = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "provider",
            "test",
            "human-provider",
        ]),
        &env,
    )
    .unwrap();
    assert_human_output(&human_test, "Connectivity");
    assert!(!String::from_utf8_lossy(&human_test.stdout).contains("provider-canary"));
    let json_test = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "provider",
            "test",
            "human-provider",
            "--json",
        ]),
        &env,
    )
    .unwrap();
    assert_eq!(
        assert_json_output(&json_test)["connectivity"]["status"],
        "unknown"
    );

    for as_json in [false, true] {
        let mut args = vec![
            "--context-file".to_owned(),
            context_file.clone(),
            "provider".to_owned(),
            "models".to_owned(),
            "human-provider".to_owned(),
        ];
        if as_json {
            args.push("--json".to_owned());
        }
        let models = run(&fixture, &args, &env).unwrap();
        assert_eq!(models.status.code(), Some(1));
        assert!(String::from_utf8_lossy(&models.stderr).contains("unsupported"));
        if as_json {
            assert_eq!(json(&models.stdout)["discovery"]["status"], "unsupported");
        } else {
            assert!(String::from_utf8_lossy(&models.stdout).contains("Model discovery"));
            assert!(serde_json::from_slice::<Value>(&models.stdout).is_err());
        }
    }

    let auth_human = run_with_stdin(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "provider",
            "auth",
            "human-provider",
            "--stdin",
        ]),
        &env,
        Some(b"auth-human-canary"),
    )
    .unwrap();
    assert_human_output(&auth_human, "Authentication committed");
    assert!(!String::from_utf8_lossy(&auth_human.stdout).contains("auth-human-canary"));
    let auth_json = run_with_stdin(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "provider",
            "auth",
            "json-provider",
            "--stdin",
            "--json",
        ]),
        &env,
        Some(b"auth-json-canary"),
    )
    .unwrap();
    assert_eq!(assert_json_output(&auth_json)["status"], "committed");
    assert!(!String::from_utf8_lossy(&auth_json.stdout).contains("auth-json-canary"));

    let http_config = fixture.path("http output connection.json");
    fs::write(
        &http_config,
        serde_json::to_vec(&serde_json::json!({
            "transport": {
                "type": "http",
                "endpoint": "https://mcp.example.test/mcp",
                "bearerSecretRef": "provider-key"
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let human_mcp_add = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "mcp",
            "add",
            "human-mcp",
            "--file",
            &path_arg(http_config.clone()),
        ]),
        &env,
    )
    .unwrap();
    assert_human_output(&human_mcp_add, "Added MCP connection human-mcp");
    let json_mcp_add = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "mcp",
            "add",
            "json-mcp",
            "--file",
            &path_arg(http_config),
            "--json",
        ]),
        &env,
    )
    .unwrap();
    assert_eq!(assert_json_output(&json_mcp_add)["name"], "json-mcp");

    for as_json in [false, true] {
        let mut args = vec![
            "--context-file".to_owned(),
            context_file.clone(),
            "mcp".to_owned(),
            "list".to_owned(),
            "--limit".to_owned(),
            "100".to_owned(),
        ];
        if as_json {
            args.push("--json".to_owned());
        }
        let listed = run(&fixture, &args, &env).unwrap();
        if as_json {
            assert!(
                assert_json_output(&listed)["items"]
                    .as_array()
                    .unwrap()
                    .len()
                    >= 2
            );
        } else {
            assert_human_output(&listed, "human-mcp");
        }
    }

    for (mode, name) in [("human", "human-mcp"), ("json", "json-mcp")] {
        let mut args = vec![
            "--context-file".to_owned(),
            context_file.clone(),
            "mcp".to_owned(),
            "inspect".to_owned(),
            name.to_owned(),
        ];
        if mode == "json" {
            args.push("--json".to_owned());
        }
        let inspected = run(&fixture, &args, &env).unwrap();
        if mode == "json" {
            assert_eq!(assert_json_output(&inspected)["transport"]["type"], "http");
        } else {
            assert_human_output(&inspected, "https://mcp.example.test/mcp");
        }
    }

    for (mode, name) in [("human", "human-mcp"), ("json", "json-mcp")] {
        let mut args = vec![
            "--context-file".to_owned(),
            context_file.clone(),
            "mcp".to_owned(),
            "auth".to_owned(),
            name.to_owned(),
        ];
        if mode == "json" {
            args.push("--json".to_owned());
        }
        let authenticated = run(&fixture, &args, &env).unwrap();
        if mode == "json" {
            assert_eq!(assert_json_output(&authenticated)["status"], "available");
        } else {
            assert_human_output(&authenticated, "Authentication references are available");
        }
    }

    let stdio_config = fixture.path("stdio output connection.json");
    fs::write(
        &stdio_config,
        serde_json::to_vec(&serde_json::json!({
            "transport": {
                "type": "stdio",
                "command": mcp_helper_command(),
                "args": [],
                "workingDirectory": fixture.root(),
                "environmentSecretRefs": {}
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let added_tool_server = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "mcp",
            "add",
            "tool-server",
            "--file",
            &path_arg(stdio_config),
            "--json",
        ]),
        &env,
    )
    .unwrap();
    assert_eq!(
        assert_json_output(&added_tool_server)["name"],
        "tool-server"
    );

    for as_json in [false, true] {
        let mut args = vec![
            "--context-file".to_owned(),
            context_file.clone(),
            "mcp".to_owned(),
            "tool".to_owned(),
            "list".to_owned(),
            "tool-server".to_owned(),
        ];
        if as_json {
            args.push("--json".to_owned());
        }
        let tools = run(&fixture, &args, &env).unwrap();
        if as_json {
            assert_eq!(assert_json_output(&tools)["items"][0]["name"], "greet");
        } else {
            assert_human_output(&tools, "greet");
            assert!(String::from_utf8_lossy(&tools.stdout).contains("test-mcp-server"));
        }
    }

    let tool_input = fixture.path("output tool input.json");
    fs::write(&tool_input, br#"{"name":"Ada"}"#).unwrap();
    for as_json in [false, true] {
        let mut args = vec![
            "--context-file".to_owned(),
            context_file.clone(),
            "mcp".to_owned(),
            "tool".to_owned(),
            "call".to_owned(),
            "tool-server".to_owned(),
            "greet".to_owned(),
            "--input".to_owned(),
            path_arg(tool_input.clone()),
        ];
        if as_json {
            args.push("--json".to_owned());
        }
        let called = run(&fixture, &args, &env).unwrap();
        if as_json {
            assert_eq!(
                assert_json_output(&called)["structuredContent"]["greeting"],
                "Hello, [redacted]!"
            );
        } else {
            assert_human_output(&called, "Hello, [redacted]!");
        }
    }

    let human_secret = run_with_stdin(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "secret",
            "set",
            "human-secret",
            "--stdin",
        ]),
        &env,
        Some(b"human-secret-canary"),
    )
    .unwrap();
    assert_human_output(&human_secret, "Secret human-secret committed");
    assert!(!String::from_utf8_lossy(&human_secret.stdout).contains("human-secret-canary"));
    let json_secret = run_with_stdin(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "secret",
            "set",
            "json-secret",
            "--stdin",
            "--json",
        ]),
        &env,
        Some(b"json-secret-canary"),
    )
    .unwrap();
    assert_eq!(
        assert_json_output(&json_secret)["secret"]["name"],
        "json-secret"
    );
    assert!(!String::from_utf8_lossy(&json_secret.stdout).contains("json-secret-canary"));

    for as_json in [false, true] {
        let mut args = vec![
            "--context-file".to_owned(),
            context_file.clone(),
            "secret".to_owned(),
            "list".to_owned(),
        ];
        if as_json {
            args.push("--json".to_owned());
        }
        let listed = run(&fixture, &args, &env).unwrap();
        if as_json {
            assert!(
                assert_json_output(&listed)["items"]
                    .as_array()
                    .unwrap()
                    .len()
                    >= 3
            );
        } else {
            assert_human_output(&listed, "human-secret");
        }
        let stdout = String::from_utf8_lossy(&listed.stdout);
        assert!(!stdout.contains("human-secret-canary"));
        assert!(!stdout.contains("json-secret-canary"));
        assert!(!stdout.contains("provider-canary"));
    }

    for (name, as_json) in [("human-secret", false), ("json-secret", true)] {
        let mut args = vec![
            "--context-file".to_owned(),
            context_file.clone(),
            "secret".to_owned(),
            "remove".to_owned(),
            name.to_owned(),
        ];
        if as_json {
            args.push("--json".to_owned());
        }
        let removed = run(&fixture, &args, &env).unwrap();
        if as_json {
            assert_eq!(assert_json_output(&removed)["removed"], true);
        } else {
            assert_human_output(&removed, "Secret human-secret removed");
        }
    }

    core.shutdown().await.unwrap();
}

#[test]
fn cli_help_explains_canonical_routing_legacy_differences_and_secret_inputs() {
    let fixture = ManagementFixture::new().expect("isolated fixture should be created");
    let (root_help, _) = help_commands(&fixture, &[]);
    assert!(root_help.contains("no implicit fallback"));
    assert!(root_help.contains("Use local execution only where the command documents"));

    let (context_help, _) = help_commands(&fixture, &["context"]);
    assert!(context_help.contains("context export <name> --output <file> [--replace]"));
    assert!(context_help.contains("validates the complete collection"));

    let (workflow_help, _) = help_commands(&fixture, &["workflow"]);
    for detail in [
        "Workflow create",
        "selected Core",
        "--limit",
        "--cursor",
        "--replace",
        "exit 2",
        "--data-dir` does not imply standalone execution",
    ] {
        assert!(
            workflow_help.contains(detail),
            "workflow help omitted {detail}:\n{workflow_help}"
        );
    }

    let (provider_help, _) = help_commands(&fixture, &["provider"]);
    for detail in [
        "create-only",
        "MiniMax",
        "unsupported",
        "upsert",
        "--cursor",
        "human-readable by default",
        "any canonical provider command",
    ] {
        assert!(
            provider_help.contains(detail),
            "provider help omitted {detail}:\n{provider_help}"
        );
    }

    let (mcp_help, _) = help_commands(&fixture, &["mcp"]);
    for detail in [
        "selected Core",
        "--input",
        "--secret",
        "mcp call <file>",
        "stdio and HTTP",
        "human-readable by default",
        "any canonical named MCP command",
    ] {
        assert!(
            mcp_help.contains(detail),
            "MCP help omitted {detail}:\n{mcp_help}"
        );
    }

    let (secret_help, _) = help_commands(&fixture, &["secret"]);
    for detail in [
        "selected Core",
        "--stdin",
        "metadata only",
        "never print secret values",
        "human-readable by default",
        "any canonical secret command",
    ] {
        assert!(
            secret_help.contains(detail),
            "secret help omitted {detail}:\n{secret_help}"
        );
    }

    let (legacy_run_help, _) = help_commands(&fixture, &["run"]);
    for detail in ["Legacy extra", "workflow run", "registers/updates", "TSV"] {
        assert!(
            legacy_run_help.contains(detail),
            "legacy run help omitted {detail}:\n{legacy_run_help}"
        );
    }

    let (legacy_executions_help, _) = help_commands(&fixture, &["executions"]);
    assert!(legacy_executions_help.contains("defaults to the local Store"));
    assert!(legacy_executions_help.contains("execution list"));
    assert!(
        !root_help.to_ascii_lowercase().contains("deprecated")
            && !context_help.to_ascii_lowercase().contains("deprecated")
            && !workflow_help.to_ascii_lowercase().contains("deprecated")
            && !provider_help.to_ascii_lowercase().contains("deprecated")
            && !mcp_help.to_ascii_lowercase().contains("deprecated")
            && !secret_help.to_ascii_lowercase().contains("deprecated")
            && !legacy_run_help.to_ascii_lowercase().contains("deprecated")
            && !legacy_executions_help
                .to_ascii_lowercase()
                .contains("deprecated"),
        "help must preserve extras without newly deprecating them"
    );
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

fn json(stdout: &[u8]) -> Value {
    serde_json::from_slice(stdout).expect("canonical JSON output should parse")
}

#[derive(Default)]
struct CliCredentialBackend(Mutex<HashMap<String, Vec<u8>>>);

impl NativeCredentialBackend for CliCredentialBackend {
    fn write(&self, key: &str, value: &[u8]) -> Result<(), String> {
        self.0
            .lock()
            .unwrap()
            .insert(key.to_owned(), value.to_vec());
        Ok(())
    }

    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        Ok(self.0.lock().unwrap().get(key).cloned())
    }

    fn delete(&self, key: &str) -> Result<(), String> {
        self.0.lock().unwrap().remove(key);
        Ok(())
    }
}

fn isolated_core_store(fixture: &ManagementFixture, name: &str) -> Store {
    Store::open_with_secret_backend(
        fixture.path(name),
        SecretBackendKind::Native,
        Arc::new(CliCredentialBackend::default()),
    )
    .expect("injected isolated store should open")
}

#[test]
fn doctor_exit_precedence_fails_required_unknown_and_any_failure_only() {
    let make_report = |unknown_area: Option<DoctorArea>, failed_area: Option<DoctorArea>| {
        DoctorReport::from_findings(
            DoctorArea::ALL
                .into_iter()
                .map(|area| {
                    let status = if Some(area) == failed_area {
                        DoctorStatus::Failed
                    } else if Some(area) == unknown_area {
                        DoctorStatus::Unknown
                    } else {
                        DoctorStatus::Warning
                    };
                    DoctorFinding::new(
                        area,
                        status,
                        "injected test applicability",
                        "injected safe finding",
                        "injected remediation",
                    )
                })
                .collect(),
        )
        .expect("all eight diagnostic findings should form a report")
    };

    assert_eq!(make_report(Some(DoctorArea::Core), None).exit_code, 0);
    assert_eq!(make_report(Some(DoctorArea::Service), None).exit_code, 0);
    assert_eq!(
        make_report(Some(DoctorArea::Configuration), None).exit_code,
        1
    );
    assert_eq!(make_report(None, Some(DoctorArea::Service)).exit_code, 1);
    assert_eq!(make_report(None, None).exit_code, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn secret_stdin_is_bounded_redacted_and_targets_only_the_selected_core() {
    let fixture = ManagementFixture::new().unwrap();
    let local_store = isolated_core_store(&fixture, "secret-cli-local");
    let target_store = isolated_core_store(&fixture, "secret-cli-target");
    let mut local = InProcessCore::start_with_store(local_store.clone())
        .await
        .unwrap();
    let mut target = InProcessCore::start_with_store(target_store.clone())
        .await
        .unwrap();
    let context_file = fixture.path("selected secret context.json");
    write_selected_context(&context_file, &target);
    let context_file = path_arg(context_file);
    const CANARY: &str = "cli-secret-private-proof";

    let set = run_with_stdin(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "secret",
            "set",
            "automation-key",
            "--stdin",
        ]),
        &[("KAKUNE_TOKEN", &target.token)],
        Some(format!("{CANARY}\r\n").as_bytes()),
    )
    .expect("explicit stdin secret command should finish");
    assert_eq!(
        set.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&set.stderr)
    );
    assert!(!String::from_utf8_lossy(&set.stdout).contains(CANARY));
    assert!(!String::from_utf8_lossy(&set.stderr).contains(CANARY));
    assert_eq!(
        target_store.resolve_secret("automation-key").unwrap(),
        CANARY
    );
    assert!(local_store.resolve_secret("automation-key").is_err());

    let one_delimiter = run_with_stdin(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "secret",
            "set",
            "one-delimiter-key",
            "--stdin",
        ]),
        &[("KAKUNE_TOKEN", &target.token)],
        Some(b"kept-newline\n\n"),
    )
    .unwrap();
    assert_eq!(one_delimiter.status.code(), Some(0));
    assert_eq!(
        target_store.resolve_secret("one-delimiter-key").unwrap(),
        "kept-newline\n"
    );

    let invalid_utf8 = run_with_stdin(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "secret",
            "set",
            "invalid-utf8-key",
            "--stdin",
        ]),
        &[("KAKUNE_TOKEN", &target.token)],
        Some(&[0xff]),
    )
    .unwrap();
    assert_ne!(invalid_utf8.status.code(), Some(0));
    assert!(target_store.resolve_secret("invalid-utf8-key").is_err());

    let maximum_input = vec![b'x'; 64 * 1024];
    let maximum = run_with_stdin(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "secret",
            "set",
            "maximum-key",
            "--stdin",
        ]),
        &[("KAKUNE_TOKEN", &target.token)],
        Some(&maximum_input),
    )
    .unwrap();
    assert_eq!(maximum.status.code(), Some(0));
    assert_eq!(
        target_store.resolve_secret("maximum-key").unwrap().len(),
        64 * 1024
    );

    let empty = run_with_stdin(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "secret",
            "set",
            "empty-key",
            "--stdin",
        ]),
        &[("KAKUNE_TOKEN", &target.token)],
        Some(b""),
    )
    .unwrap();
    assert_ne!(empty.status.code(), Some(0));
    assert!(target_store.resolve_secret("empty-key").is_err());

    let oversized = run_with_stdin(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "secret",
            "set",
            "oversized-key",
            "--stdin",
        ]),
        &[("KAKUNE_TOKEN", &target.token)],
        Some(&vec![b'x'; 65_537]),
    )
    .unwrap();
    assert_ne!(oversized.status.code(), Some(0));
    assert!(target_store.resolve_secret("oversized-key").is_err());

    let noninteractive = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "secret",
            "set",
            "prompt-key",
        ]),
        &[("KAKUNE_TOKEN", &target.token)],
    )
    .unwrap();
    assert_ne!(noninteractive.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&noninteractive.stderr).contains("interactive"));
    assert!(target_store.resolve_secret("prompt-key").is_err());

    let positional_value = run(
        &fixture,
        &strings(&["secret", "set", "bad-key", CANARY]),
        &[],
    )
    .unwrap();
    assert_eq!(positional_value.status.code(), Some(2));
    let value_flag = run(
        &fixture,
        &strings(&["secret", "set", "bad-key", "--value", CANARY]),
        &[],
    )
    .unwrap();
    assert_eq!(value_flag.status.code(), Some(2));
    assert!(!String::from_utf8_lossy(&value_flag.stderr).contains(CANARY));

    local.shutdown().await.unwrap();
    target.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_and_mcp_auth_cli_use_secret_references_and_reject_unsafe_input_modes() {
    let fixture = ManagementFixture::new().unwrap();
    let store = isolated_core_store(&fixture, "integration-auth-cli");
    store
        .create_provider_profile(ProviderProfileUpsert {
            id: "minimax-profile".into(),
            display_name: "MiniMax profile".into(),
            provider_type: ProviderType::MiniMax,
            default_model: "MiniMax-M3".into(),
            allowed_models: vec!["MiniMax-M3".into()],
            capabilities: vec!["text-generation".into()],
            auth: ProviderAuth::ApiKeySecret {
                secret_ref: "provider-key".into(),
            },
            config: serde_json::json!({}),
        })
        .unwrap();
    store
        .create_provider_profile(ProviderProfileUpsert {
            id: "codex-profile".into(),
            display_name: "Codex profile".into(),
            provider_type: ProviderType::Codex,
            default_model: "codex-default".into(),
            allowed_models: vec!["codex-default".into()],
            capabilities: vec!["text-generation".into()],
            auth: ProviderAuth::OAuthSecret {
                secret_ref: "codex-oauth".into(),
            },
            config: serde_json::json!({}),
        })
        .unwrap();
    let mut core = InProcessCore::start_with_store(store.clone())
        .await
        .unwrap();
    let context_file = fixture.path("integration auth context.json");
    write_selected_context(&context_file, &core);
    let context_file = path_arg(context_file);
    const CANARY: &str = "provider-secret-canary";

    let authenticated = run_with_stdin(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "provider",
            "auth",
            "minimax-profile",
            "--stdin",
        ]),
        &[("KAKUNE_TOKEN", &core.token)],
        Some(CANARY.as_bytes()),
    )
    .unwrap();
    assert_eq!(
        authenticated.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&authenticated.stderr)
    );
    assert_eq!(store.resolve_secret("provider-key").unwrap(), CANARY);
    assert!(!String::from_utf8_lossy(&authenticated.stdout).contains(CANARY));
    assert!(!String::from_utf8_lossy(&authenticated.stderr).contains(CANARY));

    let provider_test = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "provider",
            "test",
            "minimax-profile",
            "--json",
        ]),
        &[("KAKUNE_TOKEN", &core.token)],
    )
    .unwrap();
    assert_eq!(provider_test.status.code(), Some(0));
    let report = json(&provider_test.stdout);
    assert_eq!(report["authentication"]["status"], "unknown");
    assert_eq!(report["connectivity"]["status"], "unknown");
    assert_eq!(report["capabilities"][0]["status"], "unsupported");
    assert!(
        !provider_test
            .stdout
            .windows(CANARY.len())
            .any(|window| window == CANARY.as_bytes())
    );

    let models = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "provider",
            "models",
            "minimax-profile",
            "--json",
        ]),
        &[("KAKUNE_TOKEN", &core.token)],
    )
    .unwrap();
    assert_eq!(models.status.code(), Some(1));
    let models_json = json(&models.stdout);
    assert_eq!(models_json["discovery"]["status"], "unsupported");
    assert_eq!(models_json["models"][0]["provenance"], "configured");
    assert!(String::from_utf8_lossy(&models.stderr).contains("unsupported"));

    let api_config = fixture.path("http mcp connection.json");
    fs::write(
        &api_config,
        serde_json::to_vec(&serde_json::json!({
            "transport":{"type":"http","endpoint":"https://mcp.example.test/mcp","bearerSecretRef":null}
        }))
        .unwrap(),
    )
    .unwrap();
    let add_http = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "mcp",
            "add",
            "http-connection",
            "--file",
            &path_arg(api_config),
        ]),
        &[("KAKUNE_TOKEN", &core.token)],
    )
    .unwrap();
    assert_eq!(
        add_http.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&add_http.stderr)
    );

    let binding = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "mcp",
            "auth",
            "http-connection",
            "--secret",
            "provider-key",
        ]),
        &[("KAKUNE_TOKEN", &core.token)],
    )
    .unwrap();
    assert_eq!(
        binding.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&binding.stderr)
    );
    assert!(!String::from_utf8_lossy(&binding.stdout).contains(CANARY));

    let stdio_config = fixture.path("stdio mcp connection.json");
    fs::write(
        &stdio_config,
        serde_json::to_vec(&serde_json::json!({
            "transport":{
                "type":"stdio",
                "command":mcp_helper_command(),
                "args":[],
                "workingDirectory":fixture.root(),
                "environmentSecretRefs":{}
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let add_stdio = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "mcp",
            "add",
            "stdio-connection",
            "--file",
            &path_arg(stdio_config),
        ]),
        &[("KAKUNE_TOKEN", &core.token)],
    )
    .unwrap();
    assert_eq!(add_stdio.status.code(), Some(0));

    let tool_list = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "mcp",
            "tool",
            "list",
            "stdio-connection",
            "--json",
        ]),
        &[("KAKUNE_TOKEN", &core.token)],
    )
    .unwrap();
    assert_eq!(tool_list.status.code(), Some(0));
    assert_eq!(json(&tool_list.stdout)["items"][0]["name"], "greet");

    let tool_input = fixture.path("stdio tool input.json");
    fs::write(
        &tool_input,
        serde_json::to_vec(&serde_json::json!({"name":CANARY})).unwrap(),
    )
    .unwrap();
    let tool_input_arg = path_arg(tool_input);
    let tool_call = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "mcp",
            "tool",
            "call",
            "stdio-connection",
            "greet",
            "--input",
            &tool_input_arg,
        ]),
        &[("KAKUNE_TOKEN", &core.token)],
    )
    .unwrap();
    assert_eq!(tool_call.status.code(), Some(0));
    assert!(!String::from_utf8_lossy(&tool_call.stdout).contains(CANARY));

    let invalid_tool_input = fixture.path("invalid stdio tool input.json");
    fs::write(&invalid_tool_input, br#"{"name":42}"#).unwrap();
    let invalid_tool_input_arg = path_arg(invalid_tool_input);
    let rejected_tool_call = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "mcp",
            "tool",
            "call",
            "stdio-connection",
            "greet",
            "--input",
            &invalid_tool_input_arg,
        ]),
        &[("KAKUNE_TOKEN", &core.token)],
    )
    .unwrap();
    assert_ne!(rejected_tool_call.status.code(), Some(0));
    assert!(!String::from_utf8_lossy(&rejected_tool_call.stderr).contains(CANARY));
    let rejected_stdio_binding = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "mcp",
            "auth",
            "stdio-connection",
            "--secret",
            "provider-key",
        ]),
        &[("KAKUNE_TOKEN", &core.token)],
    )
    .unwrap();
    assert_ne!(rejected_stdio_binding.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&rejected_stdio_binding.stderr).contains("stdio"));

    let rejected_mcp_stdin = run(
        &fixture,
        &strings(&[
            "--context-file",
            &path_arg(fixture.path("must-not-be-read.json")),
            "mcp",
            "auth",
            "http-connection",
            "--stdin",
        ]),
        &[],
    )
    .unwrap();
    assert_eq!(rejected_mcp_stdin.status.code(), Some(2));

    let rejected_codex_stdin = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "provider",
            "auth",
            "codex-profile",
            "--stdin",
        ]),
        &[("KAKUNE_TOKEN", &core.token)],
    )
    .unwrap();
    assert_ne!(rejected_codex_stdin.status.code(), Some(0));
    assert!(store.resolve_secret("codex-oauth").is_err());

    core.shutdown().await.unwrap();
}

#[test]
fn doctor_reports_fixed_eight_area_matrix_and_absence_without_initializing() {
    let fixture = ManagementFixture::new().expect("isolated fixture should be created");
    let installation = fixture.path("missing installation");
    let data_dir = installation.join("data");
    let config = installation.join("config.yaml");
    let before = support::management::FilesystemSnapshot::capture(&installation)
        .expect("fixture should be snapshot-able");
    let output = run(
        &fixture,
        &strings(&[
            "doctor",
            "--data-dir",
            &path_arg(data_dir.clone()),
            "--config",
            &path_arg(config),
            "--json",
        ]),
        &[],
    )
    .expect("doctor should complete within its bounded fixture deadline");

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stderr.is_empty());
    let report = json(&output.stdout);
    let findings = report["findings"]
        .as_array()
        .expect("JSON doctor report should contain findings");
    let areas = findings
        .iter()
        .map(|finding| finding["area"].as_str().expect("finding area"))
        .collect::<Vec<_>>();
    assert_eq!(
        areas,
        [
            "configuration",
            "storage",
            "database",
            "credential-store",
            "core",
            "service",
            "compatibility",
            "platform",
        ]
    );
    for finding in findings {
        assert!(finding["message"].is_string());
        assert!(finding["remediation"].is_string());
        assert!(finding["evidenceTimestamp"].is_string());
        assert!(finding["applicabilityReason"].is_string());
        assert!(finding["status"].is_string());
        assert!(finding["required"].is_boolean());
    }
    for (area, required) in [
        ("configuration", true),
        ("storage", true),
        ("database", true),
        ("credential-store", true),
        ("core", false),
        ("service", false),
        ("compatibility", true),
        ("platform", true),
    ] {
        let finding = findings
            .iter()
            .find(|finding| finding["area"] == area)
            .expect("all diagnostic areas must be reported");
        assert_eq!(finding["required"], required, "requiredness for {area}");
    }
    assert_eq!(report["exitCode"], 1);
    before
        .ensure_unchanged(&installation)
        .expect("doctor must not create, repair, migrate, or rewrite installation files");
    assert!(!data_dir.exists());
}

#[test]
fn doctor_warning_only_optional_findings_succeed_and_required_failures_do_not() {
    let fixture = ManagementFixture::new().expect("isolated fixture should be created");
    let data_dir = fixture.path("healthy data");
    fs::create_dir_all(&data_dir).expect("isolated data directory should be created");
    let config_path = fixture.path("healthy config.yaml");
    fs::write(
        &config_path,
        serde_yaml::to_string(&CoreConfig::default()).expect("default config should serialize"),
    )
    .expect("config fixture should be written");
    drop(Store::open(data_dir.clone()).expect("current isolated database should open"));
    let database = Connection::open(data_dir.join("kakune.sqlite3"))
        .expect("database should reopen for deterministic journal cleanup");
    database
        .pragma_update(None, "journal_mode", "DELETE")
        .expect("fixture should checkpoint WAL into the main database");
    drop(database);

    let args = strings(&[
        "doctor",
        "--data-dir",
        &path_arg(data_dir.clone()),
        "--config",
        &path_arg(config_path.clone()),
        "--json",
    ]);
    let healthy = run(&fixture, &args, &[("KAKUNE_TOKEN", "doctor-test-token")])
        .expect("healthy isolated doctor should complete");
    assert_eq!(
        healthy.status.code(),
        Some(0),
        "stderr: {}\nstdout: {}",
        String::from_utf8_lossy(&healthy.stderr),
        String::from_utf8_lossy(&healthy.stdout),
    );
    assert!(healthy.stderr.is_empty());
    let report = json(&healthy.stdout);
    assert_eq!(report["exitCode"], 0);
    assert_eq!(report["findings"].as_array().unwrap().len(), 8);
    assert!(!String::from_utf8_lossy(&healthy.stdout).contains("doctor-test-token"));
    assert!(
        report["findings"]
            .as_array()
            .unwrap()
            .iter()
            .all(|finding| {
                !finding["required"].as_bool().unwrap_or(false)
                    || !matches!(finding["status"].as_str(), Some("failed" | "unknown"))
            })
    );

    fs::write(&config_path, b"not: [valid yaml").expect("invalid config fixture should write");
    let failed = run(&fixture, &args, &[("KAKUNE_TOKEN", "doctor-test-token")])
        .expect("damaged-installation doctor should complete");
    assert_eq!(failed.status.code(), Some(1));
    assert_eq!(json(&failed.stdout)["exitCode"], 1);
}

#[test]
fn local_scope_and_conflicting_context_file_options_fail_before_io() {
    let fixture = ManagementFixture::new().expect("isolated fixture should be created");
    let data_dir = fixture.path("must remain absent");
    let output = run(
        &fixture,
        &strings(&[
            "--context",
            "remote",
            "doctor",
            "--data-dir",
            &path_arg(data_dir.clone()),
            "--json",
        ]),
        &[],
    )
    .expect("invalid local-scope invocation should exit");
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("local"));
    assert!(!data_dir.exists());

    let contexts = fixture.path("contexts.json");
    let output = run(
        &fixture,
        &strings(&[
            "--context-file",
            &path_arg(contexts.clone()),
            "context",
            "list",
            "--contexts-file",
            &path_arg(fixture.path("other contexts.json")),
        ]),
        &[],
    )
    .expect("conflicting context paths should exit");
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("cannot be combined"));
    assert!(!contexts.exists());

    let output = run(
        &fixture,
        &strings(&["doctor", "--not-a-doctor-option"]),
        &[],
    )
    .expect("invalid doctor syntax should exit");
    assert_eq!(output.status.code(), Some(2));
}

#[test]
fn named_context_management_merges_exports_one_context_and_clears_active_selection() {
    let fixture = ManagementFixture::new().expect("isolated fixture should be created");
    let contexts = fixture.path("connections with spaces-λ.json");
    let contexts_arg = path_arg(contexts.clone());
    let add_alpha = strings(&[
        "context",
        "add",
        "alpha",
        "--endpoint",
        "https://alpha.example.test/api",
        "--name",
        "Álpha Core",
        "--credential-ref",
        "keychain:fixture/alpha",
        "--contexts-file",
        &contexts_arg,
    ]);
    let added = run(&fixture, &add_alpha, &[]).expect("first context should add");
    assert_eq!(
        added.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&added.stderr)
    );

    let add_beta = strings(&[
        "context",
        "add",
        "beta",
        "--endpoint",
        "https://beta.example.test",
        "--contexts-file",
        &contexts_arg,
    ]);
    let added = run(&fixture, &add_beta, &[]).expect("second context should add");
    assert_eq!(
        added.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&added.stderr)
    );

    let before_invalid = fs::read(&contexts).unwrap();
    for (id, endpoint) in [
        ("duplicate", "https://user:password@duplicate.example.test"),
        (
            "query-secret",
            "https://core.example.test?token=private-proof",
        ),
    ] {
        let invalid = strings(&[
            "context",
            "add",
            id,
            "--endpoint",
            endpoint,
            "--contexts-file",
            &contexts_arg,
        ]);
        let rejected = run(&fixture, &invalid, &[]).unwrap();
        assert_eq!(rejected.status.code(), Some(1));
        assert!(!String::from_utf8_lossy(&rejected.stderr).contains("private-proof"));
        assert_eq!(fs::read(&contexts).unwrap(), before_invalid);
    }
    let duplicate = strings(&[
        "context",
        "add",
        "alpha",
        "--endpoint",
        "https://alpha.example.test",
        "--contexts-file",
        &contexts_arg,
    ]);
    assert_eq!(
        run(&fixture, &duplicate, &[]).unwrap().status.code(),
        Some(1)
    );
    assert_eq!(fs::read(&contexts).unwrap(), before_invalid);

    let select = strings(&["context", "use", "alpha", "--contexts-file", &contexts_arg]);
    assert_eq!(run(&fixture, &select, &[]).unwrap().status.code(), Some(0));
    let inspect = strings(&[
        "context",
        "inspect",
        "alpha",
        "--contexts-file",
        &contexts_arg,
    ]);
    let inspected = run(&fixture, &inspect, &[]).unwrap();
    assert_eq!(inspected.status.code(), Some(0));
    let inspected_json = json(&inspected.stdout);
    assert_eq!(inspected_json["id"], "alpha");
    assert_eq!(inspected_json["name"], "Álpha Core");
    assert_eq!(inspected_json["credentialRef"], "keychain:fixture/alpha");
    assert!(!String::from_utf8_lossy(&inspected.stdout).contains("credential-value"));

    let exported = fixture.path("one-context export.json");
    let export = strings(&[
        "context",
        "export",
        "alpha",
        "--output",
        &path_arg(exported.clone()),
        "--contexts-file",
        &contexts_arg,
    ]);
    let result = run(&fixture, &export, &[]).unwrap();
    assert_eq!(
        result.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let export_json: Value = serde_json::from_slice(&fs::read(&exported).unwrap()).unwrap();
    assert_eq!(export_json["contexts"].as_array().unwrap().len(), 1);
    assert_eq!(export_json["contexts"][0]["id"], "alpha");
    assert!(export_json["activeContextId"].is_null());
    assert_eq!(
        export_json["contexts"][0]["credentialRef"],
        "keychain:fixture/alpha"
    );

    let prior_destination = b"preserve this export";
    fs::write(&exported, prior_destination).unwrap();
    let rejected = run(&fixture, &export, &[]).unwrap();
    assert_eq!(rejected.status.code(), Some(1));
    assert_eq!(fs::read(&exported).unwrap(), prior_destination);
    let replace = strings(&[
        "context",
        "export",
        "alpha",
        "--output",
        &path_arg(exported.clone()),
        "--replace",
        "--contexts-file",
        &contexts_arg,
    ]);
    let replaced = run(&fixture, &replace, &[]).unwrap();
    assert_eq!(replaced.status.code(), Some(0));
    assert_eq!(
        json(&fs::read(&exported).unwrap())["contexts"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    let failed_destination = fixture.path("replacement target directory");
    fs::create_dir(&failed_destination).unwrap();
    let destination_marker = failed_destination.join("preserve.txt");
    fs::write(&destination_marker, b"preserve existing destination").unwrap();
    let failed_replace = strings(&[
        "context",
        "export",
        "alpha",
        "--output",
        &path_arg(failed_destination.clone()),
        "--replace",
        "--contexts-file",
        &contexts_arg,
    ]);
    assert_eq!(
        run(&fixture, &failed_replace, &[]).unwrap().status.code(),
        Some(1)
    );
    assert_eq!(
        fs::read(&destination_marker).unwrap(),
        b"preserve existing destination"
    );
    assert!(
        fs::read_dir(fixture.root())
            .unwrap()
            .filter_map(Result::ok)
            .all(|entry| !entry
                .file_name()
                .to_string_lossy()
                .starts_with(".kakune-context-"))
    );

    let imported_path = fixture.path("imported contexts.json");
    fs::write(
        &imported_path,
        serde_json::to_vec(&serde_json::json!({
            "format": "kakune-contexts/v1",
            "exportedAt": "2026-10-26T00:00:00Z",
            "activeContextId": "gamma",
            "contexts": [{
                "id": "gamma",
                "name": "Gamma",
                "endpoint": "https://gamma.example.test",
                "expectedCoreId": null,
                "color": null,
                "credentialRef": null
            }]
        }))
        .unwrap(),
    )
    .unwrap();
    let import = strings(&[
        "context",
        "import",
        &path_arg(imported_path.clone()),
        "--contexts-file",
        &contexts_arg,
    ]);
    let imported = run(&fixture, &import, &[]).unwrap();
    assert_eq!(
        imported.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&imported.stderr)
    );
    let collection: Value = serde_json::from_slice(&fs::read(&contexts).unwrap()).unwrap();
    assert_eq!(collection["activeContextId"], "alpha");
    assert_eq!(collection["contexts"].as_array().unwrap().len(), 3);

    let original = fs::read(&contexts).unwrap();
    let conflict_path = fixture.path("conflicting batch.json");
    fs::write(
        &conflict_path,
        serde_json::to_vec(&serde_json::json!({
            "format": "kakune-contexts/v1",
            "exportedAt": "2026-10-26T00:00:00Z",
            "activeContextId": null,
            "contexts": [
                {"id":"delta","name":"Delta","endpoint":"https://delta.example.test","expectedCoreId":null,"color":null,"credentialRef":null},
                {"id":"alpha","name":"Duplicate","endpoint":"https://duplicate.example.test","expectedCoreId":null,"color":null,"credentialRef":null}
            ]
        }))
        .unwrap(),
    )
    .unwrap();
    let conflict = strings(&[
        "context",
        "import",
        &path_arg(conflict_path.clone()),
        "--contexts-file",
        &contexts_arg,
    ]);
    assert_eq!(
        run(&fixture, &conflict, &[]).unwrap().status.code(),
        Some(1)
    );
    assert_eq!(
        fs::read(&contexts).unwrap(),
        original,
        "conflicting batch is all-or-nothing"
    );

    let malformed_path = fixture.path("malformed contexts.json");
    fs::write(&malformed_path, b"{malformed json").unwrap();
    let malformed = strings(&[
        "context",
        "import",
        &path_arg(malformed_path),
        "--contexts-file",
        &contexts_arg,
    ]);
    assert_eq!(
        run(&fixture, &malformed, &[]).unwrap().status.code(),
        Some(1)
    );
    assert_eq!(
        fs::read(&contexts).unwrap(),
        original,
        "malformed import must not mutate collection"
    );

    let remove = strings(&[
        "context",
        "remove",
        "alpha",
        "--contexts-file",
        &contexts_arg,
    ]);
    assert_eq!(run(&fixture, &remove, &[]).unwrap().status.code(), Some(0));
    let after_remove: Value = serde_json::from_slice(&fs::read(&contexts).unwrap()).unwrap();
    assert!(after_remove["activeContextId"].is_null());

    let local_store = fixture.path("must not be opened locally");
    let missing_selection = strings(&[
        "--context-file",
        &contexts_arg,
        "workflow",
        "list",
        "--data-dir",
        &path_arg(local_store.clone()),
    ]);
    let failed = run(&fixture, &missing_selection, &[]).unwrap();
    assert_eq!(failed.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&failed.stderr).contains("no Core context is selected"));
    assert!(
        !local_store.exists(),
        "missing selection must not fall back to local Store::open"
    );

    let missing_parent_output = fixture.path("missing parent/export.json");
    let missing_parent = strings(&[
        "context",
        "export",
        "beta",
        "--output",
        &path_arg(missing_parent_output.clone()),
        "--contexts-file",
        &contexts_arg,
    ]);
    assert_eq!(
        run(&fixture, &missing_parent, &[]).unwrap().status.code(),
        Some(1)
    );
    assert!(!missing_parent_output.parent().unwrap().exists());
}

fn write_context_for_endpoint(fixture: &ManagementFixture, endpoint: &str) -> PathBuf {
    let path = fixture.path("workflow contexts.json");
    fs::write(
        &path,
        serde_json::to_vec(&serde_json::json!({
            "format": "kakune-contexts/v1",
            "exportedAt": "2026-01-01T00:00:00Z",
            "revision": 1,
            "activeContextId": "selected",
            "contexts": [{
                "id": "selected",
                "name": "Selected Core",
                "endpoint": endpoint,
                "expectedCoreId": null,
                "color": null,
                "credentialRef": "env:KAKUNE_TOKEN"
            }]
        }))
        .expect("context fixture should encode"),
    )
    .expect("context fixture should be written");
    path
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_cli_lifecycle_preserves_disabled_manual_run_and_selected_core_routing() {
    let fixture = ManagementFixture::new().expect("isolated fixture should be created");
    let default_source = fixture.path("demo.kakune.yaml");
    let created = run(&fixture, &strings(&["workflow", "create", "demo"]), &[])
        .expect("workflow create should finish");
    assert_eq!(
        created.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    assert!(
        default_source.is_file(),
        "safe names get a predictable YAML path"
    );
    let source = fs::read_to_string(&default_source).expect("starter YAML should be readable");
    assert!(kakune_core::analyze_workflow(&source).is_valid());

    let local_file = path_arg(default_source.clone());
    for action in ["validate", "plan"] {
        let checked = run(
            &fixture,
            &strings(&["workflow", action, "--file", &local_file]),
            &[],
        )
        .expect("local workflow check should finish");
        assert_eq!(
            checked.status.code(),
            Some(0),
            "{action}: {}",
            String::from_utf8_lossy(&checked.stderr)
        );
        assert!(String::from_utf8_lossy(&checked.stdout).contains("local"));
    }

    let reference_source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata: {id: demo, name: Demo}\ntriggers: [{id: manual, type: kakune.trigger.manual@1}]\nentry: start\nnodes:\n  - id: start\n    type: kakune.flow.pass@1\n    inputs: {value: {literal: round-trip}}\n    on: {success: finish}\n  - id: finish\n    type: kakune.flow.pass@1\n    inputs: {value: {from: start.value}}\noutputs: {result: {from: finish.value}}\n";
    fs::write(&default_source, reference_source).expect("reference workflow should be written");
    assert!(kakune_core::analyze_workflow(reference_source).is_valid());

    let mut core = InProcessCore::start(fixture.path("workflow-core"))
        .await
        .expect("selected Core should start");
    let contexts = write_context_for_endpoint(&fixture, &core.endpoint);
    let context_file = path_arg(contexts.clone());
    let source_file = path_arg(default_source.clone());
    let env = [("KAKUNE_TOKEN", core.token.as_str())];
    let imported = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "workflow",
            "import",
            &source_file,
            "--json",
        ]),
        &env,
    )
    .expect("workflow import should finish");
    assert_eq!(
        imported.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&imported.stderr)
    );
    let imported_record = json(&imported.stdout);
    assert_eq!(imported_record["id"], "demo");
    assert_eq!(imported_record["status"], "disabled");

    let duplicate = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "workflow",
            "import",
            &source_file,
        ]),
        &env,
    )
    .expect("duplicate import should return a failure");
    assert_eq!(duplicate.status.code(), Some(1));

    let replaced = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "workflow",
            "import",
            &source_file,
            "--replace",
            "--json",
        ]),
        &env,
    )
    .expect("explicit replacement should finish");
    assert_eq!(
        replaced.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&replaced.stderr)
    );
    let replaced_record = json(&replaced.stdout);
    assert_ne!(replaced_record["revision"], imported_record["revision"]);
    assert_eq!(replaced_record["status"], "disabled");

    let stale_replacement = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "workflow",
            "import",
            &source_file,
            "--replace",
            "--if-revision",
            "stale-revision",
        ]),
        &env,
    )
    .expect("stale replacement should return a conflict");
    assert_eq!(stale_replacement.status.code(), Some(1));

    let export_file = fixture.path("round trip.kakune.yaml");
    let export_arg = path_arg(export_file.clone());
    let exported = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "workflow",
            "export",
            "name:Demo",
            "--output",
            &export_arg,
        ]),
        &env,
    )
    .expect("workflow export should finish");
    assert_eq!(
        exported.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&exported.stderr)
    );
    let round_trip = fs::read_to_string(&export_file).expect("exported YAML should be readable");
    assert_eq!(
        round_trip, reference_source,
        "export preserves original YAML"
    );
    assert_eq!(
        kakune_core::WorkflowDocument::parse(&round_trip)
            .expect("exported workflow should parse")
            .metadata
            .id,
        "demo"
    );

    let no_replace_export = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "workflow",
            "export",
            "name:Demo",
            "--output",
            &export_arg,
        ]),
        &env,
    )
    .expect("existing export destination should require consent");
    assert_eq!(no_replace_export.status.code(), Some(1));
    assert_eq!(fs::read(&export_file).unwrap(), round_trip.as_bytes());

    let replace_export = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "workflow",
            "export",
            "name:Demo",
            "--output",
            &export_arg,
            "--replace",
        ]),
        &env,
    )
    .expect("explicit export replacement should finish");
    assert_eq!(
        replace_export.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&replace_export.stderr)
    );
    assert_eq!(fs::read(&export_file).unwrap(), round_trip.as_bytes());

    let listed = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "workflow",
            "list",
            "--json",
        ]),
        &env,
    )
    .expect("workflow list should finish");
    assert_eq!(listed.status.code(), Some(0));
    assert_eq!(json(&listed.stdout)["items"][0]["status"], "disabled");

    let executed = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "workflow",
            "run",
            "id:demo",
            "--request-key",
            "workflow-cli-run",
            "--json",
        ]),
        &env,
    )
    .expect("manual workflow submission should finish");
    assert_eq!(
        executed.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&executed.stderr)
    );
    assert!(json(&executed.stdout)["id"].as_str().is_some());
    let still_disabled = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "workflow",
            "list",
            "--json",
        ]),
        &env,
    )
    .expect("workflow list should still work");
    assert_eq!(
        json(&still_disabled.stdout)["items"][0]["status"],
        "disabled"
    );

    for (action, expected) in [("enable", "enabled"), ("disable", "disabled")] {
        let changed = run(
            &fixture,
            &strings(&[
                "--context-file",
                &context_file,
                "workflow",
                action,
                "name:Demo",
            ]),
            &env,
        )
        .expect("eligibility command should finish");
        assert_eq!(
            changed.status.code(),
            Some(0),
            "{action}: {}",
            String::from_utf8_lossy(&changed.stderr)
        );
        let listed = run(
            &fixture,
            &strings(&[
                "--context-file",
                &context_file,
                "workflow",
                "list",
                "--json",
            ]),
            &env,
        )
        .expect("workflow status should be observable");
        assert_eq!(json(&listed.stdout)["items"][0]["status"], expected);
    }

    let invalid_file = fixture.path("invalid nested.kakune.yaml");
    fs::write(
        &invalid_file,
        "apiVersion: kakune/v1\nkind: Workflow\nmetadata: {id: invalid, name: Invalid}\ntriggers: [{id: manual, type: kakune.trigger.manual@1}]\nentry: outer\nnodes:\n  - id: outer\n    type: kakune.flow.foreach@1\n    with: {maxConcurrency: 1}\n    inputs: {items: {literal: []}}\n    body:\n      entry: inner\n      nodes:\n        - id: inner\n          type: kakune.flow.pass@1\n          inputs: {value: {from: missing.output}}\n",
    )
    .expect("invalid workflow should be written");
    let invalid_path = path_arg(invalid_file);
    let rejected = run(
        &fixture,
        &strings(&["workflow", "validate", "--file", &invalid_path]),
        &[],
    )
    .expect("invalid nested source should be rejected");
    assert_eq!(rejected.status.code(), Some(1));

    core.shutdown().await.expect("selected Core should stop");
}

#[tokio::test]
async fn workflow_standalone_ignores_default_context_but_rejects_explicit_remote_context() {
    let fixture = ManagementFixture::new().expect("isolated fixture should be created");
    let source = fixture.path("standalone.kakune.yaml");
    fs::write(
        &source,
        "apiVersion: kakune/v1\nkind: Workflow\nmetadata: {id: standalone, name: Standalone}\ntriggers: [{id: manual, type: kakune.trigger.manual@1}]\nentry: start\nnodes: [{id: start, type: kakune.flow.pass@1}]\n",
    )
    .expect("standalone source should be written");
    let contexts = write_context_for_endpoint(&fixture, "http://127.0.0.1:1");
    let context_file = path_arg(contexts);
    let source_arg = path_arg(source);
    let data_dir = fixture.path("standalone runtime");
    let data_arg = path_arg(data_dir.clone());
    let run_args = strings(&[
        "--context-file",
        &context_file,
        "workflow",
        "run",
        "--file",
        &source_arg,
        "--standalone",
        "--data-dir",
        &data_arg,
        "--timeout-seconds",
        "30",
        "--json",
    ]);
    let completed = run(&fixture, &run_args, &[]).expect("standalone run should not contact Core");
    assert_eq!(
        completed.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&completed.stderr)
    );
    let record = json(&completed.stdout);
    assert_eq!(record["status"], "succeeded");
    let store = Store::open(data_dir).expect("isolated standalone store should be readable");
    let executions = store
        .list_executions()
        .expect("execution should be recorded");
    assert_eq!(executions.len(), 1);
    assert_eq!(executions[0].status, "succeeded");
    assert!(
        store
            .list_workflows()
            .expect("registrations should list")
            .is_empty()
    );

    let approved_temp = fixture.path("approved-temp");
    let temporary_runtime_root = approved_temp.join("opencode");
    fs::create_dir_all(&temporary_runtime_root)
        .expect("approved temporary runtime root should be created");
    let temporary_runtime_dirs = || {
        fs::read_dir(&temporary_runtime_root)
            .expect("temporary runtime root should be readable")
            .map(|entry| entry.expect("runtime entry should be readable").path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("kakune-standalone-"))
            })
            .collect::<BTreeSet<_>>()
    };
    let runtime_dirs_before = temporary_runtime_dirs();
    let approved_temp_arg = path_arg(approved_temp);
    let temporary_run = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_file,
            "workflow",
            "run",
            "--file",
            &source_arg,
            "--standalone",
        ]),
        &[("TEMP", &approved_temp_arg), ("TMP", &approved_temp_arg)],
    )
    .expect("default standalone run should complete without contacting Core");
    assert_eq!(
        temporary_run.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&temporary_run.stderr)
    );
    assert_eq!(
        temporary_runtime_dirs(),
        runtime_dirs_before,
        "successful standalone execution must remove its temporary store"
    );

    let rejected = run(
        &fixture,
        &strings(&[
            "--context",
            "selected",
            "workflow",
            "run",
            "--file",
            &source_arg,
            "--standalone",
            "--data-dir",
            &path_arg(fixture.path("rejected standalone")),
        ]),
        &[],
    )
    .expect("explicit remote selection should be rejected");
    assert_eq!(rejected.status.code(), Some(2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn execution_and_trigger_cli_observations_are_bounded_and_truthful() {
    let fixture = ManagementFixture::new().expect("isolated fixture should be created");
    let mut core = InProcessCore::start(fixture.path("execution-trigger-core"))
        .await
        .expect("isolated Core should start");
    let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata: {id: flow, name: Flow}\ntriggers: [{id: manual, type: kakune.trigger.manual@1}]\nentry: start\nnodes: [{id: start, type: kakune.flow.pass@1}]\n";
    let workflow = kakune_core::WorkflowDocument::parse(source).expect("fixture workflow parses");
    core.store
        .create_workflow(&workflow, source, "disabled")
        .expect("fixture workflow should be stored");
    let connection = rusqlite::Connection::open(core.store.data_dir().join("kakune.sqlite3"))
        .expect("fixture DB should open for an orphan schedule record");
    connection
        .pragma_update(None, "foreign_keys", "OFF")
        .expect("fixture may seed a retained orphan record");
    connection
        .execute(
            "INSERT INTO scheduled_triggers (workflow_id, trigger_id, next_run_at, updated_at) VALUES ('orphan/flow', 'cron/a%', '2026-10-26T01:00:00Z', '2026-10-26T00:00:00Z')",
            [],
        )
        .expect("legacy orphan schedule should be seeded");
    drop(connection);

    let completed = core
        .store
        .create_execution("flow")
        .expect("completed fixture execution should be created");
    core.store
        .finish_execution(&completed.id, "succeeded", None)
        .expect("fixture execution should finish");
    core.store
        .set_execution_result(
            &completed.id,
            &serde_json::json!({"apiKey":"result-canary"}),
        )
        .expect("fixture execution result should be recorded");
    let active = core
        .store
        .create_execution("flow")
        .expect("active fixture execution should be created");

    let empty_id = uuid::Uuid::new_v4().to_string();
    let database = core.store.data_dir().join("kakune.sqlite3");
    let connection = rusqlite::Connection::open(database).expect("fixture DB should open");
    connection
        .execute(
            "INSERT INTO executions (id, workflow_name, status, created_at) VALUES (?1, 'flow', 'succeeded', '2026-10-26T00:00:00Z')",
            [&empty_id],
        )
        .expect("known execution with empty history should be inserted");
    drop(connection);

    let contexts = write_context_for_endpoint(&fixture, &core.endpoint);
    let context_arg = path_arg(contexts);
    let env = [("KAKUNE_TOKEN", core.token.as_str())];
    let list = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_arg,
            "execution",
            "list",
            "--limit",
            "1",
            "--json",
        ]),
        &env,
    )
    .expect("execution list should complete");
    assert_eq!(
        list.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&list.stderr)
    );
    let page = json(&list.stdout);
    assert_eq!(page["items"].as_array().unwrap().len(), 1);
    assert!(page["nextCursor"].as_str().is_some());

    let inspect = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_arg,
            "execution",
            "inspect",
            &completed.id,
        ]),
        &env,
    )
    .expect("execution inspect should complete");
    assert_eq!(inspect.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&inspect.stdout).contains("succeeded"));
    let inspect_json = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_arg,
            "execution",
            "inspect",
            &completed.id,
            "--json",
        ]),
        &env,
    )
    .expect("JSON execution inspection should complete");
    assert_eq!(inspect_json.status.code(), Some(0));
    let detail = json(&inspect_json.stdout);
    assert_eq!(detail["result"]["availability"], "unavailable");
    assert_eq!(detail["trace"]["availability"], "unavailable");
    assert!(!String::from_utf8_lossy(&inspect_json.stdout).contains("result-canary"));

    let cancel = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_arg,
            "execution",
            "cancel",
            &active.id,
            "--wait-seconds",
            "0",
            "--json",
        ]),
        &env,
    )
    .expect("execution cancellation should complete");
    assert_eq!(cancel.status.code(), Some(0));
    let cancellation = json(&cancel.stdout);
    assert_eq!(cancellation["outcome"], "requested");
    assert_eq!(cancellation["observedStatus"], "cancelling");
    assert_eq!(cancellation["pending"], true);

    let terminal_noop = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_arg,
            "execution",
            "cancel",
            &completed.id,
            "--json",
            "--wait-seconds",
            "30",
        ]),
        &env,
    )
    .expect("terminal cancellation should be an explicit no-op");
    assert_eq!(terminal_noop.status.code(), Some(0));
    assert_eq!(json(&terminal_noop.stdout)["outcome"], "terminal_unchanged");
    let invalid_wait = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_arg,
            "execution",
            "cancel",
            &completed.id,
            "--wait-seconds",
            "31",
        ]),
        &env,
    )
    .expect("out-of-range cancellation wait should be rejected");
    assert_eq!(invalid_wait.status.code(), Some(2));

    let logs = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_arg,
            "execution",
            "logs",
            &completed.id,
            "--limit",
            "1",
            "--json",
        ]),
        &env,
    )
    .expect("execution logs should complete");
    assert_eq!(logs.status.code(), Some(0));
    assert_eq!(json(&logs.stdout)["items"].as_array().unwrap().len(), 1);
    assert!(json(&logs.stdout)["highWater"].as_u64().is_some());
    assert!(json(&logs.stdout)["nextCursor"].as_str().is_some());

    let empty_logs = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_arg,
            "execution",
            "logs",
            &empty_id,
            "--json",
        ]),
        &env,
    )
    .expect("known empty event history should be distinct from unknown execution");
    assert_eq!(empty_logs.status.code(), Some(0));
    assert!(
        json(&empty_logs.stdout)["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let unknown_logs = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_arg,
            "execution",
            "logs",
            "unknown-execution",
            "--json",
        ]),
        &env,
    )
    .expect("unknown execution should fail explicitly");
    assert_eq!(unknown_logs.status.code(), Some(1));

    let trigger_list = run(
        &fixture,
        &strings(&["--context-file", &context_arg, "trigger", "list", "--json"]),
        &env,
    )
    .expect("trigger list should complete");
    assert_eq!(
        trigger_list.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&trigger_list.stderr)
    );
    let triggers = json(&trigger_list.stdout);
    let trigger = triggers["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["triggerId"] == "cron/a%")
        .expect("orphan schedule should remain visible");
    assert_eq!(trigger["workflowId"], "orphan/flow");
    assert_eq!(trigger["eligibility"], "unknown");
    assert_eq!(trigger["effectiveState"], "unknown");
    let composite_id = trigger["compositeId"].as_str().unwrap();
    assert_eq!(composite_id, "orphan%2Fflow/cron%2Fa%25");

    let trigger_inspect = run(
        &fixture,
        &strings(&[
            "--context-file",
            &context_arg,
            "trigger",
            "inspect",
            composite_id,
            "--json",
        ]),
        &env,
    )
    .expect("escaped composite trigger ID should inspect");
    assert_eq!(trigger_inspect.status.code(), Some(0));
    assert_eq!(json(&trigger_inspect.stdout)["triggerId"], "cron/a%");

    let events_before = core
        .store
        .list_events_after(None)
        .expect("events should list")
        .len();
    for args in [
        strings(&["--context-file", &context_arg, "execution", "list"]),
        strings(&[
            "--context-file",
            &context_arg,
            "execution",
            "inspect",
            &completed.id,
        ]),
        strings(&[
            "--context-file",
            &context_arg,
            "execution",
            "logs",
            &completed.id,
        ]),
        strings(&["--context-file", &context_arg, "trigger", "list"]),
        strings(&[
            "--context-file",
            &context_arg,
            "trigger",
            "inspect",
            composite_id,
        ]),
    ] {
        let output = run(&fixture, &args, &env).expect("read-only observation should finish");
        assert_eq!(output.status.code(), Some(0));
    }
    assert_eq!(
        core.store
            .list_events_after(None)
            .expect("events should remain readable")
            .len(),
        events_before,
        "execution and trigger observations must not record events or mutate schedules"
    );

    core.shutdown().await.expect("Core should stop cleanly");
}

fn nearest_rank_p95(samples: &[Duration]) -> Duration {
    assert!(
        !samples.is_empty(),
        "benchmark must retain measured samples"
    );
    let mut ordered = samples.to_vec();
    ordered.sort_unstable();
    let rank = (95 * ordered.len()).div_ceil(100);
    ordered[rank - 1]
}

fn format_milliseconds(samples: &[Duration]) -> String {
    samples
        .iter()
        .map(|sample| format!("{:.3}", sample.as_secs_f64() * 1_000.0))
        .collect::<Vec<_>>()
        .join(",")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "opt-in SC-007 end-to-end benchmark; run with --ignored --nocapture"]
async fn metadata_commands_report_end_to_end_nearest_rank_p95() {
    const WORKFLOW_COUNT: usize = 100;
    const EXECUTION_COUNT: usize = 1_000;
    const WARMUP_COUNT: usize = 5;
    const MEASURED_COUNT: usize = 100;

    let fixture = ManagementFixture::new().expect("benchmark fixture should be isolated");
    fs::create_dir(fixture.path("benchmark-exports"))
        .expect("isolated export directory should be created");
    let store = isolated_core_store(&fixture, "benchmark-core");
    // One process may make an authenticated info request plus the operation
    // request. Keep the isolated benchmark server's rate limiter above all
    // warmup and measured requests so throttling is reported as a failure,
    // rather than silently contaminating the latency distribution.
    let benchmark_api = ApiConfig {
        rate_limit_requests_per_minute: 10_000,
        ..ApiConfig::default()
    };
    let mut core = InProcessCore::start_with_store_and_api_config(store.clone(), benchmark_api)
        .await
        .expect("already-running local benchmark Core should start");

    let mut inspected_execution = None;
    for workflow_index in 0..WORKFLOW_COUNT {
        let workflow_id = format!("benchmark-workflow-{workflow_index:03}");
        let workflow_name = format!("Benchmark workflow {workflow_index:03}");
        let source = format!(
            "apiVersion: kakune/v1\nkind: Workflow\nmetadata: {{id: {workflow_id}, name: '{workflow_name}'}}\ntriggers: [{{id: manual, type: kakune.trigger.manual@1}}]\nentry: start\nnodes: [{{id: start, type: kakune.flow.pass@1}}]\n"
        );
        let document = kakune_core::WorkflowDocument::parse(&source)
            .expect("benchmark workflow source should be valid");
        store
            .create_workflow(&document, &source, "disabled")
            .expect("benchmark workflow should be registered disabled");
    }
    for execution_index in 0..EXECUTION_COUNT {
        let workflow_id = format!("benchmark-workflow-{:03}", execution_index % WORKFLOW_COUNT);
        let execution = store
            .create_execution(&workflow_id)
            .expect("benchmark execution should be recorded");
        store
            .finish_execution(&execution.id, "succeeded", None)
            .expect("benchmark execution should reach a stable state");
        inspected_execution = Some(execution.id);
    }
    store
        .create_provider_profile(ProviderProfileUpsert {
            id: "benchmark-provider".into(),
            display_name: "Benchmark provider".into(),
            provider_type: ProviderType::MiniMax,
            default_model: "MiniMax-M3".into(),
            allowed_models: vec!["MiniMax-M3".into()],
            capabilities: vec!["text-generation".into()],
            auth: ProviderAuth::ApiKeySecret {
                secret_ref: "benchmark-secret".into(),
            },
            config: serde_json::json!({}),
        })
        .expect("benchmark provider profile should be added");
    store
        .set_secret_versioned("benchmark-secret", "deterministic-benchmark-value")
        .expect("benchmark secret metadata should be seeded in the injected backend");
    store
        .create_named_mcp_connection(NewNamedMcpConnection {
            name: "benchmark-mcp".into(),
            transport: McpConnectionTransport::Http {
                endpoint: "https://mcp.example.test/mcp".into(),
                bearer_secret_ref: None,
            },
        })
        .expect("benchmark MCP metadata should be seeded without connecting");

    let contexts = fixture.path("benchmark-contexts.json");
    write_selected_context(&contexts, &core);
    let context_arg = path_arg(contexts);
    let env = [("KAKUNE_TOKEN", core.token.as_str())];
    let inspected_execution = inspected_execution.expect("benchmark execution should exist");
    let command_specs: Vec<(&str, Vec<&str>, &str)> = vec![
        (
            "workflow list",
            vec!["workflow", "list", "--limit", "100", "--json"],
            "json",
        ),
        (
            "execution list",
            vec!["execution", "list", "--limit", "100", "--json"],
            "json",
        ),
        (
            "trigger list",
            vec!["trigger", "list", "--limit", "100", "--json"],
            "json",
        ),
        (
            "provider list",
            vec!["provider", "list", "--limit", "100", "--json"],
            "json",
        ),
        (
            "MCP list",
            vec!["mcp", "list", "--limit", "100", "--json"],
            "json",
        ),
        (
            "secret list",
            vec!["secret", "list", "--limit", "100", "--json"],
            "json",
        ),
        (
            "workflow plan",
            vec!["workflow", "plan", "id:benchmark-workflow-000", "--json"],
            "json",
        ),
        (
            "workflow export",
            vec![
                "workflow",
                "export",
                "id:benchmark-workflow-000",
                "--output",
                "$OUTPUT",
                "--json",
            ],
            "json",
        ),
        (
            "execution inspect",
            vec!["execution", "inspect", "$EXECUTION", "--json"],
            "json",
        ),
        (
            "execution logs",
            vec![
                "execution",
                "logs",
                "$EXECUTION",
                "--limit",
                "200",
                "--json",
            ],
            "json",
        ),
        (
            "trigger inspect",
            vec![
                "trigger",
                "inspect",
                "benchmark-workflow-000/manual",
                "--json",
            ],
            "json",
        ),
        (
            "MCP inspect",
            vec!["mcp", "inspect", "benchmark-mcp", "--json"],
            "json",
        ),
    ];

    for (command_name, template, _) in &command_specs {
        let mut args = vec!["--context-file".to_owned(), context_arg.clone()];
        args.extend(template.iter().map(|argument| match *argument {
            "$OUTPUT" => path_arg(fixture.path(format!(
                "benchmark-exports/{}-preflight.yaml",
                command_name.replace(' ', "-").to_lowercase()
            ))),
            "$EXECUTION" => inspected_execution.clone(),
            value => value.to_owned(),
        }));
        let output = run(&fixture, &args, &env)
            .unwrap_or_else(|error| panic!("{command_name} preflight could not run: {error}"));
        assert_eq!(
            output.status.code(),
            Some(0),
            "{command_name} preflight failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let mut reports = Vec::new();
    for (command_name, template, output_mode) in command_specs {
        let mut warmups = Vec::with_capacity(WARMUP_COUNT);
        let mut samples = Vec::with_capacity(MEASURED_COUNT);
        let mut failures = Vec::new();
        let mut total_output_bytes = 0usize;
        let mut representative_argv = Vec::new();
        for invocation in 0..WARMUP_COUNT + MEASURED_COUNT {
            let export_path = fixture.path(format!(
                "benchmark-exports/{}-{invocation:03}.yaml",
                command_name.replace(' ', "-").to_lowercase()
            ));
            let mut args = vec!["--context-file".to_owned(), context_arg.clone()];
            args.extend(template.iter().map(|argument| match *argument {
                "$OUTPUT" => path_arg(export_path.clone()),
                "$EXECUTION" => inspected_execution.clone(),
                value => value.to_owned(),
            }));
            if invocation == 0 {
                representative_argv = args.clone();
            }
            let started = Instant::now();
            let output = run(&fixture, &args, &env);
            let elapsed = started.elapsed();
            if invocation < WARMUP_COUNT {
                warmups.push(elapsed);
            } else {
                // Failed launches/commands stay in the measured distribution.
                samples.push(elapsed);
            }
            match output {
                Ok(output) => {
                    total_output_bytes = total_output_bytes
                        .saturating_add(output.stdout.len())
                        .saturating_add(output.stderr.len());
                    if output.status.code() != Some(0) {
                        failures.push(format!(
                            "sample {} exited {:?}: {}",
                            invocation.saturating_sub(WARMUP_COUNT) + 1,
                            output.status.code(),
                            String::from_utf8_lossy(&output.stderr)
                        ));
                    }
                }
                Err(error) => failures.push(format!(
                    "sample {} could not be captured: {error}",
                    invocation.saturating_sub(WARMUP_COUNT) + 1
                )),
            }
        }
        reports.push((
            command_name,
            output_mode,
            representative_argv,
            warmups,
            samples,
            failures,
            total_output_bytes,
        ));
    }

    core.shutdown()
        .await
        .expect("benchmark Core should shut down after all commands");

    eprintln!(
        "SC-007 benchmark: workflows={WORKFLOW_COUNT}, executions={EXECUTION_COUNT}, warmups={WARMUP_COUNT}, measured_per_command={MEASURED_COUNT}, platform={}, Core=already-running isolated loopback, API_rate_limit=10000/min (to permit full sampling), excluded=native service managers, live providers, external MCP, macOS (theoretical)",
        std::env::consts::OS
    );
    for (name, output_mode, argv, warmups, samples, failures, output_bytes) in reports {
        let p95 = nearest_rank_p95(&samples);
        eprintln!(
            "command={name:?} output={output_mode} argv={argv:?} warmups_ms=[{}] measured={} failures={} output_bytes={} nearest_rank_p95_ms={:.3} target_lt_2000ms={}",
            format_milliseconds(&warmups),
            samples.len(),
            failures.len(),
            output_bytes,
            p95.as_secs_f64() * 1_000.0,
            p95 < Duration::from_secs(2)
        );
        for failure in &failures {
            eprintln!("  failure: {failure}");
        }
        assert!(
            failures.is_empty(),
            "{name} had command failures: {failures:?}"
        );
        assert_eq!(samples.len(), MEASURED_COUNT);
        assert_eq!(warmups.len(), WARMUP_COUNT);
    }
}
