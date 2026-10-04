use std::{
    collections::BTreeMap,
    fs,
    io::{self, Read},
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

struct CommandFixture {
    root: PathBuf,
}

impl CommandFixture {
    fn new() -> io::Result<Self> {
        let approved_temp_root = std::env::temp_dir().join("opencode");
        fs::create_dir_all(&approved_temp_root)?;
        let root =
            approved_temp_root.join(format!("kakune-general-commands-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root)?;
        Ok(Self { root })
    }

    fn path(&self) -> &Path {
        &self.root
    }
}

impl Drop for CommandFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

struct CommandOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

fn run_kakune(
    fixture: &CommandFixture,
    arguments: &[&str],
    timeout: Duration,
) -> io::Result<CommandOutput> {
    run_kakune_with_env(fixture, arguments, &[], timeout)
}

fn run_kakune_with_env(
    fixture: &CommandFixture,
    arguments: &[&str],
    environment: &[(&str, &str)],
    timeout: Duration,
) -> io::Result<CommandOutput> {
    if timeout.is_zero() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "child timeout must be finite and nonzero",
        ));
    }

    let output_id = uuid::Uuid::new_v4();
    let stdout_path = fixture.path().join(format!("child-{output_id}.stdout"));
    let stderr_path = fixture.path().join(format!("child-{output_id}.stderr"));
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
        .args(arguments)
        .envs(environment.iter().copied())
        .current_dir(fixture.path())
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    let mut child = command.spawn()?;

    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if started.elapsed() >= timeout {
            let _ = child.kill();
            let _ = child.wait();
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("kakune exceeded the {timeout:?} test deadline"),
            ));
        }
        thread::sleep(Duration::from_millis(10));
    };

    let mut stdout = Vec::new();
    fs::File::open(stdout_path)?.read_to_end(&mut stdout)?;
    let mut stderr = Vec::new();
    fs::File::open(stderr_path)?.read_to_end(&mut stderr)?;
    Ok(CommandOutput {
        status,
        stdout,
        stderr,
    })
}

#[derive(Debug, PartialEq, Eq)]
enum SnapshotEntry {
    Directory,
    File(Vec<u8>),
}

fn snapshot_tree(path: &Path) -> io::Result<BTreeMap<PathBuf, SnapshotEntry>> {
    fn visit(
        root: &Path,
        current: &Path,
        snapshot: &mut BTreeMap<PathBuf, SnapshotEntry>,
    ) -> io::Result<()> {
        let relative = current
            .strip_prefix(root)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
        let metadata = fs::symlink_metadata(current)?;
        if metadata.is_dir() {
            snapshot.insert(relative.to_path_buf(), SnapshotEntry::Directory);
            for entry in fs::read_dir(current)? {
                visit(root, &entry?.path(), snapshot)?;
            }
        } else if metadata.is_file() {
            snapshot.insert(
                relative.to_path_buf(),
                SnapshotEntry::File(fs::read(current)?),
            );
        }
        Ok(())
    }

    let mut snapshot = BTreeMap::new();
    if path.exists() {
        visit(path, path, &mut snapshot)?;
    }
    Ok(snapshot)
}

fn assert_version_output(output: &CommandOutput) {
    let expected = format!("kakune {}\n", env!("CARGO_PKG_VERSION"));
    assert!(output.status.success());
    assert_eq!(output.stdout, expected.as_bytes());
    assert!(output.stderr.is_empty());
}

fn wait_for_marker(marker: &Path, timeout: Duration) -> io::Result<()> {
    if timeout.is_zero() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "synchronization timeout must be finite and nonzero",
        ));
    }

    let started = Instant::now();
    while started.elapsed() < timeout {
        if marker.is_file() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(10));
    }
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        "test synchronization marker was not published before its deadline",
    ))
}

#[test]
fn executable_helper_captures_output_and_keeps_child_lifetime_bounded() {
    let fixture = CommandFixture::new().expect("command fixture should be created");
    let marker = fixture.path().join("ready.marker");
    fs::write(&marker, b"ready").expect("fixture marker should be written");
    wait_for_marker(&marker, Duration::from_secs(1))
        .expect("bounded synchronization helper should see the marker");

    let output = run_kakune(&fixture, &["--help"], Duration::from_secs(10))
        .expect("help child should exit before its deadline");
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("Usage:"));
    assert!(output.stderr.is_empty());
}

#[test]
fn general_help_discloses_safe_local_entry_points_and_retains_other_commands() {
    let fixture = CommandFixture::new().expect("command fixture should be created");

    let top_level = run_kakune(&fixture, &["--help"], Duration::from_secs(10))
        .expect("top-level help should be bounded");
    assert!(top_level.status.success());
    assert!(top_level.stderr.is_empty());
    let top_level = String::from_utf8(top_level.stdout).expect("help should be UTF-8");
    assert!(
        top_level.contains("--version"),
        "built-in version flag is missing"
    );
    let command_section = top_level
        .split_once("Commands:")
        .and_then(|(_, rest)| rest.split_once("Options:"))
        .map(|(commands, _)| commands)
        .expect("top-level help should list commands and options");
    let command_names = command_section
        .lines()
        .filter_map(|line| line.split_whitespace().next())
        .collect::<Vec<_>>();
    assert_eq!(
        command_names,
        [
            "init",
            "version",
            "daemon",
            "storage",
            "service",
            "doctor",
            "context",
            "auth",
            "workflow",
            "plugin",
            "provider",
            "mcp",
            "run",
            "executions",
            "inspect",
            "help",
        ]
    );
    assert_no_subcommand(&top_level, "setup");

    let init = run_kakune(&fixture, &["init", "--help"], Duration::from_secs(10))
        .expect("init help should be bounded");
    assert!(init.status.success());
    let init = String::from_utf8(init.stdout).expect("init help should be UTF-8");
    let init = init.to_lowercase();
    for detail in [
        "local-only",
        "does not accept --context",
        "never starts the core",
        "--standalone",
        "--ca",
        "safe to repeat",
        "preserves",
        "auth recover",
        "does not rotate",
    ] {
        assert!(init.contains(detail), "init help omitted {detail}:\n{init}");
    }

    let version = run_kakune(&fixture, &["version", "--help"], Duration::from_secs(10))
        .expect("version help should be bounded");
    assert!(version.status.success());
    let version = String::from_utf8(version.stdout).expect("version help should be UTF-8");
    let version = version.to_lowercase();
    for detail in [
        "local executable",
        "build version",
        "does not access installation",
    ] {
        assert!(
            version.contains(detail),
            "version help omitted {detail}:\n{version}"
        );
    }

    for (arguments, expected) in [
        (&["doctor", "--help"][..], "diagnostics"),
        (&["daemon", "--help"][..], "foreground"),
        (
            &["auth", "recover", "--help"][..],
            "Explicitly recovers lost local access",
        ),
    ] {
        let help = run_kakune(&fixture, arguments, Duration::from_secs(10))
            .expect("retained-command help should be bounded");
        assert!(help.status.success());
        let help = String::from_utf8(help.stdout).expect("retained-command help should be UTF-8");
        assert!(help.contains(expected), "help omitted {expected}:\n{help}");
    }

    let retained_groups: &[(&[&str], &[&str])] = &[
        (&["daemon", "--help"], &["start", "stop", "status"]),
        (
            &["storage", "--help"],
            &["backup", "restore", "retain", "compact", "pin"],
        ),
        (
            &["service", "--help"],
            &["install", "uninstall", "start", "stop", "status"],
        ),
        (
            &["context", "--help"],
            &[
                "add", "list", "use", "inspect", "remove", "import", "export",
            ],
        ),
        (
            &["auth", "--help"],
            &["recover", "pair", "tokens", "revoke"],
        ),
        (
            &["workflow", "--help"],
            &["validate", "list", "enable", "disable"],
        ),
        (
            &["plugin", "--help"],
            &["prepare", "commit", "list", "remove"],
        ),
        (
            &["provider", "--help"],
            &["list", "upsert", "status", "remove", "login"],
        ),
        (&["mcp", "--help"], &["call"]),
    ];
    for (arguments, commands) in retained_groups {
        let output = run_kakune(&fixture, arguments, Duration::from_secs(10))
            .expect("retained command-group help should be bounded");
        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        let help = String::from_utf8(output.stdout).expect("command help should be UTF-8");
        for command in *commands {
            assert_subcommand_listed(&help, command);
        }
    }
}

fn assert_subcommand_listed(help: &str, subcommand: &str) {
    let is_listed = help.lines().any(|line| {
        line.split_whitespace()
            .next()
            .is_some_and(|name| name == subcommand)
    });
    assert!(
        is_listed,
        "help does not list the {subcommand} subcommand:\n{help}"
    );
}

fn assert_no_subcommand(help: &str, subcommand: &str) {
    let has_subcommand = help.lines().any(|line| {
        let line = line.trim_start();
        line.strip_prefix(subcommand)
            .is_some_and(|rest| rest.starts_with(char::is_whitespace) || rest.starts_with(','))
    });
    assert!(
        !has_subcommand,
        "help unexpectedly advertises the {subcommand} subcommand:\n{help}"
    );
}

#[test]
fn init_rejects_remote_context_before_side_effects_with_globals_before_and_after() {
    for globals_after_subcommand in [false, true] {
        let fixture = CommandFixture::new().expect("command fixture should be created");
        let data = fixture.path().join("remote-init-data");
        let config = fixture.path().join("remote-init-config.yaml");
        let contexts = fixture.path().join("remote-contexts.json");
        let data_string = data.to_string_lossy().into_owned();
        let config_string = config.to_string_lossy().into_owned();
        let contexts_string = contexts.to_string_lossy().into_owned();
        let args = if globals_after_subcommand {
            vec![
                "init".to_string(),
                "--data-dir".to_string(),
                data_string,
                "--config".to_string(),
                config_string,
                "--context".to_string(),
                "remote".to_string(),
                "--context-file".to_string(),
                contexts_string,
            ]
        } else {
            vec![
                "--context".to_string(),
                "remote".to_string(),
                "--context-file".to_string(),
                contexts_string,
                "init".to_string(),
                "--data-dir".to_string(),
                data_string,
                "--config".to_string(),
                config_string,
            ]
        };
        let args = args.iter().map(String::as_str).collect::<Vec<_>>();

        let output = run_kakune(&fixture, &args, Duration::from_secs(10))
            .expect("local-only rejection should be bounded");

        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("local-only"), "unexpected stderr: {stderr}");
        assert!(!data.exists());
        assert!(!config.exists());
        assert!(!contexts.exists());
        assert!(
            !fixture
                .path()
                .join("remote-init-data")
                .join("kakune.sqlite3")
                .exists()
        );
    }
}

#[test]
fn init_accepts_standalone_and_ca_without_reading_the_ca_file() {
    let fixture = CommandFixture::new().expect("command fixture should be created");
    let data_file = fixture.path().join("data-is-a-file");
    fs::write(&data_file, b"preserve data path").expect("blocking data file should be written");
    let config = fixture.path().join("init-config.yaml");
    let contexts = fixture.path().join("init-contexts.json");
    let missing_ca = fixture.path().join("does-not-exist.pem");
    let output = run_kakune(
        &fixture,
        &[
            "--standalone",
            "--ca",
            missing_ca.to_str().expect("fixture path is UTF-8"),
            "init",
            "--data-dir",
            data_file.to_str().expect("fixture path is UTF-8"),
            "--config",
            config.to_str().expect("fixture path is UTF-8"),
            "--context-file",
            contexts.to_str().expect("fixture path is UTF-8"),
        ],
        Duration::from_secs(10),
    )
    .expect("accepted compatibility options should not hang");

    assert!(
        !output.status.success(),
        "the file-valued data path must fail"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("initialization incomplete"));
    assert!(!stderr.contains("does-not-exist.pem"));
    assert!(!stderr.contains("cannot read CA"));
    assert!(!contexts.exists());
    assert_eq!(fs::read(data_file).unwrap(), b"preserve data path");
}

#[test]
fn invalid_init_arguments_remain_ordinary_parser_errors_without_side_effects() {
    let fixture = CommandFixture::new().expect("command fixture should be created");
    let data = fixture.path().join("invalid-args-data");
    let config = fixture.path().join("invalid-args.yaml");
    let output = run_kakune(
        &fixture,
        &[
            "init",
            "--data-dir",
            data.to_str().expect("fixture path is UTF-8"),
            "--config",
            config.to_str().expect("fixture path is UTF-8"),
            "--unexpected",
        ],
        Duration::from_secs(10),
    )
    .expect("argument error should be bounded");

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("unexpected"));
    assert!(!data.exists());
    assert!(!config.exists());
}

#[test]
fn version_forms_match_exactly_without_local_state_or_remote_access() {
    let fixture = CommandFixture::new().expect("command fixture should be created");
    let data = fixture.path().join("fresh local data");
    let data_string = data.to_string_lossy().into_owned();

    for arguments in [&["version"][..], &["--version"][..]] {
        let output = run_kakune_with_env(
            &fixture,
            arguments,
            &[("KAKUNE_DATA_DIR", &data_string)],
            Duration::from_secs(2),
        )
        .expect("version command should finish before its deadline");
        assert_version_output(&output);
        assert!(!data.exists(), "version must not create the data directory");
    }

    fs::create_dir_all(&data).expect("malformed config fixture should be created");
    let config = data.join("kakune.yaml");
    fs::write(&config, b"not: [valid yaml").expect("malformed config should be written");
    let config_snapshot = snapshot_tree(&data).expect("config fixture should be snapshotted");
    for arguments in [&["version"][..], &["--version"][..]] {
        let output = run_kakune_with_env(
            &fixture,
            arguments,
            &[("KAKUNE_DATA_DIR", &data_string)],
            Duration::from_secs(2),
        )
        .expect("version must not load local configuration");
        assert_version_output(&output);
        assert_eq!(
            snapshot_tree(&data).expect("config fixture should remain readable"),
            config_snapshot,
            "version must not change the malformed configuration or create local state"
        );
    }

    let contexts_dir = fixture.path().join("malformed-context");
    fs::create_dir(&contexts_dir).expect("context fixture directory should be created");
    let contexts = contexts_dir.join("contexts.json");
    fs::write(&contexts, b"{malformed json").expect("malformed context should be written");
    let contexts_string = contexts.to_string_lossy().into_owned();
    let contexts_snapshot =
        snapshot_tree(&contexts_dir).expect("malformed context fixture should be snapshotted");
    let malformed_context_args: &[&[&str]] = &[
        &[
            "--context",
            "remote",
            "--context-file",
            &contexts_string,
            "version",
        ],
        &[
            "version",
            "--context",
            "remote",
            "--context-file",
            &contexts_string,
        ],
        &[
            "--context",
            "remote",
            "--context-file",
            &contexts_string,
            "--version",
        ],
    ];
    for arguments in malformed_context_args {
        let output = run_kakune(&fixture, arguments, Duration::from_secs(2))
            .expect("version must return before loading selected context metadata");
        assert_version_output(&output);
        assert_eq!(
            snapshot_tree(&contexts_dir).expect("context fixture should remain readable"),
            contexts_snapshot,
            "version must not read-modify-write or replace malformed context metadata"
        );
    }

    let unreachable_dir = fixture.path().join("unreachable-context");
    fs::create_dir(&unreachable_dir).expect("remote context directory should be created");
    let unreachable_contexts = unreachable_dir.join("contexts.json");
    fs::write(
        &unreachable_contexts,
        serde_json::to_vec(&serde_json::json!({
            "format": "kakune-contexts/v1",
            "exportedAt": "2026-10-01T12:00:00Z",
            "activeContextId": "remote",
            "contexts": [{
                "id": "remote",
                "name": "Unavailable Core",
                "endpoint": "http://127.0.0.1:1",
                "credentialRef": "env:KAKUNE_TOKEN"
            }]
        }))
        .expect("remote context should serialize"),
    )
    .expect("unreachable Core context should be written");
    let unreachable_contexts_string = unreachable_contexts.to_string_lossy().into_owned();
    let missing_ca = fixture.path().join("missing-ca.pem");
    let missing_ca_string = missing_ca.to_string_lossy().into_owned();
    let unreachable_snapshot =
        snapshot_tree(&unreachable_dir).expect("unreachable context should be snapshotted");
    let unreachable_args: &[&[&str]] = &[
        &[
            "--context",
            "remote",
            "--context-file",
            &unreachable_contexts_string,
            "--ca",
            &missing_ca_string,
            "version",
        ],
        &[
            "version",
            "--context",
            "remote",
            "--context-file",
            &unreachable_contexts_string,
            "--ca",
            &missing_ca_string,
        ],
        &[
            "--context",
            "remote",
            "--context-file",
            &unreachable_contexts_string,
            "--ca",
            &missing_ca_string,
            "--version",
        ],
    ];
    for arguments in unreachable_args {
        let output = run_kakune(&fixture, arguments, Duration::from_secs(2))
            .expect("version must return without contacting the selected Core");
        assert_version_output(&output);
        assert_eq!(
            snapshot_tree(&unreachable_dir).expect("remote context should remain readable"),
            unreachable_snapshot,
            "version must not modify selected remote metadata"
        );
        assert!(
            !missing_ca.exists(),
            "version must not create or load the CA file"
        );
    }
}

#[test]
fn version_rejects_unsupported_arguments_and_keeps_parser_conflicts() {
    let fixture = CommandFixture::new().expect("command fixture should be created");
    let data_dir = fixture.path().join("unsupported-data-dir");
    let config = fixture.path().join("unsupported-config.yaml");
    let data_dir_string = data_dir.to_string_lossy().into_owned();
    let config_string = config.to_string_lossy().into_owned();
    let unsupported_arguments: &[&[&str]] = &[
        &["version", "--data-dir", &data_dir_string],
        &["version", "--config", &config_string],
        &["version", "extra-argument"],
        &["version", "--standalone", "--context", "remote"],
    ];

    for arguments in unsupported_arguments {
        let output = run_kakune(&fixture, arguments, Duration::from_secs(10))
            .expect("ordinary parser errors should be bounded");
        assert!(
            !output.status.success(),
            "unsupported or conflicting arguments should remain parser errors: {arguments:?}"
        );
        assert!(output.stdout.is_empty());
        assert!(!output.stderr.is_empty());
        assert!(!data_dir.exists());
        assert!(!config.exists());
    }

    // Clap may short-circuit parser conflict validation for its built-in --version action.
    // Preserve that existing behavior without requiring it to match the subcommand path.
    let flag_conflict = run_kakune(
        &fixture,
        &["--standalone", "--context", "remote", "--version"],
        Duration::from_secs(10),
    )
    .expect("the built-in version action should be bounded");
    assert!(
        flag_conflict.status.success() || !flag_conflict.stderr.is_empty(),
        "a built-in version conflict must retain Clap's ordinary success-or-error behavior"
    );
    assert!(!data_dir.exists());
    assert!(!config.exists());
}
