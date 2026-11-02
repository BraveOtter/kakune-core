use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

mod support;
use support::management::ManagementFixture;

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn invoke(fixture: &ManagementFixture, arguments: &[String]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_kakune"))
        .args(arguments)
        .env_remove("KAKUNE_DATA_DIR")
        .env_remove("KAKUNE_ENDPOINT")
        .env_remove("KAKUNE_TOKEN")
        .current_dir(fixture.root())
        .output()
        .expect("Kakune CLI should run")
}

fn help(fixture: &ManagementFixture, command: &[&str]) -> String {
    let mut arguments = command.to_vec();
    arguments.push("--help");
    let output = invoke(fixture, &strings(&arguments));
    assert_eq!(output.status.code(), Some(0), "{command:?} help failed");
    assert!(output.stderr.is_empty());
    String::from_utf8(output.stdout).expect("help output should be UTF-8")
}

fn help_names(text: &str) -> Vec<String> {
    let listing = text
        .split_once("Commands:")
        .map(|(_, listing)| listing.split("Options:").next().unwrap_or(listing))
        .unwrap_or("");
    listing
        .lines()
        .filter_map(|line| line.split_whitespace().next())
        .filter(|name| *name != "help")
        .map(str::to_owned)
        .collect()
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SnapshotEntry {
    Directory,
    File(Vec<u8>),
}

fn snapshot(root: &Path) -> std::io::Result<BTreeMap<PathBuf, SnapshotEntry>> {
    fn visit(
        root: &Path,
        directory: &Path,
        entries: &mut BTreeMap<PathBuf, SnapshotEntry>,
    ) -> std::io::Result<()> {
        for item in fs::read_dir(directory)? {
            let item = item?;
            let path = item.path();
            let relative = path
                .strip_prefix(root)
                .expect("entry is below fixture root");
            let kind = item.file_type()?;
            if kind.is_dir() {
                entries.insert(relative.to_path_buf(), SnapshotEntry::Directory);
                visit(root, &path, entries)?;
            } else if kind.is_file() {
                entries.insert(relative.to_path_buf(), SnapshotEntry::File(fs::read(path)?));
            }
        }
        Ok(())
    }

    let mut entries = BTreeMap::new();
    visit(root, root, &mut entries)?;
    Ok(entries)
}

fn path_arg(path: PathBuf) -> String {
    path.to_string_lossy().into_owned()
}

fn assert_rejected_without_side_effects(fixture: &ManagementFixture, arguments: &[String]) {
    let before = snapshot(fixture.root()).expect("fixture should be snapshot-able");
    let output = invoke(fixture, arguments);
    assert_eq!(
        output.status.code(),
        Some(2),
        "removed command should be a parser error: {:?}\nstdout={}\nstderr={}",
        arguments,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.is_empty(), "rejected commands must not act");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("error:"),
        "parser error should be explicit"
    );
    assert_eq!(
        snapshot(fixture.root()).expect("fixture should remain snapshot-able"),
        before,
        "rejected command changed fixture state: {arguments:?}"
    );
}

#[test]
fn recursive_help_matches_the_canonical_tree_and_has_no_public_aliases() {
    let fixture = ManagementFixture::new().expect("isolated fixture should be created");
    let inventory: &[(&[&str], &[&str])] = &[
        (
            &[],
            &[
                "init",
                "version",
                "daemon",
                "service",
                "doctor",
                "context",
                "workflow",
                "execution",
                "trigger",
                "plugin",
                "provider",
                "secret",
                "mcp",
            ],
        ),
        (&["daemon"], &["start", "stop", "status"]),
        (
            &["service"],
            &["install", "uninstall", "start", "stop", "status"],
        ),
        (
            &["context"],
            &[
                "add", "list", "use", "inspect", "remove", "import", "export",
            ],
        ),
        (
            &["workflow"],
            &[
                "create", "import", "export", "validate", "plan", "list", "run", "enable",
                "disable",
            ],
        ),
        (&["execution"], &["list", "inspect", "cancel", "logs"]),
        (&["trigger"], &["list", "inspect"]),
        (
            &["plugin"],
            &[
                "create", "inspect", "install", "list", "update", "remove", "doctor", "test",
            ],
        ),
        (&["provider"], &["add", "list", "auth", "test", "models"]),
        (&["secret"], &["set", "list", "remove"]),
        (&["mcp"], &["add", "list", "inspect", "auth", "tool"]),
        (&["mcp", "tool"], &["list", "call"]),
    ];

    for (command, expected) in inventory {
        let text = help(&fixture, command);
        let actual = help_names(&text);
        assert_eq!(
            actual, *expected,
            "public command tree differs at {command:?}"
        );
        assert!(
            !text.to_ascii_lowercase().contains("alias:"),
            "help for {command:?} advertises an alias"
        );
    }

    let root = help(&fixture, &[]);
    let service = help(&fixture, &["service"]);
    assert!(!root.contains("service run"));
    assert!(!service.contains("run"));

    let short_help = invoke(&fixture, &strings(&["-h"]));
    assert_eq!(short_help.status.code(), Some(0));
    assert_eq!(short_help.stdout, root.as_bytes());
    assert!(short_help.stderr.is_empty());

    let command_help = invoke(&fixture, &strings(&["help", "workflow", "run"]));
    assert_eq!(command_help.status.code(), Some(0));
    assert_eq!(
        command_help.stdout,
        help(&fixture, &["workflow", "run"]).as_bytes()
    );

    let version = format!("kakune {}\n", env!("CARGO_PKG_VERSION"));
    for arguments in [&["version"][..], &["--version"][..]] {
        let output = invoke(&fixture, &strings(arguments));
        assert_eq!(output.status.code(), Some(0));
        assert_eq!(output.stdout, version.as_bytes());
        assert!(output.stderr.is_empty());
    }
}

#[test]
fn every_inventoried_removed_public_path_is_rejected_before_io() {
    let fixture = ManagementFixture::new().expect("isolated fixture should be created");
    let data = path_arg(fixture.path("would-be-data"));
    let config = path_arg(fixture.path("would-be-config.yaml"));
    let workflow = path_arg(fixture.path("workflow.kakune.yaml"));
    let provider = path_arg(fixture.path("provider.json"));
    let mcp = path_arg(fixture.path("mcp.json"));
    let backup = path_arg(fixture.path("backup.tar.gz"));
    let restore = path_arg(fixture.path("restore.tar.gz"));
    let restored = path_arg(fixture.path("restored-data"));
    let plugin = path_arg(fixture.path("plugin-package"));

    fs::write(&workflow, b"not executed\n").expect("workflow sentinel should be written");
    fs::write(&provider, b"not read\n").expect("provider sentinel should be written");
    fs::write(&mcp, b"not read\n").expect("MCP sentinel should be written");
    let candidates = vec![
        strings(&["run", &workflow]),
        strings(&["executions", "--data-dir", &data]),
        strings(&["inspect", "fixture-execution", "--data-dir", &data]),
        strings(&["auth"]),
        strings(&["auth", "recover", "--data-dir", &data, "--config", &config]),
        strings(&["auth", "pair", "--data-dir", &data, "--ttl-seconds", "1"]),
        strings(&["auth", "tokens", "--data-dir", &data]),
        strings(&["auth", "revoke", "fixture-token", "--data-dir", &data]),
        strings(&["storage"]),
        strings(&["storage", "backup", &backup, "--data-dir", &data]),
        strings(&["storage", "restore", &restore, &restored]),
        strings(&["storage", "retain", "--data-dir", &data]),
        strings(&["storage", "compact", "--data-dir", &data]),
        strings(&["storage", "pin", "fixture-execution", "--value", "true"]),
        strings(&["provider", "upsert", &provider, "--data-dir", &data]),
        strings(&[
            "provider",
            "status",
            "fixture-provider",
            "--data-dir",
            &data,
        ]),
        strings(&[
            "provider",
            "remove",
            "fixture-provider",
            "--data-dir",
            &data,
        ]),
        strings(&["provider", "login", "fixture-provider", "--data-dir", &data]),
        strings(&["mcp", "call", &mcp, "--data-dir", &data]),
        strings(&["plugin", "prepare", &plugin, "--data-dir", &data]),
        strings(&[
            "plugin",
            "commit",
            "fixture-prepared",
            "--digest",
            "sha256:fixture",
            "--data-dir",
            &data,
        ]),
    ];

    for arguments in candidates {
        assert_rejected_without_side_effects(&fixture, &arguments);
    }
}
