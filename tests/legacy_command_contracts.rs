mod support;

use std::{
    fs,
    path::PathBuf,
    process::{Command, Output},
};

use support::management::{FilesystemSnapshot, ManagementFixture};

fn invoke(fixture: &ManagementFixture, arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_kakune"))
        .args(arguments)
        .env("KAKUNE_DATA_DIR", fixture.path("existing-core"))
        .env_remove("KAKUNE_ENDPOINT")
        .env_remove("KAKUNE_TOKEN")
        .current_dir(fixture.root())
        .output()
        .expect("Kakune CLI should run")
}

fn path_arg(path: PathBuf) -> String {
    path.to_string_lossy().into_owned()
}

fn assert_rejected_without_side_effects(
    fixture: &ManagementFixture,
    snapshot: &FilesystemSnapshot,
    arguments: &[String],
) {
    let borrowed = arguments.iter().map(String::as_str).collect::<Vec<_>>();
    let output = invoke(fixture, &borrowed);
    assert_eq!(
        output.status.code(),
        Some(2),
        "removed command should fail argument parsing: {arguments:?}\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.is_empty(), "rejected command must not act");
    assert!(String::from_utf8_lossy(&output.stderr).contains("error:"));
    snapshot
        .ensure_unchanged(fixture.root())
        .expect("rejected command must preserve all fixture data");
}

#[test]
fn removed_cli_contracts_are_rejected_without_replacing_canonical_operations() {
    let fixture = ManagementFixture::new().expect("isolated fixture should be created");
    let workflow = path_arg(fixture.path("existing workflow.yaml"));
    let provider = path_arg(fixture.path("provider profile.json"));
    let mcp = path_arg(fixture.path("one-shot mcp.json"));
    let data = path_arg(fixture.path("existing-core"));
    let backup = path_arg(fixture.path("backup.sqlite3"));
    let restored = path_arg(fixture.path("restored-core"));
    let plugin = path_arg(fixture.path("plugin-package"));
    for (path, contents) in [
        (&workflow, "not executed\n"),
        (&provider, "not read\n"),
        (&mcp, "not read\n"),
    ] {
        fs::write(path, contents).expect("sentinel input should be written");
    }

    let snapshot = FilesystemSnapshot::capture(fixture.root()).unwrap();
    let removed = vec![
        vec!["run".to_owned(), workflow.clone()],
        vec![
            "executions".to_owned(),
            "--data-dir".to_owned(),
            data.clone(),
        ],
        vec!["inspect".to_owned(), "execution-1".to_owned()],
        vec!["auth".to_owned()],
        vec!["auth".to_owned(), "recover".to_owned()],
        vec!["auth".to_owned(), "pair".to_owned()],
        vec!["auth".to_owned(), "tokens".to_owned()],
        vec!["auth".to_owned(), "revoke".to_owned(), "token-1".to_owned()],
        vec!["storage".to_owned()],
        vec!["storage".to_owned(), "backup".to_owned(), backup.clone()],
        vec!["storage".to_owned(), "restore".to_owned(), backup, restored],
        vec!["storage".to_owned(), "retain".to_owned()],
        vec!["storage".to_owned(), "compact".to_owned()],
        vec![
            "storage".to_owned(),
            "pin".to_owned(),
            "execution-1".to_owned(),
        ],
        vec!["provider".to_owned(), "upsert".to_owned(), provider],
        vec![
            "provider".to_owned(),
            "status".to_owned(),
            "provider-1".to_owned(),
        ],
        vec![
            "provider".to_owned(),
            "remove".to_owned(),
            "provider-1".to_owned(),
        ],
        vec![
            "provider".to_owned(),
            "login".to_owned(),
            "provider-1".to_owned(),
        ],
        vec!["mcp".to_owned(), "call".to_owned(), mcp],
        vec!["plugin".to_owned(), "prepare".to_owned(), plugin.clone()],
        vec![
            "plugin".to_owned(),
            "commit".to_owned(),
            "prepared-1".to_owned(),
            "--digest".to_owned(),
            "sha256:fixture".to_owned(),
        ],
    ];

    for arguments in &removed {
        assert_rejected_without_side_effects(&fixture, &snapshot, arguments);
    }

    // The old host argv is deliberately not part of the public-path rejection list.
    let service_help = invoke(&fixture, &["service", "--help"]);
    assert!(service_help.status.success());
    assert!(!String::from_utf8_lossy(&service_help.stdout).contains("run"));
    let host_help = invoke(&fixture, &["service", "run", "--help"]);
    assert!(host_help.status.success());
    assert!(String::from_utf8_lossy(&host_help.stdout).contains("--data-dir"));

    // Existing canonical paths remain parsed as prescribed command families.
    for arguments in [
        &["workflow", "run", "stored-id"][..],
        &["execution", "list"][..],
        &["provider", "add", "provider-id", "--file", "profile.json"][..],
        &[
            "mcp",
            "tool",
            "call",
            "connection",
            "tool",
            "--input",
            "args.json",
        ][..],
    ] {
        assert!(
            invoke(&fixture, arguments).status.code() != Some(2),
            "canonical command unexpectedly rejected: {arguments:?}"
        );
    }
}
