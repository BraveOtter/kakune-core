mod support;

use std::process::Command;
use std::process::Stdio;

use support::lifecycle::LifecycleFixture;

fn kakune() -> Command {
    Command::new(env!("CARGO_BIN_EXE_kakune"))
}

#[test]
fn all_nine_public_lifecycle_command_help_pages_are_available() {
    let commands: [&[&str]; 9] = [
        &["daemon", "--help"],
        &["daemon", "start", "--help"],
        &["daemon", "status", "--help"],
        &["daemon", "stop", "--help"],
        &["service", "install", "--help"],
        &["service", "start", "--help"],
        &["service", "status", "--help"],
        &["service", "stop", "--help"],
        &["service", "uninstall", "--help"],
    ];

    for arguments in commands {
        let output = kakune()
            .args(arguments)
            .output()
            .expect("the CLI help command should run");
        assert!(
            output.status.success(),
            "`kakune {}` should succeed: {}",
            arguments.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("Usage:"), "missing usage for {arguments:?}");
    }
}

#[test]
fn all_nine_lifecycle_commands_keep_clap_usage_failures_nonzero_and_safe() {
    let commands: [&[&str]; 9] = [
        &["daemon", "--unexpected-option"],
        &["daemon", "start", "--unexpected-option"],
        &["daemon", "status", "--unexpected-option"],
        &["daemon", "stop", "--unexpected-option"],
        &["service", "install", "--unexpected-option"],
        &["service", "start", "--unexpected-option"],
        &["service", "status", "--unexpected-option"],
        &["service", "stop", "--unexpected-option"],
        &["service", "uninstall", "--unexpected-option"],
    ];

    for arguments in commands {
        let output = kakune()
            .args(arguments)
            .stdin(Stdio::null())
            .output()
            .expect("CLI argument validation should run without lifecycle side effects");
        assert_eq!(
            output.status.code(),
            Some(2),
            "`kakune {}` should return Clap's usage exit code",
            arguments.join(" ")
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("Usage:"), "missing usage for {arguments:?}");
        assert!(
            stderr.contains("unexpected-option"),
            "missing argument diagnostic for {arguments:?}: {stderr}"
        );
    }
}

#[test]
fn service_help_explains_scope_deadlines_and_uncancellable_manager_work() {
    let output = kakune()
        .args(["service", "--help"])
        .output()
        .expect("the service help command should run");
    assert!(output.status.success());
    let help = String::from_utf8_lossy(&output.stdout);
    for required in [
        "machine-wide",
        "10 seconds",
        "30 seconds",
        "50/50/60/90 seconds",
        "not wall-clock guarantees",
        "performance goals",
        "systemd",
        "theoretical",
        "owner-scoped capability",
        "TCP peer's OS account",
    ] {
        assert!(
            help.contains(required),
            "service help is missing {required:?}"
        );
    }
}

#[test]
fn remote_context_is_rejected_before_service_state_or_context_access() {
    let missing_context = std::env::temp_dir().join(format!(
        "kakune-diagnostics-context-{}.json",
        uuid::Uuid::new_v4()
    ));
    let output = kakune()
        .args([
            "--context-file",
            missing_context.to_str().expect("temp path is UTF-8"),
            "--context",
            "remote",
            "service",
            "status",
        ])
        .output()
        .expect("the scope validation command should run");

    assert!(!output.status.success());
    let diagnostic = String::from_utf8_lossy(&output.stderr);
    assert!(diagnostic.contains("service lifecycle commands are local"));
    assert!(
        !missing_context.exists(),
        "scope rejection must not load or create a context file"
    );
}

#[test]
fn lifecycle_help_distinguishes_user_daemon_from_machine_service_scope() {
    let daemon = kakune()
        .args(["daemon", "--help"])
        .output()
        .expect("daemon help should run without starting Core");
    let service = kakune()
        .args(["service", "--help"])
        .output()
        .expect("service help should run without querying the manager");
    let daemon_help = String::from_utf8_lossy(&daemon.stdout);
    let service_help = String::from_utf8_lossy(&service.stdout);

    assert!(daemon.status.success());
    assert!(service.status.success());
    assert!(daemon_help.contains("current-user"));
    assert!(service_help.contains("machine-wide"));
    assert!(service_help.contains("LocalSystem") || service_help.contains("root"));
}

#[test]
fn clap_usage_errors_remain_nonzero_and_do_not_contact_the_service_manager() {
    let output = kakune()
        .args(["service", "install", "--unexpected-option"])
        .stdin(Stdio::null())
        .output()
        .expect("argument validation should run without manager access");

    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("Usage:"));
    assert!(String::from_utf8_lossy(&output.stderr).contains("unexpected-option"));
}

#[test]
fn absent_daemon_stop_is_successful_and_does_not_create_its_data_directory() {
    let fixture = LifecycleFixture::new().expect("isolated fixture should be created");
    let absent_data = fixture.root().join("not-created");
    let output = kakune()
        .args([
            "daemon",
            "--data-dir",
            absent_data.to_str().expect("fixture path is UTF-8"),
            "stop",
        ])
        .stdin(Stdio::null())
        .output()
        .expect("absent daemon stop should run without configuration");

    assert!(
        output.status.success(),
        "absent daemon stop should be a no-op: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!absent_data.exists());
}

#[test]
fn corrupt_daemon_status_is_unknown_nonzero_and_preserves_evidence() {
    let fixture = LifecycleFixture::new().expect("isolated fixture should be created");
    let data = fixture.root().join("corrupt-data");
    let state = data.join(".kakune");
    std::fs::create_dir_all(&state).expect("isolated state should be created");
    let record = state.join("ownership.json");
    std::fs::write(&record, b"not valid JSON").expect("corrupt evidence should be written");
    let output = kakune()
        .args(["daemon", "--data-dir"])
        .arg(&data)
        .arg("status")
        .output()
        .expect("read-only status should run");
    assert!(!output.status.success());
    let diagnostic = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(diagnostic.contains("unknown"), "{diagnostic}");
    assert!(!diagnostic.contains("state=stopped"), "{diagnostic}");
    assert_eq!(std::fs::read(record).unwrap(), b"not valid JSON");
}
