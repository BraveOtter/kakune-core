use std::{
    fs,
    path::PathBuf,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use kakune_core::lifecycle::service::{
    ServiceDefinition, ServiceManagerKind, render_service_definition,
};
use serde_json::Value;

fn fixture_dir() -> PathBuf {
    let root = std::env::temp_dir().join("opencode");
    fs::create_dir_all(&root).expect("approved temporary directory should exist");
    let path = root.join(format!("canonical-service-host-{}", uuid::Uuid::new_v4()));
    fs::create_dir(&path).expect("isolated service fixture should be created");
    path
}

fn run_cli(arguments: &[&str]) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_kakune"))
        .args(arguments)
        .env_remove("KAKUNE_DATA_DIR")
        .env_remove("KAKUNE_ENDPOINT")
        .env_remove("KAKUNE_TOKEN")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Kakune CLI should spawn");
    let start = Instant::now();
    loop {
        if child
            .try_wait()
            .expect("child process should be observable")
            .is_some()
        {
            return child
                .wait_with_output()
                .expect("CLI output should be captured");
        }
        if start.elapsed() >= Duration::from_secs(10) {
            let _ = child.kill();
            let _ = child.wait();
            panic!("internal service host fixture exceeded its 10-second test bound");
        }
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn legacy_host_invocation_stays_callable_but_is_not_advertised() {
    let root = run_cli(&["--help"]);
    assert!(root.status.success());
    let root = String::from_utf8(root.stdout).unwrap();
    assert!(!root.contains("service run"));

    let service_help = run_cli(&["service", "--help"]);
    assert!(service_help.status.success());
    let service_help = String::from_utf8(service_help.stdout).unwrap();
    assert!(!service_help.contains("run"));

    let host_help = run_cli(&["service", "run", "--help"]);
    assert!(host_help.status.success());
    assert!(String::from_utf8_lossy(&host_help.stdout).contains("--data-dir"));

    let root = fixture_dir();
    let data = root.join("legacy service data");
    fs::create_dir(&data).unwrap();
    let config = data.join("kakune.yaml");
    fs::write(&config, b"invalid: [yaml\n").unwrap();
    let before = fs::read(&config).unwrap();
    let output = run_cli(&["service", "run", "--data-dir", data.to_str().unwrap()]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains("unrecognized subcommand"), "{stderr}");
    assert!(!stderr.contains("unexpected argument"), "{stderr}");
    assert_eq!(fs::read(config).unwrap(), before);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn native_registration_keeps_the_legacy_host_argv_and_fingerprint() {
    let root = fixture_dir();
    let data_dir = root.join("Core data");
    let definition = ServiceDefinition::for_platform(&data_dir)
        .expect("current platform should produce its native service definition");

    assert_eq!(
        definition.arguments,
        vec![
            "service".to_owned(),
            "run".to_owned(),
            "--data-dir".to_owned(),
            data_dir.display().to_string(),
        ]
    );
    definition.validate().expect("registration should validate");
    let fingerprint = definition
        .definition_fingerprint
        .as_deref()
        .expect("native registration should carry a fingerprint");
    assert_eq!(definition.fingerprint().unwrap(), fingerprint);

    let persisted = serde_json::to_vec(&definition).unwrap();
    let restored: ServiceDefinition = serde_json::from_slice(&persisted).unwrap();
    restored
        .validate()
        .expect("existing registration fingerprint should remain compatible");
    assert!(definition.equivalent_to(&restored));

    let rendered = render_service_definition(&restored).unwrap();
    let host_arguments_are_rendered = match definition.manager {
        ServiceManagerKind::MacosLaunchd => {
            rendered.contains("<string>service</string>")
                && rendered.contains("<string>run</string>")
        }
        ServiceManagerKind::LinuxSystemd | ServiceManagerKind::WindowsScm => {
            rendered.contains("service run")
        }
    };
    assert!(
        host_arguments_are_rendered,
        "rendered service definition omitted the legacy host arguments: {rendered}"
    );
    assert!(rendered.contains("--data-dir"));
    let rendered_data_dir = match definition.manager {
        ServiceManagerKind::LinuxSystemd => data_dir.to_string_lossy().replace(' ', "\\x20"),
        ServiceManagerKind::WindowsScm | ServiceManagerKind::MacosLaunchd => {
            data_dir.to_string_lossy().into_owned()
        }
    };
    assert!(
        rendered.contains(&rendered_data_dir),
        "rendered service definition omitted the encoded data path: {rendered}"
    );

    let mut changed = restored.clone();
    changed.arguments[1] = "start".to_owned();
    assert!(changed.validate().is_err());
    assert!(serde_json::from_slice::<Value>(&persisted).is_ok());

    fs::remove_dir_all(root).unwrap();
}
