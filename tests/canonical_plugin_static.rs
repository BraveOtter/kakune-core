use std::{fs, path::PathBuf, process::Command};

use kakune_core::{
    PluginRegistry, Store,
    plugin_diagnostics::{CheckStatus, inspect},
    plugin_install::{self, PluginSource},
    plugin_process::PluginManifest,
    plugin_scaffold,
};
use serde_json::Value;
use uuid::Uuid;

fn temp_dir(label: &str) -> PathBuf {
    let root = std::env::temp_dir().join("opencode");
    fs::create_dir_all(&root).expect("approved temporary root exists");
    let path = root.join(format!("kakune-plugin-{label}-{}", Uuid::new_v4()));
    fs::create_dir(&path).expect("isolated fixture directory is created");
    path
}

fn write_plugin(root: &std::path::Path, id: &str, version: &str, marker: &std::path::Path) {
    fs::create_dir_all(root.join("nodes")).expect("node directory is created");
    let (command, args, script) = if cfg!(windows) {
        (
            "cmd.exe",
            vec!["/c".to_owned(), "sentinel.cmd".to_owned()],
            format!("@echo off\r\necho executed>\"{}\"\r\n", marker.display()),
        )
    } else {
        (
            "sh",
            vec!["sentinel.sh".to_owned()],
            format!("#!/bin/sh\necho executed > '{}'\n", marker.display()),
        )
    };
    fs::write(
        root.join("kakune.plugin.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "manifestVersion":"1.0", "id":id, "name":"Static fixture", "version":version,
            "pluginProtocol":">=1.0.0 <2.0.0", "runtime":{"kind":"process","command":command,"args":args},
            "description":{"en":"bearer static-diagnostic-canary"},
            "contributes":{"nodes":["nodes/hello.json"]},
            "permissions":{"filesystem":["read:workspace"]}
        }))
        .expect("manifest serializes"),
    )
    .expect("manifest is written");
    fs::write(
        root.join("nodes/hello.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "apiVersion":"kakune.dev/v1", "kind":"NodeDefinition", "type":format!("{id}.hello@1"),
            "name":"Hello", "inputSchema":{"type":"object"}, "outputSchema":{"type":"object"}
        }))
        .expect("definition serializes"),
    )
    .expect("definition is written");
    if cfg!(windows) {
        fs::write(root.join("sentinel.cmd"), script).expect("sentinel is written");
    } else {
        fs::write(root.join("sentinel.sh"), script).expect("sentinel is written");
    }
}

#[test]
fn scaffold_is_sdk_compatible_and_refuses_overwrites() {
    let parent = temp_dir("scaffold");
    let created = plugin_scaffold::create("sample_plugin", &parent).expect("scaffold is created");
    assert_eq!(created.identity, "local.sample-plugin");
    assert_eq!(created.node_type, "local.sample-plugin.hello@1");
    let manifest_path = created.destination.join("kakune.plugin.json");
    let manifest = PluginManifest::load(&manifest_path).expect("generated manifest validates");
    assert_eq!(manifest.id, created.identity);
    assert_eq!(
        manifest.contributes.as_ref().unwrap().nodes,
        ["nodes/hello.node.json"]
    );
    let mut registry = PluginRegistry::default();
    registry
        .register_manifest(manifest)
        .expect("generated node definition validates");
    let package: Value = serde_json::from_slice(
        &fs::read(created.destination.join("package.json")).expect("package file exists"),
    )
    .expect("package JSON is valid");
    assert_eq!(package["scripts"]["build"], "tsc -p tsconfig.json");
    assert_eq!(package["dependencies"]["@kakune-ai/sdk"], "^0.1.0");
    assert!(created.destination.join("tsconfig.json").is_file());
    assert!(created.destination.join("src/index.ts").is_file());

    let sentinel = created.destination.join("keep.txt");
    fs::write(&sentinel, "keep").expect("overwrite sentinel is created");
    assert!(plugin_scaffold::create("sample_plugin", &parent).is_err());
    assert_eq!(fs::read_to_string(sentinel).unwrap(), "keep");
    assert!(plugin_scaffold::create("../escape", &parent).is_err());
    fs::remove_dir_all(parent).expect("fixture is cleaned");
}

#[test]
fn plugin_create_cli_reports_destination_and_refuses_to_replace_existing_files() {
    let parent = temp_dir("scaffold-cli");
    let output = Command::new(env!("CARGO_BIN_EXE_kakune"))
        .current_dir(&parent)
        .env("KAKUNE_DATA_DIR", parent.join("unused-data"))
        .args(["plugin", "create", "cli_sample"])
        .output()
        .expect("plugin create CLI starts");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains(&parent.join("cli_sample").display().to_string()));
    assert!(stdout.contains("Identity: local.cli-sample"));
    assert!(stdout.contains("Node type: local.cli-sample.hello@1"));

    let sentinel = parent.join("cli_sample/keep.txt");
    fs::write(&sentinel, "keep").expect("overwrite sentinel is written");
    let second = Command::new(env!("CARGO_BIN_EXE_kakune"))
        .current_dir(&parent)
        .env("KAKUNE_DATA_DIR", parent.join("unused-data"))
        .args(["plugin", "create", "cli_sample"])
        .output()
        .expect("second plugin create CLI starts");
    assert!(!second.status.success());
    assert!(String::from_utf8_lossy(&second.stderr).contains("refusing to overwrite"));
    assert_eq!(fs::read_to_string(sentinel).unwrap(), "keep");
    fs::remove_dir_all(parent).expect("fixture is cleaned");
}

#[tokio::test]
async fn package_and_installed_diagnostics_never_execute_plugin_code() {
    let source = temp_dir("static-source");
    let data = temp_dir("static-data");
    let marker = source.join("must-not-exist.txt");
    write_plugin(&source, "org.example.static-check", "0.1.0", &marker);

    let package_report = inspect(&source).expect("package is inspected statically");
    assert_eq!(package_report.identity, "org.example.static-check");
    assert_eq!(package_report.declared_nodes, ["nodes/hello.json"]);
    assert!(
        package_report
            .checks
            .iter()
            .any(|check| check.name == "node-definitions" && check.status == CheckStatus::Pass)
    );
    assert!(package_report.runtime_conformance.contains("unverified"));
    let rendered_report = serde_json::to_string(&package_report).unwrap();
    assert!(!rendered_report.contains("static-diagnostic-canary"));
    assert!(
        !marker.exists(),
        "inspection must not start the sentinel executable"
    );

    for operation in ["inspect", "doctor", "test"] {
        let output = Command::new(env!("CARGO_BIN_EXE_kakune"))
            .env("KAKUNE_DATA_DIR", source.join("unused-data"))
            .args(["plugin", operation])
            .arg(&source)
            .arg("--json")
            .output()
            .expect("static diagnostic CLI starts");
        assert!(
            output.status.success(),
            "{operation}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!String::from_utf8_lossy(&output.stdout).contains("static-diagnostic-canary"));
        assert!(!String::from_utf8_lossy(&output.stderr).contains("static-diagnostic-canary"));
    }

    let store = Store::open(data.clone()).expect("isolated Store opens");
    let prepared = plugin_install::prepare(&store, PluginSource::Local(source.clone()))
        .await
        .expect("package stages without executing it");
    let installed = plugin_install::commit(&store, &prepared.record.id, &prepared.record.digest)
        .expect("approved package activates");
    let installed_report = inspect(PathBuf::from(installed.manifest_path).as_path())
        .expect("installed package is inspected statically");
    assert_eq!(installed_report.content_digest, installed.digest);
    assert!(
        !marker.exists(),
        "installed inspection must not start plugin code"
    );

    drop(store);
    fs::remove_dir_all(source).expect("source fixture is cleaned");
    fs::remove_dir_all(data).expect("data fixture is cleaned");
}
