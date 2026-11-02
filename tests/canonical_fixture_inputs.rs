use std::{fs, path::Path};

use kakune_core::{WorkflowDocument, plugin_process::PluginManifest};

const ROOT: &str = "tests/fixtures/canonical_commands";

#[test]
fn nested_workflow_fixture_parses_with_a_plugin_reference() {
    let source = fs::read_to_string(format!("{ROOT}/nested-plugin.kakune.yaml"))
        .expect("nested fixture should be readable");
    let workflow = WorkflowDocument::parse(&source);
    assert!(
        workflow.is_ok(),
        "nested fixture should parse: {workflow:?}"
    );
    assert!(source.contains("org.example.canonical-sentinel@1"));
}

#[test]
fn workflow_execution_sentinel_parses() {
    let source = fs::read_to_string(format!("{ROOT}/workflow-sentinel.kakune.yaml"))
        .expect("workflow sentinel should be readable");
    let workflow = WorkflowDocument::parse(&source);
    assert!(
        workflow.is_ok(),
        "sentinel fixture should parse: {workflow:?}"
    );
}

#[test]
fn tampered_manifest_is_valid_but_differs_from_reviewed_manifest() {
    let original = format!("{ROOT}/valid/kakune-plugin.json");
    let tampered = format!("{ROOT}/tampered/kakune-plugin.json");
    assert!(PluginManifest::load(Path::new(&tampered)).is_ok());
    assert_ne!(fs::read(original).unwrap(), fs::read(tampered).unwrap());
}

#[test]
fn interrupted_staging_manifest_is_incomplete() {
    let path = format!("{ROOT}/interrupted/staging/kakune-plugin.json");
    assert!(PluginManifest::load(Path::new(&path)).is_err());
}

#[test]
fn historical_database_fixture_has_version_zero_and_preserved_row() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    let sql = fs::read_to_string(format!("{ROOT}/schema-zero.sql")).unwrap();
    connection.execute_batch(&sql).unwrap();
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    let name: String = connection
        .query_row(
            "SELECT name FROM workflows WHERE id = 'legacy-fixture'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!((version, name.as_str()), (0, "Legacy fixture"));
}
