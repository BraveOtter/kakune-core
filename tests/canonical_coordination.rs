mod support;

use kakune_core::{InstalledPluginRecord, WorkflowDocument};
use std::sync::{Arc, Barrier};
use support::management::ManagementFixture;

const SOURCE: &str = "apiVersion: kakune/v1\nkind: Workflow\nmetadata: {id: protected, name: Protected}\ntriggers: [{id: manual, type: kakune.trigger.manual@1}]\nentry: start\nnodes:\n  - id: start\n    type: kakune.flow.pass@1\n";

#[cfg(target_os = "linux")]
#[test]
fn failed_sandbox_start_releases_admitted_plugin_pin() {
    use kakune_core::plugin_process::{PluginHostOptions, PluginManifest};
    use std::{fs, os::unix::fs::PermissionsExt};

    let fixture = ManagementFixture::new().unwrap();
    let package = fixture.path("sandbox-pinned-package");
    fs::create_dir_all(package.join("definitions")).unwrap();
    fs::write(
        package.join("definitions/sentinel.json"),
        include_bytes!("fixtures/canonical_commands/valid/definitions/sentinel.json"),
    )
    .unwrap();
    let sentinel = fixture.path("runtime-started");
    let runtime = fixture.path("sandbox-runtime");
    fs::write(
        &runtime,
        format!("#!/bin/sh\nprintf started > '{}'\n", sentinel.display()),
    )
    .unwrap();
    fs::set_permissions(&runtime, fs::Permissions::from_mode(0o700)).unwrap();
    let mut manifest: serde_json::Value = serde_json::from_str(include_str!(
        "fixtures/canonical_commands/valid/kakune-plugin.json"
    ))
    .unwrap();
    manifest["runtime"]["command"] = serde_json::json!(runtime);
    let manifest_path = package.join("kakune-plugin.json");
    fs::write(&manifest_path, manifest.to_string()).unwrap();
    let mut registry = kakune_core::PluginRegistry::default();
    registry
        .register_manifest_with_options(
            PluginManifest::load(&manifest_path).unwrap(),
            PluginHostOptions {
                filesystem_sandbox_launcher: Some(fixture.path("missing-sandbox")),
                ..Default::default()
            },
        )
        .unwrap();
    let store = kakune_core::Store::open(fixture.path("sandbox-pin-store")).unwrap();
    store
        .upsert_plugin_install(&InstalledPluginRecord {
            id: "org.example.canonical-fixture".into(),
            name: "Fixture".into(),
            version: "0.1.0".into(),
            manifest_path: manifest_path.to_string_lossy().into_owned(),
            installed_at: "now".into(),
            enabled: true,
            policy: serde_json::json!({}),
            digest: kakune_core::plugin_install::package_digest(&package).unwrap(),
            resolved_lock: serde_json::json!({}),
            provenance: serde_json::json!({}),
        })
        .unwrap();
    let source = include_str!("fixtures/canonical_commands/nested-plugin.kakune.yaml");
    store
        .upsert_workflow(&WorkflowDocument::parse(source).unwrap(), source, "enabled")
        .unwrap();
    let admitted = kakune_core::management::admit_workflow_execution(
        &store,
        "canonical-nested-plugin",
        serde_json::json!({}),
        None,
        &registry,
    )
    .unwrap();
    assert_eq!(
        store
            .plugin_execution_pin_count("org.example.canonical-fixture")
            .unwrap(),
        1
    );
    let id = admitted.execution.id.clone();
    assert!(
        kakune_core::execute_prepared_workflow_with_plugins(&store, admitted.execution, &registry)
            .is_err()
    );
    assert_eq!(store.get_execution(&id).unwrap().unwrap().status, "failed");
    assert_eq!(
        store
            .plugin_execution_pin_count("org.example.canonical-fixture")
            .unwrap(),
        0
    );
    assert!(!sentinel.exists());
}

#[test]
fn package_digest_does_not_cover_an_external_runtime_executable() {
    let fixture = ManagementFixture::new().unwrap();
    let package = fixture.path("digest-package");
    std::fs::create_dir_all(&package).unwrap();
    let executable = fixture.path("external-runtime");
    std::fs::write(&executable, b"original runtime").unwrap();
    let manifest = serde_json::json!({
        "runtime": {"kind": "process", "command": executable.to_string_lossy()}
    });
    std::fs::write(package.join("kakune-plugin.json"), manifest.to_string()).unwrap();
    let approved = kakune_core::plugin_install::package_digest(&package).unwrap();

    std::thread::spawn(move || std::fs::write(executable, b"modified runtime").unwrap())
        .join()
        .unwrap();

    assert_eq!(
        kakune_core::plugin_install::package_digest(&package).unwrap(),
        approved,
        "the package digest cannot detect modifications outside the package"
    );
}

#[test]
fn removal_barrier_and_execution_pins_survive_reopening() {
    let fixture = ManagementFixture::new().unwrap();
    let data = fixture.path("coordination");
    let store = kakune_core::Store::open(data.clone()).unwrap();
    store
        .upsert_plugin_install(&InstalledPluginRecord {
            id: "example".into(),
            name: "Example".into(),
            version: "1.0.0".into(),
            manifest_path: "active/manifest.json".into(),
            installed_at: "now".into(),
            enabled: true,
            policy: serde_json::json!({}),
            digest: "digest".into(),
            resolved_lock: serde_json::json!({}),
            provenance: serde_json::json!({}),
        })
        .unwrap();
    let workflow = WorkflowDocument::parse(SOURCE).unwrap();
    store.upsert_workflow(&workflow, SOURCE, "enabled").unwrap();
    assert!(
        store
            .set_plugin_source(
                "example",
                Some("git:https://user:secret@example.test/plugin")
            )
            .is_err()
    );
    assert!(
        store
            .mark_plugin_removal_failed("example", "failure")
            .is_err()
    );
    let execution = store.create_execution("protected").unwrap();
    store
        .acquire_plugin_uses(&execution.id, &["example"])
        .unwrap();
    assert!(store.remove_plugin_install("example").is_err());
    assert_eq!(
        store
            .request_plugin_removal("example", &["protected"])
            .unwrap(),
        1
    );
    assert!(
        store
            .acquire_plugin_uses(&execution.id, &["example"])
            .is_err()
    );
    assert_eq!(store.list_workflows().unwrap()[0].status, "disabled");
    drop(store);
    let reopened = kakune_core::Store::open(data).unwrap();
    assert_eq!(
        reopened
            .plugin_removal_state("example")
            .unwrap()
            .unwrap()
            .status,
        "pending removal"
    );
    assert_eq!(reopened.plugin_execution_pin_count("example").unwrap(), 1);
    reopened
        .set_plugin_source("example", Some("git:https://example.test/plugin"))
        .unwrap();
    reopened
        .mark_plugin_removal_failed("example", "filesystem unavailable")
        .unwrap();
    drop(reopened);
    let reopened = kakune_core::Store::open(fixture.path("coordination")).unwrap();
    let state = reopened.plugin_removal_state("example").unwrap().unwrap();
    assert_eq!(state.status, "removal failed");
    assert_eq!(
        state.failure_details.as_deref(),
        Some("filesystem unavailable")
    );
    assert_eq!(
        state.source.as_deref(),
        Some("git:https://example.test/plugin")
    );
}

#[test]
fn concurrent_removal_and_acquisition_are_serialized_across_connections() {
    let fixture = ManagementFixture::new().unwrap();
    let data = fixture.path("concurrent");
    let first = kakune_core::Store::open(data.clone()).unwrap();
    first
        .upsert_plugin_install(&InstalledPluginRecord {
            id: "example".into(),
            name: "Example".into(),
            version: "1.0.0".into(),
            manifest_path: "active/manifest.json".into(),
            installed_at: "now".into(),
            enabled: true,
            policy: serde_json::json!({}),
            digest: "digest".into(),
            resolved_lock: serde_json::json!({}),
            provenance: serde_json::json!({}),
        })
        .unwrap();
    let execution = first.create_execution("workflow").unwrap();
    let second = kakune_core::Store::open(data).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let peer = barrier.clone();
    let thread = std::thread::spawn(move || {
        peer.wait();
        second.acquire_plugin_uses(&execution.id, &["example"])
    });
    barrier.wait();
    let count = first.request_plugin_removal("example", &[]).unwrap();
    let acquired = thread.join().unwrap().is_ok();
    assert_eq!(count, u64::from(acquired));
    assert_eq!(first.plugin_execution_pin_count("example").unwrap(), count);
    assert_eq!(
        first
            .plugin_removal_state("example")
            .unwrap()
            .unwrap()
            .status,
        "pending removal"
    );
}

#[test]
fn busy_sqlite_rejects_removal_with_bounded_wait_and_no_partial_state() {
    use std::time::{Duration, Instant};
    let fixture = ManagementFixture::new().unwrap();
    let data = fixture.path("busy-boundary");
    let store = kakune_core::Store::open(data.clone()).unwrap();
    store
        .upsert_plugin_install(&InstalledPluginRecord {
            id: "example".into(),
            name: "Example".into(),
            version: "1.0.0".into(),
            manifest_path: "active/manifest.json".into(),
            installed_at: "now".into(),
            enabled: true,
            policy: serde_json::json!({}),
            digest: "digest".into(),
            resolved_lock: serde_json::json!({}),
            provenance: serde_json::json!({}),
        })
        .unwrap();
    store
        .upsert_workflow(&WorkflowDocument::parse(SOURCE).unwrap(), SOURCE, "enabled")
        .unwrap();
    let blocker = rusqlite::Connection::open(data.join("kakune.sqlite3")).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let start = Instant::now();
    assert!(
        store
            .request_plugin_removal("example", &["protected"])
            .is_err()
    );
    // Keep the five-second Store budget covered by unit tests, but allow
    // additional wall-clock slack for heavily scheduled CI runners.
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(12),
        "bounded SQLite wait exceeded its scheduling allowance: {elapsed:?}"
    );
    blocker.execute_batch("ROLLBACK").unwrap();
    assert_eq!(
        store
            .plugin_removal_state("example")
            .unwrap()
            .unwrap()
            .status,
        "active"
    );
    assert_eq!(store.list_workflows().unwrap()[0].status, "enabled");
}

#[test]
fn missing_workflow_rolls_back_removal_and_all_disablement() {
    let fixture = ManagementFixture::new().unwrap();
    let store = kakune_core::Store::open(fixture.path("rollback")).unwrap();
    store
        .upsert_plugin_install(&InstalledPluginRecord {
            id: "example".into(),
            name: "Example".into(),
            version: "1.0.0".into(),
            manifest_path: "active/manifest.json".into(),
            installed_at: "now".into(),
            enabled: true,
            policy: serde_json::json!({}),
            digest: "digest".into(),
            resolved_lock: serde_json::json!({}),
            provenance: serde_json::json!({}),
        })
        .unwrap();
    let workflow = WorkflowDocument::parse(SOURCE).unwrap();
    store.upsert_workflow(&workflow, SOURCE, "enabled").unwrap();
    assert!(
        store
            .request_plugin_removal("example", &["protected", "missing"])
            .is_err()
    );
    assert_eq!(store.list_workflows().unwrap()[0].status, "enabled");
    assert_eq!(
        store
            .plugin_removal_state("example")
            .unwrap()
            .unwrap()
            .status,
        "active"
    );
}

#[test]
fn plan_admission_and_content_pins_commit_together_and_honor_barriers() {
    let fixture = ManagementFixture::new().unwrap();
    let store = kakune_core::Store::open(fixture.path("atomic-admission")).unwrap();
    let package = store.data_dir().join("plugins").join("example");
    std::fs::create_dir_all(&package).unwrap();
    let manifest_path = package.join("manifest.json");
    std::fs::write(&manifest_path, b"{}").unwrap();
    store
        .upsert_plugin_install(&InstalledPluginRecord {
            id: "example".into(),
            name: "Example".into(),
            version: "1.0.0".into(),
            manifest_path: manifest_path.to_string_lossy().into_owned(),
            installed_at: "now".into(),
            enabled: true,
            policy: serde_json::json!({}),
            digest: "digest".into(),
            resolved_lock: serde_json::json!({}),
            provenance: serde_json::json!({}),
        })
        .unwrap();
    let workflow = WorkflowDocument::parse(SOURCE).unwrap();
    store.upsert_workflow(&workflow, SOURCE, "enabled").unwrap();
    let revision = store
        .get_workflow_source("protected")
        .unwrap()
        .unwrap()
        .revision;
    let plan = serde_json::json!({"policy": {}});
    assert!(
        store
            .admit_plan_with_plugin_uses(
                "protected",
                &revision,
                &plan,
                &["example", "missing"],
                None,
                false
            )
            .is_err()
    );
    assert_eq!(store.plugin_execution_pin_count("example").unwrap(), 0);
    let (execution, created) = store
        .admit_plan_with_plugin_uses("protected", &revision, &plan, &["example"], None, false)
        .unwrap();
    assert!(created);
    assert_eq!(store.plugin_execution_pin_count("example").unwrap(), 1);
    assert_eq!(
        store
            .request_plugin_removal("example", &["protected"])
            .unwrap(),
        1
    );
    assert!(
        store
            .admit_plan_with_plugin_uses("protected", &revision, &plan, &[], None, false)
            .is_err()
    );
    assert!(store.set_plugin_enabled("example", true).is_err());
    assert!(
        store
            .set_workflow_status_if_revision("protected", "enabled", &revision)
            .is_err()
    );
    assert!(store.set_workflow_status("protected", "enabled").is_err());
    assert!(store.execution_plan(&execution.id).unwrap().is_some());
    assert!(store.remove_plugin_install("example").is_err());
    assert_eq!(
        store
            .plugin_removal_state("example")
            .unwrap()
            .unwrap()
            .status,
        "pending removal"
    );
    store
        .finish_execution(&execution.id, "succeeded", None)
        .unwrap();
    assert_eq!(store.plugin_execution_pin_count("example").unwrap(), 0);
    assert!(store.remove_plugin_install("example").unwrap().is_none());
    assert!(store.plugin_removal_state("example").unwrap().is_none());
    assert!(!package.exists());
}

#[test]
fn management_admission_pins_nested_plugin_and_rejects_pending_removal() {
    let fixture = ManagementFixture::new().unwrap();
    let store = kakune_core::Store::open(fixture.path("management-admission")).unwrap();
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/canonical_commands/valid/kakune-plugin.json");
    let mut registry = kakune_core::PluginRegistry::default();
    registry.register_manifest_path(&manifest).unwrap();
    let installed = InstalledPluginRecord {
        id: "org.example.canonical-fixture".into(),
        name: "Fixture".into(),
        version: "0.1.0".into(),
        manifest_path: manifest.to_string_lossy().into_owned(),
        installed_at: "now".into(),
        enabled: true,
        policy: serde_json::json!({}),
        digest: kakune_core::plugin_install::package_digest(manifest.parent().unwrap()).unwrap(),
        resolved_lock: serde_json::json!({}),
        provenance: serde_json::json!({}),
    };
    let mut changed = installed.clone();
    changed.version = "0.2.0".into();
    store.upsert_plugin_install(&changed).unwrap();
    let source = include_str!("fixtures/canonical_commands/nested-plugin.kakune.yaml");
    let workflow = WorkflowDocument::parse(source).unwrap();
    store.upsert_workflow(&workflow, source, "enabled").unwrap();
    assert!(
        kakune_core::management::admit_workflow_execution(
            &store,
            "canonical-nested-plugin",
            serde_json::json!({}),
            None,
            &registry
        )
        .is_err()
    );
    assert_eq!(
        store
            .plugin_execution_pin_count("org.example.canonical-fixture")
            .unwrap(),
        0
    );
    changed.version = installed.version.clone();
    changed.manifest_path = "different/manifest.json".into();
    store.upsert_plugin_install(&changed).unwrap();
    assert!(
        kakune_core::management::admit_workflow_execution(
            &store,
            "canonical-nested-plugin",
            serde_json::json!({}),
            None,
            &registry
        )
        .is_err()
    );
    store.upsert_plugin_install(&installed).unwrap();
    let admission = kakune_core::management::admit_workflow_execution(
        &store,
        "canonical-nested-plugin",
        serde_json::json!({}),
        None,
        &registry,
    )
    .unwrap();
    assert!(admission.created);
    assert_eq!(
        store
            .plugin_execution_pin_count("org.example.canonical-fixture")
            .unwrap(),
        1
    );
    changed.manifest_path = "other/manifest.json".into();
    assert!(store.upsert_plugin_install(&changed).is_err());
    store
        .request_plugin_removal(
            "org.example.canonical-fixture",
            &["canonical-nested-plugin"],
        )
        .unwrap();
    assert!(
        kakune_core::management::admit_workflow_execution(
            &store,
            "canonical-nested-plugin",
            serde_json::json!({}),
            None,
            &registry
        )
        .is_err()
    );
}

#[test]
fn unchanged_plugin_version_and_path_do_not_hide_changed_content() {
    let fixture = ManagementFixture::new().unwrap();
    let root = fixture.path("package");
    std::fs::create_dir_all(root.join("definitions")).unwrap();
    std::fs::write(
        root.join("kakune-plugin.json"),
        include_bytes!("fixtures/canonical_commands/valid/kakune-plugin.json"),
    )
    .unwrap();
    std::fs::write(
        root.join("definitions/sentinel.json"),
        include_bytes!("fixtures/canonical_commands/valid/definitions/sentinel.json"),
    )
    .unwrap();
    let manifest = root.join("kakune-plugin.json");
    let mut registry = kakune_core::PluginRegistry::default();
    registry.register_manifest_path(&manifest).unwrap();
    let store = kakune_core::Store::open(fixture.path("core")).unwrap();
    store
        .upsert_plugin_install(&InstalledPluginRecord {
            id: "org.example.canonical-fixture".into(),
            name: "Fixture".into(),
            version: "0.1.0".into(),
            manifest_path: manifest.to_string_lossy().into_owned(),
            installed_at: "now".into(),
            enabled: true,
            policy: serde_json::json!({}),
            digest: kakune_core::plugin_install::package_digest(&root).unwrap(),
            resolved_lock: serde_json::json!({}),
            provenance: serde_json::json!({}),
        })
        .unwrap();
    let source = include_str!("fixtures/canonical_commands/nested-plugin.kakune.yaml");
    store
        .upsert_workflow(&WorkflowDocument::parse(source).unwrap(), source, "enabled")
        .unwrap();
    std::fs::write(root.join("extra.txt"), "changed content").unwrap();
    assert!(
        kakune_core::management::admit_workflow_execution(
            &store,
            "canonical-nested-plugin",
            serde_json::json!({}),
            None,
            &registry
        )
        .is_err()
    );
    assert_eq!(
        store
            .plugin_execution_pin_count("org.example.canonical-fixture")
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn plugin_host_rejects_content_changed_after_catalog_loading() {
    let fixture = ManagementFixture::new().unwrap();
    let root = fixture.path("package-before-execution");
    std::fs::create_dir_all(root.join("definitions")).unwrap();
    std::fs::write(
        root.join("kakune-plugin.json"),
        include_bytes!("fixtures/canonical_commands/valid/kakune-plugin.json"),
    )
    .unwrap();
    std::fs::write(
        root.join("definitions/sentinel.json"),
        include_bytes!("fixtures/canonical_commands/valid/definitions/sentinel.json"),
    )
    .unwrap();
    let mut registry = kakune_core::PluginRegistry::default();
    registry
        .register_manifest_path(root.join("kakune-plugin.json"))
        .unwrap();
    let store = kakune_core::Store::open(fixture.path("host-core")).unwrap();
    store
        .upsert_plugin_install(&InstalledPluginRecord {
            id: "org.example.canonical-fixture".into(),
            name: "Fixture".into(),
            version: "0.1.0".into(),
            manifest_path: root
                .join("kakune-plugin.json")
                .to_string_lossy()
                .into_owned(),
            installed_at: "now".into(),
            enabled: true,
            policy: serde_json::json!({}),
            digest: kakune_core::plugin_install::package_digest(&root).unwrap(),
            resolved_lock: serde_json::json!({}),
            provenance: serde_json::json!({}),
        })
        .unwrap();
    let source = include_str!("fixtures/canonical_commands/nested-plugin.kakune.yaml");
    store
        .upsert_workflow(&WorkflowDocument::parse(source).unwrap(), source, "enabled")
        .unwrap();
    let admitted = kakune_core::management::admit_workflow_execution(
        &store,
        "canonical-nested-plugin",
        serde_json::json!({}),
        None,
        &registry,
    )
    .unwrap();
    assert_eq!(
        store
            .plugin_execution_pin_count("org.example.canonical-fixture")
            .unwrap(),
        1
    );
    let path = root.join("extra.txt");
    std::thread::spawn(move || std::fs::write(path, "changed after admission").unwrap())
        .join()
        .unwrap();
    let result = registry
        .get("org.example.canonical-sentinel@1")
        .unwrap()
        .execute(
            store,
            &admitted.execution.id,
            "sentinel",
            Default::default(),
        )
        .await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("content changed"));
}

#[tokio::test]
async fn core_api_requires_run_scope_and_respects_persisted_removal_barrier() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;
    let fixture = ManagementFixture::new().unwrap();
    let store = kakune_core::Store::open(fixture.path("api-barrier")).unwrap();
    store
        .upsert_plugin_install(&InstalledPluginRecord {
            id: "example".into(),
            name: "Example".into(),
            version: "1.0.0".into(),
            manifest_path: "active/manifest.json".into(),
            installed_at: "now".into(),
            enabled: true,
            policy: serde_json::json!({}),
            digest: "digest".into(),
            resolved_lock: serde_json::json!({}),
            provenance: serde_json::json!({}),
        })
        .unwrap();
    let workflow = WorkflowDocument::parse(SOURCE).unwrap();
    store.upsert_workflow(&workflow, SOURCE, "enabled").unwrap();
    store
        .request_plugin_removal("example", &["protected"])
        .unwrap();
    let read = store
        .create_auth_token("read".into(), vec![kakune_core::AuthScope::Read], None)
        .unwrap();
    let run = store
        .create_auth_token("run".into(), vec![kakune_core::AuthScope::Run], None)
        .unwrap();
    let app = kakune_core::api::router(store.clone());
    let request = |token: &str| {
        Request::builder()
            .method("POST")
            .uri("/api/v1/manage/workflows/protected/executions")
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {token}"))
            .body(Body::from("{}"))
            .unwrap()
    };
    let denied = app.clone().oneshot(request(&read.token)).await.unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    let blocked = app.oneshot(request(&run.token)).await.unwrap();
    assert_eq!(blocked.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(store.plugin_execution_pin_count("example").unwrap(), 0);
}

#[tokio::test]
async fn plugin_delete_api_cannot_bypass_a_pending_removal() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;
    let fixture = ManagementFixture::new().unwrap();
    let store = kakune_core::Store::open(fixture.path("api-delete")).unwrap();
    store
        .upsert_plugin_install(&InstalledPluginRecord {
            id: "example".into(),
            name: "Example".into(),
            version: "1.0.0".into(),
            manifest_path: "active/manifest.json".into(),
            installed_at: "now".into(),
            enabled: true,
            policy: serde_json::json!({}),
            digest: "digest".into(),
            resolved_lock: serde_json::json!({}),
            provenance: serde_json::json!({}),
        })
        .unwrap();
    store.request_plugin_removal("example", &[]).unwrap();
    let token = store
        .create_auth_token("admin".into(), vec![kakune_core::AuthScope::Admin], None)
        .unwrap();
    let app = kakune_core::api::router(store.clone());
    let request = |token: Option<&str>| {
        let mut builder = Request::builder()
            .method("DELETE")
            .uri("/api/v1/plugins/example");
        if let Some(token) = token {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        builder.body(Body::empty()).unwrap()
    };
    assert_eq!(
        app.clone().oneshot(request(None)).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app.oneshot(request(Some(&token.token)))
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    assert_eq!(
        store
            .plugin_removal_state("example")
            .unwrap()
            .unwrap()
            .status,
        "pending removal"
    );
}

#[tokio::test]
async fn plugin_lifecycle_api_reports_pending_state_only_to_authorized_clients() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    let fixture = ManagementFixture::new().unwrap();
    let store = kakune_core::Store::open(fixture.path("api-lifecycle")).unwrap();
    store
        .upsert_plugin_install(&InstalledPluginRecord {
            id: "example".into(),
            name: "Example".into(),
            version: "1.0.0".into(),
            manifest_path: "active/manifest.json".into(),
            installed_at: "now".into(),
            enabled: true,
            policy: serde_json::json!({}),
            digest: "digest".into(),
            resolved_lock: serde_json::json!({}),
            provenance: serde_json::json!({}),
        })
        .unwrap();
    store.request_plugin_removal("example", &[]).unwrap();
    let read = store
        .create_auth_token("reader".into(), vec![kakune_core::AuthScope::Read], None)
        .unwrap();
    let app = kakune_core::api::router(store);
    let request = |token: Option<&str>| {
        let mut builder = Request::builder().uri("/api/v1/plugins/example/lifecycle");
        if let Some(token) = token {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        builder.body(Body::empty()).unwrap()
    };
    assert_eq!(
        app.clone().oneshot(request(None)).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    let response = app.oneshot(request(Some(&read.token))).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["status"], "pending removal");
}
