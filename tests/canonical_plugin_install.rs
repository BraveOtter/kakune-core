use std::{fs, path::PathBuf, process::Command};

use kakune_core::{
    AuthScope, Store,
    management::DEFAULT_REQUEST_BYTES,
    plugin_install::{self, PluginSource},
};
use reqwest::{StatusCode, header::AUTHORIZATION};
use serde_json::json;
use uuid::Uuid;

mod support;
use support::management::{InProcessCore, ManagementFixture};

fn temp_dir(label: &str) -> PathBuf {
    let root = std::env::temp_dir().join("opencode");
    fs::create_dir_all(&root).expect("approved temporary root exists");
    let path = root.join(format!("kakune-plugin-{label}-{}", Uuid::new_v4()));
    fs::create_dir(&path).expect("isolated fixture directory is created");
    path
}

fn write_plugin(root: &std::path::Path, id: &str, version: &str, node_type: &str, contents: &str) {
    fs::create_dir_all(root.join("nodes")).expect("node directory is created");
    fs::write(root.join("runtime.txt"), contents).expect("package bytes are written");
    fs::write(
        root.join("kakune.plugin.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "manifestVersion":"1.0", "id":id, "name":"Install fixture", "version":version,
            "pluginProtocol":">=1.0.0 <2.0.0", "runtime":{"kind":"process","command":"node","args":["runtime.js"]},
            "contributes":{"nodes":["nodes/hello.json"]}
        }))
        .unwrap(),
    )
    .expect("manifest is written");
    fs::write(
        root.join("nodes/hello.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "apiVersion":"kakune.dev/v1", "kind":"NodeDefinition", "type":node_type,
            "name":"Hello", "inputSchema":{"type":"object"}, "outputSchema":{"type":"object"}
        }))
        .unwrap(),
    )
    .expect("definition is written");
}

#[test]
fn source_schemes_are_explicit_and_credential_bearing_git_urls_are_rejected() {
    assert!(matches!(
        PluginSource::parse("npm:@acme/plugin@1.2.3"),
        Ok(PluginSource::Npm { .. })
    ));
    assert!(matches!(
        PluginSource::parse("git:https://example.test/acme/plugin.git#v1"),
        Ok(PluginSource::Git { .. })
    ));
    assert!(matches!(
        PluginSource::parse("./plugin"),
        Ok(PluginSource::Local(_))
    ));
    assert!(PluginSource::parse("git:https://user:secret@example.test/repo").is_err());
    assert!(PluginSource::parse("git:file:///tmp/plugin").is_err());
    assert!(PluginSource::parse("npm:../plugin").is_err());
}

#[tokio::test]
async fn approval_is_digest_bound_and_activation_preserves_previous_content() {
    let source_one = temp_dir("install-one");
    let source_two = temp_dir("install-two");
    let data = temp_dir("install-data");
    let id = "org.example.install-fixture";
    let node_type = "org.example.install-fixture.hello@1";
    write_plugin(&source_one, id, "0.1.0", node_type, "original bytes");
    write_plugin(
        &source_two,
        id,
        "0.1.0",
        node_type,
        "same-version changed bytes",
    );
    let store = Store::open(data.clone()).expect("isolated Store opens");

    let first = plugin_install::prepare(&store, PluginSource::Local(source_one.clone()))
        .await
        .expect("initial package stages");
    let active = plugin_install::commit(&store, &first.record.id, &first.record.digest)
        .expect("exact digest activates");

    let interrupted_source = temp_dir("install-interrupted");
    write_plugin(
        &interrupted_source,
        id,
        "0.2.0",
        node_type,
        "interrupted activation bytes",
    );
    let interrupted =
        plugin_install::prepare(&store, PluginSource::Local(interrupted_source.clone()))
            .await
            .expect("interrupted replacement stages and is reviewed");
    let partial_destination = data.join("plugins").join(id).join(format!(
        "{}-{}",
        interrupted.record.version,
        &interrupted.record.digest[7..19]
    ));
    fs::create_dir_all(&partial_destination).expect("partial activation directory is created");
    fs::write(
        partial_destination.join("partial.txt"),
        "interrupted publication",
    )
    .expect("partial activation marker is written");
    assert!(
        plugin_install::commit(&store, &interrupted.record.id, &interrupted.record.digest).is_err()
    );
    assert_eq!(
        store.list_installed_plugins().unwrap()[0].digest,
        active.digest
    );

    let update = plugin_install::prepare(&store, PluginSource::Local(source_two.clone()))
        .await
        .expect("same-version update stages");
    assert!(plugin_install::commit(&store, &update.record.id, "sha256:incorrect").is_err());
    let still_active = store.list_installed_plugins().unwrap().remove(0);
    assert_eq!(still_active.digest, active.digest);
    assert!(std::path::Path::new(&active.manifest_path).is_file());

    let staged_manifest = PathBuf::from(&update.record.staged_path).join("runtime.txt");
    fs::write(staged_manifest, "tampered after review").expect("prepared package is tampered");
    assert!(plugin_install::commit(&store, &update.record.id, &update.record.digest).is_err());
    assert_eq!(
        store.list_installed_plugins().unwrap()[0].digest,
        active.digest
    );

    let changed_source = temp_dir("install-approved-change");
    write_plugin(
        &changed_source,
        id,
        "0.1.0",
        node_type,
        "approved changed bytes",
    );
    let approved = plugin_install::prepare(&store, PluginSource::Local(changed_source.clone()))
        .await
        .expect("changed same-version package stages");
    let replacement = plugin_install::commit(&store, &approved.record.id, &approved.record.digest)
        .expect("new exact digest replaces active record");
    assert_ne!(replacement.digest, active.digest);
    assert_eq!(replacement.version, active.version);
    assert!(
        std::path::Path::new(&active.manifest_path).is_file(),
        "prior bytes remain recoverable"
    );

    let collision = temp_dir("install-collision");
    write_plugin(
        &collision,
        "org.example.collision",
        "0.1.0",
        node_type,
        "collision",
    );
    let prepared_collision =
        plugin_install::prepare(&store, PluginSource::Local(collision.clone()))
            .await
            .expect("collision package is individually valid");
    assert!(
        plugin_install::commit(
            &store,
            &prepared_collision.record.id,
            &prepared_collision.record.digest
        )
        .is_err()
    );
    assert_eq!(store.list_installed_plugins().unwrap().len(), 1);

    drop(store);
    fs::remove_dir_all(source_one).expect("fixture is cleaned");
    fs::remove_dir_all(source_two).expect("fixture is cleaned");
    fs::remove_dir_all(interrupted_source).expect("fixture is cleaned");
    fs::remove_dir_all(changed_source).expect("fixture is cleaned");
    fs::remove_dir_all(collision).expect("fixture is cleaned");
    fs::remove_dir_all(data).expect("data fixture is cleaned");
}

#[tokio::test]
async fn plugin_mutations_require_admin_scope_and_read_only_list_is_bounded() {
    let fixture = ManagementFixture::new().expect("isolated fixture is created");
    let mut core = InProcessCore::start(fixture.path("plugin-auth-core"))
        .await
        .expect("in-process Core starts");
    let manage = core
        .store
        .create_auth_token(
            "plugin manage only".to_owned(),
            vec![AuthScope::Manage],
            None,
        )
        .expect("Manage token is created");
    let client = reqwest::Client::new();
    let unauthenticated_list = client
        .get(format!("{}/api/v1/manage/plugins", core.endpoint))
        .send()
        .await
        .expect("unauthenticated list request is sent");
    assert_eq!(unauthenticated_list.status(), StatusCode::UNAUTHORIZED);

    let manage_list = client
        .get(format!("{}/api/v1/manage/plugins", core.endpoint))
        .header(AUTHORIZATION, format!("Bearer {}", manage.token))
        .send()
        .await
        .expect("Manage-only list request is sent");
    assert_eq!(manage_list.status(), StatusCode::FORBIDDEN);

    let authorized_list = client
        .get(format!("{}/api/v1/manage/plugins", core.endpoint))
        .header(AUTHORIZATION, format!("Bearer {}", core.token))
        .send()
        .await
        .expect("authorized plugin list request is sent");
    assert_eq!(authorized_list.status(), StatusCode::OK);
    assert!(authorized_list.text().await.unwrap().len() < 1_048_576);

    let denied = client
        .post(format!("{}/api/v1/manage/plugins/prepare", core.endpoint))
        .header(AUTHORIZATION, format!("Bearer {}", manage.token))
        .json(&json!({"source":"npm:acme/plugin@1.0.0"}))
        .send()
        .await
        .expect("request is sent");
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);

    let authorized = client
        .post(format!("{}/api/v1/manage/plugins/commit", core.endpoint))
        .header(AUTHORIZATION, format!("Bearer {}", core.token))
        .json(&json!({"preparedId":"missing","digest":"sha256:missing"}))
        .send()
        .await
        .expect("authorized request reaches handler");
    assert_ne!(authorized.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(authorized.status(), StatusCode::FORBIDDEN);
    const { assert!(DEFAULT_REQUEST_BYTES >= 1024 * 1024) };
    core.shutdown().await.expect("Core shuts down cleanly");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canonical_plugin_cli_approves_exact_content_and_completes_lifecycle() {
    let fixture = ManagementFixture::new().expect("isolated fixture is created");
    let mut core = InProcessCore::start(fixture.path("plugin-cli-core"))
        .await
        .expect("in-process Core starts");
    let source_one = fixture.path("plugin-source-one");
    let source_two = fixture.path("plugin-source-two");
    fs::create_dir(&source_one).expect("initial package directory is created");
    fs::create_dir(&source_two).expect("updated package directory is created");
    let id = "org.example.plugin-cli";
    let node_type = "org.example.plugin-cli.hello@1";
    write_plugin(&source_one, id, "0.1.0", node_type, "initial content");
    write_plugin(&source_two, id, "0.2.0", node_type, "updated content");
    let execution_marker = fixture.path("plugin-runtime-executed.txt");
    fs::write(
        source_one.join("runtime.js"),
        format!(
            "require('node:fs').writeFileSync({}, 'executed');\n",
            serde_json::to_string(execution_marker.to_str().unwrap()).unwrap()
        ),
    )
    .expect("runtime sentinel is written");

    let contexts = fixture.path("plugin-cli-contexts.json");
    fs::write(
        &contexts,
        serde_json::to_vec(&json!({
            "format":"kakune-contexts/v1", "exportedAt":"2026-10-26T00:00:00Z",
            "activeContextId":"selected", "revision":0,
            "contexts":[{"id":"selected", "name":"Selected", "endpoint":core.endpoint,
                "expectedCoreId":core.store.core_id().unwrap(), "color":null, "credentialRef":null}]
        }))
        .unwrap(),
    )
    .expect("selected Core context is written");

    let invoke = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_kakune"))
            .env("KAKUNE_DATA_DIR", fixture.path("unused-local-data"))
            .env("KAKUNE_TOKEN", &core.token)
            .arg("--context-file")
            .arg(&contexts)
            .args(args)
            .output()
            .expect("plugin CLI starts")
    };

    let no_approval = invoke(&["plugin", "install", source_one.to_str().unwrap()]);
    assert!(!no_approval.status.success());
    assert!(String::from_utf8_lossy(&no_approval.stderr).contains("requires --approve-digest"));
    assert!(core.store.list_installed_plugins().unwrap().is_empty());

    let first_digest = plugin_install::package_digest(&source_one).unwrap();
    let mismatched_approval = invoke(&[
        "plugin",
        "install",
        source_one.to_str().unwrap(),
        "--approve-digest",
        "sha256:mismatch",
        "--json",
    ]);
    assert!(!mismatched_approval.status.success());
    assert!(
        String::from_utf8_lossy(&mismatched_approval.stderr)
            .contains("does not match the reviewed package")
    );
    assert!(core.store.list_installed_plugins().unwrap().is_empty());

    let install = invoke(&[
        "plugin",
        "install",
        source_one.to_str().unwrap(),
        "--approve-digest",
        &first_digest,
        "--json",
    ]);
    assert!(
        install.status.success(),
        "{}",
        String::from_utf8_lossy(&install.stderr)
    );
    assert_eq!(
        core.store.list_installed_plugins().unwrap()[0].digest,
        first_digest
    );

    for args in [
        vec!["plugin", "list", "--json"],
        vec!["plugin", "inspect", id, "--json"],
        vec!["plugin", "doctor", id, "--json"],
        vec!["plugin", "test", id, "--json"],
    ] {
        let result = invoke(&args);
        assert!(
            result.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
    assert!(
        !execution_marker.exists(),
        "installed inspect, doctor, and test must not start plugin code"
    );

    let second_digest = plugin_install::package_digest(&source_two).unwrap();
    let rejected_update = invoke(&[
        "plugin",
        "update",
        id,
        "--source",
        source_two.to_str().unwrap(),
        "--approve-digest",
        "sha256:mismatch",
        "--json",
    ]);
    assert!(!rejected_update.status.success());
    assert_eq!(
        core.store.list_installed_plugins().unwrap()[0].digest,
        first_digest
    );

    let update = invoke(&[
        "plugin",
        "update",
        id,
        "--source",
        source_two.to_str().unwrap(),
        "--approve-digest",
        &second_digest,
        "--json",
    ]);
    assert!(
        update.status.success(),
        "{}",
        String::from_utf8_lossy(&update.stderr)
    );
    let active = core.store.list_installed_plugins().unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].version, "0.2.0");
    assert_eq!(active[0].digest, second_digest);

    let removal = invoke(&["plugin", "remove", id, "--json"]);
    assert!(
        removal.status.success(),
        "{}",
        String::from_utf8_lossy(&removal.stderr)
    );
    assert!(core.store.list_installed_plugins().unwrap().is_empty());
    assert!(!fixture.path("unused-local-data").exists());
    core.shutdown().await.expect("Core shuts down cleanly");
}

#[test]
fn local_plugin_paths_are_not_sent_to_a_non_loopback_core() {
    let fixture = ManagementFixture::new().expect("isolated fixture is created");
    let source = fixture.path("remote-source");
    fs::create_dir(&source).expect("package directory is created");
    write_plugin(
        &source,
        "org.example.remote-path",
        "0.1.0",
        "org.example.remote-path.hello@1",
        "fixture",
    );
    let contexts = fixture.path("remote-contexts.json");
    fs::write(
        &contexts,
        serde_json::to_vec(&json!({
            "format":"kakune-contexts/v1", "exportedAt":"2026-10-26T00:00:00Z",
            "activeContextId":"remote", "revision":0,
            "contexts":[{"id":"remote", "name":"Remote", "endpoint":"https://core.example.invalid",
                "expectedCoreId":null, "color":null, "credentialRef":null}]
        }))
        .unwrap(),
    )
    .expect("remote Core context is written");
    let output = Command::new(env!("CARGO_BIN_EXE_kakune"))
        .env("KAKUNE_TOKEN", "fixture-token")
        .env("KAKUNE_DATA_DIR", fixture.path("unused-local-data"))
        .arg("--context-file")
        .arg(&contexts)
        .args([
            "plugin",
            "install",
            source.to_str().unwrap(),
            "--approve-digest",
            "sha256:reviewed",
        ])
        .output()
        .expect("CLI starts");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("cannot be sent to a remote Core"));
    assert!(!fixture.path("unused-local-data").exists());
}
