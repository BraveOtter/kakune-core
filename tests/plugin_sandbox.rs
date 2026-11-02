#[cfg(target_os = "linux")]
#[test]
fn filesystem_launcher_enforces_write_denial_or_fails_before_exec() {
    use std::{fs, process::Command};

    let directory = std::env::temp_dir().join(format!("kakune-sandbox-{}", uuid::Uuid::new_v4()));
    fs::create_dir(&directory).unwrap();
    let existing = directory.join("existing");
    let created = directory.join("created");
    fs::write(&existing, b"original").unwrap();
    let launcher = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("kakune-plugin-sandbox");
    let output = Command::new(launcher)
        .args([
            "/bin/sh",
            "-c",
            r#"if printf changed > "$1" 2>/dev/null; then exit 41; fi
if printf new > "$2" 2>/dev/null; then exit 42; fi
if /bin/sh -c 'printf child-started; printf child > "$1"' sandbox-child "$1"; then exit 43; fi
if /bin/sh -c 'printf child-started; printf child > "$1"' sandbox-child "$2"; then exit 44; fi
printf restrictions-enforced"#,
            "sandbox-test",
        ])
        .arg(&existing)
        .arg(&created)
        .output()
        .unwrap();
    if output.status.success() {
        assert_eq!(
            output.stdout,
            b"child-startedchild-startedrestrictions-enforced"
        );
    } else {
        assert_eq!(output.status.code(), Some(1));
        assert!(String::from_utf8_lossy(&output.stderr).contains("sandbox is unavailable"));
        assert!(
            output.stdout.is_empty(),
            "the requested process must not run"
        );
        assert!(
            std::env::var_os("KAKUNE_TEST_REQUIRE_LANDLOCK").is_none(),
            "this test requires an active Landlock ABI v3 sandbox"
        );
    }
    assert_eq!(fs::read(&existing).unwrap(), b"original");
    assert!(!created.exists());
    fs::remove_file(existing).unwrap();
    fs::remove_dir(directory).unwrap();
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn plugin_host_rejects_missing_launcher_without_starting_the_runtime() {
    use kakune_core::plugin_process::{PluginHost, PluginHostOptions, PluginManifest};
    use std::{fs, os::unix::fs::PermissionsExt};

    let directory =
        std::env::temp_dir().join(format!("kakune-host-sandbox-{}", uuid::Uuid::new_v4()));
    fs::create_dir(&directory).unwrap();
    let runtime = directory.join("runtime");
    let sentinel = directory.join("started");
    fs::write(
        &runtime,
        format!("#!/bin/sh\nprintf started > '{}'\n", sentinel.display()),
    )
    .unwrap();
    fs::set_permissions(&runtime, fs::Permissions::from_mode(0o700)).unwrap();
    let manifest_path = directory.join("kakune-plugin.json");
    let manifest = serde_json::json!({"manifestVersion":"1.0","id":"org.example.sandbox-test","name":"Sandbox test","version":"0.1.0","pluginProtocol":">=1.0.0 <2.0.0","runtime":{"kind":"process","command":runtime,"args":[]}});
    fs::write(&manifest_path, manifest.to_string()).unwrap();
    let manifest = PluginManifest::load(&manifest_path).unwrap();
    let options = PluginHostOptions {
        filesystem_sandbox_launcher: Some(directory.join("missing-launcher")),
        ..Default::default()
    };
    assert!(PluginHost::start(&manifest, options).await.is_err());
    assert!(!sentinel.exists());
    fs::remove_file(manifest_path).unwrap();
    fs::remove_file(runtime).unwrap();
    fs::remove_dir(directory).unwrap();
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn plugin_host_requires_sandbox_readiness_before_accepting_requests() {
    use kakune_core::plugin_process::{PluginHost, PluginHostOptions, PluginManifest};
    use std::fs;

    let directory =
        std::env::temp_dir().join(format!("kakune-host-ready-{}", uuid::Uuid::new_v4()));
    fs::create_dir(&directory).unwrap();
    let binaries = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    let manifest_path = directory.join("kakune-plugin.json");
    let manifest = serde_json::json!({"manifestVersion":"1.0","id":"org.example.sandbox-ready","name":"Sandbox ready","version":"0.1.0","pluginProtocol":">=1.0.0 <2.0.0","runtime":{"kind":"process","command":binaries.join("plugin-helper"),"args":[]}});
    fs::write(&manifest_path, manifest.to_string()).unwrap();
    let manifest = PluginManifest::load(&manifest_path).unwrap();
    let options = PluginHostOptions {
        filesystem_sandbox_launcher: Some(binaries.join("kakune-plugin-sandbox")),
        ..Default::default()
    };
    match PluginHost::start(&manifest, options).await {
        Ok(mut host) => {
            let response = host
                .request("initialize", serde_json::json!({"host":"test"}))
                .await
                .unwrap();
            assert_eq!(response, serde_json::json!({"ok":true}));
            host.shutdown().await.unwrap();
        }
        Err(error) => {
            assert!(
                error
                    .to_string()
                    .contains("filesystem sandbox could not be established")
            );
            assert!(
                std::env::var_os("KAKUNE_TEST_REQUIRE_LANDLOCK").is_none(),
                "Landlock must be available in this environment"
            );
        }
    }
    fs::remove_file(manifest_path).unwrap();
    fs::remove_dir(directory).unwrap();
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn plugin_host_rejects_malformed_or_stalled_sandbox_handshake() {
    use kakune_core::plugin_process::{PluginHost, PluginHostOptions, PluginManifest};
    use std::{
        fs,
        os::unix::fs::PermissionsExt,
        time::{Duration, Instant},
    };

    let directory =
        std::env::temp_dir().join(format!("kakune-host-handshake-{}", uuid::Uuid::new_v4()));
    fs::create_dir(&directory).unwrap();
    let runtime = directory.join("runtime");
    let sentinel = directory.join("started");
    fs::write(
        &runtime,
        format!("#!/bin/sh\nprintf started > '{}'\n", sentinel.display()),
    )
    .unwrap();
    fs::set_permissions(&runtime, fs::Permissions::from_mode(0o700)).unwrap();
    let manifest_path = directory.join("kakune-plugin.json");
    let document = serde_json::json!({"manifestVersion":"1.0","id":"org.example.sandbox-handshake","name":"Sandbox handshake","version":"0.1.0","pluginProtocol":">=1.0.0 <2.0.0","runtime":{"kind":"process","command":runtime,"args":[]}});
    fs::write(&manifest_path, document.to_string()).unwrap();
    let manifest = PluginManifest::load(&manifest_path).unwrap();
    let launcher = directory.join("launcher");
    for script in [
        "#!/bin/sh\nprintf bad\n",
        "#!/bin/sh\nwhile :; do :; done\n",
    ] {
        fs::write(&launcher, script).unwrap();
        fs::set_permissions(&launcher, fs::Permissions::from_mode(0o700)).unwrap();
        let options = PluginHostOptions {
            filesystem_sandbox_launcher: Some(launcher.clone()),
            request_timeout: Duration::from_millis(150),
            shutdown_timeout: Duration::from_millis(150),
            ..Default::default()
        };
        let started = Instant::now();
        let error = match PluginHost::start(&manifest, options).await {
            Ok(_) => panic!("unverified sandbox was accepted"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("filesystem sandbox could not be established")
        );
        assert!(started.elapsed() < Duration::from_secs(3));
        assert!(!sentinel.exists());
    }
    fs::remove_file(launcher).unwrap();
    fs::remove_file(manifest_path).unwrap();
    fs::remove_file(runtime).unwrap();
    fs::remove_dir(directory).unwrap();
}
