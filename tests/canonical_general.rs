#![expect(
    clippy::duplicate_mod,
    reason = "The reused standalone integration suites each own the same fixture module."
)]

use kakune_core::initialization::{
    CredentialAccess, CredentialAccessError, CredentialReference, CredentialSecret,
    InitializationPaths, initialize,
};
use std::{fs, process::Command, sync::Mutex};

#[path = "daemon_commands.rs"]
mod daemon_acceptance;
#[path = "service_commands.rs"]
mod service_acceptance;

#[derive(Default)]
struct TestCredentials {
    value: Mutex<Option<String>>,
    deny_writes: bool,
}

impl CredentialAccess for TestCredentials {
    fn resolve_reference(
        &self,
        reference: Option<&str>,
    ) -> Result<CredentialReference, CredentialAccessError> {
        CredentialReference::new(reference.unwrap_or("keychain:canonical-fixture").to_owned())
    }

    fn write_secure(
        &self,
        _reference: &CredentialReference,
        secret: &CredentialSecret,
    ) -> Result<(), CredentialAccessError> {
        if self.deny_writes {
            return Err(CredentialAccessError::SecureWriteFailed);
        }
        *self.value.lock().unwrap() = Some(secret.expose_secret().to_owned());
        Ok(())
    }

    fn read(
        &self,
        _reference: &CredentialReference,
    ) -> Result<CredentialSecret, CredentialAccessError> {
        self.value
            .lock()
            .unwrap()
            .clone()
            .map(CredentialSecret::new)
            .ok_or(CredentialAccessError::Unavailable)
    }
}

fn initialization_paths(root: &std::path::Path) -> InitializationPaths {
    InitializationPaths {
        data_dir: root.join("data"),
        config_file: root.join("config.yaml"),
        context_file: root.join("contexts.json"),
    }
}

#[test]
fn repeated_init_preserves_secure_credentials_configuration_and_identity() {
    let root = std::env::temp_dir()
        .join("opencode")
        .join(format!("canonical-init-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&root).unwrap();
    let paths = initialization_paths(&root);
    let credentials = TestCredentials::default();
    assert!(
        initialize(paths.clone(), &credentials)
            .unwrap()
            .is_complete()
    );
    let credential = credentials.value.lock().unwrap().clone().unwrap();
    let config = fs::read(&paths.config_file).unwrap();
    let contexts = fs::read(&paths.context_file).unwrap();
    let identity = kakune_core::Store::open(paths.data_dir.clone())
        .unwrap()
        .core_id()
        .unwrap();
    assert!(
        initialize(paths.clone(), &credentials)
            .unwrap()
            .is_complete()
    );
    assert_eq!(
        credentials.value.lock().unwrap().as_ref(),
        Some(&credential)
    );
    assert_eq!(fs::read(&paths.config_file).unwrap(), config);
    assert_eq!(fs::read(&paths.context_file).unwrap(), contexts);
    assert_eq!(
        kakune_core::Store::open(paths.data_dir.clone())
            .unwrap()
            .core_id()
            .unwrap(),
        identity
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn denied_secure_credential_write_does_not_claim_complete_initialization() {
    let root = std::env::temp_dir()
        .join("opencode")
        .join(format!("canonical-init-denied-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&root).unwrap();
    let credentials = TestCredentials {
        deny_writes: true,
        ..Default::default()
    };
    let result = initialize(initialization_paths(&root), &credentials);
    assert!(result.is_err());
    assert!(credentials.value.lock().unwrap().is_none());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn versions_and_daemon_status_do_not_initialize_an_installation() {
    let root = std::env::temp_dir()
        .join("opencode")
        .join(format!("canonical-general-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&root).unwrap();
    let data = root.join("not-initialized");
    let run = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_kakune"))
            .env("KAKUNE_DATA_DIR", &data)
            .args(args)
            .output()
            .unwrap()
    };
    let version = run(&["version"]);
    let flag = run(&["--version"]);
    assert!(version.status.success());
    assert!(flag.status.success());
    assert_eq!(version.stdout, flag.stdout);
    let daemon = Command::new(env!("CARGO_BIN_EXE_kakune"))
        .env("KAKUNE_DATA_DIR", &data)
        .args(["daemon", "status", "--data-dir"])
        .arg(&data)
        .output()
        .unwrap();
    assert!(
        daemon.status.success(),
        "{}",
        String::from_utf8_lossy(&daemon.stderr)
    );
    assert!(!data.exists());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn local_init_rejects_remote_context_before_any_writes() {
    let data = std::env::temp_dir()
        .join("opencode")
        .join(format!("canonical-init-denied-{}", uuid::Uuid::new_v4()));
    let output = Command::new(env!("CARGO_BIN_EXE_kakune"))
        .env("KAKUNE_DATA_DIR", &data)
        .args(["--context", "remote", "init", "--data-dir"])
        .arg(&data)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(!data.exists());
}

#[test]
fn doctor_reports_missing_installation_without_creating_it() {
    let root = std::env::temp_dir()
        .join("opencode")
        .join(format!("canonical-doctor-{}", uuid::Uuid::new_v4()));
    let data = root.join("data");
    let config = root.join("config.yaml");
    let output = Command::new(env!("CARGO_BIN_EXE_kakune"))
        .env("KAKUNE_DATA_DIR", &data)
        .args(["doctor", "--data-dir"])
        .arg(&data)
        .arg("--config")
        .arg(&config)
        .arg("--json")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["findings"].as_array().unwrap().len(), 8);
    assert!(!root.exists());
}
