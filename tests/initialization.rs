use std::{
    fs, io,
    path::{Path, PathBuf},
    process::{Child, Command},
    sync::Mutex,
    thread,
    time::{Duration, Instant},
};

use kakune_core::{
    Store,
    client_config::SystemCredentialAccess,
    initialization::{
        ComponentState, CredentialAccess, CredentialAccessError, CredentialReference,
        CredentialSecret, InitializationPaths, initialize,
    },
};

struct InitializationFixture {
    root: PathBuf,
}

impl InitializationFixture {
    fn new() -> io::Result<Self> {
        let approved_temp_root = std::env::temp_dir().join("opencode");
        fs::create_dir_all(&approved_temp_root)?;
        let root =
            approved_temp_root.join(format!("kakune-initialization-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root)?;
        Ok(Self { root })
    }

    fn data_dir(&self) -> PathBuf {
        self.root.join("data")
    }

    fn config_file(&self) -> PathBuf {
        self.root.join("configuration").join("kakune.yaml")
    }

    fn context_file(&self) -> PathBuf {
        self.root.join("client").join("contexts.json")
    }

    fn paths(&self) -> InitializationPaths {
        InitializationPaths {
            data_dir: self.root.join("data with spaces-é"),
            config_file: self
                .root
                .join("configuration with spaces-é")
                .join("kakune.yaml"),
            context_file: self.root.join("client with spaces-é").join("contexts.json"),
        }
    }

    fn write_file(&self, path: &Path, contents: &[u8]) -> io::Result<()> {
        let parent = path
            .parent()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no parent"))?;
        fs::create_dir_all(parent)?;
        fs::write(path, contents)
    }

    fn open_store(&self) -> Result<Store, String> {
        Store::open(self.data_dir())
    }
}

impl Drop for InitializationFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[derive(Clone, Copy)]
enum CredentialBehavior {
    Success,
    WriteFailure,
    ReadFailure,
    WrongSecret,
    Unavailable,
}

struct CredentialDouble {
    behavior: CredentialBehavior,
    saved_secret: Mutex<Option<CredentialSecret>>,
    saved_reference: Mutex<Option<String>>,
}

impl CredentialDouble {
    fn new(behavior: CredentialBehavior) -> Self {
        Self {
            behavior,
            saved_secret: Mutex::new(None),
            saved_reference: Mutex::new(None),
        }
    }

    fn save_secret_at(&self, reference: &str, secret: &CredentialSecret) {
        *self
            .saved_secret
            .lock()
            .expect("credential fixture lock should not be poisoned") =
            Some(CredentialSecret::new(secret.expose_secret().to_owned()));
        *self
            .saved_reference
            .lock()
            .expect("credential reference fixture lock should not be poisoned") =
            Some(reference.to_owned());
    }

    fn saved_secret(&self) -> Option<CredentialSecret> {
        self.saved_secret
            .lock()
            .ok()?
            .as_ref()
            .map(|secret| CredentialSecret::new(secret.expose_secret().to_owned()))
    }
}

impl CredentialAccess for CredentialDouble {
    fn resolve_reference(
        &self,
        reference: Option<&str>,
    ) -> Result<CredentialReference, CredentialAccessError> {
        CredentialReference::new(reference.unwrap_or("env:KAKUNE_TOKEN").to_string())
    }

    fn write_secure(
        &self,
        reference: &CredentialReference,
        secret: &CredentialSecret,
    ) -> Result<(), CredentialAccessError> {
        if matches!(self.behavior, CredentialBehavior::WriteFailure) {
            return Err(CredentialAccessError::SecureWriteFailed);
        }

        let mut saved_secret = self
            .saved_secret
            .lock()
            .map_err(|_| CredentialAccessError::SecureWriteFailed)?;
        *saved_secret = Some(CredentialSecret::new(secret.expose_secret().to_owned()));
        *self
            .saved_reference
            .lock()
            .map_err(|_| CredentialAccessError::SecureWriteFailed)? =
            Some(reference.as_str().to_owned());
        Ok(())
    }

    fn read(
        &self,
        reference: &CredentialReference,
    ) -> Result<CredentialSecret, CredentialAccessError> {
        match self.behavior {
            CredentialBehavior::Unavailable => Err(CredentialAccessError::Unavailable),
            CredentialBehavior::ReadFailure => Err(CredentialAccessError::ReadFailed),
            CredentialBehavior::WrongSecret => {
                if self
                    .saved_reference
                    .lock()
                    .map_err(|_| CredentialAccessError::Unavailable)?
                    .as_deref()
                    != Some(reference.as_str())
                {
                    return Err(CredentialAccessError::Unavailable);
                }
                Ok(CredentialSecret::new("wrong-test-secret".to_string()))
            }
            CredentialBehavior::Success | CredentialBehavior::WriteFailure => {
                if self
                    .saved_reference
                    .lock()
                    .map_err(|_| CredentialAccessError::Unavailable)?
                    .as_deref()
                    != Some(reference.as_str())
                {
                    return Err(CredentialAccessError::Unavailable);
                }
                self.saved_secret
                    .lock()
                    .map_err(|_| CredentialAccessError::Unavailable)?
                    .as_ref()
                    .map(|secret| CredentialSecret::new(secret.expose_secret().to_owned()))
                    .ok_or(CredentialAccessError::Unavailable)
            }
        }
    }
}

struct NativeCredentialCleanup {
    entry: keyring::Entry,
    armed: bool,
}

impl NativeCredentialCleanup {
    fn new(account: &str) -> Self {
        Self {
            entry: keyring::Entry::new("dev.kakune.cli", account)
                .expect("native smoke credential entry should be constructible"),
            armed: true,
        }
    }

    fn remove(mut self) {
        self.entry
            .delete_credential()
            .expect("disposable native smoke credential should be removable");
        self.armed = false;
        assert!(
            self.entry.get_password().is_err(),
            "native smoke credential should be absent after cleanup"
        );
    }
}

impl Drop for NativeCredentialCleanup {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.entry.delete_credential();
        }
    }
}

struct BlockingCredential {
    ready: PathBuf,
    release: PathBuf,
    saved_secret: Mutex<Option<CredentialSecret>>,
}

impl CredentialAccess for BlockingCredential {
    fn resolve_reference(
        &self,
        reference: Option<&str>,
    ) -> Result<CredentialReference, CredentialAccessError> {
        CredentialReference::new(reference.unwrap_or("env:KAKUNE_TOKEN"))
    }

    fn write_secure(
        &self,
        _reference: &CredentialReference,
        secret: &CredentialSecret,
    ) -> Result<(), CredentialAccessError> {
        fs::write(&self.ready, b"ready").map_err(|_| CredentialAccessError::SecureWriteFailed)?;
        wait_for_marker(&self.release, Duration::from_secs(10))
            .map_err(|_| CredentialAccessError::SecureWriteFailed)?;
        *self
            .saved_secret
            .lock()
            .map_err(|_| CredentialAccessError::SecureWriteFailed)? =
            Some(CredentialSecret::new(secret.expose_secret().to_owned()));
        Ok(())
    }

    fn read(
        &self,
        _reference: &CredentialReference,
    ) -> Result<CredentialSecret, CredentialAccessError> {
        self.saved_secret
            .lock()
            .map_err(|_| CredentialAccessError::Unavailable)?
            .as_ref()
            .map(|secret| CredentialSecret::new(secret.expose_secret().to_owned()))
            .ok_or(CredentialAccessError::Unavailable)
    }
}

fn wait_for_marker(path: &Path, timeout: Duration) -> io::Result<()> {
    let started = Instant::now();
    while started.elapsed() < timeout {
        if path.is_file() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(10));
    }
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        "marker was not published before the bounded test deadline",
    ))
}

fn finish_child(child: &mut Child, timeout: Duration) -> io::Result<std::process::ExitStatus> {
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if started.elapsed() >= timeout {
            let _ = child.kill();
            let _ = child.wait();
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "initialization test child exceeded its deadline",
            ));
        }
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn fixture_isolates_real_sqlite_and_filesystem_resources() {
    let fixture = InitializationFixture::new().expect("fixture directory should be created");
    let config_file = fixture.config_file();
    let context_file = fixture.context_file();
    fixture
        .write_file(&config_file, b"isolated configuration fixture")
        .expect("fixture file should be written");
    assert_eq!(
        fs::read(&config_file).expect("fixture file should be readable"),
        b"isolated configuration fixture"
    );
    assert_eq!(
        context_file.file_name().and_then(|name| name.to_str()),
        Some("contexts.json")
    );

    let store = fixture
        .open_store()
        .expect("isolated SQLite store should open");
    assert!(
        !store
            .core_id()
            .expect("fixture should have a core identity")
            .is_empty()
    );
    assert!(fixture.data_dir().is_dir());
}

#[test]
fn credential_doubles_cover_success_and_deterministic_failures_per_instance() {
    let secret = CredentialSecret::new("fixture-test-secret".to_string());
    let reference = "keychain:kakune/core/fixture";

    let success = CredentialDouble::new(CredentialBehavior::Success);
    let success_reference = success
        .resolve_reference(Some(reference))
        .expect("test reference should resolve");
    success
        .write_secure(&success_reference, &secret)
        .expect("success double should save the secret");
    let readback = success
        .read(&success_reference)
        .expect("success double should read the secret back");
    assert!(secret.matches(&readback));

    let other_instance = CredentialDouble::new(CredentialBehavior::Success);
    assert_eq!(
        other_instance.read(&success_reference).err(),
        Some(CredentialAccessError::Unavailable)
    );

    let write_failure = CredentialDouble::new(CredentialBehavior::WriteFailure);
    assert_eq!(
        write_failure.write_secure(&success_reference, &secret),
        Err(CredentialAccessError::SecureWriteFailed)
    );

    let read_failure = CredentialDouble::new(CredentialBehavior::ReadFailure);
    read_failure
        .write_secure(&success_reference, &secret)
        .expect("read-failure double should still support writes");
    assert_eq!(
        read_failure.read(&success_reference).err(),
        Some(CredentialAccessError::ReadFailed)
    );

    let wrong_secret = CredentialDouble::new(CredentialBehavior::WrongSecret);
    wrong_secret
        .write_secure(&success_reference, &secret)
        .expect("wrong-secret double should accept the write");
    let mismatched = wrong_secret
        .read(&success_reference)
        .expect("wrong-secret double should return a deterministic value");
    assert!(!secret.matches(&mismatched));
}

#[test]
fn initializes_empty_installation_with_secure_admin_access_and_explicit_paths() {
    let fixture = InitializationFixture::new().expect("fixture directory should be created");
    let paths = fixture.paths();
    let credentials = CredentialDouble::new(CredentialBehavior::Success);

    let report = initialize(paths.clone(), &credentials)
        .expect("empty installation should initialize completely");

    assert!(report.is_complete());
    assert_eq!(report.paths, paths);
    assert_eq!(report.configuration, ComponentState::Created);
    assert_eq!(report.storage, ComponentState::Created);
    assert_eq!(report.identity, ComponentState::Created);
    assert_eq!(report.client_access, ComponentState::Created);
    assert_eq!(report.local_context, ComponentState::Created);

    let config = fs::read_to_string(&report.paths.config_file)
        .expect("default YAML configuration should be published");
    assert!(serde_yaml::from_str::<kakune_core::CoreConfig>(&config).is_ok());

    let store =
        Store::open(report.paths.data_dir.clone()).expect("initialized SQLite storage should open");
    let core_id = store.core_id().expect("installation identity should exist");
    assert!(!core_id.is_empty());
    let tokens = store
        .list_auth_tokens()
        .expect("initial credential record should be readable");
    assert_eq!(tokens.len(), 1);
    assert!(tokens[0].scopes.contains(&kakune_core::AuthScope::Admin));

    let saved_secret = credentials
        .saved_secret()
        .expect("initial secret should be written to the secure test backend");
    assert!(
        store
            .authorize_scope(saved_secret.expose_secret(), kakune_core::AuthScope::Admin)
            .expect("saved access should authorize locally")
    );

    let context = kakune_core::ContextFile::load(&report.paths.context_file)
        .expect("local connection metadata should be valid");
    assert_eq!(context.contexts.len(), 1);
    assert_eq!(context.contexts[0].id, "local");
    assert_eq!(
        context.contexts[0].expected_core_id.as_deref(),
        Some(core_id.as_str())
    );
    assert_eq!(
        context.contexts[0].credential_ref.as_deref(),
        Some(format!("keychain:kakune/core/{core_id}").as_str())
    );
    assert!(!format!("{report:?}").contains(saved_secret.expose_secret()));
    assert!(!config.contains(saved_secret.expose_secret()));
    let metadata = fs::read_to_string(&report.paths.context_file)
        .expect("connection metadata should be readable");
    assert!(!metadata.contains(saved_secret.expose_secret()));
}

#[test]
#[ignore = "opt-in native OS credential-manager smoke test; uses and deletes a disposable entry"]
fn native_keyring_first_use_and_readback() {
    let fixture = InitializationFixture::new().expect("fixture directory should be created");
    let paths = fixture.paths();
    let credentials = SystemCredentialAccess;

    let result = initialize(paths.clone(), &credentials);
    let store = Store::open(paths.data_dir.clone())
        .expect("native smoke initialization should leave a readable isolated store");
    let core_id = store
        .core_id()
        .expect("isolated native smoke store should have an identity");
    let account = format!("kakune/core/{core_id}");
    let cleanup = NativeCredentialCleanup::new(&account);

    let report = result.expect("native keyring first-use save/readback should complete");
    assert!(report.is_complete());
    let contexts = kakune_core::ContextFile::load(&paths.context_file)
        .expect("native smoke metadata should be valid");
    let reference = contexts
        .contexts
        .iter()
        .find(|context| context.id == "local")
        .and_then(|context| context.credential_ref.as_deref())
        .expect("native smoke context should carry a portable reference");
    assert_eq!(reference, format!("keychain:{account}"));

    let secret = credentials
        .read(&CredentialReference::new(reference).expect("reference should be valid"))
        .expect("native keyring readback should retrieve the saved credential");
    assert!(
        store
            .authorize_scope(secret.expose_secret(), kakune_core::AuthScope::Admin)
            .expect("native smoke credential should be checked locally"),
        "native smoke credential should have administrative access"
    );

    drop(secret);
    drop(store);
    cleanup.remove();
}

#[test]
#[ignore = "opt-in native filesystem publication smoke test using an isolated fixture"]
fn native_filesystem_publication() {
    let fixture = InitializationFixture::new().expect("fixture directory should be created");
    let paths = fixture.paths();
    let credentials = CredentialDouble::new(CredentialBehavior::Success);

    let first = initialize(paths.clone(), &credentials)
        .expect("native no-replace publications should create first-use resources");
    assert!(first.is_complete());
    assert!(paths.config_file.is_file());
    assert!(paths.context_file.is_file());

    fs::remove_file(&paths.config_file)
        .expect("isolated configuration should be removable for the publication check");
    let completed = initialize(paths.clone(), &credentials)
        .expect("native config publication should recreate only the missing file");
    assert_eq!(completed.configuration, ComponentState::Created);

    let remote_only = serde_json::json!({
        "format": "kakune-contexts/v1",
        "exportedAt": "2026-10-03T00:00:00Z",
        "activeContextId": "remote",
        "operatorMetadata": {"owner": "native-publication-smoke"},
        "contexts": [{
            "id": "remote",
            "name": "Keep this context",
            "endpoint": "https://operator.example.test:9443",
            "operatorField": "preserve-me"
        }]
    });
    fs::write(
        &paths.context_file,
        serde_json::to_vec_pretty(&remote_only).expect("isolated context should serialize"),
    )
    .expect("isolated context fixture should be written");

    let merged = initialize(paths.clone(), &credentials)
        .expect("native atomic replacement should merge the missing local context");
    assert_eq!(merged.local_context, ComponentState::Created);
    let merged_json: serde_json::Value = serde_json::from_slice(
        &fs::read(&paths.context_file).expect("published context should remain readable"),
    )
    .expect("published context should be valid JSON");
    assert_eq!(merged_json["activeContextId"], "remote");
    assert_eq!(
        merged_json["operatorMetadata"],
        remote_only["operatorMetadata"]
    );
    assert_eq!(merged_json["contexts"][0]["operatorField"], "preserve-me");
    assert_eq!(merged_json["contexts"].as_array().unwrap().len(), 2);

    for parent in [
        paths.config_file.parent().unwrap(),
        paths.context_file.parent().unwrap(),
    ] {
        let has_temporary_file = fs::read_dir(parent)
            .expect("publication directory should be readable")
            .filter_map(Result::ok)
            .any(|entry| {
                let name = entry.file_name().to_string_lossy().into_owned();
                name.starts_with(".kakune-") && name.ends_with(".tmp")
            });
        assert!(
            !has_temporary_file,
            "publication should clean owned temporaries"
        );
    }
}

#[test]
fn secure_credential_failures_report_partial_setup_without_exposing_secrets() {
    for behavior in [
        CredentialBehavior::WriteFailure,
        CredentialBehavior::ReadFailure,
        CredentialBehavior::WrongSecret,
    ] {
        let fixture = InitializationFixture::new().expect("fixture directory should be created");
        let credentials = CredentialDouble::new(behavior);
        let paths = fixture.paths();

        let error = initialize(paths, &credentials)
            .expect_err("failed secure persistence/readback must not report success");

        assert!(!error.report.is_complete());
        assert_eq!(error.report.configuration, ComponentState::Created);
        assert_eq!(error.report.storage, ComponentState::Created);
        assert_eq!(error.report.identity, ComponentState::Created);
        assert_eq!(error.report.client_access, ComponentState::Incomplete);
        assert_eq!(error.report.local_context, ComponentState::NotAttempted);
        assert!(!format!("{error:?}").contains("fixture-test-secret"));
        assert!(!error.to_string().contains("fixture-test-secret"));
        assert!(!error.to_string().contains("kakune_"));

        let store = Store::open(error.report.paths.data_dir.clone())
            .expect("durable first-use state should remain after secure-store failure");
        assert_eq!(
            store
                .list_auth_tokens()
                .expect("credential history should remain")
                .len(),
            1,
            "the durable issuance history must prevent retry-based rotation"
        );
        assert!(!error.report.paths.context_file.exists());

        let retry = initialize(
            error.report.paths.clone(),
            &CredentialDouble::new(CredentialBehavior::Success),
        )
        .expect_err("retry with an unavailable prior credential must not rotate access");
        assert_eq!(
            retry.category,
            kakune_core::initialization::InitializationFailureCategory::ClientAccessUnavailable
        );
        assert_eq!(retry.report.client_access, ComponentState::Incomplete);
        let store = Store::open(retry.report.paths.data_dir)
            .expect("the retry must retain the original storage");
        assert_eq!(store.list_auth_tokens().unwrap().len(), 1);
    }
}

#[test]
fn invalid_existing_configuration_is_preserved_and_prevents_storage_creation() {
    let fixture = InitializationFixture::new().expect("fixture directory should be created");
    let paths = fixture.paths();
    let original = b"api:\n  listen: [not a socket address]\n";
    fixture
        .write_file(&paths.config_file, original)
        .expect("invalid existing YAML should be written");

    let error = initialize(
        paths.clone(),
        &CredentialDouble::new(CredentialBehavior::Success),
    )
    .expect_err("invalid existing YAML must not be overwritten");

    assert_eq!(error.report.configuration, ComponentState::Incomplete);
    assert_eq!(error.report.storage, ComponentState::NotAttempted);
    assert_eq!(fs::read(&paths.config_file).unwrap(), original);
    assert!(!paths.data_dir.exists());
}

#[test]
fn data_location_that_is_a_file_fails_without_overwriting_it() {
    let fixture = InitializationFixture::new().expect("fixture directory should be created");
    let paths = fixture.paths();
    fs::write(&paths.data_dir, b"user data").expect("data-path file should be created");
    let original = fs::read(&paths.data_dir).expect("data-path contents should be readable");

    let error = initialize(
        paths.clone(),
        &CredentialDouble::new(CredentialBehavior::Success),
    )
    .expect_err("a file cannot be used as the data directory");

    assert_eq!(error.report.storage, ComponentState::Incomplete);
    assert_eq!(error.report.identity, ComponentState::NotAttempted);
    assert_eq!(fs::read(&paths.data_dir).unwrap(), original);
    assert_eq!(
        error.resource,
        kakune_core::initialization::InitializationResource::Storage
    );
    assert_eq!(
        error.correction,
        kakune_core::initialization::InitializationCorrection::InspectResource
    );
}

#[test]
fn initialization_context_publication_failure_retains_first_use_history() {
    let fixture = InitializationFixture::new().expect("fixture directory should be created");
    let mut paths = fixture.paths();
    let context_parent = fixture.root.join("contexts-parent-is-a-file");
    fs::write(&context_parent, b"preserve this file").expect("blocking file should be written");
    paths.context_file = context_parent.join("contexts.json");
    let credentials = CredentialDouble::new(CredentialBehavior::Success);

    let error = initialize(paths.clone(), &credentials)
        .expect_err("context publication failure must not report complete setup");

    assert_eq!(error.report.configuration, ComponentState::Created);
    assert_eq!(error.report.storage, ComponentState::Created);
    assert_eq!(error.report.identity, ComponentState::Created);
    assert_eq!(error.report.client_access, ComponentState::Created);
    assert_eq!(error.report.local_context, ComponentState::Incomplete);
    assert_eq!(
        error.resource,
        kakune_core::initialization::InitializationResource::LocalContext
    );
    assert_eq!(
        error.correction,
        kakune_core::initialization::InitializationCorrection::InspectResource
    );
    assert!(!error.to_string().contains("kakune_"));
    assert_eq!(fs::read(&context_parent).unwrap(), b"preserve this file");
    let store = Store::open(paths.data_dir).expect("durable store should remain");
    assert_eq!(store.list_auth_tokens().unwrap().len(), 1);
    assert!(credentials.saved_secret().is_some());
}

#[test]
fn existing_zero_byte_and_schema_less_databases_fail_unchanged() {
    for database_state in ["zero-byte", "schema-less", "corrupt"] {
        let fixture = InitializationFixture::new().expect("fixture directory should be created");
        let paths = fixture.paths();
        fs::create_dir_all(&paths.data_dir).expect("data directory should be created");
        let database = paths.data_dir.join("kakune.sqlite3");
        if database_state == "schema-less" {
            let connection = rusqlite::Connection::open(&database)
                .expect("schema-less SQLite file should be created");
            drop(connection);
        } else if database_state == "corrupt" {
            fs::write(&database, b"not a SQLite database")
                .expect("corrupt database fixture should be created");
        } else {
            fs::write(&database, []).expect("zero-byte database fixture should be created");
        }
        let original = fs::read(&database).expect("database bytes should be readable");

        let error = initialize(
            paths.clone(),
            &CredentialDouble::new(CredentialBehavior::Success),
        )
        .expect_err("ambiguous or corrupt preexisting database must fail conservatively");

        assert_eq!(error.report.storage, ComponentState::Incomplete);
        assert_eq!(error.report.identity, ComponentState::NotAttempted);
        assert_eq!(error.report.client_access, ComponentState::NotAttempted);
        assert_eq!(fs::read(&database).unwrap(), original);
        assert!(!paths.context_file.exists());
    }
}

#[test]
fn unsupported_future_database_schema_fails_without_mutating_database_bytes() {
    let fixture = InitializationFixture::new().expect("fixture directory should be created");
    let paths = fixture.paths();
    let store = Store::open(paths.data_dir.clone()).expect("current store should be created");
    drop(store);
    let database = paths.data_dir.join("kakune.sqlite3");
    let connection = rusqlite::Connection::open(&database)
        .expect("current database should be opened for fixture setup");
    connection
        .execute("DELETE FROM schema_migrations", [])
        .expect("migration records should be replaced");
    connection
        .execute("INSERT INTO schema_migrations (version) VALUES (999)", [])
        .expect("future schema version should be recorded");
    drop(connection);
    let original = fs::read(&database).expect("future database bytes should be captured");

    let error = initialize(
        paths.clone(),
        &CredentialDouble::new(CredentialBehavior::Success),
    )
    .expect_err("unsupported future schema must not be migrated or reset");

    assert_eq!(error.report.storage, ComponentState::Incomplete);
    assert_eq!(error.report.identity, ComponentState::NotAttempted);
    assert_eq!(fs::read(database).unwrap(), original);
}

#[test]
fn supported_existing_database_and_legacy_upgrade_never_issue_initial_access() {
    let fixture = InitializationFixture::new().expect("fixture directory should be created");
    let paths = fixture.paths();
    let existing = Store::open(paths.data_dir.clone()).expect("current store should be created");
    drop(existing);
    let credentials = CredentialDouble::new(CredentialBehavior::Success);

    let error = initialize(paths.clone(), &credentials)
        .expect_err("existing storage without usable access must not bootstrap a token");
    assert_eq!(error.report.storage, ComponentState::Reused);
    assert_eq!(error.report.identity, ComponentState::Reused);
    assert_eq!(error.report.client_access, ComponentState::Incomplete);
    let existing = Store::open(paths.data_dir.clone()).expect("current store should remain valid");
    assert!(existing.list_auth_tokens().unwrap().is_empty());
    drop(existing);

    let legacy_fixture =
        InitializationFixture::new().expect("legacy fixture directory should be created");
    let legacy_paths = legacy_fixture.paths();
    fs::create_dir_all(&legacy_paths.data_dir).expect("legacy data directory should be created");
    let legacy_database = legacy_paths.data_dir.join("kakune.sqlite3");
    let legacy =
        rusqlite::Connection::open(&legacy_database).expect("legacy database should be created");
    legacy
        .execute_batch(
            "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY);
             CREATE TABLE workflows (
               id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, source TEXT NOT NULL,
               status TEXT NOT NULL, updated_at TEXT NOT NULL
             );
             CREATE TABLE node_runs (
               id INTEGER PRIMARY KEY, execution_id TEXT NOT NULL, node_id TEXT NOT NULL,
               status TEXT NOT NULL, message TEXT, created_at TEXT NOT NULL
             );
             CREATE TABLE auth_tokens (
               id TEXT PRIMARY KEY, token_hash TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL,
               revoked_at TEXT
             );
             CREATE TABLE installed_plugins (
               id TEXT PRIMARY KEY, name TEXT NOT NULL, version TEXT NOT NULL,
               manifest_path TEXT NOT NULL, installed_at TEXT NOT NULL
             );
             -- Intentionally no migration record: the supported legacy opening
             -- creates the current identity while preserving this database.",
        )
        .expect("supported legacy schema should be created");
    drop(legacy);

    let error = initialize(
        legacy_paths.clone(),
        &CredentialDouble::new(CredentialBehavior::Success),
    )
    .expect_err("legacy migration must not grant initial issuance eligibility");
    assert_eq!(error.report.storage, ComponentState::Reused);
    assert_eq!(error.report.client_access, ComponentState::Incomplete);
    assert!(
        legacy_paths
            .data_dir
            .join("backups")
            .join("kakune.sqlite3.before-migration-v1.sqlite3")
            .is_file()
    );
    let upgraded = Store::open(legacy_paths.data_dir.clone())
        .expect("legacy store should be migrated and preserved");
    assert!(upgraded.list_auth_tokens().unwrap().is_empty());
}

#[test]
fn configuration_publication_failure_does_not_create_storage() {
    let fixture = InitializationFixture::new().expect("fixture directory should be created");
    let mut paths = fixture.paths();
    let parent_file = fixture.root.join("configuration-parent-is-a-file");
    fs::write(&parent_file, b"preserve configuration parent").unwrap();
    paths.config_file = parent_file.join("kakune.yaml");

    let error = initialize(
        paths.clone(),
        &CredentialDouble::new(CredentialBehavior::Success),
    )
    .expect_err("configuration publication must fail on a file parent");

    assert_eq!(error.report.configuration, ComponentState::Incomplete);
    assert_eq!(error.report.storage, ComponentState::NotAttempted);
    assert!(!paths.data_dir.exists());
    assert_eq!(
        fs::read(parent_file).unwrap(),
        b"preserve configuration parent"
    );
}

#[test]
fn malformed_or_conflicting_local_metadata_is_preserved_before_credential_issuance() {
    let malformed_fixture =
        InitializationFixture::new().expect("malformed fixture directory should be created");
    let malformed_paths = malformed_fixture.paths();
    let malformed = b"{not valid JSON";
    malformed_fixture
        .write_file(&malformed_paths.context_file, malformed)
        .expect("malformed context metadata should be written");

    let malformed_error = initialize(
        malformed_paths.clone(),
        &CredentialDouble::new(CredentialBehavior::Success),
    )
    .expect_err("malformed existing metadata must fail before token issuance");
    assert_eq!(
        malformed_error.report.local_context,
        ComponentState::Incomplete
    );
    let malformed_store = Store::open(malformed_paths.data_dir.clone())
        .expect("identity created before metadata validation should remain");
    assert!(malformed_store.list_auth_tokens().unwrap().is_empty());
    assert_eq!(fs::read(&malformed_paths.context_file).unwrap(), malformed);

    let conflict_fixture =
        InitializationFixture::new().expect("conflicting fixture directory should be created");
    let conflict_paths = conflict_fixture.paths();
    let conflicting_context = serde_json::json!({
        "format": "kakune-contexts/v1",
        "exportedAt": "2026-01-01T00:00:00Z",
        "activeContextId": "remote",
        "contexts": [{
            "id": "local",
            "name": "User-owned local context",
            "endpoint": "https://custom.example.test:9443",
            "expectedCoreId": "different-installation",
            "color": "#123456",
            "credentialRef": "keychain:preserve-me"
        }, {
            "id": "remote",
            "name": "Remote",
            "endpoint": "https://remote.example.test",
            "expectedCoreId": null,
            "color": null,
            "credentialRef": "keychain:remote"
        }]
    });
    let original = serde_json::to_vec_pretty(&conflicting_context).unwrap();
    conflict_fixture
        .write_file(&conflict_paths.context_file, &original)
        .expect("conflicting context metadata should be written");

    let conflict_error = initialize(
        conflict_paths.clone(),
        &CredentialDouble::new(CredentialBehavior::Success),
    )
    .expect_err("a local context bound to another identity must not be replaced");
    assert_eq!(
        conflict_error.category,
        kakune_core::initialization::InitializationFailureCategory::ConflictingResource
    );
    let conflict_store = Store::open(conflict_paths.data_dir.clone())
        .expect("new identity should remain after a context conflict");
    assert!(conflict_store.list_auth_tokens().unwrap().is_empty());
    assert_eq!(fs::read(&conflict_paths.context_file).unwrap(), original);
}

#[test]
fn creates_local_context_without_changing_existing_active_remote_selection() {
    let fixture = InitializationFixture::new().expect("fixture directory should be created");
    let paths = fixture.paths();
    let original = serde_json::json!({
        "format": "kakune-contexts/v1",
        "exportedAt": "2026-01-01T00:00:00Z",
        "activeContextId": "remote",
        "contexts": [{
            "id": "remote",
            "name": "User's remote",
            "endpoint": "https://custom.example.test:9443",
            "expectedCoreId": null,
            "color": "#abcdef",
            "credentialRef": "keychain:remote-reference"
        }]
    });
    fixture
        .write_file(
            &paths.context_file,
            &serde_json::to_vec_pretty(&original).unwrap(),
        )
        .expect("existing remote metadata should be written");

    let report = initialize(
        paths.clone(),
        &CredentialDouble::new(CredentialBehavior::Success),
    )
    .expect("a missing local entry should be added without replacing the remote entry");

    assert!(report.is_complete());
    assert_eq!(report.local_context, ComponentState::Created);
    let context_file = kakune_core::ContextFile::load(&paths.context_file)
        .expect("merged connection metadata should be valid");
    assert_eq!(context_file.active_context_id.as_deref(), Some("remote"));
    assert_eq!(context_file.contexts.len(), 2);
    let remote = context_file
        .contexts
        .iter()
        .find(|context| context.id == "remote")
        .expect("remote context should remain");
    assert_eq!(remote.name, "User's remote");
    assert_eq!(remote.endpoint, "https://custom.example.test:9443");
    assert_eq!(remote.color.as_deref(), Some("#abcdef"));
    assert_eq!(
        remote.credential_ref.as_deref(),
        Some("keychain:remote-reference")
    );
}

fn store_snapshot(store: &Store) -> serde_json::Value {
    serde_json::json!({
        "coreId": store.core_id().expect("core identity should be readable"),
        "workflows": store.list_workflows().expect("workflows should be readable"),
        "executions": store.list_executions().expect("executions should be readable"),
        "tokens": store.list_auth_tokens().expect("credential history should be readable"),
    })
}

const PRESERVATION_WORKFLOW: &str = "apiVersion: kakune/v1
kind: Workflow
metadata:
  id: phase4-preservation
  name: Phase 4 Preservation
triggers:
  - id: manual
    type: kakune.trigger.manual@1
entry: finish
nodes:
  - id: finish
    type: kakune.flow.end@1
";

#[test]
fn preserves_populated_installation_across_ten_repeats() {
    let fixture = InitializationFixture::new().expect("fixture directory should be created");
    let paths = fixture.paths();
    let credentials = CredentialDouble::new(CredentialBehavior::Success);
    initialize(paths.clone(), &credentials).expect("first-use initialization should succeed");

    let store = Store::open(paths.data_dir.clone()).expect("initialized store should open");
    let core_id = store.core_id().expect("installation identity should exist");
    let workflow = kakune_core::WorkflowDocument::parse(PRESERVATION_WORKFLOW)
        .expect("preservation workflow should parse");
    store
        .upsert_workflow(&workflow, PRESERVATION_WORKFLOW, "disabled")
        .expect("workflow should be persisted");
    store
        .create_execution("Phase 4 Preservation")
        .expect("execution history should be persisted");
    store
        .create_auth_token(
            "preservation read token".to_string(),
            vec![kakune_core::AuthScope::Read],
            None,
        )
        .expect("additional credential history should be persisted");
    let initial_secret = credentials
        .saved_secret()
        .expect("initial credential should be available in the fixture");
    drop(store);

    let mut config: kakune_core::CoreConfig = serde_yaml::from_str(
        &fs::read_to_string(&paths.config_file).expect("configuration should be readable"),
    )
    .expect("configuration should parse");
    config.api.listen = "0.0.0.0:9917".to_string();
    config.api.allowed_hosts = vec!["operator.example.test".to_string()];
    config.api.allowed_origins = vec!["https://operator.example.test".to_string()];
    config.api.request_body_limit_bytes = 2_000_000;
    fs::write(
        &paths.config_file,
        serde_yaml::to_string(&config).expect("custom configuration should serialize"),
    )
    .expect("custom configuration should be written");

    let custom_reference = "keychain:kakune/operator-managed";
    credentials.save_secret_at(custom_reference, &initial_secret);
    let customized_contexts = serde_json::json!({
        "format": "kakune-contexts/v1",
        "exportedAt": "2026-10-01T12:00:00Z",
        "activeContextId": "remote",
        "contexts": [
            {
                "id": "local",
                "name": "Operator-edited local",
                "endpoint": "https://custom-local.example.test:9443",
                "expectedCoreId": core_id,
                "color": "#123ABC",
                "credentialRef": custom_reference
            },
            {
                "id": "remote",
                "name": "Operator-edited remote",
                "endpoint": "https://remote.example.test:10443",
                "expectedCoreId": null,
                "color": "#ABC123",
                "credentialRef": "keychain:remote-operator-token"
            }
        ]
    });
    fs::write(
        &paths.context_file,
        serde_json::to_vec_pretty(&customized_contexts)
            .expect("customized contexts should serialize"),
    )
    .expect("customized contexts should be written");

    let configuration_before =
        fs::read(&paths.config_file).expect("configuration bytes should be read");
    let contexts_before = fs::read(&paths.context_file).expect("context bytes should be read");
    let initial_store = Store::open(paths.data_dir.clone()).expect("store should reopen");
    let data_before = store_snapshot(&initial_store);
    drop(initial_store);

    for run in 1..=10 {
        let report = initialize(paths.clone(), &credentials)
            .unwrap_or_else(|error| panic!("repeat initialization {run} should succeed: {error}"));
        assert!(report.is_complete(), "repeat {run} should be complete");
        assert_eq!(report.configuration, ComponentState::Reused);
        assert_eq!(report.storage, ComponentState::Reused);
        assert_eq!(report.identity, ComponentState::Reused);
        assert_eq!(report.client_access, ComponentState::Reused);
        assert_eq!(report.local_context, ComponentState::Reused);
        assert_eq!(
            fs::read(&paths.config_file).expect("configuration should remain readable"),
            configuration_before,
            "repeat {run} must preserve customized configuration bytes"
        );
        assert_eq!(
            fs::read(&paths.context_file).expect("contexts should remain readable"),
            contexts_before,
            "repeat {run} must not rewrite customized context metadata"
        );
        let current = Store::open(paths.data_dir.clone()).expect("store should remain valid");
        assert_eq!(
            store_snapshot(&current),
            data_before,
            "repeat {run} changed persisted state"
        );
    }
}

#[test]
fn creates_only_missing_resources_and_preserves_active_context() {
    let fixture = InitializationFixture::new().expect("fixture directory should be created");
    let paths = fixture.paths();
    let credentials = CredentialDouble::new(CredentialBehavior::Success);
    initialize(paths.clone(), &credentials).expect("first-use initialization should succeed");

    let store = Store::open(paths.data_dir.clone()).expect("store should open");
    let core_id = store.core_id().expect("installation identity should exist");
    let before = store_snapshot(&store);
    drop(store);
    let original_contexts = fs::read(&paths.context_file).expect("contexts should be readable");
    fs::remove_file(&paths.config_file).expect("configuration should be removed for the fixture");

    let missing_configuration = initialize(paths.clone(), &credentials)
        .expect("missing configuration should be restored from defaults");
    assert_eq!(missing_configuration.configuration, ComponentState::Created);
    assert_eq!(missing_configuration.storage, ComponentState::Reused);
    assert_eq!(missing_configuration.identity, ComponentState::Reused);
    assert_eq!(missing_configuration.client_access, ComponentState::Reused);
    assert_eq!(missing_configuration.local_context, ComponentState::Reused);
    assert_eq!(fs::read(&paths.context_file).unwrap(), original_contexts);
    let after_config_restore = Store::open(paths.data_dir.clone()).unwrap();
    assert_eq!(store_snapshot(&after_config_restore), before);
    drop(after_config_restore);

    fs::remove_file(&paths.context_file).expect("context file should be removed for the fixture");
    let missing_metadata = initialize(paths.clone(), &credentials)
        .expect("missing context metadata should be recreated using existing access");
    assert_eq!(missing_metadata.configuration, ComponentState::Reused);
    assert_eq!(missing_metadata.storage, ComponentState::Reused);
    assert_eq!(missing_metadata.identity, ComponentState::Reused);
    assert_eq!(missing_metadata.client_access, ComponentState::Reused);
    assert_eq!(missing_metadata.local_context, ComponentState::Created);
    let new_contexts = kakune_core::ContextFile::load(&paths.context_file)
        .expect("new context file should be valid");
    assert_eq!(new_contexts.active_context_id.as_deref(), Some("local"));
    let local = new_contexts
        .contexts
        .iter()
        .find(|context| context.id == "local")
        .expect("new local context should be present");
    assert_eq!(local.expected_core_id.as_deref(), Some(core_id.as_str()));
    assert_eq!(
        local.credential_ref.as_deref(),
        Some(format!("keychain:kakune/core/{core_id}").as_str())
    );

    let remote_only_contexts = serde_json::json!({
        "format": "kakune-contexts/v1",
        "exportedAt": "2026-10-01T12:00:00Z",
        "activeContextId": "remote",
        "contexts": [{
            "id": "remote",
            "name": "Keep selected remote",
            "endpoint": "https://operator.remote.example.test:8443",
            "operatorMetadata": {
                "team": "workflow-platform",
                "purpose": "preserve during local setup"
            }
        }]
    });
    fs::write(
        &paths.context_file,
        serde_json::to_vec_pretty(&remote_only_contexts).unwrap(),
    )
    .expect("remote-only context fixture should be written");
    let missing_local = initialize(paths.clone(), &credentials)
        .expect("missing local entry should be merged using existing access");
    assert_eq!(missing_local.local_context, ComponentState::Created);
    let merged = kakune_core::ContextFile::load(&paths.context_file)
        .expect("merged context file should remain valid");
    assert_eq!(merged.active_context_id.as_deref(), Some("remote"));
    assert_eq!(merged.contexts.len(), 2);
    let remote = merged
        .contexts
        .iter()
        .find(|context| context.id == "remote")
        .expect("remote context should remain");
    assert_eq!(remote.name, "Keep selected remote");
    assert_eq!(remote.endpoint, "https://operator.remote.example.test:8443");
    assert_eq!(remote.expected_core_id, None);
    assert_eq!(remote.color, None);
    assert_eq!(remote.credential_ref, None);
    let merged_json: serde_json::Value =
        serde_json::from_slice(&fs::read(&paths.context_file).unwrap()).unwrap();
    let preserved_remote = merged_json["contexts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|context| context["id"] == "remote")
        .unwrap();
    assert_eq!(
        preserved_remote, &remote_only_contexts["contexts"][0],
        "merging local metadata must preserve omitted and custom remote fields"
    );
    let after_missing_resources = Store::open(paths.data_dir.clone()).unwrap();
    assert_eq!(store_snapshot(&after_missing_resources), before);
}

#[test]
fn preserves_custom_context_with_absent_optional_fields_and_environment_fallback() {
    let fixture = InitializationFixture::new().expect("fixture directory should be created");
    let paths = fixture.paths();
    let credentials = CredentialDouble::new(CredentialBehavior::Success);
    initialize(paths.clone(), &credentials).expect("first-use initialization should succeed");
    let original_secret = credentials.saved_secret().unwrap();
    credentials.save_secret_at("env:KAKUNE_TOKEN", &original_secret);

    let custom = serde_json::json!({
        "format": "kakune-contexts/v1",
        "exportedAt": "2026-10-01T12:00:00Z",
        "activeContextId": "local",
        "contexts": [{
            "id": "local",
            "name": "Offline custom endpoint",
            "endpoint": "https://not-the-listener.example.test:9443",
            "color": "#ABCDEF"
        }]
    });
    fs::write(
        &paths.context_file,
        serde_json::to_vec_pretty(&custom).expect("custom context should serialize"),
    )
    .expect("custom context should be written");
    let before = fs::read(&paths.context_file).unwrap();

    let report = initialize(paths.clone(), &credentials)
        .expect("existing env fallback access should authorize offline");
    assert!(report.is_complete());
    assert_eq!(report.client_access, ComponentState::Reused);
    assert_eq!(report.local_context, ComponentState::Reused);
    assert_eq!(fs::read(&paths.context_file).unwrap(), before);
    let contexts = kakune_core::ContextFile::load(&paths.context_file).unwrap();
    let local = contexts
        .contexts
        .iter()
        .find(|context| context.id == "local")
        .unwrap();
    assert_eq!(local.name, "Offline custom endpoint");
    assert_eq!(local.endpoint, "https://not-the-listener.example.test:9443");
    assert_eq!(local.expected_core_id, None);
    assert_eq!(local.credential_ref, None);
    assert_eq!(local.color.as_deref(), Some("#ABCDEF"));
}

#[test]
fn rejects_unusable_existing_access_without_issuing_or_changing_credentials() {
    for failure_mode in [
        "absent",
        "inaccessible",
        "revoked",
        "expired",
        "wrong-installation",
        "insufficient-scope",
    ] {
        let fixture = InitializationFixture::new().expect("fixture directory should be created");
        let paths = fixture.paths();
        let first_use_credentials = CredentialDouble::new(CredentialBehavior::Success);
        initialize(paths.clone(), &first_use_credentials)
            .expect("first-use initialization should succeed");
        let original_secret = first_use_credentials.saved_secret().unwrap();
        let store = Store::open(paths.data_dir.clone()).expect("existing store should open");
        let core_id = store.core_id().unwrap();
        let credential_id = store.list_auth_tokens().unwrap()[0].id.clone();
        drop(store);

        let supplied = CredentialDouble::new(if failure_mode == "inaccessible" {
            CredentialBehavior::Unavailable
        } else {
            CredentialBehavior::Success
        });
        match failure_mode {
            "absent" | "inaccessible" => {}
            "revoked" => {
                let store = Store::open(paths.data_dir.clone()).unwrap();
                assert!(store.revoke_auth_token(&credential_id).unwrap());
                drop(store);
                supplied
                    .save_secret_at(&format!("keychain:kakune/core/{core_id}"), &original_secret);
            }
            "expired" => {
                let connection =
                    rusqlite::Connection::open(paths.data_dir.join("kakune.sqlite3")).unwrap();
                connection
                    .execute(
                        "UPDATE auth_tokens SET expires_at = '2000-01-01T00:00:00Z' WHERE id = ?1",
                        [&credential_id],
                    )
                    .unwrap();
                drop(connection);
                supplied
                    .save_secret_at(&format!("keychain:kakune/core/{core_id}"), &original_secret);
            }
            "wrong-installation" => {
                let foreign_store = Store::open(fixture.root.join("foreign-installation"))
                    .expect("foreign fixture store should open");
                let foreign_token = foreign_store
                    .create_auth_token(
                        "foreign admin".to_string(),
                        vec![kakune_core::AuthScope::Admin],
                        None,
                    )
                    .expect("foreign installation should have its own admin token");
                supplied.save_secret_at(
                    &format!("keychain:kakune/core/{core_id}"),
                    &CredentialSecret::new(foreign_token.token),
                );
            }
            "insufficient-scope" => {
                let connection =
                    rusqlite::Connection::open(paths.data_dir.join("kakune.sqlite3")).unwrap();
                connection
                    .execute(
                        "UPDATE auth_tokens SET scopes = '[\"read\"]' WHERE id = ?1",
                        [&credential_id],
                    )
                    .unwrap();
                drop(connection);
                supplied
                    .save_secret_at(&format!("keychain:kakune/core/{core_id}"), &original_secret);
            }
            _ => unreachable!(),
        }

        let before_store = Store::open(paths.data_dir.clone()).unwrap();
        let token_history_before = serde_json::to_value(before_store.list_auth_tokens().unwrap())
            .expect("credential records should serialize");
        drop(before_store);
        let context_before = fs::read(&paths.context_file).unwrap();
        let error = initialize(paths.clone(), &supplied)
            .expect_err("existing unusable access must fail without recovery");
        assert!(
            !error.to_string().contains("kakune_"),
            "{failure_mode} error leaked a token"
        );
        assert!(
            !format!("{error:?}").contains(original_secret.expose_secret()),
            "{failure_mode} debug error leaked the original credential"
        );
        assert_eq!(
            error.report.storage,
            ComponentState::Reused,
            "{failure_mode}"
        );
        assert_eq!(
            error.report.identity,
            ComponentState::Reused,
            "{failure_mode}"
        );
        assert_eq!(
            error.report.client_access,
            ComponentState::Incomplete,
            "{failure_mode}"
        );
        assert_eq!(
            error.report.local_context,
            ComponentState::Incomplete,
            "{failure_mode} access must leave the existing local context unready"
        );
        assert_eq!(
            error.correction,
            kakune_core::initialization::InitializationCorrection::RunExplicitAuthRecovery,
            "{failure_mode}"
        );
        assert!(!error.report.is_complete(), "{failure_mode}");
        let after_store = Store::open(paths.data_dir.clone()).unwrap();
        assert_eq!(
            serde_json::to_value(after_store.list_auth_tokens().unwrap()).unwrap(),
            token_history_before,
            "{failure_mode} access must not create, revoke, reactivate, or replace credentials"
        );
        assert_eq!(
            fs::read(&paths.context_file).unwrap(),
            context_before,
            "{failure_mode}"
        );
    }
}

#[test]
fn preserves_malformed_and_unreadable_existing_yaml_and_json() {
    for case in [
        "malformed-yaml",
        "unreadable-yaml",
        "malformed-json",
        "unreadable-json",
    ] {
        let fixture = InitializationFixture::new().expect("fixture directory should be created");
        let paths = fixture.paths();
        let (path, malformed_bytes) = match case {
            "malformed-yaml" => (&paths.config_file, Some(b"api: [invalid".as_slice())),
            "malformed-json" => (&paths.context_file, Some(b"{invalid json".as_slice())),
            "unreadable-yaml" => (&paths.config_file, None),
            "unreadable-json" => (&paths.context_file, None),
            _ => unreachable!(),
        };
        if let Some(bytes) = malformed_bytes {
            fixture
                .write_file(path, bytes)
                .expect("malformed fixture should be written");
        } else {
            fs::create_dir_all(path)
                .expect("unreadable path fixture should be created as a directory");
            fixture
                .write_file(&path.join("keep.txt"), b"preserve inaccessible resource")
                .expect("directory marker should be written");
        }
        let original = if malformed_bytes.is_some() {
            Some(fs::read(path).unwrap())
        } else {
            Some(fs::read(path.join("keep.txt")).unwrap())
        };

        let error = initialize(
            paths.clone(),
            &CredentialDouble::new(CredentialBehavior::Success),
        )
        .expect_err("malformed or unreadable existing resources must fail without replacement");
        assert!(
            !error.to_string().contains("kakune_"),
            "{case} error leaked a token"
        );
        match case {
            "malformed-yaml" | "unreadable-yaml" => {
                assert_eq!(
                    error.report.configuration,
                    ComponentState::Incomplete,
                    "{case}"
                );
                assert_eq!(error.report.storage, ComponentState::NotAttempted, "{case}");
            }
            "malformed-json" | "unreadable-json" => {
                assert_eq!(
                    error.report.configuration,
                    ComponentState::Created,
                    "{case}"
                );
                assert_eq!(error.report.storage, ComponentState::Created, "{case}");
                assert_eq!(
                    error.report.local_context,
                    ComponentState::Incomplete,
                    "{case}"
                );
                assert_eq!(
                    error.report.client_access,
                    ComponentState::NotAttempted,
                    "{case}"
                );
                let store = Store::open(paths.data_dir.clone()).unwrap();
                assert!(store.list_auth_tokens().unwrap().is_empty(), "{case}");
            }
            _ => unreachable!(),
        }
        if malformed_bytes.is_some() {
            assert_eq!(fs::read(path).unwrap(), original.unwrap(), "{case}");
        } else {
            assert_eq!(
                fs::read(path.join("keep.txt")).unwrap(),
                original.unwrap(),
                "{case}"
            );
            assert!(path.is_dir(), "{case} directory must not be replaced");
        }
    }
}

#[test]
fn preserves_existing_identity_conflicts_and_retries_partial_setup_without_rotation() {
    let fixture = InitializationFixture::new().expect("fixture directory should be created");
    let paths = fixture.paths();
    let credentials = CredentialDouble::new(CredentialBehavior::Success);
    initialize(paths.clone(), &credentials).expect("first-use initialization should succeed");

    let before_store = Store::open(paths.data_dir.clone()).unwrap();
    let token_history_before =
        serde_json::to_value(before_store.list_auth_tokens().unwrap()).unwrap();
    let core_id = before_store.core_id().unwrap();
    drop(before_store);
    let conflicting = serde_json::json!({
        "format": "kakune-contexts/v1",
        "exportedAt": "2026-10-01T12:00:00Z",
        "activeContextId": "local",
        "contexts": [{
            "id": "local",
            "name": "Do not replace this conflicting context",
            "endpoint": "https://preserve.example.test:9443",
            "expectedCoreId": "another-core-id",
            "color": "#FEDCBA",
            "credentialRef": "keychain:conflicting-reference"
        }]
    });
    fs::write(
        &paths.context_file,
        serde_json::to_vec_pretty(&conflicting).unwrap(),
    )
    .unwrap();
    let conflict_bytes = fs::read(&paths.context_file).unwrap();
    let error = initialize(paths.clone(), &credentials)
        .expect_err("a local context bound to another identity must remain a conflict");
    assert!(!error.to_string().contains("kakune_"));
    assert_eq!(
        error.category,
        kakune_core::initialization::InitializationFailureCategory::ConflictingResource
    );
    assert_eq!(fs::read(&paths.context_file).unwrap(), conflict_bytes);
    let after_conflict = Store::open(paths.data_dir.clone()).unwrap();
    assert_eq!(after_conflict.core_id().unwrap(), core_id);
    assert_eq!(
        serde_json::to_value(after_conflict.list_auth_tokens().unwrap()).unwrap(),
        token_history_before
    );
    drop(after_conflict);

    // Restore valid metadata, then simulate an interrupted completion with the
    // local metadata file absent and the existing secure reference unavailable.
    fs::write(
        &paths.context_file,
        serde_json::json!({
            "format": "kakune-contexts/v1",
            "exportedAt": "2026-10-01T12:00:00Z",
            "activeContextId": "local",
            "contexts": [{
                "id": "local",
                "name": "Local Kakune Core",
                "endpoint": "http://127.0.0.1:8787",
                "expectedCoreId": core_id,
                "color": null,
                "credentialRef": format!("keychain:kakune/core/{core_id}")
            }]
        })
        .to_string(),
    )
    .unwrap();
    fs::remove_file(&paths.context_file).unwrap();
    let unavailable = CredentialDouble::new(CredentialBehavior::Unavailable);
    let partial = initialize(paths.clone(), &unavailable)
        .expect_err("missing existing access must leave the interrupted setup incomplete");
    assert!(!partial.to_string().contains("kakune_"));
    assert_eq!(partial.report.client_access, ComponentState::Incomplete);
    assert!(!paths.context_file.exists());
    let after_partial = Store::open(paths.data_dir.clone()).unwrap();
    assert_eq!(
        serde_json::to_value(after_partial.list_auth_tokens().unwrap()).unwrap(),
        token_history_before
    );
    drop(after_partial);

    let repaired = initialize(paths.clone(), &credentials)
        .expect("retry with the original usable access should complete without issuance");
    assert!(repaired.is_complete());
    assert_eq!(repaired.storage, ComponentState::Reused);
    assert_eq!(repaired.identity, ComponentState::Reused);
    assert_eq!(repaired.client_access, ComponentState::Reused);
    assert_eq!(repaired.local_context, ComponentState::Created);
    let after_repair = Store::open(paths.data_dir.clone()).unwrap();
    assert_eq!(
        serde_json::to_value(after_repair.list_auth_tokens().unwrap()).unwrap(),
        token_history_before
    );
}

#[test]
fn process_initialization_lock_probe() {
    let Some(mode) = std::env::var_os("KAKUNE_SETUP_TEST_CHILD_MODE") else {
        return;
    };
    let paths = InitializationPaths {
        data_dir: PathBuf::from(
            std::env::var_os("KAKUNE_SETUP_TEST_DATA").expect("child data path should be set"),
        ),
        config_file: PathBuf::from(
            std::env::var_os("KAKUNE_SETUP_TEST_CONFIG").expect("child config path should be set"),
        ),
        context_file: PathBuf::from(
            std::env::var_os("KAKUNE_SETUP_TEST_CONTEXT")
                .expect("child context path should be set"),
        ),
    };
    match mode.to_string_lossy().as_ref() {
        "owner" => {
            let credentials = BlockingCredential {
                ready: PathBuf::from(
                    std::env::var_os("KAKUNE_SETUP_TEST_READY")
                        .expect("owner ready path should be set"),
                ),
                release: PathBuf::from(
                    std::env::var_os("KAKUNE_SETUP_TEST_RELEASE")
                        .expect("owner release path should be set"),
                ),
                saved_secret: Mutex::new(None),
            };
            let report = initialize(paths, &credentials)
                .expect("lock owner should complete after its bounded release");
            assert!(report.is_complete());
        }
        "contender" => {
            let error = initialize(paths, &CredentialDouble::new(CredentialBehavior::Success))
                .expect_err("contender must not enter a guarded resource");
            assert_eq!(
                error.category,
                kakune_core::initialization::InitializationFailureCategory::SetupContended
            );
        }
        other => panic!("unexpected child test mode: {other}"),
    }
}

#[test]
fn independent_processes_contend_on_installation_aliases_and_shared_external_paths() {
    let fixture = InitializationFixture::new().expect("fixture directory should be created");
    let owner_paths = fixture.paths();
    let shared_remote = serde_json::json!({
        "id": "remote",
        "name": "Remote operator context",
        "endpoint": "https://shared.example.test:9443",
        "operatorMetadata": {"owner": "platform-team"}
    });
    let initial_contexts = serde_json::json!({
        "format": "kakune-contexts/v1",
        "exportedAt": "2026-10-01T12:00:00Z",
        "activeContextId": "remote",
        "contexts": [shared_remote]
    });
    fixture
        .write_file(
            &owner_paths.context_file,
            &serde_json::to_vec_pretty(&initial_contexts).unwrap(),
        )
        .expect("shared remote context should be written before contention");
    let context_before_contention = fs::read(&owner_paths.context_file).unwrap();
    let ready = fixture.root.join("owner-ready.marker");
    let release = fixture.root.join("owner-release.marker");
    let executable = std::env::current_exe().expect("test executable path should resolve");

    let mut owner = Command::new(&executable)
        .args([
            "--exact",
            "process_initialization_lock_probe",
            "--nocapture",
        ])
        .env("KAKUNE_SETUP_TEST_CHILD_MODE", "owner")
        .env("KAKUNE_SETUP_TEST_DATA", &owner_paths.data_dir)
        .env("KAKUNE_SETUP_TEST_CONFIG", &owner_paths.config_file)
        .env("KAKUNE_SETUP_TEST_CONTEXT", &owner_paths.context_file)
        .env("KAKUNE_SETUP_TEST_READY", &ready)
        .env("KAKUNE_SETUP_TEST_RELEASE", &release)
        .spawn()
        .expect("lock owner process should spawn");
    wait_for_marker(&ready, Duration::from_secs(10))
        .expect("owner should reach secure persistence while holding its locks");

    fs::create_dir(fixture.root.join("alias-parent"))
        .expect("alias parent should be created for canonicalization");
    let alias_data_dir = fixture
        .root
        .join("alias-parent")
        .join("..")
        .join("data with spaces-é");
    let alias_paths = InitializationPaths {
        data_dir: alias_data_dir,
        config_file: owner_paths.config_file.clone(),
        context_file: owner_paths.context_file.clone(),
    };
    let mut same_installation_contender = Command::new(&executable)
        .args([
            "--exact",
            "process_initialization_lock_probe",
            "--nocapture",
        ])
        .env("KAKUNE_SETUP_TEST_CHILD_MODE", "contender")
        .env("KAKUNE_SETUP_TEST_DATA", &alias_paths.data_dir)
        .env("KAKUNE_SETUP_TEST_CONFIG", &alias_paths.config_file)
        .env("KAKUNE_SETUP_TEST_CONTEXT", &alias_paths.context_file)
        .spawn()
        .expect("same-installation contender should spawn");
    assert!(
        finish_child(&mut same_installation_contender, Duration::from_secs(10))
            .expect("alias contender should finish within its deadline")
            .success()
    );

    let shared_paths = InitializationPaths {
        data_dir: fixture.root.join("different-installation"),
        config_file: owner_paths.config_file.clone(),
        context_file: owner_paths.context_file.clone(),
    };
    let mut shared_resource_contender = Command::new(&executable)
        .args([
            "--exact",
            "process_initialization_lock_probe",
            "--nocapture",
        ])
        .env("KAKUNE_SETUP_TEST_CHILD_MODE", "contender")
        .env("KAKUNE_SETUP_TEST_DATA", &shared_paths.data_dir)
        .env("KAKUNE_SETUP_TEST_CONFIG", &shared_paths.config_file)
        .env("KAKUNE_SETUP_TEST_CONTEXT", &shared_paths.context_file)
        .spawn()
        .expect("shared-path contender should spawn");
    assert!(
        finish_child(&mut shared_resource_contender, Duration::from_secs(10))
            .expect("shared-path contender should finish within its deadline")
            .success()
    );
    assert!(!shared_paths.data_dir.exists());
    assert_eq!(
        fs::read(&owner_paths.context_file).unwrap(),
        context_before_contention,
        "contenders must not publish partial context merges while the owner is guarded"
    );

    fs::write(&release, b"release").expect("owner release marker should be published");
    assert!(
        finish_child(&mut owner, Duration::from_secs(10))
            .expect("lock owner should finish within its deadline")
            .success()
    );
    let store = Store::open(owner_paths.data_dir.clone())
        .expect("owner installation should remain valid after process exit");
    assert_eq!(store.list_auth_tokens().unwrap().len(), 1);
    let merged_contexts = kakune_core::ContextFile::load(&owner_paths.context_file)
        .expect("owner should merge a local entry after releasing the guard");
    assert_eq!(merged_contexts.active_context_id.as_deref(), Some("remote"));
    assert_eq!(merged_contexts.contexts.len(), 2);
    let merged_json: serde_json::Value =
        serde_json::from_slice(&fs::read(&owner_paths.context_file).unwrap()).unwrap();
    let preserved_remote = merged_json["contexts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|context| context["id"] == "remote")
        .unwrap();
    assert_eq!(preserved_remote, &initial_contexts["contexts"][0]);
    let retry = initialize(
        owner_paths,
        &CredentialDouble::new(CredentialBehavior::Success),
    )
    .expect_err("post-exit retry should resolve existing access, not remain locked");
    assert_eq!(
        retry.category,
        kakune_core::initialization::InitializationFailureCategory::ClientAccessUnavailable
    );
}
