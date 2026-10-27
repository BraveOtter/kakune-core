#![allow(dead_code)]

//! Isolated fixtures and deterministic adapters for Spec 003 management tests.
//!
//! Core servers run in-process on loopback, credentials are held only in memory,
//! and fixture cleanup is limited to a uniquely marked child of the approved
//! temporary fixture root. No helper accesses the host keyring or OS services.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs, io,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use axum::serve;
use kakune_core::{Store, api, config::ApiConfig};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};
use uuid::Uuid;

const FIXTURE_PREFIX: &str = "kakune-spec003-management-";
const FIXTURE_MARKER: &str = ".kakune-management-fixture";

fn approved_fixture_root() -> io::Result<PathBuf> {
    let root = std::env::temp_dir().join("opencode");
    fs::create_dir_all(&root)?;
    if fs::symlink_metadata(&root)?.file_type().is_symlink() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "approved management fixture root must not be redirected",
        ));
    }
    Ok(root)
}

/// A uniquely owned temporary root for isolated management and local-Core tests.
pub struct ManagementFixture {
    id: Uuid,
    root: PathBuf,
}

impl ManagementFixture {
    pub fn new() -> io::Result<Self> {
        let approved_root = approved_fixture_root()?;
        let id = Uuid::new_v4();
        let root = approved_root.join(format!("{FIXTURE_PREFIX}{id}"));
        fs::create_dir(&root)?;
        if let Err(error) = fs::write(root.join(FIXTURE_MARKER), id.to_string()) {
            let _ = fs::remove_dir(&root);
            return Err(error);
        }
        Ok(Self { id, root })
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn path(&self, relative: impl AsRef<Path>) -> PathBuf {
        let path = self.root.join(relative);
        assert!(
            path.starts_with(&self.root),
            "management fixture paths must remain under their owned root"
        );
        path
    }

    pub fn open_local_core(&self, name: &str) -> Result<LocalCoreFixture, String> {
        let data_dir = self.path(name);
        let store = Store::open(data_dir.clone())?;
        Ok(LocalCoreFixture { data_dir, store })
    }
}

impl Drop for ManagementFixture {
    fn drop(&mut self) {
        let Ok(approved_root) = approved_fixture_root() else {
            return;
        };
        let (Ok(approved_root), Ok(root)) =
            (approved_root.canonicalize(), self.root.canonicalize())
        else {
            return;
        };
        let expected_marker = self.id.to_string();
        if root.parent() != Some(approved_root.as_path())
            || !root
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with(FIXTURE_PREFIX))
            || fs::symlink_metadata(&self.root)
                .is_ok_and(|metadata| metadata.file_type().is_symlink())
            || fs::read_to_string(root.join(FIXTURE_MARKER))
                .ok()
                .as_deref()
                != Some(expected_marker.as_str())
        {
            return;
        }

        // Remove only this uniquely named, marker-verified fixture directory.
        let _ = fs::remove_dir_all(root);
    }
}

/// A local Store and its isolated data path, owned by a `ManagementFixture`.
pub struct LocalCoreFixture {
    pub data_dir: PathBuf,
    pub store: Store,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SnapshotEntry {
    File(Vec<u8>),
    Symlink(PathBuf),
}

/// A recursive snapshot of regular-file contents and symbolic-link targets.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FilesystemSnapshot(BTreeMap<PathBuf, SnapshotEntry>);

impl FilesystemSnapshot {
    pub fn capture(root: &Path) -> io::Result<Self> {
        let mut entries = BTreeMap::new();
        if root.exists() {
            if fs::symlink_metadata(root)?.file_type().is_symlink() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "snapshot root must not be a symbolic link",
                ));
            }
            Self::visit(root, root, &mut entries)?;
        }
        Ok(Self(entries))
    }

    fn visit(
        root: &Path,
        current: &Path,
        entries: &mut BTreeMap<PathBuf, SnapshotEntry>,
    ) -> io::Result<()> {
        for entry in fs::read_dir(current)? {
            let entry = entry?;
            let path = entry.path();
            let kind = entry.file_type()?;
            if kind.is_symlink() {
                entries.insert(
                    path.strip_prefix(root)
                        .expect("entry is under snapshot root")
                        .to_path_buf(),
                    SnapshotEntry::Symlink(fs::read_link(path)?),
                );
            } else if kind.is_dir() {
                Self::visit(root, &path, entries)?;
            } else if kind.is_file() {
                entries.insert(
                    path.strip_prefix(root)
                        .expect("entry is under snapshot root")
                        .to_path_buf(),
                    SnapshotEntry::File(fs::read(path)?),
                );
            }
        }
        Ok(())
    }

    pub fn changed_paths(&self, current: &Self) -> Vec<PathBuf> {
        let paths = self
            .0
            .keys()
            .chain(current.0.keys())
            .collect::<BTreeSet<_>>();
        paths
            .into_iter()
            .filter(|path| self.0.get(*path) != current.0.get(*path))
            .cloned()
            .collect()
    }

    pub fn ensure_unchanged(&self, root: &Path) -> io::Result<()> {
        let current = Self::capture(root)?;
        let changed = self.changed_paths(&current);
        if changed.is_empty() {
            Ok(())
        } else {
            Err(io::Error::other(format!(
                "fixture filesystem changed at: {}",
                changed
                    .iter()
                    .map(|path| path.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )))
        }
    }
}

/// One isolated Core API process, served on an ephemeral loopback port.
pub struct InProcessCore {
    pub store: Store,
    pub token: String,
    pub endpoint: String,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl InProcessCore {
    pub async fn start(data_dir: PathBuf) -> Result<Self, String> {
        let store = Store::open(data_dir)?;
        Self::start_with_store(store).await
    }

    pub async fn start_with_store(store: Store) -> Result<Self, String> {
        Self::start_with_store_and_api_config(store, ApiConfig::default()).await
    }

    pub async fn start_with_store_and_api_config(
        store: Store,
        api_config: ApiConfig,
    ) -> Result<Self, String> {
        let token = store
            .ensure_bootstrap_token()?
            .ok_or_else(|| "isolated Core did not issue its test bootstrap token".to_owned())?;
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|error| format!("cannot bind isolated Core listener: {error}"))?;
        let endpoint = format!(
            "http://{}",
            listener
                .local_addr()
                .map_err(|error| format!("cannot inspect isolated Core listener: {error}"))?
        );
        let (stop, stopped) = oneshot::channel();
        let app =
            api::router_with_plugins_and_config(store.clone(), Default::default(), api_config);
        let task = tokio::spawn(async move {
            serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
                .expect("isolated Core server should exit cleanly");
        });
        Ok(Self {
            store,
            token,
            endpoint,
            stop: Some(stop),
            task: Some(task),
        })
    }

    pub async fn shutdown(&mut self) -> Result<(), String> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.await
                .map_err(|error| format!("isolated Core server task failed: {error}"))?;
        }
        Ok(())
    }
}

/// Two independent in-process Cores for proving selected-target isolation.
pub struct CorePair {
    pub local: InProcessCore,
    pub remote: InProcessCore,
}

impl CorePair {
    pub async fn start(fixture: &ManagementFixture) -> Result<Self, String> {
        let local = InProcessCore::start(fixture.path("local-core")).await?;
        let remote = match InProcessCore::start(fixture.path("remote-core")).await {
            Ok(remote) => remote,
            Err(error) => {
                let mut local = local;
                let _ = local.shutdown().await;
                return Err(error);
            }
        };
        Ok(Self { local, remote })
    }

    pub async fn shutdown(&mut self) -> Result<(), String> {
        self.local.shutdown().await?;
        self.remote.shutdown().await
    }
}

pub trait MonotonicClock: Send + Sync {
    fn now(&self) -> Duration;
}

/// Injectable monotonic time; advancing it never sleeps the test thread.
#[derive(Clone, Debug, Default)]
pub struct FakeClock {
    elapsed_nanos: Arc<AtomicU64>,
}

impl FakeClock {
    pub fn advance(&self, duration: Duration) {
        let increment = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
        self.elapsed_nanos
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |elapsed| {
                Some(elapsed.saturating_add(increment))
            })
            .expect("fake clock update always succeeds");
    }
}

impl MonotonicClock for FakeClock {
    fn now(&self) -> Duration {
        Duration::from_nanos(self.elapsed_nanos.load(Ordering::SeqCst))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InjectedFailure {
    pub point: String,
    pub message: String,
}

#[derive(Clone, Debug)]
struct FailureRule {
    call: u64,
    message: String,
}

#[derive(Clone, Debug, Default)]
struct FailureState {
    calls: BTreeMap<String, u64>,
    rules: BTreeMap<String, FailureRule>,
}

/// Per-test failure schedule; it has no global or production process state.
#[derive(Clone, Debug, Default)]
pub struct FailureInjector(Arc<Mutex<FailureState>>);

impl FailureInjector {
    pub fn fail_on_call(&self, point: impl Into<String>, call: u64, message: impl Into<String>) {
        assert!(call > 0, "failure call numbers start at one");
        let point = point.into();
        let mut state = self
            .0
            .lock()
            .expect("failure injector lock should not be poisoned");
        state.calls.insert(point.clone(), 0);
        state.rules.insert(
            point,
            FailureRule {
                call,
                message: message.into(),
            },
        );
    }

    pub fn check(&self, point: &str) -> Result<(), InjectedFailure> {
        let mut state = self
            .0
            .lock()
            .expect("failure injector lock should not be poisoned");
        let calls = state.calls.entry(point.to_owned()).or_default();
        *calls = calls.saturating_add(1);
        let call = *calls;
        match state.rules.get(point) {
            Some(rule) if rule.call == call => Err(InjectedFailure {
                point: point.to_owned(),
                message: rule.message.clone(),
            }),
            _ => Ok(()),
        }
    }

    pub fn calls(&self, point: &str) -> u64 {
        self.0
            .lock()
            .expect("failure injector lock should not be poisoned")
            .calls
            .get(point)
            .copied()
            .unwrap_or_default()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CredentialFailure {
    Missing,
    Injected,
}

pub trait CredentialResolver: Send + Sync {
    fn resolve(&self, reference: &str) -> Result<String, CredentialFailure>;
}

#[derive(Default)]
struct CredentialState {
    values: BTreeMap<String, String>,
    requests: Vec<String>,
    fail_next_resolution: bool,
}

/// In-memory credential double. Its values are intentionally absent from Debug.
#[derive(Clone, Default)]
pub struct FakeCredentialStore(Arc<Mutex<CredentialState>>);

impl FakeCredentialStore {
    pub fn insert(&self, reference: impl Into<String>, value: impl Into<String>) {
        self.0
            .lock()
            .expect("fake credential store lock should not be poisoned")
            .values
            .insert(reference.into(), value.into());
    }

    pub fn remove(&self, reference: &str) -> bool {
        self.0
            .lock()
            .expect("fake credential store lock should not be poisoned")
            .values
            .remove(reference)
            .is_some()
    }

    pub fn fail_next_resolution(&self) {
        self.0
            .lock()
            .expect("fake credential store lock should not be poisoned")
            .fail_next_resolution = true;
    }

    pub fn requests(&self) -> Vec<String> {
        self.0
            .lock()
            .expect("fake credential store lock should not be poisoned")
            .requests
            .clone()
    }
}

impl CredentialResolver for FakeCredentialStore {
    fn resolve(&self, reference: &str) -> Result<String, CredentialFailure> {
        let mut state = self
            .0
            .lock()
            .expect("fake credential store lock should not be poisoned");
        state.requests.push(reference.to_owned());
        if std::mem::take(&mut state.fail_next_resolution) {
            return Err(CredentialFailure::Injected);
        }
        state
            .values
            .get(reference)
            .cloned()
            .ok_or(CredentialFailure::Missing)
    }
}
