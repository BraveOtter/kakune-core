#![allow(dead_code)]

//! Isolated fixtures and in-memory lifecycle adapters for Spec 002 tests.
//!
//! These helpers never elevate, access a live credential manager, mutate a real
//! service, or inspect/signal an arbitrary process. Native services, controls,
//! processes, and credentials are represented only by in-memory test doubles.

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

use kakune_core::Store;
use uuid::Uuid;

const FIXTURE_MARKER: &str = ".kakune-lifecycle-fixture";
const FIXTURE_PREFIX: &str = "kakune-spec002-lifecycle-";

fn approved_fixture_root() -> io::Result<PathBuf> {
    #[cfg(windows)]
    {
        let app_data = std::env::var_os("APPDATA").ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "APPDATA is required for secure Windows fixtures",
            )
        })?;
        Ok(PathBuf::from(app_data).join("Kakune-Spec002-Fixtures"))
    }
    #[cfg(not(windows))]
    {
        Ok(std::env::temp_dir().join("opencode"))
    }
}

/// A uniquely owned temporary directory with a private SQLite data location.
pub struct LifecycleFixture {
    id: Uuid,
    root: PathBuf,
    resources: ResourceTracker,
}

impl LifecycleFixture {
    pub fn new() -> io::Result<Self> {
        let approved_root = approved_fixture_root()?;
        fs::create_dir_all(&approved_root)?;
        if fs::symlink_metadata(&approved_root)?
            .file_type()
            .is_symlink()
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "approved lifecycle fixture root must not be redirected",
            ));
        }

        let id = Uuid::new_v4();
        let root = approved_root.join(format!("{FIXTURE_PREFIX}{id}"));
        fs::create_dir(&root)?;
        if let Err(error) = fs::write(root.join(FIXTURE_MARKER), id.to_string()) {
            let _ = fs::remove_dir(&root);
            return Err(error);
        }

        Ok(Self {
            id,
            root,
            resources: ResourceTracker::new(id),
        })
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn data_dir(&self) -> PathBuf {
        self.root.join("data")
    }

    pub fn config_file(&self) -> PathBuf {
        self.root.join("config").join("kakune.yaml")
    }

    pub fn log_file(&self) -> PathBuf {
        self.root.join("logs").join("kakune.log")
    }

    pub fn open_store(&self) -> Result<Store, String> {
        let store = Store::open(self.data_dir())?;
        self.resources.record(TestOwnedResource::Filesystem(
            self.data_dir().join("kakune.sqlite3"),
        ));
        Ok(store)
    }

    pub fn resources(&self) -> &ResourceTracker {
        &self.resources
    }
}

impl Drop for LifecycleFixture {
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

        // This is the fixture's unique, marker-verified child directory only.
        let _ = fs::remove_dir_all(root);
    }
}

/// A resource created by a test fixture or one of its in-memory adapters.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TestOwnedResource {
    Filesystem(PathBuf),
    FakeProcess(String),
    FakeService(String),
    FakeCredentialReference(String),
}

/// Tracks test-owned resources without granting access to operating-system resources.
#[derive(Clone, Debug)]
pub struct ResourceTracker {
    fixture_id: Uuid,
    resources: Arc<Mutex<BTreeSet<TestOwnedResource>>>,
}

impl ResourceTracker {
    fn new(fixture_id: Uuid) -> Self {
        Self {
            fixture_id,
            resources: Arc::new(Mutex::new(BTreeSet::new())),
        }
    }

    pub fn fixture_id(&self) -> Uuid {
        self.fixture_id
    }

    pub fn record(&self, resource: TestOwnedResource) {
        self.resources
            .lock()
            .expect("test resource tracker lock should not be poisoned")
            .insert(resource);
    }

    pub fn release(&self, resource: &TestOwnedResource) -> bool {
        self.resources
            .lock()
            .expect("test resource tracker lock should not be poisoned")
            .remove(resource)
    }

    pub fn snapshot(&self) -> Vec<TestOwnedResource> {
        self.resources
            .lock()
            .expect("test resource tracker lock should not be poisoned")
            .iter()
            .cloned()
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.resources
            .lock()
            .expect("test resource tracker lock should not be poisoned")
            .is_empty()
    }
}

pub trait MonotonicClock: Send + Sync {
    fn now(&self) -> Duration;
}

/// Deterministic monotonic time; advancing it never sleeps the test thread.
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
pub struct ProcessEvidence {
    pub pid: u32,
    pub owner: Option<String>,
    pub executable: Option<PathBuf>,
    pub start_identity: Option<String>,
    pub alive: bool,
}

pub trait ProcessEvidenceAdapter: Send + Sync {
    fn observe(&self, pid: u32) -> Option<ProcessEvidence>;
}

#[derive(Clone, Debug)]
pub struct FakeProcessAdapter {
    observations: Arc<Mutex<BTreeMap<u32, ProcessEvidence>>>,
    resources: ResourceTracker,
}

impl FakeProcessAdapter {
    fn new(resources: ResourceTracker) -> Self {
        Self {
            observations: Arc::new(Mutex::new(BTreeMap::new())),
            resources,
        }
    }

    pub fn insert(&self, evidence: ProcessEvidence) {
        let resource = TestOwnedResource::FakeProcess(evidence.pid.to_string());
        self.observations
            .lock()
            .expect("fake process adapter lock should not be poisoned")
            .insert(evidence.pid, evidence);
        self.resources.record(resource);
    }

    pub fn remove(&self, pid: u32) -> Option<ProcessEvidence> {
        let evidence = self
            .observations
            .lock()
            .expect("fake process adapter lock should not be poisoned")
            .remove(&pid);
        if evidence.is_some() {
            self.resources
                .release(&TestOwnedResource::FakeProcess(pid.to_string()));
        }
        evidence
    }
}

impl ProcessEvidenceAdapter for FakeProcessAdapter {
    fn observe(&self, pid: u32) -> Option<ProcessEvidence> {
        self.observations
            .lock()
            .expect("fake process adapter lock should not be poisoned")
            .get(&pid)
            .cloned()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControlResult {
    Acknowledged,
    Rejected,
    Unavailable,
    TimedOut,
}

pub trait LocalControlAdapter: Send + Sync {
    fn request(&self, operation: &str) -> ControlResult;
}

#[derive(Clone, Debug)]
pub struct FakeControlAdapter {
    result: Arc<Mutex<ControlResult>>,
    requests: Arc<Mutex<Vec<String>>>,
}

impl FakeControlAdapter {
    fn new() -> Self {
        Self {
            result: Arc::new(Mutex::new(ControlResult::Acknowledged)),
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn set_result(&self, result: ControlResult) {
        *self
            .result
            .lock()
            .expect("fake control adapter lock should not be poisoned") = result;
    }

    pub fn requests(&self) -> Vec<String> {
        self.requests
            .lock()
            .expect("fake control request log lock should not be poisoned")
            .clone()
    }
}

impl LocalControlAdapter for FakeControlAdapter {
    fn request(&self, operation: &str) -> ControlResult {
        self.requests
            .lock()
            .expect("fake control request log lock should not be poisoned")
            .push(operation.to_owned());
        *self
            .result
            .lock()
            .expect("fake control adapter lock should not be poisoned")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeServiceState {
    Stopped,
    Starting,
    Running,
    Stopping,
    Failed,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NativeObservation {
    Absent,
    Installed {
        definition: String,
        state: NativeServiceState,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MutationDisposition {
    Changed,
    Noop,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeManagerError {
    ConflictingDefinition,
    NotInstalled,
    MustStopBeforeRemoval,
}

pub trait NativeServiceAdapter: Send + Sync {
    fn observe(&self, service_id: &str) -> NativeObservation;
    fn install(
        &self,
        service_id: &str,
        definition: &str,
    ) -> Result<MutationDisposition, NativeManagerError>;
    fn start(&self, service_id: &str) -> Result<MutationDisposition, NativeManagerError>;
    fn stop(&self, service_id: &str) -> Result<MutationDisposition, NativeManagerError>;
    fn uninstall(&self, service_id: &str) -> Result<MutationDisposition, NativeManagerError>;
}

#[derive(Clone, Debug)]
struct FakeRegistration {
    definition: String,
    state: NativeServiceState,
}

/// An in-memory service manager; it has no operating-system implementation.
#[derive(Clone, Debug)]
pub struct FakeNativeManager {
    registrations: Arc<Mutex<BTreeMap<String, FakeRegistration>>>,
    resources: ResourceTracker,
}

impl FakeNativeManager {
    fn new(resources: ResourceTracker) -> Self {
        Self {
            registrations: Arc::new(Mutex::new(BTreeMap::new())),
            resources,
        }
    }

    pub fn set_state_for_test(
        &self,
        service_id: &str,
        state: NativeServiceState,
    ) -> Result<(), NativeManagerError> {
        let mut registrations = self
            .registrations
            .lock()
            .expect("fake native manager lock should not be poisoned");
        let registration = registrations
            .get_mut(service_id)
            .ok_or(NativeManagerError::NotInstalled)?;
        registration.state = state;
        Ok(())
    }
}

impl NativeServiceAdapter for FakeNativeManager {
    fn observe(&self, service_id: &str) -> NativeObservation {
        self.registrations
            .lock()
            .expect("fake native manager lock should not be poisoned")
            .get(service_id)
            .map(|registration| NativeObservation::Installed {
                definition: registration.definition.clone(),
                state: registration.state,
            })
            .unwrap_or(NativeObservation::Absent)
    }

    fn install(
        &self,
        service_id: &str,
        definition: &str,
    ) -> Result<MutationDisposition, NativeManagerError> {
        let mut registrations = self
            .registrations
            .lock()
            .expect("fake native manager lock should not be poisoned");
        if let Some(existing) = registrations.get(service_id) {
            return if existing.definition == definition {
                Ok(MutationDisposition::Noop)
            } else {
                Err(NativeManagerError::ConflictingDefinition)
            };
        }

        registrations.insert(
            service_id.to_owned(),
            FakeRegistration {
                definition: definition.to_owned(),
                state: NativeServiceState::Stopped,
            },
        );
        self.resources
            .record(TestOwnedResource::FakeService(service_id.to_owned()));
        Ok(MutationDisposition::Changed)
    }

    fn start(&self, service_id: &str) -> Result<MutationDisposition, NativeManagerError> {
        let mut registrations = self
            .registrations
            .lock()
            .expect("fake native manager lock should not be poisoned");
        let registration = registrations
            .get_mut(service_id)
            .ok_or(NativeManagerError::NotInstalled)?;
        if registration.state == NativeServiceState::Running {
            return Ok(MutationDisposition::Noop);
        }
        registration.state = NativeServiceState::Running;
        Ok(MutationDisposition::Changed)
    }

    fn stop(&self, service_id: &str) -> Result<MutationDisposition, NativeManagerError> {
        let mut registrations = self
            .registrations
            .lock()
            .expect("fake native manager lock should not be poisoned");
        let Some(registration) = registrations.get_mut(service_id) else {
            return Ok(MutationDisposition::Noop);
        };
        if registration.state == NativeServiceState::Stopped {
            return Ok(MutationDisposition::Noop);
        }
        registration.state = NativeServiceState::Stopped;
        Ok(MutationDisposition::Changed)
    }

    fn uninstall(&self, service_id: &str) -> Result<MutationDisposition, NativeManagerError> {
        let mut registrations = self
            .registrations
            .lock()
            .expect("fake native manager lock should not be poisoned");
        let Some(registration) = registrations.get(service_id) else {
            return Ok(MutationDisposition::Noop);
        };
        if registration.state != NativeServiceState::Stopped {
            return Err(NativeManagerError::MustStopBeforeRemoval);
        }
        registrations.remove(service_id);
        self.resources
            .release(&TestOwnedResource::FakeService(service_id.to_owned()));
        Ok(MutationDisposition::Changed)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CredentialAvailability {
    Available,
    Missing,
    Denied,
}

pub trait CredentialAdapter: Send + Sync {
    fn availability(&self, reference: &str) -> CredentialAvailability;
}

#[derive(Clone, Debug)]
pub struct FakeCredentialAdapter {
    references: Arc<Mutex<BTreeMap<String, CredentialAvailability>>>,
    resources: ResourceTracker,
}

impl FakeCredentialAdapter {
    fn new(resources: ResourceTracker) -> Self {
        Self {
            references: Arc::new(Mutex::new(BTreeMap::new())),
            resources,
        }
    }

    pub fn set_availability(&self, reference: &str, availability: CredentialAvailability) {
        self.references
            .lock()
            .expect("fake credential adapter lock should not be poisoned")
            .insert(reference.to_owned(), availability);
        self.resources
            .record(TestOwnedResource::FakeCredentialReference(
                reference.to_owned(),
            ));
    }

    pub fn remove(&self, reference: &str) -> bool {
        let removed = self
            .references
            .lock()
            .expect("fake credential adapter lock should not be poisoned")
            .remove(reference)
            .is_some();
        if removed {
            self.resources
                .release(&TestOwnedResource::FakeCredentialReference(
                    reference.to_owned(),
                ));
        }
        removed
    }
}

impl CredentialAdapter for FakeCredentialAdapter {
    fn availability(&self, reference: &str) -> CredentialAvailability {
        self.references
            .lock()
            .expect("fake credential adapter lock should not be poisoned")
            .get(reference)
            .copied()
            .unwrap_or(CredentialAvailability::Missing)
    }
}

/// The complete default adapter set for deterministic, non-privileged tests.
#[derive(Clone, Debug)]
pub struct LifecycleTestAdapters {
    pub clock: FakeClock,
    pub processes: FakeProcessAdapter,
    pub control: FakeControlAdapter,
    pub native_manager: FakeNativeManager,
    pub credentials: FakeCredentialAdapter,
}

impl LifecycleTestAdapters {
    pub fn new(resources: ResourceTracker) -> Self {
        Self {
            clock: FakeClock::default(),
            processes: FakeProcessAdapter::new(resources.clone()),
            control: FakeControlAdapter::new(),
            native_manager: FakeNativeManager::new(resources.clone()),
            credentials: FakeCredentialAdapter::new(resources),
        }
    }
}
