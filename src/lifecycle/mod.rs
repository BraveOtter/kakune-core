//! Shared, independently testable lifecycle contracts.
//!
//! This module deliberately contains no CLI parsing, prompting, signal handling,
//! or console output. Platform adapters and orchestration build on these types.

pub mod control;
pub mod daemon;
pub mod execution;
pub mod host;
pub mod launch;
pub mod ownership;
pub mod process;
pub mod service;

use std::{
    collections::BTreeMap,
    fmt,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
    time::{Duration, Instant},
};

pub const MAX_CONTROL_MESSAGE_BYTES: usize = 64 * 1024;
pub const MAX_HELPER_DIAGNOSTIC_BYTES: usize = 1024 * 1024;

pub const READINESS_BUDGET: Duration = Duration::from_secs(30);
pub const CORE_GRACE_BUDGET: Duration = Duration::from_secs(30);
pub const INSTALL_BUDGET: Duration = Duration::from_secs(30);
pub const REMOVAL_BUDGET: Duration = Duration::from_secs(30);
pub const ACQUISITION_BUDGET: Duration = Duration::from_secs(10);
pub const STATUS_BUDGET: Duration = Duration::from_secs(10);
pub const PREFLIGHT_BUDGET: Duration = Duration::from_secs(10);
pub const HELPER_BUDGET: Duration = Duration::from_secs(10);
pub const STOP_DISPATCH_BUDGET: Duration = Duration::from_secs(10);

pub const INSTALL_COMMAND_CEILING: Duration = Duration::from_secs(50);
pub const START_COMMAND_CEILING: Duration = Duration::from_secs(50);
pub const STOP_COMMAND_CEILING: Duration = Duration::from_secs(60);
pub const UNINSTALL_COMMAND_CEILING: Duration = Duration::from_secs(90);

/// Observable phase of one Core instance. `Stopped` is intentionally omitted:
/// absence is an observation, not a live runtime phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum LifecyclePhase {
    Starting = 0,
    Ready = 1,
    Stopping = 2,
    Failed = 3,
}

impl LifecyclePhase {
    fn from_byte(value: u8) -> Self {
        match value {
            1 => Self::Ready,
            2 => Self::Stopping,
            3 => Self::Failed,
            _ => Self::Starting,
        }
    }
}

struct CoreLifecycleInner {
    phase: AtomicU8,
    admission: execution::AdmissionCoordinator,
    shutdown_requested: Arc<AtomicBool>,
}

/// Shared Core state used by API admission, trigger dispatch, and shutdown.
/// Readiness can only be published from `Starting`; shutdown atomically closes
/// new execution admission before exposing `Stopping` to health observers.
#[derive(Clone)]
pub struct CoreLifecycle {
    inner: Arc<CoreLifecycleInner>,
}

impl CoreLifecycle {
    pub fn starting() -> Self {
        Self::with_phase(LifecyclePhase::Starting)
    }

    /// Creates an already-ready state for standalone API embedders and tests.
    pub fn ready() -> Self {
        Self::with_phase(LifecyclePhase::Ready)
    }

    fn with_phase(phase: LifecyclePhase) -> Self {
        let admission = if phase == LifecyclePhase::Ready {
            execution::AdmissionCoordinator::new()
        } else {
            execution::AdmissionCoordinator::closed()
        };
        Self {
            inner: Arc::new(CoreLifecycleInner {
                phase: AtomicU8::new(phase as u8),
                admission,
                shutdown_requested: Arc::new(AtomicBool::new(false)),
            }),
        }
    }

    pub fn phase(&self) -> LifecyclePhase {
        LifecyclePhase::from_byte(self.inner.phase.load(Ordering::Acquire))
    }

    pub fn is_ready(&self) -> bool {
        self.phase() == LifecyclePhase::Ready
    }

    pub fn admission(&self) -> execution::AdmissionCoordinator {
        self.inner.admission.clone()
    }

    /// Read-only handle passed to runtime workers for cooperative shutdown.
    pub fn shutdown_signal(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.inner.shutdown_requested)
    }

    /// Opens admission only after every required startup component is ready.
    pub fn mark_ready(&self) -> Result<(), LifecycleError> {
        self.inner.admission.open_admission().and_then(|()| {
            self.inner
                .phase
                .compare_exchange(
                    LifecyclePhase::Starting as u8,
                    LifecyclePhase::Ready as u8,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .map(|_| ())
                .map_err(|_| {
                    LifecycleError::UnsafeOwnership(
                        "Core readiness can only be published while starting".to_owned(),
                    )
                })
        })
    }

    /// Closes admission and signals cooperative execution checkpoints before
    /// publishing the stopping state. It does not release runtime ownership.
    pub fn begin_stopping(&self) -> Result<execution::AdmissionSnapshot, LifecycleError> {
        let snapshot = self.inner.admission.close_admission()?;
        self.inner.shutdown_requested.store(true, Ordering::Release);
        let previous = self
            .inner
            .phase
            .swap(LifecyclePhase::Stopping as u8, Ordering::AcqRel);
        if LifecyclePhase::from_byte(previous) == LifecyclePhase::Failed {
            self.inner
                .phase
                .store(LifecyclePhase::Failed as u8, Ordering::Release);
        }
        Ok(snapshot)
    }

    pub fn mark_failed(&self) -> Result<execution::AdmissionSnapshot, LifecycleError> {
        let snapshot = self.inner.admission.close_admission()?;
        self.inner.shutdown_requested.store(true, Ordering::Release);
        self.inner
            .phase
            .store(LifecyclePhase::Failed as u8, Ordering::Release);
        Ok(snapshot)
    }
}

/// Library-owned lifecycle controller for one foreground Core process. It
/// holds the data-directory lease for the complete service lifetime and
/// publishes instance-specific, versioned runtime observations.
pub struct CoreInstanceController {
    lifecycle: CoreLifecycle,
    clock: SystemMonotonicClock,
    waiter: ThreadWaiter,
    lease: ownership::RuntimeLease,
    record: ownership::RuntimeOwnershipRecord,
}

impl CoreInstanceController {
    pub fn acquire_foreground(data_dir: &Path) -> Result<Self, LifecycleError> {
        Self::acquire_with_mode(data_dir, CoreMode::Foreground, || Ok(()))
    }

    /// Acquires the user-daemon lifetime lease and gives the parent a precise
    /// handoff point before publishing ownership metadata. Background launchers
    /// use this callback to release their start-serialization lock only after
    /// this child owns the data directory.
    pub fn acquire_user_daemon_with_claim(
        data_dir: &Path,
        claim: impl FnOnce() -> Result<(), LifecycleError>,
    ) -> Result<Self, LifecycleError> {
        Self::acquire_with_mode(data_dir, CoreMode::UserDaemon, claim)
    }

    /// Acquires the shared data-directory lease for an OS-managed service.
    /// Service hosts use the same ownership evidence and runtime lifecycle as
    /// foreground and user-daemon instances.
    pub fn acquire_system_service(data_dir: &Path) -> Result<Self, LifecycleError> {
        Self::acquire_with_mode(data_dir, CoreMode::SystemService, || Ok(()))
    }

    fn acquire_with_mode(
        data_dir: &Path,
        mode: CoreMode,
        claim: impl FnOnce() -> Result<(), LifecycleError>,
    ) -> Result<Self, LifecycleError> {
        use process::ProcessEvidenceProvider;

        let clock = SystemMonotonicClock::default();
        let waiter = ThreadWaiter;
        let instance_id = uuid::Uuid::new_v4();
        let lease = ownership::RuntimeLease::acquire(data_dir, instance_id, &clock, &waiter)?;
        let current_executable = std::env::current_exe()?;
        let evidence = process::SystemProcessEvidence::default()
            .observe(
                std::process::id(),
                Deadline::after(&clock, ACQUISITION_BUDGET),
                &clock,
            )?
            .ok_or(LifecycleError::UnverifiedProcess)?;
        if evidence.identity.executable != current_executable {
            return Err(LifecycleError::UnverifiedProcess);
        }
        let instance = ownership::CoreInstance {
            instance_id,
            mode,
            owner: evidence.owner,
            process_identity: evidence.identity,
            data_dir: lease.canonical_data_dir().to_path_buf(),
            listen_address: None,
            phase: ownership::InstancePhase::Starting,
            diagnostic_location: None,
            control_endpoint: None,
        };
        let record = ownership::RuntimeOwnershipRecord::new(instance)?;
        claim()?;
        lease.publish(&record, &clock, &waiter)?;
        Ok(Self {
            lifecycle: CoreLifecycle::starting(),
            clock,
            waiter,
            lease,
            record,
        })
    }

    pub fn lifecycle(&self) -> CoreLifecycle {
        self.lifecycle.clone()
    }

    pub fn data_dir(&self) -> &Path {
        &self.record.instance.data_dir
    }

    pub fn instance_id(&self) -> uuid::Uuid {
        self.record.instance.instance_id
    }

    pub fn mode(&self) -> CoreMode {
        self.record.instance.mode
    }

    pub fn owner(&self) -> &ownership::OwnerIdentity {
        &self.record.instance.owner
    }

    pub fn process_identity(&self) -> &ownership::ProcessIdentity {
        &self.record.instance.process_identity
    }

    pub fn record(&self) -> &ownership::RuntimeOwnershipRecord {
        &self.record
    }

    pub fn set_diagnostic_location(
        &mut self,
        location: impl Into<String>,
    ) -> Result<(), LifecycleError> {
        self.record.instance.diagnostic_location = Some(location.into());
        self.publish_record()
    }

    /// Binds and publishes a private per-instance loopback endpoint and a new
    /// owner-protected capability before the child reports itself prepared.
    pub fn prepare_control(
        &mut self,
    ) -> Result<
        (
            tokio::net::TcpListener,
            control::Capability,
            control::ControlContext,
        ),
        LifecycleError,
    > {
        let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let endpoint = control::LoopbackControlEndpoint::new(address)?;
        let context = control::ControlContext {
            instance_id: self.instance_id(),
            owner: self.record.instance.owner.clone(),
            mode: self.record.instance.mode,
        };
        let capability_path = self.lease.capability_path();
        control::prepare_capability_directory(&capability_path, &self.record.instance.owner)?;
        let capability = control::create_capability(
            &capability_path,
            &self.record.instance.owner,
            &control::OsRandomSource,
            &control::OwnerProtectedFileStorage,
        )?;
        self.record.instance.control_endpoint = Some(endpoint.address().to_string());
        if let Err(error) = self.publish_record() {
            let _ = control::remove_capability(&capability_path, &self.record.instance.owner);
            return Err(error);
        }
        let listener = tokio::net::TcpListener::from_std(listener)?;
        Ok((listener, capability, context))
    }

    pub fn publish_listen_address(
        &mut self,
        address: SocketAddr,
        tls: bool,
    ) -> Result<(), LifecycleError> {
        let scheme = if tls { "https" } else { "http" };
        self.record.instance.listen_address = Some(format!("{scheme}://{address}"));
        self.publish_record()
    }

    pub fn mark_ready(&mut self) -> Result<(), LifecycleError> {
        self.lifecycle.mark_ready()?;
        self.record.instance.phase = ownership::InstancePhase::Ready;
        self.publish_record()
    }

    pub fn mark_stopping(&mut self) -> Result<(), LifecycleError> {
        self.record.instance.phase = ownership::InstancePhase::Stopping;
        self.publish_record()
    }

    pub fn mark_failed(&mut self) -> Result<(), LifecycleError> {
        let _ = self.lifecycle.mark_failed()?;
        self.record.instance.phase = ownership::InstancePhase::Failed;
        self.publish_record()
    }

    /// Removes only this instance's metadata while the lifetime lease is held.
    /// Callers must first stop the scheduler/server and drain all accepted work.
    pub fn release_after_drain(&self) -> Result<(), LifecycleError> {
        if !self.lifecycle.admission().snapshot()?.is_drained() {
            return Err(LifecycleError::UnsafeOwnership(
                "runtime ownership cannot be released while accepted work remains".to_owned(),
            ));
        }
        if self.record.instance.control_endpoint.is_some() {
            control::remove_capability(&self.lease.capability_path(), &self.record.instance.owner)?;
        }
        if !self.lease.cleanup_record(&self.clock, &self.waiter)? {
            return Err(LifecycleError::UnsafeOwnership(
                "runtime ownership record changed before cleanup".to_owned(),
            ));
        }
        Ok(())
    }

    fn publish_record(&self) -> Result<(), LifecycleError> {
        self.lease.publish(&self.record, &self.clock, &self.waiter)
    }
}

/// Monotonic time since an arbitrary origin. Wall-clock time is intentionally
/// excluded from deadline arithmetic.
pub trait MonotonicClock: Send + Sync {
    fn now(&self) -> Duration;
}

#[derive(Debug)]
pub struct SystemMonotonicClock {
    origin: Instant,
}

impl Default for SystemMonotonicClock {
    fn default() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl MonotonicClock for SystemMonotonicClock {
    fn now(&self) -> Duration {
        self.origin.elapsed()
    }
}

/// An absolute deadline in the clock domain that created it. Nested work must
/// carry this value forward instead of constructing a fresh phase deadline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Deadline {
    at: Duration,
}

impl Deadline {
    pub fn after(clock: &dyn MonotonicClock, budget: Duration) -> Self {
        Self {
            at: clock.now().saturating_add(budget),
        }
    }

    pub fn at(at: Duration) -> Self {
        Self { at }
    }

    pub fn endpoint(self) -> Duration {
        self.at
    }

    pub fn remaining(self, clock: &dyn MonotonicClock) -> Duration {
        self.at.saturating_sub(clock.now())
    }

    /// Caps this deadline by a nested operation's maximum without extending it.
    pub fn cap(self, clock: &dyn MonotonicClock, maximum: Duration) -> Self {
        Self {
            at: self.at.min(clock.now().saturating_add(maximum)),
        }
    }

    pub fn is_expired(self, clock: &dyn MonotonicClock) -> bool {
        self.remaining(clock).is_zero()
    }
}

/// Injectable wait strategy. Tests can advance a fake clock without sleeping.
pub trait DeadlineWaiter: Send + Sync {
    fn wait(&self, duration: Duration);
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ThreadWaiter;

impl DeadlineWaiter for ThreadWaiter {
    fn wait(&self, duration: Duration) {
        std::thread::sleep(duration);
    }
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CoreMode {
    Foreground,
    UserDaemon,
    SystemService,
}

impl fmt::Display for CoreMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Foreground => "foreground",
            Self::UserDaemon => "user_daemon",
            Self::SystemService => "system_service",
        })
    }
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ObservedState {
    Starting,
    Ready,
    Stopping,
    Stopped,
    Failed,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationOutcome {
    Success,
    Noop,
    Failure,
}

/// Structured operation data; presentation remains the CLI's responsibility.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LifecycleOperationResult {
    pub action: String,
    pub scope: String,
    pub outcome: OperationOutcome,
    pub observed_state: ObservedState,
    pub verified_pid: Option<u32>,
    pub data_dir: Option<std::path::PathBuf>,
    pub diagnostic_location: Option<String>,
    pub changed_resources: Vec<String>,
    pub remaining_resources: Vec<String>,
    pub recovery_guidance: Vec<String>,
    pub elapsed_application_wait: Duration,
    pub platform_completion_limitations: Vec<String>,
}

impl LifecycleOperationResult {
    /// Success cannot be represented without a confirmed requested state.
    pub fn confirmed(
        action: impl Into<String>,
        scope: impl Into<String>,
        expected_state: ObservedState,
        observed_state: ObservedState,
    ) -> Result<Self, LifecycleError> {
        if expected_state != observed_state
            || matches!(
                observed_state,
                ObservedState::Unknown | ObservedState::Failed
            )
        {
            return Err(LifecycleError::UnconfirmedSuccess);
        }
        Ok(Self {
            action: action.into(),
            scope: scope.into(),
            outcome: OperationOutcome::Success,
            observed_state,
            verified_pid: None,
            data_dir: None,
            diagnostic_location: None,
            changed_resources: Vec::new(),
            remaining_resources: Vec::new(),
            recovery_guidance: Vec::new(),
            elapsed_application_wait: Duration::ZERO,
            platform_completion_limitations: Vec::new(),
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LifecycleError {
    #[error("lifecycle deadline expired during {phase}")]
    DeadlineExpired { phase: &'static str },
    #[error("lifecycle ownership is busy")]
    OwnershipBusy,
    #[error("lifecycle ownership evidence is unknown or unsafe: {0}")]
    UnsafeOwnership(String),
    #[error("unsupported lifecycle metadata schema version {0}")]
    UnsupportedSchema(u32),
    #[error("process identity could not be verified")]
    UnverifiedProcess,
    #[error("lifecycle operation is not allowed for mode {0}")]
    ModeNotControllable(String),
    #[error("lifecycle message exceeds the {limit}-byte limit")]
    MessageTooLarge { limit: usize },
    #[error("lifecycle authentication failed")]
    AuthenticationFailed,
    #[error("lifecycle challenge has already been consumed")]
    ChallengeConsumed,
    #[error("lifecycle client OS account does not match the verified owner")]
    WrongOwner,
    #[error("owner-protected capability storage is unavailable or unverified: {0}")]
    InsecureProtection(String),
    #[error("operating-system cryptographic randomness failed")]
    RandomnessFailed,
    #[error("lifecycle success was requested without a confirmed state")]
    UnconfirmedSuccess,
    #[error("access to the native service manager was denied: {0}")]
    AccessDenied(String),
    #[error("native service manager is unavailable: {0}")]
    ManagerUnavailable(String),
    #[error("native service definition conflicts with the requested Kakune definition: {0}")]
    ConflictingServiceDefinition(String),
    #[error("the requested native service is not installed")]
    ServiceNotInstalled,
    #[error("an unresolved service mutation blocks further changes: {0}")]
    UnresolvedServiceAttempt(String),
    #[error("native service or Core did not reach the requested state")]
    ServiceStateUnconfirmed,
    #[error("owned native helper process {pid} could not be confirmed reaped")]
    HelperUnreaped { pid: u32 },
    #[error("lifecycle I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("lifecycle serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
}

/// Common phase and cumulative-budget reference, kept explicit for tests and
/// CLI diagnostics.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum LifecycleBudget {
    Readiness,
    CoreGrace,
    Acquisition,
    Status,
    Preflight,
    Helper,
    Install,
    StopDispatch,
    Removal,
    InstallCommand,
    StartCommand,
    StopCommand,
    UninstallCommand,
}

impl LifecycleBudget {
    pub const fn duration(self) -> Duration {
        match self {
            Self::Readiness => READINESS_BUDGET,
            Self::CoreGrace => CORE_GRACE_BUDGET,
            Self::Acquisition => ACQUISITION_BUDGET,
            Self::Status => STATUS_BUDGET,
            Self::Preflight => PREFLIGHT_BUDGET,
            Self::Helper => HELPER_BUDGET,
            Self::Install => INSTALL_BUDGET,
            Self::StopDispatch => STOP_DISPATCH_BUDGET,
            Self::Removal => REMOVAL_BUDGET,
            Self::InstallCommand => INSTALL_COMMAND_CEILING,
            Self::StartCommand => START_COMMAND_CEILING,
            Self::StopCommand => STOP_COMMAND_CEILING,
            Self::UninstallCommand => UNINSTALL_COMMAND_CEILING,
        }
    }

    pub const fn starts_at(self) -> &'static str {
        match self {
            Self::Readiness => "Core launch or service start dispatch",
            Self::CoreGrace => "atomic execution-admission closure",
            Self::Acquisition => "first administrative/runtime lock attempt",
            Self::Status => "first status observation",
            Self::Preflight => "service serialization acquired",
            Self::Helper => "owned helper launch",
            Self::Install => "successful service preflight",
            Self::StopDispatch => "graceful-stop dispatch start",
            Self::Removal => "confirmed Core stop before removal",
            Self::InstallCommand => "service install command start",
            Self::StartCommand => "service start command start",
            Self::StopCommand => "service stop command start",
            Self::UninstallCommand => "service uninstall command start",
        }
    }
}

/// Keeps one absolute deadline per operation phase. Repeated calls return the
/// original deadline; retries and nested helpers cannot renew a phase.
#[derive(Clone, Debug, Default)]
pub struct DeadlineBook {
    deadlines: BTreeMap<LifecycleBudget, Deadline>,
}

impl DeadlineBook {
    pub fn start_once(
        &mut self,
        phase: LifecycleBudget,
        clock: &dyn MonotonicClock,
        enclosing: Option<Deadline>,
    ) -> Deadline {
        if let Some(deadline) = self.deadlines.get(&phase) {
            return *deadline;
        }
        let requested = Deadline::after(clock, phase.duration());
        let deadline = if phase == LifecycleBudget::CoreGrace {
            // Core grace starts at admission closure and is independent of
            // helper cleanup or the managing command's dispatch wait.
            requested
        } else {
            enclosing.map_or(requested, |limit| {
                Deadline::at(requested.endpoint().min(limit.endpoint()))
            })
        };
        self.deadlines.insert(phase, deadline);
        deadline
    }

    pub fn get(&self, phase: LifecycleBudget) -> Option<Deadline> {
        self.deadlines.get(&phase).copied()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CoreStartupOptions {
    pub data_dir: PathBuf,
    pub config_file: Option<PathBuf>,
    pub listen_address: Option<String>,
    pub tls_certificate: Option<PathBuf>,
    pub tls_private_key: Option<PathBuf>,
    pub allowed_hosts: Vec<String>,
}

impl CoreStartupOptions {
    pub fn validate(&self) -> Result<(), LifecycleError> {
        if !self.data_dir.is_absolute()
            || self
                .config_file
                .as_ref()
                .is_some_and(|path| !path.is_absolute())
            || self
                .tls_certificate
                .as_ref()
                .is_some_and(|path| !path.is_absolute())
            || self
                .tls_private_key
                .as_ref()
                .is_some_and(|path| !path.is_absolute())
        {
            return Err(LifecycleError::UnsafeOwnership(
                "lifecycle startup paths must be absolute before dispatch".to_owned(),
            ));
        }
        if self.tls_certificate.is_some() != self.tls_private_key.is_some() {
            return Err(LifecycleError::UnsafeOwnership(
                "TLS certificate and private-key paths must be supplied together".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LifecycleRequest {
    Foreground { startup: CoreStartupOptions },
    DaemonStart { startup: CoreStartupOptions },
    DaemonStop { data_dir: PathBuf },
    DaemonStatus { data_dir: PathBuf },
    ServiceInstall { data_dir: PathBuf },
    ServiceStart,
    ServiceStop,
    ServiceUninstall,
    ServiceStatus,
}

impl LifecycleRequest {
    pub fn validate(&self) -> Result<(), LifecycleError> {
        match self {
            Self::Foreground { startup } | Self::DaemonStart { startup } => startup.validate(),
            Self::DaemonStop { data_dir } | Self::DaemonStatus { data_dir }
                if !data_dir.is_absolute() =>
            {
                Err(LifecycleError::UnsafeOwnership(
                    "daemon observation/control path must be absolute".to_owned(),
                ))
            }
            Self::ServiceInstall { data_dir } if !data_dir.is_absolute() => {
                Err(LifecycleError::UnsafeOwnership(
                    "service install data path must be absolute".to_owned(),
                ))
            }
            _ => Ok(()),
        }
    }
}

/// Shared library contract used by the CLI adapter. Implementations receive
/// immutable absolute deadlines and return data, never print or prompt.
pub trait LifecycleController: Send + Sync {
    fn perform(
        &self,
        request: &LifecycleRequest,
        deadlines: &DeadlineBook,
    ) -> Result<LifecycleOperationResult, LifecycleError>;
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InstallationObservation {
    Installed,
    NotInstalled,
    Unknown,
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NativeManagerState {
    Stopped,
    Starting,
    Running,
    Stopping,
    Failed,
    Unknown,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct CoreObservation {
    pub mode: CoreMode,
    pub state: ObservedState,
    pub instance_id: Option<uuid::Uuid>,
    pub owner: Option<ownership::OwnerIdentity>,
    pub process: Option<ownership::ProcessIdentity>,
    pub data_dir: PathBuf,
    pub listen_address: Option<String>,
    pub diagnostic_location: Option<String>,
    pub identity_verified: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceObservation {
    pub installed: InstallationObservation,
    pub manager_state: NativeManagerState,
    pub account: Option<String>,
    pub core: Option<CoreObservation>,
    pub diagnostic_location: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServiceMutation {
    Install,
    Start,
    Stop,
    Remove,
}

/// Native adapter contract. Query and single-step mutation calls must obey the
/// supplied enclosing deadline and report unknown rather than infer absence.
pub trait NativeServiceManager: Send + Sync {
    fn observe(
        &self,
        service_id: &str,
        deadline: Deadline,
        clock: &dyn MonotonicClock,
    ) -> Result<ServiceObservation, LifecycleError>;

    fn mutate(
        &self,
        service_id: &str,
        mutation: ServiceMutation,
        deadline: Deadline,
        clock: &dyn MonotonicClock,
    ) -> Result<LifecycleOperationResult, LifecycleError>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CredentialAvailability {
    Available,
    Missing,
    Denied,
    Unknown,
}

pub trait CredentialAccessProbe: Send + Sync {
    fn availability(
        &self,
        reference: &str,
        deadline: Deadline,
        clock: &dyn MonotonicClock,
    ) -> Result<CredentialAvailability, LifecycleError>;
}
