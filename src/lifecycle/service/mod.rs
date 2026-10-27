//! Native system-service contracts, durable mutation barriers, and platform
//! definition rendering. Platform calls are injected so the lifecycle policy
//! remains deterministic and independently testable.

use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{
    ACQUISITION_BUDGET, CORE_GRACE_BUDGET, CoreMode, CoreObservation, Deadline, DeadlineWaiter,
    HELPER_BUDGET, INSTALL_BUDGET, INSTALL_COMMAND_CEILING, InstallationObservation,
    LifecycleError, LifecycleOperationResult, MonotonicClock, NativeManagerState, ObservedState,
    OperationOutcome, PREFLIGHT_BUDGET, READINESS_BUDGET, REMOVAL_BUDGET, START_COMMAND_CEILING,
    STATUS_BUDGET, STOP_COMMAND_CEILING, STOP_DISPATCH_BUDGET, UNINSTALL_COMMAND_CEILING,
};

const SERVICE_STATE_SCHEMA_VERSION: u32 = 1;
const MAX_ATTEMPT_BYTES: usize = super::MAX_CONTROL_MESSAGE_BYTES;
const LOCK_RETRY_INTERVAL: Duration = Duration::from_millis(20);
const OBSERVATION_RETRY_INTERVAL: Duration = Duration::from_millis(100);
const ATTEMPT_FILE: &str = "attempt.json";
const ADMIN_LOCK_FILE: &str = "admin.lock";

pub mod helper;
#[cfg(target_os = "linux")]
pub mod linux;
#[cfg(target_os = "macos")]
pub mod macos;
mod native;
#[cfg(windows)]
pub mod windows;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ServiceManagerKind {
    WindowsScm,
    LinuxSystemd,
    MacosLaunchd,
}

impl ServiceManagerKind {
    pub const fn key(self) -> &'static str {
        match self {
            Self::WindowsScm => "scm",
            Self::LinuxSystemd => "systemd",
            Self::MacosLaunchd => "launchd",
        }
    }

    pub fn platform_service_id(self) -> &'static str {
        match self {
            Self::WindowsScm => "KakuneCore",
            Self::LinuxSystemd => "kakune-core.service",
            Self::MacosLaunchd => "dev.kakune.core",
        }
    }

    pub fn default_account(self) -> &'static str {
        match self {
            Self::WindowsScm => "LocalSystem",
            Self::LinuxSystemd | Self::MacosLaunchd => "root",
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BootPolicy {
    Automatic,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceDefinition {
    pub manager: ServiceManagerKind,
    pub service_id: String,
    pub executable: PathBuf,
    pub arguments: Vec<String>,
    pub account: String,
    pub boot_policy: BootPolicy,
    pub data_dir: PathBuf,
    pub definition_fingerprint: Option<String>,
}

impl ServiceDefinition {
    pub fn new(
        manager: ServiceManagerKind,
        service_id: impl Into<String>,
        executable: &Path,
        arguments: Vec<String>,
        account: impl Into<String>,
        data_dir: &Path,
    ) -> Result<Self, LifecycleError> {
        let definition = Self {
            manager,
            service_id: service_id.into(),
            executable: executable.to_path_buf(),
            arguments,
            account: account.into(),
            boot_policy: BootPolicy::Automatic,
            data_dir: data_dir.to_path_buf(),
            definition_fingerprint: None,
        };
        definition.validate()?;
        let fingerprint = definition.fingerprint()?;
        Ok(Self {
            definition_fingerprint: Some(fingerprint),
            ..definition
        })
    }

    pub fn for_platform(data_dir: &Path) -> Result<Self, LifecycleError> {
        let executable = std::env::current_exe()?;
        let manager = if cfg!(windows) {
            ServiceManagerKind::WindowsScm
        } else if cfg!(target_os = "linux") {
            ServiceManagerKind::LinuxSystemd
        } else if cfg!(target_os = "macos") {
            ServiceManagerKind::MacosLaunchd
        } else {
            return Err(LifecycleError::ManagerUnavailable(
                "system-service management is unsupported on this operating system".to_owned(),
            ));
        };
        let service_id = manager.platform_service_id();
        let arguments = vec![
            "service".to_owned(),
            "run".to_owned(),
            "--data-dir".to_owned(),
            data_dir.display().to_string(),
        ];
        Self::new(
            manager,
            service_id,
            &executable,
            arguments,
            manager.default_account(),
            data_dir,
        )
    }

    pub fn validate(&self) -> Result<(), LifecycleError> {
        self.validate_fields()?;
        if let Some(fingerprint) = &self.definition_fingerprint
            && fingerprint != &self.fingerprint()?
        {
            return Err(LifecycleError::UnsafeOwnership(
                "service definition fingerprint does not match its fields".to_owned(),
            ));
        }
        Ok(())
    }

    fn validate_fields(&self) -> Result<(), LifecycleError> {
        if !self.executable.is_absolute() || !self.data_dir.is_absolute() {
            return Err(LifecycleError::UnsafeOwnership(
                "service executable and data directory must be absolute paths".to_owned(),
            ));
        }
        if self
            .executable
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
            || self
                .data_dir
                .components()
                .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        {
            return Err(LifecycleError::UnsafeOwnership(
                "service paths must be normalized absolute paths".to_owned(),
            ));
        }
        validate_service_id(self.manager, &self.service_id)?;
        if self.account.trim().is_empty()
            || self.account.contains(['\0', '\r', '\n'])
            || self
                .arguments
                .iter()
                .any(|argument| argument.contains(['\0', '\r', '\n']))
        {
            return Err(LifecycleError::UnsafeOwnership(
                "service account or arguments contain invalid control characters".to_owned(),
            ));
        }
        if self.arguments.len() != 4
            || self.arguments[0] != "service"
            || self.arguments[1] != "run"
            || self.arguments[2] != "--data-dir"
            || Path::new(&self.arguments[3]) != self.data_dir
        {
            return Err(LifecycleError::UnsafeOwnership(
                "service arguments must be exactly `service run --data-dir <definition data path>`, with no secrets or extra options".to_owned(),
            ));
        }
        Ok(())
    }

    pub fn canonical_service_id(&self) -> String {
        canonical_service_id(self.manager, &self.service_id)
    }

    pub fn service_key(&self) -> Result<ServiceKey, LifecycleError> {
        self.validate()?;
        Ok(ServiceKey {
            manager: self.manager.key().to_owned(),
            service_id: self.canonical_service_id(),
        })
    }

    pub fn fingerprint(&self) -> Result<String, LifecycleError> {
        self.validate_without_fingerprint()?;
        let fields = serde_json::to_vec(&(
            self.manager,
            self.canonical_service_id(),
            &self.executable,
            &self.arguments,
            &self.account,
            self.boot_policy,
            &self.data_dir,
        ))?;
        Ok(hex_lower(&Sha256::digest(fields)))
    }

    pub fn equivalent_to(&self, other: &Self) -> bool {
        self.manager == other.manager
            && self.canonical_service_id() == other.canonical_service_id()
            && self.executable == other.executable
            && self.arguments == other.arguments
            && self.account == other.account
            && self.boot_policy == other.boot_policy
            && self.data_dir == other.data_dir
            && self.definition_fingerprint == other.definition_fingerprint
    }

    fn validate_without_fingerprint(&self) -> Result<(), LifecycleError> {
        let mut candidate = self.clone();
        candidate.definition_fingerprint = None;
        candidate.validate_fields()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ServiceKey {
    pub manager: String,
    pub service_id: String,
}

fn validate_service_id(
    manager: ServiceManagerKind,
    service_id: &str,
) -> Result<(), LifecycleError> {
    let valid = !service_id.is_empty()
        && service_id.len() <= 128
        && service_id != "."
        && service_id != ".."
        && service_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    if !valid {
        return Err(LifecycleError::UnsafeOwnership(
            "native service identifier must be one validated path component".to_owned(),
        ));
    }
    if manager == ServiceManagerKind::WindowsScm && service_id.contains('.') {
        return Err(LifecycleError::UnsafeOwnership(
            "SCM service identifiers cannot contain periods".to_owned(),
        ));
    }
    Ok(())
}

fn canonical_service_id(manager: ServiceManagerKind, service_id: &str) -> String {
    match manager {
        ServiceManagerKind::WindowsScm => service_id.to_ascii_lowercase(),
        ServiceManagerKind::LinuxSystemd | ServiceManagerKind::MacosLaunchd => {
            service_id.to_owned()
        }
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(HEX[(byte >> 4) as usize] as char);
        result.push(HEX[(byte & 0x0f) as usize] as char);
    }
    result
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct NativeServiceObservation {
    pub installed: InstallationObservation,
    pub manager_state: NativeManagerState,
    pub definition: Option<ServiceDefinition>,
    pub account: Option<String>,
    pub boot_enabled: bool,
    pub core: Option<CoreObservation>,
    /// Nonempty when the selected data location could not be inspected well
    /// enough to prove that no Core instance owns it.
    pub core_unknown: Option<String>,
    pub diagnostic_location: Option<String>,
    /// `true` means the manager reports accepted work that is still pending.
    pub pending_operation: bool,
}

impl NativeServiceObservation {
    pub fn absent() -> Self {
        Self {
            installed: InstallationObservation::NotInstalled,
            manager_state: NativeManagerState::Stopped,
            definition: None,
            account: None,
            boot_enabled: false,
            core: None,
            core_unknown: None,
            diagnostic_location: None,
            pending_operation: false,
        }
    }

    pub fn unknown(detail: impl Into<String>) -> Self {
        Self {
            installed: InstallationObservation::Unknown,
            manager_state: NativeManagerState::Unknown,
            definition: None,
            account: None,
            boot_enabled: false,
            core: None,
            core_unknown: None,
            diagnostic_location: Some(detail.into()),
            pending_operation: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ServiceAction {
    Install,
    Start,
    Stop,
    Remove,
}

/// A helper is authorized for exactly one of these native mutation boundaries.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NativeMutation {
    Register,
    Description,
    FailurePolicy,
    Publish,
    Reload,
    Enable,
    Start,
    Stop,
    Disable,
    Delete,
}

impl NativeMutation {
    pub fn steps(manager: ServiceManagerKind, action: ServiceAction) -> &'static [Self] {
        use NativeMutation::*;
        match (manager, action) {
            (ServiceManagerKind::WindowsScm, ServiceAction::Install) => {
                &[Register, Description, FailurePolicy]
            }
            (ServiceManagerKind::LinuxSystemd, ServiceAction::Install) => {
                &[Publish, Reload, Enable]
            }
            (ServiceManagerKind::MacosLaunchd, ServiceAction::Install) => &[Publish],
            (_, ServiceAction::Start) => &[Start],
            (_, ServiceAction::Stop) => &[Stop],
            (ServiceManagerKind::LinuxSystemd, ServiceAction::Remove) => &[Disable, Delete, Reload],
            (_, ServiceAction::Remove) => &[Delete],
        }
    }
}

impl ServiceAction {
    fn label(self) -> &'static str {
        match self {
            Self::Install => "service install",
            Self::Start => "service start",
            Self::Stop => "service stop",
            Self::Remove => "service uninstall",
        }
    }
}

pub trait ServiceManagerAdapter: Send + Sync {
    /// Native adapters require a fresh journal-authorized process per step.
    /// Deterministic injected adapters execute the logical operation in-process.
    fn requires_owned_helper(&self) -> bool {
        false
    }

    fn observe(
        &self,
        manager: ServiceManagerKind,
        service_id: &str,
        deadline: Deadline,
        clock: &dyn MonotonicClock,
    ) -> Result<NativeServiceObservation, LifecycleError>;

    /// Performs exactly one logical manager operation. Native implementations
    /// must use an owned bounded helper and must not treat helper exit as
    /// cancellation of manager work.
    fn mutate(
        &self,
        action: ServiceAction,
        manager: ServiceManagerKind,
        service_id: &str,
        definition: Option<&ServiceDefinition>,
        deadline: Deadline,
        clock: &dyn MonotonicClock,
    ) -> Result<(), LifecycleError>;
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AttemptStatus {
    Prepared,
    Authorized,
    ObservedComplete,
    Reconciled,
    OutcomeUnknown,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceAttemptJournal {
    pub schema_version: u32,
    pub attempt_id: Uuid,
    pub manager: ServiceManagerKind,
    pub native_service_id: String,
    pub action: ServiceAction,
    pub step: String,
    pub expected_definition: ServiceDefinition,
    pub prior_state: NativeServiceObservation,
    pub helper_pid: Option<u32>,
    pub helper_owner: Option<String>,
    pub status: AttemptStatus,
    pub completion_observation: Option<NativeServiceObservation>,
    pub residual_resources: Vec<String>,
    pub recovery_guidance: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceOperationResult {
    pub operation: LifecycleOperationResult,
    pub observation: NativeServiceObservation,
}

#[derive(Clone, Debug)]
pub struct ServiceStateRoot {
    path: PathBuf,
    isolated: bool,
}

impl ServiceStateRoot {
    /// Dependency-injection seam for tests and embedders; the public CLI never
    /// accepts this path from users.
    pub fn isolated(path: PathBuf) -> Result<Self, LifecycleError> {
        if !path.is_absolute() {
            return Err(LifecycleError::UnsafeOwnership(
                "isolated service-state root must be absolute".to_owned(),
            ));
        }
        Ok(Self {
            path,
            isolated: true,
        })
    }

    pub fn system() -> Result<Self, LifecycleError> {
        #[cfg(windows)]
        let path = known_folders::get_known_folder_path(known_folders::KnownFolder::ProgramData)
            .ok_or_else(|| {
                LifecycleError::UnsafeOwnership(
                    "Windows ProgramData known folder could not be resolved".to_owned(),
                )
            })?
            .join("Kakune")
            .join("lifecycle")
            .join("services");
        #[cfg(target_os = "linux")]
        let path = PathBuf::from("/var/lib/kakune/lifecycle/services");
        #[cfg(target_os = "macos")]
        let path = PathBuf::from("/Library/Application Support/Kakune/lifecycle/services");
        #[cfg(not(any(windows, target_os = "linux", target_os = "macos")))]
        return Err(LifecycleError::ManagerUnavailable(
            "no machine-wide service-state root is defined for this operating system".to_owned(),
        ));
        #[cfg(any(windows, target_os = "linux", target_os = "macos"))]
        Ok(Self {
            path,
            isolated: false,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

pub struct ServiceController {
    state_root: ServiceStateRoot,
    manager: Arc<dyn ServiceManagerAdapter>,
    clock: Arc<dyn MonotonicClock>,
    waiter: Arc<dyn DeadlineWaiter>,
    helper_launcher: Arc<dyn helper::HelperLauncher>,
}

impl ServiceController {
    pub fn with_adapters(
        root: PathBuf,
        manager: Arc<dyn ServiceManagerAdapter>,
        clock: Arc<dyn MonotonicClock>,
        waiter: Arc<dyn DeadlineWaiter>,
    ) -> Self {
        Self::with_components(
            ServiceStateRoot::isolated(root)
                .expect("injected lifecycle test root must be absolute"),
            manager,
            clock,
            waiter,
        )
    }

    pub fn with_components(
        state_root: ServiceStateRoot,
        manager: Arc<dyn ServiceManagerAdapter>,
        clock: Arc<dyn MonotonicClock>,
        waiter: Arc<dyn DeadlineWaiter>,
    ) -> Self {
        Self {
            state_root,
            manager,
            clock,
            waiter,
            helper_launcher: Arc::new(helper::SystemHelperLauncher),
        }
    }

    pub fn for_system(manager: Arc<dyn ServiceManagerAdapter>) -> Result<Self, LifecycleError> {
        Ok(Self::with_components(
            ServiceStateRoot::system()?,
            manager,
            Arc::new(super::SystemMonotonicClock::default()),
            Arc::new(super::ThreadWaiter),
        ))
    }

    /// Process-adapter injection for isolated deterministic tests and embedders.
    pub fn with_helper_launcher(mut self, launcher: Arc<dyn helper::HelperLauncher>) -> Self {
        self.helper_launcher = launcher;
        self
    }

    pub fn state_root(&self) -> &Path {
        self.state_root.path()
    }

    pub fn attempt_path(&self, definition: &ServiceDefinition) -> PathBuf {
        self.paths_for(definition.manager, &definition.service_id)
            .map(|paths| paths.attempt)
            .unwrap_or_else(|_| self.state_root.path().join("invalid").join(ATTEMPT_FILE))
    }

    /// Returns the stable per-service journal path without creating storage.
    pub fn attempt_path_for(
        &self,
        manager: ServiceManagerKind,
        service_id: &str,
    ) -> Result<PathBuf, LifecycleError> {
        Ok(self.paths_for(manager, service_id)?.attempt)
    }

    pub fn read_attempt(
        &self,
        definition: &ServiceDefinition,
    ) -> Result<Option<ServiceAttemptJournal>, LifecycleError> {
        self.read_attempt_for(definition.manager, &definition.service_id)
    }

    /// Reads existing recovery evidence without a manager query, storage creation,
    /// or reconciliation. A snapshot is historical, not a fresh observation.
    pub fn read_attempt_for(
        &self,
        manager: ServiceManagerKind,
        service_id: &str,
    ) -> Result<Option<ServiceAttemptJournal>, LifecycleError> {
        let paths = self.paths_for(manager, service_id)?;
        read_attempt_for_service(&paths, self.state_root.isolated)
    }

    pub fn install(
        &self,
        definition: &ServiceDefinition,
    ) -> Result<ServiceOperationResult, LifecycleError> {
        definition.validate()?;
        let command = self.command_deadline(INSTALL_COMMAND_CEILING);
        let lock_deadline = Deadline::after(self.clock.as_ref(), ACQUISITION_BUDGET)
            .cap(self.clock.as_ref(), command.remaining(self.clock.as_ref()));
        let paths = self.paths_for(definition.manager, &definition.service_id)?;
        let _lease = acquire_service_lease(
            &paths,
            self.state_root.isolated,
            lock_deadline,
            self.clock.as_ref(),
            self.waiter.as_ref(),
        )?;
        let preflight = Deadline::after(self.clock.as_ref(), PREFLIGHT_BUDGET)
            .cap(self.clock.as_ref(), command.remaining(self.clock.as_ref()));
        read_attempt_for_service(&paths, self.state_root.isolated)?;
        let current = self.observe(definition.manager, &definition.service_id, preflight)?;
        self.reconcile_pending(&paths, &current, preflight)?;
        if current.pending_operation {
            return Err(LifecycleError::ServiceStateUnconfirmed);
        }
        match current.installed {
            InstallationObservation::Installed => {
                let actual = current.definition.as_ref().ok_or_else(|| {
                    LifecycleError::UnsafeOwnership(
                        "installed service definition could not be verified".to_owned(),
                    )
                })?;
                if !definition.equivalent_to(actual) || !current.boot_enabled {
                    return Err(LifecycleError::ConflictingServiceDefinition(
                        "executable, arguments, data path, account, or boot policy differs".into(),
                    ));
                }
                return Ok(self.operation_result(
                    ServiceAction::Install,
                    command,
                    definition,
                    current,
                    OperationOutcome::Noop,
                    Vec::new(),
                ));
            }
            InstallationObservation::Unknown => {
                return Err(LifecycleError::UnsafeOwnership(
                    "service installation state is unknown; no mutation was attempted".to_owned(),
                ));
            }
            InstallationObservation::NotInstalled => {}
        }
        let install_deadline = Deadline::after(self.clock.as_ref(), INSTALL_BUDGET)
            .cap(self.clock.as_ref(), command.remaining(self.clock.as_ref()));
        let observation = self.mutate_and_confirm(
            &paths,
            ServiceAction::Install,
            definition,
            current,
            install_deadline,
        )?;
        if observation.installed != InstallationObservation::Installed
            || observation
                .definition
                .as_ref()
                .is_none_or(|actual| !definition.equivalent_to(actual))
            || !observation.boot_enabled
            || observation.manager_state != NativeManagerState::Stopped
        {
            return Err(LifecycleError::ServiceStateUnconfirmed);
        }
        Ok(self.operation_result(
            ServiceAction::Install,
            command,
            definition,
            observation,
            OperationOutcome::Success,
            vec![format!(
                "{}:{}",
                definition.manager.key(),
                definition.service_id
            )],
        ))
    }

    pub fn start(
        &self,
        manager: ServiceManagerKind,
        service_id: &str,
    ) -> Result<ServiceOperationResult, LifecycleError> {
        validate_service_id(manager, service_id)?;
        let command = self.command_deadline(START_COMMAND_CEILING);
        let paths = self.paths_for(manager, service_id)?;
        let acquisition = Deadline::after(self.clock.as_ref(), ACQUISITION_BUDGET)
            .cap(self.clock.as_ref(), command.remaining(self.clock.as_ref()));
        let _lease = acquire_service_lease(
            &paths,
            self.state_root.isolated,
            acquisition,
            self.clock.as_ref(),
            self.waiter.as_ref(),
        )?;
        let preflight = Deadline::after(self.clock.as_ref(), PREFLIGHT_BUDGET)
            .cap(self.clock.as_ref(), command.remaining(self.clock.as_ref()));
        read_attempt_for_service(&paths, self.state_root.isolated)?;
        let current = self.observe(manager, service_id, preflight)?;
        self.reconcile_pending(&paths, &current, preflight)?;
        let definition = require_definition(&current)?;
        if current.installed == InstallationObservation::NotInstalled {
            return Err(LifecycleError::ServiceNotInstalled);
        }
        if let Some(detail) = &current.core_unknown {
            return Err(LifecycleError::UnsafeOwnership(format!(
                "cannot verify the installed service data owner: {detail}"
            )));
        }
        if let Some(core) = &current.core
            && core.mode != CoreMode::SystemService
        {
            return Err(LifecycleError::UnsafeOwnership(format!(
                "service data directory is owned by a {} Core instance",
                core.mode
            )));
        }
        if current.manager_state == NativeManagerState::Running
            && !current.pending_operation
            && core_is_ready(current.core.as_ref())
        {
            return Ok(self.operation_result(
                ServiceAction::Start,
                command,
                &definition,
                current,
                OperationOutcome::Noop,
                Vec::new(),
            ));
        }
        if current.manager_state == NativeManagerState::Running {
            return Err(LifecycleError::ServiceStateUnconfirmed);
        }
        if current.manager_state == NativeManagerState::Starting {
            let readiness = Deadline::after(self.clock.as_ref(), READINESS_BUDGET)
                .cap(self.clock.as_ref(), command.remaining(self.clock.as_ref()));
            let observation = self.wait_for_ready(&definition, &paths, readiness)?;
            return Ok(self.operation_result(
                ServiceAction::Start,
                command,
                &definition,
                observation,
                OperationOutcome::Success,
                Vec::new(),
            ));
        }
        if current.pending_operation {
            return Err(LifecycleError::ServiceStateUnconfirmed);
        }
        if current.manager_state != NativeManagerState::Stopped {
            return Err(LifecycleError::ServiceStateUnconfirmed);
        }
        let readiness = Deadline::after(self.clock.as_ref(), READINESS_BUDGET)
            .cap(self.clock.as_ref(), command.remaining(self.clock.as_ref()));
        self.mutate_and_authorize(
            &paths,
            ServiceAction::Start,
            &definition,
            current,
            readiness,
        )?;
        let observation = self.wait_for_ready(&definition, &paths, readiness)?;
        if observation.manager_state != NativeManagerState::Running
            || !core_is_ready(observation.core.as_ref())
        {
            self.record_failed_attempt(&paths, &observation)?;
            return Err(LifecycleError::ServiceStateUnconfirmed);
        }
        self.complete_attempt(&paths, &observation)?;
        Ok(self.operation_result(
            ServiceAction::Start,
            command,
            &definition,
            observation,
            OperationOutcome::Success,
            Vec::new(),
        ))
    }

    pub fn status(
        &self,
        manager: ServiceManagerKind,
        service_id: &str,
    ) -> Result<NativeServiceObservation, LifecycleError> {
        validate_service_id(manager, service_id)?;
        let deadline = Deadline::after(self.clock.as_ref(), STATUS_BUDGET);
        let paths = self.paths_for(manager, service_id)?;
        check_existing_state_path(&paths, self.state_root.isolated)?;
        let attempt = read_attempt_for_service(&paths, self.state_root.isolated)?;
        let mut observation = self.observe(manager, service_id, deadline)?;
        observation.pending_operation |=
            attempt.is_some_and(|attempt| attempt.status != AttemptStatus::Reconciled);
        ensure_remaining(deadline, self.clock.as_ref(), "service status")?;
        Ok(observation)
    }

    pub fn stop(
        &self,
        manager: ServiceManagerKind,
        service_id: &str,
    ) -> Result<ServiceOperationResult, LifecycleError> {
        validate_service_id(manager, service_id)?;
        let command = self.command_deadline(STOP_COMMAND_CEILING);
        let paths = self.paths_for(manager, service_id)?;
        let acquisition = Deadline::after(self.clock.as_ref(), ACQUISITION_BUDGET)
            .cap(self.clock.as_ref(), command.remaining(self.clock.as_ref()));
        let _lease = acquire_service_lease(
            &paths,
            self.state_root.isolated,
            acquisition,
            self.clock.as_ref(),
            self.waiter.as_ref(),
        )?;
        let preflight = Deadline::after(self.clock.as_ref(), PREFLIGHT_BUDGET)
            .cap(self.clock.as_ref(), command.remaining(self.clock.as_ref()));
        read_attempt_for_service(&paths, self.state_root.isolated)?;
        let current = self.observe(manager, service_id, preflight)?;
        self.reconcile_pending(&paths, &current, preflight)?;
        if let Some(detail) = &current.core_unknown {
            return Err(LifecycleError::UnsafeOwnership(format!(
                "cannot safely stop a service with unknown Core state: {detail}"
            )));
        }
        if current.pending_operation {
            return Err(LifecycleError::ServiceStateUnconfirmed);
        }
        ensure_service_mode(&current)?;
        if current.installed == InstallationObservation::NotInstalled
            || (current.manager_state == NativeManagerState::Stopped
                && core_is_absent_or_stopped(
                    current.core.as_ref(),
                    current.core_unknown.as_deref(),
                ))
        {
            let definition = current
                .definition
                .clone()
                .unwrap_or_else(|| fallback_definition(manager, service_id));
            return Ok(self.operation_result(
                ServiceAction::Stop,
                command,
                &definition,
                current,
                OperationOutcome::Noop,
                Vec::new(),
            ));
        }
        let definition = require_definition(&current)?;
        let dispatch = Deadline::after(self.clock.as_ref(), STOP_DISPATCH_BUDGET)
            .cap(self.clock.as_ref(), command.remaining(self.clock.as_ref()));
        self.mutate_and_authorize(&paths, ServiceAction::Stop, &definition, current, dispatch)?;
        let grace = Deadline::after(self.clock.as_ref(), CORE_GRACE_BUDGET)
            .cap(self.clock.as_ref(), command.remaining(self.clock.as_ref()));
        let observation = self.wait_for_stopped(&definition, grace)?;
        self.complete_attempt(&paths, &observation)?;
        Ok(self.operation_result(
            ServiceAction::Stop,
            command,
            &definition,
            observation,
            OperationOutcome::Success,
            Vec::new(),
        ))
    }

    pub fn uninstall(
        &self,
        manager: ServiceManagerKind,
        service_id: &str,
    ) -> Result<ServiceOperationResult, LifecycleError> {
        validate_service_id(manager, service_id)?;
        let command = self.command_deadline(UNINSTALL_COMMAND_CEILING);
        let paths = self.paths_for(manager, service_id)?;
        let acquisition = Deadline::after(self.clock.as_ref(), ACQUISITION_BUDGET)
            .cap(self.clock.as_ref(), command.remaining(self.clock.as_ref()));
        let _lease = acquire_service_lease(
            &paths,
            self.state_root.isolated,
            acquisition,
            self.clock.as_ref(),
            self.waiter.as_ref(),
        )?;
        let preflight = Deadline::after(self.clock.as_ref(), PREFLIGHT_BUDGET)
            .cap(self.clock.as_ref(), command.remaining(self.clock.as_ref()));
        read_attempt_for_service(&paths, self.state_root.isolated)?;
        let mut current = self.observe(manager, service_id, preflight)?;
        self.reconcile_pending(&paths, &current, preflight)?;
        if let Some(detail) = &current.core_unknown {
            return Err(LifecycleError::UnsafeOwnership(format!(
                "cannot safely uninstall a service with unknown Core state: {detail}"
            )));
        }
        if current.pending_operation {
            return Err(LifecycleError::ServiceStateUnconfirmed);
        }
        ensure_service_mode(&current)?;
        if current.installed == InstallationObservation::NotInstalled {
            let definition = fallback_definition(manager, service_id);
            return Ok(self.operation_result(
                ServiceAction::Remove,
                command,
                &definition,
                current,
                OperationOutcome::Noop,
                Vec::new(),
            ));
        }
        let definition = require_definition(&current)?;
        if current.manager_state != NativeManagerState::Stopped
            || !core_is_absent_or_stopped(current.core.as_ref(), current.core_unknown.as_deref())
        {
            let dispatch = Deadline::after(self.clock.as_ref(), STOP_DISPATCH_BUDGET)
                .cap(self.clock.as_ref(), command.remaining(self.clock.as_ref()));
            self.mutate_and_authorize(&paths, ServiceAction::Stop, &definition, current, dispatch)?;
            let grace = Deadline::after(self.clock.as_ref(), CORE_GRACE_BUDGET)
                .cap(self.clock.as_ref(), command.remaining(self.clock.as_ref()));
            current = self.wait_for_stopped(&definition, grace)?;
            self.complete_attempt(&paths, &current)?;
        }
        if current.manager_state != NativeManagerState::Stopped
            || !core_is_absent_or_stopped(current.core.as_ref(), current.core_unknown.as_deref())
        {
            return Err(LifecycleError::ServiceStateUnconfirmed);
        }
        let removal = Deadline::after(self.clock.as_ref(), REMOVAL_BUDGET)
            .cap(self.clock.as_ref(), command.remaining(self.clock.as_ref()));
        let after =
            self.mutate_and_confirm(&paths, ServiceAction::Remove, &definition, current, removal)?;
        if after.installed != InstallationObservation::NotInstalled {
            return Err(LifecycleError::ServiceStateUnconfirmed);
        }
        Ok(self.operation_result(
            ServiceAction::Remove,
            command,
            &definition,
            after,
            OperationOutcome::Success,
            vec![format!("{}:{}", manager.key(), service_id)],
        ))
    }

    fn command_deadline(&self, budget: Duration) -> Deadline {
        Deadline::after(self.clock.as_ref(), budget)
    }

    fn observe(
        &self,
        manager: ServiceManagerKind,
        service_id: &str,
        deadline: Deadline,
    ) -> Result<NativeServiceObservation, LifecycleError> {
        ensure_remaining(deadline, self.clock.as_ref(), "service observation")?;
        let observation =
            self.manager
                .observe(manager, service_id, deadline, self.clock.as_ref())?;
        ensure_remaining(deadline, self.clock.as_ref(), "service observation")?;
        if observation.installed == InstallationObservation::Unknown
            || observation.manager_state == NativeManagerState::Unknown
        {
            return Err(LifecycleError::UnsafeOwnership(
                observation
                    .diagnostic_location
                    .clone()
                    .unwrap_or_else(|| "native service state is unknown".to_owned()),
            ));
        }
        if observation.installed == InstallationObservation::Installed {
            let definition = require_definition(&observation)?;
            definition.validate()?;
            if definition.manager != manager
                || definition.canonical_service_id() != canonical_service_id(manager, service_id)
            {
                return Err(LifecycleError::ConflictingServiceDefinition(
                    "native observation belongs to another manager or service identifier".into(),
                ));
            }
        }
        Ok(observation)
    }

    fn mutate_and_confirm(
        &self,
        paths: &ServicePaths,
        action: ServiceAction,
        definition: &ServiceDefinition,
        prior: NativeServiceObservation,
        deadline: Deadline,
    ) -> Result<NativeServiceObservation, LifecycleError> {
        self.mutate_and_authorize(paths, action, definition, prior, deadline)?;
        let mut observation = self.observe(definition.manager, &definition.service_id, deadline)?;
        while observation.pending_operation {
            let remaining =
                ensure_remaining(deadline, self.clock.as_ref(), "native service convergence")?;
            self.waiter.wait(remaining.min(OBSERVATION_RETRY_INTERVAL));
            observation = self.observe(definition.manager, &definition.service_id, deadline)?;
        }
        if !operation_satisfied(action, definition, &observation) {
            self.record_failed_attempt(paths, &observation)?;
            return Err(LifecycleError::ServiceStateUnconfirmed);
        }
        self.complete_attempt(paths, &observation)?;
        Ok(observation)
    }

    fn mutate_and_authorize(
        &self,
        paths: &ServicePaths,
        action: ServiceAction,
        definition: &ServiceDefinition,
        prior: NativeServiceObservation,
        deadline: Deadline,
    ) -> Result<(), LifecycleError> {
        if self.manager.requires_owned_helper() {
            return helper::run_mutations(self, paths, action, definition, prior, deadline);
        }
        ensure_remaining(deadline, self.clock.as_ref(), action.label())?;
        let mut journal = ServiceAttemptJournal {
            schema_version: SERVICE_STATE_SCHEMA_VERSION,
            attempt_id: Uuid::new_v4(),
            manager: definition.manager,
            native_service_id: definition.service_id.clone(),
            action,
            step: action.label().to_owned(),
            expected_definition: definition.clone(),
            prior_state: prior,
            helper_pid: None,
            helper_owner: None,
            status: AttemptStatus::Prepared,
            completion_observation: None,
            residual_resources: Vec::new(),
            recovery_guidance: vec![
                "Inspect the native manager and reconcile this Kakune service attempt before retrying.".to_owned(),
            ],
        };
        write_attempt_file(&paths.attempt, &journal, self.state_root.isolated)?;
        journal.status = AttemptStatus::Authorized;
        write_attempt_file(&paths.attempt, &journal, self.state_root.isolated)?;
        let helper_deadline = deadline.cap(self.clock.as_ref(), HELPER_BUDGET);
        let mutation = self
            .manager
            .mutate(
                action,
                definition.manager,
                &definition.service_id,
                Some(definition),
                helper_deadline,
                self.clock.as_ref(),
            )
            .and_then(|()| {
                ensure_remaining(
                    helper_deadline,
                    self.clock.as_ref(),
                    "native service helper",
                )
                .map(|_| ())
            });
        if let Err(error) = mutation {
            journal.status = AttemptStatus::OutcomeUnknown;
            if let LifecycleError::HelperUnreaped { pid } = &error {
                journal.helper_pid = Some(*pid);
            }
            journal.residual_resources.push(format!(
                "authorized {} mutation has an unconfirmed outcome",
                action.label()
            ));
            write_attempt_file(&paths.attempt, &journal, self.state_root.isolated)?;
            return Err(error);
        }
        Ok(())
    }

    fn wait_for_ready(
        &self,
        definition: &ServiceDefinition,
        paths: &ServicePaths,
        deadline: Deadline,
    ) -> Result<NativeServiceObservation, LifecycleError> {
        loop {
            let observation = self.observe(definition.manager, &definition.service_id, deadline)?;
            ensure_expected_definition(definition, &observation)?;
            if let Some(detail) = &observation.core_unknown {
                return Err(LifecycleError::UnsafeOwnership(format!(
                    "cannot verify Core readiness: {detail}"
                )));
            }
            if observation.manager_state == NativeManagerState::Failed {
                // The manager conclusively refused the requested state. Record
                // that settled outcome instead of leaving a permanent barrier.
                self.record_failed_attempt(paths, &observation)?;
                return Err(LifecycleError::ServiceStateUnconfirmed);
            }
            if observation.manager_state == NativeManagerState::Running
                && core_is_ready(observation.core.as_ref())
                && !observation.pending_operation
            {
                return Ok(observation);
            }
            let remaining = ensure_remaining(deadline, self.clock.as_ref(), "service readiness")?;
            self.waiter.wait(remaining.min(OBSERVATION_RETRY_INTERVAL));
        }
    }

    fn wait_for_stopped(
        &self,
        definition: &ServiceDefinition,
        deadline: Deadline,
    ) -> Result<NativeServiceObservation, LifecycleError> {
        loop {
            let observation = self.observe(definition.manager, &definition.service_id, deadline)?;
            ensure_expected_definition(definition, &observation)?;
            ensure_service_mode(&observation)?;
            // Metadata can disappear just before the exiting Core releases its
            // lifetime lease. Unknown evidence never confirms stop, but after
            // the one verified dispatch it may be re-observed within this same
            // grace deadline. Do not redispatch, signal, or renew the budget.
            if observation.manager_state == NativeManagerState::Stopped
                && !observation.pending_operation
                && core_is_absent_or_stopped(
                    observation.core.as_ref(),
                    observation.core_unknown.as_deref(),
                )
            {
                return Ok(observation);
            }
            let remaining = ensure_remaining(deadline, self.clock.as_ref(), "service stop")?;
            self.waiter.wait(remaining.min(OBSERVATION_RETRY_INTERVAL));
        }
    }

    fn reconcile_pending(
        &self,
        paths: &ServicePaths,
        observation: &NativeServiceObservation,
        deadline: Deadline,
    ) -> Result<(), LifecycleError> {
        let Some(mut journal) = read_attempt_for_service(paths, self.state_root.isolated)? else {
            return Ok(());
        };
        if journal.schema_version != SERVICE_STATE_SCHEMA_VERSION {
            return Err(LifecycleError::UnsupportedSchema(journal.schema_version));
        }
        ensure_remaining(
            deadline,
            self.clock.as_ref(),
            "service journal reconciliation",
        )?;
        if self.manager.requires_owned_helper()
            && !helper::is_native_step(&journal)
            && !matches!(
                journal.status,
                AttemptStatus::Prepared | AttemptStatus::Reconciled
            )
        {
            return Err(LifecycleError::UnresolvedServiceAttempt(
                "legacy operation-level native attempt lacks single-step completion evidence; inspect its manager effects before recovery".into(),
            ));
        }
        if helper::is_native_step(&journal)
            && journal.status == AttemptStatus::OutcomeUnknown
            && !(native_attempt_outcome_is_known(&journal)
                && native_effects_are_settled(&journal, observation))
        {
            // A native step whose effect is unknown stays a barrier until the
            // manager itself proves that no work remains pending, the observed
            // state is conclusive, and the journal proves the native command
            // actually ran. A matching definition alone never releases it.
            return Err(LifecycleError::UnresolvedServiceAttempt(format!(
                "{} ({}) has unknown native effects at {}; a matching definition alone cannot prove this step completed or that manager work was cancelled",
                journal.action.label(),
                journal.attempt_id,
                journal.step
            )));
        }
        if helper::is_native_step(&journal)
            && !helper::is_final_step(&journal)
            && (journal.status != AttemptStatus::Prepared
                || !journal.step.starts_with("0:")
                || journal.helper_pid.is_some())
        {
            return Err(LifecycleError::UnresolvedServiceAttempt(format!(
                "{} ({}) stopped at native step {}; inspect the recorded partial resources before recovery",
                journal.action.label(),
                journal.attempt_id,
                journal.step
            )));
        }
        if journal.status == AttemptStatus::Prepared {
            if journal.helper_pid.is_none() {
                fs::remove_file(&paths.attempt)?;
                return Ok(());
            }
            return Err(LifecycleError::UnresolvedServiceAttempt(format!(
                "{} ({}) was prepared for helper {} but was not authorized",
                journal.action.label(),
                journal.attempt_id,
                journal.helper_pid.unwrap_or_default()
            )));
        }
        if journal.status == AttemptStatus::Reconciled {
            fs::remove_file(&paths.attempt)?;
            return Ok(());
        }
        if let Some(pid) = journal.helper_pid {
            // A matching registration does not establish helper exit or rule
            // out a later mutation by an authorized, unreaped helper. Until
            // retained ownership can be positively reconciled, preserve the
            // journal byte-for-byte and do not signal this discovered PID.
            return Err(LifecycleError::UnresolvedServiceAttempt(format!(
                "{} ({}) retains unconfirmed helper {pid}; verify helper exit and pending manager work before retrying",
                journal.action.label(),
                journal.attempt_id,
            )));
        }
        if helper::is_native_step(&journal)
            && journal.status != AttemptStatus::Reconciled
            && native_attempt_outcome_is_known(&journal)
            && native_effects_are_settled(&journal, observation)
        {
            // The native command ran to completion and the manager reports a
            // conclusive terminal state with no pending work. Release the
            // barrier so a later command can act on the observed state.
            journal.status = AttemptStatus::ObservedComplete;
            journal.completion_observation = Some(observation.clone());
            write_attempt_file(&paths.attempt, &journal, self.state_root.isolated)?;
            journal.status = AttemptStatus::Reconciled;
            write_attempt_file(&paths.attempt, &journal, self.state_root.isolated)?;
            fs::remove_file(&paths.attempt)?;
            return Ok(());
        }
        if operation_satisfied(journal.action, &journal.expected_definition, observation)
            && !observation.pending_operation
        {
            journal.status = AttemptStatus::ObservedComplete;
            journal.completion_observation = Some(observation.clone());
            write_attempt_file(&paths.attempt, &journal, self.state_root.isolated)?;
            journal.status = AttemptStatus::Reconciled;
            write_attempt_file(&paths.attempt, &journal, self.state_root.isolated)?;
            fs::remove_file(&paths.attempt)?;
            return Ok(());
        }
        if deadline.is_expired(self.clock.as_ref()) {
            return Err(LifecycleError::DeadlineExpired {
                phase: "service journal reconciliation",
            });
        }
        if journal.status != AttemptStatus::OutcomeUnknown {
            journal.status = AttemptStatus::OutcomeUnknown;
            journal.residual_resources.push(
                "native manager effect is not confirmed; registration was not changed again".into(),
            );
            write_attempt_file(&paths.attempt, &journal, self.state_root.isolated)?;
        }
        Err(LifecycleError::UnresolvedServiceAttempt(format!(
            "{} ({})",
            journal.action.label(),
            journal.attempt_id
        )))
    }

    /// Records a native attempt whose requested state was not reached.
    ///
    /// When the manager proves that no work remains pending and the observed
    /// state is conclusive, the attempt is reconciled so a later command can
    /// act on the observed state. Otherwise the attempt keeps its durable
    /// barrier because its native effect is still unknown.
    fn record_failed_attempt(
        &self,
        paths: &ServicePaths,
        observation: &NativeServiceObservation,
    ) -> Result<(), LifecycleError> {
        let Some(mut journal) = read_attempt_for_service(paths, self.state_root.isolated)? else {
            return Ok(());
        };
        if native_effects_are_settled(&journal, observation)
            && native_attempt_outcome_is_known(&journal)
        {
            journal.status = AttemptStatus::ObservedComplete;
            journal.completion_observation = Some(observation.clone());
            write_attempt_file(&paths.attempt, &journal, self.state_root.isolated)?;
            journal.status = AttemptStatus::Reconciled;
            write_attempt_file(&paths.attempt, &journal, self.state_root.isolated)?;
            return match fs::remove_file(&paths.attempt) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error.into()),
            };
        }
        self.mark_attempt_unknown(paths, observation)
    }

    fn complete_attempt(
        &self,
        paths: &ServicePaths,
        observation: &NativeServiceObservation,
    ) -> Result<(), LifecycleError> {
        let Some(mut journal) = read_attempt_for_service(paths, self.state_root.isolated)? else {
            return Ok(());
        };
        journal.status = AttemptStatus::ObservedComplete;
        journal.completion_observation = Some(observation.clone());
        write_attempt_file(&paths.attempt, &journal, self.state_root.isolated)?;
        journal.status = AttemptStatus::Reconciled;
        write_attempt_file(&paths.attempt, &journal, self.state_root.isolated)?;
        match fs::remove_file(&paths.attempt) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn mark_attempt_unknown(
        &self,
        paths: &ServicePaths,
        observation: &NativeServiceObservation,
    ) -> Result<(), LifecycleError> {
        let Some(mut journal) = read_attempt_for_service(paths, self.state_root.isolated)? else {
            return Ok(());
        };
        let changed = journal.status != AttemptStatus::OutcomeUnknown
            || journal.completion_observation.as_ref() != Some(observation)
            || !journal
                .residual_resources
                .iter()
                .any(|resource| resource == "requested state was not confirmed");
        journal.status = AttemptStatus::OutcomeUnknown;
        journal.completion_observation = Some(observation.clone());
        if !journal
            .residual_resources
            .iter()
            .any(|resource| resource == "requested state was not confirmed")
        {
            journal
                .residual_resources
                .push("requested state was not confirmed".into());
        }
        if changed {
            write_attempt_file(&paths.attempt, &journal, self.state_root.isolated)
        } else {
            Ok(())
        }
    }

    fn operation_result(
        &self,
        action: ServiceAction,
        command: Deadline,
        definition: &ServiceDefinition,
        observation: NativeServiceObservation,
        outcome: OperationOutcome,
        changed_resources: Vec<String>,
    ) -> ServiceOperationResult {
        let core_state = observation.core.as_ref().map_or_else(
            || {
                if observation.core_unknown.is_some() {
                    ObservedState::Unknown
                } else {
                    ObservedState::Stopped
                }
            },
            |core| core.state,
        );
        ServiceOperationResult {
            operation: LifecycleOperationResult {
                action: action.label().to_owned(),
                scope: "system_service".to_owned(),
                outcome,
                observed_state: core_state,
                verified_pid: observation
                    .core
                    .as_ref()
                    .and_then(|core| core.process.as_ref())
                    .map(|process| process.pid),
                data_dir: Some(definition.data_dir.clone()),
                diagnostic_location: observation.diagnostic_location.clone(),
                changed_resources,
                remaining_resources: if observation.pending_operation {
                    vec![format!("{}:{} pending manager operation", definition.manager.key(), definition.service_id)]
                } else {
                    Vec::new()
                },
                recovery_guidance: Vec::new(),
                elapsed_application_wait: match action {
                    ServiceAction::Install => INSTALL_COMMAND_CEILING,
                    ServiceAction::Start => START_COMMAND_CEILING,
                    ServiceAction::Stop => STOP_COMMAND_CEILING,
                    ServiceAction::Remove => UNINSTALL_COMMAND_CEILING,
                }.saturating_sub(command.remaining(self.clock.as_ref())),
                platform_completion_limitations: vec![
                    "Native manager requests may outlive a bounded helper; unresolved effects remain journaled.".to_owned(),
                ],
            },
            observation,
        }
    }

    fn paths_for(
        &self,
        manager: ServiceManagerKind,
        service_id: &str,
    ) -> Result<ServicePaths, LifecycleError> {
        validate_service_id(manager, service_id)?;
        let key = canonical_service_id(manager, service_id);
        if !self.state_root.isolated
            && key != canonical_service_id(manager, manager.platform_service_id())
        {
            return Err(LifecycleError::UnsafeOwnership(
                "production service operations are limited to Kakune's native service identifier"
                    .to_owned(),
            ));
        }
        let directory = self.state_root.path().join(manager.key()).join(key);
        Ok(ServicePaths {
            manager,
            service_id: service_id.to_owned(),
            directory: directory.clone(),
            lock: directory.join(ADMIN_LOCK_FILE),
            attempt: directory.join(ATTEMPT_FILE),
        })
    }
}

struct ServicePaths {
    manager: ServiceManagerKind,
    service_id: String,
    directory: PathBuf,
    lock: PathBuf,
    attempt: PathBuf,
}

fn read_attempt_for_service(
    paths: &ServicePaths,
    isolated: bool,
) -> Result<Option<ServiceAttemptJournal>, LifecycleError> {
    let Some(journal) = read_attempt_file(&paths.attempt, isolated)? else {
        return Ok(None);
    };
    let requested_id = canonical_service_id(paths.manager, &paths.service_id);
    if journal.manager != paths.manager
        || canonical_service_id(journal.manager, &journal.native_service_id) != requested_id
        || journal.expected_definition.manager != paths.manager
        || journal.expected_definition.canonical_service_id() != requested_id
    {
        return Err(LifecycleError::UnsafeOwnership(
            "service attempt identity does not match its machine-wide journal key".to_owned(),
        ));
    }
    Ok(Some(journal))
}

struct ServiceLease {
    _lock: File,
}

fn acquire_service_lease(
    paths: &ServicePaths,
    isolated: bool,
    deadline: Deadline,
    clock: &dyn MonotonicClock,
    waiter: &dyn DeadlineWaiter,
) -> Result<ServiceLease, LifecycleError> {
    let admin = acquire_service_lock(paths, isolated, deadline, clock, waiter)?;
    // An authorized helper may outlive its parent. It retains a second stable
    // service-wide lock; do not reconcile or mutate while that owner exists.
    let helper = acquire_helper_lease(paths, isolated, deadline, clock, waiter)?;
    drop(helper);
    Ok(admin)
}

fn acquire_helper_lease(
    paths: &ServicePaths,
    isolated: bool,
    deadline: Deadline,
    clock: &dyn MonotonicClock,
    waiter: &dyn DeadlineWaiter,
) -> Result<ServiceLease, LifecycleError> {
    let helper_paths = ServicePaths {
        manager: paths.manager,
        service_id: paths.service_id.clone(),
        directory: paths.directory.clone(),
        lock: paths.directory.join("helper.lock"),
        attempt: paths.attempt.clone(),
    };
    acquire_service_lock(&helper_paths, isolated, deadline, clock, waiter)
}

fn acquire_service_lock(
    paths: &ServicePaths,
    isolated: bool,
    deadline: Deadline,
    clock: &dyn MonotonicClock,
    waiter: &dyn DeadlineWaiter,
) -> Result<ServiceLease, LifecycleError> {
    create_service_state_directory(&paths.directory, isolated)?;
    reject_redirected_path(&paths.lock)?;
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    #[cfg(windows)]
    let lock_preexisted = paths.lock.exists();
    let lock = options.open(&paths.lock)?;
    #[cfg(windows)]
    if !isolated {
        if !lock_preexisted {
            windows_state::protect_file(&paths.lock)?;
        }
        windows_state::validate_file(&paths.lock)?;
    }
    #[cfg(unix)]
    if !isolated {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let metadata = fs::symlink_metadata(&paths.lock)?;
        if !metadata.is_file()
            || metadata.file_type().is_symlink()
            || metadata.uid() != 0
            || metadata.permissions().mode() & 0o077 != 0
        {
            return Err(LifecycleError::InsecureProtection(
                "service administrative lock must be root-owned and mode 0600".to_owned(),
            ));
        }
    }
    loop {
        match lock.try_lock() {
            Ok(()) => return Ok(ServiceLease { _lock: lock }),
            Err(std::fs::TryLockError::WouldBlock) => {
                let remaining = deadline.remaining(clock);
                if remaining.is_zero() {
                    return Err(LifecycleError::DeadlineExpired {
                        phase: "service administrative-lock acquisition",
                    });
                }
                waiter.wait(remaining.min(LOCK_RETRY_INTERVAL));
            }
            Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
        }
    }
}

fn create_service_state_directory(path: &Path, isolated: bool) -> Result<(), LifecycleError> {
    if !path.is_absolute() {
        return Err(LifecycleError::UnsafeOwnership(
            "service-state path must be absolute".to_owned(),
        ));
    }
    if isolated {
        fs::create_dir_all(path)?;
        reject_redirected_path(path)?;
        return Ok(());
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
        let effective_uid = rustix::process::geteuid().as_raw();
        if effective_uid != 0 {
            return Err(LifecycleError::AccessDenied(
                "service registration requires the root account".to_owned(),
            ));
        }
        let mut current = PathBuf::new();
        for component in path.components() {
            current.push(component.as_os_str());
            if current.as_os_str().is_empty() {
                continue;
            }
            match fs::symlink_metadata(&current) {
                Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
                Ok(_) => {
                    return Err(LifecycleError::UnsafeOwnership(format!(
                        "{} is redirected or is not a directory",
                        current.display()
                    )));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    let mut builder = fs::DirBuilder::new();
                    builder.mode(0o700);
                    builder.create(&current)?;
                }
                Err(error) => return Err(error.into()),
            }
            let metadata = fs::symlink_metadata(&current)?;
            let is_private_service_directory = is_private_service_path(path, &current);
            if is_private_service_directory
                && (metadata.uid() != 0 || metadata.permissions().mode() & 0o077 != 0)
            {
                return Err(LifecycleError::InsecureProtection(format!(
                    "{} must be root-owned and mode 0700",
                    current.display()
                )));
            }
            if !is_private_service_directory
                && (metadata.uid() != 0
                    || (metadata.permissions().mode() & 0o022 != 0
                        && metadata.permissions().mode() & 0o1000 == 0))
            {
                return Err(LifecycleError::InsecureProtection(format!(
                    "{} is writable by an untrusted account",
                    current.display()
                )));
            }
        }
        Ok(())
    }
    #[cfg(windows)]
    {
        windows_state::create_and_validate_directory(path)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = path;
        Err(LifecycleError::InsecureProtection(
            "protected service-state directories are unsupported on this platform".to_owned(),
        ))
    }
}

fn check_existing_state_path(paths: &ServicePaths, isolated: bool) -> Result<(), LifecycleError> {
    match fs::symlink_metadata(&paths.directory) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
        Ok(metadata) if !metadata.is_dir() || metadata.file_type().is_symlink() => {
            return Err(LifecycleError::UnsafeOwnership(
                "service-state directory is redirected or is not a directory".to_owned(),
            ));
        }
        Ok(_) => {}
    }
    if !isolated {
        validate_existing_service_directory(&paths.directory)?;
    }
    for file in [&paths.lock, &paths.attempt] {
        reject_redirected_path(file)?;
    }
    Ok(())
}

#[cfg(unix)]
fn validate_existing_service_directory(path: &Path) -> Result<(), LifecycleError> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    for ancestor in path.ancestors() {
        let metadata = fs::symlink_metadata(ancestor)?;
        if metadata.file_type().is_symlink() {
            return Err(LifecycleError::UnsafeOwnership(format!(
                "{} is a redirected service-state path",
                ancestor.display()
            )));
        }
        if is_private_service_path(path, ancestor) {
            if metadata.uid() != 0 || metadata.permissions().mode() & 0o077 != 0 {
                return Err(LifecycleError::InsecureProtection(format!(
                    "{} must be root-owned and mode 0700",
                    ancestor.display()
                )));
            }
        } else if metadata.uid() != 0
            || (metadata.permissions().mode() & 0o022 != 0
                && metadata.permissions().mode() & 0o1000 == 0)
        {
            return Err(LifecycleError::InsecureProtection(format!(
                "{} is writable by an untrusted account",
                ancestor.display()
            )));
        }
    }
    Ok(())
}

#[cfg(unix)]
fn is_private_service_path(service_path: &Path, candidate: &Path) -> bool {
    let Some(manager_directory) = service_path.parent() else {
        return false;
    };
    let Some(services_root) = manager_directory.parent() else {
        return false;
    };
    candidate == service_path || candidate == manager_directory || candidate == services_root
}

#[cfg(windows)]
fn validate_existing_service_directory(path: &Path) -> Result<(), LifecycleError> {
    windows_state::validate_directory(path)
}

#[cfg(not(any(unix, windows)))]
fn validate_existing_service_directory(_path: &Path) -> Result<(), LifecycleError> {
    Err(LifecycleError::InsecureProtection(
        "service-state protection is unsupported on this platform".to_owned(),
    ))
}

fn reject_redirected_path(path: &Path) -> Result<(), LifecycleError> {
    for ancestor in path.ancestors() {
        match fs::symlink_metadata(ancestor) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(LifecycleError::UnsafeOwnership(format!(
                    "{} is a symlink or redirected path",
                    ancestor.display()
                )));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn read_attempt_file(
    path: &Path,
    isolated: bool,
) -> Result<Option<ServiceAttemptJournal>, LifecycleError> {
    Ok(read_attempt_snapshot(path, isolated)?.map(|snapshot| snapshot.latest))
}

struct AttemptFileSnapshot {
    latest: ServiceAttemptJournal,
    valid_length: u64,
    has_torn_tail: bool,
    needs_separator: bool,
}

fn read_attempt_snapshot(
    path: &Path,
    isolated: bool,
) -> Result<Option<AttemptFileSnapshot>, LifecycleError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(LifecycleError::UnsafeOwnership(
            "service attempt journal is redirected or not a regular file".to_owned(),
        ));
    }
    #[cfg(windows)]
    if !isolated {
        windows_state::validate_file(path)?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if !isolated && (metadata.uid() != 0 || metadata.permissions().mode() & 0o077 != 0) {
            return Err(LifecycleError::InsecureProtection(
                "service attempt journal is not root-owned and owner-only".to_owned(),
            ));
        }
    }
    if metadata.len() > MAX_ATTEMPT_BYTES as u64 {
        return Err(LifecycleError::MessageTooLarge {
            limit: MAX_ATTEMPT_BYTES,
        });
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)?
        .take((MAX_ATTEMPT_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_ATTEMPT_BYTES {
        return Err(LifecycleError::MessageTooLarge {
            limit: MAX_ATTEMPT_BYTES,
        });
    }

    // Accept the previous single-object representation for local upgrade
    // compatibility. New writes are append-only snapshots so a torn final
    // record cannot destroy the last durable authorization state.
    if let Ok(journal) = serde_json::from_slice::<ServiceAttemptJournal>(&bytes) {
        validate_attempt_snapshot(&journal, None)?;
        let has_separator = bytes.last() == Some(&b'\n');
        return Ok(Some(AttemptFileSnapshot {
            latest: journal,
            valid_length: bytes.len() as u64,
            has_torn_tail: false,
            needs_separator: !has_separator,
        }));
    }

    let complete_length = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |index| index + 1);
    let mut latest: Option<ServiceAttemptJournal> = None;
    for line in bytes[..complete_length].split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        let journal: ServiceAttemptJournal = serde_json::from_slice(line)?;
        validate_attempt_snapshot(&journal, latest.as_ref())?;
        latest = Some(journal);
    }

    let tail = &bytes[complete_length..];
    let mut valid_length = complete_length;
    let mut has_torn_tail = !tail.is_empty();
    let mut needs_separator = false;
    if !tail.is_empty()
        && let Ok(journal) = serde_json::from_slice::<ServiceAttemptJournal>(tail)
    {
        validate_attempt_snapshot(&journal, latest.as_ref())?;
        latest = Some(journal);
        valid_length = bytes.len();
        has_torn_tail = false;
        needs_separator = true;
    }

    let Some(latest) = latest else {
        return match serde_json::from_slice::<ServiceAttemptJournal>(&bytes) {
            Ok(_) => Err(LifecycleError::UnsafeOwnership(
                "service attempt journal has no complete snapshot".to_owned(),
            )),
            Err(error) => Err(error.into()),
        };
    };
    validate_attempt_snapshot(&latest, None)?;
    Ok(Some(AttemptFileSnapshot {
        latest,
        valid_length: valid_length as u64,
        has_torn_tail,
        needs_separator,
    }))
}

fn validate_attempt_snapshot(
    journal: &ServiceAttemptJournal,
    previous: Option<&ServiceAttemptJournal>,
) -> Result<(), LifecycleError> {
    if journal.schema_version != SERVICE_STATE_SCHEMA_VERSION {
        return Err(LifecycleError::UnsupportedSchema(journal.schema_version));
    }
    validate_service_id(journal.manager, &journal.native_service_id)?;
    journal.expected_definition.validate()?;
    if journal.expected_definition.manager != journal.manager
        || canonical_service_id(journal.manager, &journal.expected_definition.service_id)
            != canonical_service_id(journal.manager, &journal.native_service_id)
        || journal.step.trim().is_empty()
    {
        return Err(LifecycleError::UnsafeOwnership(
            "service attempt identity, expected definition, or mutation step is invalid".to_owned(),
        ));
    }
    if previous.is_some_and(|previous| {
        previous.attempt_id != journal.attempt_id
            || previous.manager != journal.manager
            || previous.native_service_id != journal.native_service_id
            || previous.action != journal.action
            || (previous.step != journal.step && !helper::valid_next_step(previous, journal))
            || previous.expected_definition != journal.expected_definition
            || (!valid_attempt_transition(previous.status, journal.status)
                && !helper::valid_next_step(previous, journal))
    }) {
        return Err(LifecycleError::UnsafeOwnership(
            "service attempt journal changes identity or expected definition".to_owned(),
        ));
    }
    Ok(())
}

fn valid_attempt_transition(previous: AttemptStatus, next: AttemptStatus) -> bool {
    previous == next
        || matches!(
            (previous, next),
            (AttemptStatus::Prepared, AttemptStatus::Authorized)
                | (AttemptStatus::Authorized, AttemptStatus::ObservedComplete)
                | (AttemptStatus::Authorized, AttemptStatus::OutcomeUnknown)
                | (AttemptStatus::ObservedComplete, AttemptStatus::Reconciled)
                | (
                    AttemptStatus::ObservedComplete,
                    AttemptStatus::OutcomeUnknown
                )
                | (
                    AttemptStatus::OutcomeUnknown,
                    AttemptStatus::ObservedComplete
                )
        )
}

fn write_attempt_file(
    path: &Path,
    journal: &ServiceAttemptJournal,
    isolated: bool,
) -> Result<(), LifecycleError> {
    let bytes = serde_json::to_vec(journal)?;
    if bytes.len() + 1 > MAX_ATTEMPT_BYTES {
        return Err(LifecycleError::MessageTooLarge {
            limit: MAX_ATTEMPT_BYTES,
        });
    }
    if let Some(parent) = path.parent() {
        create_service_state_directory(parent, isolated)?;
    }
    reject_redirected_path(path)?;

    if let Some(snapshot) = read_attempt_snapshot(path, isolated)? {
        validate_attempt_snapshot(journal, Some(&snapshot.latest))?;
        if snapshot.has_torn_tail {
            // A transition is followed by manager work only after the complete
            // record was written and synced. A torn tail is therefore never an
            // authorization record and can be trimmed back to the last record.
            let repair = OpenOptions::new().write(true).open(path)?;
            repair.set_len(snapshot.valid_length)?;
            repair.sync_all()?;
        }
        let mut file = OpenOptions::new().read(true).append(true).open(path)?;
        let current_length = file.metadata()?.len();
        let separator_length = u64::from(snapshot.needs_separator);
        let record_length = bytes.len() as u64 + 1;
        if current_length
            .saturating_add(separator_length)
            .saturating_add(record_length)
            > MAX_ATTEMPT_BYTES as u64
        {
            return Err(LifecycleError::MessageTooLarge {
                limit: MAX_ATTEMPT_BYTES,
            });
        }
        if snapshot.needs_separator {
            file.write_all(b"\n")?;
        }
        file.write_all(&bytes)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        return Ok(());
    }

    let temporary = path.with_extension(format!("{}.tmp", Uuid::new_v4()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary)?;
    #[cfg(windows)]
    if !isolated
        && let Err(error) = windows_state::protect_file(&temporary)
            .and_then(|()| windows_state::validate_file(&temporary))
    {
        drop(file);
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }
    let result = (|| -> Result<(), LifecycleError> {
        file.write_all(&bytes)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        drop(file);
        match fs::symlink_metadata(path) {
            Ok(_) => {
                return Err(LifecycleError::ConflictingServiceDefinition(
                    "service attempt journal appeared during publication".to_owned(),
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        fs::hard_link(&temporary, path)?;
        fs::remove_file(&temporary)?;
        #[cfg(unix)]
        File::open(path.parent().unwrap_or_else(|| Path::new(".")))?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn ensure_remaining(
    deadline: Deadline,
    clock: &dyn MonotonicClock,
    phase: &'static str,
) -> Result<Duration, LifecycleError> {
    let remaining = deadline.remaining(clock);
    if remaining.is_zero() {
        Err(LifecycleError::DeadlineExpired { phase })
    } else {
        Ok(remaining)
    }
}

fn require_definition(
    observation: &NativeServiceObservation,
) -> Result<ServiceDefinition, LifecycleError> {
    if observation.installed == InstallationObservation::NotInstalled {
        return Err(LifecycleError::ServiceNotInstalled);
    }
    observation.definition.clone().ok_or_else(|| {
        LifecycleError::UnsafeOwnership(
            "installed service definition is missing or unverified".to_owned(),
        )
    })
}

fn fallback_definition(manager: ServiceManagerKind, service_id: &str) -> ServiceDefinition {
    ServiceDefinition {
        manager,
        service_id: service_id.to_owned(),
        executable: PathBuf::new(),
        arguments: Vec::new(),
        account: manager.default_account().to_owned(),
        boot_policy: BootPolicy::Automatic,
        data_dir: PathBuf::new(),
        definition_fingerprint: None,
    }
}

fn core_is_ready(core: Option<&CoreObservation>) -> bool {
    core.is_some_and(|core| {
        core.mode == CoreMode::SystemService
            && core.state == ObservedState::Ready
            && core.identity_verified
    })
}

fn ensure_service_mode(observation: &NativeServiceObservation) -> Result<(), LifecycleError> {
    if let Some(core) = &observation.core
        && core.mode != CoreMode::SystemService
    {
        return Err(LifecycleError::UnsafeOwnership(format!(
            "service data directory is owned by a {} Core instance",
            core.mode
        )));
    }
    Ok(())
}

fn core_is_absent_or_stopped(core: Option<&CoreObservation>, unknown: Option<&str>) -> bool {
    unknown.is_none() && core.is_none_or(|core| core.state == ObservedState::Stopped)
}

fn ensure_expected_definition(
    expected: &ServiceDefinition,
    observation: &NativeServiceObservation,
) -> Result<(), LifecycleError> {
    if observation.installed != InstallationObservation::Installed
        || observation
            .definition
            .as_ref()
            .is_none_or(|actual| !expected.equivalent_to(actual))
    {
        return Err(LifecycleError::ConflictingServiceDefinition(
            "service definition disappeared or changed while observing the requested operation; retained attempt requires reconciliation".into(),
        ));
    }
    Ok(())
}

/// Decides whether a native attempt's effects have demonstrably settled.
///
/// A matching registration alone never proves that manager work finished. The
/// barrier may only be released when the manager itself reports a conclusive
/// terminal state with no pending job, no Core instance owns the data
/// directory, and the observed registration is still the owned one.
fn native_effects_are_settled(
    journal: &ServiceAttemptJournal,
    observation: &NativeServiceObservation,
) -> bool {
    if observation.pending_operation {
        // The manager still reports accepted work. Keep the barrier.
        return false;
    }
    if !matches!(
        observation.manager_state,
        NativeManagerState::Stopped | NativeManagerState::Failed
    ) {
        // Starting, stopping, running or unknown states are not conclusive.
        return false;
    }
    // The Core must not still own the data directory.
    if !core_is_absent_or_stopped(
        observation.core.as_ref(),
        observation.core_unknown.as_deref(),
    ) {
        return false;
    }
    match observation.installed {
        InstallationObservation::Unknown => false,
        // Nothing is registered, so no registration can be replaced or removed.
        InstallationObservation::NotInstalled => true,
        InstallationObservation::Installed => {
            // A replacement registration must never be reconciled as the
            // original attempt.
            ensure_expected_definition(&journal.expected_definition, observation).is_ok()
        }
    }
}

/// Decides whether the journal proves the native command itself finished.
///
/// An authorized attempt whose helper exited and was reaped proves the native
/// command ran to completion, even when the parent never recorded a later
/// observation. An unknown attempt that recorded the manager observation that
/// settled it proves the same. An interrupted helper leaves neither, so its
/// effect stays unknown and keeps the barrier.
fn native_attempt_outcome_is_known(journal: &ServiceAttemptJournal) -> bool {
    match journal.status {
        AttemptStatus::Authorized => journal.helper_pid.is_none(),
        AttemptStatus::OutcomeUnknown => journal.completion_observation.is_some(),
        _ => false,
    }
}

fn operation_satisfied(
    action: ServiceAction,
    definition: &ServiceDefinition,
    observation: &NativeServiceObservation,
) -> bool {
    if observation.pending_operation
        || observation.core_unknown.is_some()
        || (action != ServiceAction::Remove
            && ensure_expected_definition(definition, observation).is_err())
    {
        return false;
    }
    match action {
        ServiceAction::Install => {
            observation.installed == InstallationObservation::Installed
                && observation
                    .definition
                    .as_ref()
                    .is_some_and(|actual| definition.equivalent_to(actual))
                && observation.manager_state == NativeManagerState::Stopped
                && observation.boot_enabled
                && core_is_absent_or_stopped(observation.core.as_ref(), None)
        }
        ServiceAction::Start => {
            observation.manager_state == NativeManagerState::Running
                && core_is_ready(observation.core.as_ref())
        }
        ServiceAction::Stop => {
            observation.manager_state == NativeManagerState::Stopped
                && core_is_absent_or_stopped(
                    observation.core.as_ref(),
                    observation.core_unknown.as_deref(),
                )
        }
        ServiceAction::Remove => {
            observation.installed == InstallationObservation::NotInstalled
                && core_is_absent_or_stopped(observation.core.as_ref(), None)
        }
    }
}

pub fn render_service_definition(definition: &ServiceDefinition) -> Result<String, LifecycleError> {
    definition.validate()?;
    match definition.manager {
        ServiceManagerKind::WindowsScm => Ok(render_windows_command_line(definition)),
        ServiceManagerKind::LinuxSystemd => Ok(render_systemd_unit(definition)),
        ServiceManagerKind::MacosLaunchd => Ok(render_launchd_plist(definition)),
    }
}

fn render_windows_command_line(definition: &ServiceDefinition) -> String {
    std::iter::once(
        definition
            .executable
            .as_os_str()
            .to_string_lossy()
            .into_owned(),
    )
    .chain(definition.arguments.iter().cloned())
    .map(|argument| quote_windows_argument(&argument))
    .collect::<Vec<_>>()
    .join(" ")
}

fn quote_windows_argument(argument: &str) -> String {
    if !argument.is_empty() && !argument.chars().any(char::is_whitespace) && !argument.contains('"')
    {
        return argument.to_owned();
    }
    let mut quoted = String::from("\"");
    let mut backslashes = 0;
    for character in argument.chars() {
        match character {
            '\\' => backslashes += 1,
            '"' => {
                quoted.push_str(&"\\".repeat(backslashes * 2 + 1));
                quoted.push('"');
                backslashes = 0;
            }
            _ => {
                quoted.push_str(&"\\".repeat(backslashes));
                quoted.push(character);
                backslashes = 0;
            }
        }
    }
    quoted.push_str(&"\\".repeat(backslashes * 2));
    quoted.push('"');
    quoted
}

fn systemd_escape_argument(argument: &str) -> String {
    let mut escaped = String::new();
    for character in argument.chars() {
        if !character.is_ascii() {
            escaped.push(character);
            continue;
        }
        match character as u8 {
            b' ' => escaped.push_str("\\x20"),
            b'\t' => escaped.push_str("\\x09"),
            b'\\' => escaped.push_str("\\\\"),
            b'"' => escaped.push_str("\\x22"),
            b'$' => escaped.push_str("$$"),
            b'%' => escaped.push_str("%%"),
            byte if byte.is_ascii_control() => escaped.push_str(&format!("\\x{byte:02x}")),
            byte => escaped.push(byte as char),
        }
    }
    escaped
}

fn render_systemd_unit(definition: &ServiceDefinition) -> String {
    let command = std::iter::once(
        definition
            .executable
            .as_os_str()
            .to_string_lossy()
            .into_owned(),
    )
    .chain(definition.arguments.iter().cloned())
    .map(|argument| systemd_escape_argument(&argument))
    .collect::<Vec<_>>()
    .join(" ");
    format!(
        "[Unit]\nDescription=Kakune Core local automation runtime\nAfter=network-online.target\nWants=network-online.target\n\n[Service]\nType=notify\nNotifyAccess=main\nUser={}\nExecStart={}\nRestart=on-failure\nRestartSec=5\nKillMode=mixed\nSendSIGKILL=no\nTimeoutStartSec=30\nTimeoutStopSec=30\nNoNewPrivileges=true\nPrivateTmp=true\nProtectSystem=strict\nReadWritePaths={}\n\n[Install]\nWantedBy=multi-user.target\n",
        definition.account,
        command,
        systemd_escape_argument(&definition.data_dir.display().to_string()),
    )
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn render_launchd_plist(definition: &ServiceDefinition) -> String {
    let arguments = std::iter::once(
        definition
            .executable
            .as_os_str()
            .to_string_lossy()
            .into_owned(),
    )
    .chain(definition.arguments.iter().cloned())
    .map(|argument| format!("    <string>{}</string>\n", xml_escape(&argument)))
    .collect::<String>();
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict>\n  <key>Label</key><string>{}</string>\n  <key>UserName</key><string>{}</string>\n  <key>ProgramArguments</key><array>\n{}  </array>\n  <key>RunAtLoad</key><true/>\n  <key>KeepAlive</key><true/>\n  <key>ExitTimeOut</key><integer>0</integer>\n  <key>StandardOutPath</key><string>/var/log/kakune-core.log</string>\n  <key>StandardErrorPath</key><string>/var/log/kakune-core.log</string>\n</dict></plist>\n",
        xml_escape(&definition.service_id),
        xml_escape(&definition.account),
        arguments,
    )
}

fn scm_reports_failed_exit(output: &str) -> bool {
    let parse_code = |label: &str| {
        output.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if !name.trim().eq_ignore_ascii_case(label) {
                return None;
            }
            let code = value.split_whitespace().next()?;
            code.strip_prefix("0x")
                .and_then(|hex| u32::from_str_radix(hex, 16).ok())
                .or_else(|| code.parse().ok())
        })
    };

    match parse_code("WIN32_EXIT_CODE") {
        Some(0) | None => false,
        Some(1066) => parse_code("SERVICE_EXIT_CODE").is_some_and(|code| code != 0),
        Some(_) => true,
    }
}

pub fn parse_manager_state(
    manager: ServiceManagerKind,
    output: &str,
) -> Result<NativeManagerState, LifecycleError> {
    let lower = output.to_ascii_lowercase();
    if lower.contains("access is denied")
        || lower.contains("permission denied")
        || lower.contains("operation not permitted")
        || lower.contains("not authorized")
    {
        return Err(LifecycleError::AccessDenied(output.trim().to_owned()));
    }
    if lower.contains("failed to connect to bus") || lower.contains("system has not been booted") {
        return Err(LifecycleError::ManagerUnavailable(output.trim().to_owned()));
    }
    match manager {
        ServiceManagerKind::WindowsScm => {
            let Some(state) = output.lines().find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.trim()
                    .eq_ignore_ascii_case("state")
                    .then(|| value.split_whitespace().nth(1))
                    .flatten()
            }) else {
                if lower.contains("1060") || lower.contains("does not exist") {
                    return Ok(NativeManagerState::Stopped);
                }
                return Ok(NativeManagerState::Unknown);
            };
            Ok(match state.to_ascii_uppercase().as_str() {
                "STOPPED" if scm_reports_failed_exit(output) => NativeManagerState::Failed,
                "STOPPED" => NativeManagerState::Stopped,
                "START_PENDING" => NativeManagerState::Starting,
                "RUNNING" => NativeManagerState::Running,
                "STOP_PENDING" => NativeManagerState::Stopping,
                "FAILED" => NativeManagerState::Failed,
                _ => NativeManagerState::Unknown,
            })
        }
        ServiceManagerKind::LinuxSystemd => {
            let properties = output
                .lines()
                .filter_map(|line| line.split_once('='))
                .collect::<std::collections::BTreeMap<_, _>>();
            let load = properties.get("LoadState").copied().unwrap_or_default();
            let active = properties.get("ActiveState").copied().unwrap_or_default();
            let sub = properties.get("SubState").copied().unwrap_or_default();
            Ok(match (load, active, sub) {
                ("not-found", _, _) | (_, "inactive", "dead") => NativeManagerState::Stopped,
                (_, "activating", _) => NativeManagerState::Starting,
                (_, "deactivating", _) => NativeManagerState::Stopping,
                (_, "failed", _) => NativeManagerState::Failed,
                (_, "active", "running") => NativeManagerState::Running,
                _ => NativeManagerState::Unknown,
            })
        }
        ServiceManagerKind::MacosLaunchd => {
            if lower.contains("could not find service") || lower.contains("service not found") {
                return Ok(NativeManagerState::Stopped);
            }
            if lower.contains("state = running") {
                return Ok(NativeManagerState::Running);
            }
            if lower.contains("state = waiting") {
                return Ok(NativeManagerState::Stopped);
            }
            if lower.contains("state = spawn") || lower.contains("state = throttled") {
                return Ok(NativeManagerState::Starting);
            }
            if lower.contains("state = stopping") {
                return Ok(NativeManagerState::Stopping);
            }
            if lower.contains("state = exited") {
                return Ok(NativeManagerState::Failed);
            }
            Ok(NativeManagerState::Unknown)
        }
    }
}

/// Parses `systemctl is-enabled` output without treating access or manager
/// errors as an ordinary disabled service.
pub fn parse_systemd_enabled(
    output: &str,
    command_succeeded: bool,
) -> Result<bool, LifecycleError> {
    let state = output.trim().to_ascii_lowercase();
    if state.contains("access is denied")
        || state.contains("permission denied")
        || state.contains("operation not permitted")
        || state.contains("not authorized")
    {
        return Err(LifecycleError::AccessDenied(output.trim().to_owned()));
    }
    if state.contains("failed to connect to bus") || state.contains("system has not been booted") {
        return Err(LifecycleError::ManagerUnavailable(output.trim().to_owned()));
    }
    if command_succeeded && state == "enabled" {
        return Ok(true);
    }
    if matches!(
        state.as_str(),
        "disabled"
            | "static"
            | "indirect"
            | "generated"
            | "transient"
            | "masked"
            | "masked-runtime"
            | "enabled-runtime"
            | "linked"
            | "linked-runtime"
            | "alias"
            | "not-found"
    ) {
        return Ok(false);
    }
    Err(LifecycleError::ManagerUnavailable(if state.is_empty() {
        "systemctl is-enabled returned no state".to_owned()
    } else {
        format!("systemctl is-enabled returned an unrecognized state: {state}")
    }))
}

pub(super) fn observe_service_core(
    data_dir: &Path,
    deadline: Deadline,
    clock: &dyn MonotonicClock,
) -> Result<(Option<CoreObservation>, Option<String>), LifecycleError> {
    let observation = super::daemon::observe_with(
        data_dir,
        deadline,
        clock,
        &super::process::SystemProcessEvidence::default(),
    )?;
    let core = observation.core_observation().or_else(|| {
        (observation.state == ObservedState::Stopped).then(|| CoreObservation {
            mode: CoreMode::SystemService,
            state: ObservedState::Stopped,
            instance_id: None,
            owner: None,
            process: None,
            data_dir: data_dir.to_path_buf(),
            listen_address: None,
            diagnostic_location: None,
            identity_verified: false,
        })
    });
    if observation.mode == Some(CoreMode::SystemService)
        && observation.state == ObservedState::Ready
    {
        let instance = observation
            .instance_id()
            .ok_or(LifecycleError::UnverifiedProcess)?;
        let result = native::run_owned_command(
            std::process::Command::new(std::env::current_exe()?)
                .arg("--internal-service-probe")
                .arg(data_dir)
                .arg(instance.to_string()),
            deadline,
            clock,
        )?;
        if !result.status_success {
            return Ok((None, Some("system-service instance readiness proof failed; inspect installed-account access and protected capability diagnostics".into())));
        }
    }
    let unknown = if observation.state == ObservedState::Unknown {
        Some(
            observation
                .detail
                .clone()
                .unwrap_or_else(|| "Core ownership could not be verified".to_owned()),
        )
    } else {
        None
    };
    Ok((core, unknown))
}

/// Returns the native manager implementation for the current supported OS.
pub fn system_manager() -> Result<Arc<dyn ServiceManagerAdapter>, LifecycleError> {
    #[cfg(windows)]
    {
        Ok(Arc::new(windows::WindowsServiceManager))
    }
    #[cfg(target_os = "linux")]
    {
        Ok(Arc::new(linux::LinuxServiceManager))
    }
    #[cfg(target_os = "macos")]
    {
        Ok(Arc::new(macos::MacosServiceManager))
    }
    #[cfg(not(any(windows, target_os = "linux", target_os = "macos")))]
    Err(LifecycleError::ManagerUnavailable(
        "native system services are unsupported on this operating system".to_owned(),
    ))
}

#[cfg(windows)]
mod windows_state {
    use std::{collections::HashSet, fs, os::windows::fs::MetadataExt, path::Path};

    use windows_permissions::{
        LocalBox, SecurityDescriptor, Sid,
        constants::{AccessRights, AceFlags, AceType, SeObjectType, SecurityInformation},
        wrappers::{
            ConvertSecurityDescriptorToStringSecurityDescriptor as to_sddl,
            GetNamedSecurityInfo as get_security_info, SetNamedSecurityInfo as set_security_info,
        },
    };

    use super::LifecycleError;

    const SYSTEM_SID: &str = "S-1-5-18";
    const ADMIN_SID: &str = "S-1-5-32-544";
    const TRUSTED_INSTALLER_SID: &str =
        "S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464";
    const REPARSE_POINT: u32 = 0x0400;
    // ProgramData permits creating siblings and writing directory attributes.
    // These rights do not grant deletion of protected children or DACL/owner
    // changes. Every existing child still needs trusted ownership, no reparse
    // point, and (for private service state) the exact protected machine DACL.
    // Keep DELETE_CHILD, DELETE, WRITE_DAC, WRITE_OWNER and generic write/all out.
    const SAFE_ANCESTOR_ACCESS_MASK: u32 =
        0x8000_0000 | 0x2000_0000 | 0x0012_00A9 | 0x0000_0006 | 0x0000_0110;

    pub(super) fn create_and_validate_directory(path: &Path) -> Result<(), LifecycleError> {
        if !path.is_absolute() {
            return Err(insecure("service-state path must be absolute"));
        }
        let mut ancestors = path.ancestors().map(Path::to_path_buf).collect::<Vec<_>>();
        ancestors.reverse();
        let private_directories = private_service_directories(path);
        for ancestor in ancestors {
            match fs::symlink_metadata(&ancestor) {
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    fs::create_dir(&ancestor)?;
                    set_machine_directory_acl(&ancestor)?;
                }
                Err(error) => return Err(error.into()),
            }
            if private_directories.contains(&ancestor) {
                validate_exact_directory(&ancestor)?;
            } else {
                validate_ancestor_directory(&ancestor)?;
            }
        }
        Ok(())
    }

    pub(super) fn validate_directory(path: &Path) -> Result<(), LifecycleError> {
        let mut ancestors = path.ancestors().map(Path::to_path_buf).collect::<Vec<_>>();
        ancestors.reverse();
        let private_directories = private_service_directories(path);
        for ancestor in ancestors {
            if private_directories.contains(&ancestor) {
                validate_exact_directory(&ancestor)?;
            } else {
                validate_ancestor_directory(&ancestor)?;
            }
        }
        Ok(())
    }

    fn private_service_directories(path: &Path) -> Vec<std::path::PathBuf> {
        path.ancestors().take(3).map(Path::to_path_buf).collect()
    }

    pub(super) fn validate_file(path: &Path) -> Result<(), LifecycleError> {
        let metadata = fs::symlink_metadata(path)?;
        if !metadata.is_file()
            || metadata.file_type().is_symlink()
            || metadata.file_attributes() & REPARSE_POINT != 0
        {
            return Err(insecure("service-state file is redirected or not regular"));
        }
        let descriptor = read_descriptor(path)?;
        validate_exact_descriptor(&descriptor, false)
    }

    pub(super) fn protect_file(path: &Path) -> Result<(), LifecycleError> {
        let sids: Vec<LocalBox<Sid>> = vec![SYSTEM_SID, ADMIN_SID]
            .into_iter()
            .map(|sid| {
                sid.parse()
                    .map_err(|_| insecure("invalid built-in Windows SID"))
            })
            .collect::<Result<_, _>>()?;
        let acl = sids
            .iter()
            .map(|sid| format!("(A;;FA;;;{sid})"))
            .collect::<String>();
        let descriptor: LocalBox<SecurityDescriptor> = format!("O:{ADMIN_SID}D:P{acl}")
            .parse()
            .map_err(|_| insecure("could not construct service-state file ACL"))?;
        let owner = descriptor
            .owner()
            .ok_or_else(|| insecure("service-state file owner is missing"))?;
        let dacl = descriptor
            .dacl()
            .ok_or_else(|| insecure("service-state file ACL is missing"))?;
        set_security_info(
            path,
            SeObjectType::SE_FILE_OBJECT,
            SecurityInformation::Owner
                | SecurityInformation::Dacl
                | SecurityInformation::ProtectedDacl,
            Some(owner),
            None,
            Some(dacl),
            None,
        )
        .map_err(|error| insecure(format!("cannot protect service-state file: {error}")))?;
        validate_file(path)
    }

    fn validate_exact_directory(path: &Path) -> Result<(), LifecycleError> {
        let metadata = fs::symlink_metadata(path)?;
        if !metadata.is_dir()
            || metadata.file_type().is_symlink()
            || metadata.file_attributes() & REPARSE_POINT != 0
        {
            return Err(insecure(
                "service-state directory is redirected or not a directory",
            ));
        }
        let descriptor = read_descriptor(path)?;
        validate_exact_descriptor(&descriptor, true)
    }

    fn validate_ancestor_directory(path: &Path) -> Result<(), LifecycleError> {
        let metadata = fs::symlink_metadata(path)?;
        if !metadata.is_dir()
            || metadata.file_type().is_symlink()
            || metadata.file_attributes() & REPARSE_POINT != 0
        {
            return Err(insecure(format!(
                "{} is redirected or is not a directory",
                path.display()
            )));
        }
        let descriptor = read_descriptor(path)?;
        validate_ancestor_descriptor(&descriptor)
    }

    fn validate_ancestor_descriptor(descriptor: &SecurityDescriptor) -> Result<(), LifecycleError> {
        let owner = descriptor
            .owner()
            .ok_or_else(|| insecure("service-state ancestor owner is unknown"))?;
        if !trusted_ancestor(&owner.to_string()) {
            return Err(insecure("service-state ancestor has an untrusted owner"));
        }
        let dacl = descriptor
            .dacl()
            .ok_or_else(|| insecure("service-state ancestor has no DACL"))?;
        if dacl.len() > 256 {
            return Err(insecure(
                "service-state ancestor DACL exceeds the inspection bound",
            ));
        }
        for index in 0..dacl.len() {
            let ace = dacl
                .get_ace(index)
                .ok_or_else(|| insecure("service-state ancestor DACL is unreadable"))?;
            match ace.ace_type() {
                AceType::ACCESS_ALLOWED_ACE_TYPE => {
                    if ace.flags().contains(AceFlags::InheritOnly) {
                        continue;
                    }
                    let sid = ace
                        .sid()
                        .ok_or_else(|| insecure("service-state ancestor ACE SID is unknown"))?;
                    if !trusted_ancestor(&sid.to_string())
                        && ace.mask().bits() & !SAFE_ANCESTOR_ACCESS_MASK != 0
                    {
                        return Err(insecure(
                            "service-state ancestor grants untrusted write access",
                        ));
                    }
                }
                AceType::ACCESS_DENIED_ACE_TYPE => {}
                _ => return Err(insecure("service-state ancestor uses an unsupported ACE")),
            }
        }
        Ok(())
    }

    fn set_machine_directory_acl(path: &Path) -> Result<(), LifecycleError> {
        let sids: Vec<LocalBox<Sid>> = vec![SYSTEM_SID, ADMIN_SID]
            .into_iter()
            .map(|sid| {
                sid.parse()
                    .map_err(|_| insecure("invalid built-in Windows SID"))
            })
            .collect::<Result<_, _>>()?;
        let acl = sids
            .iter()
            .map(|sid| format!("(A;OICI;FA;;;{sid})"))
            .collect::<String>();
        let descriptor: LocalBox<SecurityDescriptor> = format!("O:{ADMIN_SID}D:P{acl}")
            .parse()
            .map_err(|_| insecure("could not construct service-state ACL"))?;
        let owner = descriptor
            .owner()
            .ok_or_else(|| insecure("service-state owner SID is missing"))?;
        let dacl = descriptor
            .dacl()
            .ok_or_else(|| insecure("service-state ACL is missing"))?;
        set_security_info(
            path,
            SeObjectType::SE_FILE_OBJECT,
            SecurityInformation::Owner
                | SecurityInformation::Dacl
                | SecurityInformation::ProtectedDacl,
            Some(owner),
            None,
            Some(dacl),
            None,
        )
        .map_err(|error| insecure(format!("cannot protect service-state directory: {error}")))?;
        // The caller validates the complete chain against the final service
        // namespace. Applying its last-three-private rule to an intermediate
        // directory would incorrectly require an exact machine DACL on C:\ or
        // ProgramData while constructing the hierarchy for the first time.
        validate_exact_directory(path)
    }

    fn validate_exact_descriptor(
        descriptor: &SecurityDescriptor,
        directory: bool,
    ) -> Result<(), LifecycleError> {
        let owner = descriptor
            .owner()
            .ok_or_else(|| insecure("service-state directory owner is unknown"))?;
        if !trusted_ancestor(&owner.to_string()) {
            return Err(insecure("service-state directory has an untrusted owner"));
        }
        let dacl = descriptor
            .dacl()
            .ok_or_else(|| insecure("service-state directory has no DACL"))?;
        if dacl.len() != 2 {
            return Err(insecure("service-state directory ACL is not restricted"));
        }
        let mut observed_sids = HashSet::new();
        for index in 0..dacl.len() {
            let ace = dacl
                .get_ace(index)
                .ok_or_else(|| insecure("service-state directory ACL is unreadable"))?;
            let sid = ace
                .sid()
                .ok_or_else(|| insecure("service-state directory ACL SID is unknown"))?;
            if ace.ace_type() != AceType::ACCESS_ALLOWED_ACE_TYPE
                || ace.mask() != AccessRights::FileAllAccess
                || (sid.to_string() != SYSTEM_SID && sid.to_string() != ADMIN_SID)
                || !observed_sids.insert(sid.to_string())
                || ace.flags().bits()
                    != if directory {
                        AceFlags::ObjectInherit.bits() | AceFlags::ContainerInherit.bits()
                    } else {
                        AceFlags::empty().bits()
                    }
            {
                return Err(insecure(
                    "service-state directory ACL grants unexpected access",
                ));
            }
        }
        let sddl_value = to_sddl(
            descriptor,
            SecurityInformation::Owner | SecurityInformation::Dacl,
        )
        .map_err(|error| insecure(format!("cannot verify service-state ACL: {error}")))?;
        let sddl = sddl_value.to_string_lossy();
        if !sddl.contains("D:P") {
            return Err(insecure("service-state DACL is not protected"));
        }
        Ok(())
    }

    fn read_descriptor(path: &Path) -> Result<LocalBox<SecurityDescriptor>, LifecycleError> {
        get_security_info(
            path,
            SeObjectType::SE_FILE_OBJECT,
            SecurityInformation::Owner | SecurityInformation::Dacl,
        )
        .map_err(|error| insecure(format!("cannot inspect service-state ACL: {error}")))
    }

    fn trusted_ancestor(sid: &str) -> bool {
        sid == SYSTEM_SID || sid == ADMIN_SID || sid == TRUSTED_INSTALLER_SID
    }

    fn insecure(message: impl Into<String>) -> LifecycleError {
        LifecycleError::InsecureProtection(message.into())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn programdata_ancestor_accepts_users_create_and_attribute_rights() {
            let descriptor: LocalBox<SecurityDescriptor> =
                "O:SYD:(A;;FA;;;SY)(A;;FA;;;BA)(A;CI;0x116;;;BU)"
                    .parse()
                    .unwrap();
            validate_ancestor_descriptor(&descriptor).unwrap();
        }

        #[test]
        fn ancestor_rejects_users_delete_replace_and_security_control_rights() {
            for mask in [0x40, 0x1_0000, 0x4_0000, 0x8_0000, 0x4000_0000, 0x1000_0000] {
                let descriptor: LocalBox<SecurityDescriptor> =
                    format!("O:SYD:(A;;FA;;;SY)(A;;FA;;;BA)(A;;0x{mask:x};;;BU)")
                        .parse()
                        .unwrap();
                assert!(
                    validate_ancestor_descriptor(&descriptor).is_err(),
                    "{mask:x}"
                );
            }
        }

        #[test]
        fn ancestor_attribute_rights_do_not_make_an_untrusted_owner_safe() {
            let descriptor: LocalBox<SecurityDescriptor> =
                "O:BUD:(A;;FA;;;SY)(A;;FA;;;BA)(A;CI;0x116;;;BU)"
                    .parse()
                    .unwrap();
            assert!(validate_ancestor_descriptor(&descriptor).is_err());
        }

        #[test]
        fn private_service_directory_never_accepts_users_attribute_or_creation_rights() {
            let descriptor: LocalBox<SecurityDescriptor> =
                "O:BAD:P(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)(A;CI;0x116;;;BU)"
                    .parse()
                    .unwrap();
            assert!(validate_exact_descriptor(&descriptor, true).is_err());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_service_core_is_stopped_without_inventing_process_identity() {
        let data = std::env::temp_dir().join(format!("kakune-absent-core-{}", Uuid::new_v4()));
        let clock = crate::lifecycle::SystemMonotonicClock::default();
        let (core, unknown) =
            observe_service_core(&data, Deadline::after(&clock, STATUS_BUDGET), &clock).unwrap();
        let core = core.unwrap();
        assert_eq!(core.state, ObservedState::Stopped);
        assert!(core.process.is_none() && core.owner.is_none());
        assert!(!core.identity_verified);
        assert!(unknown.is_none());
        assert!(!data.exists());
    }

    fn test_journal(root: &Path) -> ServiceAttemptJournal {
        let data_dir = root.join("data");
        let definition = ServiceDefinition::new(
            ServiceManagerKind::LinuxSystemd,
            "kakune-core.service",
            &std::env::current_exe().unwrap(),
            vec![
                "service".into(),
                "run".into(),
                "--data-dir".into(),
                data_dir.display().to_string(),
            ],
            "root",
            &data_dir,
        )
        .unwrap();
        ServiceAttemptJournal {
            schema_version: SERVICE_STATE_SCHEMA_VERSION,
            attempt_id: Uuid::new_v4(),
            manager: definition.manager,
            native_service_id: definition.service_id.clone(),
            action: ServiceAction::Install,
            step: ServiceAction::Install.label().to_owned(),
            expected_definition: definition,
            prior_state: NativeServiceObservation::absent(),
            helper_pid: None,
            helper_owner: None,
            status: AttemptStatus::Prepared,
            completion_observation: None,
            residual_resources: Vec::new(),
            recovery_guidance: vec!["Inspect the isolated test manager.".to_owned()],
        }
    }

    fn isolated_attempt_path() -> (PathBuf, PathBuf) {
        let root = std::env::temp_dir().join(format!("kakune-attempt-{}", Uuid::new_v4()));
        let path = root
            .join("systemd")
            .join("kakune-core.service")
            .join(ATTEMPT_FILE);
        (root, path)
    }

    #[test]
    fn identifiers_are_canonicalized_using_manager_semantics() {
        assert_eq!(
            canonical_service_id(ServiceManagerKind::WindowsScm, "KakuneCore"),
            "kakunecore"
        );
        assert_eq!(
            canonical_service_id(ServiceManagerKind::LinuxSystemd, "kakune-core.service"),
            "kakune-core.service"
        );
    }

    #[test]
    fn windows_argument_quoting_handles_trailing_backslashes_and_quotes() {
        assert_eq!(quote_windows_argument("plain"), "plain");
        assert_eq!(quote_windows_argument("two words"), "\"two words\"");
        assert_eq!(quote_windows_argument("ends\\"), "ends\\");
        assert_eq!(
            quote_windows_argument("ends with space\\"),
            "\"ends with space\\\\\""
        );
        assert_eq!(quote_windows_argument("a\"b"), "\"a\\\"b\"");
    }

    #[test]
    fn service_definition_fingerprint_changes_for_account_and_data_path() {
        let executable = std::env::current_exe().unwrap();
        let data_dir = std::env::temp_dir().join(format!("kakune-service-data-{}", Uuid::new_v4()));
        let first = ServiceDefinition::new(
            ServiceManagerKind::LinuxSystemd,
            "kakune-core.service",
            &executable,
            vec![
                "service".into(),
                "run".into(),
                "--data-dir".into(),
                data_dir.display().to_string(),
            ],
            "root",
            &data_dir,
        )
        .unwrap();
        let changed_account = ServiceDefinition::new(
            ServiceManagerKind::LinuxSystemd,
            "kakune-core.service",
            &executable,
            vec![
                "service".into(),
                "run".into(),
                "--data-dir".into(),
                data_dir.display().to_string(),
            ],
            "kakune",
            &data_dir,
        )
        .unwrap();
        assert_ne!(
            first.definition_fingerprint,
            changed_account.definition_fingerprint
        );
        let changed_data_dir = data_dir.with_extension("alternate");
        let changed_data = ServiceDefinition::new(
            ServiceManagerKind::LinuxSystemd,
            "kakune-core.service",
            &executable,
            vec![
                "service".into(),
                "run".into(),
                "--data-dir".into(),
                changed_data_dir.display().to_string(),
            ],
            "root",
            &changed_data_dir,
        )
        .unwrap();
        assert_ne!(
            first.definition_fingerprint,
            changed_data.definition_fingerprint
        );
        let mut tampered = first;
        tampered.definition_fingerprint = Some("0".repeat(64));
        assert!(tampered.validate().is_err());
    }

    #[test]
    fn attempt_journal_appends_snapshots_and_reads_the_latest_state() {
        let (root, path) = isolated_attempt_path();
        let mut journal = test_journal(&root);
        write_attempt_file(&path, &journal, true).unwrap();
        journal.status = AttemptStatus::Authorized;
        write_attempt_file(&path, &journal, true).unwrap();

        let bytes = fs::read(&path).unwrap();
        assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 2);
        assert_eq!(
            read_attempt_file(&path, true).unwrap().unwrap().status,
            AttemptStatus::Authorized
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn attempt_journal_trims_only_a_torn_tail_before_the_next_snapshot() {
        let (root, path) = isolated_attempt_path();
        let mut journal = test_journal(&root);
        write_attempt_file(&path, &journal, true).unwrap();
        let mut append = OpenOptions::new().append(true).open(&path).unwrap();
        append.write_all(b"{\"schema_version\":").unwrap();
        append.sync_all().unwrap();

        assert_eq!(
            read_attempt_file(&path, true).unwrap().unwrap().status,
            AttemptStatus::Prepared,
            "a torn transition must not replace the last complete state"
        );
        journal.status = AttemptStatus::Authorized;
        write_attempt_file(&path, &journal, true).unwrap();
        let bytes = fs::read(&path).unwrap();
        assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 2);
        assert_eq!(
            read_attempt_file(&path, true).unwrap().unwrap().status,
            AttemptStatus::Authorized
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn attempt_journal_rejects_a_snapshot_that_changes_attempt_identity() {
        let (root, path) = isolated_attempt_path();
        let journal = test_journal(&root);
        write_attempt_file(&path, &journal, true).unwrap();
        let mut replacement = journal.clone();
        replacement.attempt_id = Uuid::new_v4();
        replacement.status = AttemptStatus::Authorized;

        assert!(matches!(
            write_attempt_file(&path, &replacement, true),
            Err(LifecycleError::UnsafeOwnership(_))
        ));
        fs::remove_dir_all(root).unwrap();
    }
}
