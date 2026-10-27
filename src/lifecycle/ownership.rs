//! Versioned instance identity, stable runtime leases, and safe metadata IO.

use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    ACQUISITION_BUDGET, CoreMode, Deadline, DeadlineWaiter, LifecycleError, MonotonicClock,
};

pub const OWNERSHIP_SCHEMA_VERSION: u32 = 1;
const MAX_OWNERSHIP_RECORD_BYTES: usize = super::MAX_CONTROL_MESSAGE_BYTES;
pub const OWNERSHIP_DIRECTORY: &str = ".kakune";
pub const RUNTIME_LOCK_FILE: &str = "runtime.lock";
const ADMIN_LOCK_FILE: &str = "admin.lock";
const OWNERSHIP_RECORD_FILE: &str = "ownership.json";
pub const CONTROL_CAPABILITY_PREFIX: &str = "control-";
pub const CONTROL_CAPABILITY_SUFFIX: &str = ".key";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum OwnerIdentity {
    WindowsSid(String),
    UnixUid(u32),
    Other(String),
}

impl OwnerIdentity {
    pub fn validate(&self) -> Result<(), LifecycleError> {
        match self {
            Self::WindowsSid(sid) if sid.trim().is_empty() => Err(LifecycleError::UnsafeOwnership(
                "empty Windows SID".to_owned(),
            )),
            Self::Other(identity) if identity.trim().is_empty() => Err(
                LifecycleError::UnsafeOwnership("empty OS user identity".to_owned()),
            ),
            _ => Ok(()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProcessIdentity {
    pub pid: u32,
    /// OS-observed process creation/start identity. It is never inferred from PID.
    pub start_identity: String,
    /// Absolute executable path observed from the operating system.
    pub executable: PathBuf,
}

impl ProcessIdentity {
    pub fn validate(&self) -> Result<(), LifecycleError> {
        if self.pid == 0 || self.start_identity.trim().is_empty() || !self.executable.is_absolute()
        {
            return Err(LifecycleError::UnsafeOwnership(
                "process identity is missing PID, start evidence, or absolute executable evidence"
                    .to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreInstance {
    pub instance_id: Uuid,
    pub mode: CoreMode,
    pub owner: OwnerIdentity,
    pub process_identity: ProcessIdentity,
    pub data_dir: PathBuf,
    pub listen_address: Option<String>,
    pub phase: InstancePhase,
    pub diagnostic_location: Option<String>,
    pub control_endpoint: Option<String>,
}

impl CoreInstance {
    pub fn validate(&self) -> Result<(), LifecycleError> {
        self.owner.validate()?;
        self.process_identity.validate()?;
        if !self.data_dir.is_absolute() {
            return Err(LifecycleError::UnsafeOwnership(
                "Core data directory is not absolute".to_owned(),
            ));
        }
        if self.data_dir.canonicalize()? != self.data_dir {
            return Err(LifecycleError::UnsafeOwnership(
                "Core data directory is not canonicalized".to_owned(),
            ));
        }
        if self.listen_address.as_deref().is_some_and(|address| {
            address.trim().is_empty() || address.contains(['\r', '\n', '@', '?', '#'])
        }) {
            return Err(LifecycleError::UnsafeOwnership(
                "listen address is empty or contains credentials/control suffixes".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InstancePhase {
    Starting,
    Ready,
    Stopping,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeOwnershipRecord {
    pub schema_version: u32,
    pub instance: CoreInstance,
}

impl RuntimeOwnershipRecord {
    pub fn new(instance: CoreInstance) -> Result<Self, LifecycleError> {
        instance.validate()?;
        Ok(Self {
            schema_version: OWNERSHIP_SCHEMA_VERSION,
            instance,
        })
    }

    pub fn validate(&self) -> Result<(), LifecycleError> {
        if self.schema_version != OWNERSHIP_SCHEMA_VERSION {
            return Err(LifecycleError::UnsupportedSchema(self.schema_version));
        }
        self.instance.validate()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LegacyOwnershipRecord {
    pub pid: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OwnershipRead {
    Current(Box<RuntimeOwnershipRecord>),
    Legacy(LegacyOwnershipRecord),
}

/// Exclusive lifetime ownership of a canonical data-directory location.
/// Dropping the lease releases the OS lock; its stable lock file is never removed.
pub struct RuntimeLease {
    canonical_data_dir: PathBuf,
    instance_id: Uuid,
    lock_file: File,
}

impl RuntimeLease {
    pub fn acquire(
        data_dir: &Path,
        instance_id: Uuid,
        clock: &dyn MonotonicClock,
        waiter: &dyn DeadlineWaiter,
    ) -> Result<Self, LifecycleError> {
        Self::acquire_with_deadline(
            data_dir,
            instance_id,
            Deadline::after(clock, ACQUISITION_BUDGET),
            clock,
            waiter,
        )
    }

    pub fn acquire_with_deadline(
        data_dir: &Path,
        instance_id: Uuid,
        deadline: Deadline,
        clock: &dyn MonotonicClock,
        waiter: &dyn DeadlineWaiter,
    ) -> Result<Self, LifecycleError> {
        fs::create_dir_all(data_dir)?;
        let canonical_data_dir = data_dir.canonicalize()?;
        let ownership_dir = ensure_ownership_directory(&canonical_data_dir)?;
        let lock_path = ownership_dir.join(RUNTIME_LOCK_FILE);
        reject_link_if_present(&lock_path)?;
        let lock_file = open_stable_lock(&lock_path)?;
        acquire_file_lock(&lock_file, deadline, clock, waiter, "runtime lease")?;

        Ok(Self {
            canonical_data_dir,
            instance_id,
            lock_file,
        })
    }

    pub fn canonical_data_dir(&self) -> &Path {
        &self.canonical_data_dir
    }

    pub fn instance_id(&self) -> Uuid {
        self.instance_id
    }

    pub fn capability_path(&self) -> PathBuf {
        capability_path(&self.canonical_data_dir, self.instance_id)
    }

    pub fn publish(
        &self,
        record: &RuntimeOwnershipRecord,
        clock: &dyn MonotonicClock,
        waiter: &dyn DeadlineWaiter,
    ) -> Result<(), LifecycleError> {
        record.validate()?;
        if record.instance.instance_id != self.instance_id
            || record.instance.data_dir != self.canonical_data_dir
        {
            return Err(LifecycleError::UnsafeOwnership(
                "ownership record does not match the held lease".to_owned(),
            ));
        }
        let admin = AdministrativeLease::acquire_for_data_dir(
            &self.canonical_data_dir,
            Deadline::after(clock, ACQUISITION_BUDGET),
            clock,
            waiter,
        )?;
        publish_record_locked(&admin, record)
    }

    pub fn cleanup_record(
        &self,
        clock: &dyn MonotonicClock,
        waiter: &dyn DeadlineWaiter,
    ) -> Result<bool, LifecycleError> {
        let admin = AdministrativeLease::acquire_for_data_dir(
            &self.canonical_data_dir,
            Deadline::after(clock, ACQUISITION_BUDGET),
            clock,
            waiter,
        )?;
        cleanup_record_if_instance_locked(&admin, self.instance_id)
    }

    pub fn is_locked(&self) -> bool {
        // Keeping the file handle in this object is the lifetime ownership proof.
        let _ = &self.lock_file;
        true
    }
}

/// Short-lived serialization for record publication/removal. The stable file is
/// deliberately retained after release to avoid unlink/recreate lock races.
pub struct AdministrativeLease {
    data_dir: PathBuf,
    directory: PathBuf,
    lock_file: File,
}

impl AdministrativeLease {
    pub fn acquire_for_data_dir(
        data_dir: &Path,
        deadline: Deadline,
        clock: &dyn MonotonicClock,
        waiter: &dyn DeadlineWaiter,
    ) -> Result<Self, LifecycleError> {
        let canonical_data_dir = data_dir.canonicalize()?;
        let ownership_dir = ensure_ownership_directory(&canonical_data_dir)?;
        let lock_path = ownership_dir.join(ADMIN_LOCK_FILE);
        reject_link_if_present(&lock_path)?;
        let lock_file = open_stable_lock(&lock_path)?;
        acquire_file_lock(&lock_file, deadline, clock, waiter, "administrative lock")?;
        Ok(Self {
            data_dir: canonical_data_dir,
            directory: ownership_dir,
            lock_file,
        })
    }

    pub fn ownership_record_path(&self) -> PathBuf {
        self.directory.join(OWNERSHIP_RECORD_FILE)
    }

    pub fn legacy_pid_path(&self) -> PathBuf {
        self.data_dir.join("daemon.pid")
    }

    pub fn owns_lock(&self) -> bool {
        let _ = &self.lock_file;
        true
    }
}

pub fn read_ownership_record(path: &Path) -> Result<Option<OwnershipRead>, LifecycleError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(LifecycleError::UnsafeOwnership(
                "ownership metadata is redirected or not a regular file".to_owned(),
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let metadata = file.metadata()?;
    if metadata.len() > MAX_OWNERSHIP_RECORD_BYTES as u64 {
        return Err(LifecycleError::MessageTooLarge {
            limit: MAX_OWNERSHIP_RECORD_BYTES,
        });
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take((MAX_OWNERSHIP_RECORD_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_OWNERSHIP_RECORD_BYTES {
        return Err(LifecycleError::MessageTooLarge {
            limit: MAX_OWNERSHIP_RECORD_BYTES,
        });
    }

    let value: serde_json::Value = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(error) => {
            if let Ok(text) = std::str::from_utf8(&bytes)
                && let Ok(pid) = text.trim().parse::<u32>()
                && pid > 0
            {
                return Ok(Some(OwnershipRead::Legacy(LegacyOwnershipRecord {
                    pid: Some(pid),
                })));
            }
            return Err(error.into());
        }
    };
    if let Some(version) = value
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
    {
        let version = u32::try_from(version).unwrap_or(u32::MAX);
        if version != OWNERSHIP_SCHEMA_VERSION {
            return Err(LifecycleError::UnsupportedSchema(version));
        }
        let record: RuntimeOwnershipRecord = serde_json::from_value(value)?;
        record.validate()?;
        Ok(Some(OwnershipRead::Current(Box::new(record))))
    } else {
        let pid = value
            .get("pid")
            .and_then(serde_json::Value::as_u64)
            .and_then(|pid| u32::try_from(pid).ok());
        Ok(Some(OwnershipRead::Legacy(LegacyOwnershipRecord { pid })))
    }
}

/// Returns whether an existing lifetime lock is currently held, without
/// creating a directory/file or taking a mutating exclusive lock. The shared
/// observation lock is released as soon as the result is determined.
pub fn runtime_lease_is_held(data_dir: &Path) -> Result<bool, LifecycleError> {
    let lock_path = data_dir.join(OWNERSHIP_DIRECTORY).join(RUNTIME_LOCK_FILE);
    let metadata = match fs::symlink_metadata(&lock_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(LifecycleError::UnsafeOwnership(
            "runtime lease path is redirected or not a regular file".to_owned(),
        ));
    }
    let file = OpenOptions::new().read(true).write(true).open(&lock_path)?;
    match file.try_lock_shared() {
        Ok(()) => Ok(false),
        Err(std::fs::TryLockError::WouldBlock) => Ok(true),
        Err(std::fs::TryLockError::Error(error)) => Err(error.into()),
    }
}

pub fn capability_path(data_dir: &Path, instance_id: Uuid) -> PathBuf {
    data_dir.join(OWNERSHIP_DIRECTORY).join(format!(
        "{CONTROL_CAPABILITY_PREFIX}{instance_id}{CONTROL_CAPABILITY_SUFFIX}"
    ))
}

/// Removes metadata only when it still names the expected instance. Callers
/// must hold the administrative lease returned for that data directory.
pub fn cleanup_record_if_instance_locked(
    admin: &AdministrativeLease,
    expected_instance_id: Uuid,
) -> Result<bool, LifecycleError> {
    let path = admin.ownership_record_path();
    let Some(OwnershipRead::Current(record)) = read_ownership_record(&path)? else {
        return Ok(false);
    };
    if record.instance.instance_id != expected_instance_id {
        return Ok(false);
    }
    fs::remove_file(path)?;
    Ok(true)
}

/// Removes a legacy PID-only record only after a caller's independent OS
/// observer positively confirms that the referenced process is absent. The
/// PID is evidence to inspect, never authority to signal.
pub fn cleanup_legacy_if_proven_dead_locked(
    admin: &AdministrativeLease,
    process_is_absent: impl FnOnce(u32) -> Result<bool, LifecycleError>,
) -> Result<bool, LifecycleError> {
    let ownership_path = admin.ownership_record_path();
    if let Some(OwnershipRead::Legacy(LegacyOwnershipRecord { pid: Some(pid) })) =
        read_ownership_record(&ownership_path)?
    {
        if !process_is_absent(pid)? {
            return Ok(false);
        }
        fs::remove_file(ownership_path)?;
        return Ok(true);
    }

    let legacy_path = admin.legacy_pid_path();
    let Some(pid) = read_legacy_pid_file(&legacy_path)? else {
        return Ok(false);
    };
    if !process_is_absent(pid)? {
        return Ok(false);
    }
    fs::remove_file(legacy_path)?;
    Ok(true)
}

/// Reads the legacy plain-decimal `daemon.pid` format without treating it as
/// process identity or shutdown authority.
pub fn read_legacy_pid_file(path: &Path) -> Result<Option<u32>, LifecycleError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(LifecycleError::UnsafeOwnership(
                "legacy PID path is redirected or not a regular file".to_owned(),
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let pid = text
        .trim()
        .parse::<u32>()
        .map_err(|_| LifecycleError::UnsafeOwnership("legacy PID file is malformed".to_owned()))?;
    if pid == 0 {
        return Err(LifecycleError::UnsafeOwnership(
            "legacy PID file contains an invalid process identifier".to_owned(),
        ));
    }
    Ok(Some(pid))
}

pub fn publish_record_locked(
    admin: &AdministrativeLease,
    record: &RuntimeOwnershipRecord,
) -> Result<(), LifecycleError> {
    record.validate()?;
    let bytes = serde_json::to_vec(record)?;
    if bytes.len() > MAX_OWNERSHIP_RECORD_BYTES {
        return Err(LifecycleError::MessageTooLarge {
            limit: MAX_OWNERSHIP_RECORD_BYTES,
        });
    }
    let destination = admin.ownership_record_path();
    reject_link_if_present(&destination)?;
    let temporary = admin
        .directory
        .join(format!("ownership-{}.tmp", Uuid::new_v4()));
    let result = (|| -> Result<(), LifecycleError> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary, &destination)?;
        #[cfg(unix)]
        File::open(&admin.directory)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        // This path is uniquely created by this call; never remove the stable lock.
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn ensure_ownership_directory(data_dir: &Path) -> Result<PathBuf, LifecycleError> {
    if !data_dir.is_absolute() {
        return Err(LifecycleError::UnsafeOwnership(
            "data directory must be absolute".to_owned(),
        ));
    }
    let directory = data_dir.join(OWNERSHIP_DIRECTORY);
    match fs::symlink_metadata(&directory) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(LifecycleError::UnsafeOwnership(
                "ownership directory is a symlink or reparse point".to_owned(),
            ));
        }
        Ok(metadata) if !metadata.is_dir() => {
            return Err(LifecycleError::UnsafeOwnership(
                "ownership path is not a directory".to_owned(),
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                let mut builder = fs::DirBuilder::new();
                builder.mode(0o700);
                builder.create(&directory)?;
            }
            #[cfg(not(unix))]
            fs::create_dir(&directory)?;
        }
        Err(error) => return Err(error.into()),
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let metadata = fs::symlink_metadata(&directory)?;
        let mode = metadata.permissions().mode();
        if metadata.uid() != rustix::process::geteuid().as_raw()
            || mode & 0o700 != 0o700
            || mode & 0o077 != 0
        {
            return Err(LifecycleError::UnsafeOwnership(
                "ownership directory must be current-user-owned and owner-only".to_owned(),
            ));
        }
    }
    Ok(directory)
}

fn reject_link_if_present(path: &Path) -> Result<(), LifecycleError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(LifecycleError::UnsafeOwnership(
            format!("{} is a symlink or reparse point", path.display()),
        )),
        Ok(metadata) if !metadata.is_file() => Err(LifecycleError::UnsafeOwnership(format!(
            "{} is not a regular file",
            path.display()
        ))),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn open_stable_lock(path: &Path) -> Result<File, LifecycleError> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(Into::into)
}

fn acquire_file_lock(
    file: &File,
    deadline: Deadline,
    clock: &dyn MonotonicClock,
    waiter: &dyn DeadlineWaiter,
    phase: &'static str,
) -> Result<(), LifecycleError> {
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(()),
            Err(std::fs::TryLockError::WouldBlock) => {
                let remaining = deadline.remaining(clock);
                if remaining.is_zero() {
                    return Err(if phase == "runtime lease" {
                        LifecycleError::OwnershipBusy
                    } else {
                        LifecycleError::DeadlineExpired { phase }
                    });
                }
                waiter.wait(remaining.min(Duration::from_millis(20)));
            }
            Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::Duration,
    };

    #[derive(Clone, Default)]
    struct FakeClock(Arc<AtomicU64>);

    impl MonotonicClock for FakeClock {
        fn now(&self) -> Duration {
            Duration::from_nanos(self.0.load(Ordering::SeqCst))
        }
    }

    struct AdvancingWaiter(FakeClock);

    impl DeadlineWaiter for AdvancingWaiter {
        fn wait(&self, duration: Duration) {
            self.0
                .0
                .fetch_add(duration.as_nanos() as u64, Ordering::SeqCst);
        }
    }

    fn temp_dir() -> PathBuf {
        std::env::temp_dir().join(format!("kakune-lifecycle-{}", Uuid::new_v4()))
    }

    fn record(data_dir: &Path, instance_id: Uuid) -> RuntimeOwnershipRecord {
        RuntimeOwnershipRecord::new(CoreInstance {
            instance_id,
            mode: CoreMode::Foreground,
            owner: OwnerIdentity::Other("test-owner".to_owned()),
            process_identity: ProcessIdentity {
                pid: 123,
                start_identity: "start-123".to_owned(),
                executable: std::env::current_exe().expect("test executable should be absolute"),
            },
            data_dir: data_dir.to_path_buf(),
            listen_address: Some("http://127.0.0.1:1234".to_owned()),
            phase: InstancePhase::Starting,
            diagnostic_location: None,
            control_endpoint: None,
        })
        .expect("test record should validate")
    }

    #[test]
    fn lease_canonicalizes_aliases_and_stable_file_remains_after_drop() {
        let root = temp_dir();
        fs::create_dir_all(&root).unwrap();
        let alias = root.join(".");

        let clock = FakeClock::default();
        let waiter = AdvancingWaiter(clock.clone());
        let id = Uuid::new_v4();
        let lease = RuntimeLease::acquire(&root, id, &clock, &waiter).unwrap();
        let alias_lease = RuntimeLease::acquire(&alias, Uuid::new_v4(), &clock, &waiter);
        assert!(matches!(alias_lease, Err(LifecycleError::OwnershipBusy)));
        let lock_path = root.join(OWNERSHIP_DIRECTORY).join(RUNTIME_LOCK_FILE);
        drop(lease);
        assert!(lock_path.is_file(), "stable lease files are not unlinked");
        drop(alias);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn publication_is_atomic_and_cleanup_checks_instance_identity() {
        let root = temp_dir();
        fs::create_dir_all(&root).unwrap();
        let clock = FakeClock::default();
        let waiter = AdvancingWaiter(clock.clone());
        let id = Uuid::new_v4();
        let lease = RuntimeLease::acquire(&root, id, &clock, &waiter).unwrap();
        let original_record = record(lease.canonical_data_dir(), id);
        lease.publish(&original_record, &clock, &waiter).unwrap();
        let path = root.join(OWNERSHIP_DIRECTORY).join(OWNERSHIP_RECORD_FILE);
        assert_eq!(
            read_ownership_record(&path).unwrap(),
            Some(OwnershipRead::Current(Box::new(original_record)))
        );

        let replacement_id = Uuid::new_v4();
        let admin = AdministrativeLease::acquire_for_data_dir(
            &root,
            Deadline::after(&clock, ACQUISITION_BUDGET),
            &clock,
            &waiter,
        )
        .unwrap();
        let replacement = record(lease.canonical_data_dir(), replacement_id);
        publish_record_locked(&admin, &replacement).unwrap();
        drop(admin);
        assert!(
            !lease
                .cleanup_record(&clock, &waiter)
                .expect("a different instance must not be removed")
        );

        let admin = AdministrativeLease::acquire_for_data_dir(
            &root,
            Deadline::after(&clock, ACQUISITION_BUDGET),
            &clock,
            &waiter,
        )
        .unwrap();
        assert!(cleanup_record_if_instance_locked(&admin, replacement_id).unwrap());
        assert_eq!(read_ownership_record(&path).unwrap(), None);
        drop(lease);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn unknown_schema_and_legacy_pid_are_not_authoritative() {
        let root = temp_dir();
        fs::create_dir_all(&root).unwrap();
        let path = root.join(OWNERSHIP_RECORD_FILE);
        fs::write(&path, br#"{"schema_version":99,"instance":{}}"#).unwrap();
        assert!(matches!(
            read_ownership_record(&path),
            Err(LifecycleError::UnsupportedSchema(99))
        ));
        fs::write(&path, br#"{"pid":12345}"#).unwrap();
        assert_eq!(
            read_ownership_record(&path).unwrap(),
            Some(OwnershipRead::Legacy(LegacyOwnershipRecord {
                pid: Some(12345)
            }))
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn an_ownership_symlink_is_rejected_before_locking() {
        let root = temp_dir();
        fs::create_dir_all(&root).unwrap();
        let linked = root.join(OWNERSHIP_DIRECTORY);
        let target = temp_dir();
        fs::create_dir_all(&target).unwrap();
        std::os::unix::fs::symlink(&target, &linked).unwrap();
        let clock = FakeClock::default();
        let waiter = AdvancingWaiter(clock.clone());
        assert!(matches!(
            RuntimeLease::acquire(&root, Uuid::new_v4(), &clock, &waiter),
            Err(LifecycleError::UnsafeOwnership(_))
        ));
        fs::remove_dir_all(root).unwrap();
        fs::remove_dir_all(target).unwrap();
    }
}
