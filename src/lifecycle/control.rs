//! Bounded, instance-bound local control authentication and capability storage.

use std::{collections::HashSet, io, net::SocketAddr, path::Path, sync::Mutex, time::Duration};

#[cfg(unix)]
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::PathBuf,
};

use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use subtle::ConstantTimeEq;
use uuid::Uuid;
use zeroize::Zeroize;

use super::{
    CoreMode, Deadline, LifecycleError, MAX_CONTROL_MESSAGE_BYTES, MonotonicClock,
    ownership::OwnerIdentity,
};

pub const CONTROL_PROTOCOL_VERSION: u8 = 1;
pub const CONTROL_NONCE_BYTES: usize = 32;
pub const CONTROL_CAPABILITY_BYTES: usize = 32;
const FRAME_HEADER_BYTES: usize = 4;
const MAX_USED_CONTROL_CHALLENGES: usize = 65_536;
type HmacSha256 = Hmac<Sha256>;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ControlContext {
    pub instance_id: Uuid,
    pub owner: OwnerIdentity,
    pub mode: CoreMode,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ControlChallenge {
    pub protocol_version: u8,
    pub context: ControlContext,
    pub client_nonce: [u8; CONTROL_NONCE_BYTES],
    pub server_nonce: [u8; CONTROL_NONCE_BYTES],
}

impl ControlChallenge {
    pub fn generate(
        context: ControlContext,
        random: &dyn RandomSource,
    ) -> Result<Self, LifecycleError> {
        let mut client_nonce = [0_u8; CONTROL_NONCE_BYTES];
        let mut server_nonce = [0_u8; CONTROL_NONCE_BYTES];
        random.fill(&mut client_nonce)?;
        random.fill(&mut server_nonce)?;
        Ok(Self {
            protocol_version: CONTROL_PROTOCOL_VERSION,
            context,
            client_nonce,
            server_nonce,
        })
    }

    pub fn validate(&self) -> Result<(), LifecycleError> {
        if self.protocol_version != CONTROL_PROTOCOL_VERSION {
            return Err(LifecycleError::AuthenticationFailed);
        }
        self.context.owner.validate()?;
        if self.client_nonce.iter().all(|byte| *byte == 0)
            || self.server_nonce.iter().all(|byte| *byte == 0)
        {
            return Err(LifecycleError::AuthenticationFailed);
        }
        Ok(())
    }

    /// Builds the server half of the handshake from a client-supplied fresh
    /// nonce. The client nonce is never replaced or trusted as server entropy.
    pub fn answer_client_hello(
        context: ControlContext,
        client_nonce: [u8; CONTROL_NONCE_BYTES],
        random: &dyn RandomSource,
    ) -> Result<Self, LifecycleError> {
        if client_nonce.iter().all(|byte| *byte == 0) {
            return Err(LifecycleError::AuthenticationFailed);
        }
        let mut server_nonce = [0_u8; CONTROL_NONCE_BYTES];
        random.fill(&mut server_nonce)?;
        let challenge = Self {
            protocol_version: CONTROL_PROTOCOL_VERSION,
            context,
            client_nonce,
            server_nonce,
        };
        challenge.validate()?;
        Ok(challenge)
    }
}

/// A validated TCP endpoint for lifecycle control. Public/wildcard addresses
/// and unbound port zero are not representable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LoopbackControlEndpoint(SocketAddr);

impl LoopbackControlEndpoint {
    pub fn new(address: SocketAddr) -> Result<Self, LifecycleError> {
        if !address.ip().is_loopback() || address.port() == 0 {
            return Err(LifecycleError::UnsafeOwnership(
                "lifecycle control must use a bound loopback-only endpoint".to_owned(),
            ));
        }
        Ok(Self(address))
    }

    pub fn address(self) -> SocketAddr {
        self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlOperation {
    Readiness,
    GracefulStop,
    ServiceShutdown,
}

#[derive(Clone, Copy, Debug)]
pub struct ControlOperationRequest<'a> {
    pub operation: &'a ControlOperation,
    pub arguments: &'a [u8],
}

impl ControlOperation {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Readiness => "readiness",
            Self::GracefulStop => "graceful-stop",
            Self::ServiceShutdown => "service-shutdown",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ControlRequest {
    pub operation: ControlOperation,
    pub arguments: Vec<u8>,
    pub proof: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ControlResponse {
    pub operation: ControlOperation,
    pub result: Vec<u8>,
    pub proof: [u8; 32],
}

/// Secret bytes are zeroized when the in-memory capability leaves scope.
pub struct Capability([u8; CONTROL_CAPABILITY_BYTES]);

impl Capability {
    pub fn from_bytes(bytes: [u8; CONTROL_CAPABILITY_BYTES]) -> Self {
        Self(bytes)
    }

    fn bytes(&self) -> &[u8; CONTROL_CAPABILITY_BYTES] {
        &self.0
    }
}

impl Drop for Capability {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

pub trait RandomSource: Send + Sync {
    fn fill(&self, destination: &mut [u8]) -> Result<(), LifecycleError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct OsRandomSource;

impl RandomSource for OsRandomSource {
    fn fill(&self, destination: &mut [u8]) -> Result<(), LifecycleError> {
        getrandom::fill(destination).map_err(|_| LifecycleError::RandomnessFailed)
    }
}

/// Platform-neutral seam used by lifecycle tests and platform ACL adapters.
pub trait CapabilityStorage: Send + Sync {
    fn store_new(
        &self,
        path: &Path,
        capability: &Capability,
        expected_owner: &OwnerIdentity,
    ) -> Result<(), LifecycleError>;

    fn load(
        &self,
        path: &Path,
        expected_owner: &OwnerIdentity,
    ) -> Result<Capability, LifecycleError>;
}

/// Validates owner-only POSIX protection or an explicit Windows ACL before
/// writing capability bytes. Unsupported or unverified protection fails closed.
#[derive(Clone, Copy, Debug, Default)]
pub struct OwnerProtectedFileStorage;

/// Establishes the verified owner's directory identity while the Core holds
/// its runtime lease. Windows elevated tokens may default new directories to
/// Administrators ownership; no untrusted owner or writable ancestor is repaired.
pub(crate) fn prepare_capability_directory(
    path: &Path,
    owner: &OwnerIdentity,
) -> Result<(), LifecycleError> {
    #[cfg(windows)]
    windows_capability_storage::prepare_directory_owner(path, owner)?;
    #[cfg(not(windows))]
    let _ = (path, owner);
    Ok(())
}

/// Persists a bounded, already-sanitized Windows service-host diagnostic under
/// the same verified owner protection as lifecycle secrets. Never a fallback
/// for a path whose protection could not be established.
#[cfg(windows)]
pub fn write_service_diagnostic(
    path: &Path,
    message: &[u8],
    owner: &OwnerIdentity,
) -> Result<(), LifecycleError> {
    if message.len() > 8192 {
        return Err(LifecycleError::MessageTooLarge { limit: 8192 });
    }
    windows_capability_storage::store_bytes(path, message, owner)
}

impl CapabilityStorage for OwnerProtectedFileStorage {
    fn store_new(
        &self,
        path: &Path,
        capability: &Capability,
        expected_owner: &OwnerIdentity,
    ) -> Result<(), LifecycleError> {
        validate_owner_kind(expected_owner)?;
        #[cfg(unix)]
        {
            let parent = path.parent().ok_or_else(|| {
                LifecycleError::InsecureProtection("capability path has no parent".to_owned())
            })?;
            validate_unix_private_directory(parent)?;
            reject_redirected_path(path)?;

            use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
            let mut file = OpenOptions::new()
                .write(true)
                .read(true)
                .create_new(true)
                .mode(0o600)
                .open(path)?;
            let metadata = file.metadata()?;
            let effective_uid = rustix::process::geteuid().as_raw();
            if metadata.uid() != effective_uid || metadata.permissions().mode() & 0o777 != 0o600 {
                drop(file);
                let _ = fs::remove_file(path);
                return Err(LifecycleError::InsecureProtection(
                    "capability file owner or mode was not verified before publication".to_owned(),
                ));
            }
            // Restrictive mode and owner are checked before any secret bytes are written.
            if let Err(error) = file
                .write_all(capability.bytes())
                .and_then(|()| file.sync_all())
            {
                drop(file);
                let _ = fs::remove_file(path);
                return Err(error.into());
            }
            Ok(())
        }
        #[cfg(windows)]
        {
            windows_capability_storage::store_new(path, capability, expected_owner)
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = (path, capability);
            Err(LifecycleError::InsecureProtection(
                "owner-protected capability storage is unsupported on this platform".to_owned(),
            ))
        }
    }

    fn load(
        &self,
        path: &Path,
        expected_owner: &OwnerIdentity,
    ) -> Result<Capability, LifecycleError> {
        validate_owner_kind(expected_owner)?;
        #[cfg(unix)]
        {
            let parent = path.parent().ok_or_else(|| {
                LifecycleError::InsecureProtection("capability path has no parent".to_owned())
            })?;
            validate_unix_private_directory(parent)?;
            reject_redirected_path(path)?;
            validate_unix_private_file(path)?;
            let mut file = File::open(path)?;
            let mut bytes = [0_u8; CONTROL_CAPABILITY_BYTES];
            file.read_exact(&mut bytes)?;
            let mut trailing = [0_u8; 1];
            if file.read(&mut trailing)? != 0 {
                bytes.zeroize();
                return Err(LifecycleError::MessageTooLarge {
                    limit: CONTROL_CAPABILITY_BYTES,
                });
            }
            Ok(Capability::from_bytes(bytes))
        }
        #[cfg(windows)]
        {
            windows_capability_storage::load(path, expected_owner)
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = path;
            Err(LifecycleError::InsecureProtection(
                "owner-protected capability storage is unsupported on this platform".to_owned(),
            ))
        }
    }
}

/// Removes only a capability whose owner and filesystem protection still
/// match the verified runtime owner. Capability files are per-instance and are
/// never reused across daemon launches.
pub fn remove_capability(
    path: &Path,
    expected_owner: &OwnerIdentity,
) -> Result<bool, LifecycleError> {
    validate_owner_kind(expected_owner)?;
    #[cfg(unix)]
    {
        let parent = path.parent().ok_or_else(|| {
            LifecycleError::InsecureProtection("capability path has no parent".to_owned())
        })?;
        validate_unix_private_directory(parent)?;
        reject_redirected_path(path)?;
        match fs::symlink_metadata(path) {
            Ok(_) => validate_unix_private_file(path)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error.into()),
        }
        fs::remove_file(path)?;
        Ok(true)
    }
    #[cfg(windows)]
    {
        windows_capability_storage::remove(path, expected_owner)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = path;
        Err(LifecycleError::InsecureProtection(
            "owner-protected capability storage is unsupported on this platform".to_owned(),
        ))
    }
}

#[cfg(windows)]
mod windows_capability_storage {
    use std::{
        collections::HashSet,
        fs::{self, File, OpenOptions},
        io::{Read, Write},
        os::windows::fs::MetadataExt,
        path::{Component, Path},
    };

    use windows_permissions::{
        LocalBox, SecurityDescriptor, Sid,
        constants::{AccessRights, AceFlags, AceType, SeObjectType, SecurityInformation},
        wrappers::{
            ConvertSecurityDescriptorToStringSecurityDescriptor as to_sddl,
            GetNamedSecurityInfo as get_security_info, SetNamedSecurityInfo as set_security_info,
        },
    };
    use zeroize::Zeroize;

    use super::{CONTROL_CAPABILITY_BYTES, Capability, LifecycleError, OwnerIdentity};

    const MAX_WINDOWS_ACL_ENTRIES: u32 = 256;
    const MAX_WINDOWS_PATH_ANCESTORS: usize = 256;
    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
    const SYSTEM_SID: &str = "S-1-5-18";
    const BUILTIN_ADMINISTRATORS_SID: &str = "S-1-5-32-544";
    const TRUSTED_INSTALLER_SID: &str =
        "S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464";

    // Directories may be readable/traversable by other accounts, but only the
    // verified owner, SYSTEM, and Administrators may change an ancestor.
    const SAFE_ANCESTOR_ACCESS_MASK: u32 = 0x8000_0000 | 0x2000_0000 | 0x0012_00A9 | 0x0000_0006;

    pub(super) fn prepare_directory_owner(
        path: &Path,
        expected_owner: &OwnerIdentity,
    ) -> Result<(), LifecycleError> {
        let owner_sid = parse_owner_sid(expected_owner)?;
        let directory = path
            .parent()
            .ok_or_else(|| insecure("capability path has no parent directory"))?;
        let descriptor = read_security_descriptor(directory)?;
        if descriptor.owner() == Some(&*owner_sid) {
            return validate_directory_chain(directory, &owner_sid);
        }
        validate_owner_preparation(&descriptor, &owner_sid)?;
        validate_directory_chain_inner(directory, &owner_sid, false)?;
        set_security_info(
            directory,
            SeObjectType::SE_FILE_OBJECT,
            SecurityInformation::Owner,
            Some(&owner_sid),
            None,
            None,
            None,
        )
        .map_err(|error| {
            insecure(format!(
                "could not establish capability directory owner: {error}"
            ))
        })?;
        validate_directory_chain(directory, &owner_sid)
    }

    fn validate_owner_preparation(
        descriptor: &SecurityDescriptor,
        expected_owner: &Sid,
    ) -> Result<(), LifecycleError> {
        let owner = descriptor
            .owner()
            .ok_or_else(|| insecure("capability directory owner is unknown"))?;
        if owner != expected_owner
            && owner.to_string() != SYSTEM_SID
            && owner.to_string() != BUILTIN_ADMINISTRATORS_SID
        {
            return Err(insecure(
                "cannot reassign an untrusted capability directory owner",
            ));
        }
        validate_directory_dacl(descriptor, expected_owner)
    }

    pub(super) fn store_new(
        path: &Path,
        capability: &Capability,
        expected_owner: &OwnerIdentity,
    ) -> Result<(), LifecycleError> {
        store_bytes(path, capability.bytes(), expected_owner)
    }

    pub(super) fn store_bytes(
        path: &Path,
        bytes: &[u8],
        expected_owner: &OwnerIdentity,
    ) -> Result<(), LifecycleError> {
        let owner_sid = parse_owner_sid(expected_owner)?;
        let parent = path
            .parent()
            .ok_or_else(|| insecure("capability path has no parent directory"))?;
        validate_directory_chain(parent, &owner_sid)?;

        let mut file = OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(path)?;
        let result = (|| {
            // The exclusively-created file is still empty while its inherited
            // DACL is replaced, protected, and independently read back.
            set_and_verify_capability_acl(path, &owner_sid)?;
            file.write_all(bytes)?;
            file.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            drop(file);
            // The containing directory was checked against untrusted writes and
            // this path was created exclusively by this call.
            let _ = fs::remove_file(path);
        }
        result
    }

    pub(super) fn load(
        path: &Path,
        expected_owner: &OwnerIdentity,
    ) -> Result<Capability, LifecycleError> {
        let owner_sid = parse_owner_sid(expected_owner)?;
        let parent = path
            .parent()
            .ok_or_else(|| insecure("capability path has no parent directory"))?;
        validate_directory_chain(parent, &owner_sid)?;
        validate_regular_non_reparse_file(path)?;
        validate_capability_acl(path, &owner_sid)?;

        let mut file = File::open(path)?;
        let mut bytes = [0_u8; CONTROL_CAPABILITY_BYTES];
        if let Err(error) = file.read_exact(&mut bytes) {
            bytes.zeroize();
            return Err(error.into());
        }
        let mut trailing = [0_u8; 1];
        let trailing_bytes = match file.read(&mut trailing) {
            Ok(read) => read,
            Err(error) => {
                bytes.zeroize();
                return Err(error.into());
            }
        };
        if trailing_bytes != 0 {
            bytes.zeroize();
            return Err(LifecycleError::MessageTooLarge {
                limit: CONTROL_CAPABILITY_BYTES,
            });
        }
        Ok(Capability::from_bytes(bytes))
    }

    pub(super) fn remove(
        path: &Path,
        expected_owner: &OwnerIdentity,
    ) -> Result<bool, LifecycleError> {
        let owner_sid = parse_owner_sid(expected_owner)?;
        let parent = path
            .parent()
            .ok_or_else(|| insecure("capability path has no parent directory"))?;
        validate_directory_chain(parent, &owner_sid)?;
        match fs::symlink_metadata(path) {
            Ok(_) => {
                validate_regular_non_reparse_file(path)?;
                validate_capability_acl(path, &owner_sid)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error.into()),
        }
        fs::remove_file(path)?;
        Ok(true)
    }

    fn parse_owner_sid(owner: &OwnerIdentity) -> Result<LocalBox<Sid>, LifecycleError> {
        let OwnerIdentity::WindowsSid(sid) = owner else {
            return Err(insecure(
                "Windows capability storage requires a verified SID",
            ));
        };
        sid.parse()
            .map_err(|_| insecure("the expected Windows SID is invalid"))
    }

    fn trusted_sids(owner_sid: &Sid) -> HashSet<String> {
        [
            owner_sid.to_string(),
            SYSTEM_SID.to_owned(),
            BUILTIN_ADMINISTRATORS_SID.to_owned(),
        ]
        .into_iter()
        .collect()
    }

    fn trusted_ancestor_sids(owner_sid: &Sid) -> HashSet<String> {
        let mut sids = trusted_sids(owner_sid);
        // TrustedInstaller owns protected Windows system directories and is a
        // privileged OS service, but is never granted capability-file access.
        sids.insert(TRUSTED_INSTALLER_SID.to_owned());
        sids
    }

    fn desired_capability_descriptor(
        owner_sid: &Sid,
    ) -> Result<LocalBox<SecurityDescriptor>, LifecycleError> {
        let mut sids = trusted_sids(owner_sid).into_iter().collect::<Vec<_>>();
        sids.sort_unstable();
        let dacl = sids
            .iter()
            .map(|sid| format!("(A;;FA;;;{sid})"))
            .collect::<String>();
        format!("D:P{dacl}")
            .parse()
            .map_err(|_| insecure("could not construct the required protected Windows DACL"))
    }

    fn set_and_verify_capability_acl(path: &Path, owner_sid: &Sid) -> Result<(), LifecycleError> {
        establish_verified_owner(path, owner_sid)?;
        let desired = desired_capability_descriptor(owner_sid)?;
        let dacl = desired
            .dacl()
            .ok_or_else(|| insecure("the required Windows DACL is missing"))?;
        set_security_info(
            path,
            SeObjectType::SE_FILE_OBJECT,
            SecurityInformation::Dacl | SecurityInformation::ProtectedDacl,
            None,
            None,
            Some(dacl),
            None,
        )
        .map_err(|error| insecure(format!("could not set the Windows capability ACL: {error}")))?;
        validate_capability_acl(path, owner_sid)
    }

    /// Elevated tokens create files owned by the Administrators group instead
    /// of the acting account. Only that exact trusted case is corrected, and
    /// only towards the already verified owner. A foreign owner fails closed.
    fn establish_verified_owner(path: &Path, owner_sid: &Sid) -> Result<(), LifecycleError> {
        let descriptor = read_security_descriptor(path)?;
        if !owner_needs_establishment(&descriptor, owner_sid)? {
            return Ok(());
        }
        set_security_info(
            path,
            SeObjectType::SE_FILE_OBJECT,
            SecurityInformation::Owner,
            Some(owner_sid),
            None,
            None,
            None,
        )
        .map_err(|error| {
            insecure(format!(
                "could not establish the verified capability file owner: {error}"
            ))
        })?;
        let updated = read_security_descriptor(path)?;
        if updated.owner() != Some(owner_sid) {
            return Err(insecure(
                "capability file owner does not match the verified Windows SID",
            ));
        }
        Ok(())
    }

    fn owner_needs_establishment(
        descriptor: &SecurityDescriptor,
        owner_sid: &Sid,
    ) -> Result<bool, LifecycleError> {
        let owner = descriptor
            .owner()
            .ok_or_else(|| insecure("capability file owner is unknown"))?;
        if owner == owner_sid {
            return Ok(false);
        }
        if owner.to_string() != BUILTIN_ADMINISTRATORS_SID {
            return Err(insecure(
                "capability file owner does not match the verified Windows SID",
            ));
        }
        Ok(true)
    }

    fn validate_capability_acl(path: &Path, owner_sid: &Sid) -> Result<(), LifecycleError> {
        let descriptor = read_security_descriptor(path)?;
        validate_capability_descriptor(&descriptor, owner_sid)
    }

    fn validate_capability_descriptor(
        descriptor: &SecurityDescriptor,
        owner_sid: &Sid,
    ) -> Result<(), LifecycleError> {
        if descriptor.owner() != Some(owner_sid) {
            return Err(insecure(
                "capability file owner does not match the verified Windows SID",
            ));
        }
        let dacl = descriptor
            .dacl()
            .ok_or_else(|| insecure("capability file has no Windows DACL"))?;
        let sddl = to_sddl(
            descriptor,
            SecurityInformation::Owner | SecurityInformation::Dacl,
        )
        .map_err(|error| insecure(format!("could not verify the Windows DACL: {error}")))?;
        let sddl = sddl.to_string_lossy();
        let dacl_start = sddl
            .find("D:")
            .ok_or_else(|| insecure("capability file has no verifiable DACL section"))?;
        if !sddl[dacl_start..].starts_with("D:P") {
            return Err(insecure(
                "capability file DACL is not protected from inherited permissions",
            ));
        }

        let expected_sids = trusted_sids(owner_sid);
        if dacl.len() != expected_sids.len() as u32 {
            return Err(insecure(
                "capability file DACL contains missing or additional access entries",
            ));
        }
        let mut observed_sids = HashSet::with_capacity(expected_sids.len());
        for index in 0..dacl.len() {
            let ace = dacl
                .get_ace(index)
                .ok_or_else(|| insecure("capability file DACL contains an unreadable entry"))?;
            let sid = ace
                .sid()
                .ok_or_else(|| insecure("capability file DACL contains an unidentified SID"))?;
            if ace.ace_type() != AceType::ACCESS_ALLOWED_ACE_TYPE
                || !ace.flags().is_empty()
                || ace.mask() != AccessRights::FileAllAccess
                || !expected_sids.contains(&sid.to_string())
                || !observed_sids.insert(sid.to_string())
            {
                return Err(insecure(
                    "capability file DACL is not exactly owner/System/Administrators-only",
                ));
            }
        }
        if observed_sids != expected_sids {
            return Err(insecure(
                "capability file DACL is missing a required trusted principal",
            ));
        }
        Ok(())
    }

    fn validate_directory_chain(
        directory: &Path,
        expected_owner: &Sid,
    ) -> Result<(), LifecycleError> {
        validate_directory_chain_inner(directory, expected_owner, true)
    }

    fn validate_directory_chain_inner(
        directory: &Path,
        expected_owner: &Sid,
        require_last_owner: bool,
    ) -> Result<(), LifecycleError> {
        if !directory.is_absolute()
            || directory
                .components()
                .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        {
            return Err(insecure(
                "capability directory must be absolute and normalized",
            ));
        }

        let mut ancestors = Vec::with_capacity(16);
        for ancestor in directory.ancestors() {
            if ancestors.len() >= MAX_WINDOWS_PATH_ANCESTORS {
                return Err(insecure("capability path has too many ancestors to verify"));
            }
            ancestors.push(ancestor.to_path_buf());
        }
        ancestors.reverse();
        for (index, ancestor) in ancestors.iter().enumerate() {
            let metadata = fs::symlink_metadata(ancestor)?;
            if !metadata.is_dir()
                || metadata.file_type().is_symlink()
                || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
            {
                return Err(insecure(format!(
                    "{} is redirected or is not a directory",
                    ancestor.display()
                )));
            }
            let descriptor = read_security_descriptor(ancestor)?;
            validate_directory_owner(
                &descriptor,
                expected_owner,
                require_last_owner && index + 1 == ancestors.len(),
            )?;
            validate_directory_dacl(&descriptor, expected_owner)?;
        }
        Ok(())
    }

    fn validate_directory_owner(
        descriptor: &SecurityDescriptor,
        expected_owner: &Sid,
        is_capability_parent: bool,
    ) -> Result<(), LifecycleError> {
        let owner = descriptor
            .owner()
            .ok_or_else(|| insecure("capability ancestor has no verifiable owner"))?;
        if is_capability_parent {
            if owner != expected_owner {
                return Err(insecure(
                    "capability directory owner does not match the verified Windows SID",
                ));
            }
        } else if !trusted_ancestor_sids(expected_owner).contains(&owner.to_string()) {
            // A directory owner can change its DACL even without an explicit
            // WRITE_DAC ACE, so every traversed ancestor owner is trusted too.
            return Err(insecure(format!(
                "capability ancestor is owned by untrusted Windows SID {owner}"
            )));
        }
        Ok(())
    }

    fn validate_directory_dacl(
        descriptor: &SecurityDescriptor,
        expected_owner: &Sid,
    ) -> Result<(), LifecycleError> {
        let dacl = descriptor
            .dacl()
            .ok_or_else(|| insecure("capability ancestor has no Windows DACL"))?;
        if dacl.len() > MAX_WINDOWS_ACL_ENTRIES {
            return Err(insecure(
                "capability ancestor DACL exceeds the verification bound",
            ));
        }
        let trusted = trusted_ancestor_sids(expected_owner);
        for index in 0..dacl.len() {
            let ace = dacl
                .get_ace(index)
                .ok_or_else(|| insecure("capability ancestor DACL contains an unreadable entry"))?;
            match ace.ace_type() {
                AceType::ACCESS_ALLOWED_ACE_TYPE => {
                    // Inherit-only ACEs do not authorize access to this
                    // directory. Each traversed descendant's effective DACL
                    // is checked separately.
                    if ace.flags().contains(AceFlags::InheritOnly) {
                        continue;
                    }
                    let sid = ace
                        .sid()
                        .ok_or_else(|| insecure("capability ancestor ACE has no verified SID"))?;
                    let sid = sid.to_string();
                    if trusted.contains(&sid) {
                        continue;
                    }
                    if ace.mask().bits() & !SAFE_ANCESTOR_ACCESS_MASK != 0 {
                        return Err(insecure(format!(
                            "capability ancestor grants untrusted write access (SID {sid}, mask 0x{:08X})",
                            ace.mask().bits()
                        )));
                    }
                }
                AceType::ACCESS_DENIED_ACE_TYPE => {}
                _ => {
                    return Err(insecure(
                        "capability ancestor contains an unsupported Windows ACL entry",
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_regular_non_reparse_file(path: &Path) -> Result<(), LifecycleError> {
        let metadata = fs::symlink_metadata(path)?;
        if !metadata.is_file()
            || metadata.file_type().is_symlink()
            || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
        {
            return Err(insecure(
                "capability path is redirected or is not a regular file",
            ));
        }
        Ok(())
    }

    fn read_security_descriptor(
        path: &Path,
    ) -> Result<LocalBox<SecurityDescriptor>, LifecycleError> {
        get_security_info(
            path,
            SeObjectType::SE_FILE_OBJECT,
            SecurityInformation::Owner | SecurityInformation::Dacl,
        )
        .map_err(|error| insecure(format!("could not inspect Windows ACL: {error}")))
    }

    fn insecure(message: impl Into<String>) -> LifecycleError {
        LifecycleError::InsecureProtection(message.into())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn descriptor(sddl: &str) -> LocalBox<SecurityDescriptor> {
            sddl.parse().expect("test security descriptor should parse")
        }

        #[test]
        fn capability_dacl_verification_requires_exact_protected_trusted_entries() {
            let owner: LocalBox<Sid> = "S-1-5-21-100-200-300-400".parse().unwrap();
            let protected = desired_capability_descriptor(&owner).unwrap();
            let sddl = to_sddl(
                &protected,
                SecurityInformation::Owner | SecurityInformation::Dacl,
            )
            .unwrap()
            .to_string_lossy()
            .into_owned();
            let owner_descriptor: LocalBox<SecurityDescriptor> =
                format!("O:{}{}", owner, sddl).parse().unwrap();
            validate_capability_descriptor(&owner_descriptor, &owner).unwrap();

            let inherited = descriptor(&format!(
                "O:{}D:(A;;FA;;;{})(A;;FA;;;S-1-5-18)(A;;FA;;;S-1-5-32-544)",
                owner, owner
            ));
            assert!(super::validate_capability_descriptor(&inherited, &owner).is_err());

            let broad = descriptor(&format!(
                "O:{}D:P(A;;FA;;;{})(A;;FA;;;S-1-5-18)(A;;FA;;;S-1-5-32-544)(A;;FR;;;WD)",
                owner, owner
            ));
            assert!(super::validate_capability_descriptor(&broad, &owner).is_err());
        }

        #[test]
        fn directory_acl_rejects_untrusted_write_but_allows_read_only_access() {
            let owner: LocalBox<Sid> = "S-1-5-21-100-200-300-400".parse().unwrap();
            let trusted_directory = descriptor(&format!(
                "D:(A;;FA;;;{})(A;;FA;;;S-1-5-18)(A;;FA;;;S-1-5-32-544)(A;;GR;;;WD)",
                owner
            ));
            validate_directory_dacl(&trusted_directory, &owner).unwrap();

            let writable_directory = descriptor(&format!(
                "D:(A;;FA;;;{})(A;;FA;;;S-1-5-18)(A;;FA;;;S-1-5-32-544)(A;;GW;;;WD)",
                owner
            ));
            assert!(validate_directory_dacl(&writable_directory, &owner).is_err());
        }

        #[test]
        fn directory_owner_must_be_trusted_to_change_its_dacl() {
            let owner: LocalBox<Sid> = "S-1-5-21-100-200-300-400".parse().unwrap();
            let trusted_ancestor = descriptor(&format!(
                "O:{}D:(A;;FA;;;{})(A;;FA;;;S-1-5-18)(A;;FA;;;S-1-5-32-544)",
                TRUSTED_INSTALLER_SID, owner
            ));
            validate_directory_owner(&trusted_ancestor, &owner, false).unwrap();
            validate_directory_dacl(&trusted_ancestor, &owner).unwrap();

            let untrusted_owner = descriptor(&format!(
                "O:S-1-5-21-900-800-700-600D:(A;;FA;;;{})(A;;FA;;;S-1-5-18)(A;;FA;;;S-1-5-32-544)",
                owner
            ));
            assert!(validate_directory_owner(&untrusted_owner, &owner, false).is_err());
        }

        #[test]
        fn directory_owner_preparation_accepts_only_already_trusted_protection() {
            let system: LocalBox<Sid> = SYSTEM_SID.parse().unwrap();
            let admin_owned = descriptor("O:BAD:P(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)");
            assert!(validate_directory_owner(&admin_owned, &system, true).is_err());
            validate_owner_preparation(&admin_owned, &system).unwrap();
            let foreign_owned =
                descriptor("O:S-1-5-21-900-800-700-600D:P(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)");
            assert!(validate_owner_preparation(&foreign_owned, &system).is_err());
            let writable = descriptor("O:BAD:(A;;FA;;;SY)(A;;FA;;;BA)(A;;GW;;;WD)");
            assert!(validate_owner_preparation(&writable, &system).is_err());
        }

        #[test]
        fn elevated_file_owner_is_established_but_foreign_owner_is_rejected() {
            let system: LocalBox<Sid> = SYSTEM_SID.parse().unwrap();
            let admin_owned = descriptor("O:BAD:P(A;;FA;;;SY)(A;;FA;;;BA)");
            assert!(validate_capability_descriptor(&admin_owned, &system).is_err());
            assert!(owner_needs_establishment(&admin_owned, &system).unwrap());
            let system_owned = descriptor("O:SYD:P(A;;FA;;;SY)(A;;FA;;;BA)");
            assert!(!owner_needs_establishment(&system_owned, &system).unwrap());
            let foreign_owned = descriptor("O:S-1-5-21-900-800-700-600D:P(A;;FA;;;SY)(A;;FA;;;BA)");
            assert!(owner_needs_establishment(&foreign_owned, &system).is_err());
        }
    }
}

/// Generates and stores a fresh per-instance capability. Failure to obtain
/// cryptographic randomness or verify protection never creates a fallback key.
pub fn create_capability(
    path: &Path,
    owner: &OwnerIdentity,
    random: &dyn RandomSource,
    storage: &dyn CapabilityStorage,
) -> Result<Capability, LifecycleError> {
    let mut bytes = [0_u8; CONTROL_CAPABILITY_BYTES];
    if let Err(error) = random.fill(&mut bytes) {
        bytes.zeroize();
        return Err(error);
    }
    if bytes.iter().all(|byte| *byte == 0) {
        bytes.zeroize();
        return Err(LifecycleError::RandomnessFailed);
    }
    let capability = Capability::from_bytes(bytes);
    storage.store_new(path, &capability, owner)?;
    Ok(capability)
}

pub fn verify_client_owner(
    client_owner: &OwnerIdentity,
    live_instance_owner: &OwnerIdentity,
) -> Result<(), LifecycleError> {
    client_owner.validate()?;
    live_instance_owner.validate()?;
    if client_owner == live_instance_owner {
        Ok(())
    } else {
        Err(LifecycleError::WrongOwner)
    }
}

pub fn server_proof(
    capability: &Capability,
    challenge: &ControlChallenge,
) -> Result<[u8; 32], LifecycleError> {
    challenge.validate()?;
    calculate_proof(capability, &base_fields(challenge, b"server-proof"))
}

pub fn client_request_proof(
    capability: &Capability,
    challenge: &ControlChallenge,
    operation: &ControlOperation,
    arguments: &[u8],
) -> Result<[u8; 32], LifecycleError> {
    validate_control_payload(operation.as_str(), arguments)?;
    challenge.validate()?;
    let mut fields = base_fields(challenge, b"client-request");
    fields.push(operation.as_str().as_bytes().to_vec());
    fields.push(arguments.to_vec());
    calculate_proof(capability, &fields)
}

pub fn server_response_proof(
    capability: &Capability,
    challenge: &ControlChallenge,
    operation: &ControlOperation,
    arguments: &[u8],
    result: &[u8],
) -> Result<[u8; 32], LifecycleError> {
    validate_control_payload(operation.as_str(), arguments)?;
    validate_size(result.len())?;
    challenge.validate()?;
    let mut fields = base_fields(challenge, b"server-response");
    fields.push(operation.as_str().as_bytes().to_vec());
    fields.push(arguments.to_vec());
    fields.push(result.to_vec());
    calculate_proof(capability, &fields)
}

pub fn verify_proof(expected: &[u8; 32], supplied: &[u8; 32]) -> Result<(), LifecycleError> {
    if expected.ct_eq(supplied).unwrap_u8() == 1 {
        Ok(())
    } else {
        Err(LifecycleError::AuthenticationFailed)
    }
}

/// Bounded lifetime replay guard. Used server nonces are retained for the whole
/// instance lifetime; saturation fails closed rather than evicting replay data.
#[derive(Default)]
pub struct ChallengeRegistry {
    used_server_nonces: Mutex<HashSet<[u8; CONTROL_NONCE_BYTES]>>,
}

impl ChallengeRegistry {
    pub fn open_session(
        &self,
        challenge: ControlChallenge,
        deadline: Deadline,
        clock: &dyn MonotonicClock,
    ) -> Result<SingleRequestSession, LifecycleError> {
        challenge.validate()?;
        if deadline.is_expired(clock) {
            return Err(LifecycleError::DeadlineExpired {
                phase: "control challenge",
            });
        }
        let mut used = self.used_server_nonces.lock().map_err(|_| {
            LifecycleError::UnsafeOwnership("control replay guard was poisoned".to_owned())
        })?;
        if used.contains(&challenge.server_nonce) {
            return Err(LifecycleError::AuthenticationFailed);
        }
        if used.len() >= MAX_USED_CONTROL_CHALLENGES {
            return Err(LifecycleError::UnsafeOwnership(
                "control replay guard reached its bounded capacity".to_owned(),
            ));
        }
        used.insert(challenge.server_nonce);
        Ok(SingleRequestSession {
            challenge,
            deadline,
            consumed: false,
        })
    }
}

/// One-connection request validator. A challenge is consumed on the first
/// attempt, whether valid or not, and cannot authorize a second request.
pub struct SingleRequestSession {
    challenge: ControlChallenge,
    deadline: Deadline,
    consumed: bool,
}

impl SingleRequestSession {
    pub fn verify_request(
        &mut self,
        capability: &Capability,
        operation: &ControlOperation,
        arguments: &[u8],
        proof: &[u8; 32],
        clock: &dyn MonotonicClock,
    ) -> Result<(), LifecycleError> {
        if self.consumed {
            return Err(LifecycleError::ChallengeConsumed);
        }
        self.consumed = true;
        if self.deadline.is_expired(clock) {
            return Err(LifecycleError::DeadlineExpired {
                phase: "control challenge",
            });
        }
        let mode_allows_operation = match operation {
            ControlOperation::Readiness => true,
            ControlOperation::GracefulStop => self.challenge.context.mode == CoreMode::UserDaemon,
            ControlOperation::ServiceShutdown => {
                self.challenge.context.mode == CoreMode::SystemService
            }
        };
        if !mode_allows_operation {
            return Err(LifecycleError::ModeNotControllable(
                self.challenge.context.mode.to_string(),
            ));
        }
        let expected = client_request_proof(capability, &self.challenge, operation, arguments)?;
        verify_proof(&expected, proof)
    }

    pub fn response_proof(
        &self,
        capability: &Capability,
        operation: &ControlOperation,
        arguments: &[u8],
        result: &[u8],
        clock: &dyn MonotonicClock,
    ) -> Result<[u8; 32], LifecycleError> {
        if self.deadline.is_expired(clock) {
            return Err(LifecycleError::DeadlineExpired {
                phase: "control response",
            });
        }
        server_response_proof(capability, &self.challenge, operation, arguments, result)
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlMessage {
    ClientHello {
        protocol_version: u8,
        instance_id: Uuid,
        client_nonce: [u8; CONTROL_NONCE_BYTES],
    },
    ServerChallenge {
        protocol_version: u8,
        instance_id: Uuid,
        owner: OwnerIdentity,
        mode: CoreMode,
        server_nonce: [u8; CONTROL_NONCE_BYTES],
        proof: [u8; 32],
    },
    ClientRequest {
        operation: String,
        arguments: Vec<u8>,
        proof: [u8; 32],
    },
    ServerResponse {
        operation: String,
        result: Vec<u8>,
        proof: [u8; 32],
    },
}

pub fn encode_control_message(message: &ControlMessage) -> Result<Vec<u8>, LifecycleError> {
    let body = serde_json::to_vec(message)?;
    validate_size(body.len())?;
    let length = u32::try_from(body.len()).map_err(|_| LifecycleError::MessageTooLarge {
        limit: MAX_CONTROL_MESSAGE_BYTES,
    })?;
    let mut frame = Vec::with_capacity(FRAME_HEADER_BYTES + body.len());
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(&body);
    Ok(frame)
}

pub async fn read_control_message<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
    deadline: Deadline,
    clock: &dyn MonotonicClock,
) -> Result<ControlMessage, LifecycleError> {
    use tokio::io::AsyncReadExt;

    let remaining = remaining(deadline, clock, "control read")?;
    let read = async {
        let mut header = [0_u8; FRAME_HEADER_BYTES];
        reader.read_exact(&mut header).await?;
        let length = u32::from_be_bytes(header) as usize;
        validate_size(length).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "control frame exceeds size limit",
            )
        })?;
        let mut body = vec![0_u8; length];
        reader.read_exact(&mut body).await?;
        Ok::<_, io::Error>(body)
    };
    let body = tokio::time::timeout(remaining, read).await.map_err(|_| {
        LifecycleError::DeadlineExpired {
            phase: "control read",
        }
    })??;
    serde_json::from_slice(&body).map_err(Into::into)
}

pub async fn write_control_message<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    message: &ControlMessage,
    deadline: Deadline,
    clock: &dyn MonotonicClock,
) -> Result<(), LifecycleError> {
    use tokio::io::AsyncWriteExt;

    let frame = encode_control_message(message)?;
    let remaining = remaining(deadline, clock, "control write")?;
    tokio::time::timeout(remaining, async {
        writer.write_all(&frame).await?;
        writer.flush().await
    })
    .await
    .map_err(|_| LifecycleError::DeadlineExpired {
        phase: "control write",
    })??;
    Ok(())
}

/// Serves one authenticated request per loopback connection. The capability is
/// owner-protected and instance-scoped; the TCP peer identity is never trusted.
pub async fn serve_control(
    listener: tokio::net::TcpListener,
    context: ControlContext,
    capability: Capability,
    lifecycle: super::CoreLifecycle,
    stop_sender: tokio::sync::oneshot::Sender<()>,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), LifecycleError> {
    let mut stop_sender = Some(stop_sender);
    let registry = ChallengeRegistry::default();
    loop {
        let accepted = tokio::select! {
            _ = &mut shutdown => return Ok(()),
            result = listener.accept() => result?,
        };
        let (mut stream, peer) = accepted;
        if !peer.ip().is_loopback() {
            continue;
        }
        // Shutdown owns the in-flight exchange too. Dropping this future only
        // cancels socket I/O; no worker or native mutation is detached.
        let should_stop = tokio::select! {
            biased;
            _ = &mut shutdown => return Ok(()),
            result = handle_control_connection(
                &mut stream, &context, &capability, &lifecycle, &registry
            ) => result.unwrap_or(false),
        };
        if should_stop && let Some(sender) = stop_sender.take() {
            let _ = sender.send(());
        }
    }
}

async fn handle_control_connection(
    stream: &mut tokio::net::TcpStream,
    context: &ControlContext,
    capability: &Capability,
    lifecycle: &super::CoreLifecycle,
    registry: &ChallengeRegistry,
) -> Result<bool, LifecycleError> {
    let clock = super::SystemMonotonicClock::default();
    let deadline = Deadline::after(&clock, super::STOP_DISPATCH_BUDGET);
    let hello = read_control_message(stream, deadline, &clock).await?;
    let ControlMessage::ClientHello {
        protocol_version,
        instance_id,
        client_nonce,
    } = hello
    else {
        return Err(LifecycleError::AuthenticationFailed);
    };
    if protocol_version != CONTROL_PROTOCOL_VERSION || instance_id != context.instance_id {
        return Err(LifecycleError::AuthenticationFailed);
    }
    let challenge =
        ControlChallenge::answer_client_hello(context.clone(), client_nonce, &OsRandomSource)?;
    let proof = server_proof(capability, &challenge)?;
    write_control_message(
        stream,
        &ControlMessage::ServerChallenge {
            protocol_version: CONTROL_PROTOCOL_VERSION,
            instance_id: context.instance_id,
            owner: context.owner.clone(),
            mode: context.mode,
            server_nonce: challenge.server_nonce,
            proof,
        },
        deadline,
        &clock,
    )
    .await?;

    let mut session = registry.open_session(challenge.clone(), deadline, &clock)?;
    let request = read_control_message(stream, deadline, &clock).await?;
    let ControlMessage::ClientRequest {
        operation,
        arguments,
        proof,
    } = request
    else {
        return Err(LifecycleError::AuthenticationFailed);
    };
    let operation = parse_operation(&operation)?;
    session.verify_request(capability, &operation, &arguments, &proof, &clock)?;

    let (result, should_stop) = match operation {
        ControlOperation::Readiness => {
            let phase = lifecycle.phase();
            let result = match (arguments.as_slice(), phase) {
                (b"prepared", super::LifecyclePhase::Starting) => b"prepared".to_vec(),
                (b"ready", super::LifecyclePhase::Ready) => b"ready".to_vec(),
                _ => b"not-ready".to_vec(),
            };
            (result, false)
        }
        ControlOperation::GracefulStop | ControlOperation::ServiceShutdown => {
            (b"accepted".to_vec(), true)
        }
    };
    let response_proof =
        session.response_proof(capability, &operation, &arguments, &result, &clock)?;
    write_control_message(
        stream,
        &ControlMessage::ServerResponse {
            operation: operation.as_str().to_owned(),
            result,
            proof: response_proof,
        },
        deadline,
        &clock,
    )
    .await?;
    Ok(should_stop)
}

fn parse_operation(value: &str) -> Result<ControlOperation, LifecycleError> {
    match value {
        "readiness" => Ok(ControlOperation::Readiness),
        "graceful-stop" => Ok(ControlOperation::GracefulStop),
        "service-shutdown" => Ok(ControlOperation::ServiceShutdown),
        _ => Err(LifecycleError::AuthenticationFailed),
    }
}

/// Authenticates the server and request result over one bounded loopback
/// connection. Caller-side owner verification must be performed before loading
/// a capability from disk.
pub async fn request_control(
    endpoint: LoopbackControlEndpoint,
    context: &ControlContext,
    client_owner: &OwnerIdentity,
    capability: &Capability,
    request: &ControlOperationRequest<'_>,
    deadline: Deadline,
    clock: &dyn MonotonicClock,
) -> Result<Vec<u8>, LifecycleError> {
    // Service readiness is read-only and may be observed by an administrator
    // who can read the service owner's protected capability. This exception
    // never authorizes either shutdown operation or a user-daemon request.
    if context.mode != CoreMode::SystemService || *request.operation != ControlOperation::Readiness
    {
        verify_client_owner(client_owner, &context.owner)?;
    }
    validate_control_payload(request.operation.as_str(), request.arguments)?;
    if deadline.is_expired(clock) {
        return Err(LifecycleError::DeadlineExpired {
            phase: "control connection",
        });
    }
    let remaining = deadline.remaining(clock);
    let mut stream = tokio::time::timeout(
        remaining,
        tokio::net::TcpStream::connect(endpoint.address()),
    )
    .await
    .map_err(|_| LifecycleError::DeadlineExpired {
        phase: "control connect",
    })??;
    stream.set_nodelay(true)?;

    let mut client_nonce = [0_u8; CONTROL_NONCE_BYTES];
    OsRandomSource.fill(&mut client_nonce)?;
    if client_nonce.iter().all(|byte| *byte == 0) {
        return Err(LifecycleError::RandomnessFailed);
    }
    write_control_message(
        &mut stream,
        &ControlMessage::ClientHello {
            protocol_version: CONTROL_PROTOCOL_VERSION,
            instance_id: context.instance_id,
            client_nonce,
        },
        deadline,
        clock,
    )
    .await?;

    let message = read_control_message(&mut stream, deadline, clock).await?;
    let ControlMessage::ServerChallenge {
        protocol_version,
        instance_id,
        owner,
        mode,
        server_nonce,
        proof,
    } = message
    else {
        return Err(LifecycleError::AuthenticationFailed);
    };
    if protocol_version != CONTROL_PROTOCOL_VERSION
        || instance_id != context.instance_id
        || owner != context.owner
        || mode != context.mode
    {
        return Err(LifecycleError::AuthenticationFailed);
    }
    let challenge = ControlChallenge {
        protocol_version,
        context: context.clone(),
        client_nonce,
        server_nonce,
    };
    challenge.validate()?;
    let expected_server_proof = server_proof(capability, &challenge)?;
    verify_proof(&expected_server_proof, &proof)?;

    let request_proof =
        client_request_proof(capability, &challenge, request.operation, request.arguments)?;
    write_control_message(
        &mut stream,
        &ControlMessage::ClientRequest {
            operation: request.operation.as_str().to_owned(),
            arguments: request.arguments.to_vec(),
            proof: request_proof,
        },
        deadline,
        clock,
    )
    .await?;

    let response = read_control_message(&mut stream, deadline, clock).await?;
    let ControlMessage::ServerResponse {
        operation: response_operation,
        result,
        proof,
    } = response
    else {
        return Err(LifecycleError::AuthenticationFailed);
    };
    if response_operation != request.operation.as_str() {
        return Err(LifecycleError::AuthenticationFailed);
    }
    let expected_response_proof = server_response_proof(
        capability,
        &challenge,
        request.operation,
        request.arguments,
        &result,
    )?;
    verify_proof(&expected_response_proof, &proof)?;
    Ok(result)
}

#[derive(Serialize)]
struct OwnerTranscript<'a> {
    owner: &'a OwnerIdentity,
    mode: CoreMode,
}

fn base_fields(challenge: &ControlChallenge, role: &[u8]) -> Vec<Vec<u8>> {
    let owner = serde_json::to_vec(&OwnerTranscript {
        owner: &challenge.context.owner,
        mode: challenge.context.mode,
    })
    .expect("owner transcript serialization is infallible for fixed data");
    vec![
        vec![CONTROL_PROTOCOL_VERSION],
        challenge.client_nonce.to_vec(),
        challenge.server_nonce.to_vec(),
        challenge.context.instance_id.as_bytes().to_vec(),
        owner,
        role.to_vec(),
    ]
}

fn calculate_proof(
    capability: &Capability,
    fields: &[Vec<u8>],
) -> Result<[u8; 32], LifecycleError> {
    let mut mac = HmacSha256::new_from_slice(capability.bytes())
        .map_err(|_| LifecycleError::AuthenticationFailed)?;
    for field in fields {
        let length = u32::try_from(field.len()).map_err(|_| LifecycleError::MessageTooLarge {
            limit: MAX_CONTROL_MESSAGE_BYTES,
        })?;
        mac.update(&length.to_be_bytes());
        mac.update(field);
    }
    Ok(mac.finalize().into_bytes().into())
}

fn validate_control_payload(operation: &str, arguments: &[u8]) -> Result<(), LifecycleError> {
    if operation.is_empty() || operation.len() > 256 {
        return Err(LifecycleError::UnsafeOwnership(
            "control operation name has an invalid length".to_owned(),
        ));
    }
    validate_size(arguments.len())
}

fn validate_size(size: usize) -> Result<(), LifecycleError> {
    if size > MAX_CONTROL_MESSAGE_BYTES {
        Err(LifecycleError::MessageTooLarge {
            limit: MAX_CONTROL_MESSAGE_BYTES,
        })
    } else {
        Ok(())
    }
}

fn remaining(
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

#[cfg(unix)]
fn validate_unix_private_directory(path: &Path) -> Result<(), LifecycleError> {
    use std::os::unix::fs::MetadataExt;

    if !path.is_absolute() {
        return Err(LifecycleError::InsecureProtection(
            "capability directory must be absolute".to_owned(),
        ));
    }
    let effective_uid = rustix::process::geteuid().as_raw();
    let mut ancestors: Vec<PathBuf> = path.ancestors().map(Path::to_path_buf).collect();
    ancestors.reverse();
    for ancestor in &ancestors {
        let metadata = fs::symlink_metadata(ancestor)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(LifecycleError::InsecureProtection(format!(
                "{} is redirected or is not a directory",
                ancestor.display()
            )));
        }
        let mode = metadata.mode();
        let group_or_other_write = mode & 0o022 != 0;
        let sticky_directory = mode & 0o1000 != 0;
        if group_or_other_write && !sticky_directory {
            return Err(LifecycleError::InsecureProtection(format!(
                "{} is writable by other users",
                ancestor.display()
            )));
        }
    }
    let parent_metadata = fs::symlink_metadata(path)?;
    if parent_metadata.uid() != effective_uid || parent_metadata.mode() & 0o077 != 0 {
        return Err(LifecycleError::InsecureProtection(
            "capability directory must be owned by the current user with owner-only permissions"
                .to_owned(),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn validate_unix_private_file(path: &Path) -> Result<(), LifecycleError> {
    use std::os::unix::fs::MetadataExt;
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(LifecycleError::InsecureProtection(
            "capability file is redirected or not a regular file".to_owned(),
        ));
    }
    let effective_uid = rustix::process::geteuid().as_raw();
    if metadata.uid() != effective_uid || metadata.mode() & 0o777 != 0o600 {
        return Err(LifecycleError::InsecureProtection(
            "capability file must be current-user-owned with mode 0600".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn reject_redirected_path(path: &Path) -> Result<(), LifecycleError> {
    for ancestor in path.ancestors() {
        match fs::symlink_metadata(ancestor) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(LifecycleError::InsecureProtection(format!(
                    "{} is a symlink",
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

fn validate_owner_kind(owner: &OwnerIdentity) -> Result<(), LifecycleError> {
    owner.validate()?;
    #[cfg(unix)]
    match owner {
        OwnerIdentity::UnixUid(uid) if *uid == rustix::process::geteuid().as_raw() => {}
        OwnerIdentity::UnixUid(_) => return Err(LifecycleError::WrongOwner),
        _ => {
            return Err(LifecycleError::InsecureProtection(
                "Unix capability storage requires a verified UID".to_owned(),
            ));
        }
    }
    #[cfg(windows)]
    if !matches!(owner, OwnerIdentity::WindowsSid(_)) {
        return Err(LifecycleError::InsecureProtection(
            "Windows capability storage requires a verified SID".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    };

    struct FixedRandom {
        fill: u8,
        fail: AtomicBool,
    }

    impl RandomSource for FixedRandom {
        fn fill(&self, destination: &mut [u8]) -> Result<(), LifecycleError> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(LifecycleError::RandomnessFailed);
            }
            destination.fill(self.fill);
            Ok(())
        }
    }

    #[derive(Clone, Default)]
    struct FakeClock(Arc<AtomicU64>);

    impl MonotonicClock for FakeClock {
        fn now(&self) -> Duration {
            Duration::from_nanos(self.0.load(Ordering::SeqCst))
        }
    }

    fn new_session(
        challenge: ControlChallenge,
        registry: &ChallengeRegistry,
        clock: &FakeClock,
    ) -> Result<SingleRequestSession, LifecycleError> {
        registry.open_session(
            challenge,
            Deadline::after(clock, Duration::from_secs(10)),
            clock,
        )
    }

    fn challenge(mode: CoreMode, suffix: u8) -> ControlChallenge {
        ControlChallenge {
            protocol_version: CONTROL_PROTOCOL_VERSION,
            context: ControlContext {
                instance_id: Uuid::new_v4(),
                owner: test_owner(),
                mode,
            },
            client_nonce: [suffix; CONTROL_NONCE_BYTES],
            server_nonce: [suffix.wrapping_add(1); CONTROL_NONCE_BYTES],
        }
    }

    #[cfg(unix)]
    fn test_owner() -> OwnerIdentity {
        OwnerIdentity::UnixUid(rustix::process::geteuid().as_raw())
    }

    #[cfg(windows)]
    fn test_owner() -> OwnerIdentity {
        OwnerIdentity::WindowsSid("S-1-5-18".to_owned())
    }

    #[cfg(not(any(unix, windows)))]
    fn test_owner() -> OwnerIdentity {
        OwnerIdentity::Other("test-owner".to_owned())
    }

    #[test]
    fn nonce_hmac_proofs_bind_role_operation_and_response() {
        let capability = Capability::from_bytes([7; CONTROL_CAPABILITY_BYTES]);
        let challenge = challenge(CoreMode::UserDaemon, 1);
        let operation = ControlOperation::GracefulStop;
        let request = client_request_proof(&capability, &challenge, &operation, b"close").unwrap();
        let clock = FakeClock::default();
        let registry = ChallengeRegistry::default();
        let mut session = new_session(challenge.clone(), &registry, &clock).unwrap();
        session
            .verify_request(&capability, &operation, b"close", &request, &clock)
            .unwrap();
        assert!(matches!(
            session.verify_request(&capability, &operation, b"close", &request, &clock),
            Err(LifecycleError::ChallengeConsumed)
        ));

        let response = session
            .response_proof(&capability, &operation, b"close", b"ack", &clock)
            .unwrap();
        let response_changed =
            server_response_proof(&capability, &challenge, &operation, b"close", b"different")
                .unwrap();
        assert!(verify_proof(&response, &response_changed).is_err());
        let operation_changed = client_request_proof(
            &capability,
            &challenge,
            &ControlOperation::Readiness,
            b"close",
        )
        .unwrap();
        assert!(verify_proof(&request, &operation_changed).is_err());
        assert!(verify_proof(&server_proof(&capability, &challenge).unwrap(), &request).is_err());
    }

    #[test]
    fn wrong_os_account_and_foreground_stop_are_rejected() {
        let wrong = match test_owner() {
            OwnerIdentity::UnixUid(uid) => OwnerIdentity::UnixUid(uid.saturating_add(1)),
            OwnerIdentity::WindowsSid(_) => OwnerIdentity::WindowsSid("S-1-5-19".to_owned()),
            OwnerIdentity::Other(_) => OwnerIdentity::Other("other".to_owned()),
        };
        assert!(matches!(
            verify_client_owner(&wrong, &test_owner()),
            Err(LifecycleError::WrongOwner)
        ));
        let capability = Capability::from_bytes([2; CONTROL_CAPABILITY_BYTES]);
        let challenge = challenge(CoreMode::Foreground, 4);
        let operation = ControlOperation::GracefulStop;
        let proof = client_request_proof(&capability, &challenge, &operation, b"").unwrap();
        let clock = FakeClock::default();
        let registry = ChallengeRegistry::default();
        let mut session = new_session(challenge, &registry, &clock).unwrap();
        assert!(matches!(
            session.verify_request(&capability, &operation, b"", &proof, &clock),
            Err(LifecycleError::ModeNotControllable(_))
        ));
    }

    #[tokio::test]
    async fn control_shutdown_joins_an_incomplete_authenticated_exchange() {
        let context = challenge(CoreMode::UserDaemon, 91).context;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let lifecycle = super::super::CoreLifecycle::ready();
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let (shutdown, shutdown_receiver) = tokio::sync::oneshot::channel();
        let mut server = tokio::spawn(serve_control(
            listener,
            context.clone(),
            Capability::from_bytes([91; CONTROL_CAPABILITY_BYTES]),
            lifecycle.clone(),
            stop,
            shutdown_receiver,
        ));
        let clock = super::super::SystemMonotonicClock::default();
        let deadline = Deadline::after(&clock, Duration::from_secs(3));
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        write_control_message(
            &mut client,
            &ControlMessage::ClientHello {
                protocol_version: CONTROL_PROTOCOL_VERSION,
                instance_id: context.instance_id,
                client_nonce: [91; CONTROL_NONCE_BYTES],
            },
            deadline,
            &clock,
        )
        .await
        .unwrap();
        // Receiving the challenge proves the server owns this incomplete
        // exchange; no sleep or scheduler-order assumption is needed.
        assert!(matches!(
            read_control_message(&mut client, deadline, &clock)
                .await
                .unwrap(),
            ControlMessage::ServerChallenge { .. }
        ));
        shutdown.send(()).unwrap();
        let joined = tokio::time::timeout(Duration::from_secs(1), &mut server).await;
        if joined.is_err() {
            server.abort();
            let _ = server.await;
        }
        joined
            .expect("shutdown must cancel the pending exchange")
            .unwrap()
            .unwrap();
        assert!(lifecycle.is_ready());
        assert!(matches!(
            stopped.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Closed)
        ));
    }

    #[tokio::test]
    async fn service_readiness_proof_never_grants_user_daemon_or_cross_owner_shutdown_authority() {
        let context = challenge(CoreMode::SystemService, 92).context;
        let other = match &context.owner {
            OwnerIdentity::UnixUid(uid) => OwnerIdentity::UnixUid(uid.saturating_add(1)),
            OwnerIdentity::WindowsSid(_) => OwnerIdentity::WindowsSid("S-1-5-19".into()),
            OwnerIdentity::Other(_) => OwnerIdentity::Other("observer".into()),
        };
        let capability = Capability::from_bytes([93; CONTROL_CAPABILITY_BYTES]);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = LoopbackControlEndpoint::new(listener.local_addr().unwrap()).unwrap();
        let lifecycle = super::super::CoreLifecycle::ready();
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let (shutdown, shutdown_receiver) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(serve_control(
            listener,
            context.clone(),
            Capability::from_bytes([93; CONTROL_CAPABILITY_BYTES]),
            lifecycle.clone(),
            stop,
            shutdown_receiver,
        ));
        let clock = super::super::SystemMonotonicClock::default();
        let deadline = Deadline::after(&clock, Duration::from_secs(3));
        let ready = request_control(
            endpoint,
            &context,
            &other,
            &capability,
            &ControlOperationRequest {
                operation: &ControlOperation::Readiness,
                arguments: b"ready",
            },
            deadline,
            &clock,
        )
        .await
        .unwrap();
        assert_eq!(ready, b"ready");
        for operation in [
            ControlOperation::GracefulStop,
            ControlOperation::ServiceShutdown,
        ] {
            assert!(matches!(
                request_control(
                    endpoint,
                    &context,
                    &other,
                    &capability,
                    &ControlOperationRequest {
                        operation: &operation,
                        arguments: b"close"
                    },
                    deadline,
                    &clock
                )
                .await,
                Err(LifecycleError::WrongOwner)
            ));
        }
        assert!(
            request_control(
                endpoint,
                &context,
                &context.owner,
                &capability,
                &ControlOperationRequest {
                    operation: &ControlOperation::GracefulStop,
                    arguments: b"close"
                },
                deadline,
                &clock
            )
            .await
            .is_err()
        );
        assert!(matches!(
            stopped.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        assert!(lifecycle.is_ready());
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    #[test]
    fn a_server_challenge_cannot_be_reused_on_another_connection() {
        let registry = ChallengeRegistry::default();
        let clock = FakeClock::default();
        let challenge = challenge(CoreMode::UserDaemon, 20);
        assert!(new_session(challenge.clone(), &registry, &clock).is_ok());
        assert!(matches!(
            new_session(challenge, &registry, &clock),
            Err(LifecycleError::AuthenticationFailed)
        ));
    }

    #[test]
    fn generated_nonces_are_fresh_and_randomness_failure_has_no_fallback() {
        let random = FixedRandom {
            fill: 9,
            fail: AtomicBool::new(false),
        };
        let context = ControlContext {
            instance_id: Uuid::new_v4(),
            owner: test_owner(),
            mode: CoreMode::UserDaemon,
        };
        let challenge = ControlChallenge::generate(context, &random).unwrap();
        assert_eq!(challenge.client_nonce, [9; CONTROL_NONCE_BYTES]);
        assert_eq!(challenge.server_nonce, [9; CONTROL_NONCE_BYTES]);
        random.fail.store(true, Ordering::SeqCst);
        let random_error = ControlChallenge::generate(
            ControlContext {
                instance_id: Uuid::new_v4(),
                owner: test_owner(),
                mode: CoreMode::UserDaemon,
            },
            &random,
        );
        assert!(matches!(
            random_error,
            Err(LifecycleError::RandomnessFailed)
        ));
        let temp = std::env::temp_dir().join(format!("kakune-secret-{}.key", Uuid::new_v4()));
        assert!(matches!(
            create_capability(&temp, &test_owner(), &random, &OwnerProtectedFileStorage),
            Err(LifecycleError::RandomnessFailed)
        ));
        assert!(!temp.exists());
    }

    #[test]
    fn control_frames_obey_the_hard_size_cap() {
        let message = ControlMessage::ClientRequest {
            operation: "test".to_owned(),
            arguments: vec![b'x'; MAX_CONTROL_MESSAGE_BYTES],
            proof: [0; 32],
        };
        assert!(matches!(
            encode_control_message(&message),
            Err(LifecycleError::MessageTooLarge { .. })
        ));
    }

    #[test]
    fn control_endpoint_must_be_bound_loopback_only() {
        let valid: SocketAddr = "127.0.0.1:18702".parse().unwrap();
        assert_eq!(
            LoopbackControlEndpoint::new(valid).unwrap().address(),
            valid
        );
        for invalid in ["0.0.0.0:18702", "192.0.2.1:18702", "127.0.0.1:0"] {
            let address = invalid.parse().unwrap();
            assert!(LoopbackControlEndpoint::new(address).is_err());
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_secret_storage_rejects_malformed_owner_sid_before_file_creation() {
        let path = std::env::temp_dir().join(format!("kakune-secret-{}.key", Uuid::new_v4()));
        let capability = Capability::from_bytes([3; CONTROL_CAPABILITY_BYTES]);
        assert!(matches!(
            OwnerProtectedFileStorage.store_new(
                &path,
                &capability,
                &OwnerIdentity::WindowsSid("not-a-sid".to_owned())
            ),
            Err(LifecycleError::InsecureProtection(_))
        ));
        assert!(!path.exists());
    }

    #[cfg(windows)]
    #[test]
    fn windows_secret_storage_sets_and_verifies_private_acl_before_publication() {
        use std::{fs, io::Write, path::PathBuf};

        use crate::lifecycle::process::{ProcessEvidenceProvider, SystemProcessEvidence};

        let clock = crate::lifecycle::SystemMonotonicClock::default();
        let owner = SystemProcessEvidence::default()
            .observe(
                std::process::id(),
                Deadline::after(&clock, Duration::from_secs(5)),
                &clock,
            )
            .expect("current test process identity should be observable")
            .expect("current test process should exist")
            .owner;
        let OwnerIdentity::WindowsSid(_) = owner else {
            panic!("Windows process observation should return a SID");
        };
        let root = std::env::var_os("USERPROFILE")
            .map(PathBuf::from)
            .expect("Windows test profile path should be set")
            .join(format!(".kakune-capability-test-{}", Uuid::new_v4()));
        fs::create_dir(&root).expect("unique Windows test directory should be created");
        let path = root.join("control.capability");
        let random = FixedRandom {
            fill: 0x33,
            fail: AtomicBool::new(false),
        };

        let outcome = (|| {
            let capability = create_capability(&path, &owner, &random, &OwnerProtectedFileStorage)?;
            let loaded = OwnerProtectedFileStorage.load(&path, &owner)?;
            let challenge = ControlChallenge {
                protocol_version: CONTROL_PROTOCOL_VERSION,
                context: ControlContext {
                    instance_id: Uuid::new_v4(),
                    owner: owner.clone(),
                    mode: CoreMode::UserDaemon,
                },
                client_nonce: [1; CONTROL_NONCE_BYTES],
                server_nonce: [2; CONTROL_NONCE_BYTES],
            };
            assert_eq!(
                server_proof(&capability, &challenge)?,
                server_proof(&loaded, &challenge)?
            );

            // The trailing-byte check is length-based: an extra NUL is still
            // an overlong secret file and must not be accepted.
            let mut file = fs::OpenOptions::new().append(true).open(&path)?;
            file.write_all(&[0])?;
            drop(file);
            assert!(matches!(
                OwnerProtectedFileStorage.load(&path, &owner),
                Err(LifecycleError::MessageTooLarge {
                    limit: CONTROL_CAPABILITY_BYTES
                })
            ));
            Ok::<(), LifecycleError>(())
        })();
        let cleanup = fs::remove_dir_all(&root);
        assert!(
            cleanup.is_ok(),
            "test-owned capability directory should be removed"
        );
        outcome.expect("Windows capability should be protected before publication");
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_capability_path_is_rejected_without_secret_publication() {
        use std::os::unix::fs::symlink;
        let root = std::env::temp_dir().join(format!("kakune-secret-dir-{}", Uuid::new_v4()));
        let link = root.with_extension("link");
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        symlink(&root, &link).unwrap();
        let path = link.join("capability.key");
        let capability = Capability::from_bytes([3; CONTROL_CAPABILITY_BYTES]);
        assert!(matches!(
            OwnerProtectedFileStorage.store_new(&path, &capability, &test_owner()),
            Err(LifecycleError::InsecureProtection(_))
        ));
        assert!(!root.join("capability.key").exists());
        fs::remove_file(&link).unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn owner_only_capability_is_published_after_permission_verification() {
        use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};

        let root = std::env::temp_dir().join(format!("kakune-private-{}", Uuid::new_v4()));
        let mut builder = fs::DirBuilder::new();
        builder.mode(0o700);
        builder.create(&root).unwrap();
        let path = root.join("control.capability");
        let owner = OwnerIdentity::UnixUid(rustix::process::geteuid().as_raw());
        let random = FixedRandom {
            fill: 0x33,
            fail: AtomicBool::new(false),
        };
        let capability = create_capability(&path, &owner, &random, &OwnerProtectedFileStorage)
            .expect("private file should be securely published");
        let metadata = fs::metadata(&path).unwrap();
        assert_eq!(metadata.uid(), rustix::process::geteuid().as_raw());
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);

        let loaded = OwnerProtectedFileStorage.load(&path, &owner).unwrap();
        let challenge = ControlChallenge {
            protocol_version: CONTROL_PROTOCOL_VERSION,
            context: ControlContext {
                instance_id: Uuid::new_v4(),
                owner,
                mode: CoreMode::UserDaemon,
            },
            client_nonce: [1; CONTROL_NONCE_BYTES],
            server_nonce: [2; CONTROL_NONCE_BYTES],
        };
        assert_eq!(
            server_proof(&capability, &challenge).unwrap(),
            server_proof(&loaded, &challenge).unwrap()
        );
        drop(loaded);
        drop(capability);
        fs::remove_dir_all(root).unwrap();
    }
}
