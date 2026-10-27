use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Take, Write},
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};

use crate::initialization::{
    CredentialAccess, CredentialAccessError, CredentialReference, CredentialSecret,
};
use serde::{Deserialize, Serialize};

const CLI_KEYRING_SERVICE: &str = "dev.kakune.cli";
const MAX_CONTEXT_FILE_BYTES: u64 = 1_048_576;
const MAX_CONTEXTS: usize = 1_000;
const CONTEXT_LOCK_WAIT: Duration = Duration::from_secs(5);
const CONTEXT_LOCK_POLL: Duration = Duration::from_millis(25);

/// Production adapter for the existing CLI environment and OS-keyring
/// credential conventions.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemCredentialAccess;

impl CredentialAccess for SystemCredentialAccess {
    fn resolve_reference(
        &self,
        reference: Option<&str>,
    ) -> Result<CredentialReference, CredentialAccessError> {
        let reference = reference.unwrap_or("env:KAKUNE_TOKEN");
        if reference.is_empty()
            || reference.strip_prefix("env:").is_some_and(str::is_empty)
            || reference
                .strip_prefix("keychain:")
                .is_some_and(str::is_empty)
        {
            return Err(CredentialAccessError::InvalidReference);
        }
        CredentialReference::new(reference.to_string())
    }

    fn write_secure(
        &self,
        reference: &CredentialReference,
        secret: &CredentialSecret,
    ) -> Result<(), CredentialAccessError> {
        let entry = keyring_entry(reference).ok_or(CredentialAccessError::SecureWriteFailed)?;
        entry
            .set_password(secret.expose_secret())
            .map_err(|_| CredentialAccessError::SecureWriteFailed)
    }

    fn read(
        &self,
        reference: &CredentialReference,
    ) -> Result<CredentialSecret, CredentialAccessError> {
        if let Some(variable) = reference.as_str().strip_prefix("env:") {
            return std::env::var(variable)
                .map(CredentialSecret::new)
                .map_err(|_| CredentialAccessError::Unavailable);
        }

        keyring_entry(reference)
            .and_then(|entry| entry.get_password().ok())
            .map(CredentialSecret::new)
            .ok_or(CredentialAccessError::Unavailable)
    }
}

fn keyring_entry(reference: &CredentialReference) -> Option<keyring::Entry> {
    let account = reference
        .as_str()
        .strip_prefix("keychain:")
        .unwrap_or_else(|| reference.as_str());
    if account.is_empty() || reference.as_str().starts_with("env:") {
        return None;
    }
    keyring::Entry::new(CLI_KEYRING_SERVICE, account).ok()
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionContext {
    pub id: String,
    pub name: String,
    pub endpoint: String,
    pub expected_core_id: Option<String>,
    pub color: Option<String>,
    pub credential_ref: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextFile {
    pub format: String,
    pub exported_at: String,
    pub active_context_id: Option<String>,
    #[serde(default)]
    pub revision: u64,
    pub contexts: Vec<ConnectionContext>,
}

impl Default for ContextFile {
    fn default() -> Self {
        Self {
            format: "kakune-contexts/v1".to_string(),
            exported_at: timestamp().unwrap_or_default(),
            active_context_id: None,
            revision: 0,
            contexts: Vec::new(),
        }
    }
}

impl ContextFile {
    pub fn load(path: &Path) -> Result<Self, String> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let source = read_context_file(path)?;
        let contexts: Self = serde_json::from_slice(&source)
            .map_err(|error| format!("cannot parse {}: {error}", path.display()))?;
        contexts.validate()?;
        Ok(contexts)
    }

    pub fn save(&mut self, path: &Path) -> Result<(), String> {
        self.validate()?;
        self.exported_at = timestamp()?;
        let contents = serialize_context_file(self)?;
        atomic_write(path, &contents, true, true)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.format != "kakune-contexts/v1" {
            return Err("contexts format must be kakune-contexts/v1".to_string());
        }
        if self.contexts.len() > MAX_CONTEXTS {
            return Err(format!(
                "context collections may contain at most {MAX_CONTEXTS} entries"
            ));
        }
        for context in &self.contexts {
            validate_context(context)?;
        }
        for (index, context) in self.contexts.iter().enumerate() {
            if self.contexts[..index]
                .iter()
                .any(|other| other.id == context.id)
            {
                return Err(format!("duplicate context ID {}", context.id));
            }
        }
        if let Some(active) = &self.active_context_id
            && !self.contexts.iter().any(|context| &context.id == active)
        {
            return Err(format!("active context {active} does not exist"));
        }
        Ok(())
    }

    pub(crate) fn ensure_local_context(
        path: &Path,
        core_id: &str,
        endpoint: &str,
        credential_ref: &str,
    ) -> Result<bool, String> {
        let existed = path.exists();
        let (mut contexts, mut document) = if existed {
            let source = fs::read_to_string(path)
                .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
            let document: serde_json::Value = serde_json::from_str(&source)
                .map_err(|error| format!("cannot parse {}: {error}", path.display()))?;
            let contexts: Self = serde_json::from_value(document.clone())
                .map_err(|error| format!("cannot parse {}: {error}", path.display()))?;
            contexts.validate()?;
            (contexts, document)
        } else {
            let contexts = Self::default();
            let document = serde_json::to_value(&contexts)
                .map_err(|error| format!("cannot serialize contexts: {error}"))?;
            (contexts, document)
        };
        if let Some(local) = contexts
            .contexts
            .iter()
            .find(|context| context.id == "local")
        {
            if local
                .expected_core_id
                .as_deref()
                .is_some_and(|expected| expected != core_id)
            {
                return Err("local context is bound to a different installation".to_string());
            }
            return Ok(false);
        }

        let local = ConnectionContext {
            id: "local".to_string(),
            name: "Local Kakune Core".to_string(),
            endpoint: endpoint.to_string(),
            expected_core_id: Some(core_id.to_string()),
            color: None,
            credential_ref: Some(credential_ref.to_string()),
        };
        let select_local = contexts.active_context_id.is_none();
        contexts.contexts.push(local.clone());
        if select_local {
            contexts.active_context_id = Some("local".to_string());
        }
        contexts.validate()?;
        let exported_at = timestamp()?;
        let document_object = document
            .as_object_mut()
            .ok_or_else(|| "contexts document must be a JSON object".to_string())?;
        let document_contexts = document_object
            .get_mut("contexts")
            .and_then(serde_json::Value::as_array_mut)
            .ok_or_else(|| "contexts field must be a JSON array".to_string())?;
        document_contexts.push(
            serde_json::to_value(local)
                .map_err(|error| format!("cannot serialize context: {error}"))?,
        );
        document_object.insert(
            "exportedAt".to_string(),
            serde_json::Value::String(exported_at),
        );
        if select_local {
            document_object.insert(
                "activeContextId".to_string(),
                serde_json::Value::String("local".to_string()),
            );
        }
        let contents = serde_json::to_vec_pretty(&document)
            .map_err(|error| format!("cannot serialize contexts: {error}"))?;
        if existed {
            crate::initialization::replace_file(path, &contents)
                .map_err(|error| format!("cannot publish local context: {error}"))?;
        } else {
            crate::initialization::publish_new_file(path, &contents)
                .map_err(|error| format!("cannot publish local context: {error}"))?;
        }
        Ok(true)
    }
}

/// A bounded, atomic repository for the user's named Core contexts.
///
/// Every writer takes a cross-process lock, rereads the latest collection, and
/// compares its source bytes again before atomic publication. Reads never
/// create a collection or resolve credential references.
#[derive(Clone, Debug)]
pub struct ContextRepository {
    path: PathBuf,
}

impl ContextRepository {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load(&self) -> Result<ContextFile, String> {
        ContextFile::load(&self.path)
    }

    pub fn add(&self, context: ConnectionContext) -> Result<(), String> {
        validate_context(&context)?;
        self.transact(true, |collection| {
            if collection
                .contexts
                .iter()
                .any(|existing| existing.id == context.id)
            {
                return Err(format!("context {} already exists", context.id));
            }
            if collection.contexts.len() == MAX_CONTEXTS {
                return Err(format!(
                    "context collections may contain at most {MAX_CONTEXTS} entries"
                ));
            }
            collection.contexts.push(context);
            Ok(())
        })
    }

    /// Validates the full imported batch before taking the write lock or
    /// publishing any entry. Imported selection and revision are not portable.
    pub fn import(&self, source: &Path) -> Result<(), String> {
        let bytes = read_context_file(source)?;
        let imported: ContextFile = serde_json::from_slice(&bytes)
            .map_err(|error| format!("cannot parse {}: {error}", source.display()))?;
        imported.validate()?;
        self.transact(true, |collection| {
            let mut incoming = std::collections::BTreeSet::new();
            for context in &imported.contexts {
                if !incoming.insert(context.id.as_str()) {
                    return Err(format!(
                        "import contains duplicate context ID {}",
                        context.id
                    ));
                }
                if collection
                    .contexts
                    .iter()
                    .any(|existing| existing.id == context.id)
                {
                    return Err(format!("context {} already exists", context.id));
                }
            }
            if collection
                .contexts
                .len()
                .saturating_add(imported.contexts.len())
                > MAX_CONTEXTS
            {
                return Err(format!(
                    "context collections may contain at most {MAX_CONTEXTS} entries"
                ));
            }
            collection
                .contexts
                .extend(imported.contexts.iter().cloned());
            Ok(())
        })
    }

    pub fn use_context(&self, id: &str) -> Result<(), String> {
        self.transact(false, |collection| {
            if !collection.contexts.iter().any(|context| context.id == id) {
                return Err(format!("context {id} was not found"));
            }
            collection.active_context_id = Some(id.to_owned());
            Ok(())
        })
    }

    pub fn remove(&self, id: &str) -> Result<(), String> {
        self.transact(false, |collection| {
            let original_len = collection.contexts.len();
            collection.contexts.retain(|context| context.id != id);
            if collection.contexts.len() == original_len {
                return Err(format!("context {id} was not found"));
            }
            if collection.active_context_id.as_deref() == Some(id) {
                collection.active_context_id = None;
            }
            Ok(())
        })
    }

    /// Exports one context, preserving only its non-secret metadata and the
    /// collection revision. Destination parents must already exist.
    pub fn export(&self, id: &str, destination: &Path, replace: bool) -> Result<(), String> {
        if same_path(&self.path, destination) {
            return Err(
                "context export destination must differ from the collection file".to_owned(),
            );
        }
        let collection = self.load()?;
        let context = collection
            .contexts
            .iter()
            .find(|context| context.id == id)
            .ok_or_else(|| format!("context {id} was not found"))?;
        let exported = ContextFile {
            format: "kakune-contexts/v1".to_owned(),
            exported_at: timestamp()?,
            active_context_id: None,
            revision: collection.revision,
            contexts: vec![context.clone()],
        };
        let bytes = serialize_context_file(&exported)?;
        atomic_write(destination, &bytes, replace, false)
    }

    fn transact(
        &self,
        create_parent: bool,
        update: impl FnOnce(&mut ContextFile) -> Result<(), String>,
    ) -> Result<(), String> {
        let parent = parent_directory(&self.path);
        if create_parent {
            fs::create_dir_all(parent)
                .map_err(|error| format!("cannot create contexts directory: {error}"))?;
        } else if !parent.is_dir() {
            return Err(format!(
                "context collection {} is unavailable",
                self.path.display()
            ));
        }

        let _lock = CollectionLock::acquire(&self.path)?;
        let before = read_optional_context_file(&self.path)?;
        let mut collection = match &before {
            Some(bytes) => serde_json::from_slice::<ContextFile>(bytes)
                .map_err(|error| format!("cannot parse {}: {error}", self.path.display()))?,
            None => ContextFile::default(),
        };
        collection.validate()?;
        update(&mut collection)?;
        collection.revision = collection
            .revision
            .checked_add(1)
            .ok_or_else(|| "context collection revision is exhausted".to_owned())?;
        collection.exported_at = timestamp()?;
        collection.validate()?;
        let contents = serialize_context_file(&collection)?;

        if read_optional_context_file(&self.path)? != before {
            return Err("context collection changed concurrently; refresh and retry".to_owned());
        }
        atomic_write(&self.path, &contents, before.is_some(), false)
    }
}

struct CollectionLock {
    _file: File,
}

impl CollectionLock {
    fn acquire(collection_path: &Path) -> Result<Self, String> {
        let file_name = collection_path
            .file_name()
            .ok_or_else(|| "contexts path has no file name".to_owned())?;
        let mut lock_name = file_name.to_os_string();
        lock_name.push(".lock");
        let lock_path = collection_path.with_file_name(lock_name);
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .map_err(|error| format!("cannot lock contexts collection: {error}"))?;
        let started = Instant::now();
        loop {
            match file.try_lock() {
                Ok(()) => return Ok(Self { _file: file }),
                Err(std::fs::TryLockError::WouldBlock) => {
                    let elapsed = started.elapsed();
                    if elapsed >= CONTEXT_LOCK_WAIT {
                        return Err(
                            "context collection is busy; retry after the current update finishes"
                                .to_owned(),
                        );
                    }
                    thread::sleep(CONTEXT_LOCK_POLL.min(CONTEXT_LOCK_WAIT - elapsed));
                }
                Err(std::fs::TryLockError::Error(error)) => {
                    return Err(format!("cannot lock contexts collection: {error}"));
                }
            }
        }
    }
}

fn read_optional_context_file(path: &Path) -> Result<Option<Vec<u8>>, String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
            read_context_file(path).map(Some)
        }
        Ok(_) => Err(format!("{} is not a regular context file", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(format!("cannot inspect {}: {error}", path.display())),
    }
}

fn serialize_context_file(contexts: &ContextFile) -> Result<Vec<u8>, String> {
    let contents = serde_json::to_vec_pretty(contexts)
        .map_err(|error| format!("cannot serialize contexts: {error}"))?;
    if contents.len() as u64 > MAX_CONTEXT_FILE_BYTES {
        return Err("context files are limited to 1 MiB".to_owned());
    }
    Ok(contents)
}

fn atomic_write(
    path: &Path,
    contents: &[u8],
    replace: bool,
    create_parent: bool,
) -> Result<(), String> {
    let parent = parent_directory(path);
    if create_parent {
        fs::create_dir_all(parent)
            .map_err(|error| format!("cannot create publication directory: {error}"))?;
    } else if !parent.is_dir() {
        return Err(format!(
            "destination directory {} does not exist",
            parent.display()
        ));
    }
    if !replace && fs::symlink_metadata(path).is_ok() {
        return Err(format!(
            "destination {} already exists; pass --replace to replace it",
            path.display()
        ));
    }

    let mut temporary = None;
    for _ in 0..8 {
        let candidate = parent.join(format!(".kakune-context-{}.tmp", uuid::Uuid::new_v4()));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(mut file) => {
                if let Err(error) = file.write_all(contents).and_then(|()| file.sync_all()) {
                    let _ = fs::remove_file(&candidate);
                    return Err(format!(
                        "cannot prepare atomic context publication: {error}"
                    ));
                }
                temporary = Some(candidate);
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(format!(
                    "cannot create atomic context temporary file: {error}"
                ));
            }
        }
    }
    let temporary =
        temporary.ok_or_else(|| "cannot allocate a unique context temporary file".to_owned())?;
    let publish = if replace {
        fs::rename(&temporary, path)
    } else {
        fs::hard_link(&temporary, path)
    };
    if let Err(error) = publish {
        let _ = fs::remove_file(&temporary);
        return Err(format!(
            "cannot atomically publish {}: {error}",
            path.display()
        ));
    }
    if !replace {
        let _ = fs::remove_file(&temporary);
    }
    Ok(())
}

fn parent_directory(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn same_path(left: &Path, right: &Path) -> bool {
    if left == right {
        return true;
    }
    match (left.canonicalize(), right.canonicalize()) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

pub fn default_contexts_path(data_dir: PathBuf) -> PathBuf {
    data_dir.join("cli").join("contexts.json")
}

fn validate_context(context: &ConnectionContext) -> Result<(), String> {
    if context.id.trim().is_empty()
        || context.id.len() > 256
        || context.id.contains(char::is_control)
    {
        return Err("context ID must be 1-256 characters".to_string());
    }
    if context.name.trim().is_empty()
        || context.name.len() > 200
        || context.name.contains(char::is_control)
    {
        return Err("context name must be 1-200 characters".to_string());
    }
    let endpoint = reqwest::Url::parse(&context.endpoint)
        .map_err(|_| "context endpoint must be a valid HTTP or HTTPS URL".to_string())?;
    if !matches!(endpoint.scheme(), "http" | "https")
        || endpoint.host_str().is_none()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.fragment().is_some()
    {
        return Err("context endpoint must be an HTTP or HTTPS URL".to_string());
    }
    if endpoint.query_pairs().any(|(key, _)| {
        matches!(
            key.to_ascii_lowercase().as_str(),
            "token"
                | "access_token"
                | "api_key"
                | "apikey"
                | "secret"
                | "password"
                | "authorization"
                | "credential"
        )
    }) {
        return Err("context endpoint must not contain credential query parameters".to_string());
    }
    if let Some(expected) = &context.expected_core_id
        && (expected.trim().is_empty()
            || expected.len() > 256
            || expected.contains(char::is_control))
    {
        return Err("expected Core identity must be 1-256 non-control characters".to_string());
    }
    if let Some(color) = &context.color
        && (color.len() != 7
            || !color.starts_with('#')
            || !color[1..].bytes().all(|byte| byte.is_ascii_hexdigit()))
    {
        return Err("context color must be a #RRGGBB value".to_string());
    }
    if context.credential_ref.as_deref().is_some_and(|reference| {
        reference.trim().is_empty()
            || reference.len() > 1_024
            || reference.contains(char::is_whitespace)
            || reference.contains(char::is_control)
    }) {
        return Err(
            "context credentialRef must be a non-secret reference without whitespace".to_string(),
        );
    }
    Ok(())
}

fn read_context_file(path: &Path) -> Result<Vec<u8>, String> {
    let metadata = fs::metadata(path)
        .map_err(|error| format!("cannot inspect {}: {error}", path.display()))?;
    if !metadata.is_file() {
        return Err(format!("{} is not a regular context file", path.display()));
    }
    if metadata.len() > MAX_CONTEXT_FILE_BYTES {
        return Err("context files are limited to 1 MiB".to_string());
    }
    let file =
        File::open(path).map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    let mut capped: Take<File> = file.take(MAX_CONTEXT_FILE_BYTES.saturating_add(1));
    let mut source = Vec::new();
    capped
        .read_to_end(&mut source)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    if source.len() as u64 > MAX_CONTEXT_FILE_BYTES {
        return Err("context files are limited to 1 MiB".to_string());
    }
    Ok(source)
}

fn timestamp() -> Result<String, String> {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{ConnectionContext, ContextFile, SystemCredentialAccess};
    use crate::initialization::{CredentialAccess, CredentialAccessError, CredentialSecret};

    #[test]
    fn credential_adapter_preserves_existing_reference_resolution() {
        let adapter = SystemCredentialAccess;

        assert_eq!(
            adapter
                .resolve_reference(None)
                .expect("missing reference should use the existing fallback")
                .as_str(),
            "env:KAKUNE_TOKEN"
        );
        assert_eq!(
            adapter
                .resolve_reference(Some("env:KAKUNE_TEST_TOKEN"))
                .expect("environment reference should be retained")
                .as_str(),
            "env:KAKUNE_TEST_TOKEN"
        );
        assert_eq!(
            adapter
                .resolve_reference(Some("keychain:kakune/core/test"))
                .expect("keyring reference should be retained")
                .as_str(),
            "keychain:kakune/core/test"
        );
    }

    #[test]
    fn credential_adapter_rejects_empty_references_and_never_writes_env_fallbacks() {
        let adapter = SystemCredentialAccess;

        assert_eq!(
            adapter.resolve_reference(Some("env:")),
            Err(CredentialAccessError::InvalidReference)
        );
        assert_eq!(
            adapter.resolve_reference(Some("keychain:")),
            Err(CredentialAccessError::InvalidReference)
        );

        let fallback = adapter
            .resolve_reference(None)
            .expect("fallback reference should resolve");
        assert_eq!(
            adapter.write_secure(&fallback, &CredentialSecret::new("test-secret".into())),
            Err(CredentialAccessError::SecureWriteFailed)
        );
    }

    #[test]
    fn credential_secret_debug_output_is_redacted() {
        let secret = CredentialSecret::new("sensitive-test-value".into());

        assert_eq!(format!("{secret:?}"), "CredentialSecret([REDACTED])");
        assert!(!format!("{secret:?}").contains("sensitive-test-value"));
    }

    #[test]
    fn persists_only_connection_metadata() {
        let directory =
            std::env::temp_dir().join(format!("kakune-contexts-test-{}", uuid::Uuid::new_v4()));
        let path = directory.join("contexts.json");
        let mut contexts = ContextFile::default();
        contexts.contexts.push(ConnectionContext {
            id: "local".to_string(),
            name: "Local".to_string(),
            endpoint: "http://127.0.0.1:8787".to_string(),
            expected_core_id: None,
            color: None,
            credential_ref: Some("keychain:kakune/local".to_string()),
        });
        contexts.active_context_id = Some("local".to_string());
        contexts.save(&path).expect("contexts should save");
        let loaded = ContextFile::load(&path).expect("contexts should reload");
        assert_eq!(
            loaded.contexts[0].credential_ref.as_deref(),
            Some("keychain:kakune/local")
        );
        fs::remove_dir_all(directory).expect("temporary contexts should be removed");
    }
}
