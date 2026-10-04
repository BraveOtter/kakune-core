use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::initialization::{
    CredentialAccess, CredentialAccessError, CredentialReference, CredentialSecret,
};
use serde::{Deserialize, Serialize};

const CLI_KEYRING_SERVICE: &str = "dev.kakune.cli";

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
    pub contexts: Vec<ConnectionContext>,
}

impl Default for ContextFile {
    fn default() -> Self {
        Self {
            format: "kakune-contexts/v1".to_string(),
            exported_at: timestamp().unwrap_or_default(),
            active_context_id: None,
            contexts: Vec::new(),
        }
    }
}

impl ContextFile {
    pub fn load(path: &Path) -> Result<Self, String> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let source = fs::read_to_string(path)
            .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
        let contexts: Self = serde_json::from_str(&source)
            .map_err(|error| format!("cannot parse {}: {error}", path.display()))?;
        contexts.validate()?;
        Ok(contexts)
    }

    pub fn save(&mut self, path: &Path) -> Result<(), String> {
        self.validate()?;
        self.exported_at = timestamp()?;
        let parent = path
            .parent()
            .ok_or_else(|| "contexts path has no parent directory".to_string())?;
        fs::create_dir_all(parent)
            .map_err(|error| format!("cannot create contexts directory: {error}"))?;
        let temporary = path.with_extension("json.tmp");
        fs::write(
            &temporary,
            serde_json::to_vec_pretty(self)
                .map_err(|error| format!("cannot serialize contexts: {error}"))?,
        )
        .map_err(|error| format!("cannot write {}: {error}", temporary.display()))?;
        fs::rename(&temporary, path)
            .map_err(|error| format!("cannot replace {}: {error}", path.display()))
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.format != "kakune-contexts/v1" {
            return Err("contexts format must be kakune-contexts/v1".to_string());
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

pub fn default_contexts_path(data_dir: PathBuf) -> PathBuf {
    data_dir.join("cli").join("contexts.json")
}

fn validate_context(context: &ConnectionContext) -> Result<(), String> {
    if context.id.is_empty() || context.id.len() > 256 {
        return Err("context ID must be 1-256 characters".to_string());
    }
    if context.name.is_empty() || context.name.len() > 200 {
        return Err("context name must be 1-200 characters".to_string());
    }
    if !(context.endpoint.starts_with("http://") || context.endpoint.starts_with("https://"))
        || context.endpoint.contains(char::is_whitespace)
    {
        return Err("context endpoint must be an HTTP or HTTPS URL".to_string());
    }
    if let Some(color) = &context.color
        && (color.len() != 7
            || !color.starts_with('#')
            || !color[1..].bytes().all(|byte| byte.is_ascii_hexdigit()))
    {
        return Err("context color must be a #RRGGBB value".to_string());
    }
    if context.credential_ref.as_deref().is_some_and(str::is_empty) {
        return Err("context credentialRef must not be empty".to_string());
    }
    Ok(())
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
