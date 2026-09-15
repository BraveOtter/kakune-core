use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

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

    use super::{ConnectionContext, ContextFile};

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
