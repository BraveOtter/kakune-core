use std::{
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CoreConfig {
    pub api: ApiConfig,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct ApiConfig {
    pub listen: String,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    pub allowed_hosts: Vec<String>,
    pub allowed_origins: Vec<String>,
    pub request_body_limit_bytes: usize,
    pub rate_limit_requests_per_minute: u32,
}

pub struct LoadedCoreConfig {
    pub config: CoreConfig,
    pub path: PathBuf,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:8787".to_string(),
            tls_cert: None,
            tls_key: None,
            allowed_hosts: Vec::new(),
            allowed_origins: Vec::new(),
            request_body_limit_bytes: 1_048_576,
            rate_limit_requests_per_minute: 120,
        }
    }
}

impl CoreConfig {
    pub fn load_or_create(
        data_dir: &Path,
        config_path: Option<PathBuf>,
    ) -> Result<LoadedCoreConfig, String> {
        let path = config_path.unwrap_or_else(|| data_dir.join("kakune.yaml"));
        if !path.exists() {
            let parent = path
                .parent()
                .ok_or_else(|| "configuration path has no parent directory".to_string())?;
            fs::create_dir_all(parent)
                .map_err(|error| format!("cannot create configuration directory: {error}"))?;
            let config = Self::default();
            fs::write(
                &path,
                serde_yaml::to_string(&config)
                    .map_err(|error| format!("cannot serialize configuration: {error}"))?,
            )
            .map_err(|error| format!("cannot create {}: {error}", path.display()))?;
            return Ok(LoadedCoreConfig { config, path });
        }
        let source = fs::read_to_string(&path)
            .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
        let config = serde_yaml::from_str::<Self>(&source)
            .map_err(|error| format!("cannot parse {}: {error}", path.display()))?;
        config.validate()?;
        Ok(LoadedCoreConfig { config, path })
    }

    pub fn listen_addr(&self) -> Result<SocketAddr, String> {
        self.api
            .listen
            .parse()
            .map_err(|_| "api.listen must be a socket address such as 127.0.0.1:8787".to_string())
    }

    pub fn validate(&self) -> Result<(), String> {
        let _ = self.listen_addr()?;
        if self.api.tls_cert.is_some() != self.api.tls_key.is_some() {
            return Err("api.tlsCert and api.tlsKey must be configured together".to_string());
        }
        for host in &self.api.allowed_hosts {
            if host.is_empty()
                || host.contains(['/', '@', '?', '#'])
                || host.contains(char::is_whitespace)
            {
                return Err(
                    "api.allowedHosts must contain explicit hostnames or IP addresses".to_string(),
                );
            }
        }
        if !(1..=16 * 1024 * 1024).contains(&self.api.request_body_limit_bytes) {
            return Err("api.requestBodyLimitBytes must be between 1 and 16777216".to_string());
        }
        if !(1..=10_000).contains(&self.api.rate_limit_requests_per_minute) {
            return Err("api.rateLimitRequestsPerMinute must be between 1 and 10000".to_string());
        }
        for origin in &self.api.allowed_origins {
            if !(origin.starts_with("http://") || origin.starts_with("https://"))
                || origin.contains(char::is_whitespace)
            {
                return Err(format!(
                    "api.allowedOrigins contains an invalid origin: {origin}"
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::CoreConfig;

    #[test]
    fn creates_and_reloads_a_default_configuration() {
        let directory =
            std::env::temp_dir().join(format!("kakune-config-test-{}", uuid::Uuid::new_v4()));
        let created = CoreConfig::load_or_create(&directory, None)
            .expect("default configuration should create");
        assert_eq!(created.config.api.listen, "127.0.0.1:8787");
        assert!(created.path.ends_with("kakune.yaml"));
        let loaded =
            CoreConfig::load_or_create(&directory, None).expect("configuration should reload");
        assert_eq!(loaded.config.api.request_body_limit_bytes, 1_048_576);
        fs::remove_dir_all(directory).expect("temporary configuration should be removed");
    }

    #[test]
    fn rejects_unknown_configuration_keys_instead_of_silently_ignoring_typos() {
        let directory =
            std::env::temp_dir().join(format!("kakune-config-typo-test-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&directory).expect("temporary configuration directory should create");
        let path = directory.join("kakune.yaml");
        fs::write(
            &path,
            "api:\n  listen: 127.0.0.1:8787\n  requestBodyLimitByets: 1024\n",
        )
        .expect("invalid configuration fixture should write");
        let error = match CoreConfig::load_or_create(&directory, None) {
            Ok(_) => panic!("unknown configuration key must fail fast"),
            Err(error) => error,
        };
        assert!(error.contains("requestBodyLimitByets"));
        fs::remove_dir_all(directory).expect("temporary configuration should be removed");
    }
}
