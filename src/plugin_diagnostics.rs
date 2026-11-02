//! Bounded, static plugin inspection. No code from a package is executed.

use std::{
    collections::BTreeSet,
    env, fs,
    path::{Path, PathBuf},
};

use serde::Serialize;
use serde_json::Value;

use crate::{
    plugin_install::{PLUGIN_MANIFEST_NAMES, package_digest},
    plugin_process::{PluginManifest, PluginRegistry},
};

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CheckStatus {
    Pass,
    Fail,
    Unavailable,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DiagnosticCheck {
    pub name: String,
    pub status: CheckStatus,
    pub message: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginDiagnosticReport {
    pub identity: String,
    pub name: String,
    pub version: String,
    pub content_digest: String,
    pub manifest: Value,
    pub declared_nodes: Vec<String>,
    pub declared_triggers: Vec<String>,
    pub declared_providers: Vec<String>,
    pub declared_services: Vec<String>,
    pub runtime_support: Vec<String>,
    pub runtime_conformance: String,
    pub checks: Vec<DiagnosticCheck>,
}

impl PluginDiagnosticReport {
    pub fn has_failures(&self) -> bool {
        self.checks
            .iter()
            .any(|check| check.status == CheckStatus::Fail)
    }
}

/// Inspects a directory or manifest file with finite package/digest bounds.
pub fn inspect(path: &Path) -> Result<PluginDiagnosticReport, String> {
    let manifest_path = find_manifest(path)?;
    let manifest = PluginManifest::load(&manifest_path).map_err(|error| error.to_string())?;
    let package_root = manifest_path
        .parent()
        .ok_or_else(|| "plugin manifest has no parent directory".to_owned())?;
    let digest = package_digest(package_root)?;
    let manifest_source =
        fs::read_to_string(&manifest_path).map_err(|_| "cannot read plugin manifest".to_owned())?;
    let mut manifest_json: Value = serde_json::from_str(&manifest_source)
        .map_err(|_| "plugin manifest is not valid JSON".to_owned())?;
    sanitize_manifest(&mut manifest_json, None, 0);

    let contributes = manifest.contributes.as_ref();
    let declared_nodes = contributes
        .map(|value| value.nodes.clone())
        .unwrap_or_default();
    let declared_triggers = contributes
        .map(|value| value.triggers.clone())
        .unwrap_or_default();
    let declared_providers = contributes
        .map(|value| value.providers.clone())
        .unwrap_or_default();
    let declared_services = contributes
        .map(|value| value.services.clone())
        .unwrap_or_default();
    let mut checks = vec![DiagnosticCheck {
        name: "manifest".to_owned(),
        status: CheckStatus::Pass,
        message: "manifest identity, version, protocol, runtime and declarations are valid"
            .to_owned(),
    }];

    let mut registry = PluginRegistry::default();
    if declared_nodes.is_empty() {
        checks.push(DiagnosticCheck {
            name: "node-definitions".to_owned(),
            status: if declared_triggers.is_empty() && declared_providers.is_empty() && declared_services.is_empty() {
                CheckStatus::Fail
            } else {
                CheckStatus::Unavailable
            },
            message: if declared_nodes.is_empty() {
                "no process-node definitions are declared; other contribution types are declarations only".to_owned()
            } else {
                String::new()
            },
        });
    } else {
        match registry.register_manifest(manifest.clone()) {
            Ok(()) => checks.push(DiagnosticCheck {
                name: "node-definitions".to_owned(),
                status: CheckStatus::Pass,
                message: format!(
                    "{} node definition(s) validate statically",
                    declared_nodes.len()
                ),
            }),
            Err(error) => checks.push(DiagnosticCheck {
                name: "node-definitions".to_owned(),
                status: CheckStatus::Fail,
                message: format!("node definitions are invalid or collide: {error}"),
            }),
        }
    }

    let runtime_available = executable_available(&manifest.runtime.command, package_root);
    checks.push(DiagnosticCheck {
        name: "runtime-dependency".to_owned(),
        status: if runtime_available {
            CheckStatus::Pass
        } else {
            CheckStatus::Unavailable
        },
        message: if runtime_available {
            "declared runtime executable is present; it was not started".to_owned()
        } else {
            "declared runtime executable is not present; it was not started".to_owned()
        },
    });
    if let Some(requirements) = &manifest.requires {
        for requirement in &requirements.executables {
            let available = executable_available(&requirement.name, package_root);
            checks.push(DiagnosticCheck {
                name: format!("required-executable:{}", requirement.name),
                status: CheckStatus::Unavailable,
                message: if available {
                    "executable is present, but the requested version was not executed or verified"
                        .to_owned()
                } else {
                    "required executable is unavailable; version checks are not executed".to_owned()
                },
            });
        }
    }
    for kind in ["triggers", "providers", "services"] {
        let count = match kind {
            "triggers" => declared_triggers.len(),
            "providers" => declared_providers.len(),
            _ => declared_services.len(),
        };
        if count > 0 {
            checks.push(DiagnosticCheck {
                name: format!("declared-{kind}"),
                status: CheckStatus::Unavailable,
                message: format!("{count} {kind} contribution(s) are declared; this runtime does not advertise their execution support"),
            });
        }
    }

    Ok(PluginDiagnosticReport {
        identity: manifest.id,
        name: manifest.name,
        version: manifest.version,
        content_digest: digest,
        manifest: manifest_json,
        declared_nodes,
        declared_triggers,
        declared_providers,
        declared_services,
        runtime_support: vec!["process nodes".to_owned()],
        runtime_conformance: "unverified; inspection never starts plugin code".to_owned(),
        checks,
    })
}

/// Resolves a manifest from a directory or a directly supplied manifest file.
pub fn find_manifest(path: &Path) -> Result<PathBuf, String> {
    if path.is_file() {
        if PLUGIN_MANIFEST_NAMES
            .iter()
            .any(|name| path.file_name().is_some_and(|value| value == *name))
        {
            return Ok(path.to_path_buf());
        }
        return Err("plugin file must be a Kakune plugin manifest".to_owned());
    }
    PLUGIN_MANIFEST_NAMES
        .iter()
        .map(|name| path.join(name))
        .find(|candidate| candidate.is_file())
        .ok_or_else(|| "plugin directory does not contain a supported Kakune manifest".to_owned())
}

fn executable_available(name: &str, package_root: &Path) -> bool {
    let candidate = Path::new(name);
    if candidate.is_absolute() || candidate.components().count() > 1 {
        return package_root.join(candidate).is_file() || candidate.is_file();
    }
    let Some(paths) = env::var_os("PATH") else {
        return false;
    };
    let extensions: Vec<String> = if cfg!(windows) {
        env::var("PATHEXT")
            .unwrap_or_else(|_| ".COM;.EXE;.BAT;.CMD".to_owned())
            .split(';')
            .map(str::to_owned)
            .collect()
    } else {
        vec![String::new()]
    };
    env::split_paths(&paths).any(|directory| {
        extensions
            .iter()
            .any(|extension| directory.join(format!("{name}{extension}")).is_file())
            || directory.join(name).is_file()
    })
}

fn sanitize_manifest(value: &mut Value, parent_key: Option<&str>, depth: usize) {
    if depth > 64 {
        *value = Value::String("[omitted: nesting limit]".to_owned());
        return;
    }
    match value {
        Value::Object(object) => {
            for (key, nested) in object.iter_mut() {
                let normalized = key.to_ascii_lowercase();
                if [
                    "secretvalue",
                    "tokenvalue",
                    "password",
                    "authorization",
                    "apikey",
                    "credentialvalue",
                ]
                .iter()
                .any(|sensitive| normalized.contains(sensitive))
                {
                    *nested = Value::String("[REDACTED]".to_owned());
                } else if key == "args" && parent_key == Some("runtime") {
                    *nested = Value::Array(
                        nested
                            .as_array()
                            .map(|args| {
                                args.iter()
                                    .map(|_| Value::String("[omitted]".to_owned()))
                                    .collect()
                            })
                            .unwrap_or_default(),
                    );
                } else {
                    sanitize_manifest(nested, Some(key), depth + 1);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                sanitize_manifest(item, parent_key, depth + 1);
            }
        }
        Value::String(text) => {
            let lowercase = text.to_ascii_lowercase();
            if [
                "token=",
                "secret=",
                "password=",
                "api_key=",
                "apikey=",
                "bearer ",
            ]
            .iter()
            .any(|marker| lowercase.contains(marker))
            {
                *text = "[REDACTED]".to_owned();
            }
        }
        _ => {}
    }
}

/// Resolves the unique workflow node types provided by this plugin.
pub(crate) fn declared_node_types(path: &Path) -> Result<BTreeSet<String>, String> {
    let manifest_path = find_manifest(path)?;
    let manifest = PluginManifest::load(&manifest_path).map_err(|error| error.to_string())?;
    let root = manifest_path
        .parent()
        .ok_or("manifest has no parent directory")?;
    // The package digest applies the same finite file, depth, symlink, and
    // aggregate-byte limits before any declared definition is traversed.
    let _ = package_digest(root)?;
    let declarations = manifest
        .contributes
        .as_ref()
        .map(|value| &value.nodes)
        .cloned()
        .unwrap_or_default();
    let mut types = BTreeSet::new();
    for declaration in declarations {
        let file = root.join(declaration);
        let source = fs::read_to_string(&file)
            .map_err(|_| "cannot read plugin node definition".to_owned())?;
        let value: Value = serde_json::from_str(&source)
            .map_err(|_| "plugin node definition is invalid JSON".to_owned())?;
        let node_type = value
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| "plugin node definition has no type".to_owned())?;
        types.insert(node_type.to_owned());
    }
    Ok(types)
}

/// Walks a bounded YAML value and returns whether it references one of the
/// supplied plugin node types, including nested control-flow bodies.
pub(crate) fn workflow_references(
    source: &str,
    node_types: &BTreeSet<String>,
) -> Result<bool, String> {
    const MAX_SOURCE_BYTES: usize = 1_048_576;
    if source.len() > MAX_SOURCE_BYTES {
        return Err("workflow source exceeds the plugin reference scan limit".to_owned());
    }
    let value: serde_yaml::Value = serde_yaml::from_str(source).map_err(|_| {
        "stored workflow YAML could not be inspected for plugin references".to_owned()
    })?;
    fn visit(
        value: &serde_yaml::Value,
        node_types: &BTreeSet<String>,
        depth: usize,
    ) -> Result<bool, String> {
        if depth > 128 {
            return Err("workflow nesting exceeds the plugin reference scan limit".to_owned());
        }
        match value {
            serde_yaml::Value::Mapping(map) => {
                for (key, value) in map {
                    if key.as_str() == Some("type")
                        && value.as_str().is_some_and(|kind| node_types.contains(kind))
                    {
                        return Ok(true);
                    }
                    if visit(value, node_types, depth + 1)? {
                        return Ok(true);
                    }
                }
            }
            serde_yaml::Value::Sequence(values) => {
                for value in values {
                    if visit(value, node_types, depth + 1)? {
                        return Ok(true);
                    }
                }
            }
            _ => {}
        }
        Ok(false)
    }
    visit(&value, node_types, 0)
}
