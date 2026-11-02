//! Local, non-destructive generation of an SDK-compatible plugin starter.

use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::Serialize;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginScaffold {
    pub destination: PathBuf,
    pub identity: String,
    pub node_type: String,
}

/// Creates `<parent>/<name>` and refuses to replace any existing destination.
///
/// The identity mapping is intentionally stable and visible: a simple name
/// `my-plugin` becomes `local.my-plugin`, and its starter node is
/// `local.my-plugin.hello@1`.
pub fn create(name: &str, parent: &Path) -> Result<PluginScaffold, String> {
    let slug = validate_name(name)?;
    let identity = format!("local.{slug}");
    let node_type = format!("{identity}.hello@1");
    let destination = parent.join(name);
    if destination.exists() {
        return Err(format!(
            "refusing to overwrite existing plugin destination {}",
            destination.display()
        ));
    }

    let staging = parent.join(format!(".kakune-plugin-{}", Uuid::new_v4()));
    fs::create_dir(&staging)
        .map_err(|error| format!("cannot create plugin staging directory: {error}"))?;
    let result = write_scaffold(&staging, name, &identity, &node_type).and_then(|()| {
        fs::rename(&staging, &destination)
            .map_err(|error| format!("cannot publish plugin scaffold: {error}"))
    });
    if result.is_err() {
        let _ = fs::remove_dir_all(&staging);
    }
    result?;

    Ok(PluginScaffold {
        destination,
        identity,
        node_type,
    })
}

fn validate_name(name: &str) -> Result<String, String> {
    if name.is_empty()
        || name.len() > 48
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        || !name.as_bytes()[0].is_ascii_alphanumeric()
    {
        return Err(
            "plugin name must be 1-48 ASCII letters, digits, hyphens, or underscores and start with a letter or digit".to_owned(),
        );
    }
    Ok(name.to_ascii_lowercase().replace('_', "-"))
}

fn write_scaffold(root: &Path, name: &str, identity: &str, node_type: &str) -> Result<(), String> {
    fs::create_dir(root.join("src"))
        .and_then(|()| fs::create_dir(root.join("nodes")))
        .map_err(|error| format!("cannot create plugin directories: {error}"))?;

    let safe_name = name.replace(['\r', '\n', '"'], " ");
    let manifest = serde_json::json!({
        "manifestVersion": "1.0",
        "id": identity,
        "name": safe_name,
        "version": "0.1.0",
        "pluginProtocol": ">=1.0.0 <2.0.0",
        "runtime": { "kind": "process", "command": "node", "args": ["dist/index.js"] },
        "description": { "en": format!("A Kakune plugin named {safe_name}.") },
        "requires": { "executables": [{ "name": "node", "version": ">=22.0.0", "versionArgs": ["--version"] }] },
        "contributes": { "nodes": ["nodes/hello.node.json"] }
    });
    let definition = serde_json::json!({
        "apiVersion": "kakune.dev/v1",
        "kind": "NodeDefinition",
        "type": node_type,
        "name": "Hello",
        "description": "Returns a greeting.",
        "inputSchema": {
            "type": "object",
            "required": ["name"],
            "properties": { "name": { "type": "string", "minLength": 1 } }
        },
        "outputSchema": {
            "type": "object",
            "properties": { "message": { "type": "string" } }
        }
    });
    let package = serde_json::json!({
        "name": identity,
        "version": "0.1.0",
        "private": true,
        "type": "module",
        "scripts": { "build": "tsc -p tsconfig.json" },
        "dependencies": { "@kakune-ai/sdk": "^0.1.0" },
        "devDependencies": { "typescript": "^5.8.3" }
    });

    write_json(root.join("kakune.plugin.json"), &manifest)?;
    write_json(root.join("nodes/hello.node.json"), &definition)?;
    write_json(root.join("package.json"), &package)?;
    fs::write(
        root.join("tsconfig.json"),
        r#"{
  "compilerOptions": {
    "target": "ES2022",
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "rootDir": "src",
    "outDir": "dist",
    "strict": true,
    "skipLibCheck": true
  },
  "include": ["src/**/*.ts"]
}
"#,
    )
    .map_err(|error| format!("cannot write TypeScript configuration: {error}"))?;
    fs::write(root.join("src/index.ts"), entrypoint(node_type))
        .map_err(|error| format!("cannot write plugin entrypoint: {error}"))?;
    fs::write(
        root.join("README.md"),
        format!(
            "# {safe_name}\n\nPlugin identity: `{identity}`.\n\nThe generated node type is `{node_type}`. Install dependencies with `npm install`, then build with `npm run build`. The manifest and node definition follow the Kakune process-plugin SDK contracts.\n"
        ),
    )
    .map_err(|error| format!("cannot write plugin README: {error}"))?;
    Ok(())
}

fn write_json(path: PathBuf, value: &serde_json::Value) -> Result<(), String> {
    let text = serde_json::to_string_pretty(value).map_err(|error| error.to_string())?;
    fs::write(path, format!("{text}\n"))
        .map_err(|error| format!("cannot write plugin starter file: {error}"))
}

fn entrypoint(node_type: &str) -> String {
    format!(
        r#"import {{ createPluginFailure, createPluginSuccess, isPluginJsonRpcRequest, runPluginStdio }} from "@kakune-ai/sdk";

function isRecord(value: unknown): value is Record<string, unknown> {{
  return typeof value === "object" && value !== null && !Array.isArray(value);
}}

await runPluginStdio((message) => {{
  if (!isPluginJsonRpcRequest(message)) return;
  if (message.method === "initialize") {{
    return createPluginSuccess(message.id, {{ protocolVersion: "1.0", nodes: ["{node_type}"] }});
  }}
  if (message.method === "shutdown") {{
    return createPluginSuccess(message.id, {{ stopped: true }});
  }}
  if (message.method !== "node/execute") {{
    return createPluginFailure(message.id, -32601, "Method not found", {{ method: message.method }});
  }}
  const inputs = isRecord(message.params) && isRecord(message.params.inputs) ? message.params.inputs : undefined;
  if (!inputs || typeof inputs.name !== "string" || inputs.name.trim() === "") {{
    return createPluginFailure(message.id, -32602, "inputs.name must be a non-empty string");
  }}
  return createPluginSuccess(message.id, {{ route: "success", outputs: {{ message: `Hello, ${{inputs.name}}!` }} }});
}});
"#
    )
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use super::create;

    fn temp_dir() -> PathBuf {
        let path = std::env::temp_dir().join(format!("kakune-scaffold-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&path).expect("temporary parent is created");
        path
    }

    #[test]
    fn creates_expected_sdk_layout_and_refuses_overwrite() {
        let parent = temp_dir();
        let generated = create("hello_world", &parent).expect("scaffold is generated");
        assert_eq!(generated.identity, "local.hello-world");
        assert_eq!(generated.node_type, "local.hello-world.hello@1");
        assert!(generated.destination.join("package.json").is_file());
        assert!(generated.destination.join("tsconfig.json").is_file());
        assert!(generated.destination.join("src/index.ts").is_file());
        assert!(
            generated
                .destination
                .join("nodes/hello.node.json")
                .is_file()
        );
        assert!(create("hello_world", &parent).is_err());
        fs::remove_dir_all(parent).expect("temporary parent is removed");
    }
}
