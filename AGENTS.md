# AGENTS.md — kakune-core

## Project

Kakune Core is a Rust-based engine for defining and executing automation workflows. It uses a node-based execution model where workflows are composed by connecting reusable nodes that perform specific operations. Nodes can be deterministic, such as conditions, loops, and control-flow primitives, or AI-powered for tasks that require model-driven processing. Kakune Core runs in the background, executing scheduled workflows and reacting to configured triggers, while remaining fully manageable from the command line.

## Commands

The commands below are product requirements, not obsolete commands. Some may not yet be implemented; preserve them. Check `src/main.rs` or `cargo run -- --help` for the currently implemented CLI.

### General

`kakune init`: Initializes Kakune for first use, preparing its configuration, data directory, storage, and initial client credentials. Running it again must not destroy existing data or silently replace existing credentials.

`kakune version` / `kakune --version`: Displays the installed Kakune version.

`kakune doctor`: Diagnoses the local Kakune installation and reports configuration, storage, database, credential store, Core, service, compatibility, and platform-related problems without modifying the installation.

### Daemon

`kakune daemon`: Runs the Kakune Core in the foreground, keeping it attached to the current terminal.

`kakune daemon start`: Starts the Kakune Core as a background process under the current user.

`kakune daemon stop`: Stops the background Kakune Core started for the current user.

`kakune daemon status`: Shows whether the user-level Kakune Core is running and reports relevant runtime information when available.

### Service

`kakune service install`: Installs the Kakune Core as an operating-system-managed service using the native service mechanism of the current platform.

`kakune service uninstall`: Removes the Kakune system service without deleting Kakune workflows, configuration, or persistent data.

`kakune service start`: Starts the installed Kakune system service.

`kakune service stop`: Stops the installed Kakune system service gracefully.

`kakune service status`: Shows whether the Kakune system service is installed and its current runtime status.

### Contexts

`kakune context add <name>`: Adds a Core context representing a local or remote Kakune Core and stores its connection configuration.

`kakune context list`: Lists all configured Core contexts and identifies the currently selected context.

`kakune context use <name>`: Sets the specified Core context as the default context for subsequent CLI operations.

`kakune context inspect <name>`: Shows the configuration and connection information associated with a Core context without exposing secret credentials.

`kakune context remove <name>`: Removes a configured Core context without affecting the Core or its data.

`kakune context import <file>`: Imports one or more Core context definitions from a file.

`kakune context export <name>`: Exports a Core context definition without including secret credentials unless an explicitly supported secure export mechanism is requested.

### Workflows

`kakune workflow create <name>`: Creates a new workflow and generates its initial YAML definition.

`kakune workflow import <file>`: Imports an existing workflow definition from a YAML file into the selected Core.

`kakune workflow export <workflow>`: Exports an existing workflow from the selected Core to a YAML file.

`kakune workflow validate <workflow>`: Parses and validates a workflow without executing it, reporting syntax, schema, type, reference, and semantic errors.

`kakune workflow plan <workflow>`: Resolves and compiles a valid workflow into its execution plan and displays the resulting execution structure without running it.

`kakune workflow list`: Lists all workflows known to the selected Core and their current status.

`kakune workflow run <workflow>`: Manually starts a new execution of the specified workflow using the selected Core.

`kakune workflow run <workflow> --standalone`: Executes the specified workflow using a temporary local runtime without connecting to an existing Core daemon.

`kakune workflow enable <workflow>`: Enables a workflow so its configured triggers can start new executions automatically.

`kakune workflow disable <workflow>`: Disables a workflow and prevents its triggers from starting new executions without deleting the workflow.

### Executions

`kakune execution list`: Lists workflow executions and their current status.

`kakune execution inspect <execution-id>`: Shows detailed information about a specific execution, including its status, results, errors, timing, nodes, and execution trace when available.

`kakune execution cancel <execution-id>`: Requests cancellation of an active execution.

`kakune execution logs <execution-id>`: Shows the logs and events associated with a specific execution.

### Triggers

`kakune trigger list`: Lists the triggers registered in the selected Core, including their workflow association and current status.

`kakune trigger inspect <trigger-id>`: Shows detailed information about a trigger, including its type, configuration, associated workflow, state, and scheduling information when applicable.

### Plugins

`kakune plugin create <name>`: Creates the initial structure for a new Kakune plugin using the supported plugin SDK and manifest format.

`kakune plugin inspect <plugin>`: Inspects a plugin package or installed plugin and displays its manifest, capabilities, nodes, triggers, version, and compatibility information without executing plugin code.

`kakune plugin install <source>`: Installs a plugin from a supported source such as a local package, Git repository, or package registry.

`kakune plugin list`: Lists installed plugins and their current versions and status.

`kakune plugin update <plugin>`: Updates an installed plugin to a compatible version according to the requested source or update policy.

`kakune plugin remove <plugin>`: Removes an installed plugin without silently deleting workflows that reference it.

`kakune plugin doctor <plugin>`: Diagnoses installation, compatibility, protocol, manifest, dependency, and runtime problems affecting an installed plugin.

`kakune plugin test <plugin>`: Runs the Kakune plugin conformance and compatibility tests against a plugin.

### Providers

`kakune provider add <provider>`: Adds and configures an AI model provider for the selected Core.

`kakune provider list`: Lists configured AI providers and their current availability or authentication status.

`kakune provider auth <provider>`: Configures or refreshes authentication for an AI provider using the provider's supported authentication mechanism.

`kakune provider test <provider>`: Tests connectivity, authentication, and basic availability of a configured AI provider.

`kakune provider models <provider>`: Lists the models exposed by a configured provider together with available capability information when supported.

### MCP

`kakune mcp add <name>`: Adds and configures an MCP server connection.

`kakune mcp list`: Lists configured MCP servers and their current status.

`kakune mcp inspect <name>`: Shows the configuration and capabilities of a configured MCP server without exposing secret credentials.

`kakune mcp auth <name>`: Performs or refreshes authentication for an MCP server when its transport or authentication mechanism requires it.

`kakune mcp tool list <name>`: Lists the tools exposed by the specified MCP server.

`kakune mcp tool call <name> <tool>`: Calls a tool exposed by the specified MCP server with the provided input.

### Secrets

`kakune secret set <name>`: Creates or replaces a secret in the configured secure secret store. Secret values must not be supplied through insecure command-line arguments when a secure input mechanism is available.

`kakune secret list`: Lists available secret names and metadata without revealing their values.

`kakune secret remove <name>`: Removes a secret from the configured secure secret store.

## Style

Use the development toolchain pinned to Rust 1.98.1 in `rust-toolchain.toml`.
Workflows are stored as YAML files
Code and documentation are written in English
Follow `.specify/memory/constitution.md`, the sole authoritative and ratified project constitution, for project constraints.

## Core boundaries

- `src/lib.rs` exposes the core library; `src/main.rs` wires the CLI. Keep core logic independently testable and console I/O in the CLI.
- `src/workflow.rs` and `src/planner.rs` define and validate workflows; `src/runtime.rs` executes them; `src/scheduler.rs` manages triggers; `src/store.rs` owns persistence and migrations.
- SQLite migrations run when the store opens and back up an existing database before upgrading. Schema changes must preserve upgrades from existing databases; do not rely on deleting the database.

## Local execution

`run` calls the Core API by default, even with `--data-dir`. For direct local execution, use `--standalone` and an isolated data directory to avoid touching normal user data:

```text
cargo run -- --standalone run examples/write-note.kakune.yaml --data-dir <isolated-directory>
```

## After completing any task

Run the CI-equivalent checks in this order:

```text
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

Focused checks: `cargo test --locked --test plugin_process` runs one integration suite; `cargo test --locked <test_name>` filters tests by name. Finish with the full test suite.
