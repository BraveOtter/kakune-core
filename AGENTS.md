# AGENTS.md — kakune-core

## Project

Kakune Core is a Rust-based engine for defining and executing automation workflows. It uses a node-based execution model where workflows are composed by connecting reusable nodes that perform specific operations. Nodes can be deterministic, such as conditions, loops, and control-flow primitives, or AI-powered for tasks that require model-driven processing. Kakune Core runs in the background, executing scheduled workflows and reacting to configured triggers, while remaining fully manageable from the command line.

## Commands

The commands below are product requirements, not obsolete commands. Some may not yet be implemented; preserve them. Check `src/main.rs` or `cargo run -- --help` for the currently implemented CLI.

### General

`kakune init`: Initializes Kakune for first use, preparing its configuration, data directory, storage, and initial client credentials. Running it again must not destroy existing data or silently replace existing credentials.

`kakune version` / `kakune --version`: Displays the installed Kakune version.

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
