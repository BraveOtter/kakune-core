# AGENTS.md — kakune-core

## Project

Kakune Core is a Rust-based engine for defining and executing automation workflows. It uses a node-based execution model where workflows are composed by connecting reusable nodes that perform specific operations. Nodes can be deterministic, such as conditions, loops, and control-flow primitives, or AI-powered for tasks that require model-driven processing. Kakune Core runs in the background, executing scheduled workflows and reacting to configured triggers, while remaining fully manageable from the command line.

## Commands

The commands below are product requirements, not obsolete commands. Some may not yet be implemented; preserve them. Check `src/main.rs` or `cargo run -- --help` for the currently implemented CLI.

`kakune connect` / `kakune -c`: Connects Kakune to an AI provider from the available provider list.
`kakune workflows add` / `kakune -w -a`: Creates a new workflow.
`kakune workflows list` / `kakune -w -l`: Lists all workflows and their current status.
`kakune workflows remove` / `kakune -w -r`: Removes a workflow from the list. The workflow code can also be provided directly.
`kakune workflows edit` / `kakune -w -e`: Edits an existing workflow. The workflow code can also be provided directly.

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
