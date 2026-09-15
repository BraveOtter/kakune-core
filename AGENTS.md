# Repository guidance

## Commands and verification

- Single Cargo package: library `kakune_core`, CLI/default binary `kakune`. Rust is pinned to **1.98.1** in `rust-toolchain.toml`; use that toolchain with rustfmt and Clippy.
- Match CI checks in order: `cargo fmt --check`, `cargo clippy --locked --all-targets -- -D warnings`, `cargo test --locked`. CI covers Linux, Windows, and both macOS architectures.
- Focused integration test: `cargo test --locked --test plugin_process`; append a test-name filter to select one case. Library-only filter: `cargo test --locked --lib planner::tests`.
- Process integration tests use Rust fixture binaries, not installed Python/Node packages: `plugin-helper`, `mcp-helper`, and `kakune-process-fixture`. Run via Cargo so binaries are built. For standalone test execution, plugin/MCP helper paths can be overridden with `KAKUNE_PLUGIN_HELPER` / `KAKUNE_MCP_HELPER`.
- The MiniMax live test is ignored by default. Its explicit command is `cargo test --locked live_messages_request_requires_explicit_credential -- --ignored`, requiring `KAKUNE_MINIMAX_SUBSCRIPTION_KEY`; see `.github/workflows/live-providers.yml`.
- Dependency checks in CI: `cargo audit --deny warnings` and `cargo deny check advisories bans licenses sources` (install `cargo-audit` / `cargo-deny` first). `deny.toml` rejects unknown registries and Git sources.

## Local execution

- Initialize with `cargo run -- init --data-dir ./data`, then run `cargo run -- daemon --data-dir ./data`. Pass the same explicit data directory to subsequent local commands; otherwise selection falls through `KAKUNE_DATA_DIR`, `APPDATA`, `XDG_DATA_HOME`, then `HOME/.local/share/kakune` (`src/lib.rs`).
- `init` prints the bootstrap Bearer token only once. API calls such as `/api/v1/info` need authentication; the README's bare HTTP example omits the header. Browser origins are configured in `<data-dir>/kakune.yaml` under `api.allowedOrigins`.
- Built-in workflow validation: `cargo run -- workflow validate ./examples/write-note.kakune.yaml`. Add `--data-dir ./data` to load installed, enabled plugin definitions; without it validation uses only built-ins and does not open the store.

## Boundaries that matter

- `src/main.rs::serve_daemon` performs recovery, resumes queued executions, loads plugins, and starts the scheduler before serving. Constructing `api::router` alone does not start the full daemon lifecycle.
- `workflow.rs` defines the semantic workflow model; `planner.rs` owns YAML diagnostics and compilation to an immutable execution plan; `runtime.rs` executes that persisted plan rather than rereading mutable workflow source. AST/CST and execution-plan types are private; public embedding exports live in `src/lib.rs`.
- Runtime execution is blocking: API execution submission uses `tokio::task::spawn_blocking` after preparing/persisting the execution. Preserve that boundary when wiring async handlers.
- SQLite is bundled through `rusqlite`. Schema changes belong in `src/store.rs`: update `LATEST_SCHEMA_VERSION` and the sequential `migrate` dispatch. `Store::open` migrates automatically and backs up an existing database before upgrading; there is no separate migration CLI.
- README milestone descriptions can lag implementation. In particular, `/api/v1/events` replays events and remains open with keep-alives; tests must stop reading or time out rather than wait for EOF. See `src/api.rs` and `tests/api_smoke.rs`.
- Windows HTTP/SSE tests may retain SQLite handles briefly after disconnect. Follow the bounded cleanup retry in `tests/api_smoke.rs` when adding similar tests.

## Distribution

- Build only the shipped binary with `cargo build --release --locked --bin kakune`; CI also runs `cargo package --locked` to validate the source package.
- Release packaging uses **PowerShell 7 (`pwsh`)**, including on Windows: scripts rely on `$IsWindows` / `$IsMacOS`. After building, run `pwsh -File ./scripts/package-release.ps1 -Version <Cargo-version> -OutputDirectory release-artifacts`, then `pwsh -File ./scripts/smoke-release.ps1 -AssetPath <archive>`.
- Packaging reads `target/release/kakune[.exe]` directly; a custom target directory or cross-target build layout will not be picked up. Release tags must equal `v<Cargo.toml version>`. Service templates and archive verification are documented in `packaging/README.md`.
