# Phase 0 Research: General Commands

**Date**: 2026-09-30. Planning research only; source behavior remains unchanged.
**Inputs**: [spec.md](spec.md), [baseline-review.md](baseline-review.md), constitution 1.0.0, source inspection, and delegated persistence/CLI research.

**Revalidated**: 2026-10-01 against constitution 2.0.0. Historical inputs above are retained; the updated decisions below govern implementation.

## 1. Initialization ownership and testability

**Decision**: Add a small `src/initialization.rs` library coordinator exported through `src/lib.rs`. It accepts explicit resolved paths and injected credential access, returning a secret-free structured outcome/error. Keep path-option parsing and all console rendering in `src/main.rs`; retain persistence/migrations in `src/store.rs`, YAML configuration in `src/config.rs`, and portable metadata in `src/client_config.rs`.

**Rationale**: Current initialization combines I/O, keyring operations, and reporting in `execute` and `configure_local_connection`. Constitution XII requires independent core testing. A dedicated coordinator supports failure injection without a CLI dependency or process-global keyring mock.

**Alternatives considered**: Keeping all logic in the binary violates independent testability; a broad service/repository abstraction adds unnecessary scope. Do not add a new public setup command or HTTP endpoint.

## 2. First-use classification and durable retry barrier

**Decision**: Introduce a guarded store-opening outcome for initialization that records eligibility before ordinary opening/migrations create `core_state`. Existing identity or any credential history prohibits issuance. Only demonstrably new storage is eligible; damaged or ambiguous existing resources fail conservatively. Claim initial issuance with an immediate SQLite transaction that rechecks eligibility/history before inserting at most one administrative credential. Keep ordinary `Store::open` and legacy bootstrap callers compatible unless a narrowly scoped persistence invariant fix is proven necessary.

**Rationale**: `Store::ensure_bootstrap_token` (`src/store.rs:787`) only counts unrevoked tokens and separates lookup from insertion. `Store::open` (`:432`) and migrations (`:3064`) establish identity before the CLI can determine prior state. File existence, token count after opening, or an in-memory boolean alone cannot prove first use. Retained identity/history after an interrupted first run blocks silent replacement.

**Alternatives considered**: Reusing current bootstrap logic restores revoked access; deleting records after failed keyring saving destroys the retry barrier. A new persistent initialization-state table is unnecessary for this design: existing identity/history suffice. If implementation discovers a necessary schema change, add a backed-up compatible migration and upgrade tests, never a reset.

**Readiness clarification**: Only an absent database observed under the guards is eligible.
Preexisting supported schemas, including legacy schemas without identity/history, remain
existing installations and may undergo supported backed-up migrations but cannot bootstrap
new access through init. Zero-byte/schema-less databases are ambiguous and fail unchanged;
corrupt/unsupported storage follows existing non-destructive failure rules.

## 3. Concurrent setup and resource publication

**Decision**: Use Rust 1.98.1 standard-library file locking with a nonblocking, OS-backed exclusive guard for the selected installation and configuration/context resources. Normalize absolute resource keys (including existing-parent canonicalization and platform path rules), deduplicate aliases, acquire in deterministic order, and release through owned guards. Contention returns a conflict immediately; no retry loop or stale-sentinel deletion. Cover first-use classification, store opening/migrations, credential issuance, and metadata decisions. Recheck database eligibility in an immediate transaction as defense in depth.

For missing configuration or context files, serialize into unique exclusively created same-directory temporary files and publish without replacing an existing destination (e.g. same-filesystem hard-link publication). Fail safely if the filesystem does not support the chosen no-replace primitive. For existing context updates, hold the resource guard throughout load/merge/save and atomically replace using unique temporary files and the existing platform-supported save mechanism. Never save an unchanged context. Resource guards must cover two initializations targeting the same explicit external file even with different data directories.

**Rationale**: `Store::with_connection` (`src/store.rs:2830`) only protects clones, not separate connections; migration ranges can be stale under concurrent opens (`:2883`). `CoreConfig::load_or_create` (`src/config.rs:52`) uses exists/write; `ContextFile::save` (`src/client_config.rs:52`) uses a shared temporary filename. Atomic replacement without guarding read/merge does not prevent lost updates. OS locks release on process exit.

**Alternatives considered**: SQLite-only locking cannot protect external YAML/JSON. A create-new sentinel needs stale-lock recovery. Direct create-new writes protect against overwrite but expose partial content; fully serialized no-replace publication is safer. No new lock dependency or product option is needed. These guards coordinate initialization attempts, not a new global concurrent-editing contract for every other command.

## 4. Secure access readiness and failure reporting

**Decision**: Preserve existing local metadata and credential reference; resolve references with existing env/keyring/fallback semantics. Verify retrieved access locally through `Store::authorize_scope(..., AuthScope::Admin)`; it already checks token hash, revocation, expiration, and scope (`src/store.rs:815`). A new credential must be securely saved, read back, and locally authorized before complete success. Keep raw credentials transient and zeroized where ownership permits; exclude them from reports, debug output, errors, and portable metadata.

**Rationale**: A successful keyring read is not proof of usable access. Current local setup (`src/main.rs:1193`) ignores custom references, issues before metadata validation, and prints a token on secure-save failure (`:1233`). Reports must track completed work rather than assume a transaction spanning SQLite, files, and keyring. Existing resources are validated before issuance whenever feasible; late failure remains explicit partial completion with durable history preserved.

**Alternatives considered**: Network checks would violate local-only initialization; refreshing custom metadata violates preservation; issuing on a missing keyring entry rotates access implicitly; printing a fallback violates FR-007. Keep `auth recover` deliberate and unchanged.

**Readiness clarification**: Compatibility means valid metadata, matching expected identity
when present, and local admin authorization; it is not a connectivity assertion. Valid custom
endpoints need not match the current listener and remain unchanged. Preserve absent identity
fields. Existing contexts without a credential reference use the existing `KAKUNE_TOKEN`
fallback; absent metadata uses the installation's canonical secure reference. Explicit
references retain env/keyring resolution. No DNS lookup or endpoint probe is added.

## 5. Offline tests and compatible shared setup

**Decision**: Inject per-instance credential read/write access, including reference resolution, into the library initializer. Use in-memory success/error doubles plus isolated real SQLite/filesystem fixtures. Add library integration tests in `tests/initialization.rs`, executable parsing/version tests in `tests/general_commands.rs`, and CLI adapter tests for report/error rendering. Use independent connections and child processes where needed for lock races; bound child lifetimes and fixture synchronization.

**Rationale**: Default tests must not depend on OS keyring availability or external credentials. Current metadata tests do not establish credential correctness. Existing startup invokes local setup (`src/main.rs:825`) and bootstrap (`:850`); recovery rotates deliberately (`:1245`). New init policy must not silently propagate through those paths.

**Alternatives considered**: A global keyring mock risks parallel-test interference; production test flags/env switches would expand the public surface and could expose secrets. Native keyring validation stays explicit opt-in. Preserve existing caller behavior with additive, initialization-specific entry points; test shared changes before consolidation.

## 6. Version source and dispatch

**Decision**: Keep Clap's existing `--version` parser support and add the required `Version` subcommand. Render the root command through `Cli::command().render_long_version()` using `CommandFactory`, writing the returned string without a second newline. This shares the same build-time package version and formatting as `--version` (currently `kakune 0.1.2\n`). Dispatch `version` before remote-context selection or any installation access; reject initialization's explicit remote context at that same routing boundary. Preserve parser-level invalid-syntax/conflict behavior rather than pre-scanning raw arguments.

**Rationale**: Current Clap declaration already uses `name = "kakune", version` (`src/main.rs:23`), but `Command` lacks a version variant. Remote routing precedes command matching (`:390`). A local early return prevents remote version queries and context-file/keyring reads.

**Alternatives considered**: Disabling/reimplementing Clap version handling adds risk; adding a diagnostic command is outside scope; remote Core version reporting contradicts FR-011. Compare exact stdout/newline/stderr/exit status at the executable boundary, with valid global options in both positions and invalid arguments handled normally.

**Verified dependency evidence**: Cached Clap 4.6.6 `clap_builder` sources (`builder/command.rs:1122,4898`, `parser/parser.rs:1322`) show root long-version rendering includes a trailing newline and falls back to the ordinary version. Version propagation is disabled by default. Clap's `--version` action may short-circuit conflict validation, whereas a normal `version` subcommand retains existing conflicts such as `--standalone` plus `--context`. Do not rewrite argv to manufacture identical parser short-circuiting; equality concerns successful version results. Poisoned file paths alone prove independence from their validity, not zero reads: enforce and review the early-return control-flow boundary as well.

## 7. Limits, platforms, and migration compatibility

**Decision**: Retain Rust/Tokio/Clap/rusqlite/keyring and require feature acceptance on Windows x86_64 and Linux x86_64 with glibc. macOS arm64 is a theoretical, unvalidated compatibility target, not an acceptance prerequisite; do not infer its behavior from another OS. No workflow execution, network calls, providers, or new storage schema is planned. Lock attempts are immediate, setup does not retry indefinitely, and SQLite initialization operations use an explicit finite busy timeout (at most five seconds). Validate version <=2 seconds and new initialization <=10 seconds on an idle supported workstation; exclude supported upgrade time and environmental contention as specified.

**Rationale**: Existing code/dependencies and release documentation establish the technical context. Native credential-service and filesystem call duration is platform-dependent; performance targets are acceptance measurements, not an unsafe forced cancellation of committed work. Do not claim a new universal hard timeout around platform I/O. Existing backed-up migration behavior remains authoritative.

**Constitution 2.0.0 synchronization**: The platform-operation inventory in plan.md now names
filesystem publication/backup/read/write and OS credential entry/write/readback calls, their
non-guaranteed completion times, and failure/partial-state policies. Use supported safe
timeouts/cancellation where available; otherwise rely on platform completion only for these
administrative operations. No detached mutation may be represented as cancelled. SQLite busy
wait limits are per wait, not total-operation deadlines. Offline tests cover bounded contention
and deterministic platform failure; workflow limits are unaffected.

**Alternatives considered**: New lock libraries, new configuration layering, and authentication redesign are unnecessary. Migration bypass or deleted data is constitutionally prohibited.

## Research closure

All technical choices required for Phase 1 are resolved; no `NEEDS CLARIFICATION` remains. Current defects are implementation targets, not approved constitutional exceptions. See [plan.md](plan.md) for pre/post-design gates and [quickstart.md](quickstart.md) for validation.
