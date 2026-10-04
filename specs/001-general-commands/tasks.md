---
description: "Executable implementation tasks for General command initialization and version reporting"
---

# Tasks: General Command Initialization and Version Contract

**Input**: Design documents from `specs/001-general-commands/`
**Prerequisites**: `plan.md`, `spec.md`, `research.md`, `data-model.md`, `contracts/cli.md`, `contracts/library.md`, `quickstart.md`, and `.specify/memory/constitution.md` (2.0.0).
**Readiness review**: 2026-10-01; the original 42-task plan began at T001. Phase 8 added T043–T044; the current plan contains 44 tasks, with completion status recorded below.
**Tests**: Required by specification FR-016 and constitution VII–VIII. Write behavioral/regression tests before their implementation, observe the relevant failure, then make them pass. Existing passing behavior must stay covered.
**Organization**: Tasks are grouped by user story; shared prerequisites establish interfaces, not story behavior. Implementation and acceptance evidence are recorded in `baseline-review.md`.

## Format: `[ID] [P?] [Story] Description`

- `[P]`: Tasks in an explicitly identified parallel batch touch different files and have no dependency on another unfinished task in that batch.
- `[US1]`–`[US4]`: Correspond to the numbered stories in `spec.md`.
- Paths are relative to the repository root. Retain the current branch; do not create or rename it.
- After **each completed task**, run `cargo fmt --check`, `cargo clippy --locked --all-targets -- -D warnings`, then `cargo test --locked`, in that order. Report blockers; do not claim verification without passing gates. Focused tests do not replace the full suite.

### Red-to-green completion protocol

Test-writing and implementation are separate work descriptions, not a requirement to complete
a failing test task before starting its corresponding fix. Work in small related red-to-green
slices: write a named regression/acceptance test, record its expected failure, implement only
the matching behavior, and restore the full suite. Test tasks and dependent implementation
tasks remain unchecked/in progress until their scope is green. A dependency on a test task
means its relevant test has been written and its failure understood, not falsely marked done.
Unexpected failures must be fixed before proceeding; no unrelated story or parallel batch
may advance while the suite is red. Do not disable/ignore tests to make completion gates pass.

Once a slice is green, finish each task only after its entire scope is covered and run the
ordered full gates for each task closure. Broad test tasks may span several related slices
and remain open while their corresponding implementation advances. Existing passing
preservation tests can complete immediately after gates. The foundation must genuinely be
complete before any story starts; this protocol does not waive constitutional quality gates.

## Path Conventions

Single Rust crate: `src/`, `tests/`, `Cargo.toml`, `Cargo.lock`, and `README.md`; feature evidence lives under `specs/001-general-commands/`. Use Rust 1.98.1, existing locked dependencies, safe Rust, English documentation, isolated fixtures, and per-instance fake credentials. No new schema, network endpoint, public test option, or command family is planned.

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Confirm existing project constraints and establish the compatibility baseline without changing product behavior.

- [X] T001 Confirm the Rust 1.98.1 pin and existing Clap/Tokio/rusqlite/keyring/serde/uuid/thiserror/zeroize dependencies in `rust-toolchain.toml`, `Cargo.toml`, and `Cargo.lock`; run the ordered baseline gates and record actual results or blockers in `specs/001-general-commands/baseline-review.md`, without adding dependencies or changing the toolchain.
- [X] T002 Inventory all advertised top-level/grouped commands and every caller of `configure_local_connection`, `save_local_context`, `Store::open`, and `ensure_bootstrap_token` in `src/main.rs`, `src/client_config.rs`, `src/store.rs`, and other `src/` consumers; append file/symbol evidence, overlap dispositions (retained/aligned/proposed deprecation), and compatibility impact to `specs/001-general-commands/baseline-review.md`, preserving historical evidence and all AGENTS.md requirements.
- [X] T003 Create the acceptance-to-evidence table in `specs/001-general-commands/baseline-review.md` covering US1–US4 scenarios, every edge case, FR-001–FR-017, and SC-001–SC-006; allocate named tests in `tests/initialization.rs`, `tests/general_commands.rs`, `src/main.rs`, and `tests/phase11_storage.rs`, plus explicit opt-in platform results, leaving outcomes pending until executed.

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Define independently testable shared boundaries before implementing stories.

**CRITICAL**: Complete T001–T006 before any story work.

- [X] T004 Define `InitializationPaths`, fixed configuration/storage/identity/client-access/local-context component states, `InitializationReport`, and typed `InitializationError` in `src/initialization.rs`, and export them from `src/lib.rs`; apply the exact constraints "Resolved locations | Data, configuration, and context-file paths, available even for partial reporting.", "Completion | Complete only when every required component is ready.", "Failure | Typed secret-free category, affected resource, completed-component summary, and corrective action.", and "status created, reused, or incomplete, with unattempted work distinguished internally." Keep console I/O and process exits out of the library.
- [X] T005 Define a per-instance credential-access trait at the initialization boundary and its production adapter in `src/client_config.rs`; support reference resolution, secure write, and readback using existing env/keyring/fallback resolver semantics and service/account naming. Retain the exact constraints "Credential reference | Portable reference, not token material; follows existing env/keyring resolver semantics." and "Secret value | Transient in memory and secure local credential facility; excluded from result/error/debug/log/JSON output." Map backend errors to safe categories rather than echoing arbitrary secret-bearing messages; do not change recovery policy.
- [X] T006 Establish isolated real SQLite/filesystem fixture helpers and deterministic success/write-failure/read-failure/wrong-secret credential doubles in `tests/initialization.rs`, plus bounded executable helpers in `tests/general_commands.rs`; use unique approved temporary paths, bounded child lifetimes and synchronization, no process-global credential mock or production test environment switch, and no live AI/Core/native-keyring dependency.

**Checkpoint**: Public result/error and dependency-injection boundaries exist; fixtures can exercise the core without the CLI.

## Phase 3: User Story 1 — Prepare a Local Installation (Priority: P1) — MVP

**Goal**: One local initialization creates valid resources and usable secure administrative access without starting Core; failures are truthful, safe, and actionable.

**Independent Test**: Run the exported initializer on an empty isolated fixture with fake credentials. Assert resolved default/explicit paths, valid YAML/SQLite, stable identity, exactly one authorized admin credential, local metadata, all components ready, and no startup/network/provider activity. CLI seam tests establish success/nonzero mapping and secret-free rendering.

### Tests for User Story 1

- [X] T007 [P] [US1] Add first-use library acceptance tests in `tests/initialization.rs` for empty/default and explicit paths (including spaces/non-ASCII), one admin credential, secure write/readback/authorization, component reports, save/readback/mismatched-secret failures, invalid YAML, unwritable resources, data path as a file, and no token in report/debug/error/metadata; failing backends must preserve durable identity/history and report partial completion.
- [X] T008 [P] [US1] Add CLI parser/adapter tests in `src/main.rs` using injected initialization dependencies for resolved default paths including `KAKUNE_DATA_DIR`, explicit overrides, complete versus partial output/status, local-only `init --context` rejection before effects, and accepted `--standalone`/`--ca` no-ops; ensure recovery guidance names selected paths and no secret reaches either stream. Avoid process-global environment mutation by injecting path resolution inputs where needed.

### Implementation for User Story 1

- [X] T009 [US1] Implement normalized resource keys and owned nonblocking OS-backed exclusive guards in `src/initialization.rs` for installation/configuration/context paths; canonicalize existing parents, account for platform aliases, deduplicate and acquire in deterministic order. Enforce "Locks are OS-owned, released on exit; lock-file existence alone is not a setup state." Hold guards through decisions/publication; return immediate contention with retry guidance, never an indefinite retry or stale-sentinel deletion.
- [X] T010 [US1] Implement initialization-specific missing-YAML publication in `src/config.rs` using existing `CoreConfig` validation, unique exclusively created same-directory temporary files, and a safe no-replace publication primitive; enforce "Existing files are parsed and validated, never reset or refreshed. Publication is guarded and no-replace; malformed originals remain intact." Fail safely on unsupported publication, clean only owned temporary artifacts, and preserve ordinary caller behavior.
- [X] T011 [US1] Add initialization-specific guarded store opening/provenance in `src/store.rs`, establishing prior persisted identity/history before migration creates identity and returning demonstrably-new versus existing provenance; enforce "Core identity | Existing persisted core_state identity; stable across repeats/upgrades." and "Opening provenance | Transient guarded outcome indicating demonstrably new versus existing storage, determined before creation/migrations. Not inferred from absence of active credentials." Preserve ordinary `Store::open`, backed-up upgrades, and set setup SQLite busy timeout to at most five seconds; corrupt/unreadable/ambiguous existing storage must not become new.
- [X] T012 [US1] Add transactional initial administrative issuance in `src/store.rs`, requiring guarded new-opening eligibility and rechecking identity provenance/history in one immediate transaction before insertion; enforce "Credential ID | Existing token-record identity.", "Token hash | Existing SHA-256 digest in auth_tokens; raw values are never stored there.", "Scopes | Existing authorization scopes; initial local access requires administrative authority.", and "Initial creation is at most once for an eligible guarded first-use opening." Retain ordinary bootstrap/recovery entry points and never return raw credentials in initialization reports.
- [X] T013 [US1] Add guarded initialization-specific missing-local-context construction/publication in `src/client_config.rs` with existing endpoint/reference conventions and unique temporary files; retain "Context: id, name, endpoint, optional expected_core_id, optional color, optional credential_ref." and "Collection: existing format version, context entries, optional active-context selection, and existing export bookkeeping." Enforce "IDs remain unique under existing metadata validation." Use existing `local` ID and current default active-selection rule only when no selection exists; never replace malformed/conflicting resources or place raw secrets in JSON.
- [X] T014 [US1] Implement first-use orchestration in `src/initialization.rs`: guarded prevalidation before issuance wherever feasible, missing config publication, provenance-aware store opening, one initial credential, secure save, readback, local `Store::authorize_scope(..., AuthScope::Admin)`, then missing context publication. Zeroize transient secret ownership where possible; retain identity/history on late failure, distinguish unattempted components, and return complete only after every required component succeeds; perform no daemon/service/workflow/provider/network startup.
- [X] T015 [US1] Replace only the `Command::Init` branch in `src/main.rs` with the library coordinator and production credential adapter; resolve existing path precedence and reject any explicit `--context` before setup or remote dispatch. Accept `--standalone`/`--ca` as no-ops, report all resolved locations and component states, send actionable required failures to stderr/nonzero, and provide `kakune auth recover` guidance with selected local paths without printing token fallback or full-success wording prematurely.
- [X] T016 [US1] Add executable local-routing and ordinary-invalid-argument regression tests in `tests/general_commands.rs` for globals before/after `init`, remote rejection on absent isolated paths, and `--standalone`/`--ca` parsing; prove rejection creates no installation/config/context/lock artifacts, and keep secure-backend-dependent success/failure assertions in the injected CLI seam rather than requiring a native keyring.
- [X] T017 [US1] Add independent-connection/process contention tests in `tests/initialization.rs` for same-installation attempts, aliases, and different installations sharing explicit configuration/context paths; verify bounded conflict/nonzero guidance, no overwrite, at-most-one initial credential, guard release on ordinary failure and child exit, and no partial resource visibility. Use finite barriers/child deadlines and test-only process plumbing, not new product flags.
- [X] T018 [US1] Add store unit tests in `src/store.rs` for guarded provenance and immediate-transaction issuance across independently opened connections, preexisting identity or any token history, and interrupted opening before issuance; establish that first-use claims are not reusable to mint extra tokens and busy waiting remains finite without changing existing bootstrap semantics.

**Required US1 boundary cases**: T007/T011/T018 cover an absent database, a preexisting
zero-byte file, a valid schema-less SQLite file, a supported current schema, and a supported
legacy schema without prior identity/history. Only absence permits new-opening eligibility;
ambiguous files fail unchanged before migration/issuance. Measure the <=5-second busy-wait
bound with deterministic contention, allowing bounded test scheduling tolerance rather than
claiming a total SQL deadline. T007/T014 also cover finite credential write/readback attempts,
safe backend failures, and no detached mutation/automatic retry.
- [X] T019 [US1] Execute US1 acceptance and failure/race fixtures from `tests/initialization.rs`, `tests/general_commands.rs`, and `src/main.rs`; record named outcomes, component-state evidence, secret checks, and the MVP checkpoint in `specs/001-general-commands/baseline-review.md`. Do not mark native first-use success or SC-006 timing passed based solely on fake-backend results.

**Checkpoint**: First-use initialization and safe partial failures are independently usable; no automatic retry recovery or credential rotation is introduced.

## Phase 4: User Story 2 — Repeat Initialization Without Losing State (Priority: P1)

**Goal**: Complete only missing resources, preserve user state and credential history, and fail safely on inaccessible access or damaged resources.

**Independent Test**: Perform ten runs per populated/custom fixture and compare identity, configuration, workflows, executions, credential records/status, custom contexts, and active selection. Separately verify missing resources, invalid access, malformed resources, interrupted setup, and backed-up legacy upgrades without replacement issuance.

### Tests for User Story 2

- [X] T020 [P] [US2] Add repeat-run preservation and failure tests in `tests/initialization.rs`: ten iterations on populated/custom fixtures; missing configuration/full metadata/local entry; active remote selection; customized local endpoint/name/color/reference and absent optional fields; revoked/expired/absent/inaccessible/wrong-installation/insufficient-scope access; malformed/unreadable YAML/JSON; identity conflict; and interrupted/late-failure retry. Snapshot user-owned fields and credential history, allow only documented bookkeeping/upgrade changes, and require no replacement/reactivation/revocation.
- [X] T021 [P] [US2] Extend `tests/phase11_storage.rs` with initialization-driven supported legacy migration fixtures, recoverable pre-upgrade backup contents, retained identity/workflow/execution/token records, corrupt storage, unsupported schema, and failed upgrade cases; failures must leave original data recoverable without deletion/reset, and default tests must not depend on secure credential facilities.

### Implementation for User Story 2

- [X] T022 [US2] Implement existing-access readiness in `src/initialization.rs` by resolving preserved local references or the existing canonical local reference when metadata is absent, then authorizing admin scope locally against the selected store; enforce "Expiration / revocation | Existing optional validity and revocation state; retained unchanged on initialization repeats." Existing identity or any history blocks issuance, including after interrupted first use; missing/revoked/expired/inaccessible/wrong-store access returns incomplete setup and explicit recovery, never implicit rotation.
- [X] T023 [US2] Implement guarded existing-context load/validate/merge in `src/client_config.rs`: enforce "Preserve every existing user-owned field and unrelated entry.", "Existing optional identity/reference fields are not filled by overwriting customized metadata.", "Retain customized endpoints rather than refreshing them from changed configuration.", and "Bookkeeping timestamps may change when a real save is required; unchanged metadata need not be saved." Validate expected identity and local compatibility without network access, fail on conflicts, add only a missing local entry using available existing access, and preserve active selection.
- [X] T024 [US2] Harden initialization context saves in `src/client_config.rs` with unique same-directory temporary files, guard-held load/merge/save, no-replace publication for absent destinations, and platform-supported atomic replacement for valid existing collections; test serialization/publication failures preserve originals and clean only owned temporaries, and do not broaden this into a new global editing contract for unrelated commands.
- [X] T025 [US2] Complete repeat-run orchestration and partial-state accounting in `src/initialization.rs`: create only missing valid config/context resources, prevalidate malformed/conflicting existing resources before issuance where possible, preserve already completed work on credential or metadata failure, and ensure retries take the existing-installation path with no silent replacement. Report the affected resource and correction for data-file, corrupted/unsupported database, inaccessible location, and unsupported publication failures.
- [X] T026 [US2] Resolve any initialization-specific provenance/upgrade test failures in `src/store.rs` while retaining existing migration ownership and backup-before-upgrade behavior; enforce "No new table/column/version is required by this design." If a schema change proves necessary, stop affected work for specification review rather than adding a reset or unplanned migration.
- [X] T027 [US2] Add safe partial/error rendering regressions in `src/main.rs` for invalid existing access, context conflict, malformed existing metadata, migration errors, and retry after secure-save failure; verify original credential bytes never appear even if a credential double returns a secret-bearing backend error, output never claims complete success, and recovery/correction guidance uses the selected local paths.
- [X] T028 [US2] Add shared-external-resource preservation regressions in `tests/initialization.rs` for populated installations and concurrent context merges, normalized aliases, unique-temp save failures, and unchanged context no-save behavior; retain custom fields/active selection, require at-most-one issuance and bounded safe conflicts, and distinguish expected conflict from successful compatible merge.
- [X] T029 [US2] Run all ten-repeat populated/custom fixtures and legacy/damaged/interrupted/concurrency cases in `tests/initialization.rs` and `tests/phase11_storage.rs`; record actual per-scenario outcomes and SC-002/SC-004 preservation/secret counts in `specs/001-general-commands/baseline-review.md`, including platform limitations rather than claiming unsupported checks passed.

**Required US2 compatibility matrix**: T020/T023/T027 cover valid custom endpoints differing
from listener host/port/scheme (preserve, no probe, not a connectivity claim), matching/missing/
conflicting expected identity, explicit env/keyring references, absent reference with valid
or missing `KAKUNE_TOKEN` fallback, and missing metadata with the canonical secure reference.
Missing optional fields remain absent. Locally unauthorized credentials fail unchanged.
T021 covers legacy migration-created identity without granting initial issuance. T024/T027
cover deterministic publication/credential errors with preserved partial state.

**Checkpoint**: US1 and US2 each remain testable, and repeat initialization never becomes access recovery.

## Phase 5: User Story 3 — Identify the Installed Version (Priority: P2)

**Goal**: Both intentional equivalent entry points report the same local build version without installation or remote access.

**Independent Test**: Compare exact stdout bytes, one trailing newline, empty normal stderr, and exit 0 for both forms against missing/poisoned local paths and unreachable selected contexts; verify no state changes and ordinary parser errors remain.

### Tests for User Story 3

- [X] T030 [US3] Add executable version contract tests in `tests/general_commands.rs` for exact `kakune <CARGO_PKG_VERSION>\n` equality, empty stderr/status 0, fresh and malformed context/config fixtures, unavailable Core, missing CA, valid existing globals before/after the `version` subcommand, and both documented forms; snapshot files/absence and reject unsupported version-specific `--data-dir`/`--config`, extra arguments, and existing parser conflicts without demanding identical Clap early-exit conflict handling.

### Implementation for User Story 3

- [X] T031 [US3] Add the `Version` enum variant and early local dispatch in `src/main.rs` using `clap::CommandFactory` and root `Cli::command().render_long_version()`; write its existing trailing newline without adding another, retain built-in `--version`, enforce "Build-time package version of the invoked executable" and "No persisted entity, installation relationship, or remote lookup.", and return before context/config/CA/credential access or remote routing. Do not pre-scan/rewrite argv or alter existing parser conflicts.
- [X] T032 [US3] Add direct CLI seam/control-flow regression coverage in `src/main.rs` proving version exits before setup, credential resolution, and remote dispatch, including poison dependencies where practical; review early-return paths because malformed-file fixtures alone cannot prove zero reads, and leave unrelated command routing unchanged.
- [X] T033 [US3] Execute the version matrix from `tests/general_commands.rs`, measure both built-executable forms against the two-second idle-workstation target excluding build time, and record exact-output/side-effect/control-flow evidence and environment in `specs/001-general-commands/baseline-review.md`; do not contact an actual remote Core or native credential backend for default tests.

**Checkpoint**: `version` and `--version` remain supported, equivalent successful results describe the invoked executable, and neither needs initialized data.

## Phase 6: User Story 4 — Use a Clear, Compatible Command Surface (Priority: P2)

**Goal**: General help/docs are discoverable and clear; shared helper changes preserve startup, diagnostic, and deliberate recovery contracts.

**Independent Test**: Review the complete command inventory and overlap dispositions, assert top-level/subcommand help, and run preservation regressions for affected daemon/doctor/recovery consumers. All required commands remain requirements even if not implemented in this feature.

### Tests for User Story 4

- [X] T034 [P] [US4] Add top-level/init/version help and retained-command discovery regressions in `tests/general_commands.rs`; require discoverable `init`, `version`, and `--version`, clear local-only purposes, repeat safety and explicit recovery distinction, no additional setup/version alias, and preservation of currently advertised unrelated commands without asserting missing product commands are implemented.
- [X] T035 [P] [US4] Add injected compatibility regressions in `src/main.rs` for every changed shared setup consumer identified by T002, including foreground/background daemon preparation, `doctor`, and explicit `auth recover`; verify initialization starts no Core, startup does not silently become recovery, diagnostics remain diagnostics, and deliberate recovery retains its existing mutation contract. Test preparation seams without launching real daemons/services or relying on native credentials; augment `tests/remote_contexts.rs` only if remote routing coverage requires it.

### Implementation for User Story 4

- [X] T036 [US4] Update Clap General descriptions/help in `src/main.rs` for local-only initialization and local version reporting, explicit remote rejection, no-op compatibility flags, repeat-run preservation, and recovery distinction; preserve parser/public command contracts and correct only shared-call-site regressions demonstrated by T035 rather than propagating initialization-only issuance policy into ordinary startup/recovery.
- [X] T037 [US4] Update `README.md` with the three General entry points, existing path defaults/overrides, complete versus partial setup, secure-save/readback requirements, repeat safety, context preservation, intentional no-plaintext-fallback and first-use-only compatibility changes, and safe selected-path `auth recover` guidance; distinguish daemon startup/diagnostics/recovery and do not redesign their documented behavior.
- [X] T038 [US4] Finalize the complete advertised-command inventory and each General/shared-setup overlap disposition in `specs/001-general-commands/baseline-review.md` using implemented help, source call sites, and compatibility test results; retain both version forms, all required command requirements, historical baseline evidence, and document any deliberate General behavior change without proposing unrelated removals/redesigns.

**Checkpoint**: Every General overlap has evidence and a compatibility rationale; help/docs and retained shared consumers agree with the specification.

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Complete acceptance evidence, platform validation, security review, and final quality gates without speculative features.

- [X] T039 Audit changed paths in `src/initialization.rs`, `src/config.rs`, `src/client_config.rs`, `src/store.rs`, and `src/main.rs` for safe Rust, library/CLI ownership, finite lock/SQLite/child waits, report/debug/log/JSON secret exclusion, no plaintext init fallback, preservation on failure, and unchanged unrelated contracts; record constitution compliance and resolve in-scope findings with regression coverage in `specs/001-general-commands/baseline-review.md`.
- [X] T040 Run the isolated scenarios in `specs/001-general-commands/quickstart.md` and explicitly opt-in native-keyring/file-publication smoke checks on the required Windows x86_64 and Linux x86_64/glibc environments; record platform/environment, one-invocation first-use result, <=10-second empty-init and <=2-second version measurements excluding upgrades/contention/build, safe partial failures, and any required-platform blockers in `specs/001-general-commands/baseline-review.md`, without exposing tokens or touching normal user data. Record macOS arm64 as theoretical and outside acceptance; it is not a test dependency.
- [X] T041 Reconcile every acceptance scenario, edge case, FR-001–FR-017, and SC-001–SC-006 against named automated tests or executed opt-in platform evidence in `specs/001-general-commands/baseline-review.md`; update `specs/001-general-commands/quickstart.md` to implemented usage where needed, keep required Windows/Linux results and theoretical macOS status explicit, and do not claim evidence that was not executed.
- [X] T042 Run final `cargo fmt --check`, `cargo clippy --locked --all-targets -- -D warnings`, and `cargo test --locked` in that exact order after all changes; record actual results/counts/blockers in `specs/001-general-commands/baseline-review.md`, finish with the full suite, and confirm `Cargo.lock`/toolchain remain unchanged unless separately justified.

**Constitution IX audit scope (T039/T040)**: Review every platform operation inventoried in
plan.md for a supported safe timeout/cancellation mechanism and use it where available.
Otherwise retain synchronous administrative platform completion, document the missing total
deadline and defined error/partial-state behavior, and never claim detached work is cancelled.
Review nonblocking locks, per-wait SQLite bounds, fixed credential attempts, no automatic
retry/polling, and finite test child/synchronization deadlines. SC-006 measurements do not
prove enforced deadlines; no workflow limits are relaxed.

## Dependencies & Execution Order

### Phase Dependencies

```text
Setup T001–T003 -> Foundation T004–T006
                          |-> US1 T007–T019 -> US2 T020–T029 --|
                          |-> US3 T030–T033 ------------------|-> US4 T034–T038
                                                             |-> Polish T039–T042
```

- All stories require the completed foundation. US2 consumes US1's guarded initialization/provenance infrastructure but uses its own populated fixtures and is independently verifiable once available.
- US3 has no behavioral dependency on US1/US2; its implementation can proceed after the foundation only while the existing suite is green, and `src/main.rs` edits and `tests/general_commands.rs` edits must be serialized with other stories.
- US4's inventory baseline is T002; final help/compatibility work depends on US1–US3's settled behavior. Polish follows all four stories.
- Recommended single-implementer order: US1 (P1), US2 (P1), US3 (P2), US4 (P2). Phase numbering reflects specification priority, not a claim that all code can be edited concurrently.

### Within Each Story

- US1: T007 and T008 form a parallel test-writing batch under the red-to-green protocol; they need not be marked complete before related implementation starts. T009 precedes guarded operations. T010–T013 use foundation interfaces and may be scheduled in dependency-aware file-isolated batches, but are conservatively unmarked because coordinator/store interface integration is shared. T014 requires T009–T013 behavior; T015 requires T014 behavior; T016–T018 validate behavior and reproduce defects before corresponding fixes; T019 closes the story only after T007–T018 and their gates pass.
- US2: T020 and T021 form a parallel test batch after US1. T022–T024 precede final orchestration T025. T026 addresses migration evidence without speculative schema work. T027–T028 follow implementations; T029 closes the story.
- US3: T030 -> T031 -> T032 -> T033. The story shares CLI and executable-test files, so it has no safe intra-story parallel batch.
- US4: T034 and T035 form a parallel test batch after US1–US3. T036 follows those tests; T037 documents settled behavior; T038 records verified dispositions.
- Tasks writing the same evidence file (`baseline-review.md`) are serialized. Quality gates run after each completed task, including parallel batches' individual integrated changes.

### Parallel Opportunities

Six tasks are marked `[P]` in three explicit two-task batches: T007/T008, T020/T021, and T034/T035. Additional cross-story progress is possible for US3 after the foundation, but coordinate shared-file ownership; do not run concurrent edits of `src/main.rs`, `src/store.rs`, `src/client_config.rs`, `tests/initialization.rs`, or `tests/general_commands.rs`.

## Parallel Example: User Story 1

```text
After T006, write these tests concurrently:
T007: First-use/failure library cases in tests/initialization.rs.
T008: Parser/adapter/report cases in src/main.rs.
Integrate relevant tests and record expected failures before their matching fixes.
Keep T007/T008 open until green; do not block related T009–T015 on false completion.
```

## Parallel Example: User Story 2

```text
After T019, write these tests concurrently:
T020: Populated/custom/access/retry fixtures in tests/initialization.rs.
T021: Backed-up legacy and invalid-storage fixtures in tests/phase11_storage.rs.
Integrate relevant tests and record expected failures before related T022–T026 fixes.
Keep broad test tasks open until all their scenarios and ordered gates pass.
```

## Parallel Example: User Story 3

```text
No intra-story batch: T030–T033 are sequential.
Cross-story option after T006: T030 in tests/general_commands.rs may run alongside
T009 in src/initialization.rs; pause if US1 executable tests or CLI edits need
the same file. Do not overlap T031/T032 with US1/US2/US4 src/main.rs edits.
```

## Parallel Example: User Story 4

```text
After US1–US3, write these tests concurrently:
T034: General help and retained surface cases in tests/general_commands.rs.
T035: Startup/doctor/recovery preparation compatibility cases in src/main.rs.
Record expected failures, apply related T036 fixes, then close tests with green gates.
T037/T038 document the settled, verified behavior rather than completing a red slice.
```

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Setup and Foundation (T001–T006).
2. Complete US1 (T007–T019), including injected failure coverage and bounded concurrency protection.
3. Stop and validate empty isolated setup, secure-save/readback failures, truthfully partial output, routing rejection, and ordered gates.
4. Demo first-use local initialization only. This MVP is not acceptance of repeat-run or version requirements; do not advertise the whole feature as complete.

### Incremental Delivery

1. Add US2 and verify ten populated repeats plus damaged/legacy/interrupted fixtures.
2. Add US3 and verify exact version equality and early side-effect-free dispatch.
3. Add US4 and verify discoverability, compatible helper consumers, and documented intentional changes.
4. Complete Polish and close the acceptance matrix with actual automated/platform results and final ordered gates.

### Parallel Team Strategy

Complete foundation together, then use the listed test batches and optionally file-isolated US3 work only while the full suite is green. Assign one owner at a time to each shared source/test/evidence file. During an expected red slice, advance only matching tests/fixes, keep tasks open, and restore green before unrelated work. Run ordered full gates at each task closure; no delegation or parallel agent execution is required by this task list.

## Notes

- Existing identity/history is the durable retry barrier; filesystem/SQLite/keyring do not form a distributed transaction. Do not delete durable history to simulate rollback.
- Existing optional context values are preserved, not filled/refreshed to force success. Conflicting/unusable existing access requires explicit correction or recovery.
- Native secure-credential success is opt-in platform evidence, not a prerequisite for default tests. Fixture success cannot establish all native platform results.
- If implementation requires a new schema or an unresolved public-contract decision, stop affected work for specification review in accordance with constitution XI.
- Task totals: 44; Setup 3, Foundation 3, US1 13, US2 10, US3 4, US4 5, Polish 4, Convergence 2.

## Phase 8: Convergence

- [X] T043 Complete native initialization validation on Linux x86_64/glibc and reconcile it with Windows T040 evidence, including secure-credential first use and filesystem publication; record both required-platform results per SC-001 and document macOS arm64 as theoretical, unvalidated, and outside acceptance.
- [X] T044 Record empty-initialization and both version-form timings on an idle supported workstation, including host conditions and excluding build time, to close the remaining performance evidence per SC-006 and T033/T040
