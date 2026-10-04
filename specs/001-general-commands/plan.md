# Implementation Plan: General Command Initialization and Version Contract

**Branch**: `feature/general-commands` | **Date**: 2026-09-30 | **Spec**: [spec.md](spec.md)

**Input**: `specs/001-general-commands/spec.md`

**Readiness review**: 2026-10-01 against constitution 2.0.0. Implementation and acceptance evidence were finalized on 2026-10-03 for the required Windows/Linux scope; macOS is theoretical and excluded from acceptance.

**Resolution note**: `setup-plan.ps1 -Json` returned feature identifier/branch `001-general-commands`; `git branch --show-current` confirms the actual branch is `feature/general-commands`, consistent with the specification. Use the resolved feature directory without renaming or creating a branch.

## Summary

Align the three required General entry points: safe, repeatable local `init`, and identical local version results from `version` and `--version`. Add an independently testable library initialization coordinator with explicit paths, injected credential access, guarded resource publication, first-use-only transactional issuance, local authorization checks, and secret-free complete/partial reports. Keep CLI parsing/rendering in `src/main.rs` and migrations in `src/store.rs`. Preserve customized configuration/context values, identity, user data, access history, and unrelated command contracts.

Research and Phase 1 design are complete. The original 42-task breakdown was extended with T043–T044 for convergence; all 44 tasks and current acceptance evidence are recorded in `tasks.md` and `baseline-review.md`.

## Technical Context

**Language/Version**: Rust 1.98.1, edition 2024; pinned by `rust-toolchain.toml` (manifest minimum 1.98).

**Primary Dependencies**: Existing Clap 4.6.6 derive/command rendering, Tokio 1.53.1, rusqlite 0.40.2, keyring 4.2.0, serde/serde_json/serde_yaml, uuid, thiserror, and zeroize. Use standard-library OS-backed file locks; no new dependency planned. Versions remain governed by `Cargo.lock`.

**Storage**: Existing SQLite identity/token/history/workflow/execution storage; YAML configuration; JSON connection metadata containing references only; current OS credential manager for initial client secret. No schema change planned; supported existing migrations still back up before upgrading.

**Testing**: Rust unit/integration/doc tests via `cargo test --locked`; `std::process::Command` for executable contracts; injected credential doubles and isolated real storage/filesystem fixtures. No live services or external credentials in default tests.

**Target Platform**: Required acceptance environments for this feature are Windows x86_64 and Linux x86_64 (glibc). Native credential-manager/filesystem smoke checks are opt-in to the default suite but required as recorded acceptance evidence on those two environments. macOS arm64 is theoretical and unvalidated; it is not required for acceptance and no compatibility claim is inferred.

**Project Type**: One Rust library plus CLI binary and existing auxiliary binaries. No new daemon endpoint, service, frontend, or command family.

**Performance Goals**: Version <=2 seconds; empty initialization <=10 seconds on an idle supported workstation, excluding upgrades and environmental resource contention (SC-006).

**Constraints**: Local-only and noninteractive; no provider/network startup; no plaintext token fallback for init; preserve user values and prior credentials; reject remote selection before side effects. Setup lock acquisition is nonblocking, no indefinite retry; SQLite setup busy timeout <=5 seconds. Platform I/O performance is measured rather than claiming unsafe universal cancellation. Project-authored Rust remains safe.

**Scale/Scope**: One selected current-user installation per invocation and its optional external configuration/context paths. At most one initial credential for a genuinely new installation; ten repeat-run preservation checks per populated fixture. Other command families are reviewed only for overlaps/compatibility, not implemented or redesigned.

## Constitution Check

**Authority**: `.specify/memory/constitution.md`, version 2.0.0. The original research used 1.0.0; the 2026-10-01 readiness review revalidated the design under the amended administrative I/O rule. Design and implementation gates PASS; Windows/Linux acceptance evidence is recorded in `baseline-review.md`. macOS is theoretical and outside acceptance. No exception outside the constitution is requested.

| Principle / constraint | Pre-research gate | Post-design evidence |
| --- | --- | --- |
| I. Rust, Tokio, YAML | PASS: existing stack retained. | Rust coordinator; no workflow format/runtime change. |
| II. Local-first CLI | PASS: only local General operations. | Early version dispatch and init context rejection; no network dependency. |
| III. Validate before execution | PASS: no workflow execution added. | Setup resources validated before use; existing workflow validation unchanged. |
| IV. Explicit AI boundaries | PASS: no AI integration needed. | No providers loaded by init/version; offline tests. |
| V. English and safe Rust | PASS: English artifacts, unsafe forbidden. | Safe std locks, Result-based failures, no unsafe implementation planned. |
| VI. Ordered quality gates | PASS: required checks retained. | Run fmt, Clippy, full tests in order for this planning task and implementation; report blockers. |
| VII. Behavioral/regression coverage | PASS: tests required by FR-016. | Fixture matrix covers source defects and shared-call-site preservation. |
| VIII. Independent default tests | PASS: no external credentials. | Per-instance fake credential dependency; native smoke checks opt-in. |
| IX. Bounded execution | PASS under 2.0.0: workflow limits unchanged; administrative scope explicit. | Nonblocking locks, SQLite busy timeout <=5 seconds, no automatic retries; platform-call inventory, safe-timeout review, and deterministic failure/partial-state coverage below. |
| X. Secrets and compatible persistence | PASS: no reset or secret output. | Secret-free reports; local authorization; existing backed-up upgrades; no new schema. |
| XI. Specification-driven scope | PASS: FR-001–FR-017 define scope. | Additive init-only policy; preserve required product commands and unrelated behavior. |
| XII. Independent core | PASS: CLI/core boundary retained. | Exported coordinator with injected credentials; parsing and console I/O only in CLI. |
| Architecture ownership | PASS: existing module responsibilities retained. | New orchestration module does not move persistence, runtime, workflow, or scheduler ownership. |
| Isolated local checks | PASS: no normal user data needed. | Quickstart uses isolated paths; direct workflow fixture checks require `--standalone`. |

The gaps identified in the original baseline were implementation correction targets, not design gate exceptions; their outcomes are recorded in `baseline-review.md`. Any future necessary schema change, unrelated contract alteration, or unresolved functional decision must be reviewed before proceeding with affected work.

## Project Structure

### Documentation (this feature)

```text
specs/001-general-commands/
├── spec.md
├── baseline-review.md
├── checklists/requirements.md
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
└── contracts/
    ├── cli.md
    └── library.md
```

`tasks.md` contains the complete 44-task implementation breakdown, including Phase 8 convergence. Completion markers and current platform limitations are maintained there and in `baseline-review.md`.

### Source Code (repository root; planned changes marked)

```text
src/
├── lib.rs                  # Export initializer and report boundary.
├── main.rs                 # Version dispatch, init adapter, help/reporting.
├── initialization.rs       # New testable setup coordinator and guards.
├── config.rs               # Preserve/validate YAML; safe missing-file publication.
├── client_config.rs        # Preserve metadata; guarded unique-temp publication.
└── store.rs                # First-use provenance, transactional issuance, migrations.
tests/
├── general_commands.rs     # New binary argument/help/version contract tests.
├── initialization.rs       # New library setup/failure/race integration tests.
├── remote_contexts.rs      # Existing remote compatibility coverage.
└── phase11_storage.rs      # Existing persistence/backup coverage.
README.md                   # Planned General behavior/compatibility documentation.
```

**Structure Decision**: Retain the single-crate layout. Add only a library orchestration module and focused tests. The production credential adapter lives at the existing client-configuration/library boundary, preserving keyring naming; no separate framework or repository layer is warranted. Documentation updates are English per constitution; no translation work is authorized by this feature.

## Phase 0: Research Results

See [research.md](research.md) for decisions, rationale, rejected alternatives, and source evidence. Research resolved:

1. First-use eligibility must be established before migrations create identity; existing identity/history is the durable no-reissuance barrier.
2. Installation/resource guards and SQLite transactional checks are both needed; existing in-process store mutexes are insufficient.
3. Secure access must be resolved from preserved references and locally authorized; keyring presence alone is insufficient.
4. Context load/merge/publication requires unique temporary files and preservation, not replacement of existing `local` entries.
5. Per-instance credential injection provides default offline failure-path tests without public test options.
6. Root Clap long-version rendering aligns output without rewriting argument handling or remote dispatch.

No unresolved technical clarification remains.

## Administrative Limits and Platform I/O

Initialization performs one fixed sequence for one installation: one nonblocking acquisition
attempt per distinct resource guard (at most three: installation, configuration, context), one initial issuance attempt only when eligible, one
secure write/readback on first use, and no automatic recovery, polling, or retry loop.
SQLite setup busy waits are capped at five seconds per wait; this is not an aggregate
initialization deadline. Existing context collections are traversed finitely, without polling
or retries. Test synchronization and child processes use explicit finite deadlines declared
by T006 before race cases run; deadlines terminate test children rather than detached product I/O.

| Platform operations | Completion policy | Failure and preservation policy |
| --- | --- | --- |
| Filesystem metadata/canonicalization, directory creation, file open/read/write/flush, unique temporary creation, hard-link publication, atomic replacement, cleanup, and migration backup I/O | Synchronous platform completion where the existing safe API provides no supported cancellation/deadline. Nonblocking lock acquisition is separate and remains enforced. | Map errors to the affected component/resource, retain original resources and completed durable work, clean only owned temporary artifacts, and release owned guards on return/process exit. |
| OS credential-manager entry access, secret write, and readback | Platform completion where the backend exposes no supported safe timeout/cancellation. Review the selected backend; use a supported safe mechanism when available. | Return a secret-free partial error, preserve identity/token history, and require explicit recovery when usable access is absent; no plaintext fallback or retry issuance. |
| SQLite migration/open/transaction work beyond configurable busy waits | Database/platform completion; the five-second busy timeout bounds contention waits only, not backup or statement execution time. | Preserve backup-before-upgrade and transactional failure guarantees; never reset storage or classify damaged storage as new. |

No guaranteed whole-command wall-clock deadline is promised for these administrative calls.
SC-006 remains a measured idle-workstation target. Do not spawn detached mutating work and
report a timeout as cancellation. T007, T017–T018, T020–T021, T024, T027–T028, and T039
cover/review applicable limits and deterministic credential, publication, migration, and
partial-state failures. Native performance evidence is recorded by T033/T040. This policy
does not relax workflow execution limits or secret/persistence protection.

## First-Use and Context Decisions

The spec's database-state classification and offline local-context compatibility rules are
authoritative. Only an absent database observed under guards permits first-use eligibility;
preexisting supported schemas are existing installations, including legacy schemas without
identity/history. Zero-byte/schema-less files fail conservatively before migration or issuance.
T007/T018/T021 test each boundary, including migration-created identity and interrupted setup.

Preserve valid custom endpoints regardless of changes to configured listener host/port/scheme;
do not infer remote reachability. Reject conflicting declared identity and locally invalid
admin credentials. Preserve absent optional fields: an existing missing credential reference
uses `KAKUNE_TOKEN`, while missing metadata uses the installation-specific canonical keyring
reference. T020/T023/T027 cover these cases and secret-free correction guidance.

## Phase 1: Design

### Initialization sequence

1. CLI parses once, resolves existing path defaults, rejects `init --context` before setup, and invokes the library initializer. `--ca`/`--standalone` do not change init behavior.
2. Normalize resource keys and acquire nonblocking guards in deterministic order; deduplicate aliased paths and account for shared external resources. Return conflict/retry guidance on contention.
3. Validate existing YAML/JSON and detect local-context conflicts before credential issuance wherever feasible. Never replace malformed resources.
4. Publish only missing defaults and open storage through the initialization-specific provenance boundary. Preserve backed-up migration behavior and inspect prior identity/history before new identity creation.
5. On eligible first use, atomically check/insert one administrative credential; securely save, read back, and authorize it. On existing installations, resolve preserved access and authorize without creating/revoking/reactivating anything.
6. Add a missing compatible local context without changing existing fields or active selection. Preserve completed state if a later operation fails.
7. Return a complete or partial secret-free report. CLI prints success only for complete setup and actionable stderr/nonzero for required failures, including explicit recovery guidance.

The design does not promise distributed rollback across files, SQLite, and OS credentials. A crash after identity creation may require explicit recovery; that is preferable to silent credential replacement on retry.

### Version sequence

Keep built-in `--version`; add `version` and handle it before remote routing or setup. Render the root Clap command's long version and write it without an extra newline. Existing valid global options are ignored; existing parser conflicts and usage errors remain. A `--version` parser early exit need not match a normal subcommand's conflict-validation sequence.

### Public and internal interfaces

- [contracts/cli.md](contracts/cli.md): arguments, routing, output, failure/preservation semantics, compatibility boundaries.
- [contracts/library.md](contracts/library.md): injected dependencies, structured report/error, store provenance/issuance boundary.
- [data-model.md](data-model.md): existing entities, transient reports, state transitions, invariants.
- [quickstart.md](quickstart.md): isolated validation commands, fixture matrix, ordered gates.

### Validation and acceptance mapping

| Requirement / story | Planned evidence |
| --- | --- |
| FR-001, FR-011–013 / Story 3 and help | Exact executable stdout/newline/stderr/status, root/subcommand help, globals in both positions, poisoned/missing paths, early-return review. |
| FR-002–003, FR-007–009 / Story 1 | Empty explicit-path fixture, local routing rejection before effects, secure-save/readback failures, truthful partial reports, no daemon/network/provider activity. |
| FR-004–006, FR-010 / Story 2 | Ten runs on populated/custom fixtures; missing setup resources; revoked/expired/missing/wrong/inaccessible access; malformed resources; backed-up legacy upgrades. |
| FR-017 / concurrency edge | Separate connections/processes, same installation and shared external paths, at-most-one issuance, original resource preservation, bounded conflict results. |
| FR-014–015 / Story 4 | Complete advertised-command inventory, overlap disposition/evidence, daemon/doctor/recovery compatibility regressions. |
| FR-016 / all stories | Every acceptance scenario and edge mapped to named tests or explicit platform results; no default native-keyring/live-service requirement. |
| SC-001–006 | One-invocation first-use success, zero repeat-state losses, matching side-effect-free version results, non-secret failures, completed review/test results, workstation timing evidence. |

Record updated conformance results in `baseline-review.md` during implementation; do not replace existing historical evidence with unexecuted claims. Review every shared-helper caller before changing it. The current bootstrap/startup behavior is not automatically redesigned by adopting init-only first-use policy.

## Complexity Tracking

No constitutional violations or exception-driven complexity. Additional guards and injected credential access are required by concurrency, security, and independent-testability requirements; existing persistence and authentication formats remain intact.
