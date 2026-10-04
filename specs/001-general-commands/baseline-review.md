# General Commands: Baseline Review

**Reviewed**: 2026-09-30
**Scope**: Historical baseline plus implementation, conformance, and acceptance evidence. Current acceptance scope is Windows x86_64 and Linux x86_64/glibc; macOS arm64 is theoretical, unvalidated, and excluded from acceptance.

## Current Acceptance Scope — 2026-10-03

- Native acceptance evidence is required on Windows x86_64 and Linux x86_64/glibc. Linux evidence
  may use an isolated Linux/glibc container with a real ephemeral Secret Service and filesystem.
- The maintainer clarified that macOS arm64 support is theoretical. No macOS validation is
  required to accept this feature; macOS remains unvalidated, and no compatibility or release
  guarantee is inferred from Windows/Linux results.
- T043 and T044 are complete under this clarified scope. Windows and Linux native initialization
  evidence is recorded below; T044's idle Windows timings are recorded in the Phase 8 section.
- Dated historical phase notes remain unchanged where they accurately describe earlier runs. Any
  earlier statement that macOS validation blocks acceptance is superseded by this scope record.

## Specification Setup

- `.specify/extensions.yml` was absent during pre-execution checks; no pre-hooks were registered or run.
- `specify preset resolve spec-template` resolved `.specify/templates/spec-template.md` from the core layer; its section order and headings were used.
- `.specify/init-options.json` selects sequential feature numbering. No `specs/` directory existed, so this invocation creates only `specs/001-general-commands`.
- The existing branch is `feature/general-commands`; branch creation is not needed and directory resolution is independent of that name.

## Observed Behavior

| Entry point | Evidence | Assessment |
| --- | --- | --- |
| `kakune init` | Present in `src/main.rs`; executed successfully on a new isolated directory and on repeat runs. | Exists and basic setup works; full target-contract conformance remains unverified. |
| `kakune --version` | Executed the newly built executable; output was `kakune 0.1.2`. | Exists and reports the local build version. |
| `kakune version` | Executed the same executable; it returned an unrecognized-subcommand error and exit status 1. | Missing required entry point. |
| Top-level help | `cargo run --locked -- --help` advertises `init` and `--version`, but no `version` command. | Discovery must be aligned with the required contract. |

Initialization smoke checks used an isolated directory under `C:\Users\AdrianMadu\AppData\Local\Temp\opencode`. A corrected repeat-run comparison confirmed unchanged configuration text and unchanged credential metadata. Connection-file bytes changed; `ContextFile::save` refreshes `exported_at`, so this alone does not demonstrate a user-value regression. These checks did not populate workflows, test upgrades, simulate failures, or prove concurrent safety.

Two preliminary comparison attempts had harness-script errors: `Get-FileHash` was unavailable, and the connection file was initially looked up outside its `cli/` directory. Their preservation results are not treated as evidence; the corrected comparison used direct file reads and the actual connection path.

## Gaps Found by Source Inspection

- `execute` routes commands with an explicit `--context` to remote execution before dispatching initialization; the proposed local-only contract requires rejection instead.
- `configure_local_connection` prints a raw initial token when secure credential saving fails and still returns success. The specification explicitly changes this initialization behavior to a non-secret partial-failure result.
- `save_local_context` replaces an existing `local` entry, including its name, endpoint, identity, color, and credential reference. Preservation and conflict handling need coverage and correction.
- `Store::ensure_bootstrap_token` checks only for non-revoked tokens. It can create a new bootstrap credential in an existing installation with no non-revoked credentials; this differs from first-use-only issuance. It does not establish that an existing credential is unexpired or usable.
- Initialization reports data and configuration success before client-connection setup completes. The result must distinguish complete from partial setup.
- Existing configuration tests cover default creation/reload and unknown-key rejection. Existing integration suites use bootstrap credentials, but the reviewed test surface does not establish end-to-end General-command conformance.

## Equivalence and Refactoring Dispositions

| Overlap | Disposition | Compatibility constraint |
| --- | --- | --- |
| `version` and `--version` | Retain both and align output; their equivalence is intentional. | Neither documented form may be removed. |
| Configuration/storage preparation in `init`, `doctor`, and daemon commands | Review shared preparation for safe reuse, not public-command consolidation. | Preserve distinct setup, diagnostic, and lifecycle purposes. |
| Local connection setup in `init`, foreground daemon startup, and background daemon startup | Candidate for reuse with explicit preservation and credential-policy boundaries. | Shared changes must not silently alter unrelated startup behavior. |
| `auth recover` versus `init` | Retain separately: recovery deliberately changes access; initialization must not. | Do not fold token rotation into repeat initialization. |
| Store opening and supported migrations | Preserve the existing persistence responsibility and upgrade contract. | Backups and supported upgrades remain mandatory. |
| Other top-level/grouped workflow and execution commands | Outside this feature, even if naming overlaps exist. | No unrelated command deletion or redesign is authorized. |

No additional advertised initialization or version synonym was found in the reviewed command declarations. Implementation planning must extend the review to observable compatibility and regression tests rather than treating shared helper calls as proof of equivalence.

## Verification Status

- Specification quality review passed; implementation work has not been performed.
- Full acceptance verification is a requirement of the planned feature, not an outcome of these smoke checks.
- Ordered quality gates passed: `cargo fmt --check`, `cargo clippy --locked --all-targets -- -D warnings`, and `cargo test --locked`. The full suite passed 116 tests with two explicitly opt-in live-provider tests ignored; Windows linker informational warnings appeared during the test build.
- `.specify/extensions.yml` was also absent at the mandatory post-execution check; no post-hooks were registered or dispatched.

## Implementation Readiness Review — 2026-10-01

The 2026-09-30 source and smoke evidence above is historical and remains unchanged.
This review synchronizes spec 001 with the maintainer-authorized constitution 2.0.0
amendment; it does not implement commands or mark feature acceptance tests passed.

| Analysis finding | Resolution | Planned evidence |
| --- | --- | --- |
| C1: administrative execution-time bounds | Constitution 2.0.0 distinguishes enforced workflow limits and bounded application-controlled administrative work from documented filesystem/keyring calls without safe cancellation. plan.md inventories calls, no guaranteed total deadline, and failure/partial-state behavior; safe supported timeouts must still be used. | T007, T017–T018, T020–T021, T024, T027–T028, T039–T040; implementation evidence pending. |
| I1: red tests versus per-task green gates | tasks.md defines related red-to-green slices; test/implementation tasks remain open until green, unrelated work cannot advance while red, and ordered full gates run at each task closure. | At this 2026-10-01 review, all original 42 tasks were unchecked; current 44-task completion state is recorded in Phase 8 below. |
| A1: undefined offline context compatibility | Valid customized endpoints are preserved regardless of listener settings; readiness checks metadata, declared identity when present, and local admin authorization, not connectivity. Existing absent reference uses `KAKUNE_TOKEN`; absent identity remains absent; absent metadata uses the canonical secure reference. | T020, T023, T027; scenario matrix in quickstart.md. |
| U1: ambiguous first-use database states | Only an absent database under guards permits eligibility. Existing supported current/legacy schemas remain existing installations; zero-byte/schema-less files fail unchanged. Migration-created identity never grants first-use eligibility. | T007, T011, T018, T021; state matrix in data-model.md. |

All 17 functional requirements and six buildable success criteria retain planned coverage
in the original 42-task plan. IDs and unchecked states were preserved at the time of this review.
Both checklists have
been re-reviewed for requirements quality; no outstanding design blocker was found in the
four identified issues. Actual native-platform results and acceptance evidence remain pending.

Readiness artifact checks passed: all 11 Markdown artifacts have valid local links and
balanced code fences; task IDs T001–T042 are unique, contiguous, and unchecked; the spec
retains 17 FRs and six SCs; both requirements-quality checklists have no unchecked items.
The prerequisite script resolved spec 001 with spec/plan/tasks present, using no-persist mode.
Final ordered baseline gates for this synchronization are reported in the completion response;
they do not establish acceptance of the planned feature.

**Next step**: `/speckit.implement` for spec 001, beginning with T001–T003 baseline work
and T004–T006 foundation. Do not start story implementation before the foundation is complete.

## Phase 1 Setup Evidence — 2026-10-01

### T001 — Toolchain, dependency, and baseline verification

- `rust-toolchain.toml` pins Rust `1.98.1` with `clippy` and `rustfmt`; the active toolchain is `1.98.1-x86_64-pc-windows-msvc`. Cargo reports `1.98.1`.
- `Cargo.toml` retains Clap `4.6.6`, Tokio `1.53.1`, rusqlite `0.40.2`, keyring `4.2.0`, serde/serde_json/serde_yaml, uuid, thiserror, and zeroize. `Cargo.lock` contains the corresponding locked packages (including serde `1.0.229`, serde_json `1.0.151`, serde_yaml `0.9.34+deprecated`, uuid `1.26.0`, thiserror `2.0.20`, and zeroize `1.9.0`). No dependency or toolchain change was made.
- Ordered baseline gates on Windows x86_64 all passed: `cargo fmt --check`; `cargo clippy --locked --all-targets -- -D warnings`; `cargo test --locked` (117 passed, 0 failed, 2 explicitly credential-gated tests ignored). The test build emitted informational MSVC linker stdout warnings only.
- Git ignore setup was verified for this Rust repository. The existing `.gitignore` already covered `target/`, local data/config/secrets/logs, OS metadata, and Rust backup/profiling files; root-level `debug/` and `release/`, `*.rlib`, general `*.prof*`, editor directories, and temporary/swap files were added. No Dockerfile, ESLint/Prettier/npm, Terraform, or Helm setup was found, so no corresponding ignore file applies.
- `Cargo.lock` and `rust-toolchain.toml` remain unchanged.

### T002 — Advertised command and shared-caller inventory

The inventory was checked against `src/main.rs` command declarations (lines 22–377), dispatch
(390–614), the built executable's `--help`, and the named-symbol search across `src/`. The current
advertised surface is:

| Group / entry point | Current advertised operations | Source evidence / disposition |
| --- | --- | --- |
| General | `init`; built-in `--version` option | `src/main.rs:22–46`; retain `init` and `--version`. `version` subcommand is absent and must be aligned without removing `--version`. |
| `daemon` | foreground default; `start`, `stop`, `status` | `src/main.rs:115–141, 789–915`; retain lifecycle and startup behavior. |
| `storage` | `backup`, `restore`, `retain`, `compact`, `pin` | `src/main.rs:143–180, 628–671`; retain. |
| `service` | `install`, `uninstall`, `start`, `stop`, `status` (hidden `run` is not advertised) | `src/main.rs:182–197, 616–626`; retain platform behavior. |
| `doctor` | local configuration/storage/runtime diagnostics | `src/main.rs:59–65, 421–432`; retain as diagnostics, not initialization. |
| `context` | `add`, `list`, `use`, `inspect`, `remove`, `import`, `export` | `src/main.rs:66–70, 199–245`; retain. |
| `auth` | `recover`, `pair`, `tokens`, `revoke` | `src/main.rs:71–75, 247–283, 1113–1159`; retain deliberate recovery separately from initialization. |
| `workflow` | `validate`, `list`, `enable`, `disable` | `src/main.rs:76–80, 356–377, 435–489`; retain; missing AGENTS.md-required workflow operations remain product requirements. |
| `plugin` | `prepare`, `commit`, `list`, `remove` | `src/main.rs:81–85, 285–311`; retain. |
| `provider` | `list`, `upsert`, `status`, `remove`, `login` | `src/main.rs:86–90, 313–344`; retain. |
| `mcp` | `call` | `src/main.rs:91–95, 346–354`; retain. |
| Standalone execution/inspection | `run`, `executions`, `inspect` | `src/main.rs:96–113, 558–611`; retain. |

Clap also supplies `help`; `--version` is currently the only version entry point. `kakune version`
is missing. The current workflow command group also lacks the required `create`, `import`,
`export`, `plan`, and grouped `run`; the required `execution` group and its `list`, `inspect`,
`cancel`, and `logs` commands are not advertised (some functionality has differently named
top-level entries). These are recorded as existing command-surface gaps, not removed or
redesigned by this General-command phase.

Shared setup/access call sites and compatibility findings:

| Symbol / owner | All source callers observed | Evidence and impact |
| --- | --- | --- |
| `configure_local_connection` (`src/main.rs:1187`) | Init at `411`; foreground daemon preparation at `826`; background daemon preparation at `888` | Calls `ensure_bootstrap_token` at `1193`, writes local context, and reports setup. Init has the documented plaintext fallback and overwrite gaps; safer init behavior must be additive/scoped so daemon startup remains compatible. |
| `save_local_context` (`src/main.rs:1423`) | `configure_local_connection` at `1221`; explicit `recover_local_auth` at `1271`; a unit test at `1746` | It replaces an existing `local` context. Keep `auth recover`'s deliberate mutation contract; initialization preservation must not silently change recovery or daemon consumers. The helper is in `main.rs`, not `client_config.rs`. |
| `Store::open` implementation (`src/store.rs:427`) | `src/main.rs`: `402`, `424`, `441`, `459`, `467`, `485`, `492`, `506`, `514`, `520`, `547`, `561`, `583`, `595`, `634`, `647`, `655`, `664`, `685`, `698`, `703`, `715`, `722`, `825`, `845`, `884`, `1119`, `1134`, `1152`, `1308`; restore path in `src/store.rs:629` | CLI callers cover init, doctor, workflow, plugin, MCP, run/execution inspection, storage, provider, daemon, auth, and pairing. Opening/migration policy remains in `store.rs`; init-specific provenance must not alter ordinary callers. Other source hits are test setup in `src/api.rs`, `src/runtime.rs`, `src/scheduler.rs`, and `src/store.rs`. |
| `ensure_bootstrap_token` (`src/store.rs:787`) | Direct production calls: daemon serving at `src/main.rs:850`; shared local-connection helper at `src/main.rs:1193` (reached from init and daemon preparation) | Existing daemon/bootstrap and explicit recovery behavior is retained; first-use-only issuance must use an initialization-specific boundary. Test calls also occur in `src/api.rs` tests. |
| `src/client_config.rs` | No callers/definitions for the four named symbols | It owns `ContextFile::load`/`save` (`:40`, `:52`) and context validation. Changes there must preserve unrelated context commands; the CLI-only `save_local_context` is a separate helper. |

No overlap is proposed for deprecation. Keep `init`, `doctor`, daemon lifecycle, and `auth
recover` as distinct public purposes; align only the General initialization/version contract, and
preserve all other advertised commands and AGENTS.md requirements. No product behavior changed
as part of this inventory.

### T003 — Initial acceptance-to-evidence allocation (historical pending snapshot)

This was the initial evidence allocation before implementation. At that time, named tests were
planned for T007/T008 and related storage/compatibility tasks, and outcomes were pending. The table
has since been reconciled with executed results; its current acceptance status is summarized in the
tables above and in the Acceptance Closure section.

#### User scenarios and edge cases

| Acceptance item | Allocated evidence | Outcome |
| --- | --- | --- |
| US1-S1: empty writable first use creates valid configuration/storage/identity/admin access without starting Core | `tests/initialization.rs::initializes_empty_installation_with_secure_admin_access_and_explicit_paths`; `src/main.rs::injected_init_adapter_reports_complete_and_secret_free_partial_results`; `native_keyring_first_use_and_readback`; isolated CLI smoke in T040 | Pass with injected backend and native Windows and Linux x86_64/glibc keyring checks; macOS is theoretical and outside acceptance |
| US1-S2: explicit data/config/context paths are honored, including spaces and non-ASCII | `tests/initialization.rs::initializes_empty_installation_with_secure_admin_access_and_explicit_paths`; `src/main.rs::init_adapter_resolves_default_and_explicit_paths_without_environment_mutation` | Pass |
| US1-S3: secure-save failure is partial, nonzero, secret-free, and recommends explicit recovery | `tests/initialization.rs::secure_credential_failures_report_partial_setup_without_exposing_secrets`; `src/main.rs::injected_init_adapter_reports_complete_and_secret_free_partial_results` | Pass with injected backend |
| US1-S4: invalid config/unwritable required location fails without success wording or destructive replacement | `tests/initialization.rs::invalid_existing_configuration_is_preserved_and_prevents_storage_creation`; `tests/initialization.rs::configuration_publication_failure_does_not_create_storage`; `tests/initialization.rs::data_location_that_is_a_file_fails_without_overwriting_it` | Pass |
| US2-S1: ten populated/custom repeats preserve data, identity, credential history, configuration, and context | `tests/initialization.rs::preserves_populated_installation_across_ten_repeats` | Pass: 10/10 repeats; snapshot equality throughout one populated/custom fixture. |
| US2-S2: only missing resources are created and existing active selection is preserved | `tests/initialization.rs::creates_only_missing_resources_and_preserves_active_context` | Pass: missing config, complete metadata, and local entry; remote active selection and unrelated fields retained. |
| US2-S3: unavailable, expired, revoked, missing, wrong-installation, or insufficient-scope access does not rotate/revoke/reactivate | `tests/initialization.rs::rejects_unusable_existing_access_without_issuing_or_changing_credentials`; `src/main.rs::existing_initialization_failures_render_partial_paths_without_secrets_or_success_claims` | Pass: all six access states fail safely; credential history remains identical and explicit recovery is rendered. |
| US2-S4: malformed/unreadable existing YAML/JSON is retained | `tests/initialization.rs::preserves_malformed_and_unreadable_existing_yaml_and_json` | Pass: all four malformed/unreadable path cases preserve originals and issue no token. |
| US2-S5: supported legacy upgrade backs up and preserves data/identity | `tests/phase11_storage.rs::initialization_migrates_legacy_store_with_recoverable_backup`; `tests/phase11_storage.rs::failed_initialization_upgrade_keeps_original_data_and_backup_recoverable` | Pass: version-14 upgrade retains identity/workflows/executions/tokens; backup is readable; failed upgrade retains original and backup. |
| US3-S1: `version` and `--version` produce identical exact output, newline, status, and stderr | `tests/general_commands.rs::version_forms_match_exactly_without_local_state_or_remote_access` | Pass: exact `kakune 0.1.2\n`, exit 0, empty stderr. |
| US3-S2: version is independent of absent/poisoned local resources, Core, network, and credentials, with no state changes | `tests/general_commands.rs::version_forms_match_exactly_without_local_state_or_remote_access`; `src/main.rs::version_dispatch_precedes_remote_and_local_file_access` | Pass: fresh/malformed local fixtures and missing CA unchanged; early dispatch succeeds with poison context/CA paths. |
| US3-S3: a selected remote context still reports the local executable version | `tests/general_commands.rs::version_forms_match_exactly_without_local_state_or_remote_access` | Pass: selected unreachable loopback Core does not alter local version output or metadata. |
| US4-S1: help discovers local-only init and both version forms | `tests/general_commands.rs::general_help_discloses_safe_local_entry_points_and_retains_other_commands` | Pass: top-level/init/version help documents local purposes, remote rejection, accepted no-op flags, repeat safety, explicit recovery, and both version entry points. |
| US4-S2: both intentional version entry points remain supported and equivalent | `tests/general_commands.rs::version_forms_match_exactly_without_local_state_or_remote_access` | Pass: both forms remain and return byte-identical successful results. |
| US4-S3: initialization, daemon startup, diagnostics, and explicit recovery retain distinct behavior | `src/main.rs::tests::init_uses_injected_local_setup_without_starting_core_or_running_recovery`; `src/main.rs::tests::daemon_connection_preparation_keeps_bootstrap_separate_from_recovery`; `src/main.rs::tests::doctor_remains_diagnostic_and_does_not_recover_or_configure_local_access`; `src/main.rs::tests::explicit_auth_recovery_still_replaces_active_access_with_injected_credentials` | Pass: injected tests verify no Core startup, startup reuse without rotation, diagnostics without recovery, and deliberate replacement/revocation through explicit recovery. |
| US4-S4: all command overlaps receive retained/aligned/deprecation disposition and compatibility rationale | Final advertised-command inventory and overlap table in Phase 6 evidence below; `tests/general_commands.rs::general_help_discloses_safe_local_entry_points_and_retains_other_commands`; `src/main.rs` shared-call-site review | Pass: advertised commands and shared setup/access consumers are inventoried; no unrelated command is deprecated or redesigned. |
| Edge: interrupted setup retains identity/history and retry does not issue a replacement | `tests/initialization.rs::secure_credential_failures_report_partial_setup_without_exposing_secrets` | Pass |
| Edge: existing `local` context bound to another core is preserved and reported as conflict | `tests/initialization.rs::malformed_or_conflicting_local_metadata_is_preserved_before_credential_issuance` | Pass |
| Edge: adding a missing `local` context retains a different active selection | `tests/initialization.rs::creates_local_context_without_changing_existing_active_remote_selection` | Pass |
| Edge: path with spaces/non-ASCII has ordinary success and preservation semantics | `tests/initialization.rs::initializes_empty_installation_with_secure_admin_access_and_explicit_paths` | Pass |
| Edge: data path is a file; corrupted or unsupported storage fails without reset | `tests/initialization.rs::data_location_that_is_a_file_fails_without_overwriting_it`; `tests/initialization.rs::existing_zero_byte_and_schema_less_databases_fail_unchanged`; `tests/initialization.rs::unsupported_future_database_schema_fails_without_mutating_database_bytes` | Pass |
| Edge: concurrent init and shared external config/context paths cannot overwrite or issue twice | `tests/initialization.rs::independent_processes_contend_on_installation_aliases_and_shared_external_paths`; `store::tests::initial_issuance_is_single_use_across_independent_sqlite_connections` | Pass |
| Edge: `init --context` is rejected before file, database, lock, credential, or network effects | `tests/general_commands.rs::init_rejects_remote_context_before_side_effects_with_globals_before_and_after`; `src/main.rs::init_rejects_remote_context_before_filesystem_or_lock_side_effects` | Pass |
| Edge: `--standalone` and `--ca` are accepted no-ops for init | `tests/general_commands.rs::init_accepts_standalone_and_ca_without_reading_the_ca_file`; `src/main.rs::init_parser_accepts_standalone_and_ca_compatibility_no_ops` | Pass |
| Edge: version accepts valid existing global options but invalid syntax, conflicts, and unsupported version-only paths remain parser errors | `tests/general_commands.rs::version_rejects_unsupported_arguments_and_keeps_parser_conflicts` | Pass: globals work before/after `version`; `--data-dir`, `--config`, extra arguments, and subcommand conflicts remain errors. Built-in `--version` conflict short-circuit behavior is intentionally not equated. |
| Edge: zero-byte/schema-less DB fails unchanged; supported legacy DB stays existing and cannot issue first-use access | `tests/initialization.rs::existing_zero_byte_and_schema_less_databases_fail_unchanged`; `tests/initialization.rs::supported_existing_database_and_legacy_upgrade_never_issue_initial_access` | Pass |
| Edge: custom endpoint is preserved offline; identity conflicts/malformed metadata/unusable access fail safely | `tests/initialization.rs::preserves_custom_context_with_absent_optional_fields_and_environment_fallback`; `tests/initialization.rs::preserves_existing_identity_conflicts_and_retries_partial_setup_without_rotation` | Pass: endpoint differs from listener; no endpoint probe; conflicts and invalid access preserve metadata/history. |
| Edge: absent optional context fields stay absent; resolver behavior uses existing `KAKUNE_TOKEN` fallback or canonical missing-metadata reference | `tests/initialization.rs::preserves_custom_context_with_absent_optional_fields_and_environment_fallback`; `tests/initialization.rs::creates_only_missing_resources_and_preserves_active_context` | Pass: absent optionals remain absent; env fallback and canonical missing-metadata path both authorize locally. |

#### Functional requirements

| Requirement | Allocated evidence | Outcome |
| --- | --- | --- |
| FR-001 | `tests/general_commands.rs::version_forms_match_exactly_without_local_state_or_remote_access`; `tests/general_commands.rs::general_help_discloses_safe_local_entry_points_and_retains_other_commands` | Pass: both version entry points and help discovery are covered. |
| FR-002 | `src/main.rs::init_adapter_resolves_default_and_explicit_paths_without_environment_mutation`; `tests/general_commands.rs::init_rejects_remote_context_before_side_effects_with_globals_before_and_after` | Pass for US1 routing/path behavior |
| FR-003 | `tests/initialization.rs::initializes_empty_installation_with_secure_admin_access_and_explicit_paths`; `native_keyring_first_use_and_readback`; isolated CLI smoke in T040/T043 | Pass with injected backend and Windows plus Linux x86_64/glibc native smoke |
| FR-004 | Ten-repeat preservation fixture; custom config/context/credential and active-selection assertions | Pass on the populated/custom fixture (10/10 repeats). |
| FR-005 | `tests/initialization.rs::supported_existing_database_and_legacy_upgrade_never_issue_initial_access`; `store::tests::initialization_opening_provenance_is_lost_when_an_unissued_store_is_reopened`; missing-only repeat completion tests | Pass: only absent storage permits first-use issuance; existing history/identity never rotates on repeat or retry. |
| FR-006 | `tests/initialization.rs::rejects_unusable_existing_access_without_issuing_or_changing_credentials`; CLI recovery/error-rendering seam | Pass for absent, inaccessible, revoked, expired, wrong-store, and insufficient-scope credentials. |
| FR-007 | Credential write/readback failure doubles, report/error/metadata checks, injected CLI output checks, and `native_keyring_first_use_and_readback` | Pass for deterministic failures and native credential save/readback on Windows and Linux x86_64/glibc |
| FR-008 | Complete and partial component-report assertions plus CLI status/output mapping | Pass for US1 complete and tested partial outcomes |
| FR-009 | Malformed/conflicting/unwritable resource preservation and partial-retry tests | Pass for US1/US2 matrices, including malformed metadata, conflicts, missing-access retry, and no replacement. |
| FR-010 | `tests/initialization.rs::supported_existing_database_and_legacy_upgrade_never_issue_initial_access`; initialization legacy/corrupt/unsupported/failed-upgrade cases in `tests/phase11_storage.rs` | Pass for supported v14 migration, backup preservation, corrupt/future schemas, and forced transactional upgrade failure. |
| FR-011 | Exact executable byte comparison for both forms, including one newline, empty stderr, and exit 0 | Pass: `kakune 0.1.2\n` for both forms. |
| FR-012 | Version tests with missing/poisoned files and selected remote context; side-effect snapshots and early-dispatch seam test | Pass for the tested absent/malformed local state and selected unreachable Core matrix. |
| FR-013 | `tests/general_commands.rs::general_help_discloses_safe_local_entry_points_and_retains_other_commands`; README General commands section; injected CLI compatibility tests | Pass for help/documentation and distinct init/doctor/daemon/recovery purposes. |
| FR-014 | Final advertised-command inventory and overlap dispositions below; top-level and grouped help assertions; injected shared-caller regressions | Pass: all currently advertised entries retained; missing AGENTS.md command requirements remain recorded as requirements, not claimed as implemented. |
| FR-015 | Version equivalence, init routing/no-op option tests, and exact help command inventory | Pass: both version forms remain available; no additional setup/version alias is advertised. |
| FR-016 | All 16 acceptance-scenario rows and 11 edge-case rows above; named deterministic tests and T040/T043 opt-in Windows/Linux evidence | Pass for the isolated automated matrix and both required native environments; macOS is explicitly outside acceptance |
| FR-017 | `tests/initialization.rs::independent_processes_contend_on_installation_aliases_and_shared_external_paths`; `store::tests::initial_issuance_is_single_use_across_independent_sqlite_connections` | Pass for tested process/connection races |

#### Success criteria and opt-in platform evidence

| Criterion / environment | Allocated evidence | Outcome |
| --- | --- | --- |
| SC-001 | Fake-backend first-use acceptance suite plus one-invocation native keyring success/readback smoke on each required acceptance platform | Pass: Windows x86_64 and Linux x86_64/glibc passed; macOS arm64 is theoretical and excluded from acceptance |
| SC-002 | Ten consecutive runs for each populated/custom fixture with before/after snapshots | Pass for the populated/custom fixture tested: 10/10 runs; zero changes to workflow/execution records, identity, token records/status, config bytes, context bytes, or user-owned fields. |
| SC-003 | Both executable version forms, exact output and no-side-effect matrix | Pass for the US3 executable matrix and side-effect snapshots. |
| SC-004 | Full deterministic failure matrix with nonzero status, actionable guidance, no secret disclosure, and resource-preservation assertions | Pass for the US2 deterministic matrix: 15/15 credential/resource/storage failure variants preserve state and emit secret-free typed errors; two bounded contention outcomes are safe. CLI seam verifies partial wording, selected paths, and recovery guidance. |
| SC-005 | Completed command/overlap inventory and a recorded result for every scenario and requirement row | Pass: all scenario rows have named evidence and pass/blocked outcomes; required entry points and overlap dispositions are retained. |
| SC-006 | Excluding build time, measure empty-init and both version forms on an idle supported workstation; record environment and measured times, not hard-timeout claims | Pass: idle Windows x86_64 rerun in T044 met all numeric targets; exact samples and host-load evidence are recorded below. |
| Windows x86_64 (required) | Opt-in `native_keyring_first_use_and_readback` and `native_filesystem_publication` smoke; record OS/toolchain and outcome | Pass: both tests passed on Windows 11 Pro 10.0.26200 with Rust 1.98.1 MSVC. |
| Linux x86_64 glibc (required) | Opt-in `native_keyring_first_use_and_readback` and `native_filesystem_publication` smoke; record OS/toolchain and outcome | Pass: both tests passed in isolated Debian 12 `linux/amd64` with glibc 2.36, Rust 1.98.1, and ephemeral GNOME Keyring Secret Service. |
| macOS arm64 (theoretical; excluded) | No native validation required for feature acceptance; do not infer behavior from other platforms | Not validated; no macOS host or runner is available. This is a documented limitation, not an acceptance blocker. |

Required Windows and Linux checks must use isolated paths and a disposable per-instance credential,
never the normal Kakune installation. Native credential/backend checks are opt-in and are not part
of default tests. A missing required-platform result must be reported as blocked, not passed based
on fake-backend coverage. macOS is outside this feature's acceptance matrix and is explicitly
unvalidated; no result is inferred for it.

## Phase 3 Implementation Evidence — User Story 1 — 2026-10-01

The deterministic Windows x86_64 suite exercised the US1 coordinator through an injected
per-instance credential backend. All 15 tests in `tests/initialization.rs`, all 4 tests in
`tests/general_commands.rs`, and all 7 CLI seam tests in `src/main.rs` passed. Named outcomes:

| Evidence | Observed result |
| --- | --- |
| `initializes_empty_installation_with_secure_admin_access_and_explicit_paths` | Pass: valid YAML and SQLite, stable identity, one admin token, successful secure write/readback/local authorization, all five report components `created`, and only a canonical reference in local metadata. Paths included spaces and non-ASCII characters. |
| `secure_credential_failures_report_partial_setup_without_exposing_secrets` | Pass for write, readback, and mismatched-secret doubles: configuration/storage/identity remain created, client access is incomplete, local context is unattempted, one durable token record remains, the token is absent from errors/reports, and retry does not issue another credential. |
| `invalid_existing_configuration_is_preserved_and_prevents_storage_creation`; `configuration_publication_failure_does_not_create_storage`; `data_location_that_is_a_file_fails_without_overwriting_it` | Pass: invalid/unwritable resources are not replaced; reports identify the affected component and retain completed-state distinctions. |
| `malformed_or_conflicting_local_metadata_is_preserved_before_credential_issuance`; `creates_local_context_without_changing_existing_active_remote_selection`; `initialization_context_publication_failure_retains_first_use_history` | Pass: malformed/conflicting metadata is preserved before issuance where detectable; missing local metadata merges without changing a remote selection; late publication failure retains the durable credential history. |
| `existing_zero_byte_and_schema_less_databases_fail_unchanged`; `unsupported_future_database_schema_fails_without_mutating_database_bytes`; `supported_existing_database_and_legacy_upgrade_never_issue_initial_access` | Pass: ambiguous/corrupt/future storage is not reset; existing current and supported legacy storage is not classified as new; the legacy path backs up before migration and does not issue initial access. |
| `independent_processes_contend_on_installation_aliases_and_shared_external_paths`; `initial_issuance_is_single_use_across_independent_sqlite_connections` | Pass: separate processes receive bounded contention for normalized aliases/shared context paths; only one first-use admin record is issued; guards release after completion/failure/process exit. |
| CLI seam and executable routing tests | Pass: path defaults/overrides are injectable, `init --context` rejects before setup effects, `--standalone`/`--ca` parse as no-ops, and ordinary invalid arguments remain parser errors. |

Ordered gates on the pinned Windows x86_64 toolchain passed: `cargo fmt --check`,
`cargo clippy --locked --all-targets -- -D warnings`, and `cargo test --locked` (147 passed,
0 failed, 2 explicitly credential-gated tests ignored). MSVC emitted informational linker stdout
messages during test builds; Clippy remained warning-free.

**MVP checkpoint**: deterministic first-use and partial-failure behavior is independently tested
and passed. This is not evidence of a native OS-keyring first-use success: the opt-in native
credential smoke was not run. Linux x86_64 glibc and macOS arm64 checks are unavailable in this
Windows session. SC-006 timings were not measured and remain pending. US2–US4 preservation,
version, help/documentation, complete acceptance reconciliation, and platform smoke tasks remain
open for their later phases.

## Phase 4 Implementation Evidence — User Story 2 — 2026-10-01

- T020 preservation/failure acceptance: `cargo test --locked --test initialization` passed all 21
  tests. `preserves_populated_installation_across_ten_repeats` compared storage snapshots and
  exact configuration/context bytes after each of ten runs; all ten snapshots matched. The
  fixture included a workflow, execution, multiple credential records, customized listener and
  connection values, a custom reference, and an active remote selection.
- Missing-resource behavior: `creates_only_missing_resources_and_preserves_active_context`
  passed for removed configuration, removed context metadata, and a missing local entry. Existing
  data remained unchanged; the active remote context and its exact raw fields survived a merge.
- Access readiness: six injected states (absent, inaccessible, revoked, expired, wrong-store, and
  insufficient-scope) failed without creating/reactivating/revoking/replacing any token record.
  Existing optional fields, a custom endpoint that differs from the listener, and the
  `KAKUNE_TOKEN` fallback were verified without endpoint/network access.
- Resource and recovery failures: four malformed/unreadable YAML/JSON cases, a local identity
  conflict, and retry after unavailable existing access all retained the original resource and
  credential history. Retrying with the original usable access completed without issuance.
- T021 storage/migration acceptance: `cargo test --locked --test phase11_storage` passed all six
  tests. Initialization upgraded supported schema v14 to v15 with a readable v14 backup and
  identical identity/workflow/execution/token snapshots. Corrupt and future-schema databases
  remained byte-identical; a forced transactional migration failure retained both original data
  and its pre-upgrade backup. No schema version/table/column change was introduced by this feature.
- T024/T028 publication and concurrency: failed atomic publication preserved destination bytes
  and cleaned owned temporary files. Independent processes contending on normalized aliases and
  shared external paths returned bounded conflict results; contenders did not expose partial
  context writes, while the lock owner subsequently merged local metadata and preserved the
  active remote entry. Unchanged contexts remained byte-identical across repeats.
- T027 CLI seam: invalid access, conflicting/malformed metadata, unsupported storage, initial
  secure-save failure, and retry after secure-save failure rendered incomplete status and selected
  paths without raw credential values or premature success wording; access failures directed to
  explicit recovery. The typed credential boundary does not carry arbitrary backend error text.
- SC-002 result for the populated/custom fixture: 10/10 repeat runs; zero workflow, execution,
  identity, credential-record/status, configuration-byte, context-byte, active-selection, or
  user-owned-field differences.
- SC-004 result for the US2 deterministic library/storage matrix: 15/15 credential/resource/
  storage failure variants returned typed failures, preserved protected state, and exposed no
  token value; two concurrent contenders returned safe bounded contention. CLI error rendering
  also passed its secret/path/recovery assertions. This is US2 evidence, not full-feature
  acceptance.
- Environment and limitations: Windows x86_64 with the pinned Rust 1.98.1 MSVC toolchain; all
  tests used isolated filesystems/SQLite and injected credentials. Native OS-keyring checks were
  not run. Linux x86_64 glibc and macOS arm64 remain unavailable here; SC-006 timings remain
  pending.
- Ordered full gates after the Phase 4 changes passed: `cargo fmt --check`,
  `cargo clippy --locked --all-targets -- -D warnings`, and `cargo test --locked` (158 passed,
  0 failed, 2 explicitly credential-gated tests ignored). MSVC emitted informational linker
  stdout messages only.

## Phase 5 Implementation Evidence — User Story 3 — 2026-10-02

- T030 red-to-green contract: before the implementation, the version output test failed because
  `version` was not a supported subcommand. After the change, `cargo test --locked --test
  general_commands version_` passed both version tests. The suite asserts exact stdout bytes,
  one trailing LF, empty stderr, and exit 0 for `version` and `--version`.
- Local-state and routing matrix: both forms passed with an absent data location and malformed
  default YAML; malformed context metadata with remote selection; a valid context targeting an
  unavailable loopback Core; and a missing CA path. Snapshot assertions found no file changes or
  creation. Valid global options passed before and after `version`. Unsupported `--data-dir`,
  `--config`, extra arguments, and the `version` subcommand's existing `--standalone`/`--context`
  conflict remained ordinary parser errors. The built-in `--version` conflict path is not required
  to match subcommand conflict short-circuit behavior.
- T032 early-dispatch evidence: `src/main.rs::version_dispatch_precedes_remote_and_local_file_access`
  passed with explicit remote selection and nonexistent poison context/CA paths. Review confirms
  `execute` checks `Command::Version`, renders the root Clap long-version, and returns before
  context routing, initialization, configuration, CA, or credential access; unrelated command
  dispatch remains below that branch.
- Exact output in this build: `kakune 0.1.2\n` (UTF-8 hex `6B616B756E6520302E312E320A`), status 0,
  and zero stderr bytes for both forms. A fresh `KAKUNE_DATA_DIR` remained absent after ten manual
  invocations (five per form).
- SC-006 version timing: already-built `target/debug/kakune.exe` on Windows x86_64 (X64), pinned
  Rust 1.98.1 MSVC environment; Cargo build time excluded. Five process-wall-time samples per form
  (including process startup): `version` min/median/max 14.970/15.487/21.676 ms;
  `--version` 13.730/14.437/16.234 ms. All ten invocations were below two seconds and had exact
  output/status/stderr. Host load was not instrumented; these are observed measurements, not an
  enforced deadline. Empty-initialization timing and native credential/platform checks remain
  pending.
- Environment: Windows x86_64, Rust 1.98.1 MSVC; automated fixtures used isolated temporary paths
  and no live Core, network service, or native credential facility.
- Ordered full gates passed after the US3 changes: `cargo fmt --check`,
  `cargo clippy --locked --all-targets -- -D warnings`, and `cargo test --locked` (161 passed,
  0 failed, 2 explicitly credential-gated tests ignored). MSVC emitted informational linker stdout
  messages only.

## Phase 6 Implementation Evidence — User Story 4 — 2026-10-02

### Final advertised command inventory

The built Windows executable's `--help` output was reviewed after the General help changes.
`tests/general_commands.rs::general_help_discloses_safe_local_entry_points_and_retains_other_commands`
asserts the exact advertised top-level command-name list, the built-in `--version` option,
the relevant General help text, and each currently advertised grouped command. This assertion
does not imply that unadvertised AGENTS.md command requirements have been implemented.

| Group / entry point | Currently advertised operations | Final disposition / evidence |
| --- | --- | --- |
| General | `init`, `version`; built-in `--version` | Retain both version forms; `version` reports the invoked local build. `init` is local-only. No `setup` alias or additional General alias is advertised. Help and version equivalence tests pass. |
| `daemon` | Foreground default; `start`, `stop`, `status` | Retained. Foreground and background preparation continue through the ordinary shared local-connection/bootstrap path; compatibility seam test passes without starting a daemon. |
| `storage` | `backup`, `restore`, `retain`, `compact`, `pin` | Retained; grouped help regression passes. |
| `service` | `install`, `uninstall`, `start`, `stop`, `status` (`run` remains hidden) | Retained; grouped help regression passes. No service process is launched by compatibility tests. |
| `doctor` | Local configuration, storage, and idle-runtime diagnostics | Retained as diagnostics; injected test confirms existing credential history is unchanged and no local client setup/recovery is performed. |
| `context` | `add`, `list`, `use`, `inspect`, `remove`, `import`, `export` | Retained; grouped help regression passes. |
| `auth` | `recover`, `pair`, `tokens`, `revoke` | Retained. `recover` remains explicit mutation; injected regression confirms new access is authorized and prior active access is revoked. |
| `workflow` | `validate`, `list`, `enable`, `disable` | Retained; grouped help regression passes. AGENTS.md-required `create`, `import`, `export`, `plan`, and grouped `run` remain product requirements, not advertised or claimed complete here. |
| `plugin` | `prepare`, `commit`, `list`, `remove` | Retained; grouped help regression passes. |
| `provider` | `list`, `upsert`, `status`, `remove`, `login` | Retained; grouped help regression passes. |
| `mcp` | `call` | Retained; grouped help regression passes. |
| Standalone execution/inspection | `run`, `executions`, `inspect` | Retained. The distinct required `execution list/inspect/cancel/logs` group remains an existing product requirement, not an implementation claim. |
| Clap-generated help | `help` | Retained. |

No currently advertised unrelated command was removed or deprecated. The help test validates
the existing surface and intentionally does not require missing product commands to appear.

### General/shared-setup overlap dispositions

| Overlap / source call site | Final disposition | Compatibility evidence |
| --- | --- | --- |
| `init` (`execute_with_credentials` -> `run_initialization_with`; `src/initialization.rs`) | Uses the initialization-specific coordinator and credential injection boundary. Rejects explicit `--context`, does not start Core, does not route remotely, and does not invoke daemon bootstrap or explicit recovery. Secure first-use creation is separate from ordinary startup policy. | `init_rejects_remote_context_before_side_effects_with_globals_before_and_after`; `init_uses_injected_local_setup_without_starting_core_or_running_recovery`; initialization acceptance suite. |
| `version` and `--version` (`execute_with_credentials` early return) | Retain both. They render the local build version before remote routing or local resource access; successful outputs remain byte-identical. | `version_forms_match_exactly_without_local_state_or_remote_access`; `version_dispatch_precedes_remote_and_local_file_access`; General help test. |
| Foreground/background daemon preparation (`execute_daemon`, `start_background_daemon`, `configure_local_connection`) | Retained ordinary bootstrap behavior through `Store::ensure_bootstrap_token`; a repeat with existing access does not become explicit recovery. The secure credential operation is injected only at the preparation seam for deterministic tests. | `daemon_connection_preparation_keeps_bootstrap_separate_from_recovery`; source review confirms both foreground and background callers continue through `configure_local_connection`. Tests do not launch a daemon or service. |
| `doctor` (`Command::Doctor`) | Remains a configuration/storage/runtime diagnostic path. It does not call initialization's credential coordinator, configure a local context, or recover access. | `doctor_remains_diagnostic_and_does_not_recover_or_configure_local_access`; isolated fixture confirms the pre-existing token record and revocation state are unchanged and no context file is created. |
| `auth recover` (`execute_auth` -> `recover_local_auth`) | Remains an explicit recovery operation: it creates replacement access, writes the local connection, and revokes previous active tokens. Initialization never invokes it. | `explicit_auth_recovery_still_replaces_active_access_with_injected_credentials`; store recovery regression remains passing. |
| `save_local_context` shared by daemon preparation and recovery | Existing consumers remain on the established helper. Initialization instead uses guarded context validation/merge in `client_config.rs`, preserving its initialization-only policy boundary. | Daemon/recovery injected tests above; initialization context preservation and active-selection fixtures. |
| `Store::open` and migrations | Existing open/bootstrap/migration ownership remains unchanged for ordinary command consumers. Initialization uses its guarded provenance-specific open and issuance path; schema and upgrade requirements remain unchanged. | `phase11_storage` migration/backup suite and initialization provenance tests pass; T035 shared-caller regressions pass. |

The deliberate General compatibility changes are limited to those specified: `version` is added
while `--version` is retained; `init` rejects remote context selection, accepts `--standalone`/
`--ca` as no-ops, issues initial access only for demonstrably new storage, preserves repeat-run
state/custom connection metadata, and never prints a plaintext token fallback. Lost access remains
the user's explicit `auth recover` action. No daemon, doctor, service, workflow, execution, or
other authentication contract was deprecated or redesigned.

- T034 red-to-green evidence: the new help test first failed because `init --help` only described
  directory/database creation. It passes with the updated descriptions and retained command
  inventory assertions.
- T035 compatibility evidence: injected initializer, daemon-preparation, doctor, and auth-recovery
  tests pass without a native credential facility or live daemon/service process.
- T037 documentation evidence: README now records all three General entry points, path selection,
  complete/partial initialization, secure-save/readback requirements, repeat preservation, and
  a selected-path explicit recovery example; it distinguishes init's no-token-fallback policy
  from the existing recovery fallback.
- Ordered gates after the Phase 6 source/documentation changes passed: `cargo fmt --check`,
  `cargo clippy --locked --all-targets -- -D warnings`, and `cargo test --locked` (166 passed,
  0 failed, 2 explicitly credential-gated tests ignored). Windows MSVC emitted informational
  linker stdout messages only. Native credential checks and other target platforms remain
  pending for Phase 7; this is not full feature acceptance.

## Phase 7 Polish & Cross-Cutting Evidence — 2026-10-03

### T039 — Security, ownership, and bounded-work audit

- Reviewed the changed initialization paths in `src/initialization.rs`, `src/config.rs`,
  `src/client_config.rs`, `src/store.rs`, and `src/main.rs`, plus their focused tests.
- **Safe Rust and ownership:** no project-authored `unsafe` was found in the reviewed paths.
  Initialization orchestration, resource validation, persistence, and credential authorization
  remain in the library; argument routing and all console output remain in the CLI. No Core,
  network, provider, or daemon startup is introduced by initialization.
- **Bounds:** setup acquires at most three deduplicated OS-backed file guards in deterministic
  order with nonblocking `try_lock`; SQLite initialization uses a five-second busy timeout per
  wait. Test marker waits and child-process lifetimes have finite deadlines, and timed-out test
  children are killed and reaped. Filesystem and OS credential calls without a supported safe
  cancellation mechanism remain synchronous, with no application retry/poll loop; this matches
  the documented administrative-I/O allowance in constitution IX and plan.md.
- **Secret handling:** the initializer's result and typed error contain no credential value;
  transient secrets use `CredentialSecret`'s zeroizing drop and redacted `Debug`; the production
  adapter maps keyring errors to fixed safe categories. Initialization JSON contains only
  portable references. The `init` branch has no plaintext fallback and prints success only after
  all components are ready. The distinct ordinary daemon/bootstrap and explicit recovery
  behaviors remain unchanged and covered by injected compatibility tests.
- **Failure/preservation:** guarded no-replace config publication, guarded context merge,
  atomic temporary-file publication, existing-store provenance checks, and transactional
  first issuance preserve existing resources and durable retry history. Automated fixtures cover
  malformed/unreadable paths, unsupported storage, credential write/readback failures, and
  publication failures without exposing secrets.
- **Findings:** no in-scope audit finding required a code change. Native credential-manager and
  platform-specific publication evidence is handled separately by T040 and is not inferred from
  the deterministic fixture results.
- **Constitution review:** principles V–XII applicable to these changes remain satisfied; no
  workflow execution limits, storage schema, or unrelated command contract was changed.
  Windows x86_64 is the only platform available in this session; cross-platform evidence is
  recorded in T040.

### T040 — Isolated quickstart and native platform checks

This records the original T040 run before the later Linux container validation and the clarified
platform-acceptance scope. The current required-platform matrix is summarized at the start of this
review and completed under T043 below.

- **Available environment:** Windows 11 Pro 10.0.26200, x86_64, pinned Rust 1.98.1 MSVC.
  Linux x86_64 glibc and macOS arm64 are unavailable in this session and are not inferred from
  Windows results.
- **Opt-in filesystem smoke:**
  `cargo test --locked --test initialization native_filesystem_publication -- --ignored --exact`
  passed. It exercised native first-use no-replace file creation, recreation of a missing config,
  existing-context atomic merge/replacement, preservation of unrelated context fields/selection,
  and temporary-file cleanup.
- **Opt-in native keyring smoke:**
  `cargo test --locked --test initialization native_keyring_first_use_and_readback -- --ignored --exact`
  passed. The production `SystemCredentialAccess` saved and read back the first-use credential,
  local admin authorization succeeded, and the disposable per-installation credential was
  deleted and verified absent. These checks remain ignored in the default suite.
- **Built-executable quickstart:** after `cargo build --locked --bin kakune` (build excluded from
  timings), a fresh isolated path containing spaces and non-ASCII characters initialized fully
  in one invocation: exit 0, complete report, empty stderr, and all five required components
  created. Wall time including process start was 102.495 ms. Repeat initialization also exited 0
  and reported every component reused. Explicit remote `init --context` returned nonzero with the
  local-only diagnostic and created no data/config/context paths. The missing CA path remained
  absent during version checks.
- **SC-006 measurements:** with the already-built executable and PowerShell process timing,
  `version` took 69.088 ms and built-in `--version` took 74.300 ms; both exited 0 with empty
  stderr and equivalent displayed version output. First-use init took 102.495 ms. All measured
  values were below the 2-second/10-second targets; host load was not instrumented, so these are
  observed workstation timings, not enforced deadlines. Automated executable tests assert the
  exact UTF-8 version bytes and trailing newline; PowerShell's file redirection itself writes
  UTF-16 capture files and is not used as byte-level contract evidence.
- **Partial-failure coverage:** the quickstart `general_commands` suite passed 7/7 tests and the
  default `initialization` suite passed 21/21 tests (two opt-in checks ignored). The injected
  write/readback/mismatched-secret failures remain partial, nonzero at the CLI seam, preserve the
  durable retry barrier, and expose no token. Native failures were not induced against the user's
  credential manager.
- Manual quickstart files remain under the isolated root
  `C:\Users\AdrianMadu\AppData\Local\Temp\opencode\kakune-phase7-8b81e9e5-560a-4a5b-99b0-b748387a79c1`.
  The temporary native keyring entry for that manual run was deleted and verified absent; no
  normal Kakune data or credential was accessed.
- **Platform matrix at this run:** Windows x86_64 native keyring/filesystem smoke passed. Linux
  x86_64 glibc and macOS arm64 were unavailable during this T040 session; T043 later supplies the
  required Linux result, and macOS was subsequently clarified as theoretical/outside acceptance.
  SC-001 and SC-006 then-current evidence was incomplete; see Phase 8 for current outcomes.
- Ordered full gates after the T040 test/evidence changes passed:
  `cargo fmt --check`, `cargo clippy --locked --all-targets -- -D warnings`, and
  `cargo test --locked` (166 passed, 0 failed, 4 ignored: two provider-credential tests and the
  two opt-in native platform smoke tests). MSVC emitted informational linker stdout messages.

### T041 — Full specification-to-evidence reconciliation

This is the Phase 7 reconciliation snapshot before T043/T044 and the maintainer's platform-scope
clarification. Its then-current partial results are superseded by the Phase 8 status below.

- Reconciled all 16 acceptance scenarios (US1–US4) and all 11 listed edge cases against the
  named tests and T040's executed Windows smoke/CLI results in the evidence tables above. Every
  scenario and edge row now has a pass result or an explicit platform limitation; no scenario is
  left without evidence.
- Reconciled all 17 functional requirements. FR-001–FR-015 and FR-017 have passing named test or
  source/inventory evidence; FR-016's isolated default suite exercises the complete scenario/edge
  matrix without requiring native credentials or live AI services. Native secure-store success
  is recorded only for Windows x86_64.
- Reconciled SC-001–SC-006 individually: SC-001 is partial (Windows native pass; Linux/macOS
  unavailable); SC-002 passes for the tested populated/custom fixture (10/10 unchanged repeats);
  SC-003 passes the exact executable contract; SC-004 passes the deterministic failure matrix;
  SC-005 passes with the command inventory and every scenario result recorded; SC-006 is partial
  because the Windows measurements met targets but idle load was not instrumented and other
  supported platforms were unavailable.
- Updated `quickstart.md` from a planning-only guide to implemented deterministic suites and
  explicit ignored native-check instructions. It links to the evidence record and distinguishes
  observed results from unavailable platform results.
- **Acceptance decision at this snapshot (superseded):** no feature-wide acceptance claim was made
  because required evidence was unresolved at that time. T043/T044 and the clarified acceptance
  scope below close the Windows/Linux feature gate; macOS remains outside that gate.
- Ordered full gates after the T041 reconciliation/quickstart changes passed:
  `cargo fmt --check`, `cargo clippy --locked --all-targets -- -D warnings`, and
  `cargo test --locked` (166 passed, 0 failed, 4 ignored). MSVC emitted informational linker
  stdout messages.

### T042 — Phase 7 ordered quality gates (historical snapshot)

- Final checks on the pinned Rust 1.98.1 Windows x86_64 MSVC toolchain passed in the required
  order: `cargo fmt --check`; `cargo clippy --locked --all-targets -- -D warnings`; and
  `cargo test --locked` (166 passed, 0 failed, 4 ignored).
- The four default-suite ignores are the two provider tests requiring explicitly configured
  external credentials and the two opt-in native smoke tests, both of which were run separately
  and passed on Windows in T040.
- `Cargo.lock` and `rust-toolchain.toml` remain unchanged. MSVC printed informational linker
  stdout messages during test builds; they did not fail formatting, Clippy, or tests.
- At that point, final test success did not close the then-open platform/performance evidence;
  see the completed Phase 8 acceptance status below.

## Phase 8 Convergence Evidence — 2026-10-03

### T043 — Required Linux validation and macOS scope disposition (complete)

- **Linux environment:** Docker Desktop Linux engine, `linux/amd64`; Debian GNU/Linux 12
  (bookworm), glibc 2.36, Rust 1.98.1 (`x86_64-unknown-linux-gnu`). The repository was mounted
  read-only and test temporary paths were bound to the approved isolated host temp directory.
- `cargo test --locked --test initialization native_filesystem_publication -- --ignored --exact`
  passed (1 passed, 0 failed). The test exercised Linux filesystem no-replace publication,
  recreation of a missing configuration, context merge/atomic replacement, preservation, and
  temporary cleanup.
- `cargo test --locked --test initialization native_keyring_first_use_and_readback -- --ignored --exact`
  passed (1 passed, 0 failed) with an ephemeral D-Bus session and GNOME Keyring 42.1 Secret
  Service. First-use secure save, readback, and local admin authorization succeeded; the test
  deleted and verified absence of its disposable credential. No host or normal Kakune credential
  store was used.
- **macOS arm64:** no macOS host or runner is available. The maintainer clarified that macOS support
  is theoretical; no result is inferred from Windows or containerized Linux, and macOS remains
  explicitly unvalidated. It is excluded from the feature acceptance matrix, so no Mac run is needed
  to complete T043.
- **SC-001/T043 status:** pass for the required Windows x86_64 (T040) and Linux x86_64/glibc
  environments. Linux filesystem and Secret Service integration passed in an isolated container;
  this does not imply desktop-host or macOS behavior. T043 is complete under the clarified scope.

### T044 — Initial Windows timing observation (superseded)

This initial attempt is retained as historical evidence. Its load readings did not establish an
idle host; the qualifying T044 rerun is recorded below.

- **Build exclusion and setup:** `cargo build --locked --bin kakune` completed before timing.
  Five new isolated first-use installations were invoked with the built `target/debug/kakune.exe`;
  each returned complete success with empty stderr. Their disposable per-installation Windows
  Credential Manager entries were deleted after each sample and checked absent. Five samples each
  were also captured for `version` and `--version`; every result matched the exact version line,
  exited 0, and had empty stderr. Process wall time includes process startup and output capture;
  Cargo build time is excluded.
- **Host:** Windows 11 Pro 10.0.26200, x86_64; AMD Ryzen 7 5800X (8 cores/16 logical processors),
  64 GiB visible memory (24.73 GiB free at start), Rust 1.98.1 MSVC. Isolated artifacts were kept
  under `C:\Users\AdrianMadu\AppData\Local\Temp\opencode\kakune-phase8-t044-df342b427f384cc387b4db783a1f1581`.
- **Observed process times (ms; five samples; median):** empty init `747.746, 113.639, 114.656,
  111.676, 115.429` (median `114.656`); `version` `29.820, 28.192, 27.837, 28.518, 27.040`
  (median `28.192`); `--version` `28.846, 25.807, 27.951, 27.392, 26.739` (median `27.392`).
  All samples were numerically below their SC-006 targets; the first init sample was a substantial
  outlier relative to the remaining four.
- **Idle-condition limitation:** WMI processor-load observations interleaved with measurements
  ranged from 3% to 37%; a follow-up ten-sample observation ranged from 0% to 65% (mean 16.6%).
  Although 24.73 GiB of memory was free, these load samples did not establish an idle workstation.
  This initial attempt did not close SC-006; the qualifying rerun is recorded below.

- The first T044 run did not establish an idle workstation; it is retained as history and superseded
  by the qualifying rerun below. T043 is complete for the required Windows/Linux matrix.
- Ordered Windows x86_64 quality gates after the Phase 8 evidence updates passed:
  `cargo fmt --check`; `cargo clippy --locked --all-targets -- -D warnings`; and
  `cargo test --locked` (166 passed, 0 failed, 4 ignored). The ignored tests are the two
  explicitly credential-gated provider tests and the two opt-in native smoke tests; the latter
  were run separately on Windows and containerized Linux. MSVC emitted informational linker
  stdout messages only.

### T044 — Idle Windows workstation rerun (complete) — 2026-10-03

- **Build exclusion and invocation:** reused the already-built `target/debug/kakune.exe`; no Cargo
  build ran during the measurements. Five empty isolated first-use invocations and five invocations
  each of `version` and `--version` were timed with a process wall-clock stopwatch. This includes
  process startup and output capture, excludes build time, and uses explicit data/config/context
  paths under the approved isolated temp directory.
- Every init returned the complete-success output, exit 0, and empty stderr. Each generated
  per-installation Windows Credential Manager entry was deleted and verified absent before the
  next sample. Both version forms returned exact `kakune 0.1.2\n`, exit 0, and empty stderr in all
  five samples.
- **Host:** Windows 11 Pro 10.0.26200, x86_64; AMD Ryzen 7 5800X (8 cores/16 logical processors),
  64 GiB visible memory, 47.74 GiB free at start; Rust 1.98.1 MSVC. `VmmemWSL` was no longer
  running after the user stopped WSL-backed applications. Artifacts remain under
  `C:\Users\AdrianMadu\AppData\Local\Temp\opencode\kakune-phase8-t044-repeat-7b06b315d8da4be1952f912100cc9cbc`.
- **Idle host evidence:** ten pre-run total-CPU samples were `0,0,0,0,0,0,3,4,0,3%` (min 0%,
  max 4%, mean 1%). Ten post-run samples were `6,6,5,8,0,0,0,0,2,0%` (min 0%, max 8%, mean
  2.7%). Free memory was 47.74 GiB. The 15 total-CPU readings interleaved between invocations
  ranged from 2% to 13%; the pre/post observations confirm the workstation was otherwise idle
  around the timed batch.
- **Observed process times (ms; five samples; median):** empty init `730.084, 107.336, 108.643,
  107.009, 109.385` (median `108.643`); `version` `28.570, 27.335, 27.813, 25.848, 27.843`
  (median `27.813`); `--version` `27.389, 24.539, 25.188, 24.584, 24.643` (median `24.643`).
  All samples were below the SC-006 targets; the first empty-init sample was a cold-start outlier,
  while all five still completed well within ten seconds.
- **SC-006/T044 status:** pass for the required idle Windows x86_64 workstation measurement. This
  does not establish timing performance on Linux or macOS; macOS remains theoretical and
  unvalidated. Feature acceptance is limited to the defined Windows/Linux scope.
- Ordered Windows x86_64 quality gates after closing T044 passed in the required order:
  `cargo fmt --check`; `cargo clippy --locked --all-targets -- -D warnings`; and
  `cargo test --locked` (166 passed, 0 failed, 4 ignored). The four ignored tests are the two
  provider tests requiring explicit credentials and the two opt-in native smoke tests; the native
  smoke tests passed separately on Windows and containerized Linux. MSVC emitted informational
  linker stdout messages only.

## Acceptance Closure — 2026-10-03

- The active acceptance scope is Windows x86_64 and Linux x86_64/glibc. Both passed native
  initialization/filesystem and secure-credential first-use checks. Linux evidence used an isolated
  Debian 12 container with glibc 2.36 and an ephemeral GNOME Keyring Secret Service.
- macOS arm64 is theoretical, unvalidated, and explicitly excluded from acceptance. It is not a
  dependency for feature completion; no Windows/Linux result is represented as macOS evidence.
- SC-001 and SC-006 pass for the defined scope; SC-002–SC-005 retain their passing evidence in the
  acceptance tables above. All 44 tasks (T001–T044) are marked `[X]` under this scope.
- Final ordered gates after the specification-wide scope update passed on Windows x86_64 with Rust
  1.98.1 MSVC: `cargo fmt --check`; `cargo clippy --locked --all-targets -- -D warnings`; and
  `cargo test --locked` (170 passed, 0 failed, 4 ignored; confirmed on the final rerun). The ignored cases require explicitly
  configured provider credentials or are opt-in native checks, which passed separately on the two
  required environments. MSVC emitted informational linker stdout messages only.
