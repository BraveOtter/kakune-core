# General Commands: Validation Quickstart

This guide covers the implemented General command behavior and its isolated validation paths.
Deterministic suites use fake credentials by default. Native keyring and filesystem smoke tests
are explicitly ignored and must be selected by name. The required acceptance environments are
Windows x86_64 and Linux x86_64/glibc. Their evidence is recorded in [baseline-review.md](baseline-review.md).
macOS arm64 is theoretical and unvalidated; it is not required for acceptance. See
[CLI contract](contracts/cli.md) and [data model](data-model.md).

**Implementation status**: Stories 1–4 and their deterministic regression suites are present.
The Windows x86_64 native smoke checks passed on 2026-10-03. Linux x86_64/glibc filesystem and
keyring smoke checks also passed in an isolated Debian 12 Docker Desktop container using
`gnome-keyring`; this is container-backed Linux evidence, not macOS or host desktop-keyring
validation. The maintainer clarifies macOS support is theoretical; no macOS validation environment
is available, so macOS remains unvalidated and is excluded from acceptance. Native checks passed on
the required Windows/Linux environments, and T044's idle Windows timing run is complete. Results
and host conditions are in [baseline-review.md](baseline-review.md). Do not interpret one platform's
result as validation on another platform.

## Prerequisites

- Pinned Rust 1.98.1 with rustfmt and Clippy; locked dependencies.
- Required acceptance environment: Windows x86_64 or Linux x86_64/glibc. A real first-use success requires the current user's OS credential facility; automated tests use an injected deterministic backend instead of depending on it. Linux may be exercised in an isolated glibc container with a real ephemeral Secret Service. macOS is theoretical, unvalidated, and not required.
- All data and context paths isolated from normal user installations. Do not use real credentials or production databases in failure tests.

## Build and create isolated paths (PowerShell)

```powershell
cargo build --locked --bin kakune
$root = Join-Path 'C:\Users\AdrianMadu\AppData\Local\Temp\opencode' ('general-validation-' + [guid]::NewGuid())
$data = Join-Path $root 'data with spaces-é'
$config = Join-Path $root 'configuration\kakune.yaml'
$contexts = Join-Path $root 'client\contexts.json'
$exe = Join-Path (Get-Location) 'target\debug\kakune.exe'
```

On Unix use the built `target/debug/kakune` and an equivalent isolated directory; quote every path. No daemon or service is needed for these checks.

## Version without installation

```powershell
& $exe version
& $exe --version
& $exe --context unreachable --context-file $contexts --ca (Join-Path $root 'missing-ca.pem') version
& $exe --context unreachable --context-file $contexts --ca (Join-Path $root 'missing-ca.pem') --version
```

Expect the identical `kakune <installed-version>` line with newline, exit 0, no normal stderr, no files created, and no network/keyring calls. Automated tests also supply malformed metadata and configuration and inspect side effects. Each version invocation must meet the two-second target on an idle supported workstation; do not include Cargo build time.

## First-use and repeat initialization

```powershell
& $exe --context-file $contexts init --data-dir $data --config $config
$LASTEXITCODE
& $exe --standalone --context-file $contexts init --data-dir $data --config $config
$LASTEXITCODE
```

Expect exit 0 only when all required setup is complete, all three locations reported, one stable identity and one securely saved initial administrative credential, and no daemon/service/workflow/provider startup. Empty setup must meet the ten-second target excluding resource contention. If secure saving fails, expect partial completion, nonzero, and explicit recovery guidance without a token; do not treat this as an end-to-end success.

The ten-second/two-second targets are measurements, not total-operation timeout guarantees.
The plan inventories administrative filesystem/keyring calls that may await platform completion
where safe cancellation is unsupported. Locks remain nonblocking, SQLite busy waits are capped
at five seconds per wait, and init performs no automatic retry. Use deterministic failure
doubles rather than intentionally hanging a native keyring. Never call detached mutation cancelled.

Use deterministic automated fixtures for ten repeated runs on populated storage. Compare user-owned configuration values, workflows, execution history, identity, credential records/status, every custom connection field, unrelated contexts, and active selection. Ignore only allowed bookkeeping timestamps and compatible schema representation changes.

## Routing rejection

Use a separate fresh path to prove rejection happens before setup:

```powershell
$rejected = Join-Path $root 'must-remain-absent'
& $exe --context remote --context-file (Join-Path $rejected 'contexts.json') init --data-dir $rejected
$LASTEXITCODE
Test-Path $rejected
```

Expect nonzero, an actionable local-only usage diagnostic, `False`, and zero network or credential calls.

## Deterministic acceptance suites

```text
cargo test --locked --test general_commands
cargo test --locked --test initialization
```

The first suite covers executable arguments/help/version, routing, output, and exit status without a real keyring. The second exercises the exported library initializer with real isolated SQLite/filesystem fixtures and fake credential backends. CLI initialization rendering/failure mapping is also tested in `src/main.rs` through the same injected initializer seam. The default full suite does not require the native smoke tests. Do not add a production environment flag that exposes test credentials or enables plaintext persistence.

## Explicit opt-in native smoke tests

Run these individually on the required Windows x86_64 and Linux x86_64/glibc acceptance environments. Use isolated fixtures and the native platform credential facility; the Linux check may use an isolated Linux/glibc container with a real ephemeral Secret Service:

```text
cargo test --locked --test initialization native_filesystem_publication -- --ignored --exact
cargo test --locked --test initialization native_keyring_first_use_and_readback -- --ignored --exact
```

Both tests use an isolated fixture. The native keyring test creates a unique per-installation
credential, verifies secure save/readback and local authorization, then deletes the disposable
entry and verifies it is absent. Do not run the keyring check against a normal installation or
replace its isolated fixture paths with production data. Record missing required-platform results
as blockers rather than inferring them from another platform. macOS is outside this feature's
acceptance matrix and its behavior remains unvalidated; see the [baseline evidence](baseline-review.md).

Safe partial failures are deterministic by default: `tests/initialization.rs::secure_credential_failures_report_partial_setup_without_exposing_secrets` exercises write, readback, and mismatched-secret failures; `src/main.rs::tests::injected_init_adapter_reports_complete_and_secret_free_partial_results` verifies CLI mapping and output. These tests do not require or intentionally break the native credential store.

Required fixture matrix:

| Fixture | Expected observation |
| --- | --- |
| Missing configuration or metadata | Only missing setup resources created; existing state/selection preserved. |
| Revoked, expired, absent, inaccessible, or wrong-installation credential | Nonzero; no issuance/reactivation; recovery guidance. |
| Failed secure write/readback or interrupted setup | Nonzero partial report, no exposed token; retry does not rotate. |
| Custom local context / active remote context | Preserve custom values and active choice; fail on installation conflict. |
| Valid custom endpoint differs from listener host/port/scheme | Preserve it without DNS/network checks; success is offline readiness, not a connectivity assertion. |
| Missing expected identity / missing credential reference | Preserve absent fields; check local admin access via existing `KAKUNE_TOKEN` fallback when the existing reference is absent; fail safely if unavailable. |
| Zero-byte / schema-less preexisting SQLite file | Fail unchanged before migration/issuance; never classify as new. |
| Supported legacy schema without identity/history | Backed-up upgrade follows existing-installation policy; migration-created identity does not permit initial issuance. |
| Malformed/unreadable config or metadata; data path is a file | Nonzero, original unchanged, corrective guidance. |
| Supported legacy / corrupt / unsupported database | Recoverable pre-upgrade backup and preservation, or non-destructive failure. |
| Two overlapping initializations; shared explicit config/context paths | No overwritten resources; at most one initial credential per installation; conflict/retry result bounded. |
| Changed shared-helper consumers | Preserve daemon, diagnostics, and explicit recovery contracts. |

Map each Story 1–4 acceptance scenario and edge case to a named automated test or documented Windows/Linux platform result before feature acceptance. Record conformance evidence and compatibility dispositions in `baseline-review.md`; macOS is theoretical and not an acceptance dependency.

## Final quality gates

Run in this exact order, finishing with the full suite:

```text
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

Real OS-keyring smoke checks are explicit opt-in platform validation, never default-test prerequisites. Keep isolated artifacts for inspection; do not delete normal user data or indiscriminately clear the credential store. If direct workflow execution is needed for fixture validation, use `--standalone` with the isolated data directory.
