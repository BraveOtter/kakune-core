# Initialization Library Boundary

This implemented boundary satisfies constitution XII; it adds no HTTP endpoint or public CLI mode. The library/API contract is validated by the deterministic suite and the required Windows x86_64 and Linux x86_64/glibc acceptance checks. macOS arm64 remains theoretical and unvalidated.

## Inputs and dependency injection

- `InitializationPaths`: owned resolved data, configuration, and context-file paths. CLI resolves flags/defaults; the library does not parse arguments or read terminal input.
- A per-instance credential-access dependency supporting reference resolution, secure write, and readback. The production adapter retains existing keyring service/account naming and environment-reference semantics; fake adapters are passed explicitly by tests.
- No process-global test backend, test-only production environment switch, hosted service, or network client is part of initialization.

## Outputs and errors

- Complete `InitializationReport`: resolved paths and per-component created/reused/incomplete states; no secrets.
- Fallible operations return `Result`; typed `InitializationError` carries a partial report, category, affected resource, and safe recovery/correction guidance. Use existing `thiserror` rather than panics.
- Error categories distinguish invalid/conflicting/inaccessible resources, unsupported or failed persistence upgrades, unavailable/invalid client access, secure-save/readback failure, and setup contention.
- CLI renders report/error and maps complete to exit 0, all required failures to nonzero. Library performs no `println!`, `eprintln!`, prompts, or process exit.

## Store boundary

- Add an initialization-specific guarded open/provenance operation and transactional initial issuance operation in `src/store.rs`.
- Determine prior identity/history before migration manufactures a new identity. Require new-opening eligibility and recheck history in an immediate transaction for issuance.
- Retain existing ordinary store opening, backup/upgrade responsibility, authorization, and explicit recovery operations; do not expose the raw bootstrap token in an initialization result.
- Credential material is passed only through the credential dependency and authorization check, then dropped/zeroized. Never derive unrestricted secret-bearing debug output.

## Operational guarantees

Initialization guards span decisions and publication for one installation and shared external setup paths. Lock contention fails immediately; SQLite busy waits are explicitly finite (at most five seconds); no automatic recovery or indefinite retry occurs. Whole-system rollback is not promised: errors faithfully report completed durable work and retain the retry barrier.

Constitution 2.0.0 permits administrative filesystem and OS credential-manager calls without
supported safe timeout/cancellation to await platform completion. The plan inventories these
calls and their failure behavior; there is no guaranteed total wall-clock deadline. Review
available safe timeout mechanisms and use them when supported. Busy limits apply per wait,
not to total SQL/backup execution. No detached mutation may be represented as cancelled.
Deterministic doubles exercise platform errors and durable partial state, not native timing.

Only an absent database observed under guards can produce new-opening eligibility. Existing
supported legacy schemas never become eligible through migration-created identity; zero-byte
and schema-less existing databases fail unchanged. Context compatibility is an offline
metadata/identity/local-authorization check, not a reachability or listener-equality check;
preserve existing resolver fallback behavior and optional fields as specified in the CLI contract.

The exported initializer must be testable without the CLI or real credential facilities. Preserve unrelated daemon/diagnostics/recovery policy by using additive initialization-specific entry points and explicit compatibility tests for any shared helper change.
