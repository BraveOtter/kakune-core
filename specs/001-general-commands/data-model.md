# General Commands: Data Model

**Status**: Phase 1 design baseline confirmed by implementation. Existing persisted entities are retained; no schema change was introduced.

## Local installation

| Field | Representation / rule |
| --- | --- |
| Data location | Resolved `PathBuf`; identifies local storage and setup guard. |
| Database | Existing SQLite storage under the data directory; owned by `Store`. |
| Core identity | Existing persisted `core_state` identity; stable across repeats/upgrades. |
| Opening provenance | Transient guarded outcome indicating demonstrably new versus existing storage, determined before creation/migrations. Not inferred from absence of active credentials. |

One installation owns configuration, credential history, workflows, and execution records. Existing identity or any historical token prohibits new bootstrap issuance by initialization. Unreadable/corrupt/ambiguous existing storage does not become a new installation.

| Database before guarded opening | Classification / action |
| --- | --- |
| Absent | Eligible new opening; transactional issuance still rechecks history and the guarded claim. |
| Supported current schema | Existing installation; never initial issuance. |
| Supported legacy schema, including absent identity/history | Existing installation; backed-up migration may create identity, but access recovery remains explicit. |
| Zero-byte file or valid SQLite file without a supported Kakune schema | Ambiguous resource; fail unchanged before migrations/issuance. |
| Corrupt, unreadable, or unsupported schema | Fail non-destructively; never treat as first use. |

## Configuration

- Existing `CoreConfig` schema and resolved file location remain unchanged.
- Contains listener/API settings used to construct a missing local context; no new configuration key is introduced.
- Missing configuration may be created using defaults without changing persisted installation state.
- Existing files are parsed and validated, never reset or refreshed. Publication is guarded and no-replace; malformed originals remain intact.

## Client credential

| Field | Representation / rule |
| --- | --- |
| Credential ID | Existing token-record identity. |
| Token hash | Existing SHA-256 digest in `auth_tokens`; raw values are never stored there. |
| Scopes | Existing authorization scopes; initial local access requires administrative authority. |
| Expiration / revocation | Existing optional validity and revocation state; retained unchanged on initialization repeats. |
| Credential reference | Portable reference, not token material; follows existing env/keyring resolver semantics. |
| Secret value | Transient in memory and secure local credential facility; excluded from result/error/debug/log/JSON output. |

Initial creation is at most once for an eligible guarded first-use opening. Eligibility checks and insert are one immediate transaction. Authorization checks digest, expiry, revocation, and required scope against the selected store; no network connection is needed. A secure write followed by successful readback/authorization is required before reporting first-use success.

Secure persistence and SQLite are not one transaction. If persistence or readback fails, retain durable identity/history and report incomplete access. Retry checks existing access; it does not issue a replacement. Only explicit `auth recover` may change access under its existing contract.

## Connection context

Retain existing `ConnectionContext` and `ContextFile` schemas:

- Context: `id`, `name`, `endpoint`, optional `expected_core_id`, optional `color`, optional `credential_ref`.
- Collection: existing format version, context entries, optional active-context selection, and existing export bookkeeping.
- IDs remain unique under existing metadata validation. Initialization uses the existing `local` ID for a missing local entry; no new naming scheme is added.
- Preserve every existing user-owned field and unrelated entry. Check expected identity against the selected installation when present; conflicting identity fails without replacement.
- Existing optional identity/reference fields are not filled by overwriting customized metadata. If existing metadata cannot establish usable access, report incomplete/conflicting setup for explicit correction.
- If `local` is missing, add it using the selected installation and available existing reference; do not issue replacement access to fill it. Preserve an existing active selection; use the current default-selection rule only when no selection exists.
- Retain customized endpoints rather than refreshing them from changed configuration. Validate compatibility locally where possible; do not contact a Core.
- Offline compatibility checks metadata validity, expected identity when present, and administrative authorization against the selected store, not endpoint reachability or equality with listener settings. Preserve valid custom hosts/ports/schemes. Missing expected identity remains absent; an existing missing credential reference resolves through `KAKUNE_TOKEN`. Missing metadata resolves the canonical installation-specific secure reference. Invalid/unavailable access fails without adding optional fields or rotating credentials.
- Bookkeeping timestamps may change when a real save is required; unchanged metadata need not be saved.

## Installed version

- Build-time package version of the invoked executable; currently `0.1.2` in `Cargo.toml`.
- No persisted entity, installation relationship, or remote lookup.
- Both CLI forms render `kakune <installed-version>` plus one newline.

## Initialization outcome (new, transient)

| Field | Rule |
| --- | --- |
| Resolved locations | Data, configuration, and context-file paths, available even for partial reporting. |
| Component status | Fixed setup components: configuration, storage, identity, client access, local context; status created, reused, or incomplete, with unattempted work distinguished internally. |
| Completion | Complete only when every required component is ready. |
| Failure | Typed secret-free category, affected resource, completed-component summary, and corrective action. |

The initializer returns complete results or failures carrying partial reports. No raw credential belongs in either type. CLI maps complete to exit 0 and failures to nonzero, renders the report, and sends actionable errors to stderr. This is a library representation, not a new serialized public output mode.

## Setup transitions

1. Resolve paths and reject incompatible CLI routing before setup.
2. Acquire nonblocking resource guards; contention -> conflict without credential creation.
3. Inspect and validate existing resources; malformed/conflicting resources -> failure preserving originals.
4. Create missing configuration; guarded store open determines first-use provenance and runs required backed-up upgrades.
5. New eligible installation -> one durable initial credential -> secure save/readback/authorization. Existing installation -> resolve preserved access -> authorize without issuance.
6. Add only missing compatible local metadata under its resource guard; preserve active selection and existing fields.
7. All components ready -> complete. Any late failure -> partial report; durable work remains, guards release, and retry follows existing-installation rules.

Metadata validation and credential issuance are ordered to detect existing conflicts before issuance wherever feasible. Crashes after identity creation but before issuance still establish an existing installation; recovery is explicit, never an inferred permission to bootstrap again.

## Persistence and concurrency invariants

- No new table/column/version is required by this design. Store opening retains the backup-before-upgrade contract and compatible records.
- Resource identity accounts for normalized path aliases and shared explicit configuration/context paths; locks are deduplicated and acquired in deterministic order.
- Locks are OS-owned, released on exit; lock-file existence alone is not a setup state.
- Serialize into unique temporary files before publication. Never replace an existing configuration; merge contexts only while holding their guard.
- Independently opened stores/processes must satisfy at-most-one initial credential, not just clones sharing an in-process mutex.
