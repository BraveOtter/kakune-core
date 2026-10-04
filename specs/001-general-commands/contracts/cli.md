# General Command CLI Contract

**Status**: Implemented and acceptance-verified for Windows x86_64 and Linux x86_64/glibc. macOS arm64 is theoretical and unvalidated, and is not an acceptance prerequisite.
**Authority**: [spec.md](../spec.md), FR-001–FR-017.

## Entry points and routing

| Entry point | Accepted options | Local behavior |
| --- | --- | --- |
| `kakune init` | `--data-dir PATH`, `--config PATH`, global `--context-file PATH`, `--standalone`, `--ca PATH` | Prepare/check the local installation; never start Core or contact a network endpoint. |
| `kakune version` | Existing global connection options | Report the invoked executable version; do not use connection options. |
| `kakune --version` | Existing global connection options | Same version result, without installation access. |

`init --context NAME` fails before filesystem, storage, credential, or network setup. `--standalone` and `--ca` are compatibility no-ops for initialization. Version entry points do not read context, configuration, CA, or credential files. Preserve existing argument syntax and parser conflicts; this does not authorize new options or acceptance of malformed command lines. In particular, `--data-dir` and `--config` remain initialization-specific, not new version options.

## Path selection

Preserve existing resolution: explicit data path, otherwise `default_data_dir()` (including `KAKUNE_DATA_DIR`); explicit configuration path, otherwise `<data-dir>/kakune.yaml`; explicit global connection-file path, otherwise `<data-dir>/cli/contexts.json`. Treat spaces and non-ASCII characters as ordinary path content. Report resolved locations without requiring that missing files already exist.

## Initialization result

- Exit 0 only when configuration, storage, identity, secure administrative client access, and compatible local connection metadata are all ready.
- Standard output identifies resolved data, configuration, and connection-file paths and created/reused/incomplete components. Exact prose and ordering are not public machine-readable schemas.
- Required failures return nonzero, with standard-error diagnostics naming the failed resource, partial-completion state, and a safe corrective action. Do not print full-success wording before all required components succeed.
- Never print credential values on either stream or include them in logs or connection metadata, including when secure saving fails.
- Recovery guidance refers to `kakune auth recover` with the selected local paths; do not run recovery automatically. Do not suggest that repeating initialization rotates access.

## Preservation and failure rules

| Existing state | Required result |
| --- | --- |
| Complete installation | Success; preserve identity, data, credential history/status, user configuration, connection fields, and active selection. |
| Missing configuration or connection metadata | Create only missing resources; report incomplete client access if it cannot be established without issuing replacement access. |
| Missing/revoked/expired/inaccessible access | Nonzero; preserve history; direct to explicit recovery. |
| `local` context bound to another installation | Nonzero conflict; preserve context and selection. |
| Malformed/unreadable resource | Nonzero; preserve original; no default replacement. |
| Supported older database | Backup before upgrade; preserve compatible records and identity. |
| Corrupt/unsupported database or failed upgrade | Nonzero; no destructive reset. |
| Secure credential save or metadata save fails | Partial setup and nonzero; preserve completed durable work; no token fallback or credential rotation on retry. |
| Concurrent initialization | At most one initial credential; no resource overwrite; unsafe contender fails with retry guidance. |

Initial issuance is permitted only when no prior persisted identity or credential history exists. Readiness requires locally verifying saved access against the selected installation, not merely finding an unrevoked credential row. Existing custom local connection values must not be refreshed from changed configuration; conflict/unusable-access correction remains explicit.

Initial eligibility additionally requires an absent database observed under setup guards.
Preexisting supported databases are existing installations even if a legacy upgrade adds
identity; zero-byte/schema-less databases fail unchanged as ambiguous resources.

Local-context readiness checks metadata validity, declared identity if present, and local
administrative authorization. Valid customized endpoints need not match current listener
settings, and success does not assert endpoint connectivity. Never resolve DNS or probe an
endpoint. Preserve absent expected identity; an existing absent credential reference uses
the existing `KAKUNE_TOKEN` fallback. Missing metadata uses the canonical secure reference.
Malformed metadata, mismatched declared identity, or unusable access returns nonzero without
rewriting custom fields.

Timing targets are acceptance measurements, not guaranteed command deadlines. Administrative
filesystem and OS credential-manager calls without supported safe timeout/cancellation may
await platform completion under constitution 2.0.0; errors return the preserved partial state.
Lock attempts remain nonblocking, SQLite setup busy waits <=5 seconds, and setup never retries
or recovers automatically. Do not report detached mutating work as cancelled.

## Version output

Both forms emit exactly the same single line and trailing newline:

```text
kakune <installed-version>
```

Exit 0; no normal stderr output. The value is the local package/build version, not a remote Core version. This must work with missing data, malformed configuration/metadata, unavailable credential facilities, and an unreachable selected Core, without reading or writing persistent state.

## Compatibility review

Retain both version forms, `init`, `doctor`, daemon lifecycle, and `auth recover`. Align only General behavior. Inventory every advertised command and shared setup call site before refactoring, retaining evidence and disposition in the [baseline review](../baseline-review.md). No deprecation or redesign of unrelated workflow, execution, service, provider, plugin, or authentication contracts is authorized.

Document the intentional initialization changes: remote context rejection, removal of plaintext fallback, first-use-only issuance, preservation of customized connection metadata, and truthful partial-failure status. Existing startup/recovery helpers must not silently inherit initialization-specific policy.
