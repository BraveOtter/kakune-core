# Feature Specification: General Command Initialization and Version Contract

**Feature Branch**: `feature/general-commands` (existing branch; no branch hook executed)

**Created**: 2026-09-30

**Status**: Accepted for the defined Windows x86_64 and Linux x86_64/glibc validation scope. macOS arm64 is theoretical and unvalidated; it is not an acceptance prerequisite.

**Readiness review**: 2026-10-01; synchronized with constitution 2.0.0. Acceptance evidence and the platform boundary were finalized on 2026-10-03; see `baseline-review.md`.

**Input**: User description: "Specify the General commands required by AGENTS.md, verify whether existing commands conform, identify equivalent or redundant commands, and define justified improvements or refactoring."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Prepare a Local Installation (Priority: P1)

As a local operator, I want `kakune init` to prepare my installation and initial client access without starting the Core, so that I can safely proceed to daemon operation.

**Why this priority**: Initialization establishes the installation and access used by subsequent operations.

**Independent Test**: Run initialization against an isolated, empty installation directory and verify the configuration, persistent storage, installation identity, and usable local client credentials without starting a daemon.

**Acceptance Scenarios**:

1. **Given** an empty writable installation location, **When** the operator runs `kakune init`, **Then** the command creates valid default configuration, storage, a stable installation identity, and initial administrative client credentials, reports the resolved locations, and exits successfully without starting a daemon or service.
2. **Given** explicit `--data-dir`, `--config`, and `--context-file` paths, **When** initialization succeeds, **Then** it uses those paths rather than normal user defaults and identifies them in its result.
3. **Given** the system cannot securely save the initial client credential, **When** initialization runs, **Then** it reports incomplete client-access setup, returns a nonzero exit status, never prints the credential value, and gives a safe next step using the existing explicit access-recovery operation.
4. **Given** invalid configuration or an unwritable required location, **When** initialization runs, **Then** it returns a nonzero exit status, identifies the failed resource and corrective action without secrets, and does not claim full initialization succeeded.

---

### User Story 2 - Repeat Initialization Without Losing State (Priority: P1)

As an existing operator, I want to repeat initialization safely, so that checking or completing setup never resets my installation or invalidates connected clients.

**Why this priority**: Silent data loss or credential changes would undermine trust in the command.

**Independent Test**: Initialize a populated fixture repeatedly, compare configuration, installation identity, workflows, execution history, credential records, and connection settings, and exercise incomplete and damaged setup separately.

**Acceptance Scenarios**:

1. **Given** a complete installation with existing workflows, executions, credentials, and customized connection metadata, **When** initialization runs again, **Then** it succeeds without replacing configuration, changing identity, losing records, creating or revoking credentials, or overwriting user-edited connection settings.
2. **Given** an otherwise valid installation missing configuration or local connection metadata, **When** initialization runs, **Then** it creates only missing setup resources, preserves existing state and active connection selection, and reports what was created or remains incomplete.
3. **Given** an existing installation with missing, revoked, expired, or inaccessible client credentials, **When** initialization runs, **Then** it preserves credential history, does not issue replacement credentials or reactivate revoked access, returns a nonzero status for incomplete client access, and directs the operator to explicit recovery.
4. **Given** unreadable or malformed existing configuration or connection metadata, **When** initialization runs, **Then** it reports the problem and preserves the original resource instead of resetting it to defaults.
5. **Given** supported older persistent data, **When** initialization upgrades it, **Then** a recoverable backup exists before the upgrade and existing user data and identity remain available afterward.

---

### User Story 3 - Identify the Installed Version (Priority: P2)

As an operator, I want both documented version forms to identify the executable I am running, so that support reports and upgrade decisions are reliable.

**Why this priority**: Version reporting is a small, independent capability needed for troubleshooting.

**Independent Test**: Compare `kakune version` and `kakune --version` from the same executable with no initialized installation and with unavailable local data and credential facilities.

**Acceptance Scenarios**:

1. **Given** the same installed executable, **When** either documented version form runs, **Then** both return the same single line `kakune <installed-version>` on standard output, terminate successfully, and produce no normal diagnostic output on standard error.
2. **Given** no initialized installation, malformed local configuration, or an unavailable Core, **When** either version form runs, **Then** it still succeeds without creating or changing files, contacting a Core, or accessing client credentials.
3. **Given** a selected remote context, **When** either version form runs, **Then** it reports the local executable version, not the remote Core version.

---

### User Story 4 - Use a Clear, Compatible Command Surface (Priority: P2)

As an operator, I want setup, diagnostics, access recovery, and version reporting to have distinct documented purposes, so that equivalent entry points do not produce conflicting behavior.

**Why this priority**: Clear boundaries prevent accidental access resets and make existing automation dependable.

**Independent Test**: Review the complete advertised command surface, classify initialization and version overlaps, and compare observable behavior of retained equivalent entry points before and after improvements.

**Acceptance Scenarios**:

1. **Given** top-level help, **When** the operator looks for General commands, **Then** `init`, `version`, and `--version` are discoverable and their local-only purposes are explained.
2. **Given** both documented version forms, **When** equivalent entry points are consolidated, **Then** both remain supported with identical results and neither is removed as redundant.
3. **Given** setup behavior is also used by daemon startup or diagnostics, **When** related behavior is improved, **Then** initialization does not start the Core, startup does not become access recovery, and diagnostics do not replace the documented initialization command.
4. **Given** a proposed change to an existing public entry point, **When** the command review concludes, **Then** it records whether the entry point is retained, aligned, or proposed for deprecation, with evidence and compatibility impact; unrelated commands are not removed or redesigned in this feature.

### Edge Cases

- Existing setup is interrupted between creating storage and saving the local client credential: report partial completion and preserve created state; a retry must not silently rotate credentials.
- A `local` connection entry already refers to another installation: preserve it and fail with actionable conflict guidance rather than replacing its identity or credentials.
- A different connection is active: retain that selection while adding a missing local connection.
- Paths contain spaces or non-ASCII characters: the same success and preservation guarantees apply.
- The selected data path is a file, storage is corrupted, or an upgrade is unsupported: fail without destructive reset.
- Two initialization attempts overlap: they must not overwrite existing resources or create multiple initial credentials; report a conflict safely if both cannot complete.
- `init` receives `--context`: reject remote selection before setup changes or network activity; do not silently route initialization to a remote Core.
- `--standalone` on `init` is accepted as a compatibility no-op because initialization is already local. `--ca` is also a no-op for initialization and version reporting because neither contacts a Core.
- Version reporting accepts the existing global connection options without using them; invalid syntax or unsupported command-specific arguments still produce ordinary usage errors.
- A preexisting zero-byte or schema-less database is ambiguous, not first use: fail without changing it. A supported legacy database remains an existing installation even if migrations must add its identity; missing access requires explicit recovery.
- Local-context readiness is an offline metadata and authorization check, not proof that its endpoint is reachable or serves this installation. Preserve valid customized endpoints even when configuration changes; reject malformed metadata, a conflicting declared identity, or unusable local administrative access without contacting the endpoint.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The command surface MUST support `kakune init`, `kakune version`, and `kakune --version`; the two version forms are intentional equivalents, not candidates for removal.
- **FR-002**: Initialization MUST target the current user's local installation by default, support existing explicit data, configuration, and connection-file paths, and never contact a selected remote Core. `--context` MUST be rejected for initialization before side effects.
- **FR-003**: First-use initialization MUST prepare valid configuration, persistent storage, stable installation identity, initial administrative client credentials, and local connection metadata without starting a daemon, installing a service, executing a workflow, or requiring an AI provider.
- **FR-004**: Initialization MUST preserve existing configuration values, identity, workflows, execution history, credentials and their status, unrelated connection entries, customized local connection settings, and active connection selection. Incidental bookkeeping timestamps may change; user-owned values may not.
- **FR-005**: Initialization MUST create only missing setup resources for an existing installation. Initial credentials MUST be issued only for a genuinely new installation, not merely because an existing installation has no currently usable credential.
- **FR-006**: If existing client access is missing, expired, revoked, or unavailable, initialization MUST report incomplete setup and point to the explicit recovery operation without creating, replacing, revoking, or reactivating credentials.
- **FR-007**: Initial credential values MUST be saved securely and MUST NOT appear in normal command output, error output, logs, or portable connection metadata. If secure saving fails, initialization MUST return a nonzero status and provide recovery guidance without exposing a token.
- **FR-008**: Initialization MUST distinguish complete success from partial or failed setup. It MUST report resolved data, configuration, and connection-file locations and identify created, reused, or incomplete setup components. Required failures MUST produce actionable standard-error diagnostics and a nonzero status.
- **FR-009**: Invalid, conflicting, or inaccessible existing resources MUST NOT be overwritten with defaults. After partial failure, existing user state MUST remain intact and retries MUST preserve completed work without silent credential replacement.
- **FR-010**: Supported storage upgrades MUST preserve existing records and identity and create a recoverable backup before upgrading. Unsupported or failed upgrades MUST fail without deleting or resetting storage.
- **FR-011**: Both version forms MUST return the identical single line `kakune <installed-version>` with a trailing newline, a successful exit status, and no normal standard-error output. The version MUST describe the invoked local executable.
- **FR-012**: Version reporting MUST work without initialized data, valid local configuration, a running Core, internet access, or available credential facilities, and MUST NOT modify persistent state or retrieve credentials.
- **FR-013**: Help and user documentation MUST describe all General entry points, initialization's local-only behavior, repeat-run safety, and the distinction between initialization, daemon startup, diagnostics, and explicit credential recovery.
- **FR-014**: The feature review MUST inventory initialization/version equivalents and shared setup behavior across the existing command surface, record evidence of conformance or gaps, and give each overlap a justified disposition and compatibility impact. Required commands in AGENTS.md MUST remain preserved.
- **FR-015**: Improvements MUST align equivalent General entry points and remove avoidable behavioral divergence where identified. Refactoring is justified only when it preserves these observable guarantees and unrelated command behavior; it MUST NOT introduce an additional public setup or version command.
- **FR-016**: Verification MUST exercise every acceptance scenario and relevant edge case using isolated installation data, without live AI services or external credentials. Existing passing behavior MUST receive preservation coverage, and corrected defects MUST receive regression coverage. Required native acceptance environments are Windows x86_64 and Linux x86_64 with glibc. macOS arm64 is a theoretical, unvalidated compatibility target and MUST NOT be required to accept this feature or be described as validated based on other platforms.
- **FR-017**: Concurrent initialization MUST preserve existing resources and result in at most one initial credential for a new installation. An attempt that cannot safely complete MUST return a nonzero status with retry guidance.

### Key Entities *(include if feature involves data)*

- **Local Installation**: The selected data location, persistent records, and stable Core identity belonging to the local operator.
- **Configuration**: Existing or newly created operating settings and their resolved location; user values must survive repeat initialization.
- **Client Credential**: Administrative access created for first use, with identity, validity, and revocation status; the secret value is not portable connection metadata.
- **Connection Context**: Named connection metadata with an endpoint, expected installation identity, credential reference, and optional user presentation settings; one context may be active.
- **Installed Version**: The release identifier of the invoked executable, independent of installation data or remote connections.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: On each required acceptance environment (Windows x86_64 and Linux x86_64/glibc) with writable locations and a secure credential facility, 100% of first-use acceptance fixtures complete setup with one initialization invocation and no manual file editing. macOS arm64 is outside this acceptance measurement because support is theoretical and no validation environment is available.
- **SC-002**: Across ten consecutive initialization runs on each populated fixture, zero existing workflows, executions, identities, credential records, or user-owned configuration and connection values are lost or changed.
- **SC-003**: In 100% of version scenarios, both forms report exactly the same installed version and leave installation state unchanged, including when configuration is invalid or the Core is unavailable.
- **SC-004**: In 100% of tested initialization failure scenarios, the operator receives a nonzero result and actionable guidance, with zero credential values exposed and zero destructive replacement of existing resources.
- **SC-005**: Every identified General-command overlap has a documented disposition and compatibility rationale; all three required entry points remain available and every acceptance scenario has a recorded pass/fail result before feature acceptance.
- **SC-006**: On an idle supported local workstation, each version form completes within two seconds and empty-installation initialization completes within ten seconds, excluding supported upgrades and environmental resource contention.

## Assumptions

- This feature concerns only the General category of AGENTS.md. Other categories are reviewed only for overlapping behavior and compatibility; their missing product commands are not implemented here.
- The current default installation location and explicit path options remain authoritative; no new location-selection mechanism or interactive setup wizard is introduced.
- Existing explicit `auth recover` remains the deliberate recovery path. Redesigning its own token-output behavior or other authentication operations is outside this feature.
- Native platform acceptance for this feature is limited to Windows x86_64 and Linux x86_64/glibc. An isolated Linux container is acceptable when the binary runs as Linux/glibc and uses an isolated Linux Secret Service and filesystem. macOS arm64 remains theoretical and unvalidated; its lack of a host or runner does not block acceptance, and Windows/Linux results do not imply macOS compatibility.
- A first-use success requires secure local credential saving. Returning failure rather than printing a fallback token is an intentional compatibility change to initialization and must be documented during implementation.
- A genuinely new installation requires an absent database under the setup guard and no prior persisted installation identity or credential history. A preexisting database is never eligible for initial issuance: supported schemas follow the existing-installation path, while zero-byte, schema-less, corrupt, or unsupported files fail conservatively without mutation. Migration-created identity does not make a legacy installation new.
- Existing connection metadata is preserved rather than automatically refreshed from changed configuration. Conflicts or unusable access are reported for explicit correction.
- Storage may perform supported, backed-up upgrades; preservation means compatible user state, not byte-for-byte equality of upgraded storage or bookkeeping timestamps.
- Existing shared setup behavior may be reused or consolidated during implementation, but this specification does not prescribe code structure or authorize changes to unrelated command contracts.
- Local-context compatibility means valid existing metadata, a matching expected installation identity when provided, and credentials authorized administratively against the selected local store. An absent expected identity remains absent. An explicit credential reference uses the existing resolver; an existing context without a reference uses the existing `KAKUNE_TOKEN` fallback. Missing metadata uses the selected installation's canonical secure reference. No DNS resolution, endpoint probe, or host/port/scheme comparison with current listener configuration is required; success does not certify endpoint connectivity.
- SC-006 defines measured acceptance targets, not guaranteed deadlines. Local administrative filesystem and OS credential-manager operations without supported safe timeout/cancellation may rely on platform completion under constitution IX; the plan identifies those operations and their error/partial-state behavior. Application-controlled waits and retries remain bounded, and this allowance does not apply to workflow execution.
- The constitution remains authoritative for independent core testability, safe secret handling, compatible persistence, regression coverage, and mandatory quality gates. No constitutional conflict is intentionally introduced.
