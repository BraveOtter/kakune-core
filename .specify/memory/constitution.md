# Kakune Core Constitution

## Core Principles

### I. Rust and Declarative Workflows

The core MUST be implemented in Rust. Async execution MUST use Tokio. Workflow definitions
MUST be stored as YAML.

### II. Local-First Operation and CLI Management

Execution MUST remain local-first, and all workflow management operations MUST be available
through the CLI. Core execution and deterministic workflow management MUST NOT require a
hosted AI service. Local API, GUI, and SDK clients MAY share the same Core capabilities;
their existence MUST NOT replace CLI access or make local operation depend on a remote service.

### III. Validate Before Execution

Core MUST validate workflow schemas, node and output references, and control flow before
executing workflow nodes. Invalid workflows MUST fail with actionable validation errors
without executing nodes. Validation MUST cover nested control bodies as well as the
top-level graph, so invalid structure cannot bypass checks through nesting.

### IV. Explicit AI Boundaries

Deterministic nodes MUST remain independent of AI providers and credentials. AI integrations
MUST be explicitly configured and invoked only where the workflow requires them. Missing
provider configuration MUST produce an explicit failure rather than silently selecting a
provider or changing deterministic behavior.

### V. Readable and Safe Implementation

Project code and documentation MUST be written in English. Project-authored Rust MUST NOT
use unsafe code. Dependencies MUST NOT be treated as permission to introduce unsafe blocks
into project code. These rules keep the implementation consistently reviewable and preserve
the project's memory-safety boundary.

### VI. Mandatory Quality Gates

Every completed task MUST run the following checks in order:

```text
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

All three checks MUST pass before merging. Focused tests MAY run during development but MUST
NOT replace the final full suite. Test failures MUST be resolved before continuing unrelated
work. If an environment blocker prevents a check, the completion report MUST identify the
blocked command and cause; the task MUST NOT be represented as fully verified.

### VII. Behavioral and Regression Coverage

Behavioral changes MUST include tests for the changed behavior and relevant failure paths.
Bug fixes MUST include regression tests that reproduce the defect and verify its correction.
Tests MUST exercise the appropriate boundary, including integration coverage when a change
crosses runtime, persistence, scheduler, provider, or plugin contracts.

### VIII. Independent Default Tests

The default test suite MUST run without live AI services or external credentials. Provider
interactions MUST use deterministic fixtures, mocks, or local test doubles. Any live-service
tests MUST be explicitly opt-in and MUST NOT become a prerequisite for default verification.

### IX. Bounded Execution

Loops, concurrency, retries, execution time, and output sizes MUST have explicit finite bounds.
Core MUST validate applicable limits and enforce them during execution. Exceeding a bound
MUST produce a defined result or failure, not unbounded work. New execution mechanisms MUST
declare and test their limits before becoming available to workflows.

### X. Secret Protection and Compatible Persistence

Secrets MUST NOT be stored as plaintext values in workflow files or exposed in logs. Workflows
MUST use secret references instead. Breaking public contracts and storage migrations MUST be
documented with their compatibility impact and upgrade procedure. SQLite schema changes MUST
preserve upgrades from existing databases, and an existing database MUST be backed up before
an upgrade. Deleting user data MUST NOT be an upgrade strategy.

### XI. Specification-Driven Scope

Implementation MUST be limited to behavior defined in the active specification. When a
required decision is missing or requirements conflict, work on the affected behavior MUST
stop and clarification MUST be requested. Existing product requirements MUST NOT be removed
merely because their implementation is incomplete. This prevents speculative behavior and
silent changes to the product contract.

### XII. Independently Testable Core

The entire core MUST be testable independently of the CLI. Console I/O MUST remain in the
CLI, and the CLI MUST stay a thin adapter over core capabilities. Core logic MUST NOT depend
on interactive prompts, terminal state, or process-wide CLI argument parsing.

## Architecture and Operational Constraints

- `src/lib.rs` MUST expose the core library; `src/main.rs` MUST wire the CLI.
- Workflow definition and validation MUST remain in `src/workflow.rs` and `src/planner.rs`;
  execution in `src/runtime.rs`; trigger management in `src/scheduler.rs`; and persistence
  and migrations in `src/store.rs`. Changes to these ownership boundaries MUST be justified
  in the active specification and reviewed for independent testability.
- SQLite migrations MUST run when the store opens, with the backup and upgrade guarantees
  in Principle X. Migration tests MUST cover upgrades from supported existing schemas.
- Local execution checks MUST use an isolated data directory rather than normal user data.
  `run` uses the Core API by default even with `--data-dir`; direct local checks MUST use
  `--standalone` explicitly.

## Development Workflow

1. Before implementation, review the active specification against all twelve principles.
   Resolve missing required decisions and governance conflicts before changing behavior.
2. Keep changes within the approved scope, preserve unrelated user work, and add the
   behavioral, failure-path, and regression coverage required by Principles VII and VIII.
3. Document changes to public contracts, configuration, and storage upgrades alongside
   the affected change. Verify bounds and secret handling at the relevant boundaries.
4. Run the ordered quality gates in Principle VI. Report results and blockers accurately.
5. Review MUST check constitution compliance, independent core testability, and migration
   compatibility before merge. An unresolved violation blocks acceptance; it requires a
   compliant change or an approved constitutional amendment, not an undocumented exception.

## Governance

This file, `.specify/memory/constitution.md`, is the sole authoritative project constitution.
Specifications and implementation guidance, including `AGENTS.md`, MUST conform to this
constitution. A conflicting instruction MUST be surfaced for resolution, not silently used
to weaken a principle.

Amendments MUST identify the affected rules, explain the rationale and compatibility impact,
document any migration or follow-up work, and receive project maintainer approval before
acceptance. Each amendment MUST update the version and last-amended date and include a
temporary Sync Impact Report for review. The report MUST be removed before committing the
amendment. The original ratification date MUST remain unchanged once confirmed.

Constitution versions MUST follow semantic versioning: MAJOR for incompatible principle
removals or redefinitions; MINOR for new principles, sections, or materially expanded
guidance; PATCH for clarifications, wording corrections, and other non-semantic refinements.
Version 1.0.0 is the initial formal version of the previously unversioned constitution, not
a claim that an earlier numbered version existed.

Every specification review and change review MUST check compliance with the applicable
principles and record unresolved conflicts. Maintainers MUST review dependent guidance for
consistency when accepting an amendment; this constitution workflow does not modify that
guidance or its templates automatically.

Ratification approval: explicitly approved by the project maintainer on 2026-09-30.
This date records formal ratification of version 1.0.0, not the undocumented adoption date
of the earlier unversioned rules.

**Version**: 1.0.0 | **Ratified**: 2026-09-30 | **Last Amended**: 2026-09-30
