# Kakune Core

Kakune Core is the local-first runtime for Kakune workflows. It owns workflow validation, execution history, an isolated workspace, and the public HTTP API consumed by the GUI and SDK.

## Prerequisites

- Rust 1.98.1, installed automatically by `rustup` from `rust-toolchain.toml`.

## Install a release

Install the latest release with PowerShell on Windows:

```powershell
irm https://raw.githubusercontent.com/BraveOtter/kakune-core/master/scripts/install.ps1 | iex
```

Or with a POSIX shell on macOS or Linux:

```sh
curl -fsSL https://raw.githubusercontent.com/BraveOtter/kakune-core/master/scripts/install.sh | sh
```

The scripts select the platform build, verify its SHA-256 checksum, and install Kakune in a per-user directory. Current release builds are Windows x86_64, Linux x86_64 with glibc, and macOS Apple Silicon (arm64). Run the same command again to update. The Unix script accepts `--version 0.1.1` and `--install-dir <directory>` when downloaded and run locally; the PowerShell script accepts `-Version 0.1.1` and `-InstallDir <directory>`. After installation, run `kakune init --data-dir <directory>` (or `kakune init` to use the default data directory); on macOS or Linux, open a new terminal if `kakune` is not yet on PATH. If Kakune is running as a service, restart it after updating; the installer restarts the Windows `KakuneCore` service when it uses the same executable path.

Each release has an SPDX SBOM and GitHub provenance attestation; see [`packaging/README.md`](packaging/README.md) for verification and service installation details. Windows signing and macOS notarization are stated only when the corresponding release asset is signed.

## Run locally

```powershell
cargo run -- init --data-dir .\data
cargo run -- daemon --data-dir .\data
```

The daemon listens on `http://127.0.0.1:8787` by default. Kakune selects its data directory from `KAKUNE_DATA_DIR` when set, then `%APPDATA%\Kakune`, `$XDG_DATA_HOME/kakune`, or `~/.local/share/kakune`. Override it per command with `--data-dir <directory>`.

### General and local commands

```text
kakune init [--data-dir <directory>] [--config <file>] [--context-file <file>]
kakune version
kakune --version
kakune doctor [--data-dir <directory>] [--config <file>] [--json]
kakune context add <name> --endpoint <url> [--name <display-name>]
kakune context list
kakune context use <name>
kakune context inspect <name>
kakune context remove <name>
kakune context import <file>
kakune context export <name> --output <file> [--replace]
```

`init`, `doctor`, context management, storage maintenance, and Core API authentication (`auth`) are local operations. `doctor` is read-only: it reports configuration, storage, database, credentials, Core, service, compatibility, and platform findings without setup, repair, or migration. Any failed finding or required unknown finding exits 1; optional warnings/unknown findings alone exit 0; invalid arguments exit 2. `kakune version` and `kakune --version` report the same local build version without reading installation files or contacting a Core.

Initialization remains safe to repeat. It creates only missing valid resources and preserves existing configuration, credential history, custom connection fields, unrelated contexts, and active selection. Initial access is saved and verified in the OS credential manager; failure is reported as partial setup, never as a printed-token fallback. Recovery remains explicit:

```text
kakune --context-file <contexts-file> auth recover --data-dir <directory> --config <config-file>
```

Unless overridden, configuration is `<data-dir>/kakune.yaml` and connection metadata is `<data-dir>/cli/contexts.json`. `init` configures the loopback listener, explicit browser-origin allowlist, 1 MiB API request limit, and request rate limit:

```yaml
api:
  listen: 127.0.0.1:8787
  allowedOrigins: [http://localhost:5173]
  requestBodyLimitBytes: 1048576
  rateLimitRequestsPerMinute: 120
```

Use `daemon start|status|stop` for the current-user background Core; `service install|start|status|stop|uninstall` operates the machine-wide native service. These are separate scopes and neither is controlled through a remote context.

### Routing, output, and bounds

- Core-backed workflow, execution, trigger, provider, named MCP, and secret commands use the active context, or `--context <id>` to select one. `--context-file <path>` selects a connection collection and `--ca <file>` supplies a TLS CA. Missing selection, identity mismatch, authorization failure, and unreachable Cores fail; there is no implicit localhost or local-Store fallback.
- `--data-dir` selects local data for commands that support it; it **does not** make a Core-backed command standalone. Use `--standalone` only for a documented direct-local execution. Local authoring/checks and context/doctor operations disclose their local scope.
- Canonical commands use human-readable output by default and `--json` for machine-readable outcomes where available. Every canonical provider, named MCP, and secret command supports both modes; `--json` does not change the separate legacy `provider` or one-shot `mcp call` contracts. Canonical argument errors exit 2; validation, authorization, unsupported requested capability, and operation failures exit 1; success and explicit no-op outcomes exit 0. Legacy extras retain their own output and exit contracts.
- Canonical list commands return one bounded page. Use `--limit <n>` and `--cursor <opaque>` to continue; a cursor is opaque and must be reused with the same query. Results are not silently fetched without a bound.
- File output is create-only unless `--replace` is supplied. Provide `--output <file>` where required; parent directories are not created automatically. Imports validate before changing a Core. File paths must use the command's documented positional argument or explicit `--file`/`--input` option; a workflow file is never guessed from its name.

### Canonical workflow commands

```text
kakune workflow create <name> [--output <file>] [--replace]
kakune workflow import <file> [--replace] [--if-revision <revision>]
kakune workflow export <workflow> --output <file> [--replace]
kakune workflow validate <workflow>
kakune workflow validate --file <file>
kakune workflow plan <workflow>
kakune workflow plan --file <file>
kakune workflow list [--limit <n>] [--cursor <opaque>]
kakune workflow run <workflow> [--request-key <key>]
kakune workflow enable <stored-workflow>
kakune workflow disable <stored-workflow>
```

Create and local file checks do not register or execute a workflow. Import creates a disabled registration; replacement requires `--replace`, and `--if-revision` can guard it. Export preserves the original YAML source and secret references, not resolved values. Stored selectors can be IDs or unique names; use `id:<id>` or `name:<name>` when needed. Manual run submits one execution and does not enable triggers. Enable/disable change trigger eligibility only; disabling does not cancel work already admitted.

The canonical syntax intentionally corrects earlier file-oriented forms: use `workflow validate --file <file>` rather than passing a path as a positional workflow selector; `workflow enable` and `disable` take a stored workflow, not a YAML file; `context export <name> --output <file>` exports one context, not a whole collection. For a local file, create/import it as appropriate, then select its stored ID to enable it.

Direct local execution is a separate, bounded path and requires local YAML:

```powershell
cargo run -- --standalone workflow run --file .\examples\write-note.kakune.yaml --data-dir .\isolated-run --timeout-seconds 30
```

It does not contact a selected Core or leave a background runtime. Do not use an ordinary data directory for isolated checks.

Execution and trigger observations are also Core-backed:

```text
kakune execution list [--limit <n>] [--cursor <opaque>]
kakune execution inspect <execution-id>
kakune execution cancel <execution-id> [--wait-seconds 0..30]
kakune execution logs <execution-id> [--limit <n>] [--cursor <opaque>]
kakune trigger list [--limit <n>] [--cursor <opaque>]
kakune trigger inspect <workflow-id>/<trigger-id>
```

Cancellation request acceptance is distinct from observed termination. Logs are finite snapshots, not a follow stream; trigger inspection is read-only and does not fire or reconcile schedules.

### Providers, MCP, and secrets

```text
kakune provider add <id> --file <profile.json> [--json]
kakune provider list [--limit <n>] [--cursor <opaque>] [--json]
kakune provider auth <id> [--stdin] [--json]
kakune provider test <id> [--json]
kakune provider models <id> [--json]
kakune mcp add <name> --file <connection.json> [--json]
kakune mcp list [--limit <n>] [--cursor <opaque>] [--json]
kakune mcp inspect <name> [--json]
kakune mcp auth <name> [--secret <existing-reference>] [--json]
kakune mcp tool list <name> [--limit <n>] [--cursor <opaque>] [--json]
kakune mcp tool call <name> <tool> --input <json-file> [--json]
kakune secret set <name> [--stdin] [--json]
kakune secret list [--limit <n>] [--cursor <opaque>] [--json]
kakune secret remove <name> [--json]
```

Provider add is create-only. MiniMax authentication accepts hidden terminal input or explicit bounded `--stdin`; Codex uses its supported browser flow. Neither current provider supports live model discovery: `provider models` labels configured/static data and reports discovery as unsupported rather than inventing results. MCP uses the existing stdio and HTTP transports; tool input is validated against the discovered schema before one named call. `mcp auth --secret <reference>` binds an existing secret reference and never accepts secret values or stdin. Run `secret set <name> --stdin` separately when needed. Secret values are never accepted as ordinary arguments, included in output, or shown by `secret list`.

Without `--json`, provider, named MCP, and secret commands print human-readable summaries/tables. Use `--json` on any canonical command in these three families for typed JSON output, including paginated list results and operation outcomes. Secret values are excluded from both modes. This option does not apply to legacy `provider upsert|status|remove|login` or the one-shot `mcp call` command.

### Legacy extras and migration

Legacy entry points remain available; they are **not** aliases for the canonical commands. Their compatibility ledger and per-command evidence are in [`specs/003-complete-command-surface/command-review.md`](specs/003-complete-command-surface/command-review.md).

| Legacy invocation | Possible canonical migration | Difference to account for |
| --- | --- | --- |
| `kakune run flow.yaml` | `workflow import flow.yaml`, then `workflow run id:flow` | Legacy run registers/enables; canonical import starts disabled and manual run does not enable. Explicitly `workflow enable` only if that old side effect is desired. |
| `kakune executions --data-dir data` | `execution list --limit 100 --json` | Only if the selected Core is the intended target; canonical list is Core-backed and paginated. |
| `kakune inspect E --data-dir data` | `execution inspect E --json` | Target and output schema differ; canonical detail includes explicit availability/continuation. |
| `provider upsert profile.json` | `provider add id --file profile.json` | Only for create; canonical add rejects duplicates. Existing-profile update has no canonical replacement. |
| `provider login codex-id` | `provider auth codex-id` | Codex browser auth overlaps, but target and outcome/presentation contracts differ. |
| `provider status id` | No exact replacement | Cached diagnostics are not equivalent to active `provider test`. |
| `mcp call call.json` | `mcp add name --file connection.json`, then `mcp tool call name tool --input args.json` | Converts a one-shot file into persistent named configuration and adds discovery/schema validation; convert credentials to references deliberately. |
| `provider remove`, `auth recover|pair|tokens|revoke`, `storage backup|restore|retain|compact|pin` | No exact replacement | These operations have distinct profile, Core-access, pairing, backup, retention, compaction, or pinning effects. Keep their existing invocations. |

The legacy `provider upsert|status|remove|login`, `mcp call`, top-level `run|executions|inspect`, `auth`, and `storage` contracts remain separate. No extra is removed or newly deprecated. Existing Plugin commands remain unchanged and Plugin expansion is outside this scope.

Measure the release Core's idle private memory and CPU on Windows with `powershell -ExecutionPolicy Bypass -File .\scripts\measure-idle.ps1`. The script also fails if an empty Core has a Codex, Node, or Python child process.

```powershell
Invoke-RestMethod http://127.0.0.1:8787/api/v1/info
```

## Workflow example

```yaml
apiVersion: kakune/v1
kind: Workflow
metadata:
  id: write-note
  name: Write note
triggers:
  - id: manual
    type: kakune.trigger.manual@1
entry: write-note
nodes:
  - id: write-note
    type: kakune.fs.write-text@1
    inputs:
      path: { literal: notes/hello.txt }
      content: { literal: Hello from Kakune }
```

```powershell
cargo run -- workflow validate --file .\examples\write-note.kakune.yaml
cargo run -- workflow plan --file .\examples\write-note.kakune.yaml
cargo run -- workflow import .\examples\write-note.kakune.yaml
cargo run -- workflow list --json
cargo run -- workflow run id:write-note --request-key write-note-example
cargo run -- workflow enable id:write-note
cargo run -- workflow disable id:write-note
```

Core-backed commands above use the active context; select a different Core with `--context <id>` and, if needed, select its context collection with `--context-file <file>`. `workflow validate --file` and `workflow plan --file` are local and do not contact the Core. A local file can be checked without registering it; import it before using a stored selector with run/enable/disable. `workflow import` creates a disabled registration. Existing output files require `--replace` on commands that write files.

Pass `--data-dir .\data` to local `workflow validate --file` or `workflow plan --file` when the workflow uses installed process plugins; local validation loads enabled plugin definitions from that data directory. Without the flag, validation uses built-in node definitions and does not open the Store.

The initial native node catalog supports `kakune.log@1`, `kakune.fs.write-text@1`, `kakune.fs.read-text@1`, `kakune.fs.copy@1`, `kakune.fs.move@1`, `kakune.http.request@1`, `kakune.flow.end@1`, `kakune.flow.if@1`, `kakune.flow.delay@1`, `kakune.flow.pass@1`, and structured control nodes. File operations are restricted to the Kakune workspace in the selected data directory. The HTTP node accepts an `inputs.url`, optional object-of-strings `inputs.headers`, optional `inputs.body`, and bounded `with.method` / `with.timeoutSeconds`; its route is `success` for a 2xx status and `httpError` otherwise.

## Structured control nodes

Top-level graphs and every control body are acyclic. Iteration is only expressed by the following structured bodies; a route may never point back to an earlier node.

`kakune.flow.switch@1` selects a named branch from scalar `inputs.value`. `with.cases` maps stringified input values to branch names; `with.default` is optional. The selected body output fields are copied to the switch result, along with `branch`, and its selected branch name is the route.

```yaml
- id: choose
  type: kakune.flow.switch@1
  inputs:
    value: { literal: blue }
  with:
    cases: { blue: selected }
    default: fallback
  branches:
    selected:
      entry: emit
      nodes:
        - id: emit
          type: kakune.flow.pass@1
          inputs: { result: { literal: selected } }
      outputs: { result: { from: emit.result } }
    fallback:
      entry: emit
      nodes:
        - id: emit
          type: kakune.flow.pass@1
          inputs: { result: { literal: fallback } }
      outputs: { result: { from: emit.result } }
```

`kakune.flow.foreach@1` requires array `inputs.items`, a `body`, and `with.maxConcurrency` from 1 to 64. Its optional `with.onError` is `fail` (default) or `continue`. A foreach body may bind `{ from: $item }` and `{ from: $index }`. The node output `results` is always ordered by input index. Each element is `{ index, item, outputs }` or, with `onError: continue`, `{ index, item, error }`.

```yaml
- id: each
  type: kakune.flow.foreach@1
  inputs: { items: { literal: [one, two] } }
  with: { maxConcurrency: 2, onError: fail }
  body:
    entry: copy
    nodes:
      - id: copy
        type: kakune.flow.pass@1
        inputs: { value: { from: $item } }
    outputs: { value: { from: copy.value } }
```

`kakune.flow.loop@1` requires `inputs.state`, `with.maxIterations` from 1 to 10,000, and a `body` that exports `state` and boolean `continue`. Its body can bind `$state` and `$iteration`. A false `continue` returns route `success` with `completed: true`; otherwise the loop stops at the mandatory bound with route `maxIterations` and `completed: false`. Its final body outputs, `state`, `iterations`, and `completed` are node outputs.

```yaml
- id: repeat
  type: kakune.flow.loop@1
  inputs: { state: { literal: initial } }
  with: { maxIterations: 10 }
  body:
    entry: step
    nodes:
      - id: step
        type: kakune.flow.pass@1
        inputs:
          state: { from: $state }
          continue: { literal: false }
    outputs:
      state: { from: step.state }
      continue: { from: step.continue }
```

Subgraph `outputs` use the existing `{ from: node.output }` bindings. Body nodes must be built-in node types, unless execution uses `run_workflow_with_plugins` with an explicitly registered plugin registry.

Bindings can also use a deterministic, data-only `expr` tree. Supported operations are comparison (`equal`, `greaterThan`), boolean (`and`, `or`, `not`), arithmetic (`add`, `subtract`, `multiply`, `divide`, `modulo`), `concat`, `array`, `object`, `get`, `coalesce`, and `contains`. Expression arguments may be literals or nested `{ literal }`, `{ from }`, `{ secret }`, and `{ expr }` bindings; they never evaluate source code.

```yaml
inputs:
  label:
    expr:
      op: concat
      args:
        - literal: "total="
        - expr:
            op: add
            args: [{ literal: 2 }, { literal: 3 }]
```

The built-in filesystem nodes operate only below the Core workspace. In addition to `kakune.fs.read-text@1` and `kakune.fs.write-text@1`, `kakune.fs.copy@1` and `kakune.fs.move@1` accept `inputs.source` and `inputs.destination` workspace-relative file paths.

## Scheduled triggers

Enabled workflow sources may use `kakune.trigger.datetime@1`, `kakune.trigger.interval@1`, `kakune.trigger.cron@1`, `kakune.trigger.startup@1`, and `kakune.trigger.filesystem@1`. The scheduler runs the source snapshot stored for an enabled workflow; filesystem notifications never read or execute a YAML file from disk.

```yaml
triggers:
  - id: daily-report
    type: kakune.trigger.cron@1
    with:
      expression: "0 9 * * *"
      timezone: America/New_York
      misfire: runOnce
  - id: one-time-report
    type: kakune.trigger.datetime@1
    with:
      at: "2026-11-01T01:30"
      timezone: America/New_York
      dst: latest
  - id: incoming-file
    type: kakune.trigger.filesystem@1
    with:
      root: inbox
      events: [created, renamed]
      debounceMs: 500
```

Calendar timestamps are stored as UTC. Cron uses UTC by default and accepts an IANA `timezone`; local date/time timestamps require `timezone` and choose the first repeated DST hour by default (`dst: latest` selects the second). A nonexistent local DST time is rejected. The next due time and misfire policy are persisted. `misfire` accepts `skip`, `runOnce` (default), and `catchUp`; `catchUp` requires a bounded `maxCatchUp` from 1 to 100.

Filesystem roots must be existing directories expressed as workspace-relative paths without `.` or `..`. Core resolves the path before watching and rejects roots, including symlinks, that escape the workspace. Watches are recursive. Supported events are `created`, `modified`, `removed`, and `renamed`; notifications for a trigger are coalesced with a trailing debounce of 10 to 60,000 ms (500 ms by default). The watcher channel is bounded; dropped notifications are retained as `scheduler.filesystem_overflow` durable events.

Filesystem trigger values include `path`, the last workspace-relative path in the debounce window, and `paths`, the de-duplicated workspace-relative paths from that window. Map either value into a workflow input with `$trigger.path` or `$trigger.paths`; `path` can be passed directly to a filesystem node.

```yaml
triggers:
  - id: incoming-file
    type: kakune.trigger.filesystem@1
    with:
      root: inbox
      events: [created, renamed]
    map:
      filePath: { from: $trigger.path }
```

## External plugin nodes

External nodes are opt-in process plugins. Installations always use a two-phase review: `kakune plugin prepare <local-path|npm:package@version|git:url#ref>` copies or downloads bytes, validates only static manifests and definitions, and returns a digest; `kakune plugin commit <prepared-id> --digest <digest>` activates exactly those bytes. Package scripts, dependency installers, and plugin processes are never run during preparation. The same flow is available through `POST /api/v1/plugins/prepare` and `POST /api/v1/plugin-installations/{id}/commit`.

An embedding Rust caller can also explicitly load a static manifest and register its declared definitions before using `run_workflow_with_plugins`:

```rust
use kakune_core::{PluginRegistry, run_workflow_with_plugins};

let mut plugins = PluginRegistry::default();
plugins.register_manifest_path("C:/plugins/example/kakune.plugin.json")?;
let execution = run_workflow_with_plugins(&store, &workflow, &plugins)?;
```

`contributes.nodes` contains relative JSON paths. Each definition must be inside the plugin directory (including after symlink resolution) and use this static shape:

```json
{
  "apiVersion": "kakune.dev/v1",
  "kind": "NodeDefinition",
  "type": "org.example.greet@1",
  "name": "Greet",
  "inputSchema": { "type": "object" },
  "outputSchema": { "type": "object" }
}
```

Built-in node types cannot be replaced and duplicate external types are rejected. The resolved source, content digest, lock, and effective declared policy are persisted with each installed plugin. For each external node invocation Core starts the manifest's process through `PluginHost`, applies its message, stderr, request, and shutdown limits, then calls `node/execute` with `executionId`, `nodeId`, `nodeType`, `operationId`, and an `inputs` object. Execution cancellation sends `operation/cancel` and terminates the isolated host after its grace period. The response is restricted to `{ route?, outputs?, message? }`; successful results and failures are persisted with the node run. Plugin-originated privileged requests are not executed by Core in this milestone.

## Public API

The versioned API is rooted at `/api/v1`:

- `GET /api/v1/info`
- `GET /api/v1/events` (durable SSE event replay)
- `GET, POST /api/v1/auth/tokens`
- `POST /api/v1/auth/tokens/{id}/revoke`
- `POST /api/v1/auth/pair/claim`, `/status`, and `/exchange` (one-use GUI pairing)
- `GET, POST /api/v1/providers`
- `GET, PUT, DELETE /api/v1/providers/{id}`
- `POST /api/v1/providers/{id}/diagnose`
- `GET /api/v1/plugins/{name}/manifest`
- `GET, POST /api/v1/workflows`
- `POST /api/v1/workflows/analyze`
- `GET, PUT /api/v1/workflows/{id}/source`
- `POST /api/v1/workflows/{id}/enable`
- `POST /api/v1/workflows/{id}/disable`
- `GET, POST /api/v1/executions`
- `GET /api/v1/executions/{id}`

`kakune init` provisions initial administrative access in the operating system's credential manager and configures the local CLI context; normal local CLI use does not require copying a token. The database stores only SHA-256 token digests. If local access is lost, run `kakune auth recover --data-dir <directory>` on the Core machine to deliberately issue a new credential and revoke previous active tokens. Recovery remains distinct from initialization; if the OS credential manager is unavailable, the recovery command retains its existing behavior of showing the new token once for use through `KAKUNE_TOKEN`. Initialization never uses that plaintext fallback.

To connect a GUI, run `kakune auth pair --data-dir <directory>` in a Core terminal, scan its QR from the GUI, then approve the named device in that terminal. Updating an existing Core and restarting it once enables these endpoints; the database migration and pre-upgrade backup run automatically. The invitation expires after five minutes and can be claimed once. The resulting device token has `read`, `run`, and `manage` scopes by default; `--admin` explicitly grants full administration. For a GUI on another machine, provide its reachable HTTPS Core URL with `--endpoint https://core.example.com:8787`. Configure the Core's `api.allowedHosts` and, for browser-based GUIs, the GUI origin in `api.allowedOrigins` before pairing. The GUI should poll the pairing status until approval, exchange the approved claim for its token, and keep that token in its own OS credential manager. Use `kakune auth tokens --data-dir <directory>` to list credentials and `kakune auth revoke <token-id> --data-dir <directory>` to revoke one device. The pairing endpoints are unauthenticated by design, but require the high-entropy QR code, a GUI-generated claim secret, the local approval, and HTTPS for non-loopback endpoints.

The QR payload uses `format: "kakune-pairing/v1"` and includes `endpoint`, `coreId`, `pairingCode`, `expiresAt`, and granted `scopes`. The GUI generates a fresh `claimSecret` from at least 32 random bytes, posts `{ pairingCode, claimSecret, deviceName }` to `/api/v1/auth/pair/claim`, then polls `/api/v1/auth/pair/status` with `{ pairingCode, claimSecret }`. After the local approval, it posts the same proof to `/api/v1/auth/pair/exchange`; the one-time response contains `coreId`, `token`, `tokenId`, `name`, and `scopes`. The GUI should verify `coreId` matches the QR before saving the token.

An `admin` token can create scoped, optionally expiring tokens: `read` permits queries and event replay, `run` permits execution creation, `manage` permits workflow mutations, and `admin` permits all operations including token management. Revocation takes effect immediately. Keep the Core bound to loopback unless remote TLS, host, and origin policies are configured. External node registration is deliberately available only to embedding local callers, not through the HTTP API.

`GET /api/v1/events` replays retained durable events in sequence order and then closes the stream in this milestone. Supply the last SSE event ID in `Last-Event-ID`, or as `?cursor=<eventId>`, to resume after that event. Each SSE data value is a version 1 envelope containing `eventVersion`, `coreId`, `eventId`, `sequence`, `timestamp`, `type`, `resourceId`, optional `executionId`, and `payload`.

## Service, secrets, and recovery

The user daemon (`kakune daemon start|status|stop`) runs under the current user. The separate system service (`kakune service install|start|status|stop|uninstall`) is machine-wide and runs under its native service identity: `LocalSystem` on Windows and `root` for the Linux systemd and theoretical macOS launchd definitions. Provider profiles and credentials belong to that Core identity, not to a remote GUI user or a Codex Desktop session. Initialize and provision required credentials for the intended service identity; an interactive user's credential store is not automatically available to `LocalSystem` or `root`. The Windows default service data location is `%ProgramData%\Kakune`; the packaged Linux and macOS examples use `/var/lib/kakune` and `/Library/Application Support/Kakune` respectively. Pass an explicit absolute `--data-dir` when installing.

The CLI has initial native service adapters, but they have not completed end-to-end native service acceptance. Use only an approved disposable machine or VM for service installation; do not experiment with an existing `KakuneCore`, systemd unit, or launchd job. Linux target compilation/native acceptance is not established here, and macOS remains theoretical. Windows commands require an elevated terminal; Linux/macOS machine-wide commands require root. For example, the Windows command sequence is:

```powershell
kakune service install --data-dir "$env:ProgramData\Kakune"
kakune service status
kakune service start
kakune service stop
kakune service uninstall
```

Installation registers automatic boot but leaves the service stopped. An equivalent install is a no-op; a conflicting executable, argument, data path, account, or boot policy is not silently rewritten. Start succeeds only after Core readiness is observed. Stop confirms Core exit and native stopped state; uninstall removes only the registration after a confirmed stop. All workflows, configuration, credentials, execution history, logs, and persistent data are retained. A failed or uncertain stop leaves the registration in place. Old or conflicting registrations require an explicit stop/uninstall/reinstall procedure; never delete the data directory or silently replace credentials to resolve a conflict.

Application-controlled waits are bounded: 10 seconds for lock acquisition, service status, preflight/reconciliation, helper work, and stop dispatch; 30 seconds for Core readiness/grace, installation, and post-stop removal. The accumulated application-wait ceilings are 50/50/60/90 seconds for install/start/stop/uninstall. These are not total wall-clock guarantees: filesystem and OS credential calls may not provide safe cancellation, and a native manager can accept work before its client times out. A timeout therefore does not prove cancellation or completion. Inspect the service manager and the retained attempt journal before retrying. Journal roots are Windows ProgramData `Kakune\lifecycle\services`, `/var/lib/kakune/lifecycle/services` on Linux, and `/Library/Application Support/Kakune/lifecycle/services` on macOS. Status is read-only; unknown or inaccessible state is an error, not absence.

The Linux systemd and macOS launchd definitions in `packaging/` are machine-wide templates, not user units/agents. Update their absolute executable/data paths when adapting the templates. The macOS definition and manager semantics are explicitly unverified.

Secrets use the platform store by default: Windows Credential Manager, macOS Keychain, or Linux Secret Service. Names are scoped by the persisted Core ID, so two Core data directories under one OS identity cannot collide. A desktop-less installation must opt into the encrypted vault and supply its unlock material on every service start:

```text
KAKUNE_SECRET_STORE=vault
KAKUNE_VAULT_PASSPHRASE=<secret supplied by the service manager>
```

`KAKUNE_SECRET_STORE=native-with-vault-backup` writes both stores for a planned migration. There is no plaintext fallback, and a native-store read failure never silently serves the vault copy. Backups deliberately exclude secret records because OS credentials are user and machine bound; restore the provider credentials through the intended service identity.

Create and restore consistent backups with SQLite's backup API. Restore always requires a distinct, empty directory.

```powershell
kakune storage backup .\kakune-backup.tar.gz --data-dir .\data
kakune storage restore .\kakune-backup.tar.gz .\restored-data
kakune storage retain --execution-days 90 --event-days 90 --artifact-limit-bytes 5368709120 --data-dir .\data
kakune storage compact --data-dir .\data
kakune storage pin <execution-id> --value true --data-dir .\data
```

Retention keeps pinned executions, removes completed unpinned executions and expired events incrementally, deletes unreferenced artifacts only when the configured disk limit is exceeded, and checkpoints plus compacts SQLite. Backups include workflows, executions, artifacts, workspace data, and configuration, but not secrets.

`scripts/measure-idle.ps1` measures private memory, CPU, and child runtimes for the documented Windows reference profile. Run it for 10 minutes against a release build and retain its JSON output, the service logs, a restored backup, and the process listing from a stop/crash drill.

## Verify

```powershell
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```
