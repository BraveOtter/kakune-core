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

The daemon listens on `http://127.0.0.1:8787` by default. Its local data directory defaults to `%APPDATA%\Kakune` on Windows, or can be overridden with `KAKUNE_DATA_DIR`.

`init` creates `<data-dir>/kakune.yaml` if it does not exist. It configures the loopback listener, an explicit browser-origin allowlist, a 1 MiB request-body limit, and a 120 request/minute local API limit. Add trusted browser origins to `api.allowedOrigins`; requests that include any other `Origin` are rejected.

```yaml
api:
  listen: 127.0.0.1:8787
  allowedOrigins: [http://localhost:5173]
  requestBodyLimitBytes: 1048576
  rateLimitRequestsPerMinute: 120
```

Use `kakune daemon start`, `kakune daemon status`, and `kakune daemon stop` for a user-managed background process. `kakune init` creates the active local context and stores its credential in the OS credential manager. `kakune context add|list|use|inspect|remove|import|export` stores only portable connection metadata and credential references in `<data-dir>/cli/contexts.json`; it never stores token values.

`kakune provider list|upsert|status|remove` manages non-secret provider profiles. A Codex profile uses `oauthSecret`, whose reference points to an encrypted ChatGPT Plus/Pro OAuth token. `kakune provider login <id>` opens a browser authorization flow directly and does not require, invoke, or read credentials from Codex CLI.

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
cargo run -- workflow validate .\examples\write-note.kakune.yaml
cargo run -- workflow enable .\examples\write-note.kakune.yaml --data-dir .\data
cargo run -- run .\examples\write-note.kakune.yaml --data-dir .\data
```

Pass `--data-dir .\data` to `workflow validate` when the workflow uses an installed process plugin; the command loads enabled plugin definitions from that directory. Without the flag, validation uses only built-in node definitions and does not open the data store.

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

`kakune init` provisions the initial `admin` credential in the operating system's credential manager and configures the local CLI context; normal local CLI use does not require copying a token. The database stores only SHA-256 token digests. If local access is lost, run `kakune auth recover --data-dir <directory>` on the Core machine to issue a new credential and revoke all previous active tokens. If the OS credential manager is unavailable, the command falls back to showing the new token once for use through `KAKUNE_TOKEN`.

To connect a GUI, run `kakune auth pair --data-dir <directory>` in a Core terminal, scan its QR from the GUI, then approve the named device in that terminal. Updating an existing Core and restarting it once enables these endpoints; the database migration and pre-upgrade backup run automatically. The invitation expires after five minutes and can be claimed once. The resulting device token has `read`, `run`, and `manage` scopes by default; `--admin` explicitly grants full administration. For a GUI on another machine, provide its reachable HTTPS Core URL with `--endpoint https://core.example.com:8787`. Configure the Core's `api.allowedHosts` and, for browser-based GUIs, the GUI origin in `api.allowedOrigins` before pairing. The GUI should poll the pairing status until approval, exchange the approved claim for its token, and keep that token in its own OS credential manager. Use `kakune auth tokens --data-dir <directory>` to list credentials and `kakune auth revoke <token-id> --data-dir <directory>` to revoke one device. The pairing endpoints are unauthenticated by design, but require the high-entropy QR code, a GUI-generated claim secret, the local approval, and HTTPS for non-loopback endpoints.

The QR payload uses `format: "kakune-pairing/v1"` and includes `endpoint`, `coreId`, `pairingCode`, `expiresAt`, and granted `scopes`. The GUI generates a fresh `claimSecret` from at least 32 random bytes, posts `{ pairingCode, claimSecret, deviceName }` to `/api/v1/auth/pair/claim`, then polls `/api/v1/auth/pair/status` with `{ pairingCode, claimSecret }`. After the local approval, it posts the same proof to `/api/v1/auth/pair/exchange`; the one-time response contains `coreId`, `token`, `tokenId`, `name`, and `scopes`. The GUI should verify `coreId` matches the QR before saving the token.

An `admin` token can create scoped, optionally expiring tokens: `read` permits queries and event replay, `run` permits execution creation, `manage` permits workflow mutations, and `admin` permits all operations including token management. Revocation takes effect immediately. Keep the Core bound to loopback unless remote TLS, host, and origin policies are configured. External node registration is deliberately available only to embedding local callers, not through the HTTP API.

`GET /api/v1/events` replays retained durable events in sequence order and then closes the stream in this milestone. Supply the last SSE event ID in `Last-Event-ID`, or as `?cursor=<eventId>`, to resume after that event. Each SSE data value is a version 1 envelope containing `eventVersion`, `coreId`, `eventId`, `sequence`, `timestamp`, `type`, `resourceId`, optional `executionId`, and `payload`.

## Service, secrets, and recovery

Kakune runs under the identity selected by its service manager. Provider profiles and secrets belong to that Core identity, never to a remote GUI user or to a Codex Desktop session. The default data directories are `%APPDATA%\Kakune` for an interactive Windows user, `%ProgramData%\Kakune` when installing the Windows service with `--data-dir`, `~/.local/share/kakune` for the included systemd user unit, and `~/Library/Application Support/Kakune` for the included launchd agent.

On Windows, install and operate the native service from an elevated terminal:

```powershell
kakune service install --data-dir "$env:ProgramData\Kakune"
kakune service start
kakune service status
kakune service stop
kakune service uninstall
```

The service is `KakuneCore`, runs as `LocalSystem`, starts automatically, restarts after failures, and terminates its Core process tree on stop. Its credentials are therefore stored for `LocalSystem`; an interactive user's Credential Manager is intentionally not reused.

For Linux, install `packaging/linux/kakune-core.service` as `~/.config/systemd/user/kakune-core.service`, then run `systemctl --user daemon-reload` and `systemctl --user enable --now kakune-core`. The unit uses `KillMode=control-group`. For macOS, create `~/Library/Logs/Kakune`, copy `packaging/macos/dev.kakune.core.plist` to `~/Library/LaunchAgents/`, then run `launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/dev.kakune.core.plist`. Adjust `/usr/local/bin/kakune` when installed elsewhere.

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
