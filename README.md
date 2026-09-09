# Kakune Core

Kakune Core is the local-first runtime for Kakune workflows. It owns workflow validation, execution history, an isolated workspace, and the public HTTP API consumed by the GUI and SDK.

## Prerequisites

- Rust 1.98.1, installed automatically by `rustup` from `rust-toolchain.toml`.

## Run locally

```powershell
cargo run -- init --data-dir .\data
cargo run -- daemon --data-dir .\data
```

The daemon listens on `http://127.0.0.1:8787` by default. Its local data directory defaults to `%APPDATA%\Kakune` on Windows, or can be overridden with `KAKUNE_DATA_DIR`.

```powershell
Invoke-RestMethod http://127.0.0.1:8787/api/v1/info
```

## Workflow example

```yaml
apiVersion: kakune/v1
kind: Workflow
metadata:
  name: write-note
spec:
  triggers:
    manual: {}
  steps:
    - id: write-note
      plugin: "@kakune/core"
      with:
        action: write-file
        path: notes/hello.txt
        content: Hello from Kakune
```

```powershell
cargo run -- workflow validate .\examples\write-note.kakune.yaml
cargo run -- workflow enable .\examples\write-note.kakune.yaml --data-dir .\data
cargo run -- run .\examples\write-note.kakune.yaml --data-dir .\data
```

The initial native `@kakune/core` plugin supports `log`, `write-file`, and `read-file`. File operations are restricted to the Kakune workspace in the selected data directory.

## Public API

The versioned API is rooted at `/api/v1`:

- `GET /api/v1/info`
- `GET /api/v1/events` (SSE readiness event in this milestone)
- `GET /api/v1/plugins/{name}/manifest`
- `GET, POST /api/v1/workflows`
- `POST /api/v1/workflows/analyze`
- `GET, PUT /api/v1/workflows/{name}`
- `POST /api/v1/workflows/{name}/run`
- `GET /api/v1/executions`
- `GET /api/v1/executions/{id}`

This first milestone is intentionally loopback-oriented and does not expose remote authentication or third-party plugin execution yet.

## Verify

```powershell
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```
