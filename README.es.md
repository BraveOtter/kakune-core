# Kakune Core

Kakune Core es el runtime local-first para los workflows de Kakune. Se encarga de validar workflows, conservar el historial de ejecuciones, proporcionar un workspace aislado y exponer la API HTTP publica que consumen la GUI y el SDK.

## Requisitos

- Rust 1.98.1, instalado automaticamente por `rustup` mediante `rust-toolchain.toml`.

## Ejecutar localmente

```powershell
cargo run -- init --data-dir .\data
cargo run -- daemon --data-dir .\data
```

El daemon escucha por defecto en `http://127.0.0.1:8787`. En Windows, el directorio de datos local predeterminado es `%APPDATA%\Kakune`; tambien se puede sobrescribir con `KAKUNE_DATA_DIR`.

```powershell
Invoke-RestMethod http://127.0.0.1:8787/api/v1/info
```

## Ejemplo de workflow

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

El plugin nativo inicial `@kakune/core` admite `log`, `write-file` y `read-file`. Las operaciones de archivos estan restringidas al workspace de Kakune dentro del directorio de datos seleccionado.

## API publica

La API versionada usa la raiz `/api/v1`:

- `GET /api/v1/info`
- `GET /api/v1/events` (evento SSE de disponibilidad en este milestone)
- `GET /api/v1/plugins/{name}/manifest`
- `GET, POST /api/v1/workflows`
- `POST /api/v1/workflows/analyze`
- `GET, PUT /api/v1/workflows/{name}`
- `POST /api/v1/workflows/{name}/run`
- `GET /api/v1/executions`
- `GET /api/v1/executions/{id}`

Este primer milestone esta orientado deliberadamente a loopback y todavia no expone autenticacion remota ni ejecucion de plugins de terceros.

## Verificar

```powershell
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```
