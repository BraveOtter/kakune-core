# Kakune Core

Kakune Core es el runtime local-first para los workflows de Kakune. Se encarga de validar workflows, conservar el historial de ejecuciones, proporcionar un workspace aislado y exponer la API HTTP publica que consumen la GUI y el SDK.

## Requisitos

- Rust 1.98.1, instalado automaticamente por `rustup` mediante `rust-toolchain.toml`.

## Instalar una release

Instala la ├║ltima release con PowerShell en Windows:

```powershell
irm https://raw.githubusercontent.com/BraveOtter/kakune-core/master/scripts/install.ps1 | iex
```

O con una shell POSIX en macOS o Linux:

```sh
curl -fsSL https://raw.githubusercontent.com/BraveOtter/kakune-core/master/scripts/install.sh | sh
```

Los scripts seleccionan el binario de la plataforma, verifican su checksum SHA-256 e instalan Kakune en un directorio del usuario. Las releases actuales incluyen Windows x86_64, Linux x86_64 con glibc y macOS Apple Silicon (arm64). Ejecuta el mismo comando otra vez para actualizar. El script Unix acepta `--version 0.1.1` y `--install-dir <directorio>` si lo descargas y ejecutas localmente; el script de PowerShell acepta `-Version 0.1.1` y `-InstallDir <directorio>`. Tras instalar, ejecuta `kakune init --data-dir <directorio>` (o `kakune init` para usar el directorio de datos predeterminado); en macOS o Linux, abre una terminal nueva si `kakune` a├║n no aparece en PATH. Si Kakune funciona como servicio, rein├¡cialo despu├®s de actualizar; el instalador reinicia el servicio Windows `KakuneCore` cuando utiliza la misma ruta del ejecutable.

Cada release incluye un SBOM SPDX y una atestaci├│n de provenance de GitHub; consulta [`packaging/README.md`](packaging/README.md) para verificarlo e instalar el servicio. La firma de Windows y la notarizaci├│n de macOS solo se indican cuando el artefacto correspondiente est├í realmente firmado.

## Ejecutar localmente

```powershell
cargo run -- init --data-dir .\data
cargo run -- daemon --data-dir .\data
```

El daemon escucha por defecto en `http://127.0.0.1:8787`. En Windows, el directorio de datos local predeterminado es `%APPDATA%\Kakune`; tambien se puede sobrescribir con `KAKUNE_DATA_DIR`.

`init` crea `<data-dir>/kakune.yaml` cuando no existe. Configura el listener loopback, una lista explicita de origenes de navegador autorizados, un limite de cuerpo de 1 MiB y un limite local de 120 peticiones/minuto. Anade los origenes de navegador de confianza a `api.allowedOrigins`; cualquier peticion que incluya otro `Origin` se rechaza.

```yaml
api:
  listen: 127.0.0.1:8787
  allowedOrigins: [http://localhost:5173]
  requestBodyLimitBytes: 1048576
  rateLimitRequestsPerMinute: 120
```

Usa `kakune daemon start`, `kakune daemon status` y `kakune daemon stop` para un proceso en segundo plano administrado por el usuario. `kakune init` crea el contexto local activo y guarda su credencial en el gestor de credenciales del sistema. `kakune context add|list|use|inspect|remove|import|export` almacena solo metadatos de conexion y referencias de credenciales en `<data-dir>/cli/contexts.json`; nunca almacena valores de token.

En Windows, mide la memoria privada y CPU idle del Core release con `powershell -ExecutionPolicy Bypass -File .\scripts\measure-idle.ps1`. El script tambien falla si un Core vacio inicia un proceso hijo de Codex, Node o Python.

```powershell
Invoke-RestMethod http://127.0.0.1:8787/api/v1/info
```

## Ejemplo de workflow

```yaml
apiVersion: kakune/v1
kind: Workflow
metadata:
  id: write-note
  name: Escribir nota
triggers:
  - id: manual
    type: kakune.trigger.manual@1
entry: write-note
nodes:
  - id: write-note
    type: kakune.fs.write-text@1
    inputs:
      path: { literal: notes/hello.txt }
      content: { literal: Hola desde Kakune }
```

```powershell
cargo run -- workflow validate .\examples\write-note.kakune.yaml
cargo run -- workflow enable .\examples\write-note.kakune.yaml --data-dir .\data
cargo run -- run .\examples\write-note.kakune.yaml --data-dir .\data
```

Pasa `--data-dir .\data` a `workflow validate` cuando el workflow usa un plugin de proceso instalado; el comando carga las definiciones de plugins habilitados desde ese directorio. Sin el flag, la validacion usa solo los nodos nativos y no abre el almacenamiento.

El catalogo nativo inicial admite `kakune.log@1`, `kakune.fs.write-text@1`, `kakune.fs.read-text@1`, `kakune.fs.copy@1`, `kakune.fs.move@1`, `kakune.http.request@1`, `kakune.flow.end@1`, `kakune.flow.if@1`, `kakune.flow.delay@1`, `kakune.flow.pass@1`, `kakune.flow.join@1`, `kakune.flow.merge@1`, `kakune.flow.set-variable@1` y los nodos estructurados `switch`, `foreach` y `loop`. Las operaciones de archivos estan restringidas al workspace de Kakune dentro del directorio de datos seleccionado. `copy` y `move` reciben `inputs.source` e `inputs.destination` como rutas relativas al workspace. El nodo HTTP recibe `inputs.url`, `inputs.headers` de strings y un `inputs.body` opcional, con `with.method` y `with.timeoutSeconds` acotados; toma la ruta `success` con estado 2xx y `httpError` en cualquier otro caso.

Los bindings admiten tambien un arbol `expr` determinista, sin `eval` ni ejecucion de codigo: comparacion, booleanos, aritmetica, `concat`, `array`, `object`, `get`, `coalesce` y `contains`. Sus argumentos pueden ser literales o bindings anidados `{ literal }`, `{ from }`, `{ secret }` y `{ expr }`.

## Triggers programados

Las fuentes de workflows habilitados pueden usar `kakune.trigger.datetime@1`, `kakune.trigger.interval@1`, `kakune.trigger.cron@1`, `kakune.trigger.startup@1` y `kakune.trigger.filesystem@1`. El scheduler ejecuta el snapshot de la fuente guardada para el workflow habilitado; las notificaciones filesystem nunca leen ni ejecutan un YAML desde disco.

```yaml
triggers:
  - id: informe-diario
    type: kakune.trigger.cron@1
    with:
      expression: "0 9 * * *"
      timezone: America/New_York
      misfire: runOnce
  - id: informe-unico
    type: kakune.trigger.datetime@1
    with:
      at: "2026-11-01T01:30"
      timezone: America/New_York
      dst: latest
  - id: archivo-entrante
    type: kakune.trigger.filesystem@1
    with:
      root: inbox
      events: [created, renamed]
      debounceMs: 500
```

Los calendarios se guardan en UTC. Cron usa UTC por defecto y acepta `timezone` IANA; una fecha/hora local necesita `timezone` y elige por defecto la primera hora DST repetida (`dst: latest` selecciona la segunda). Una hora DST inexistente se rechaza. Se persisten el proximo vencimiento y la politica de misfire. `misfire` acepta `skip`, `runOnce` (por defecto) y `catchUp`; `catchUp` exige `maxCatchUp` acotado entre 1 y 100.

Las raices filesystem deben ser directorios existentes expresados como paths relativos al workspace, sin componentes `.` ni `..`. Core resuelve el path antes de observarlo y rechaza raices, incluso mediante enlaces simbolicos, que salgan del workspace. Las observaciones son recursivas. Los eventos admitidos son `created`, `modified`, `removed` y `renamed`; las notificaciones de un trigger se agrupan con debounce final de 10 a 60000 ms (500 ms por defecto). El canal del watcher es acotado; las notificaciones descartadas se retienen como eventos durables `scheduler.filesystem_overflow`. El `map` del trigger puede obtener los paths relativos agrupados desde `$trigger.paths`.

## Nodos de plugins externos

Los nodos externos son plugins de proceso locales y requieren una accion explicita. `kakune plugin prepare` descarga o copia bytes sin ejecutar scripts ni procesos, valida sus manifiestos estaticos y devuelve un digest; `kakune plugin commit <prepared-id> --digest <digest>` activa exactamente esos bytes. El mismo flujo esta disponible mediante `POST /api/v1/plugins/prepare` y `POST /api/v1/plugin-installations/{id}/commit`. Un llamador Rust embebido tambien puede cargar el manifiesto estatico y registrar sus definiciones antes de usar `run_workflow_with_plugins`:

```rust
use kakune_core::{PluginRegistry, run_workflow_with_plugins};

let mut plugins = PluginRegistry::default();
plugins.register_manifest_path("C:/plugins/example/kakune-plugin.json")?;
let execution = run_workflow_with_plugins(&store, &workflow, &plugins)?;
```

`contributes.nodes` contiene paths JSON relativos. Cada definicion debe permanecer dentro del directorio del plugin, incluso tras resolver enlaces simbolicos, y usa esta forma estatica:

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

No se pueden sustituir tipos nativos y se rechazan los tipos externos duplicados. Para cada invocacion, Core inicia el proceso del manifiesto mediante `PluginHost`, conserva sus limites de mensajes, stderr y timeouts, e invoca `node/execute` con `executionId`, `nodeId`, `nodeType`, `operationId` y el objeto `inputs`. La respuesta se limita a `{ route?, outputs?, message? }`; los resultados correctos y los errores se persisten con la ejecucion del nodo. En este milestone Core no ejecuta solicitudes iniciadas por el plugin.

## API publica

La API versionada usa la raiz `/api/v1`:

- `GET /api/v1/info`
- `GET /api/v1/events` (replay SSE de eventos durables)
- `GET, POST /api/v1/auth/tokens`
- `POST /api/v1/auth/tokens/{id}/revoke`
- `POST /api/v1/auth/pair/claim`, `/status` y `/exchange` (emparejamiento de GUI de un solo uso)
- `GET /api/v1/plugins/{name}/manifest`
- `GET, POST /api/v1/workflows`
- `POST /api/v1/workflows/analyze`
- `GET, PUT /api/v1/workflows/{id}/source`
- `POST /api/v1/workflows/{id}/enable`
- `POST /api/v1/workflows/{id}/disable`
- `GET, POST /api/v1/executions`
- `GET /api/v1/executions/{id}`

`kakune init` guarda la credencial `admin` inicial en el gestor de credenciales del sistema y configura el contexto local del CLI; el uso local habitual no requiere copiar tokens. La base de datos solo conserva digests SHA-256. Si se pierde el acceso local, ejecuta `kakune auth recover --data-dir <directorio>` en la maquina del Core para emitir una credencial nueva y revocar todos los tokens activos anteriores. Si el gestor de credenciales del sistema no esta disponible, el comando muestra el token nuevo una sola vez para usarlo mediante `KAKUNE_TOKEN`.

Para conectar una GUI, ejecuta `kakune auth pair --data-dir <directorio>` en una terminal del Core, escanea el QR desde la GUI y aprueba el dispositivo identificado en esa terminal. Para habilitar los endpoints en un Core existente, actualiza el binario y reinicialo una vez; la migracion de la base de datos y su copia de seguridad previa se realizan automaticamente. La invitacion caduca a los cinco minutos y solo puede reclamarse una vez. Por defecto, el token del dispositivo tiene scopes `read`, `run` y `manage`; `--admin` concede administracion completa de forma explicita. Para una GUI en otro equipo, indica la URL HTTPS accesible del Core con `--endpoint https://core.example.com:8787`. Antes del emparejamiento configura `api.allowedHosts` y, si la GUI usa navegador, su origen en `api.allowedOrigins`. La GUI debe consultar el estado hasta recibir aprobacion, intercambiar la solicitud aprobada por su token y guardar ese token en su propio gestor de credenciales. Usa `kakune auth tokens --data-dir <directorio>` para listar credenciales y `kakune auth revoke <id-del-token> --data-dir <directorio>` para revocar un dispositivo. Los endpoints de emparejamiento no requieren autenticacion Bearer, pero exigen el QR de alta entropia, un secreto de reclamacion generado por la GUI, aprobacion local y HTTPS para endpoints que no sean loopback.

El payload del QR usa `format: "kakune-pairing/v1"` e incluye `endpoint`, `coreId`, `pairingCode`, `expiresAt` y los scopes concedidos. La GUI genera un `claimSecret` nuevo con al menos 32 bytes aleatorios, envia `{ pairingCode, claimSecret, deviceName }` a `/api/v1/auth/pair/claim` y consulta `/api/v1/auth/pair/status` con `{ pairingCode, claimSecret }`. Tras la aprobacion local, envia la misma prueba a `/api/v1/auth/pair/exchange`; la respuesta de un solo uso contiene `coreId`, `token`, `tokenId`, `name` y `scopes`. Antes de guardar el token, la GUI debe comprobar que `coreId` coincide con el del QR.

Un token `admin` puede crear tokens con expiracion opcional y scopes: `read` permite consultas y replay de eventos, `run` permite iniciar ejecuciones, `manage` permite modificar workflows y `admin` permite todas las operaciones, incluida la gestion de tokens. La revocacion tiene efecto inmediato. Mant├®n el Core en loopback salvo que configures TLS y las politicas remotas de host y origen. El registro de nodos externos esta disponible deliberadamente solo para llamadores locales embebidos, no mediante la API HTTP.

`GET /api/v1/events` reproduce los eventos durables retenidos en orden de secuencia y despues cierra el stream en este milestone. Para continuar despues de un evento, envie su ID SSE en `Last-Event-ID` o como `?cursor=<eventId>`. Cada valor SSE `data` es un envelope version 1 con `eventVersion`, `coreId`, `eventId`, `sequence`, `timestamp`, `type`, `resourceId`, `executionId` opcional y `payload`.

## Servicio, secretos y recuperacion

Los perfiles y secretos pertenecen a la identidad que ejecuta Core, nunca al usuario de una GUI remota ni a una sesion de Codex Desktop. En Windows, instala el servicio nativo desde una consola elevada:

```powershell
kakune service install --data-dir "$env:ProgramData\Kakune"
kakune service start
kakune service status
```

El servicio `KakuneCore` se inicia automaticamente como `LocalSystem`, se reinicia tras un fallo y termina el arbol de procesos del Core al detenerse. Sus credenciales pertenecen a `LocalSystem`; no reutiliza las del usuario interactivo.

Las unidades para Linux y macOS se incluyen en `packaging/linux/kakune-core.service` y `packaging/macos/dev.kakune.core.plist`. Instala la unidad systemd como usuario mediante `systemctl --user daemon-reload` y `systemctl --user enable --now kakune-core`; usa `KillMode=control-group`. Para launchd, crea `~/Library/Logs/Kakune`, copia el plist a `~/Library/LaunchAgents/` y ejecuta `launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/dev.kakune.core.plist`.

Por defecto los secretos usan Credential Manager, Keychain o Secret Service, aislados por el `coreId` persistente. Para un host headless configura explicitamente `KAKUNE_SECRET_STORE=vault` y proporciona `KAKUNE_VAULT_PASSPHRASE` desde el gestor de servicios. No existe fallback a texto plano ni se oculta un fallo del keyring con una copia vault.

Los backups excluyen secretos por estar ligados a usuario y maquina. Incluyen workflows, ejecuciones, artefactos, workspace y configuracion; el restore exige otro directorio vacio:

```powershell
kakune storage backup .\kakune-backup.tar.gz --data-dir .\data
kakune storage restore .\kakune-backup.tar.gz .\restored-data
kakune storage retain --execution-days 90 --event-days 90 --artifact-limit-bytes 5368709120 --data-dir .\data
kakune storage compact --data-dir .\data
```

La retencion conserva ejecuciones fijadas, borra ejecuciones completas y eventos vencidos, y elimina artefactos sin referencias solo al superar el limite de disco. `scripts/measure-idle.ps1` conserva mediciones de memoria privada, CPU y procesos hijo para la evidencia de la fase.

## Verificar

```powershell
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```
