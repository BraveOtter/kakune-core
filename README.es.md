# Kakune Core

Kakune Core es el runtime local-first para los workflows de Kakune. Se encarga de validar workflows, conservar el historial de ejecuciones, proporcionar un workspace aislado y exponer la API HTTP publica que consumen la GUI y el SDK.

## Documentacion

- [Manual tecnico y de uso (espanol)](docs/manual-es.md): instalacion, arquitectura, referencia completa de la CLI, lenguaje de workflows, catalogo de nodos, triggers, integraciones, API HTTP, almacenamiento y diagnostico.
- [Guia de migracion de la CLI](docs/cli-migration.md): rutas eliminadas y su equivalente canonico.
- [Esquema de almacenamiento 17](docs/storage-schema-17.md): esquema SQLite, respaldo previo a migraciones y superficie de observacion.

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

Los nodos externos son plugins de proceso locales y requieren una accion explicita. `kakune plugin install <ruta-local|npm:paquete@version|git:url#ref>` y `plugin update` revisan la identidad, version, digest, contenido y capacidades solicitadas antes de activar solo el digest aprobado. La revision no ejecuta scripts, instaladores de dependencias ni procesos del plugin. La API de gestion mantiene endpoints separados de preparacion/confirmacion para clientes que implementen ese protocolo; no son comandos adicionales del CLI. Un llamador Rust embebido tambien puede cargar el manifiesto estatico y registrar sus definiciones antes de usar `run_workflow_with_plugins`:

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

`kakune init` guarda la credencial `admin` inicial en el gestor de credenciales del sistema y configura el contexto local del CLI; el uso local habitual no requiere copiar tokens. La base de datos solo conserva digests SHA-256. El CLI canonico no incluye comandos de recuperacion de acceso, invitacion de emparejamiento, listado de tokens ni revocacion. Repetir la inicializacion no recupera el acceso ni sustituye/revoca credenciales existentes. Si se pierde el acceso, sigue el procedimiento administrativo autorizado de la implementacion y conserva el directorio de datos; no uses comandos eliminados ni borres el almacenamiento.

La API HTTP conserva endpoints de gestion de tokens y emparejamiento de GUI, pero los comandos interactivos eliminados no estan disponibles. Los clientes de API deben usar el protocolo documentado `/api/v1/auth/tokens` y `/api/v1/auth/pair/*` con las protecciones TLS, host y origen configuradas. Las solicitudes de emparejamiento requieren un codigo de invitacion, un secreto de reclamacion generado por la GUI y aprobacion local; el protocolo API no convierte `auth pair`, `auth tokens`, `auth revoke` ni `auth recover` en comandos validos del CLI.

El payload del QR usa `format: "kakune-pairing/v1"` e incluye `endpoint`, `coreId`, `pairingCode`, `expiresAt` y los scopes concedidos. La GUI genera un `claimSecret` nuevo con al menos 32 bytes aleatorios, envia `{ pairingCode, claimSecret, deviceName }` a `/api/v1/auth/pair/claim` y consulta `/api/v1/auth/pair/status` con `{ pairingCode, claimSecret }`. Tras la aprobacion local, envia la misma prueba a `/api/v1/auth/pair/exchange`; la respuesta de un solo uso contiene `coreId`, `token`, `tokenId`, `name` y `scopes`. Antes de guardar el token, la GUI debe comprobar que `coreId` coincide con el del QR.

Un token `admin` puede crear tokens con expiracion opcional y scopes: `read` permite consultas y replay de eventos, `run` permite iniciar ejecuciones, `manage` permite modificar workflows y `admin` permite todas las operaciones, incluida la gestion de tokens. La revocacion tiene efecto inmediato. Mant├®n el Core en loopback salvo que configures TLS y las politicas remotas de host y origen. El registro de nodos externos esta disponible deliberadamente solo para llamadores locales embebidos, no mediante la API HTTP.

`GET /api/v1/events` reproduce los eventos durables retenidos en orden de secuencia y despues cierra el stream en este milestone. Para continuar despues de un evento, envie su ID SSE en `Last-Event-ID` o como `?cursor=<eventId>`. Cada valor SSE `data` es un envelope version 1 con `eventVersion`, `coreId`, `eventId`, `sequence`, `timestamp`, `type`, `resourceId`, `executionId` opcional y `payload`.

## Servicio, secretos y recuperacion

El daemon de usuario (`kakune daemon start|status|stop`) se ejecuta con la identidad actual. El servicio de sistema, distinto (`kakune service install|start|status|stop|uninstall`), es de toda la maquina y usa la identidad del gestor nativo: `LocalSystem` en Windows y `root` en systemd de Linux y en la definicion teorica de launchd para macOS. Los perfiles y credenciales pertenecen a esa identidad de Core, no al usuario de una GUI remota ni a una sesion de Codex Desktop. Inicializa y provisiona las credenciales necesarias para la identidad del servicio; el almacen del usuario interactivo no esta disponible automaticamente para `LocalSystem` o `root`. La ubicacion predeterminada del servicio Windows es `%ProgramData%\Kakune`; los ejemplos de Linux y macOS usan `/var/lib/kakune` y `/Library/Application Support/Kakune`. Especifica `--data-dir` absoluto al instalar.

Los adaptadores nativos son una implementacion inicial y aun no han completado la aceptacion integral con gestores de servicios reales. Usa los comandos solo en una maquina o VM desechable aprobada; no pruebes sobre un `KakuneCore`, unidad systemd o trabajo launchd existente. La compilacion/aceptacion nativa de Linux no esta establecida aqui y macOS sigue siendo teorico. Windows requiere una consola elevada; Linux/macOS, root. Secuencia de ejemplo para Windows:

```powershell
kakune service install --data-dir "$env:ProgramData\Kakune"
kakune service status
kakune service start
kakune service stop
kakune service uninstall
```

La instalacion registra el inicio automatico, pero deja el servicio detenido. Una definicion equivalente no modifica nada; una ruta de ejecutable/datos, argumentos, cuenta o politica de arranque en conflicto no se reescribe silenciosamente. `start` solo tiene exito tras observar Core listo. `stop` confirma la salida de Core y el estado detenido nativo; `uninstall` elimina solo el registro despues de una parada confirmada. Se conservan workflows, configuracion, credenciales, historial, logs y datos. Si la parada falla o queda incierta, el registro se conserva. Resuelve registros antiguos o incompatibles con una secuencia explicita detener/desinstalar/reinstalar; nunca borres el directorio de datos ni sustituyas credenciales silenciosamente.

Los tiempos de espera controlados por la aplicacion tienen limites: 10 segundos para adquirir locks, consultar estado, preflight/reconciliacion, helpers y despacho de parada; 30 segundos para readiness/gracia de Core, instalacion y eliminacion posterior a la parada. Los techos acumulados de espera son 50/50/60/90 segundos para instalar/iniciar/detener/desinstalar. No son garantias de tiempo total: puede que llamadas de filesystem/credenciales no se puedan cancelar de forma segura, y el gestor nativo puede aceptar una operacion antes de que expire su cliente. Un timeout no demuestra cancelacion ni finalizacion. Inspecciona el gestor y el diario retenido antes de reintentar. Las raices del diario son ProgramData `Kakune\lifecycle\services` en Windows, `/var/lib/kakune/lifecycle/services` en Linux y `/Library/Application Support/Kakune/lifecycle/services` en macOS. `status` es de solo lectura; un estado desconocido o inaccesible es error, no ausencia.

Las definiciones systemd y launchd de `packaging/` son plantillas de sistema, no servicios/agentes de usuario. Ajusta las rutas absolutas del ejecutable y datos. La definicion de macOS y su semantica de gestor no estan verificadas.

Por defecto los secretos usan Credential Manager, Keychain o Secret Service, aislados por el `coreId` persistente. Para un host headless configura explicitamente `KAKUNE_SECRET_STORE=vault` y proporciona `KAKUNE_VAULT_PASSPHRASE` desde el gestor de servicios. No existe fallback a texto plano ni se oculta un fallo del keyring con una copia vault. No copies tokens a unidades, archivos plist, logs o argumentos de comandos.

Usa los procedimientos de backup consistente y retencion admitidos por el despliegue; backup, restore, retencion, compactacion y fijado de ejecuciones no son operaciones del CLI canonico. Los procedimientos de restore deben usar un destino aislado y distinto. La [guia de migracion del CLI](docs/cli-migration.md) enumera estas eliminaciones e indica que no hay comandos CLI equivalentes. `scripts/measure-idle.ps1` conserva mediciones de memoria privada, CPU y procesos hijo para la evidencia de la fase.

## Verificar

```powershell
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```
