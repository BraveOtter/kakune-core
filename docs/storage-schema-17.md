# Storage schema 17

Opening an existing supported database with this Core first creates a SQLite backup in the data directory's `backups/` directory, then upgrades the schema. The backup name identifies the first migration to be applied. Do not delete the database to upgrade it. If backup or migration fails, preserve the original database and backup for investigation before retrying.

Schema 17 adds plugin lifecycle state, update-source metadata, affected workflow references, and execution content pins. Existing installed and staged plugin records are retained; existing installations start in the `active` state. Staged content remains separate from active installation records. Previous Core versions do not understand schema 17; restoring a pre-upgrade backup is necessary to return to an older Core and discards changes made after that backup.

Local read-only inspection does not migrate an old database. If SQLite WAL files are present, local inspection reports that a safe observation is unavailable; use the selected running Core where supported or stop it and retry after SQLite has settled.

## Concurrency and trust limits

The Core serializes plugin admission, pin acquisition and removal intent through SQLite transactions. A pin prevents Core-managed replacement and direct deletion while an execution holds it. SQLite lock contention has a finite wait; a failed removal request does not partially disable workflows. These guarantees apply only to participants using the Core's coordination boundary.

Pins do not make package files immutable against a process running as the Core user. The Core compares a bounded package digest at catalog loading, admission and immediately before starting a plugin, but another process can still alter files between verification and execution or while the plugin runs. Plugins run with the Core user's privileges; these checks are not a sandbox or protection against a malicious same-user process. Do not grant untrusted users or processes write access to the Core's plugin directories. Fully immutable content publication and execution-aware removal reconciliation remain part of the later plugin lifecycle work.

Local observation checks the database header and sidecars and reports unavailable when it detects WAL mode, sidecars or concurrent changes. It does not use SQLite's immutable mode to bypass live WAL state.

On Windows, local SQLite observation copies the database under a handle that excludes writes and replacement, then queries the copy in memory. The default library observer on other platforms reports local database and plugin catalog observation unavailable, including otherwise healthy offline databases. Linux and macOS library consumers may explicitly configure the trusted `kakune-observation-helper` executable through `IsolatedDatabaseObservationExecutor`; their CLI uses the helper beside its own executable for `doctor` and local `plugin list`. The worker runs separately because opening and closing another descriptor in the Core process can release SQLite's POSIX locks. The helper and executor reject WAL or journal sidecars, limit the snapshot size and bound process time and output. If the helper is missing or cannot run, observation reports unavailable. The macOS implementation has not yet been executed on a native runner. Use the running Core where supported. Platform-specific safe snapshot support remains unfinished.

## Published observation and coordination surfaces

The shared read-only observation service is available to library consumers through `kakune_core::observation`, and the Core publishes it as `GET /api/v1/observation`. That route requires a Core credential with read scope, returns only status and guidance for the configuration, storage directory and database, and never creates, repairs or migrates storage. It never returns configuration content, credential references or secret values, and the library performs no console I/O for it.

The coordination boundary is available to library consumers through `kakune_core::management` and `kakune_core::Store`, and the Core publishes plugin lifecycle, admission and removal state through the existing management routes. Those routes keep their operation-scoped authorization checks.
