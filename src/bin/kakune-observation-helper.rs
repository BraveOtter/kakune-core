//! Isolated, non-mutating SQLite observation worker.

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn main() {
    let mut args = std::env::args_os();
    let _program = args.next();
    let Some(first) = args.next() else {
        std::process::exit(2);
    };
    let (mode, path) = if first == "--plugins" || first == "--catalog" {
        let Some(path) = args.next() else {
            std::process::exit(2)
        };
        (Some(first.to_string_lossy().into_owned()), path)
    } else {
        (None, first)
    };
    if args.next().is_some() {
        std::process::exit(2);
    }
    let result = observe(std::path::Path::new(&path), mode.as_deref())
        .unwrap_or_else(|_| serde_json::json!({"status": "unavailable"}));
    let output = result.to_string();
    if output.len() > 500_000 {
        println!("{{\"status\":\"unavailable\"}}");
    } else {
        println!("{output}");
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn main() {
    std::process::exit(2);
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn observe(path: &std::path::Path, mode: Option<&str>) -> Result<serde_json::Value, ()> {
    use nix::{
        fcntl::{FcntlArg, fcntl},
        libc,
    };
    use rusqlite::Connection;
    use std::{fs, io::Read, os::unix::fs::MetadataExt};

    const MAX_SNAPSHOT_BYTES: u64 = 256 * 1024 * 1024;
    let mut file = fs::File::open(path).map_err(|_| ())?;
    let lock = libc::flock {
        l_type: libc::F_RDLCK as _,
        l_whence: libc::SEEK_SET as _,
        l_start: 0,
        l_len: 0,
        l_pid: 0,
    };
    #[cfg(target_os = "linux")]
    fcntl(&file, FcntlArg::F_OFD_SETLK(&lock)).map_err(|_| ())?;
    // This worker is a separate process and opens only this descriptor for
    // the database, so closing it cannot release the Core's POSIX locks.
    #[cfg(target_os = "macos")]
    fcntl(&file, FcntlArg::F_SETLK(&lock)).map_err(|_| ())?;
    for suffix in ["-wal", "-shm", "-journal"] {
        let mut name = path.as_os_str().to_os_string();
        name.push(suffix);
        match fs::symlink_metadata(name) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            _ => return Err(()),
        }
    }
    let metadata = file.metadata().map_err(|_| ())?;
    let current = fs::metadata(path).map_err(|_| ())?;
    if !metadata.is_file()
        || metadata.len() == 0
        || metadata.len() > MAX_SNAPSHOT_BYTES
        || metadata.dev() != current.dev()
        || metadata.ino() != current.ino()
    {
        return Err(());
    }
    let mut bytes = Vec::new();
    file.by_ref()
        .take(MAX_SNAPSHOT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| ())?;
    let current = fs::metadata(path).map_err(|_| ())?;
    if bytes.len() as u64 != metadata.len()
        || metadata.dev() != current.dev()
        || metadata.ino() != current.ino()
        || (bytes.len() >= 20 && (bytes[18] == 2 || bytes[19] == 2))
    {
        return Err(());
    }
    drop(file);
    let mut db = Connection::open_in_memory().map_err(|_| ())?;
    db.deserialize_read_exact(rusqlite::MAIN_DB, bytes.as_slice(), bytes.len(), true)
        .map_err(|_| ())?;
    let recognized: bool = db.query_row(
        "SELECT (SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('workflows','node_runs','auth_tokens'))=3",
        [], |row| row.get(0),
    ).map_err(|_| ())?;
    if !recognized {
        return Ok(serde_json::json!({"status":"corrupt"}));
    }
    let migrations: bool = db.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_migrations')", [], |row| row.get(0)).map_err(|_| ())?;
    let version: u32 = if migrations {
        db.query_row(
            "SELECT COALESCE(MAX(version),0) FROM schema_migrations",
            [],
            |row| row.get(0),
        )
        .map_err(|_| ())?
    } else {
        0
    };
    let workflows: i64 = db
        .query_row("SELECT COUNT(*) FROM workflows", [], |row| row.get(0))
        .map_err(|_| ())?;
    let executions: bool = db
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='executions')",
            [],
            |row| row.get(0),
        )
        .map_err(|_| ())?;
    let executions: Option<i64> = if executions {
        Some(
            db.query_row("SELECT COUNT(*) FROM executions", [], |row| row.get(0))
                .map_err(|_| ())?,
        )
    } else {
        None
    };
    let supported = kakune_core::observation::SUPPORTED_SCHEMA_VERSION;
    if version == supported && executions.is_none() {
        return Ok(serde_json::json!({"status":"corrupt"}));
    }
    if mode == Some("--plugins") {
        if version != supported {
            return Err(());
        }
        let mut statement = db.prepare("SELECT substr(id,1,4097), substr(version,1,4097), substr(name,1,4097) FROM installed_plugins ORDER BY id LIMIT 10001").map_err(|_| ())?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(|_| ())?;
        let mut items = Vec::new();
        let mut bytes = 0_usize;
        for row in rows {
            let (id, version, name) = row.map_err(|_| ())?;
            if [id.len(), version.len(), name.len()]
                .into_iter()
                .any(|length| length > 4096)
            {
                return Err(());
            }
            bytes = bytes
                .saturating_add(id.len())
                .saturating_add(version.len())
                .saturating_add(name.len());
            if items.len() >= 10_000 || bytes > 1_048_576 {
                return Err(());
            }
            items.push((id, version, name));
        }
        return Ok(serde_json::json!({"status":"healthy", "schemaVersion":version, "items":items}));
    }
    if mode == Some("--catalog") {
        if version != supported {
            return Err(());
        }
        let pin_rows: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM (SELECT 1 FROM plugin_execution_pins LIMIT 100001)",
                [],
                |row| row.get(0),
            )
            .map_err(|_| ())?;
        if pin_rows > 100_000 {
            return Err(());
        }
        let mut statement = db
            .prepare(
                "WITH pin_counts AS (
                    SELECT plugin_id, COUNT(*) AS active_count
                    FROM plugin_execution_pins GROUP BY plugin_id
                 )
                 SELECT substr(p.id,1,4097), substr(p.name,1,4097), substr(p.version,1,4097),
                        p.enabled, substr(l.status,1,65), substr(l.failure_details,1,257),
                        COALESCE(pin_counts.active_count, 0)
                 FROM installed_plugins p
                 LEFT JOIN plugin_lifecycle l ON l.plugin_id=p.id
                 LEFT JOIN pin_counts ON pin_counts.plugin_id=p.id
                 ORDER BY p.id LIMIT 10001",
            )
            .map_err(|_| ())?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, bool>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, i64>(6)?,
                ))
            })
            .map_err(|_| ())?;
        let mut items = Vec::new();
        let mut bytes = 0_usize;
        for row in rows {
            let (id, name, version, enabled, status, failure_details, active_executions) =
                row.map_err(|_| ())?;
            let status = status.ok_or(())?;
            if [id.len(), name.len(), version.len(), status.len()]
                .into_iter()
                .any(|length| length > 4096)
                || failure_details
                    .as_ref()
                    .is_some_and(|details| details.len() > 256)
                || !matches!(
                    status.as_str(),
                    "active" | "pending removal" | "removal failed"
                )
                || active_executions < 0
            {
                return Err(());
            }
            bytes = bytes
                .saturating_add(id.len())
                .saturating_add(name.len())
                .saturating_add(version.len())
                .saturating_add(status.len())
                .saturating_add(failure_details.as_ref().map_or(0, String::len));
            if items.len() >= 10_000 || bytes > 1_048_576 {
                return Err(());
            }
            items.push(kakune_core::observation::PluginCatalogEntry {
                id,
                name,
                version,
                enabled,
                status,
                failure_details,
                active_executions: u64::try_from(active_executions).map_err(|_| ())?,
            });
        }
        return Ok(serde_json::json!({"status":"healthy", "schemaVersion":version, "items":items}));
    }
    Ok(
        serde_json::json!({"status": if version < supported {"legacy"} else if version > supported {"incompatible"} else {"healthy"}, "schemaVersion": version, "workflowCount": workflows, "executionCount": executions}),
    )
}
