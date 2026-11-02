mod support;

use std::fs;

use kakune_core::Store;
use rusqlite::Connection;
use support::management::ManagementFixture;

#[test]
fn schema_zero_upgrade_keeps_legacy_workflow_and_a_preupgrade_backup() {
    let fixture = ManagementFixture::new().unwrap();
    let data = fixture.path("schema-zero");
    fs::create_dir_all(&data).unwrap();
    let db = Connection::open(data.join("kakune.sqlite3")).unwrap();
    db.execute_batch(include_str!("fixtures/canonical_commands/schema-zero.sql"))
        .unwrap();
    drop(db);
    let store = Store::open(data.clone()).unwrap();
    assert_eq!(store.schema_version().unwrap(), 17);
    let records = store.list_workflows().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].id, "legacy-fixture");
    assert_eq!(records[0].status, "disabled");
    drop(store);
    let backup = fs::read_dir(data.join("backups"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .contains("before-migration-v1")
        })
        .unwrap();
    let db = Connection::open(backup).unwrap();
    let source: String = db
        .query_row(
            "SELECT source FROM workflows WHERE id='legacy-fixture'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(source, "fixture source");
    let has_migrations: bool = db
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='schema_migrations')",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(!has_migrations);
}

#[test]
fn schema_sixteen_upgrade_backs_up_existing_plugin_and_staging_records() {
    let fixture = ManagementFixture::new().unwrap();
    let data = fixture.path("migration");
    drop(Store::open(data.clone()).unwrap());
    let database = data.join("kakune.sqlite3");
    let db = Connection::open(&database).unwrap();
    db.execute_batch(
        "PRAGMA journal_mode=DELETE;
         DROP TRIGGER IF EXISTS installed_plugin_lifecycle_insert;
         DROP TRIGGER IF EXISTS execution_release_plugin_pins;
         DROP TABLE IF EXISTS plugin_execution_pins;
         DROP TABLE IF EXISTS plugin_removal_workflows;
         DROP TABLE IF EXISTS plugin_lifecycle;
         DELETE FROM schema_migrations WHERE version > 16;
         INSERT INTO installed_plugins (id,name,version,manifest_path,installed_at,digest)
           VALUES ('example','Example','1.0.0','active/manifest.json','now','active-digest');
         INSERT INTO prepared_plugin_installs (id,plugin_id,plugin_name,version,digest,staged_path,prepared_at,policy_json,lock_json,provenance_json)
           VALUES ('staged','example','Example','2.0.0','staged-digest','staging/path','now','{}','{}','{}');",
    ).unwrap();
    drop(db);

    let store = Store::open(data.clone()).unwrap();
    assert_eq!(store.schema_version().unwrap(), 17);
    assert_eq!(
        store.list_installed_plugins().unwrap()[0].digest,
        "active-digest"
    );
    assert_eq!(
        store
            .prepared_plugin_install("staged")
            .unwrap()
            .unwrap()
            .digest,
        "staged-digest"
    );
    drop(store);
    let db = Connection::open(&database).unwrap();
    let status: String = db
        .query_row(
            "SELECT status FROM plugin_lifecycle WHERE plugin_id='example'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(status, "active");
    let backup = fs::read_dir(data.join("backups"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .contains("before-migration-v17")
        })
        .unwrap();
    let db = Connection::open(backup).unwrap();
    let version: i64 = db
        .query_row("SELECT MAX(version) FROM schema_migrations", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(version, 16);
    let digest: String = db
        .query_row(
            "SELECT digest FROM installed_plugins WHERE id='example'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(digest, "active-digest");
}
