use std::{fs, path::PathBuf, time::Duration};

use kakune_core::plugin_install::{
    MAX_PLUGIN_FILES, MAX_PLUGIN_PACKAGE_BYTES, MAX_PLUGIN_PROCESS_OUTPUT, PLUGIN_ADMIN_TIMEOUT,
    package_digest,
};
use uuid::Uuid;

fn temp_dir() -> PathBuf {
    let root = std::env::temp_dir().join("opencode");
    fs::create_dir_all(&root).expect("approved temporary root exists");
    let path = root.join(format!("kakune-plugin-limits-{}", Uuid::new_v4()));
    fs::create_dir(&path).expect("isolated fixture directory is created");
    path
}

#[test]
fn package_walk_rejects_excessive_depth_and_file_count() {
    let root = temp_dir();
    let deep = root.join("deep");
    let mut current = deep.clone();
    for _ in 0..36 {
        current.push("a");
    }
    fs::create_dir_all(&current).expect("deep fixture directories are created");
    assert!(package_digest(&deep).unwrap_err().contains("depth limit"));

    let many = root.join("many");
    fs::create_dir(&many).expect("file-count fixture is created");
    for index in 0..=MAX_PLUGIN_FILES {
        fs::write(many.join(format!("f{index:05}")), b"").expect("bounded fixture file is created");
    }
    assert!(
        package_digest(&many)
            .unwrap_err()
            .contains("file-count limit")
    );
    fs::remove_dir_all(root).expect("fixture is cleaned");
}

#[test]
fn plugin_administration_limits_are_finite_and_explicit() {
    assert_eq!(MAX_PLUGIN_PACKAGE_BYTES, 256 * 1024 * 1024);
    assert_eq!(MAX_PLUGIN_PROCESS_OUTPUT, 1024 * 1024);
    const { assert!(MAX_PLUGIN_FILES > 0 && MAX_PLUGIN_FILES <= 10_000) };
    assert_eq!(PLUGIN_ADMIN_TIMEOUT, Duration::from_secs(30));
}
