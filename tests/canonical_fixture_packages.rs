use std::path::Path;

use kakune_core::plugin_process::PluginManifest;

#[test]
fn valid_fixture_manifest_loads_without_starting_the_plugin() {
    let manifest = PluginManifest::load(Path::new(
        "tests/fixtures/canonical_commands/valid/kakune-plugin.json",
    ));
    assert!(manifest.is_ok(), "fixture must load: {manifest:?}");
}

#[test]
fn malformed_fixture_manifest_is_rejected() {
    let manifest = PluginManifest::load(Path::new(
        "tests/fixtures/canonical_commands/malformed/kakune-plugin.json",
    ));
    assert!(manifest.is_err());
}

#[test]
fn incompatible_fixture_manifest_is_rejected() {
    let error = PluginManifest::load(Path::new(
        "tests/fixtures/canonical_commands/incompatible/kakune-plugin.json",
    ))
    .expect_err("incompatible protocol must be rejected");
    assert!(error.to_string().contains("pluginProtocol"));
}
