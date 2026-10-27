mod support;

use std::time::Duration;

use support::lifecycle::{
    ControlResult, CredentialAdapter, CredentialAvailability, LifecycleFixture,
    LifecycleTestAdapters, LocalControlAdapter, MonotonicClock, MutationDisposition,
    NativeObservation, NativeServiceAdapter, NativeServiceState, ProcessEvidence,
    ProcessEvidenceAdapter, TestOwnedResource,
};

#[test]
fn default_lifecycle_adapters_are_isolated_and_in_memory() {
    let fixture = LifecycleFixture::new().expect("isolated lifecycle fixture should be created");
    let fixture_root = fixture.root().to_path_buf();
    assert_eq!(fixture.resources().fixture_id(), fixture.id());
    let store = fixture
        .open_store()
        .expect("fixture store should open in isolated data directory");
    assert!(fixture.data_dir().starts_with(&fixture_root));
    assert!(fixture.config_file().starts_with(&fixture_root));
    assert!(fixture.log_file().starts_with(&fixture_root));
    assert!(
        fixture
            .resources()
            .snapshot()
            .iter()
            .any(|resource| matches!(
                resource,
                TestOwnedResource::Filesystem(path) if path.starts_with(&fixture_root)
            ))
    );
    drop(store);
    assert!(fixture.resources().release(&TestOwnedResource::Filesystem(
        fixture.data_dir().join("kakune.sqlite3"),
    )));

    let adapters = LifecycleTestAdapters::new(fixture.resources().clone());
    assert_eq!(adapters.clock.now(), Duration::ZERO);
    adapters.clock.advance(Duration::from_secs(10));
    assert_eq!(adapters.clock.now(), Duration::from_secs(10));

    let evidence = ProcessEvidence {
        pid: 1234,
        owner: Some("test-owner".to_owned()),
        executable: Some(fixture_root.join("kakune-test.exe")),
        start_identity: Some("test-start-1".to_owned()),
        alive: true,
    };
    adapters.processes.insert(evidence.clone());
    assert_eq!(adapters.processes.observe(1234), Some(evidence));
    adapters.control.set_result(ControlResult::TimedOut);
    assert_eq!(adapters.control.request("stop"), ControlResult::TimedOut);
    adapters.control.set_result(ControlResult::Rejected);
    assert_eq!(
        adapters.control.request("wrong-operation"),
        ControlResult::Rejected
    );
    adapters.control.set_result(ControlResult::Unavailable);
    assert_eq!(
        adapters.control.request("unavailable"),
        ControlResult::Unavailable
    );
    adapters.control.set_result(ControlResult::Acknowledged);
    assert_eq!(
        adapters.control.request("status"),
        ControlResult::Acknowledged
    );
    assert_eq!(
        adapters.control.requests(),
        ["stop", "wrong-operation", "unavailable", "status"]
    );

    assert_eq!(
        adapters
            .native_manager
            .install("fake-kakune-service", "isolated-test-definition")
            .expect("fake manager should accept its in-memory test registration"),
        MutationDisposition::Changed
    );
    assert_eq!(
        adapters.native_manager.observe("fake-kakune-service"),
        NativeObservation::Installed {
            definition: "isolated-test-definition".to_owned(),
            state: NativeServiceState::Stopped,
        }
    );
    for state in [
        NativeServiceState::Starting,
        NativeServiceState::Stopping,
        NativeServiceState::Failed,
        NativeServiceState::Unknown,
    ] {
        adapters
            .native_manager
            .set_state_for_test("fake-kakune-service", state)
            .expect("fake manager should change only its in-memory observation");
        assert_eq!(
            adapters.native_manager.observe("fake-kakune-service"),
            NativeObservation::Installed {
                definition: "isolated-test-definition".to_owned(),
                state,
            }
        );
    }
    assert_eq!(
        adapters
            .native_manager
            .start("fake-kakune-service")
            .expect("fake manager should start only its in-memory registration"),
        MutationDisposition::Changed
    );
    assert_eq!(
        adapters
            .native_manager
            .stop("fake-kakune-service")
            .expect("fake manager should stop only its in-memory registration"),
        MutationDisposition::Changed
    );
    adapters
        .credentials
        .set_availability("test-only:fixture", CredentialAvailability::Available);
    assert_eq!(
        adapters.credentials.availability("test-only:fixture"),
        CredentialAvailability::Available
    );
    adapters
        .credentials
        .set_availability("test-only:denied", CredentialAvailability::Denied);
    assert_eq!(
        adapters.credentials.availability("test-only:denied"),
        CredentialAvailability::Denied
    );

    assert!(adapters.processes.remove(1234).is_some());
    assert!(adapters.credentials.remove("test-only:fixture"));
    assert!(adapters.credentials.remove("test-only:denied"));
    assert_eq!(
        adapters
            .native_manager
            .uninstall("fake-kakune-service")
            .expect("fake manager should remove only its in-memory registration"),
        MutationDisposition::Changed
    );
    assert!(fixture.resources().is_empty());
}
