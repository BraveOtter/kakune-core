mod support;

use std::{fs, time::Duration};

use support::management::{
    CorePair, CredentialFailure, CredentialResolver, FailureInjector, FakeClock,
    FakeCredentialStore, FilesystemSnapshot, ManagementFixture, MonotonicClock,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn management_helpers_isolate_cores_files_credentials_and_failures() {
    let fixture = ManagementFixture::new().expect("management fixture should be created");
    let mut cores = CorePair::start(&fixture)
        .await
        .expect("two isolated Core servers should start");
    assert_ne!(
        cores.local.store.core_id().unwrap(),
        cores.remote.store.core_id().unwrap()
    );
    assert_ne!(cores.local.endpoint, cores.remote.endpoint);
    assert_ne!(cores.local.token, cores.remote.token);

    let before = fixture.path("observation");
    fs::create_dir_all(&before).expect("snapshot fixture directory should be created");
    fs::write(before.join("state.json"), br#"{"unchanged":true}"#)
        .expect("snapshot file should be created");
    let snapshot = FilesystemSnapshot::capture(&before).expect("filesystem should be snapshotted");
    snapshot
        .ensure_unchanged(&before)
        .expect("an observational check should preserve fixture files");

    let clock = FakeClock::default();
    assert_eq!(clock.now(), Duration::ZERO);
    clock.advance(Duration::from_secs(12));
    assert_eq!(clock.now(), Duration::from_secs(12));

    let credentials = FakeCredentialStore::default();
    credentials.insert("env:TEST_API_KEY", "secret-canary");
    assert_eq!(
        credentials.resolve("env:TEST_API_KEY"),
        Ok("secret-canary".to_owned())
    );
    credentials.fail_next_resolution();
    assert_eq!(
        credentials.resolve("env:TEST_API_KEY"),
        Err(CredentialFailure::Injected)
    );
    assert_eq!(
        credentials.requests(),
        ["env:TEST_API_KEY", "env:TEST_API_KEY"]
    );
    assert_eq!(
        credentials.resolve("env:MISSING"),
        Err(CredentialFailure::Missing)
    );

    let failures = FailureInjector::default();
    failures.fail_on_call("metadata.publish", 2, "simulated publication failure");
    assert_eq!(failures.check("metadata.publish"), Ok(()));
    let failure = failures
        .check("metadata.publish")
        .expect_err("the configured second operation should fail deterministically");
    assert_eq!(failure.point, "metadata.publish");
    assert_eq!(failure.message, "simulated publication failure");
    assert_eq!(failures.calls("metadata.publish"), 2);

    cores
        .shutdown()
        .await
        .expect("both isolated Core servers should stop cleanly");
}
