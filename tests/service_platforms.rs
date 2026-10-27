mod support;

use std::{path::Path, time::Duration};

use kakune_core::lifecycle::{
    LifecycleError, NativeManagerState,
    service::{
        NativeMutation, ServiceAction, ServiceDefinition, ServiceManagerKind, parse_manager_state,
        parse_systemd_enabled, render_service_definition,
    },
};
use support::lifecycle::LifecycleFixture;

fn definition(
    manager: ServiceManagerKind,
    service_id: &str,
    executable: &Path,
    data_dir: &Path,
    account: &str,
) -> ServiceDefinition {
    ServiceDefinition::new(
        manager,
        service_id,
        executable,
        vec![
            "service".into(),
            "run".into(),
            "--data-dir".into(),
            data_dir.display().to_string(),
        ],
        account,
        data_dir,
    )
    .expect("native service definition should validate")
}

#[test]
fn platform_identifiers_accounts_and_boot_policy_are_explicit() {
    let fixture = LifecycleFixture::new().unwrap();
    let executable = std::env::current_exe().unwrap();
    let data = fixture.data_dir();

    let windows = definition(
        ServiceManagerKind::WindowsScm,
        "KakuneCore",
        &executable,
        &data,
        "LocalSystem",
    );
    let linux = definition(
        ServiceManagerKind::LinuxSystemd,
        "kakune-core.service",
        &executable,
        &data,
        "root",
    );
    let macos = definition(
        ServiceManagerKind::MacosLaunchd,
        "dev.kakune.core",
        &executable,
        &data,
        "root",
    );

    assert_eq!(windows.canonical_service_id(), "kakunecore");
    assert_eq!(linux.canonical_service_id(), "kakune-core.service");
    assert_eq!(macos.canonical_service_id(), "dev.kakune.core");
    assert_eq!(windows.account, "LocalSystem");
    assert_eq!(linux.account, "root");
    assert_eq!(macos.account, "root");
    assert_eq!(
        windows.boot_policy,
        kakune_core::lifecycle::service::BootPolicy::Automatic
    );
    assert!(windows.definition_fingerprint.is_some());
}

#[test]
fn windows_definition_quotes_absolute_paths_with_spaces_and_non_ascii() {
    let fixture = LifecycleFixture::new().unwrap();
    let directory = fixture.root().join("Kakune 雪 service");
    let executable = directory.join("kakune core.exe");
    let data = directory.join("data space");
    let definition = definition(
        ServiceManagerKind::WindowsScm,
        "KakuneCore",
        &executable,
        &data,
        "LocalSystem",
    );

    let rendered = render_service_definition(&definition).unwrap();
    assert!(rendered.contains("service run"));
    assert!(rendered.contains("--data-dir"));
    assert!(rendered.contains(&data.to_string_lossy().to_string()));
    assert!(rendered.contains("雪"));
}

#[test]
fn systemd_definition_is_root_owned_shape_and_installs_without_starting() {
    let fixture = LifecycleFixture::new().unwrap();
    let data = fixture.root().join("kakune 雪 $state 100% data");
    let executable = fixture.root().join("kakune core");
    let definition = definition(
        ServiceManagerKind::LinuxSystemd,
        "kakune-core.service",
        &executable,
        &data,
        "root",
    );
    let rendered = render_service_definition(&definition).unwrap();

    assert!(rendered.contains("Type=notify"));
    assert!(rendered.contains("NotifyAccess=main"));
    assert!(rendered.contains("User=root"));
    assert!(rendered.contains("WantedBy=multi-user.target"));
    assert!(rendered.contains("KillMode=mixed"));
    assert!(rendered.contains("SendSIGKILL=no"));
    assert!(rendered.contains("TimeoutStopSec=30"));
    assert!(rendered.contains("\\x20"));
    assert!(rendered.contains("$$state"));
    assert!(rendered.contains("100%%"));
    assert!(rendered.contains("\\雪") || rendered.contains("雪"));
    assert!(!rendered.contains("--now"));
    assert!(!rendered.contains("%h"));
}

#[test]
#[cfg(target_os = "linux")]
fn packaged_linux_unit_matches_the_root_service_definition() {
    let definition = definition(
        ServiceManagerKind::LinuxSystemd,
        "kakune-core.service",
        Path::new("/usr/local/bin/kakune"),
        Path::new("/var/lib/kakune"),
        "root",
    );
    assert_eq!(
        render_service_definition(&definition).unwrap(),
        include_str!("../packaging/linux/kakune-core.service")
    );
}

#[test]
fn theoretical_launchd_definition_uses_direct_absolute_arguments_and_explicit_shutdown_policy() {
    let fixture = LifecycleFixture::new().unwrap();
    let executable = fixture.root().join("Kakune 雪/kakune core");
    let data = fixture.root().join("Kakune 雪/data & state");
    let definition = definition(
        ServiceManagerKind::MacosLaunchd,
        "dev.kakune.core",
        &executable,
        &data,
        "root",
    );
    let rendered = render_service_definition(&definition).unwrap();

    assert!(rendered.contains("dev.kakune.core"));
    assert!(rendered.contains("ProgramArguments"));
    assert!(rendered.contains("/kakune core"));
    assert!(rendered.contains("&amp;"));
    assert!(rendered.contains("RunAtLoad"));
    assert!(rendered.contains("KeepAlive"));
    assert!(rendered.contains("ExitTimeOut"));
    assert!(!rendered.contains("/bin/sh"));
    assert!(!rendered.contains("$HOME"));
}

#[test]
fn native_mutation_plans_separate_publication_boot_enablement_and_live_domain_changes() {
    use NativeMutation::*;
    assert_eq!(
        NativeMutation::steps(ServiceManagerKind::WindowsScm, ServiceAction::Install),
        &[Register, Description, FailurePolicy]
    );
    assert_eq!(
        NativeMutation::steps(ServiceManagerKind::LinuxSystemd, ServiceAction::Install),
        &[Publish, Reload, Enable]
    );
    assert_eq!(
        NativeMutation::steps(ServiceManagerKind::LinuxSystemd, ServiceAction::Remove),
        &[Disable, Delete, Reload]
    );
    // Theoretical: publishing the boot plist must not bootstrap its live domain.
    assert_eq!(
        NativeMutation::steps(ServiceManagerKind::MacosLaunchd, ServiceAction::Install),
        &[Publish]
    );
    assert_eq!(
        NativeMutation::steps(ServiceManagerKind::MacosLaunchd, ServiceAction::Start),
        &[Start]
    );
    assert_eq!(
        NativeMutation::steps(ServiceManagerKind::MacosLaunchd, ServiceAction::Stop),
        &[Stop]
    );
    assert_eq!(
        NativeMutation::steps(ServiceManagerKind::MacosLaunchd, ServiceAction::Remove),
        &[Delete]
    );
}

#[test]
fn production_adapters_refuse_unjournaled_mutation_without_contacting_native_managers() {
    let fixture = LifecycleFixture::new().unwrap();
    let definition = ServiceDefinition::for_platform(&fixture.data_dir()).unwrap();
    let adapter = kakune_core::lifecycle::service::system_manager().unwrap();
    let clock = kakune_core::lifecycle::SystemMonotonicClock::default();
    assert!(adapter.requires_owned_helper());
    for action in [
        ServiceAction::Install,
        ServiceAction::Start,
        ServiceAction::Stop,
        ServiceAction::Remove,
    ] {
        assert!(matches!(
            adapter.mutate(
                action,
                definition.manager,
                &definition.service_id,
                Some(&definition),
                kakune_core::lifecycle::Deadline::after(&clock, Duration::from_secs(10)),
                &clock
            ),
            Err(LifecycleError::UnsafeOwnership(_))
        ));
    }
}

#[test]
#[cfg(target_os = "macos")]
fn theoretical_launchd_template_matches_the_direct_root_definition() {
    let definition = definition(
        ServiceManagerKind::MacosLaunchd,
        "dev.kakune.core",
        Path::new("/usr/local/bin/kakune"),
        Path::new("/Library/Application Support/Kakune"),
        "root",
    );
    assert_eq!(
        render_service_definition(&definition).unwrap(),
        include_str!("../packaging/macos/dev.kakune.core.plist")
    );
}

#[test]
fn native_manager_states_distinguish_pending_failed_unknown_and_running() {
    assert_eq!(
        parse_manager_state(
            ServiceManagerKind::LinuxSystemd,
            "LoadState=loaded\nActiveState=activating\nSubState=start"
        )
        .unwrap(),
        NativeManagerState::Starting
    );
    assert_eq!(
        parse_manager_state(
            ServiceManagerKind::LinuxSystemd,
            "LoadState=loaded\nActiveState=active\nSubState=running"
        )
        .unwrap(),
        NativeManagerState::Running
    );
    assert_eq!(
        parse_manager_state(
            ServiceManagerKind::LinuxSystemd,
            "LoadState=not-found\nActiveState=inactive\nSubState=dead"
        )
        .unwrap(),
        NativeManagerState::Stopped
    );
    assert_eq!(
        parse_manager_state(
            ServiceManagerKind::LinuxSystemd,
            "LoadState=loaded\nActiveState=failed\nSubState=failed"
        )
        .unwrap(),
        NativeManagerState::Failed
    );
    assert_eq!(
        parse_manager_state(
            ServiceManagerKind::WindowsScm,
            "STATE              : 2  START_PENDING"
        )
        .unwrap(),
        NativeManagerState::Starting
    );
    assert_eq!(
        parse_manager_state(
            ServiceManagerKind::WindowsScm,
            "STATE              : 7  PAUSED"
        )
        .unwrap(),
        NativeManagerState::Unknown
    );
    assert_eq!(
        parse_manager_state(
            ServiceManagerKind::WindowsScm,
            "STATE              : 1  STOPPED\nWIN32_EXIT_CODE    : 0  (0x0)"
        )
        .unwrap(),
        NativeManagerState::Stopped
    );
    assert_eq!(
        parse_manager_state(
            ServiceManagerKind::WindowsScm,
            "STATE              : 1  STOPPED\nWIN32_EXIT_CODE    : 1066  (0x42a)\nSERVICE_EXIT_CODE  : 55  (0x37)"
        )
        .unwrap(),
        NativeManagerState::Failed
    );
    assert_eq!(
        parse_manager_state(
            ServiceManagerKind::WindowsScm,
            "STATE              : 4  RUNNING"
        )
        .unwrap(),
        NativeManagerState::Running
    );
    assert_eq!(
        parse_manager_state(
            ServiceManagerKind::WindowsScm,
            "STATE              : 3  STOP_PENDING"
        )
        .unwrap(),
        NativeManagerState::Stopping
    );
    assert_eq!(
        parse_manager_state(
            ServiceManagerKind::LinuxSystemd,
            "LoadState=loaded\nActiveState=deactivating\nSubState=stop-sigterm"
        )
        .unwrap(),
        NativeManagerState::Stopping
    );
    assert_eq!(
        parse_manager_state(
            ServiceManagerKind::MacosLaunchd,
            "service = { state = waiting }"
        )
        .unwrap(),
        NativeManagerState::Stopped
    );
    assert_eq!(
        parse_manager_state(
            ServiceManagerKind::MacosLaunchd,
            "service = { state = exited }"
        )
        .unwrap(),
        NativeManagerState::Failed
    );
    assert_eq!(
        parse_manager_state(
            ServiceManagerKind::MacosLaunchd,
            "service = { state = spawn }"
        )
        .unwrap(),
        NativeManagerState::Starting
    );
    assert_eq!(
        parse_manager_state(
            ServiceManagerKind::MacosLaunchd,
            "service = { state = running }"
        )
        .unwrap(),
        NativeManagerState::Running
    );
    assert_eq!(
        parse_manager_state(
            ServiceManagerKind::MacosLaunchd,
            "service = { state = stopping }"
        )
        .unwrap(),
        NativeManagerState::Stopping
    );
    assert_eq!(
        parse_manager_state(ServiceManagerKind::LinuxSystemd, "not-a-manager-response").unwrap(),
        NativeManagerState::Unknown
    );
}

#[test]
fn native_state_parser_never_maps_permission_or_manager_errors_to_absence() {
    assert!(
        parse_manager_state(
            ServiceManagerKind::LinuxSystemd,
            "Failed to connect to bus: Permission denied"
        )
        .is_err()
    );
    assert!(
        parse_manager_state(
            ServiceManagerKind::WindowsScm,
            "OpenSCManager failed 5: Access is denied."
        )
        .is_err()
    );
    assert!(matches!(
        parse_manager_state(
            ServiceManagerKind::MacosLaunchd,
            "Bootstrap failed: 125: Domain does not support specified action: Operation not permitted"
        ),
        Err(LifecycleError::AccessDenied(_))
    ));
    assert!(matches!(
        parse_systemd_enabled("Operation not permitted", false),
        Err(LifecycleError::AccessDenied(_))
    ));
    assert!(matches!(
        parse_systemd_enabled("Failed to connect to bus", false),
        Err(LifecycleError::ManagerUnavailable(_))
    ));
}

#[test]
fn systemd_enablement_parser_distinguishes_automatic_disabled_and_unknown() {
    assert!(parse_systemd_enabled("enabled\n", true).unwrap());
    assert!(!parse_systemd_enabled("disabled\n", false).unwrap());
    assert!(!parse_systemd_enabled("enabled-runtime\n", true).unwrap());
    assert!(!parse_systemd_enabled("static\n", false).unwrap());
    assert!(matches!(
        parse_systemd_enabled("unrecognized-state", false),
        Err(LifecycleError::ManagerUnavailable(_))
    ));
}

#[test]
fn native_definitions_do_not_contain_credentials_or_user_shell_expansion() {
    let fixture = LifecycleFixture::new().unwrap();
    let definition = definition(
        ServiceManagerKind::LinuxSystemd,
        "kakune-core.service",
        std::env::current_exe().unwrap().as_path(),
        fixture.data_dir().as_path(),
        "root",
    );
    let rendered = render_service_definition(&definition).unwrap();
    assert!(!rendered.contains("password"));
    assert!(!rendered.contains("token"));
    assert!(!rendered.contains("$HOME"));
    drop(fixture);
}

#[test]
fn service_definition_uses_a_single_automatic_boot_policy_and_absolute_paths() {
    let fixture = LifecycleFixture::new().unwrap();
    let definition = definition(
        ServiceManagerKind::LinuxSystemd,
        "kakune-core.service",
        std::env::current_exe().unwrap().as_path(),
        fixture.data_dir().as_path(),
        "root",
    );
    assert!(definition.executable.is_absolute());
    assert!(definition.data_dir.is_absolute());
    assert_eq!(
        definition.boot_policy,
        kakune_core::lifecycle::service::BootPolicy::Automatic
    );
    assert_eq!(
        Duration::from_secs(30),
        kakune_core::lifecycle::INSTALL_BUDGET
    );
}
