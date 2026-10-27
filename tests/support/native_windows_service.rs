//! Explicit LocalSystem service/boot acceptance in an approved disposable VM.

use super::*;
use std::io::Write;

use kakune_core::lifecycle::{
    CoreMode,
    ownership::{OwnerIdentity, RuntimeOwnershipRecord},
};
use windows_permissions::{
    LocalBox, SecurityDescriptor,
    constants::{AccessRights, AceType, SeObjectType, SecurityInformation},
    wrappers::{
        ConvertSecurityDescriptorToStringSecurityDescriptor as to_sddl,
        GetNamedSecurityInfo as get_security_info, SetNamedSecurityInfo as set_security_info,
    },
};

fn private_descriptor() -> LocalBox<SecurityDescriptor> {
    "O:SYD:P(A;;FA;;;SY)(A;;FA;;;BA)".parse().unwrap()
}

fn verify_private_inventory(path: &Path) {
    use std::os::windows::fs::MetadataExt;
    for ancestor in path.ancestors() {
        let metadata = fs::symlink_metadata(ancestor).unwrap();
        assert!(!metadata.file_type().is_symlink());
        assert_eq!(
            metadata.file_attributes() & 0x400,
            0,
            "redirected inventory"
        );
    }
    let descriptor = get_security_info(
        path,
        SeObjectType::SE_FILE_OBJECT,
        SecurityInformation::Owner | SecurityInformation::Dacl,
    )
    .unwrap();
    verify_private_descriptor(&descriptor).expect("inventory must remain System/Admin-only");
}

fn verify_private_descriptor(descriptor: &SecurityDescriptor) -> Result<(), &'static str> {
    if descriptor
        .owner()
        .is_none_or(|owner| owner.to_string() != "S-1-5-18")
    {
        return Err("inventory owner is not LocalSystem");
    }
    let dacl = descriptor.dacl().ok_or("inventory DACL is missing")?;
    let sddl = to_sddl(
        descriptor,
        SecurityInformation::Owner | SecurityInformation::Dacl,
    )
    .unwrap()
    .to_string_lossy()
    .into_owned();
    let start = sddl.find("D:").ok_or("inventory DACL cannot be verified")?;
    if !sddl[start..].starts_with("D:P") || dacl.len() != 2 {
        return Err("inventory DACL is not protected and restricted");
    }
    let mut observed = std::collections::HashSet::new();
    for index in 0..dacl.len() {
        let ace = dacl.get_ace(index).ok_or("inventory ACE is unreadable")?;
        let sid = ace.sid().ok_or("inventory ACE has no SID")?.to_string();
        if ace.ace_type() != AceType::ACCESS_ALLOWED_ACE_TYPE
            || !ace.flags().is_empty()
            || ace.mask() != AccessRights::FileAllAccess
            || !["S-1-5-18", "S-1-5-32-544"].contains(&sid.as_str())
            || !observed.insert(sid)
        {
            return Err("inventory ACE grants unexpected access");
        }
    }
    Ok(())
}

#[test]
fn boot_inventory_acl_accepts_auto_inheritance_metadata_but_not_inherited_access() {
    for (sddl, valid) in [
        ("O:SYD:P(A;;FA;;;SY)(A;;FA;;;BA)", true),
        ("O:SYD:PAI(A;;FA;;;SY)(A;;FA;;;BA)", true),
        ("O:SYD:AI(A;;FA;;;SY)(A;;FA;;;BA)", false),
        ("O:SYD:P(A;ID;FA;;;SY)(A;;FA;;;BA)", false),
        ("O:SYD:P(A;;FA;;;SY)(A;;FA;;;BA)(A;;FR;;;WD)", false),
        ("O:BAD:P(A;;FA;;;SY)(A;;FA;;;BA)", false),
    ] {
        let descriptor: LocalBox<SecurityDescriptor> = sddl.parse().unwrap();
        assert_eq!(
            verify_private_descriptor(&descriptor).is_ok(),
            valid,
            "{sddl}"
        );
    }
}

fn persistence_snapshot(data: &Path) -> serde_json::Value {
    let store = Store::open(data.to_path_buf()).unwrap();
    let executions = store.list_executions().unwrap();
    let source = store
        .get_workflow_source("native-acceptance")
        .unwrap()
        .unwrap();
    let nodes = executions
        .iter()
        .map(|execution| {
            (
                execution.id.clone(),
                store.list_node_runs(&execution.id).unwrap(),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    json!({
        "coreId": store.core_id().unwrap(),
        "tokens": store.list_auth_tokens().unwrap(),
        "workflows": store.list_workflows().unwrap(),
        "source": {"yaml": source.source, "revision": source.revision},
        "executions": executions,
        "nodes": nodes,
        "config": fs::read_to_string(data.join("kakune.yaml")).unwrap(),
    })
}

pub(super) fn write_boot_inventory(
    path: &Path,
    fixture: &NativeFixture,
    data: &Path,
    listen: &str,
    token: &str,
) {
    let log_dir = data.join("logs");
    fs::create_dir_all(&log_dir).unwrap();
    fs::write(log_dir.join("retained-native.log"), b"pre-boot test log\n").unwrap();
    let mut file = fs::File::create_new(path).unwrap();
    let descriptor = private_descriptor();
    set_security_info(
        path,
        SeObjectType::SE_FILE_OBJECT,
        SecurityInformation::Owner | SecurityInformation::Dacl | SecurityInformation::ProtectedDacl,
        descriptor.owner(),
        None,
        descriptor.dacl(),
        None,
    )
    .unwrap();
    verify_private_inventory(path);
    // Apply and verify protection while empty, before any fixture token is written.
    let inventory = json!({
        "fixtureId": fixture.id, "dataDir": data, "listen": listen, "token": token,
        "executable": env!("CARGO_BIN_EXE_kakune"), "serviceId": "KakuneCore",
        "beforeBoot": persistence_snapshot(data),
    });
    file.write_all(&serde_json::to_vec(&inventory).unwrap())
        .unwrap();
    file.sync_all().unwrap();
}

fn verify_persistence(data: &Path, before: &serde_json::Value, token: &str) {
    let after = persistence_snapshot(data);
    for key in ["coreId", "tokens", "workflows", "source", "config"] {
        assert_eq!(after[key], before[key], "retained {key}");
    }
    for execution in before["executions"].as_array().unwrap() {
        assert!(
            after["executions"].as_array().unwrap().contains(execution),
            "lost or changed pre-boot execution"
        );
    }
    for (id, nodes) in before["nodes"].as_object().unwrap() {
        assert_eq!(&after["nodes"][id], nodes, "retained node history");
    }
    assert!(
        Store::open(data.to_path_buf())
            .unwrap()
            .authorize(token)
            .unwrap()
    );
    assert_eq!(
        fs::read(data.join("logs/retained-native.log")).unwrap(),
        b"pre-boot test log\n"
    );
    verify_retention_sentinel(&data.join("retained-by-service-uninstall.txt")).unwrap();
}

async fn accepted_workflow(listen: &str, token: &str, data: &Path) -> String {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let response = client
        .post(format!("http://{listen}/api/v1/executions"))
        .bearer_auth(token)
        .json(&json!({"workflowId":"native-acceptance"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    let id = response.json::<serde_json::Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let execution = Store::open(data.to_path_buf())
            .unwrap()
            .get_execution(&id)
            .unwrap()
            .unwrap();
        if execution.status == "succeeded" {
            return id;
        }
        assert!(Instant::now() < deadline, "workflow did not complete");
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
}

#[tokio::test]
#[ignore = "requires approved VM reboot and a protected staged LocalSystem boot inventory"]
async fn windows_native_service_post_boot_and_preservation() {
    // Unlike an absent-registration cycle, this stage explicitly expects the
    // exact test-owned registration retained by the pre-boot procedure.
    assert_eq!(
        std::env::var("KAKUNE_RUN_NATIVE_ACCEPTANCE").as_deref(),
        Ok("1")
    );
    assert_eq!(
        std::env::var("KAKUNE_NATIVE_POST_BOOT_APPROVAL").as_deref(),
        Ok("APPROVED-STAGED-KAKUNECORE-REBOOT")
    );
    let path = PathBuf::from(std::env::var_os("KAKUNE_NATIVE_BOOT_INVENTORY").unwrap());
    let root = path.parent().unwrap().canonicalize().unwrap();
    assert_eq!(
        root.parent(),
        Some(
            approved_native_root()
                .unwrap()
                .canonicalize()
                .unwrap()
                .as_path()
        )
    );
    verify_private_inventory(&path);
    let inventory: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    let id = inventory["fixtureId"].as_str().unwrap();
    assert_eq!(fs::read_to_string(root.join(FIXTURE_MARKER)).unwrap(), id);
    assert_eq!(inventory["executable"], env!("CARGO_BIN_EXE_kakune"));
    assert_eq!(inventory["serviceId"], "KakuneCore");
    let data = PathBuf::from(inventory["dataDir"].as_str().unwrap());
    assert_eq!(data.parent().unwrap().canonicalize().unwrap(), root);
    let token = inventory["token"].as_str().unwrap();
    let listen = inventory["listen"].as_str().unwrap();
    let preserve = Arc::new(AtomicBool::new(true));
    let fixture = NativeFixture {
        root: root.clone(),
        id: id.to_owned(),
        preserve: preserve.clone(),
    };
    let mut registration = ServiceRegistrationGuard {
        root: data.clone(),
        fixture_root: root.clone(),
        retention_sentinel: data.join("retained-by-service-uninstall.txt"),
        preserve,
        armed: false,
    };
    let query = run_cli(
        &root,
        &["--internal-service-query", "KakuneCore"],
        Duration::from_secs(12),
    )
    .unwrap();
    assert!(query.status.success(), "{}", cli_text(&query));
    let observed: kakune_core::lifecycle::service::NativeServiceObservation =
        serde_json::from_slice(&query.stdout).unwrap();
    let definition = observed.definition.as_ref().unwrap();
    assert_eq!(
        definition.data_dir.canonicalize().unwrap(),
        data.canonicalize().unwrap()
    );
    assert_eq!(
        definition.executable.canonicalize().unwrap(),
        Path::new(env!("CARGO_BIN_EXE_kakune"))
            .canonicalize()
            .unwrap()
    );
    assert_eq!(definition.account, "LocalSystem");
    registration.armed = true;
    let status = run_cli(&root, &["service", "status"], Duration::from_secs(12)).unwrap();
    assert!(status.status.success(), "{}", cli_text(&status));
    assert!(
        cli_text(&status).contains("Running") && cli_text(&status).contains("coreState\tready"),
        "{}",
        cli_text(&status)
    );
    let record: RuntimeOwnershipRecord =
        serde_json::from_slice(&fs::read(data.join(".kakune/ownership.json")).unwrap()).unwrap();
    assert_eq!(record.instance.mode, CoreMode::SystemService);
    assert_eq!(
        record.instance.owner,
        OwnerIdentity::WindowsSid("S-1-5-18".into())
    );
    verify_persistence(&data, &inventory["beforeBoot"], token);
    let execution_id = accepted_workflow(listen, token, &data).await;
    println!(
        "native-windows-boot\tLocalSystem service ready; authenticated workflow succeeded; pre-boot state retained"
    );

    let repeated = run_cli(&root, &["service", "start"], Duration::from_secs(55)).unwrap();
    assert!(repeated.status.success(), "{}", cli_text(&repeated));
    let same: RuntimeOwnershipRecord =
        serde_json::from_slice(&fs::read(data.join(".kakune/ownership.json")).unwrap()).unwrap();
    assert_eq!(same.instance.instance_id, record.instance.instance_id);
    let other_data = root.join("conflicting-data");
    fs::create_dir(&other_data).unwrap();
    let conflict = run_cli(
        &root,
        &[
            "service",
            "install",
            "--data-dir",
            other_data.to_str().unwrap(),
        ],
        Duration::from_secs(55),
    )
    .unwrap();
    assert!(
        !conflict.status.success(),
        "conflicting install must not replace registration"
    );
    let wrong_mode = run_cli(
        &root,
        &["daemon", "--data-dir", data.to_str().unwrap(), "stop"],
        Duration::from_secs(12),
    )
    .unwrap();
    assert!(
        !wrong_mode.status.success(),
        "daemon stop must not control system service"
    );
    assert!(verify_runtime_lease_released(&data).is_err());
    println!(
        "native-windows-service-failures\tconflicting install and daemon-mode stop rejected; service remains alive"
    );

    let ownership_path = data.join(".kakune/ownership.json");
    let restore = RecordRestore {
        path: ownership_path.clone(),
        bytes: fs::read(&ownership_path).unwrap(),
        preserve: fixture.preserve.clone(),
    };
    let mut tampered = record.clone();
    tampered
        .instance
        .process_identity
        .start_identity
        .push_str("-mismatch");
    let changed = serde_json::to_vec(&tampered).unwrap();
    fs::write(&ownership_path, &changed).unwrap();
    for action in ["stop", "uninstall"] {
        let rejected = run_cli(&root, &["service", action], Duration::from_secs(65)).unwrap();
        assert!(
            !rejected.status.success(),
            "{action} accepted mismatched process identity"
        );
        assert_eq!(fs::read(&ownership_path).unwrap(), changed);
        assert!(verify_runtime_lease_released(&data).is_err());
    }
    drop(restore);
    accepted_workflow(listen, token, &data).await;
    println!(
        "native-windows-service-identity\tstop and uninstall rejected mismatched identity; registration/lease preserved; original record restored and API verified"
    );

    registration.cleanup().unwrap();
    verify_persistence(&data, &inventory["beforeBoot"], token);
    registration.armed = true;
    let reinstall = run_cli(
        &root,
        &["service", "install", "--data-dir", data.to_str().unwrap()],
        Duration::from_secs(55),
    )
    .unwrap();
    assert!(reinstall.status.success(), "{}", cli_text(&reinstall));
    let start = run_cli(&root, &["service", "start"], Duration::from_secs(55)).unwrap();
    assert!(start.status.success(), "{}", cli_text(&start));
    verify_persistence(&data, &inventory["beforeBoot"], token);
    assert_eq!(
        Store::open(data.clone())
            .unwrap()
            .get_execution(&execution_id)
            .unwrap()
            .unwrap()
            .status,
        "succeeded"
    );
    accepted_workflow(listen, token, &data).await;
    registration.cleanup().unwrap();
    verify_persistence(&data, &inventory["beforeBoot"], token);
    println!(
        "native-windows-service-retention\tpost-boot uninstall/reinstall/API passed; final registration absent and lease released; marked data retained"
    );
    drop(fixture);
}

struct RecordRestore {
    path: PathBuf,
    bytes: Vec<u8>,
    preserve: Arc<AtomicBool>,
}

impl Drop for RecordRestore {
    fn drop(&mut self) {
        if let Err(error) = fs::write(&self.path, &self.bytes) {
            self.preserve.store(true, Ordering::SeqCst);
            eprintln!(
                "Cannot restore test-owned identity record: {error}; preserve fixture for recovery"
            );
        }
    }
}

#[tokio::test]
#[ignore = "requires explicit opt-in and a stopped, marker-owned LocalSystem fixture"]
async fn windows_native_service_account_startup_probe() {
    assert_eq!(
        std::env::var("KAKUNE_RUN_NATIVE_ACCEPTANCE").as_deref(),
        Ok("1")
    );
    let data = PathBuf::from(std::env::var_os("KAKUNE_NATIVE_SERVICE_PROBE_DATA").unwrap());
    let root = data.parent().unwrap().canonicalize().unwrap();
    assert_eq!(
        root.parent(),
        Some(
            approved_native_root()
                .unwrap()
                .canonicalize()
                .unwrap()
                .as_path()
        )
    );
    let id = fs::read_to_string(root.join(FIXTURE_MARKER)).unwrap();
    assert_eq!(
        root.file_name().unwrap().to_string_lossy(),
        format!("kakune-native-{id}")
    );
    let mut controller =
        kakune_core::lifecycle::CoreInstanceController::acquire_system_service(&data)
            .expect("LocalSystem runtime ownership acquisition");
    assert_eq!(
        controller.owner(),
        &OwnerIdentity::WindowsSid("S-1-5-18".into())
    );
    println!("native-service-probe\tSystem ownership acquired");
    let loaded =
        kakune_core::CoreConfig::load_or_create(&data, None).expect("service configuration");
    loaded
        .config
        .validate()
        .expect("service configuration validation");
    Store::open(data.clone()).expect("service storage access");
    println!("native-service-probe\tconfiguration and storage accessible");
    let control = controller
        .prepare_control()
        .expect("service private-control preparation");
    drop(control);
    println!("native-service-probe\tprivate control prepared; no listener tasks dispatched");
}
