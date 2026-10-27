//! Test fixture for verifying descendant cleanup without platform-specific shells.
use std::{
    io::Write,
    process::{Command, Stdio},
    time::Duration,
};

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    if args.get(1).map(String::as_str) == Some("lifecycle-launch-stall") {
        println!("CLAIM");
        std::io::stdout().flush().unwrap();
        std::thread::sleep(Duration::from_secs(10));
        return;
    }
    if args.get(1).map(String::as_str) == Some("lifecycle-sentinel") {
        use std::io::Read;
        // Only the retained test parent's pipe can release this sentinel.
        let _ = std::io::stdin().read(&mut [0_u8; 1]);
        return;
    }
    if args.get(1).map(String::as_str) == Some("lifecycle-slow-plugin") {
        slow_lifecycle_plugin(std::path::Path::new(&args[2]));
        return;
    }
    if args.get(1).map(String::as_str) == Some("service-helper-fixture") {
        use kakune_core::lifecycle::{
            InstallationObservation, NativeManagerState, SystemMonotonicClock,
            service::{
                NativeMutation, NativeServiceObservation, ServiceManagerKind, ServiceStateRoot,
                helper,
            },
        };
        let root = std::path::PathBuf::from(&args[2]);
        let manager = match args[3].as_str() {
            "scm" => ServiceManagerKind::WindowsScm,
            "systemd" => ServiceManagerKind::LinuxSystemd,
            "launchd" => ServiceManagerKind::MacosLaunchd,
            _ => panic!("invalid fixture manager"),
        };
        let mode = &args[6];
        let result = helper::serve_with(
            ServiceStateRoot::isolated(root.clone()).unwrap(),
            manager,
            &args[4],
            args[5].parse().unwrap(),
            &mut std::io::stdin().lock(),
            &mut std::io::stdout().lock(),
            &SystemMonotonicClock::default(),
            &|step, definition, _| {
                std::fs::write(
                    root.join("authorized-step"),
                    serde_json::to_vec(&step).unwrap(),
                )
                .unwrap();
                if mode == "crash" || mode == &format!("crash:{step:?}") {
                    std::process::exit(77);
                }
                if mode == "hang" {
                    std::thread::sleep(Duration::from_secs(5));
                }
                if mode == "output-cap" {
                    let block = vec![b'x'; 2 * 1024 * 1024];
                    std::io::stderr().write_all(&block).unwrap();
                }
                let path = root.join("native-observation.json");
                let mut observation: NativeServiceObservation = std::fs::read(&path)
                    .ok()
                    .and_then(|bytes| serde_json::from_slice(&bytes).ok())
                    .unwrap_or_else(NativeServiceObservation::absent);
                match step {
                    NativeMutation::Register | NativeMutation::Publish => {
                        observation.installed = InstallationObservation::Installed;
                        observation.manager_state = NativeManagerState::Stopped;
                        observation.definition = Some(definition.clone());
                        observation.account = Some(definition.account.clone());
                        observation.boot_enabled = step == NativeMutation::Register;
                    }
                    NativeMutation::Enable => observation.boot_enabled = true,
                    NativeMutation::Disable => observation.boot_enabled = false,
                    NativeMutation::Delete => observation = NativeServiceObservation::absent(),
                    _ => {}
                }
                std::fs::write(path, serde_json::to_vec(&observation).unwrap()).unwrap();
                Ok(())
            },
        );
        if let Err(error) = result {
            print!("{}", helper::error_frame(&error));
            std::process::exit(1);
        }
        return;
    }
    if std::env::args().nth(1).as_deref() == Some("descendant") {
        println!("descendant-ready");
        std::io::stdout().flush().unwrap();
        std::thread::sleep(Duration::from_secs(30));
    } else {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .arg("descendant")
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::null());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x08000000);
        }
        let mut descendant = command.spawn().unwrap();
        descendant.wait().unwrap();
    }
}

fn slow_lifecycle_plugin(root: &std::path::Path) {
    use serde_json::{Value, json};
    use std::io::BufRead;

    assert!(root.join(".kakune-native-acceptance-fixture").is_file());
    for line in std::io::stdin().lock().lines() {
        let request: Value = serde_json::from_str(&line.unwrap()).unwrap();
        let method = request["method"].as_str().unwrap();
        let gate = match method {
            "node/execute" => {
                let mut calls = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(root.join("plugin-calls.txt"))
                    .unwrap();
                writeln!(calls, "node/execute").unwrap();
                let temporary = root.join("plugin-entered.tmp");
                std::fs::write(&temporary, std::process::id().to_string()).unwrap();
                std::fs::rename(temporary, root.join("plugin-entered.json")).unwrap();
                "release-node"
            }
            "shutdown" => "release-shutdown",
            _ => panic!("unexpected lifecycle fixture method"),
        };
        let deadline = std::time::Instant::now() + Duration::from_secs(25);
        while !root.join(gate).exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "fixture gate expired: {gate}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        let result = if method == "node/execute" {
            json!({"route":"success", "outputs":{}, "message":"native fixture released"})
        } else {
            json!({"ok":true})
        };
        println!(
            "{}",
            json!({"jsonrpc":"2.0", "id":request["id"], "result":result})
        );
        std::io::stdout().flush().unwrap();
        if method == "shutdown" {
            return;
        }
    }
}
