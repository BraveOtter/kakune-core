//! Test fixture for verifying descendant cleanup without platform-specific shells.
use std::{
    io::Write,
    process::{Command, Stdio},
    time::Duration,
};

fn main() {
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
