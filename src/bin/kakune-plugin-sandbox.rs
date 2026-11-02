//! Experimental Linux filesystem write-denial launcher; not a complete sandbox.
//!
//! This launcher is not yet wired into plugin execution. It does not restrict
//! network access or protect files against other processes of the same user.

#[cfg(target_os = "linux")]
fn main() {
    use landlock::{ABI, AccessFs, CompatLevel, Compatible, Ruleset, RulesetAttr, RulesetStatus};
    use std::os::unix::process::CommandExt;

    let mut args = std::env::args_os().skip(1);
    let Some(first) = args.next() else {
        eprintln!("sandbox requires an absolute executable path");
        std::process::exit(2);
    };
    let (host_protocol, program) = if first == "--host-protocol" {
        let Some(program) = args.next() else {
            eprintln!("sandbox requires an absolute executable path");
            std::process::exit(2);
        };
        (true, program)
    } else {
        (false, first)
    };
    if !std::path::Path::new(&program).is_absolute() {
        eprintln!("sandbox requires an absolute executable path");
        std::process::exit(2);
    }
    // ABI v3 includes truncate protection; do not silently downgrade.
    let result = Ruleset::default()
        .set_compatibility(CompatLevel::HardRequirement)
        .handle_access(AccessFs::from_write(ABI::V3))
        .and_then(|ruleset| ruleset.create())
        .and_then(|ruleset| ruleset.restrict_self());
    if !matches!(result, Ok(status) if status.ruleset == RulesetStatus::FullyEnforced) {
        eprintln!("required filesystem sandbox is unavailable");
        std::process::exit(1);
    }
    if host_protocol {
        use std::io::Write;
        if std::io::stdout()
            .write_all(b"KAKUNE_SANDBOX_READY_V1\n")
            .is_err()
        {
            std::process::exit(1);
        }
    }
    let error = std::process::Command::new(program).args(args).exec();
    eprintln!("sandboxed process could not be started: {error}");
    std::process::exit(1);
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("filesystem sandbox is unavailable on this platform");
    std::process::exit(1);
}
