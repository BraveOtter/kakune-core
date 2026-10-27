//! Linux identity uses kernel boot identity and unconverted process start ticks.

use std::io::Read;

use uuid::Uuid;

use super::LifecycleError;

const MAX_PROC_BYTES: usize = 64 * 1024;

fn read_bounded(reader: impl Read) -> Result<Vec<u8>, LifecycleError> {
    let mut bytes = Vec::new();
    reader
        .take((MAX_PROC_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| LifecycleError::UnverifiedProcess)?;
    if bytes.len() > MAX_PROC_BYTES {
        return Err(LifecycleError::UnverifiedProcess);
    }
    Ok(bytes)
}

fn parse_start_identity(pid: u32, boot_id: &[u8], stat: &[u8]) -> Option<String> {
    let boot_id = Uuid::parse_str(std::str::from_utf8(boot_id).ok()?.trim()).ok()?;
    if boot_id.is_nil() || pid == 0 {
        return None;
    }
    // comm may contain spaces, closing parentheses, newlines, or non-UTF-8 bytes.
    // Only the PID prefix and the fields after the final ')' are ASCII data.
    let open = stat.iter().position(|byte| *byte == b'(')?;
    let close = stat.iter().rposition(|byte| *byte == b')')?;
    if close <= open || stat.get(close + 1) != Some(&b' ') {
        return None;
    }
    let observed_pid = std::str::from_utf8(&stat[..open])
        .ok()?
        .trim()
        .parse::<u32>()
        .ok()?;
    if observed_pid != pid {
        return None;
    }
    let ticks = std::str::from_utf8(&stat[close + 1..])
        .ok()?
        .split_ascii_whitespace()
        .nth(19)?
        .parse::<u64>()
        .ok()?;
    if ticks == 0 {
        return None;
    }
    // Never add btime or convert ticks to wall time: NTP/VM clock corrections
    // can change that derived timestamp while this process remains alive.
    Some(format!("linux:{boot_id}:{ticks}"))
}

#[cfg(target_os = "linux")]
pub(super) fn observe_start_identity(pid: u32) -> Result<Option<String>, LifecycleError> {
    let stat = match std::fs::File::open(format!("/proc/{pid}/stat")) {
        Ok(file) => read_bounded(file)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(LifecycleError::UnverifiedProcess),
    };
    let boot_id = read_bounded(
        std::fs::File::open("/proc/sys/kernel/random/boot_id")
            .map_err(|_| LifecycleError::UnverifiedProcess)?,
    )?;
    parse_start_identity(pid, &boot_id, &stat)
        .map(Some)
        .ok_or(LifecycleError::UnverifiedProcess)
}

#[cfg(test)]
mod tests {
    use std::io::{self, Cursor};

    use super::*;

    const BOOT: &[u8] = b"083faa28-08e4-4cef-baef-3aef3b0d22e9\n";

    fn process_stat(pid: u32, comm: &[u8], ticks: &str) -> Vec<u8> {
        let mut stat = format!("{pid} (").into_bytes();
        stat.extend_from_slice(comm);
        stat.extend_from_slice(b") S");
        // Fields 4 through 21 precede the raw starttime field (22).
        for _ in 4..22 {
            stat.extend_from_slice(b" 0");
        }
        stat.extend_from_slice(format!(" {ticks} 0 0\n").as_bytes());
        stat
    }

    #[test]
    fn start_identity_preserves_raw_ticks_without_wall_clock_conversion() {
        assert_eq!(
            parse_start_identity(1753, BOOT, &process_stat(1753, b"kakune", "1218")),
            Some("linux:083faa28-08e4-4cef-baef-3aef3b0d22e9:1218".to_owned())
        );
    }

    #[test]
    fn process_names_with_delimiters_and_non_utf8_do_not_shift_start_field() {
        assert_eq!(
            parse_start_identity(42, BOOT, &process_stat(42, b"name ) (\n\xff", "1218")),
            parse_start_identity(42, BOOT, &process_stat(42, b"kakune", "1218"))
        );
    }

    #[test]
    fn reused_pid_with_different_start_ticks_has_different_identity() {
        assert_ne!(
            parse_start_identity(42, BOOT, &process_stat(42, b"kakune", "1218")),
            parse_start_identity(42, BOOT, &process_stat(42, b"kakune", "1219"))
        );
    }

    #[test]
    fn another_boot_with_same_pid_and_ticks_has_different_identity() {
        let stat = process_stat(42, b"kakune", "1218");
        assert_ne!(
            parse_start_identity(42, BOOT, &stat),
            parse_start_identity(42, b"183faa28-08e4-4cef-baef-3aef3b0d22e9", &stat)
        );
    }

    #[test]
    fn mismatched_pid_is_not_accepted() {
        assert!(parse_start_identity(43, BOOT, &process_stat(42, b"kakune", "1218")).is_none());
    }

    #[test]
    fn invalid_boot_identity_is_not_accepted() {
        for boot in [
            b"".as_slice(),
            b"not-a-uuid",
            b"00000000-0000-0000-0000-000000000000",
        ] {
            assert!(parse_start_identity(42, boot, &process_stat(42, b"kakune", "1218")).is_none());
        }
    }

    #[test]
    fn missing_or_invalid_start_ticks_are_not_accepted() {
        for ticks in ["0", "-1", "invalid", "18446744073709551616"] {
            assert!(parse_start_identity(42, BOOT, &process_stat(42, b"kakune", ticks)).is_none());
        }
        for stat in [
            b"42 (kakune) S".as_slice(),
            b"42 kakune S",
            b"42 (kakune)S 1",
        ] {
            assert!(parse_start_identity(42, BOOT, stat).is_none());
        }
    }

    #[test]
    fn proc_content_is_bounded_without_reading_an_unlimited_stream() {
        assert!(read_bounded(io::repeat(b'x')).is_err());
        assert_eq!(read_bounded(Cursor::new(b"bounded")).unwrap(), b"bounded");
    }

    #[test]
    fn proc_read_failure_is_unverified_not_a_fallback_timestamp() {
        struct Denied;
        impl Read for Denied {
            fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::from(io::ErrorKind::PermissionDenied))
            }
        }
        assert!(matches!(
            read_bounded(Denied),
            Err(LifecycleError::UnverifiedProcess)
        ));
    }
}
