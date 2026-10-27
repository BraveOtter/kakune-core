//! Private, journal-authorized single-mutation helpers. The managing CLI keeps
//! admin.lock; each authorized helper owns helper.lock until all native work ends.
//! Every later administrator acquires/checks both, in that order. Thus parent
//! death cannot release serialization while an authorized helper is still alive.

use std::{
    io::{self, BufRead, BufReader, Read, Write},
    process::{Command, Stdio},
    sync::{Mutex, OnceLock, mpsc},
    thread::{self, JoinHandle},
    time::Duration,
};

use super::*;
use crate::lifecycle::process::{ProcessEvidence, ProcessEvidenceProvider, SystemProcessEvidence};

const READY: &str = "KAKUNE-HELPER-READY";
const DONE: &str = "KAKUNE-HELPER-DONE";

type Child = Box<dyn process_wrap::std::ChildWrapper>;
struct Retained {
    child: Child,
    output: JoinHandle<io::Result<()>>,
    diagnostics: JoinHandle<io::Result<super::super::process::BoundedCapture>>,
}
static RETAINED: OnceLock<Mutex<Vec<Retained>>> = OnceLock::new();

/// Injectable launch/cleanup boundary. Production uses the current executable;
/// isolated tests can run a fixture without contacting a native manager.
pub trait HelperLauncher: Send + Sync {
    fn command(
        &self,
        journal: &ServiceAttemptJournal,
        root: &Path,
    ) -> Result<Command, LifecycleError>;
    fn terminate(&self, child: &mut dyn process_wrap::std::ChildWrapper) -> io::Result<()> {
        child.kill()
    }
}

pub struct SystemHelperLauncher;
impl HelperLauncher for SystemHelperLauncher {
    fn command(
        &self,
        journal: &ServiceAttemptJournal,
        _root: &Path,
    ) -> Result<Command, LifecycleError> {
        let mut command = Command::new(std::env::current_exe()?);
        command.args([
            "--internal-service-helper",
            journal.manager.key(),
            &journal.native_service_id,
            &journal.attempt_id.to_string(),
        ]);
        Ok(command)
    }
}

fn helper_error(detail: impl Into<String>) -> LifecycleError {
    LifecycleError::UnresolvedServiceAttempt(detail.into())
}

/// Non-secret typed protocol category; detailed diagnostics stay on capped stderr.
pub fn error_frame(error: &LifecycleError) -> &'static str {
    match error {
        LifecycleError::AccessDenied(_) => "KAKUNE-HELPER-ERROR access_denied\n",
        LifecycleError::ManagerUnavailable(_) => "KAKUNE-HELPER-ERROR manager_unavailable\n",
        LifecycleError::ConflictingServiceDefinition(_) => "KAKUNE-HELPER-ERROR conflict\n",
        LifecycleError::UnverifiedProcess
        | LifecycleError::UnsafeOwnership(_)
        | LifecycleError::InsecureProtection(_) => "KAKUNE-HELPER-ERROR identity\n",
        _ => "KAKUNE-HELPER-ERROR unconfirmed\n",
    }
}

pub(super) fn step_label(index: usize, step: NativeMutation) -> String {
    format!(
        "{index}:{}",
        serde_json::to_string(&step).unwrap_or_default()
    )
}

fn step_for(journal: &ServiceAttemptJournal) -> Result<(usize, NativeMutation), LifecycleError> {
    NativeMutation::steps(journal.manager, journal.action)
        .iter()
        .copied()
        .enumerate()
        .find(|(index, step)| journal.step == step_label(*index, *step))
        .ok_or_else(|| helper_error("journal does not identify a supported single native mutation"))
}

pub(super) fn is_native_step(journal: &ServiceAttemptJournal) -> bool {
    step_for(journal).is_ok()
}

pub(super) fn is_final_step(journal: &ServiceAttemptJournal) -> bool {
    step_for(journal).is_ok_and(|(index, _)| {
        index + 1 == NativeMutation::steps(journal.manager, journal.action).len()
    })
}

pub(super) fn valid_next_step(
    previous: &ServiceAttemptJournal,
    next: &ServiceAttemptJournal,
) -> bool {
    previous.status == AttemptStatus::ObservedComplete
        && next.status == AttemptStatus::Prepared
        && next.helper_pid.is_none()
        && step_for(previous)
            .ok()
            .zip(step_for(next).ok())
            .is_some_and(|((before, _), (after, _))| before + 1 == after)
}

pub(super) fn run_mutations(
    controller: &ServiceController,
    paths: &ServicePaths,
    action: ServiceAction,
    definition: &ServiceDefinition,
    prior: NativeServiceObservation,
    deadline: Deadline,
) -> Result<(), LifecycleError> {
    let mut journal = ServiceAttemptJournal {
        schema_version: SERVICE_STATE_SCHEMA_VERSION,
        attempt_id: Uuid::new_v4(),
        manager: definition.manager,
        native_service_id: definition.service_id.clone(),
        action,
        step: String::new(),
        expected_definition: definition.clone(),
        prior_state: prior,
        helper_pid: None,
        helper_owner: None,
        status: AttemptStatus::Prepared,
        completion_observation: None,
        residual_resources: Vec::new(),
        recovery_guidance: vec![format!(
            "Inspect native manager effects and the retained attempt at {} before retrying; helper timeout does not cancel manager work.",
            paths.attempt.display()
        )],
    };
    let steps = NativeMutation::steps(definition.manager, action);
    for (index, step) in steps.iter().copied().enumerate() {
        ensure_remaining(
            deadline,
            controller.clock.as_ref(),
            "service mutation phase",
        )?;
        journal.step = step_label(index, step);
        journal.status = AttemptStatus::Prepared;
        journal.helper_pid = None;
        journal.helper_owner = None;
        journal.completion_observation = None;
        write_attempt_file(&paths.attempt, &journal, controller.state_root.isolated)?;
        let helper_deadline = deadline.cap(controller.clock.as_ref(), HELPER_BUDGET);
        let result = run_one(controller, paths, &mut journal, helper_deadline);
        if let Err(error) = result {
            // Prepared means authorization was never durably issued. Keep that
            // distinction even if a failed launch/reap left an owned residual.
            if journal.status == AttemptStatus::Authorized {
                journal.status = AttemptStatus::OutcomeUnknown;
            }
            journal.residual_resources.push(format!(
                "native step {} is unconfirmed; no compensating mutation was attempted",
                journal.step
            ));
            write_attempt_file(&paths.attempt, &journal, controller.state_root.isolated)?;
            return Err(error);
        }
        if index + 1 < steps.len() {
            let observation =
                controller.observe(definition.manager, &definition.service_id, deadline)?;
            journal.status = AttemptStatus::ObservedComplete;
            journal.completion_observation = Some(observation);
            write_attempt_file(&paths.attempt, &journal, controller.state_root.isolated)?;
        }
    }
    Ok(())
}

fn run_one(
    controller: &ServiceController,
    paths: &ServicePaths,
    journal: &mut ServiceAttemptJournal,
    deadline: Deadline,
) -> Result<(), LifecycleError> {
    reap_finished();
    let clock = controller.clock.as_ref();
    if retained_helper_count() >= 16 {
        return Err(helper_error(
            "sixteen owned helper resources remain unresolved; reconcile them before launching another helper",
        ));
    }
    let work = super::native::helper_work_deadline(deadline, clock);
    ensure_remaining(work, clock, "service helper launch")?;
    // The installed executable need not be this CLI's build. Helpers always use
    // the current controller binary; the installed path is only definition data.
    let mut command = controller
        .helper_launcher
        .command(journal, controller.state_root.path())?;
    let expected_executable = PathBuf::from(command.get_program()).canonicalize()?;
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = crate::process_supervisor::spawn_sync(command)?;
    let pid = child.id();
    let mut input = child
        .stdin()
        .take()
        .ok_or_else(|| helper_error("helper stdin unavailable"))?;
    let stdout = child
        .stdout()
        .take()
        .ok_or_else(|| helper_error("helper stdout unavailable"))?;
    let stderr = child
        .stderr()
        .take()
        .ok_or_else(|| helper_error("helper stderr unavailable"))?;
    let (sender, messages) = mpsc::sync_channel(4);
    let output = thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        for _ in 0..3 {
            let mut frame = Vec::new();
            let size = reader.by_ref().take(257).read_until(b'\n', &mut frame)?;
            if size == 0 {
                return Ok(());
            }
            if size > 256 || frame.last() != Some(&b'\n') {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "oversized helper protocol frame",
                ));
            }
            if sender.send(frame).is_err() {
                return Ok(());
            }
        }
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected helper protocol output",
        ))
    });
    let diagnostics = thread::spawn(move || {
        super::super::process::capture_reader_with_limit(
            stderr,
            super::super::MAX_HELPER_DIAGNOSTIC_BYTES,
        )
    });
    let result = (|| {
        let evidence = SystemProcessEvidence::default()
            .observe(pid, work, clock)?
            .ok_or(LifecycleError::UnverifiedProcess)?;
        let parent = SystemProcessEvidence::default()
            .observe(std::process::id(), work, clock)?
            .ok_or(LifecycleError::UnverifiedProcess)?;
        if evidence.owner != parent.owner
            || evidence.identity.executable.canonicalize()? != expected_executable
        {
            return Err(LifecycleError::UnverifiedProcess);
        }
        journal.helper_pid = Some(pid);
        journal.helper_owner = Some(serde_json::to_string(&evidence)?);
        write_attempt_file(&paths.attempt, journal, controller.state_root.isolated)?;
        // The helper waits for this bootstrap before reading its journal. It
        // then retains helper.lock and reports READY without any mutation.
        input.write_all(b"PREPARE\n")?;
        input.flush()?;
        expect_frame(&messages, READY, work, clock)?;
        journal.status = AttemptStatus::Authorized;
        write_attempt_file(&paths.attempt, journal, controller.state_root.isolated)?;
        ensure_remaining(work, clock, "service helper authorization")?;
        writeln!(input, "AUTHORIZE {}", journal.attempt_id)?;
        input.flush()?;
        drop(input);
        expect_frame(&messages, DONE, work, clock)?;
        loop {
            if let Some(status) = child.try_wait()? {
                if !status.success() {
                    return Err(helper_error(
                        "authorized helper failed; inspect retained attempt",
                    ));
                }
                break;
            }
            let remaining = ensure_remaining(work, clock, "service helper exit")?;
            thread::sleep(remaining.min(Duration::from_millis(10)));
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = controller.helper_launcher.terminate(child.as_mut());
    }
    while !deadline.is_expired(clock) {
        if matches!(child.try_wait(), Ok(Some(_)))
            && output.is_finished()
            && diagnostics.is_finished()
        {
            output
                .join()
                .map_err(|_| helper_error("helper protocol reader panicked"))??;
            diagnostics
                .join()
                .map_err(|_| helper_error("helper diagnostic reader panicked"))??;
            journal.helper_pid = None;
            if result.is_err() && journal.status == AttemptStatus::Authorized {
                journal.status = AttemptStatus::OutcomeUnknown;
            }
            // The retained handle, not discovered PID evidence, confirmed exit.
            write_attempt_file(&paths.attempt, journal, controller.state_root.isolated)?;
            return result;
        }
        thread::sleep(deadline.remaining(clock).min(Duration::from_millis(10)));
    }
    let retained = Retained {
        child,
        output,
        diagnostics,
    };
    RETAINED
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(retained);
    Err(LifecycleError::HelperUnreaped { pid })
}

fn expect_frame(
    messages: &mpsc::Receiver<Vec<u8>>,
    expected: &str,
    deadline: Deadline,
    clock: &dyn MonotonicClock,
) -> Result<(), LifecycleError> {
    let frame = loop {
        let remaining = ensure_remaining(deadline, clock, "service helper IPC")?;
        match messages.recv_timeout(remaining.min(Duration::from_millis(20))) {
            Ok(frame) => break frame,
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(helper_error(
                    "helper IPC closed before confirming the requested state",
                ));
            }
        }
    };
    match frame.as_slice() {
        b"KAKUNE-HELPER-ERROR access_denied\n" => return Err(LifecycleError::AccessDenied("native helper was denied access; inspect the protected service journal and installed account permissions".into())),
        b"KAKUNE-HELPER-ERROR manager_unavailable\n" => return Err(LifecycleError::ManagerUnavailable("native helper could not access the service manager; inspect its availability and retained journal".into())),
        b"KAKUNE-HELPER-ERROR conflict\n" => return Err(LifecycleError::ConflictingServiceDefinition("native definition changed before the authorized mutation".into())),
        b"KAKUNE-HELPER-ERROR identity\n" => return Err(LifecycleError::UnsafeOwnership("native helper could not verify protected ownership".into())),
        _ => {}
    }
    if frame != format!("{expected}\n").as_bytes() {
        return Err(helper_error("unexpected service helper response"));
    }
    ensure_remaining(deadline, clock, "service helper IPC").map(|_| ())
}

fn reap_finished() {
    let Some(registry) = RETAINED.get() else {
        return;
    };
    let mut retained = registry
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut pending = Vec::new();
    for mut helper in retained.drain(..) {
        if matches!(helper.child.try_wait(), Ok(Some(_)))
            && helper.output.is_finished()
            && helper.diagnostics.is_finished()
        {
            let _ = helper.output.join();
            let _ = helper.diagnostics.join();
        } else {
            pending.push(helper);
        }
    }
    *retained = pending;
}

/// Observes/reaps only already-exited retained helpers; never signals a PID.
pub fn retained_helper_count() -> usize {
    reap_finished();
    RETAINED.get().map_or(0, |registry| {
        registry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    })
}

/// Executes the private helper protocol over caller-owned streams. No console
/// access, argument parsing, credentials or runtime initialization occurs here.
pub fn serve(
    manager: ServiceManagerKind,
    service_id: &str,
    attempt: Uuid,
    input: impl BufRead + Send + 'static,
    output: &mut dyn Write,
) -> Result<(), LifecycleError> {
    // The helper enforces its own IPC bound even if its parent remains alive
    // but stops making progress. The reader only reads inherited pipe bytes;
    // it never owns mutation authority. The helper process owns its lifetime.
    let expires = std::time::Instant::now() + HELPER_BUDGET;
    let (sender, receiver) = mpsc::sync_channel(2);
    let _reader = thread::spawn(move || {
        let mut input = input;
        for _ in 0..2 {
            let mut bytes = Vec::new();
            let result = input
                .by_ref()
                .take(257)
                .read_until(b'\n', &mut bytes)
                .map(|_| bytes);
            let ended = !result.as_ref().is_ok_and(|bytes| !bytes.is_empty());
            if sender.send(result).is_err() || ended {
                break;
            }
        }
    });
    let mut input = TimedInput {
        receiver,
        buffer: io::Cursor::new(Vec::new()),
        expires,
    };
    let root = ServiceStateRoot::system()?;
    let clock = super::super::SystemMonotonicClock::default();
    serve_with(
        root,
        manager,
        service_id,
        attempt,
        &mut input,
        output,
        &clock,
        &|step, definition, deadline| execute_step(step, definition, deadline, &clock),
    )
}

struct TimedInput {
    receiver: mpsc::Receiver<io::Result<Vec<u8>>>,
    buffer: io::Cursor<Vec<u8>>,
    expires: std::time::Instant,
}

impl Read for TimedInput {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        let available = self.fill_buf()?;
        let length = available.len().min(output.len());
        output[..length].copy_from_slice(&available[..length]);
        self.consume(length);
        Ok(length)
    }
}

impl BufRead for TimedInput {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if self.buffer.position() as usize >= self.buffer.get_ref().len() {
            let remaining = self
                .expires
                .saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "helper authorization deadline expired",
                ));
            }
            let bytes = self.receiver.recv_timeout(remaining).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "helper parent channel closed or exceeded its deadline",
                )
            })??;
            self.buffer = io::Cursor::new(bytes);
        }
        self.buffer.fill_buf()
    }
    fn consume(&mut self, amount: usize) {
        self.buffer.consume(amount);
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "injected helper protocol boundary"
)]
#[doc(hidden)]
pub fn serve_with(
    root: ServiceStateRoot,
    manager: ServiceManagerKind,
    service_id: &str,
    attempt: Uuid,
    input: &mut dyn BufRead,
    output: &mut dyn Write,
    clock: &dyn MonotonicClock,
    mutate: &dyn Fn(NativeMutation, &ServiceDefinition, Deadline) -> Result<(), LifecycleError>,
) -> Result<(), LifecycleError> {
    let deadline = Deadline::after(clock, HELPER_BUDGET);
    let controller = ServiceController::with_components(
        root,
        super::system_manager()?,
        Arc::new(super::super::SystemMonotonicClock::default()),
        Arc::new(super::super::ThreadWaiter),
    );
    let paths = controller.paths_for(manager, service_id)?;
    check_existing_state_path(&paths, controller.state_root.isolated)?;
    read_frame(input, b"PREPARE\n")?;
    let prepared = read_attempt_for_service(&paths, controller.state_root.isolated)?
        .ok_or_else(|| helper_error("helper attempt journal is absent"))?;
    validate_helper(&prepared, attempt, AttemptStatus::Prepared, deadline, clock)?;
    let _helper_lock = acquire_helper_lease(
        &paths,
        controller.state_root.isolated,
        deadline,
        clock,
        &super::super::ThreadWaiter,
    )?;
    let (_, step) = step_for(&prepared)?;
    writeln!(output, "{READY}")?;
    output.flush()?;
    read_frame(input, format!("AUTHORIZE {attempt}\n").as_bytes())?;
    let authorized = read_attempt_for_service(&paths, controller.state_root.isolated)?
        .ok_or_else(|| helper_error("authorized helper journal disappeared"))?;
    validate_helper(
        &authorized,
        attempt,
        AttemptStatus::Authorized,
        deadline,
        clock,
    )?;
    if authorized.step != prepared.step
        || authorized.expected_definition != prepared.expected_definition
    {
        return Err(helper_error(
            "helper authorization changed its mutation target",
        ));
    }
    ensure_remaining(deadline, clock, "authorized helper mutation")?;
    mutate(step, &authorized.expected_definition, deadline)?;
    writeln!(output, "{DONE}")?;
    output.flush()?;
    Ok(())
}

fn read_frame(input: &mut dyn BufRead, expected: &[u8]) -> Result<(), LifecycleError> {
    let mut frame = Vec::new();
    input.take(257).read_until(b'\n', &mut frame)?;
    if frame.len() > 256 {
        return Err(LifecycleError::MessageTooLarge { limit: 256 });
    }
    if frame != expected {
        return Err(helper_error(
            "parent channel closed or authorization is invalid; no mutation authorized",
        ));
    }
    Ok(())
}

fn validate_helper(
    journal: &ServiceAttemptJournal,
    attempt: Uuid,
    status: AttemptStatus,
    deadline: Deadline,
    clock: &dyn MonotonicClock,
) -> Result<(), LifecycleError> {
    if journal.attempt_id != attempt
        || journal.status != status
        || journal.helper_pid != Some(std::process::id())
    {
        return Err(helper_error(
            "helper does not own this journal authorization",
        ));
    }
    let expected: ProcessEvidence = serde_json::from_str(
        journal
            .helper_owner
            .as_deref()
            .ok_or(LifecycleError::UnverifiedProcess)?,
    )?;
    let actual = SystemProcessEvidence::default()
        .observe(std::process::id(), deadline, clock)?
        .ok_or(LifecycleError::UnverifiedProcess)?;
    if expected != actual {
        return Err(LifecycleError::UnverifiedProcess);
    }
    Ok(())
}

fn execute_step(
    step: NativeMutation,
    definition: &ServiceDefinition,
    deadline: Deadline,
    clock: &dyn MonotonicClock,
) -> Result<(), LifecycleError> {
    // Revalidate immediately before native work. External administrators remain
    // outside Kakune serialization; never claim a name-only atomic guarantee.
    let observed =
        system_manager()?.observe(definition.manager, &definition.service_id, deadline, clock)?;
    let installing = matches!(step, NativeMutation::Register | NativeMutation::Publish);
    if installing {
        if observed.installed != InstallationObservation::NotInstalled || observed.pending_operation
        {
            return Err(LifecycleError::ConflictingServiceDefinition(
                "native registration appeared before publication".into(),
            ));
        }
    } else if step != NativeMutation::Reload
        || observed.installed != InstallationObservation::NotInstalled
    {
        ensure_expected_definition(definition, &observed)?;
        if observed.pending_operation || observed.core_unknown.is_some() {
            return Err(LifecycleError::ServiceStateUnconfirmed);
        }
        ensure_service_mode(&observed)?;
        if step == NativeMutation::Start
            && observed.manager_state == NativeManagerState::Running
            && core_is_ready(observed.core.as_ref())
        {
            return Ok(());
        }
        if matches!(step, NativeMutation::Disable | NativeMutation::Delete)
            && (observed.manager_state != NativeManagerState::Stopped
                || !core_is_absent_or_stopped(
                    observed.core.as_ref(),
                    observed.core_unknown.as_deref(),
                ))
        {
            return Err(LifecycleError::ServiceStateUnconfirmed);
        }
    }
    #[cfg(windows)]
    {
        super::windows::execute_step(step, definition, deadline, clock)
    }
    #[cfg(target_os = "linux")]
    {
        super::linux::execute_step(step, definition, deadline, clock)
    }
    #[cfg(target_os = "macos")]
    {
        super::macos::execute_step(step, definition, deadline, clock)
    }
    #[cfg(not(any(windows, target_os = "linux", target_os = "macos")))]
    {
        Err(LifecycleError::ManagerUnavailable(
            "native helpers are unsupported".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authorization_pipe_deadline_does_not_depend_on_parent_cleanup_or_renew_per_frame() {
        let (_sender, receiver) = mpsc::sync_channel(2);
        let expires = std::time::Instant::now() + Duration::from_millis(10);
        let mut input = TimedInput {
            receiver,
            buffer: io::Cursor::new(Vec::new()),
            expires,
        };
        assert_eq!(
            input.fill_buf().unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert_eq!(input.expires, expires);
        assert_eq!(
            input.fill_buf().unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
    }
}
