//! Atomic lifecycle admission and drain accounting for accepted work.

use std::{
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

use super::{Deadline, LifecycleError, MonotonicClock};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrackedExecutionState {
    Preparation,
    Queued,
    Active,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AdmissionSnapshot {
    pub accepting: bool,
    pub preparation: usize,
    pub queued: usize,
    pub active: usize,
}

impl AdmissionSnapshot {
    pub fn outstanding(self) -> usize {
        self.preparation
            .saturating_add(self.queued)
            .saturating_add(self.active)
    }

    pub fn is_drained(self) -> bool {
        self.outstanding() == 0
    }
}

#[derive(Debug)]
struct State {
    snapshot: AdmissionSnapshot,
    sealed: bool,
}

#[derive(Debug)]
struct Inner {
    state: Mutex<State>,
    changed: Condvar,
}

/// Work admission and registration share one lock. Closing admission therefore
/// cannot race an accepted preparation/queued claim that has not been counted.
#[derive(Clone, Debug)]
pub struct AdmissionCoordinator {
    inner: Arc<Inner>,
}

impl Default for AdmissionCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl AdmissionCoordinator {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    snapshot: AdmissionSnapshot {
                        accepting: true,
                        ..AdmissionSnapshot::default()
                    },
                    sealed: false,
                }),
                changed: Condvar::new(),
            }),
        }
    }

    /// Creates an admission gate that starts closed but may be opened once
    /// startup has completed. Shutdown closure is permanent.
    pub fn closed() -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    snapshot: AdmissionSnapshot::default(),
                    sealed: false,
                }),
                changed: Condvar::new(),
            }),
        }
    }

    pub fn open_admission(&self) -> Result<(), LifecycleError> {
        let mut inner = self
            .inner
            .state
            .lock()
            .map_err(|_| poisoned_coordinator())?;
        if inner.sealed {
            return Err(LifecycleError::UnsafeOwnership(
                "execution admission cannot reopen after shutdown begins".to_owned(),
            ));
        }
        inner.snapshot.accepting = true;
        self.inner.changed.notify_all();
        Ok(())
    }

    pub fn admit(&self, state: TrackedExecutionState) -> Result<TaskPermit, LifecycleError> {
        let mut inner = self
            .inner
            .state
            .lock()
            .map_err(|_| poisoned_coordinator())?;
        if !inner.snapshot.accepting {
            return Err(LifecycleError::UnsafeOwnership(
                "execution admission is closed".to_owned(),
            ));
        }
        increment(&mut inner.snapshot, state);
        self.inner.changed.notify_all();
        Ok(TaskPermit {
            inner: Arc::clone(&self.inner),
            state,
            released: false,
        })
    }

    /// Closes admission atomically with all successful `admit` registrations.
    pub fn close_admission(&self) -> Result<AdmissionSnapshot, LifecycleError> {
        let mut inner = self
            .inner
            .state
            .lock()
            .map_err(|_| poisoned_coordinator())?;
        inner.snapshot.accepting = false;
        inner.sealed = true;
        let snapshot = inner.snapshot;
        self.inner.changed.notify_all();
        Ok(snapshot)
    }

    pub fn snapshot(&self) -> Result<AdmissionSnapshot, LifecycleError> {
        self.inner
            .state
            .lock()
            .map(|inner| inner.snapshot)
            .map_err(|_| poisoned_coordinator())
    }

    /// Waits on the same absolute grace deadline supplied by the controller.
    /// This method never creates or renews a shutdown budget.
    pub fn wait_for_drain(
        &self,
        deadline: Deadline,
        clock: &dyn MonotonicClock,
    ) -> Result<AdmissionSnapshot, LifecycleError> {
        let mut inner = self
            .inner
            .state
            .lock()
            .map_err(|_| poisoned_coordinator())?;
        loop {
            if inner.snapshot.is_drained() {
                return Ok(inner.snapshot);
            }
            let remaining = deadline.remaining(clock);
            if remaining.is_zero() {
                return Err(LifecycleError::DeadlineExpired {
                    phase: "execution drain",
                });
            }
            let wait = remaining.min(Duration::from_millis(50));
            let (next, _) = self
                .inner
                .changed
                .wait_timeout(inner, wait)
                .map_err(|_| poisoned_coordinator())?;
            inner = next;
        }
    }

    /// Waits until every accepted task has relinquished its mutation permit.
    /// This is used only after reporting grace expiry to keep the runtime lease
    /// alive while owned blocking workers still have access to storage.
    pub fn wait_until_drained(&self) -> Result<AdmissionSnapshot, LifecycleError> {
        let mut inner = self
            .inner
            .state
            .lock()
            .map_err(|_| poisoned_coordinator())?;
        while !inner.snapshot.is_drained() {
            inner = self
                .inner
                .changed
                .wait(inner)
                .map_err(|_| poisoned_coordinator())?;
        }
        Ok(inner.snapshot)
    }
}

/// RAII registration. A blocking worker must retain this permit for as long as
/// it can still mutate storage; dropping an async join handle is not enough.
pub struct TaskPermit {
    inner: Arc<Inner>,
    state: TrackedExecutionState,
    released: bool,
}

impl TaskPermit {
    pub fn state(&self) -> TrackedExecutionState {
        self.state
    }

    pub fn transition(&mut self, next: TrackedExecutionState) -> Result<(), LifecycleError> {
        let allowed = matches!(
            (self.state, next),
            (
                TrackedExecutionState::Preparation,
                TrackedExecutionState::Queued
            ) | (TrackedExecutionState::Queued, TrackedExecutionState::Active)
        );
        if !allowed {
            return Err(LifecycleError::UnsafeOwnership(
                "invalid execution admission state transition".to_owned(),
            ));
        }
        let mut inner = self
            .inner
            .state
            .lock()
            .map_err(|_| poisoned_coordinator())?;
        decrement(&mut inner.snapshot, self.state);
        increment(&mut inner.snapshot, next);
        self.state = next;
        self.inner.changed.notify_all();
        Ok(())
    }

    pub fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if self.released {
            return;
        }
        if let Ok(mut inner) = self.inner.state.lock() {
            decrement(&mut inner.snapshot, self.state);
            self.inner.changed.notify_all();
        }
        self.released = true;
    }
}

impl Drop for TaskPermit {
    fn drop(&mut self) {
        self.release_inner();
    }
}

fn increment(snapshot: &mut AdmissionSnapshot, state: TrackedExecutionState) {
    match state {
        TrackedExecutionState::Preparation => snapshot.preparation += 1,
        TrackedExecutionState::Queued => snapshot.queued += 1,
        TrackedExecutionState::Active => snapshot.active += 1,
    }
}

fn decrement(snapshot: &mut AdmissionSnapshot, state: TrackedExecutionState) {
    let count = match state {
        TrackedExecutionState::Preparation => &mut snapshot.preparation,
        TrackedExecutionState::Queued => &mut snapshot.queued,
        TrackedExecutionState::Active => &mut snapshot.active,
    };
    *count = count.saturating_sub(1);
}

fn poisoned_coordinator() -> LifecycleError {
    LifecycleError::UnsafeOwnership("execution admission lock was poisoned".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        thread,
        time::Duration,
    };

    #[derive(Clone, Default)]
    struct FakeClock(Arc<AtomicU64>);

    impl MonotonicClock for FakeClock {
        fn now(&self) -> Duration {
            Duration::from_nanos(self.0.load(Ordering::SeqCst))
        }
    }

    #[test]
    fn accepted_work_is_registered_before_admission_closes() {
        let coordinator = AdmissionCoordinator::new();
        let permit = coordinator
            .admit(TrackedExecutionState::Preparation)
            .unwrap();
        let snapshot = coordinator.close_admission().unwrap();
        assert!(!snapshot.accepting);
        assert_eq!(snapshot.preparation, 1);
        assert!(coordinator.admit(TrackedExecutionState::Queued).is_err());
        drop(permit);
        assert!(coordinator.snapshot().unwrap().is_drained());
    }

    #[test]
    fn permits_account_for_preparation_queue_active_and_release() {
        let coordinator = AdmissionCoordinator::new();
        let mut permit = coordinator
            .admit(TrackedExecutionState::Preparation)
            .unwrap();
        permit.transition(TrackedExecutionState::Queued).unwrap();
        permit.transition(TrackedExecutionState::Active).unwrap();
        assert_eq!(coordinator.snapshot().unwrap().active, 1);
        drop(permit);
        assert!(coordinator.snapshot().unwrap().is_drained());
    }

    #[test]
    fn competing_admission_and_close_leave_no_untracked_success() {
        let coordinator = AdmissionCoordinator::new();
        let worker_coordinator = coordinator.clone();
        let worker = thread::spawn(move || worker_coordinator.admit(TrackedExecutionState::Queued));
        let closed = coordinator.close_admission().unwrap();
        let result = worker.join().unwrap();
        let now = coordinator.snapshot().unwrap();
        assert!(!now.accepting);
        if result.is_ok() {
            assert_eq!(now.queued, 1);
            assert!(!closed.accepting);
        }
        drop(result);
        assert!(coordinator.snapshot().unwrap().is_drained());
    }

    #[test]
    fn drain_uses_the_original_deadline() {
        let coordinator = AdmissionCoordinator::new();
        let _permit = coordinator.admit(TrackedExecutionState::Active).unwrap();
        let clock = FakeClock::default();
        let deadline = Deadline::after(&clock, Duration::ZERO);
        assert!(matches!(
            coordinator.wait_for_drain(deadline, &clock),
            Err(LifecycleError::DeadlineExpired { .. })
        ));
    }
}
