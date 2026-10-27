//! Injectable reporting boundary for diagnostics produced by Core operations.

use std::{fmt, sync::Arc};

/// Core subsystem that produced an operational diagnostic.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoreDiagnosticSource {
    Api,
    Scheduler,
    Plugin,
}

impl CoreDiagnosticSource {
    fn label(self) -> &'static str {
        match self {
            Self::Api => "api",
            Self::Scheduler => "scheduler",
            Self::Plugin => "plugin",
        }
    }
}

/// A non-fatal operational diagnostic emitted by a Core subsystem.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoreDiagnostic {
    pub source: CoreDiagnosticSource,
    pub message: String,
}

impl fmt::Display for CoreDiagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "kakune {}: {}",
            self.source.label(),
            self.message
        )
    }
}

/// Injectable sink for operational diagnostics.
///
/// The default reporter intentionally discards diagnostics. Library code never
/// selects a console or logging destination; executable adapters can inject a
/// renderer appropriate to their environment.
#[derive(Clone)]
pub struct DiagnosticReporter {
    handler: Arc<dyn Fn(CoreDiagnostic) + Send + Sync>,
}

impl DiagnosticReporter {
    /// Creates a reporter that forwards each diagnostic to `handler`.
    pub fn new(handler: impl Fn(CoreDiagnostic) + Send + Sync + 'static) -> Self {
        Self {
            handler: Arc::new(handler),
        }
    }

    /// Reports a diagnostic without imposing a rendering policy on the caller.
    pub fn report(&self, source: CoreDiagnosticSource, message: impl Into<String>) {
        (self.handler)(CoreDiagnostic {
            source,
            message: message.into(),
        });
    }
}

impl Default for DiagnosticReporter {
    fn default() -> Self {
        Self::new(|_| {})
    }
}

impl fmt::Debug for DiagnosticReporter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DiagnosticReporter")
            .finish_non_exhaustive()
    }
}
