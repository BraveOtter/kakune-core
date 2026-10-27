//! Shared Core hosting. Callers supply shutdown and reporting adapters, never
//! cancel the hosting future: shutdown must retain ownership until all accepted
//! work and transport/control tasks have relinquished their resources.

use std::{future::Future, net::SocketAddr, path::PathBuf, pin::Pin, sync::Arc, time::Duration};

use uuid::Uuid;

use super::{
    CORE_GRACE_BUDGET, CoreInstanceController, CoreMode, Deadline, LifecycleError,
    READINESS_BUDGET, SystemMonotonicClock, execution::TrackedExecutionState,
};
use crate::{CoreConfig, Store, load_installed_plugin_registry, reporting::DiagnosticReporter};

/// Effective local selections; omitted values retain configuration defaults.
#[derive(Clone, Debug)]
pub struct HostOptions {
    pub data_dir: PathBuf,
    pub config_file: Option<PathBuf>,
    pub listen: Option<SocketAddr>,
    pub tls_certificate: Option<PathBuf>,
    pub tls_private_key: Option<PathBuf>,
    pub allowed_hosts: Vec<String>,
    pub diagnostic_location: Option<PathBuf>,
    pub mode: CoreMode,
}

#[derive(Clone)]
pub enum HostEvent {
    Claimed,
    Prepared(Uuid),
    Ready {
        instance_id: Uuid,
        address: String,
        lifecycle: super::CoreLifecycle,
    },
    Stopping,
    Stopped,
    Diagnostic(String),
}

/// CLI/service adapters own console I/O, native notifications and parent pipes.
/// Report errors are startup/shutdown failures, not permission to abandon tasks.
pub trait HostAdapter: Send + Sync {
    fn report(&self, event: HostEvent) -> Result<(), String>;

    fn parent_commit(
        &self,
        remaining: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + '_>>;
}

struct HostedTasks {
    server: tokio::task::JoinHandle<Result<(), String>>,
    server_ended: bool,
    transport_shutdown: tokio::sync::oneshot::Sender<tokio::time::Instant>,
    control: Option<tokio::task::JoinHandle<Result<(), LifecycleError>>>,
    control_shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    scheduler: Option<crate::SchedulerHandle>,
}

/// Runs foreground, user-daemon and system-service Core through one owned path.
/// The shutdown future is supplied by the caller (terminal/SCM/Unix adapter).
pub async fn serve(
    options: HostOptions,
    host: Arc<dyn HostAdapter>,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<(), String> {
    let diagnostic_host = host.clone();
    let reporter = DiagnosticReporter::new(move |diagnostic| {
        let _ = diagnostic_host.report(HostEvent::Diagnostic(diagnostic.to_string()));
    });
    let startup_clock = SystemMonotonicClock::default();
    let startup_deadline = Deadline::after(&startup_clock, READINESS_BUDGET);
    let mut controller = match options.mode {
        CoreMode::Foreground => CoreInstanceController::acquire_foreground(&options.data_dir),
        CoreMode::UserDaemon => {
            CoreInstanceController::acquire_user_daemon_with_claim(&options.data_dir, || {
                host.report(HostEvent::Claimed)
                    .map_err(LifecycleError::UnsafeOwnership)
            })
        }
        CoreMode::SystemService => {
            CoreInstanceController::acquire_system_service(&options.data_dir)
        }
    }
    .map_err(|error| format!("cannot acquire Core runtime ownership: {error}"))?;
    let prepared = (|| {
        if let Some(path) = &options.diagnostic_location {
            controller
                .set_diagnostic_location(path.display().to_string())
                .map_err(|e| e.to_string())?;
        }
        let mut loaded = CoreConfig::load_or_create(controller.data_dir(), options.config_file)?;
        if let Some(path) = options.tls_certificate {
            loaded.config.api.tls_cert = Some(path);
        }
        if let Some(path) = options.tls_private_key {
            loaded.config.api.tls_key = Some(path);
        }
        if !options.allowed_hosts.is_empty() {
            loaded.config.api.allowed_hosts = options.allowed_hosts;
        }
        loaded.config.validate()?;
        let listen = options.listen.unwrap_or(loaded.config.listen_addr()?);
        if !listen.ip().is_loopback() && loaded.config.api.tls_cert.is_none() {
            return Err("non-loopback listeners require TLS certificate and key".to_owned());
        }
        let tls = crate::server::tls_config(&loaded.config.api)?;
        let store = Store::open(controller.data_dir().to_path_buf())?;
        let recovered = store.recover_incomplete_executions()?;
        if recovered > 0 {
            host.report(HostEvent::Diagnostic(format!(
                "Recovered {recovered} incomplete execution(s) after a previous Core shutdown."
            )))?;
        }
        let plugins =
            load_installed_plugin_registry(&store)?.with_diagnostic_reporter(reporter.clone());
        let queued = store.queued_executions()?;
        let control = if options.mode != CoreMode::Foreground {
            Some(controller.prepare_control().map_err(|e| e.to_string())?)
        } else {
            None
        };
        Ok((
            loaded.config.api,
            listen,
            tls.is_some(),
            store,
            plugins,
            queued,
            control,
        ))
    })();
    let (api_config, listen, tls, store, plugins, queued, control) =
        prepared.map_err(|error| startup_failure(&mut controller, error))?;
    let listener = tokio::time::timeout(
        startup_deadline.remaining(&startup_clock),
        tokio::net::TcpListener::bind(listen),
    )
    .await
    .map_err(|_| {
        startup_failure(
            &mut controller,
            "Core listener readiness deadline expired".into(),
        )
    })?
    .map_err(|error| {
        startup_failure(
            &mut controller,
            format!("cannot listen on {listen}: {error}"),
        )
    })?;
    let address = listener
        .local_addr()
        .map_err(|error| startup_failure(&mut controller, error.to_string()))?;
    controller
        .publish_listen_address(address, tls)
        .map_err(|error| startup_failure(&mut controller, error.to_string()))?;
    let lifecycle = controller.lifecycle();
    let app = crate::api::router_with_lifecycle_and_reporter(
        store.clone(),
        plugins.clone(),
        api_config.clone(),
        lifecycle.clone(),
        reporter.clone(),
    );
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        crate::server::serve_until_ready(
            listener,
            app,
            &api_config,
            async {
                shutdown_rx
                    .await
                    .unwrap_or_else(|_| tokio::time::Instant::now())
            },
            Some(ready_tx),
        )
        .await
    });
    let mut tasks = HostedTasks {
        server,
        server_ended: false,
        transport_shutdown: shutdown_tx,
        control: None,
        control_shutdown: None,
        scheduler: None,
    };
    let mut stop_rx = None;
    if let Some((listener, capability, context)) = control {
        let (stop_tx, receiver) = tokio::sync::oneshot::channel();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        tasks.control = Some(tokio::spawn(super::control::serve_control(
            listener,
            context,
            capability,
            lifecycle.clone(),
            stop_tx,
            shutdown_rx,
        )));
        tasks.control_shutdown = Some(shutdown_tx);
        stop_rx = Some(receiver);
    }

    // Every fallible operation after the first spawn is inside this result.
    // Its only exit is the shared owned drain below, including broken pipes.
    let startup =
        async {
            match tokio::time::timeout(startup_deadline.remaining(&startup_clock), ready_rx).await {
                Ok(Ok(Ok(actual))) if actual == address => {}
                Ok(Ok(Err(error))) => return Err(error),
                _ => return Err(
                    "transport stopped or readiness deadline expired before Core startup committed"
                        .into(),
                ),
            }
            if options.mode == CoreMode::UserDaemon {
                host.report(HostEvent::Prepared(controller.instance_id()))?;
                let remaining = startup_deadline.remaining(&startup_clock);
                if remaining.is_zero() {
                    return Err("background launch commit deadline expired".into());
                }
                match tokio::time::timeout(remaining, host.parent_commit(remaining)).await {
                    Ok(Ok(true)) => {}
                    Ok(Ok(false)) => {
                        return Err("background parent channel closed before launch commit".into());
                    }
                    Ok(Err(error)) => return Err(error),
                    Err(_) => return Err("background launch commit deadline expired".into()),
                }
            }
            tasks.scheduler = Some(crate::start_managed_scheduler_with_reporter(
                store.clone(),
                lifecycle.clone(),
                reporter.clone(),
            ));
            let scheduler = tasks
                .scheduler
                .as_mut()
                .expect("scheduler was just retained");
            tokio::time::timeout(
                startup_deadline.remaining(&startup_clock),
                scheduler.wait_started(),
            )
            .await
            .map_err(|_| "Core scheduler readiness deadline expired".to_owned())??;
            if startup_deadline.is_expired(&startup_clock) {
                return Err("Core readiness deadline expired".into());
            }
            controller.mark_ready().map_err(|error| error.to_string())?;
            for execution in queued {
                let mut permit = lifecycle
                    .admission()
                    .admit(TrackedExecutionState::Preparation)
                    .map_err(|error| error.to_string())?;
                permit
                    .transition(TrackedExecutionState::Queued)
                    .map_err(|error| error.to_string())?;
                let store = store.clone();
                let plugins = plugins.clone();
                let signal = lifecycle.shutdown_signal();
                let host = host.clone();
                tokio::task::spawn_blocking(move || {
                    if let Err(error) = permit.transition(TrackedExecutionState::Active) {
                        let _ = host.report(HostEvent::Diagnostic(format!(
                            "Recovered execution was not tracked: {error}"
                        )));
                        return;
                    }
                    if let Err(error) = crate::execute_prepared_workflow_with_plugins_and_shutdown(
                        &store,
                        execution.clone(),
                        &plugins,
                        signal,
                    ) && error != "Core shutdown requested"
                    {
                        let _ = host.report(HostEvent::Diagnostic(format!(
                            "Recovered execution {} stopped: {error}",
                            execution.id
                        )));
                    }
                });
            }
            host.report(HostEvent::Ready {
                instance_id: controller.instance_id(),
                address: format!("{}://{address}", if tls { "https" } else { "http" }),
                lifecycle: lifecycle.clone(),
            })?;
            Ok::<(), String>(())
        }
        .await;
    let mut failures = Vec::new();
    match startup {
        Err(error) => failures.push(error),
        Ok(()) => {
            tokio::select! {
                () = shutdown => {}
                () = wait_for_stop(stop_rx) => {}
                result = &mut tasks.server => {
                    tasks.server_ended = true;
                    failures.push(match result {
                        Ok(Ok(())) => "Core transport ended without a shutdown request".into(),
                        Ok(Err(error)) => format!("Core transport failed: {error}"),
                        Err(error) => format!("Core transport task failed: {error}"),
                    });
                }
            }
        }
    }
    drain(&mut controller, tasks, host, failures).await
}

async fn wait_for_stop(receiver: Option<tokio::sync::oneshot::Receiver<()>>) {
    match receiver {
        Some(receiver) => {
            let _ = receiver.await;
        }
        None => std::future::pending().await,
    }
}

fn startup_failure(controller: &mut CoreInstanceController, mut error: String) -> String {
    if let Err(cleanup) = controller.mark_failed() {
        error.push_str(&format!(
            "; failed to publish failed-start state: {cleanup}"
        ));
    }
    if let Err(cleanup) = controller.release_after_drain() {
        error.push_str(&format!(
            "; runtime ownership remains for recovery: {cleanup}"
        ));
    }
    error
}

async fn drain(
    controller: &mut CoreInstanceController,
    mut tasks: HostedTasks,
    host: Arc<dyn HostAdapter>,
    mut failures: Vec<String>,
) -> Result<(), String> {
    let lifecycle = controller.lifecycle();
    let clock = SystemMonotonicClock::default();
    let deadline = Deadline::after(&clock, CORE_GRACE_BUDGET);
    let transport_deadline = tokio::time::Instant::now() + CORE_GRACE_BUDGET;
    if let Err(error) = lifecycle.begin_stopping() {
        failures.push(format!("cannot close Core execution admission: {error}"));
    }
    if let Err(error) = host.report(HostEvent::Stopping) {
        failures.push(error);
    }
    if let Err(error) = controller.mark_stopping() {
        failures.push(format!("cannot publish Core stopping state: {error}"));
    }
    if let Some(sender) = tasks.control_shutdown.take() {
        let _ = sender.send(());
    }
    let _ = tasks.transport_shutdown.send(transport_deadline);
    if let Some(scheduler) = &mut tasks.scheduler
        && let Err(error) = scheduler.stop_until(transport_deadline).await
    {
        failures.push(error);
        if let Err(error) = scheduler.join().await {
            failures.push(error);
        }
    }
    if !tasks.server_ended {
        match tokio::time::timeout_at(transport_deadline, &mut tasks.server).await {
            Ok(result) => record_server_result(result, &mut failures),
            Err(_) => {
                failures.push("Core transport did not drain before the grace deadline".into());
                record_server_result(tasks.server.await, &mut failures);
            }
        }
    }
    if let Some(mut control) = tasks.control {
        let result = match tokio::time::timeout_at(transport_deadline, &mut control).await {
            Ok(result) => result,
            Err(_) => {
                failures
                    .push("lifecycle control server did not stop before the grace deadline".into());
                control.await
            }
        };
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => failures.push(format!("lifecycle control server failed: {error}")),
            Err(error) => failures.push(format!("lifecycle control task failed: {error}")),
        }
    }
    let admission = lifecycle.admission();
    let draining = admission.clone();
    // Join the waiter even on expiry. A blocking worker's mutation permit, not
    // dropping its async handle, determines when ownership can be released.
    let result =
        tokio::task::spawn_blocking(move || draining.wait_for_drain(deadline, &clock)).await;
    if !matches!(result, Ok(Ok(snapshot)) if snapshot.is_drained()) {
        failures.push("Core execution grace expired or drain failed; ownership is retained until workers finish".into());
    }
    if !admission
        .snapshot()
        .is_ok_and(|snapshot| snapshot.is_drained())
    {
        let _ = host.report(HostEvent::Diagnostic("Core shutdown grace expired; retaining runtime ownership while accepted workers finish.".into()));
        if !matches!(
            tokio::task::spawn_blocking(move || admission.wait_until_drained()).await,
            Ok(Ok(_))
        ) {
            let _ = host.report(HostEvent::Diagnostic(
                "Cannot prove Core work is drained; retaining ownership indefinitely.".into(),
            ));
            std::future::pending::<()>().await;
        }
    }
    match controller.release_after_drain() {
        Ok(()) => {
            if let Err(error) = host.report(HostEvent::Stopped) {
                failures.push(error);
            }
        }
        Err(error) => failures.push(format!(
            "Core ownership cleanup failed; inspect retained lifecycle state: {error}"
        )),
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
}

fn record_server_result(
    result: Result<Result<(), String>, tokio::task::JoinError>,
    failures: &mut Vec<String>,
) {
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => failures.push(format!("Core transport failed during shutdown: {error}")),
        Err(error) => failures.push(format!(
            "Core transport task failed during shutdown: {error}"
        )),
    }
}
