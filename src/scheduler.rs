use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fs,
    path::{Component, Path, PathBuf},
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use chrono::{DateTime, LocalResult, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;
use croner::Cron;
use notify::{
    Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher,
    event::{CreateKind, ModifyKind, RemoveKind},
};
use serde::Serialize;
use serde_json::{Map as JsonMap, Value};
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::{
    sync::{
        mpsc::{self, Receiver, Sender, error::TrySendError},
        oneshot,
    },
    task::JoinHandle,
    time::Instant,
};

use crate::{
    PluginRegistry, Store, WorkflowDocument,
    lifecycle::{CoreLifecycle, execution::TrackedExecutionState},
    load_installed_plugin_registry,
    reporting::{CoreDiagnosticSource, DiagnosticReporter},
    runtime::{
        execute_prepared_workflow_with_plugins_and_shutdown,
        prepare_automatic_execution_with_context,
    },
    workflow::{WorkflowBinding, WorkflowTrigger},
};

const WORKFLOW_SCAN_INTERVAL: Duration = Duration::from_secs(1);
const FILESYSTEM_CHANNEL_CAPACITY: usize = 1_024;
const DEFAULT_FILESYSTEM_DEBOUNCE: Duration = Duration::from_millis(500);
const MIN_FILESYSTEM_DEBOUNCE: Duration = Duration::from_millis(10);
const MAX_FILESYSTEM_DEBOUNCE: Duration = Duration::from_secs(60);

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisteredTriggerProjection {
    pub composite_id: String,
    pub workflow_id: String,
    pub workflow_name: Option<String>,
    pub workflow_revision: Option<String>,
    pub trigger_id: String,
    pub trigger_type: Option<String>,
    pub configuration: Value,
    pub eligibility: String,
    pub effective_state: String,
    pub last_run_at: Option<String>,
    pub next_run_at: Option<String>,
    pub observation_at: String,
    pub unavailable_reason: Option<String>,
    pub orphan: bool,
}

/// Projects workflow trigger definitions over durable schedule observations.
/// It deliberately does not start, reconcile, or query a live watcher: durable
/// schedule data alone cannot prove active scheduling.
pub fn registered_trigger_projections(
    store: &Store,
) -> Result<Vec<RegisteredTriggerProjection>, String> {
    let workflows = store.list_workflows()?;
    let schedules = store.list_scheduled_trigger_observations()?;
    let schedules = schedules
        .into_iter()
        .map(|schedule| {
            (
                (schedule.workflow_id.clone(), schedule.trigger_id.clone()),
                schedule,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let observed_at = OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .map_err(|error| format!("cannot format trigger observation time: {error}"))?;
    let records = workflows
        .iter()
        .map(|workflow| (workflow.id.as_str(), workflow))
        .collect::<HashMap<_, _>>();
    let mut seen = HashSet::new();
    let mut projections = Vec::new();

    for workflow_record in &workflows {
        let source = store.get_workflow_source(&workflow_record.id)?;
        let document = source
            .as_ref()
            .and_then(|record| crate::WorkflowDocument::parse(&record.source).ok());
        let Some(document) = document else {
            continue;
        };
        for trigger in &document.triggers {
            let key = (workflow_record.id.clone(), trigger.id.clone());
            seen.insert(key.clone());
            let schedule = schedules.get(&key);
            let is_manual = trigger.node_type == "kakune.trigger.manual@1";
            let disabled = workflow_record.status != "enabled";
            let effective_state = if disabled {
                "disabled"
            } else if is_manual {
                "manual"
            } else {
                "unknown"
            };
            let unavailable_reason = if disabled || is_manual {
                None
            } else {
                Some(
                    "Live scheduler and watcher evidence is unavailable; durable schedule metadata does not prove activity."
                        .to_owned(),
                )
            };
            projections.push(RegisteredTriggerProjection {
                composite_id: format!(
                    "{}/{}",
                    encode_trigger_path_component(&workflow_record.id),
                    encode_trigger_path_component(&trigger.id)
                ),
                workflow_id: workflow_record.id.clone(),
                workflow_name: Some(workflow_record.name.clone()),
                workflow_revision: Some(workflow_record.revision.clone()),
                trigger_id: trigger.id.clone(),
                trigger_type: Some(trigger.node_type.clone()),
                configuration: sanitize_trigger_configuration(&trigger.with),
                eligibility: workflow_record.status.clone(),
                effective_state: effective_state.to_owned(),
                last_run_at: schedule.and_then(|schedule| schedule.last_run_at.clone()),
                next_run_at: schedule.and_then(|schedule| schedule.next_run_at.clone()),
                observation_at: observed_at.clone(),
                unavailable_reason,
                orphan: false,
            });
        }
    }

    for ((workflow_id, trigger_id), schedule) in &schedules {
        if seen.contains(&(workflow_id.clone(), trigger_id.clone())) {
            continue;
        }
        let workflow = records.get(workflow_id.as_str());
        projections.push(RegisteredTriggerProjection {
            composite_id: format!(
                "{}/{}",
                encode_trigger_path_component(workflow_id),
                encode_trigger_path_component(trigger_id)
            ),
            workflow_id: workflow_id.clone(),
            workflow_name: workflow.map(|record| record.name.clone()),
            workflow_revision: workflow.map(|record| record.revision.clone()),
            trigger_id: trigger_id.clone(),
            trigger_type: None,
            configuration: Value::Object(JsonMap::new()),
            eligibility: workflow
                .map(|record| record.status.clone())
                .unwrap_or_else(|| "unknown".to_owned()),
            effective_state: "unknown".to_owned(),
            last_run_at: schedule.last_run_at.clone(),
            next_run_at: schedule.next_run_at.clone(),
            observation_at: observed_at.clone(),
            unavailable_reason: Some(if workflow.is_some() {
                "Durable schedule record has no matching trigger definition and may be orphaned."
                    .to_owned()
            } else {
                "Durable schedule record references a missing workflow and is orphaned.".to_owned()
            }),
            orphan: true,
        });
    }
    Ok(projections)
}

fn encode_trigger_path_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else {
            use std::fmt::Write as _;
            write!(&mut encoded, "%{byte:02X}").expect("writing to String cannot fail");
        }
    }
    encoded
}

fn sanitize_trigger_configuration(configuration: &JsonMap<String, Value>) -> Value {
    let mut safe = JsonMap::new();
    for (key, value) in configuration {
        let normalized = key.to_ascii_lowercase();
        if [
            "secret",
            "token",
            "auth",
            "credential",
            "password",
            "bearer",
            "api_key",
        ]
        .iter()
        .any(|sensitive| normalized.contains(sensitive))
        {
            continue;
        }
        safe.insert(key.clone(), sanitize_trigger_value(value));
    }
    Value::Object(safe)
}

fn sanitize_trigger_value(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut safe = JsonMap::new();
            for (key, value) in object {
                let normalized = key.to_ascii_lowercase();
                if [
                    "secret",
                    "token",
                    "auth",
                    "credential",
                    "password",
                    "bearer",
                    "api_key",
                ]
                .iter()
                .any(|sensitive| normalized.contains(sensitive))
                {
                    continue;
                }
                safe.insert(key.clone(), sanitize_trigger_value(value));
            }
            Value::Object(safe)
        }
        Value::Array(values) => Value::Array(values.iter().map(sanitize_trigger_value).collect()),
        Value::String(text)
            if text.to_ascii_lowercase().contains("bearer ")
                || text.to_ascii_lowercase().contains("token=")
                || text.to_ascii_lowercase().contains("api_key=") =>
        {
            Value::String("[redacted]".to_owned())
        }
        other => other.clone(),
    }
}

trait SchedulerClock: Send + Sync {
    fn now(&self) -> OffsetDateTime;
}

struct SystemClock;

impl SchedulerClock for SystemClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }
}

pub fn start(store: Store) {
    start_with_reporter(store, DiagnosticReporter::default());
}

pub(crate) fn start_with_reporter(store: Store, reporter: DiagnosticReporter) {
    let plugins = load_installed_plugin_registry(&store).unwrap_or_else(|error| {
        reporter.report(
            CoreDiagnosticSource::Scheduler,
            format!("cannot load installed plugins: {error}"),
        );
        PluginRegistry::default()
    });
    let plugins = plugins.with_diagnostic_reporter(reporter.clone());
    start_with_clock(
        store,
        Arc::new(SystemClock),
        plugins,
        None,
        None,
        None,
        reporter,
    );
}

pub struct SchedulerHandle {
    shutdown: Option<oneshot::Sender<()>>,
    started: Option<oneshot::Receiver<Result<(), String>>>,
    task: Option<JoinHandle<()>>,
}

impl SchedulerHandle {
    pub async fn wait_started(&mut self) -> Result<(), String> {
        let Some(started) = self.started.take() else {
            return Ok(());
        };
        started
            .await
            .map_err(|_| "scheduler exited before trigger handling initialized".to_string())?
    }

    pub async fn stop_until(&mut self, deadline: Instant) -> Result<(), String> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let Some(task) = self.task.as_mut() else {
            return Ok(());
        };
        match tokio::time::timeout_at(deadline, task).await {
            Ok(Ok(())) => {
                self.task = None;
                Ok(())
            }
            Ok(Err(error)) => {
                self.task = None;
                Err(format!("scheduler task failed while stopping: {error}"))
            }
            Err(_) => Err("scheduler did not stop before the Core grace deadline".to_string()),
        }
    }

    /// Keeps the scheduler task and filesystem watchers owned after a bounded
    /// stop wait expires; callers must not release the runtime lease first.
    pub async fn join(&mut self) -> Result<(), String> {
        let Some(task) = self.task.as_mut() else {
            return Ok(());
        };
        let result = task
            .await
            .map_err(|error| format!("scheduler task failed while stopping: {error}"));
        self.task = None;
        result
    }
}

pub(crate) fn start_managed(
    store: Store,
    lifecycle: CoreLifecycle,
    reporter: DiagnosticReporter,
) -> SchedulerHandle {
    let (shutdown, receiver) = oneshot::channel();
    let (started, started_receiver) = oneshot::channel();
    let plugins = match load_installed_plugin_registry(&store) {
        Ok(plugins) => plugins.with_diagnostic_reporter(reporter.clone()),
        Err(error) => {
            let task = tokio::spawn(async move {
                let _ = started.send(Err(format!(
                    "cannot load installed plugins before scheduler startup: {error}"
                )));
                let _ = receiver.await;
            });
            return SchedulerHandle {
                shutdown: Some(shutdown),
                started: Some(started_receiver),
                task: Some(task),
            };
        }
    };
    let task = start_with_clock(
        store,
        Arc::new(SystemClock),
        plugins,
        Some(receiver),
        Some(lifecycle),
        Some(started),
        reporter,
    );
    SchedulerHandle {
        shutdown: Some(shutdown),
        started: Some(started_receiver),
        task: Some(task),
    }
}

fn start_with_clock(
    store: Store,
    clock: Arc<dyn SchedulerClock>,
    plugins: PluginRegistry,
    mut shutdown: Option<oneshot::Receiver<()>>,
    lifecycle: Option<CoreLifecycle>,
    ready_sender: Option<oneshot::Sender<Result<(), String>>>,
    reporter: DiagnosticReporter,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut started = HashSet::new();
        let mut seen_schedules = HashSet::new();
        let mut filesystem = FilesystemScheduler::new(reporter.clone());
        if let Some(ready_sender) = ready_sender {
            let _ = ready_sender.send(Ok(()));
        }
        let mut next_scan = Instant::now();
        loop {
            let monotonic_now = Instant::now();
            if monotonic_now >= next_scan {
                if lifecycle.as_ref().is_none_or(CoreLifecycle::is_ready) {
                    refresh(
                        &store,
                        &mut started,
                        &mut seen_schedules,
                        &mut filesystem,
                        clock.now(),
                        &plugins,
                        lifecycle.as_ref(),
                    );
                }
                next_scan = monotonic_now + WORKFLOW_SCAN_INTERVAL;
            }

            filesystem.drain_events(Instant::now());
            filesystem.record_overflow(&store);
            for pending in filesystem.take_due(Instant::now()) {
                execute(ScheduledExecution {
                    store: store.clone(),
                    plugins: plugins.clone(),
                    workflow_id: pending.workflow.metadata.id,
                    source: pending.source,
                    revision: pending.revision,
                    trigger: Some(pending.trigger),
                    trigger_values: pending.trigger_values,
                    lifecycle: lifecycle.as_ref(),
                    reporter: reporter.clone(),
                });
            }

            let wake_at = filesystem
                .next_due()
                .map_or(next_scan, |due| due.min(next_scan));
            if let Some(shutdown) = shutdown.as_mut() {
                tokio::select! {
                    _ = shutdown => break,
                    _ = tokio::time::sleep_until(wake_at) => {}
                    Some(event) = filesystem.events.recv() => filesystem.handle_event(event, Instant::now()),
                }
            } else {
                tokio::select! {
                    _ = tokio::time::sleep_until(wake_at) => {}
                    Some(event) = filesystem.events.recv() => filesystem.handle_event(event, Instant::now()),
                }
            }
        }
    })
}

pub(crate) fn validate_enabled_triggers(
    workflow: &WorkflowDocument,
    workspace: &Path,
) -> Result<(), String> {
    for trigger in &workflow.triggers {
        match trigger.node_type.as_str() {
            "kakune.trigger.datetime@1" | "kakune.trigger.date-time@1" => {
                datetime_due(trigger)?;
                misfire_config(trigger)?;
            }
            "kakune.trigger.interval@1" => {
                interval_seconds(trigger)?;
                misfire_config(trigger)?;
            }
            "kakune.trigger.cron@1" => {
                cron_next_due(trigger, OffsetDateTime::now_utc())?;
                misfire_config(trigger)?;
            }
            "kakune.trigger.filesystem@1" => {
                filesystem_config(trigger, workspace)?;
            }
            "kakune.trigger.startup@1" => validate_keys(trigger, &[])?,
            _ => {}
        }
    }
    Ok(())
}

fn refresh(
    store: &Store,
    started: &mut HashSet<String>,
    seen_schedules: &mut HashSet<String>,
    filesystem: &mut FilesystemScheduler,
    now: OffsetDateTime,
    plugins: &PluginRegistry,
    lifecycle: Option<&CoreLifecycle>,
) {
    let workflows = match store.list_enabled_workflow_sources() {
        Ok(workflows) => workflows,
        Err(error) => {
            filesystem.reporter.report(
                CoreDiagnosticSource::Scheduler,
                format!("cannot list workflows: {error}"),
            );
            return;
        }
    };
    let workspace = match store.workspace_dir() {
        Ok(workspace) => workspace,
        Err(error) => {
            filesystem.reporter.report(
                CoreDiagnosticSource::Scheduler,
                format!("cannot access workspace: {error}"),
            );
            return;
        }
    };
    let mut filesystem_specs = Vec::new();

    for (workflow_id, source, revision) in workflows {
        let workflow = match WorkflowDocument::parse(&source) {
            Ok(workflow) => workflow,
            Err(error) => {
                filesystem.reporter.report(
                    CoreDiagnosticSource::Scheduler,
                    format!("workflow {workflow_id} is invalid: {error}"),
                );
                continue;
            }
        };
        for trigger in &workflow.triggers {
            match trigger.node_type.as_str() {
                "kakune.trigger.startup@1" => {
                    let key = trigger_key(&workflow_id, &trigger.id);
                    if started.insert(key) {
                        execute(ScheduledExecution {
                            store: store.clone(),
                            plugins: plugins.clone(),
                            workflow_id: workflow_id.clone(),
                            source: source.clone(),
                            revision: revision.clone(),
                            trigger: Some(trigger.clone()),
                            trigger_values: serde_json::json!({ "type": "startup", "timestamp": now.unix_timestamp() }),
                            lifecycle,
                            reporter: filesystem.reporter.clone(),
                        });
                    }
                }
                "kakune.trigger.datetime@1"
                | "kakune.trigger.date-time@1"
                | "kakune.trigger.interval@1"
                | "kakune.trigger.cron@1" => {
                    let key = trigger_key(&workflow_id, &trigger.id);
                    let recovering = !seen_schedules.contains(&key);
                    match schedule_trigger(store, &workflow_id, trigger, &revision, now, recovering)
                    {
                        Ok(count) => {
                            seen_schedules.insert(key);
                            for _ in 0..count {
                                execute(ScheduledExecution {
                                    store: store.clone(),
                                    plugins: plugins.clone(),
                                    workflow_id: workflow_id.clone(),
                                    source: source.clone(),
                                    revision: revision.clone(),
                                    trigger: Some(trigger.clone()),
                                    trigger_values: serde_json::json!({ "type": trigger.node_type, "timestamp": now.unix_timestamp() }),
                                    lifecycle,
                                    reporter: filesystem.reporter.clone(),
                                });
                            }
                        }
                        Err(error) => filesystem.reporter.report(
                            CoreDiagnosticSource::Scheduler,
                            format!("cannot schedule {workflow_id}/{}: {error}", trigger.id),
                        ),
                    }
                }
                "kakune.trigger.filesystem@1" => match filesystem_config(trigger, &workspace) {
                    Ok(config) => filesystem_specs.push(FilesystemSpec {
                        key: trigger_key(&workflow_id, &trigger.id),
                        workflow: workflow.clone(),
                        source: source.clone(),
                        revision: revision.clone(),
                        trigger: trigger.clone(),
                        config,
                    }),
                    Err(error) => filesystem.reporter.report(
                        CoreDiagnosticSource::Scheduler,
                        format!(
                            "invalid filesystem trigger {workflow_id}/{}: {error}",
                            trigger.id
                        ),
                    ),
                },
                _ => {}
            }
        }
    }
    filesystem.sync(filesystem_specs);
}

struct ScheduledExecution<'a> {
    store: Store,
    plugins: PluginRegistry,
    workflow_id: String,
    source: String,
    revision: String,
    trigger: Option<WorkflowTrigger>,
    trigger_values: Value,
    lifecycle: Option<&'a CoreLifecycle>,
    reporter: DiagnosticReporter,
}

fn execute(invocation: ScheduledExecution<'_>) {
    let ScheduledExecution {
        store,
        plugins,
        workflow_id,
        source,
        revision,
        trigger,
        trigger_values,
        lifecycle,
        reporter,
    } = invocation;
    let inputs = trigger.as_ref().map_or_else(
        || Ok(Value::Object(Default::default())),
        |trigger| trigger_inputs(trigger, &trigger_values),
    );
    let Ok(inputs) = inputs else {
        reporter.report(
            CoreDiagnosticSource::Scheduler,
            format!(
                "invalid trigger map: {}",
                inputs.expect_err("only errors reach this branch")
            ),
        );
        return;
    };
    let mut permit = lifecycle.and_then(|lifecycle| {
        lifecycle
            .admission()
            .admit(TrackedExecutionState::Preparation)
            .ok()
    });
    if lifecycle.is_some() && permit.is_none() {
        return;
    }
    let execution = match prepare_automatic_execution_with_context(
        &store,
        &source,
        &revision,
        &plugins,
        inputs,
        trigger_values.clone(),
    ) {
        Ok((execution, true)) => execution,
        Ok((_, false)) => return,
        Err(error) => {
            reporter.report(
                CoreDiagnosticSource::Scheduler,
                format!("workflow {} was not admitted: {error}", workflow_id),
            );
            return;
        }
    };
    if let Some(permit) = permit.as_mut()
        && permit.transition(TrackedExecutionState::Queued).is_err()
    {
        return;
    }
    let shutdown_requested = lifecycle
        .map(CoreLifecycle::shutdown_signal)
        .unwrap_or_else(|| Arc::new(std::sync::atomic::AtomicBool::new(false)));
    tokio::task::spawn_blocking(move || {
        let mut permit = permit;
        if let Some(permit) = permit.as_mut()
            && permit.transition(TrackedExecutionState::Active).is_err()
        {
            return;
        }
        if let Err(error) = execute_prepared_workflow_with_plugins_and_shutdown(
            &store,
            execution,
            &plugins,
            shutdown_requested,
        ) && error != "Core shutdown requested"
        {
            reporter.report(
                CoreDiagnosticSource::Scheduler,
                format!("workflow {} failed: {error}", workflow_id),
            );
        }
    });
}

fn trigger_inputs(trigger: &WorkflowTrigger, values: &Value) -> Result<Value, String> {
    let mut inputs = serde_json::Map::new();
    for (name, binding) in &trigger.map {
        let value = match binding {
            WorkflowBinding::Literal { literal } => literal.clone(),
            WorkflowBinding::From { from } if from == "$trigger" => values.clone(),
            WorkflowBinding::From { from } if from.starts_with("$trigger.") => from[9..]
                .split('.')
                .try_fold(values, |value, segment| {
                    value
                        .as_object()
                        .and_then(|object| object.get(segment))
                        .ok_or_else(|| format!("trigger value {from} is unavailable"))
                })?
                .clone(),
            _ => return Err(format!("trigger map {name} must use literal or $trigger")),
        };
        inputs.insert(name.clone(), value);
    }
    Ok(Value::Object(inputs))
}

fn trigger_key(workflow_id: &str, trigger_id: &str) -> String {
    format!("{workflow_id}/{trigger_id}")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MisfirePolicy {
    Skip,
    RunOnce,
    CatchUp(u32),
}

impl MisfirePolicy {
    fn name(self) -> &'static str {
        match self {
            Self::Skip => "skip",
            Self::RunOnce => "runOnce",
            Self::CatchUp(_) => "catchUp",
        }
    }
}

fn misfire_config(trigger: &WorkflowTrigger) -> Result<MisfirePolicy, String> {
    let policy = trigger
        .with
        .get("misfire")
        .and_then(Value::as_str)
        .unwrap_or("runOnce");
    match policy {
        "skip" => Ok(MisfirePolicy::Skip),
        "runOnce" => Ok(MisfirePolicy::RunOnce),
        "catchUp" => {
            let limit = trigger
                .with
                .get("maxCatchUp")
                .and_then(Value::as_u64)
                .filter(|value| (1..=100).contains(value))
                .ok_or_else(|| {
                    "with.maxCatchUp is required for misfire catchUp and must be from 1 to 100"
                        .to_string()
                })?;
            Ok(MisfirePolicy::CatchUp(limit as u32))
        }
        _ => Err("with.misfire must be skip, runOnce, or catchUp".to_string()),
    }
}

fn schedule_trigger(
    store: &Store,
    workflow_id: &str,
    trigger: &WorkflowTrigger,
    workflow_revision: &str,
    now: OffsetDateTime,
    recovering: bool,
) -> Result<u32, String> {
    let policy = misfire_config(trigger)?;
    let trigger_bytes = serde_json::to_vec(trigger)
        .map_err(|error| format!("cannot encode scheduled trigger configuration: {error}"))?;
    let schedule_identity = format!(
        "{}|revision:{}|trigger:{}",
        policy.name(),
        workflow_revision,
        crate::artifacts::artifact_id(&trigger_bytes)
    );
    let existing = store.scheduled_trigger(workflow_id, &trigger.id)?;
    let initial_due = initial_due(trigger, now)?;
    let Some(existing) = existing else {
        store.ensure_scheduled_trigger(
            workflow_id,
            &trigger.id,
            initial_due,
            &schedule_identity,
            now,
            false,
        )?;
        return Ok(0);
    };
    if existing.schedule_identity != schedule_identity {
        let is_legacy_identity = existing.schedule_identity == policy.name();
        store.ensure_scheduled_trigger(
            workflow_id,
            &trigger.id,
            initial_due,
            &schedule_identity,
            now,
            !is_legacy_identity,
        )?;
        if !is_legacy_identity {
            return Ok(0);
        }
    }
    if existing.completed_at.is_some() {
        return Ok(0);
    }
    if existing.next_run_at > now {
        return Ok(0);
    }

    let one_shot = is_datetime_trigger(trigger);
    let (runs, next_due, completed) = if !recovering {
        (1, next_due_after(trigger, now)?, one_shot)
    } else {
        match policy {
            MisfirePolicy::Skip => (0, next_due_after(trigger, now)?, one_shot),
            MisfirePolicy::RunOnce => (1, next_due_after(trigger, now)?, one_shot),
            MisfirePolicy::CatchUp(_) if one_shot => (1, existing.next_run_at, true),
            MisfirePolicy::CatchUp(limit) => {
                let mut runs = 0;
                let mut due = existing.next_run_at;
                while due <= now && runs < limit {
                    runs += 1;
                    due = next_due_after(trigger, due)?;
                }
                // The limit is a hard cap, so an older backlog is explicitly skipped.
                if due <= now {
                    due = next_due_after(trigger, now)?;
                }
                (runs, due, false)
            }
        }
    };
    store.advance_scheduled_trigger(workflow_id, &trigger.id, next_due, completed, now)?;
    Ok(runs)
}

fn is_datetime_trigger(trigger: &WorkflowTrigger) -> bool {
    matches!(
        trigger.node_type.as_str(),
        "kakune.trigger.datetime@1" | "kakune.trigger.date-time@1"
    )
}

fn initial_due(trigger: &WorkflowTrigger, now: OffsetDateTime) -> Result<OffsetDateTime, String> {
    match trigger.node_type.as_str() {
        "kakune.trigger.datetime@1" | "kakune.trigger.date-time@1" => datetime_due(trigger),
        "kakune.trigger.interval@1" => Ok(now),
        "kakune.trigger.cron@1" => cron_next_due(trigger, now),
        _ => Err("trigger is not calendar scheduled".to_string()),
    }
}

fn next_due_after(
    trigger: &WorkflowTrigger,
    after: OffsetDateTime,
) -> Result<OffsetDateTime, String> {
    match trigger.node_type.as_str() {
        "kakune.trigger.datetime@1" | "kakune.trigger.date-time@1" => datetime_due(trigger),
        "kakune.trigger.interval@1" => {
            let seconds = i64::try_from(interval_seconds(trigger)?)
                .map_err(|_| "with.everySeconds is too large".to_string())?;
            after
                .checked_add(TimeDuration::seconds(seconds))
                .ok_or_else(|| "next interval deadline is outside the supported range".to_string())
        }
        "kakune.trigger.cron@1" => cron_next_due(trigger, after),
        _ => Err("trigger is not calendar scheduled".to_string()),
    }
}

fn interval_seconds(trigger: &WorkflowTrigger) -> Result<u64, String> {
    validate_keys(trigger, &["everySeconds", "misfire", "maxCatchUp"])?;
    let value = trigger
        .with
        .get("everySeconds")
        .ok_or_else(|| "with.everySeconds is required".to_string())?;
    match value {
        Value::Number(value) => value.as_u64().filter(|value| *value > 0),
        Value::String(value) => value.parse::<u64>().ok().filter(|value| *value > 0),
        _ => None,
    }
    .ok_or_else(|| "with.everySeconds must be a positive integer".to_string())
}

fn datetime_due(trigger: &WorkflowTrigger) -> Result<OffsetDateTime, String> {
    validate_keys(trigger, &["at", "timezone", "dst", "misfire", "maxCatchUp"])?;
    let at = trigger
        .with
        .get("at")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "with.at must be a non-empty RFC 3339 timestamp".to_string())?;
    if let Ok(absolute) = OffsetDateTime::parse(at, &time::format_description::well_known::Rfc3339)
    {
        if let Some(timezone) = trigger.with.get("timezone") {
            timezone_value(timezone)?;
        }
        return Ok(absolute);
    }
    let timezone = trigger
        .with
        .get("timezone")
        .map(timezone_value)
        .transpose()?
        .ok_or_else(|| "with.timezone is required when with.at has no offset".to_string())?;
    let local = ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"]
        .iter()
        .find_map(|format| NaiveDateTime::parse_from_str(at, format).ok())
        .ok_or_else(|| {
            "with.at must be an RFC 3339 timestamp or a local YYYY-MM-DDTHH:MM[:SS] timestamp"
                .to_string()
        })?;
    let datetime = match timezone.from_local_datetime(&local) {
        LocalResult::Single(datetime) => datetime,
        LocalResult::Ambiguous(earlier, later) => {
            match trigger.with.get("dst").and_then(Value::as_str) {
                None | Some("earliest") => earlier,
                Some("latest") => later,
                Some(_) => return Err("with.dst must be earliest or latest".to_string()),
            }
        }
        LocalResult::None => {
            return Err(
                "with.at is a nonexistent local time in with.timezone due to DST".to_string(),
            );
        }
    };
    OffsetDateTime::from_unix_timestamp(datetime.timestamp())
        .map_err(|error| format!("with.at is outside the supported range: {error}"))?
        .replace_nanosecond(datetime.timestamp_subsec_nanos())
        .map_err(|error| format!("with.at has invalid precision: {error}"))
}

fn cron_next_due(
    trigger: &WorkflowTrigger,
    after: OffsetDateTime,
) -> Result<OffsetDateTime, String> {
    validate_keys(
        trigger,
        &["expression", "timezone", "misfire", "maxCatchUp"],
    )?;
    let expression = trigger
        .with
        .get("expression")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "with.expression must be a non-empty cron expression".to_string())?;
    let cron = Cron::from_str(expression)
        .map_err(|error| format!("with.expression is not a valid cron expression: {error}"))?;
    let timezone = trigger
        .with
        .get("timezone")
        .map(timezone_value)
        .transpose()?
        .unwrap_or(chrono_tz::UTC);
    let after = DateTime::<Utc>::from_timestamp(after.unix_timestamp(), after.nanosecond())
        .ok_or_else(|| "current time is outside the supported cron range".to_string())?
        .with_timezone(&timezone);
    let next = cron
        .find_next_occurrence(&after, false)
        .map_err(|error| format!("cannot calculate next cron occurrence: {error}"))?;
    OffsetDateTime::from_unix_timestamp(next.timestamp())
        .map_err(|error| format!("next cron occurrence is outside the supported range: {error}"))?
        .replace_nanosecond(next.timestamp_subsec_nanos())
        .map_err(|error| format!("next cron occurrence has invalid precision: {error}"))
}

fn timezone_value(value: &Value) -> Result<Tz, String> {
    value
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "with.timezone must be a non-empty IANA timezone".to_string())?
        .parse::<Tz>()
        .map_err(|_| "with.timezone must be a valid IANA timezone".to_string())
}

fn validate_keys(trigger: &WorkflowTrigger, allowed: &[&str]) -> Result<(), String> {
    for key in trigger.with.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(format!(
                "trigger {} type {} does not support with.{key}",
                trigger.id, trigger.node_type
            ));
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FilesystemConfig {
    workspace: PathBuf,
    root: PathBuf,
    events: BTreeSet<FilesystemEventKind>,
    debounce: Duration,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum FilesystemEventKind {
    Created,
    Modified,
    Removed,
    Renamed,
}

fn filesystem_config(
    trigger: &WorkflowTrigger,
    workspace: &Path,
) -> Result<FilesystemConfig, String> {
    validate_keys(trigger, &["root", "events", "debounceMs"])?;
    let root = trigger
        .with
        .get("root")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "with.root must be a non-empty workspace-relative path".to_string())?;
    let relative_root = Path::new(root);
    if relative_root.components().any(|component| {
        matches!(
            component,
            Component::Prefix(_) | Component::RootDir | Component::CurDir | Component::ParentDir
        )
    }) {
        return Err(
            "with.root must be a workspace-relative path without . or .. components".to_string(),
        );
    }
    let workspace = fs::canonicalize(workspace)
        .map_err(|error| format!("cannot resolve workspace: {error}"))?;
    let root = fs::canonicalize(workspace.join(relative_root))
        .map_err(|error| format!("cannot resolve configured root: {error}"))?;
    if !root.starts_with(&workspace) {
        return Err("with.root must resolve inside the workspace".to_string());
    }
    if !root.is_dir() {
        return Err("with.root must resolve to an existing directory".to_string());
    }

    let values = trigger
        .with
        .get("events")
        .and_then(Value::as_array)
        .filter(|values| !values.is_empty())
        .ok_or_else(|| "with.events must be a non-empty array".to_string())?;
    let mut events = BTreeSet::new();
    for value in values {
        let event = match value.as_str() {
            Some("created") => FilesystemEventKind::Created,
            Some("modified") => FilesystemEventKind::Modified,
            Some("removed") => FilesystemEventKind::Removed,
            Some("renamed") => FilesystemEventKind::Renamed,
            _ => {
                return Err(
                    "with.events may contain only created, modified, removed, or renamed"
                        .to_string(),
                );
            }
        };
        events.insert(event);
    }
    let debounce = match trigger.with.get("debounceMs") {
        None => DEFAULT_FILESYSTEM_DEBOUNCE,
        Some(Value::Number(value)) => Duration::from_millis(
            value
                .as_u64()
                .ok_or_else(|| "with.debounceMs must be an integer".to_string())?,
        ),
        _ => return Err("with.debounceMs must be an integer".to_string()),
    };
    if !(MIN_FILESYSTEM_DEBOUNCE..=MAX_FILESYSTEM_DEBOUNCE).contains(&debounce) {
        return Err("with.debounceMs must be between 10 and 60000".to_string());
    }
    Ok(FilesystemConfig {
        workspace,
        root,
        events,
        debounce,
    })
}

struct FilesystemSpec {
    key: String,
    workflow: WorkflowDocument,
    source: String,
    revision: String,
    trigger: WorkflowTrigger,
    config: FilesystemConfig,
}

struct RegisteredFilesystemTrigger {
    workflow: WorkflowDocument,
    source: String,
    revision: String,
    trigger: WorkflowTrigger,
    config: FilesystemConfig,
    overflow: Arc<AtomicU64>,
    _watcher: RecommendedWatcher,
}

struct PendingFilesystemTrigger {
    workflow: WorkflowDocument,
    source: String,
    revision: String,
    trigger: WorkflowTrigger,
    trigger_values: Value,
    due_at: Instant,
}

type FilesystemNotification = (String, notify::Result<Event>);

struct FilesystemScheduler {
    sender: Sender<FilesystemNotification>,
    events: Receiver<FilesystemNotification>,
    watchers: HashMap<String, RegisteredFilesystemTrigger>,
    pending: HashMap<String, PendingFilesystemTrigger>,
    reporter: DiagnosticReporter,
}

impl FilesystemScheduler {
    fn new(reporter: DiagnosticReporter) -> Self {
        let (sender, events) = mpsc::channel(FILESYSTEM_CHANNEL_CAPACITY);
        Self {
            sender,
            events,
            watchers: HashMap::new(),
            pending: HashMap::new(),
            reporter,
        }
    }

    fn sync(&mut self, specs: Vec<FilesystemSpec>) {
        let desired = specs
            .iter()
            .map(|spec| spec.key.as_str())
            .collect::<HashSet<_>>();
        self.watchers
            .retain(|key, _| desired.contains(key.as_str()));
        self.pending.retain(|key, _| desired.contains(key.as_str()));

        for spec in specs {
            if let Some(existing) = self.watchers.get_mut(&spec.key)
                && existing.config == spec.config
            {
                if existing.revision != spec.revision {
                    self.pending.remove(&spec.key);
                }
                existing.workflow = spec.workflow;
                existing.source = spec.source;
                existing.revision = spec.revision;
                existing.trigger = spec.trigger;
                continue;
            }
            self.pending.remove(&spec.key);
            self.watchers.remove(&spec.key);
            match new_filesystem_watcher(&spec, self.sender.clone()) {
                Ok(watcher) => {
                    self.watchers.insert(spec.key, watcher);
                }
                Err(error) => {
                    self.reporter.report(
                        CoreDiagnosticSource::Scheduler,
                        format!("cannot watch filesystem trigger: {error}"),
                    );
                }
            }
        }
    }

    fn drain_events(&mut self, now: Instant) {
        while let Ok(event) = self.events.try_recv() {
            self.handle_event(event, now);
        }
    }

    fn record_overflow(&self, store: &Store) {
        for (key, watcher) in &self.watchers {
            let dropped = watcher.overflow.swap(0, Ordering::Relaxed);
            if dropped > 0
                && let Err(error) = store.record_scheduler_event(
                    "scheduler.filesystem_overflow",
                    key,
                    serde_json::json!({ "dropped": dropped, "capacity": FILESYSTEM_CHANNEL_CAPACITY }),
                )
            {
                self.reporter.report(
                    CoreDiagnosticSource::Scheduler,
                    format!("cannot persist filesystem overflow for {key}: {error}"),
                );
            }
        }
    }

    fn handle_event(&mut self, (key, event): FilesystemNotification, now: Instant) {
        let Some(watcher) = self.watchers.get(&key) else {
            return;
        };
        let event = match event {
            Ok(event) => event,
            Err(error) => {
                self.reporter.report(
                    CoreDiagnosticSource::Scheduler,
                    format!("filesystem watcher {key} reported an error: {error}"),
                );
                return;
            }
        };
        if !filesystem_event_matches(&event.kind, &watcher.config.events)
            || !event
                .paths
                .iter()
                .any(|path| path.starts_with(&watcher.config.root))
        {
            return;
        }
        coalesce_pending(
            &mut self.pending,
            key,
            PendingFilesystemTrigger {
                workflow: watcher.workflow.clone(),
                source: watcher.source.clone(),
                revision: watcher.revision.clone(),
                trigger: watcher.trigger.clone(),
                trigger_values: filesystem_trigger_values(&event, &watcher.config),
                due_at: now + watcher.config.debounce,
            },
        );
    }

    fn take_due(&mut self, now: Instant) -> Vec<PendingFilesystemTrigger> {
        take_due_pending(&mut self.pending, now)
    }

    fn next_due(&self) -> Option<Instant> {
        self.pending.values().map(|pending| pending.due_at).min()
    }
}

fn filesystem_trigger_values(event: &Event, config: &FilesystemConfig) -> Value {
    let paths = event
        .paths
        .iter()
        .filter(|path| path.starts_with(&config.root))
        .filter_map(|path| path.strip_prefix(&config.workspace).ok())
        .map(|path| Value::String(path.to_string_lossy().replace('\\', "/")))
        .collect::<Vec<_>>();
    let path = paths.last().cloned().unwrap_or(Value::Null);
    serde_json::json!({ "type": "filesystem", "path": path, "paths": paths })
}

fn new_filesystem_watcher(
    spec: &FilesystemSpec,
    sender: Sender<FilesystemNotification>,
) -> Result<RegisteredFilesystemTrigger, String> {
    let key = spec.key.clone();
    let overflow = Arc::new(AtomicU64::new(0));
    let callback_overflow = overflow.clone();
    let mut watcher = notify::recommended_watcher(move |event| {
        if let Err(TrySendError::Full(_)) = sender.try_send((key.clone(), event)) {
            callback_overflow.fetch_add(1, Ordering::Relaxed);
        }
    })
    .map_err(|error| format!("cannot create watcher: {error}"))?;
    watcher
        .watch(&spec.config.root, RecursiveMode::Recursive)
        .map_err(|error| format!("cannot watch {}: {error}", spec.config.root.display()))?;
    Ok(RegisteredFilesystemTrigger {
        workflow: spec.workflow.clone(),
        source: spec.source.clone(),
        revision: spec.revision.clone(),
        trigger: spec.trigger.clone(),
        config: spec.config.clone(),
        overflow,
        _watcher: watcher,
    })
}

fn filesystem_event_matches(kind: &EventKind, events: &BTreeSet<FilesystemEventKind>) -> bool {
    match kind {
        EventKind::Create(
            CreateKind::Any | CreateKind::File | CreateKind::Folder | CreateKind::Other,
        ) => events.contains(&FilesystemEventKind::Created),
        EventKind::Modify(ModifyKind::Name(_)) => events.contains(&FilesystemEventKind::Renamed),
        EventKind::Modify(_) => events.contains(&FilesystemEventKind::Modified),
        EventKind::Remove(
            RemoveKind::Any | RemoveKind::File | RemoveKind::Folder | RemoveKind::Other,
        ) => events.contains(&FilesystemEventKind::Removed),
        _ => false,
    }
}

fn coalesce_pending(
    pending: &mut HashMap<String, PendingFilesystemTrigger>,
    key: String,
    incoming: PendingFilesystemTrigger,
) {
    if let Some(existing) = pending.get_mut(&key) {
        let paths = incoming
            .trigger_values
            .get("paths")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let path = incoming.trigger_values.get("path").cloned();
        let existing_paths = existing
            .trigger_values
            .as_object_mut()
            .and_then(|values| values.get_mut("paths"))
            .and_then(Value::as_array_mut)
            .expect("filesystem trigger values always contain paths");
        for candidate in paths {
            if !existing_paths.contains(&candidate) {
                existing_paths.push(candidate);
            }
        }
        if let Some(path) = path {
            existing
                .trigger_values
                .as_object_mut()
                .expect("filesystem trigger values are objects")
                .insert("path".to_string(), path);
        }
        existing.workflow = incoming.workflow;
        existing.source = incoming.source;
        existing.revision = incoming.revision;
        existing.trigger = incoming.trigger;
        existing.due_at = incoming.due_at;
        return;
    }
    pending.insert(key, incoming);
}

fn take_due_pending(
    pending: &mut HashMap<String, PendingFilesystemTrigger>,
    now: Instant,
) -> Vec<PendingFilesystemTrigger> {
    let due = pending
        .iter()
        .filter(|(_, pending)| pending.due_at <= now)
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    due.into_iter()
        .filter_map(|key| pending.remove(&key))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        fs,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use notify::{Event, EventKind, event::CreateKind};
    use serde_json::json;
    use tokio::time::Instant;

    use crate::{
        CoreDiagnosticSource, DiagnosticReporter, PluginRegistry, Store, WorkflowDocument,
        workflow::{WorkflowBinding, WorkflowTrigger},
    };

    use super::{
        FilesystemConfig, FilesystemEventKind, PendingFilesystemTrigger, ScheduledExecution,
        coalesce_pending, cron_next_due, execute, filesystem_config, filesystem_event_matches,
        filesystem_trigger_values, interval_seconds, schedule_trigger, take_due_pending,
        trigger_inputs,
    };

    fn trigger(
        node_type: &str,
        with: serde_json::Map<String, serde_json::Value>,
    ) -> WorkflowTrigger {
        WorkflowTrigger {
            id: "tick".to_string(),
            node_type: node_type.to_string(),
            with,
            map: Default::default(),
        }
    }

    #[test]
    fn accepts_a_positive_interval() {
        assert_eq!(
            interval_seconds(&trigger(
                "kakune.trigger.interval@1",
                serde_json::Map::from_iter([(String::from("everySeconds"), json!(30))])
            ))
            .expect("interval should parse"),
            30
        );
    }

    #[test]
    fn calculates_cron_dates_in_an_iana_timezone_across_dst() {
        let cron = trigger(
            "kakune.trigger.cron@1",
            serde_json::Map::from_iter([
                (String::from("expression"), json!("0 9 * * *")),
                (String::from("timezone"), json!("America/New_York")),
            ]),
        );
        let before_dst = time::macros::datetime!(2026-03-08 12:30 UTC);
        assert_eq!(
            cron_next_due(&cron, before_dst).expect("cron should calculate"),
            time::macros::datetime!(2026-03-08 13:00 UTC)
        );
    }

    #[test]
    fn resolves_an_ambiguous_datetime_with_an_explicit_dst_choice() {
        let datetime = trigger(
            "kakune.trigger.datetime@1",
            serde_json::Map::from_iter([
                (String::from("at"), json!("2026-11-01T01:30")),
                (String::from("timezone"), json!("America/New_York")),
                (String::from("dst"), json!("latest")),
            ]),
        );
        assert_eq!(
            super::datetime_due(&datetime).expect("datetime should resolve"),
            time::macros::datetime!(2026-11-01 06:30 UTC)
        );
    }

    #[test]
    fn restart_misfire_policy_is_durable_and_limited() {
        let directory = std::env::temp_dir().join(format!(
            "kakune-scheduler-misfire-test-{}",
            uuid::Uuid::new_v4()
        ));
        let store = Store::open(directory.clone()).expect("store should open");
        let workflow = "sample";
        let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: sample\n  name: Sample\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: hello\nnodes:\n  - id: hello\n    type: kakune.log@1\n    inputs:\n      message: { literal: hello }\n";
        let document = WorkflowDocument::parse(source).expect("workflow should parse");
        store
            .upsert_workflow(&document, source, "enabled")
            .expect("workflow should save");
        let interval = trigger(
            "kakune.trigger.interval@1",
            serde_json::Map::from_iter([
                (String::from("everySeconds"), json!(60)),
                (String::from("misfire"), json!("catchUp")),
                (String::from("maxCatchUp"), json!(2)),
            ]),
        );
        let initial = time::macros::datetime!(2026-09-09 09:00 UTC);
        assert_eq!(
            schedule_trigger(&store, workflow, &interval, "revision-1", initial, false)
                .expect("schedule should initialize"),
            0
        );
        assert_eq!(
            schedule_trigger(
                &store,
                workflow,
                &interval,
                "revision-1",
                time::macros::datetime!(2026-09-09 09:05 UTC),
                true
            )
            .expect("misfire should recover"),
            2
        );
        let state = store
            .scheduled_trigger(workflow, "tick")
            .expect("state should load")
            .expect("state should exist");
        assert!(state.next_run_at > time::macros::datetime!(2026-09-09 09:05 UTC));
        let revised_interval = trigger(
            "kakune.trigger.interval@1",
            serde_json::Map::from_iter([
                (String::from("everySeconds"), json!(30)),
                (String::from("misfire"), json!("catchUp")),
                (String::from("maxCatchUp"), json!(2)),
            ]),
        );
        let revised_at = time::macros::datetime!(2026-09-09 10:00 UTC);
        assert_eq!(
            schedule_trigger(
                &store,
                workflow,
                &revised_interval,
                "revision-2",
                revised_at,
                true,
            )
            .expect("changed trigger should reconcile without firing"),
            0
        );
        let reconciled = store
            .scheduled_trigger(workflow, "tick")
            .expect("reconciled schedule should load")
            .expect("reconciled schedule should exist");
        assert_eq!(reconciled.next_run_at, revised_at);
        assert_eq!(
            reconciled.schedule_identity,
            format!(
                "catchUp|revision:revision-2|trigger:{}",
                crate::artifacts::artifact_id(&serde_json::to_vec(&revised_interval).unwrap())
            )
        );
        drop(store);
        fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    #[test]
    fn filesystem_root_must_stay_inside_workspace() {
        let directory =
            std::env::temp_dir().join(format!("kakune-scheduler-test-{}", uuid::Uuid::new_v4()));
        let workspace = directory.join("workspace");
        fs::create_dir_all(workspace.join("inbox")).expect("workspace should create");
        let trigger = trigger(
            "kakune.trigger.filesystem@1",
            serde_json::Map::from_iter([
                (String::from("root"), json!("inbox")),
                (String::from("events"), json!(["created"])),
            ]),
        );
        assert!(filesystem_config(&trigger, &workspace).is_ok());
        let escaped = WorkflowTrigger {
            with: serde_json::Map::from_iter([
                (String::from("root"), json!("../outside")),
                (String::from("events"), json!(["created"])),
            ]),
            ..trigger
        };
        assert!(filesystem_config(&escaped, &workspace).is_err());
        fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    #[test]
    fn filesystem_events_are_filtered_and_coalesced() {
        let events = std::collections::BTreeSet::from([FilesystemEventKind::Created]);
        assert!(filesystem_event_matches(
            &EventKind::Create(CreateKind::File),
            &events
        ));
        assert!(!filesystem_event_matches(
            &EventKind::Modify(notify::event::ModifyKind::Any),
            &events
        ));
        let workflow = WorkflowDocument::parse("apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: sample\n  name: Sample\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: hello\nnodes:\n  - id: hello\n    type: kakune.log@1\n    inputs:\n      message: { literal: hello }\n").expect("workflow should parse");
        let trigger = WorkflowTrigger {
            id: "incoming".to_string(),
            node_type: "kakune.trigger.filesystem@1".to_string(),
            with: Default::default(),
            map: Default::default(),
        };
        let now = tokio::time::Instant::now();
        let mut pending = std::collections::HashMap::<String, PendingFilesystemTrigger>::new();
        coalesce_pending(
            &mut pending,
            "sample/incoming".to_string(),
            PendingFilesystemTrigger {
                workflow: workflow.clone(),
                source: String::new(),
                revision: "revision".to_owned(),
                trigger: trigger.clone(),
                trigger_values: json!({ "paths": ["one.txt"] }),
                due_at: now + std::time::Duration::from_secs(1),
            },
        );
        coalesce_pending(
            &mut pending,
            "sample/incoming".to_string(),
            PendingFilesystemTrigger {
                workflow,
                source: String::new(),
                revision: "revision".to_owned(),
                trigger,
                trigger_values: json!({ "paths": ["two.txt"] }),
                due_at: now + std::time::Duration::from_secs(2),
            },
        );
        assert!(take_due_pending(&mut pending, now + std::time::Duration::from_secs(1)).is_empty());
        assert_eq!(
            take_due_pending(&mut pending, now + std::time::Duration::from_secs(2)).len(),
            1
        );
    }

    #[test]
    fn filesystem_paths_are_workspace_relative_and_coalesced() {
        let workspace = std::env::temp_dir().join(format!(
            "kakune-filesystem-trigger-test-{}",
            uuid::Uuid::new_v4()
        ));
        let inbox = workspace.join("inbox");
        fs::create_dir_all(&inbox).expect("workspace should create");
        let config = FilesystemConfig {
            workspace: fs::canonicalize(&workspace).expect("workspace should resolve"),
            root: fs::canonicalize(&inbox).expect("inbox should resolve"),
            events: Default::default(),
            debounce: Duration::from_millis(500),
        };
        let document = WorkflowDocument::parse("apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: sample\n  name: Sample\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: hello\nnodes:\n  - id: hello\n    type: kakune.log@1\n    inputs:\n      message: { literal: hello }\n")
            .expect("workflow should parse");
        let mut trigger = trigger("kakune.trigger.filesystem@1", Default::default());
        trigger.map.insert(
            "filePath".to_string(),
            WorkflowBinding::From {
                from: "$trigger.path".to_string(),
            },
        );
        let first = filesystem_trigger_values(
            &Event::new(EventKind::Create(CreateKind::File))
                .add_path(config.root.join("first.txt")),
            &config,
        );
        let second = filesystem_trigger_values(
            &Event::new(EventKind::Create(CreateKind::File))
                .add_path(config.root.join("second.txt")),
            &config,
        );
        assert_eq!(first["path"], json!("inbox/first.txt"));
        assert_eq!(
            trigger_inputs(&trigger, &first).expect("trigger path should map"),
            json!({ "filePath": "inbox/first.txt" })
        );

        let now = Instant::now();
        let mut pending = HashMap::new();
        coalesce_pending(
            &mut pending,
            "sample/incoming".to_string(),
            PendingFilesystemTrigger {
                workflow: document.clone(),
                source: String::new(),
                revision: "revision".to_owned(),
                trigger: trigger.clone(),
                trigger_values: first,
                due_at: now,
            },
        );
        coalesce_pending(
            &mut pending,
            "sample/incoming".to_string(),
            PendingFilesystemTrigger {
                workflow: document,
                source: String::new(),
                revision: "revision".to_owned(),
                trigger,
                trigger_values: second,
                due_at: now,
            },
        );
        let due = take_due_pending(&mut pending, now);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].trigger_values["path"], json!("inbox/second.txt"));
        assert_eq!(
            due[0].trigger_values["paths"],
            json!(["inbox/first.txt", "inbox/second.txt"])
        );
        fs::remove_dir_all(workspace).expect("workspace should remove");
    }

    #[test]
    fn invalid_trigger_mapping_uses_the_injected_diagnostic_reporter() {
        let directory = std::env::temp_dir().join(format!(
            "kakune-scheduler-reporting-test-{}",
            uuid::Uuid::new_v4()
        ));
        let store = Store::open(directory.clone()).expect("store should open");
        let workflow = WorkflowDocument::parse(
            "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: report\n  name: Report\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: hello\nnodes:\n  - id: hello\n    type: kakune.flow.pass@1\n",
        )
        .expect("workflow should parse");
        let trigger = WorkflowTrigger {
            id: "startup".to_string(),
            node_type: "kakune.trigger.startup@1".to_string(),
            with: Default::default(),
            map: std::collections::BTreeMap::from([(
                "input".to_string(),
                WorkflowBinding::From {
                    from: "unsupported".to_string(),
                },
            )]),
        };
        let captured = Arc::new(Mutex::new(Vec::new()));
        let reporter = {
            let captured = Arc::clone(&captured);
            DiagnosticReporter::new(move |diagnostic| {
                captured.lock().expect("diagnostic lock").push(diagnostic);
            })
        };

        execute(ScheduledExecution {
            store: store.clone(),
            plugins: PluginRegistry::default(),
            workflow_id: workflow.metadata.id.clone(),
            source: String::new(),
            revision: "revision".to_owned(),
            trigger: Some(trigger),
            trigger_values: serde_json::json!({}),
            lifecycle: None,
            reporter,
        });

        let captured = captured.lock().expect("diagnostic lock");
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].source, CoreDiagnosticSource::Scheduler);
        assert!(
            captured[0]
                .message
                .contains("trigger map input must use literal or $trigger")
        );
        drop(captured);
        drop(store);
        fs::remove_dir_all(directory).expect("temporary store should be removed");
    }
}
