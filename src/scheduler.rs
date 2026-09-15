use std::{
    collections::{BTreeSet, HashMap, HashSet},
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
use serde_json::Value;
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::{
    sync::mpsc::{self, Receiver, Sender, error::TrySendError},
    time::Instant,
};

use crate::{
    PluginRegistry, Store, WorkflowDocument, load_installed_plugin_registry,
    run_workflow_with_context,
    workflow::{WorkflowBinding, WorkflowTrigger},
};

const WORKFLOW_SCAN_INTERVAL: Duration = Duration::from_secs(1);
const FILESYSTEM_CHANNEL_CAPACITY: usize = 1_024;
const DEFAULT_FILESYSTEM_DEBOUNCE: Duration = Duration::from_millis(500);
const MIN_FILESYSTEM_DEBOUNCE: Duration = Duration::from_millis(10);
const MAX_FILESYSTEM_DEBOUNCE: Duration = Duration::from_secs(60);

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
    let plugins = load_installed_plugin_registry(&store).unwrap_or_else(|error| {
        eprintln!("kakune scheduler: cannot load installed plugins: {error}");
        PluginRegistry::default()
    });
    start_with_clock(store, Arc::new(SystemClock), plugins);
}

fn start_with_clock(store: Store, clock: Arc<dyn SchedulerClock>, plugins: PluginRegistry) {
    tokio::spawn(async move {
        let mut started = HashSet::new();
        let mut seen_schedules = HashSet::new();
        let mut filesystem = FilesystemScheduler::new();
        let mut next_scan = Instant::now();
        loop {
            let monotonic_now = Instant::now();
            if monotonic_now >= next_scan {
                refresh(
                    &store,
                    &mut started,
                    &mut seen_schedules,
                    &mut filesystem,
                    clock.now(),
                    &plugins,
                );
                next_scan = monotonic_now + WORKFLOW_SCAN_INTERVAL;
            }

            filesystem.drain_events(Instant::now());
            filesystem.record_overflow(&store);
            for pending in filesystem.take_due(Instant::now()) {
                execute(
                    store.clone(),
                    plugins.clone(),
                    pending.workflow,
                    Some(&pending.trigger),
                    pending.trigger_values,
                );
            }

            let wake_at = filesystem
                .next_due()
                .map_or(next_scan, |due| due.min(next_scan));
            tokio::select! {
                _ = tokio::time::sleep_until(wake_at) => {}
                Some(event) = filesystem.events.recv() => filesystem.handle_event(event, Instant::now()),
            }
        }
    });
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
) {
    let workflows = match store.list_enabled_workflow_sources() {
        Ok(workflows) => workflows,
        Err(error) => {
            eprintln!("kakune scheduler: cannot list workflows: {error}");
            return;
        }
    };
    let workspace = match store.workspace_dir() {
        Ok(workspace) => workspace,
        Err(error) => {
            eprintln!("kakune scheduler: cannot access workspace: {error}");
            return;
        }
    };
    let mut filesystem_specs = Vec::new();

    for (workflow_id, source) in workflows {
        let workflow = match WorkflowDocument::parse(&source) {
            Ok(workflow) => workflow,
            Err(error) => {
                eprintln!("kakune scheduler: workflow {workflow_id} is invalid: {error}");
                continue;
            }
        };
        for trigger in &workflow.triggers {
            match trigger.node_type.as_str() {
                "kakune.trigger.startup@1" => {
                    let key = trigger_key(&workflow_id, &trigger.id);
                    if started.insert(key) {
                        execute(
                            store.clone(),
                            plugins.clone(),
                            workflow.clone(),
                            Some(trigger),
                            serde_json::json!({ "type": "startup", "timestamp": now.unix_timestamp() }),
                        );
                    }
                }
                "kakune.trigger.datetime@1"
                | "kakune.trigger.date-time@1"
                | "kakune.trigger.interval@1"
                | "kakune.trigger.cron@1" => {
                    let key = trigger_key(&workflow_id, &trigger.id);
                    let recovering = !seen_schedules.contains(&key);
                    match schedule_trigger(store, &workflow_id, trigger, now, recovering) {
                        Ok(count) => {
                            seen_schedules.insert(key);
                            for _ in 0..count {
                                execute(
                                    store.clone(),
                                    plugins.clone(),
                                    workflow.clone(),
                                    Some(trigger),
                                    serde_json::json!({ "type": trigger.node_type, "timestamp": now.unix_timestamp() }),
                                );
                            }
                        }
                        Err(error) => eprintln!(
                            "kakune scheduler: cannot schedule {workflow_id}/{}: {error}",
                            trigger.id
                        ),
                    }
                }
                "kakune.trigger.filesystem@1" => match filesystem_config(trigger, &workspace) {
                    Ok(config) => filesystem_specs.push(FilesystemSpec {
                        key: trigger_key(&workflow_id, &trigger.id),
                        workflow: workflow.clone(),
                        trigger: trigger.clone(),
                        config,
                    }),
                    Err(error) => eprintln!(
                        "kakune scheduler: invalid filesystem trigger {workflow_id}/{}: {error}",
                        trigger.id
                    ),
                },
                _ => {}
            }
        }
    }
    filesystem.sync(filesystem_specs);
}

fn execute(
    store: Store,
    plugins: PluginRegistry,
    workflow: WorkflowDocument,
    trigger: Option<&WorkflowTrigger>,
    trigger_values: Value,
) {
    let inputs = trigger.map_or_else(
        || Ok(Value::Object(Default::default())),
        |trigger| trigger_inputs(trigger, &trigger_values),
    );
    let Ok(inputs) = inputs else {
        eprintln!(
            "kakune scheduler: invalid trigger map: {}",
            inputs.expect_err("only errors reach this branch")
        );
        return;
    };
    tokio::task::spawn_blocking(move || {
        if let Err(error) =
            run_workflow_with_context(&store, &workflow, &plugins, inputs, trigger_values)
        {
            eprintln!(
                "kakune scheduler: workflow {} failed: {error}",
                workflow.metadata.id
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
    now: OffsetDateTime,
    recovering: bool,
) -> Result<u32, String> {
    let policy = misfire_config(trigger)?;
    let existing = store.scheduled_trigger(workflow_id, &trigger.id)?;
    let initial_due = initial_due(trigger, now)?;
    let Some(existing) = existing else {
        store.ensure_scheduled_trigger(
            workflow_id,
            &trigger.id,
            initial_due,
            policy.name(),
            now,
        )?;
        return Ok(0);
    };
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
        root,
        events,
        debounce,
    })
}

struct FilesystemSpec {
    key: String,
    workflow: WorkflowDocument,
    trigger: WorkflowTrigger,
    config: FilesystemConfig,
}

struct RegisteredFilesystemTrigger {
    workflow: WorkflowDocument,
    trigger: WorkflowTrigger,
    config: FilesystemConfig,
    overflow: Arc<AtomicU64>,
    _watcher: RecommendedWatcher,
}

struct PendingFilesystemTrigger {
    workflow: WorkflowDocument,
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
}

impl FilesystemScheduler {
    fn new() -> Self {
        let (sender, events) = mpsc::channel(FILESYSTEM_CHANNEL_CAPACITY);
        Self {
            sender,
            events,
            watchers: HashMap::new(),
            pending: HashMap::new(),
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
                existing.workflow = spec.workflow;
                existing.trigger = spec.trigger;
                continue;
            }
            self.watchers.remove(&spec.key);
            match new_filesystem_watcher(&spec, self.sender.clone()) {
                Ok(watcher) => {
                    self.watchers.insert(spec.key, watcher);
                }
                Err(error) => {
                    eprintln!("kakune scheduler: cannot watch filesystem trigger: {error}")
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
                eprintln!("kakune scheduler: cannot persist filesystem overflow for {key}: {error}");
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
                eprintln!("kakune scheduler: filesystem watcher {key} reported an error: {error}");
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
        let paths: Vec<Value> = event
            .paths
            .iter()
            .filter_map(|path| path.strip_prefix(&watcher.config.root).ok())
            .map(|path| Value::String(path.to_string_lossy().replace('\\', "/")))
            .collect();
        coalesce_pending(
            &mut self.pending,
            key,
            watcher.workflow.clone(),
            watcher.trigger.clone(),
            serde_json::json!({ "type": "filesystem", "paths": paths }),
            now + watcher.config.debounce,
        );
    }

    fn take_due(&mut self, now: Instant) -> Vec<PendingFilesystemTrigger> {
        take_due_pending(&mut self.pending, now)
    }

    fn next_due(&self) -> Option<Instant> {
        self.pending.values().map(|pending| pending.due_at).min()
    }
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
    workflow: WorkflowDocument,
    trigger: WorkflowTrigger,
    trigger_values: Value,
    due_at: Instant,
) {
    pending.insert(
        key,
        PendingFilesystemTrigger {
            workflow,
            trigger,
            trigger_values,
            due_at,
        },
    );
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
    use std::fs;

    use notify::{EventKind, event::CreateKind};
    use serde_json::json;

    use crate::{Store, WorkflowDocument, workflow::WorkflowTrigger};

    use super::{
        FilesystemEventKind, PendingFilesystemTrigger, coalesce_pending, cron_next_due,
        filesystem_config, filesystem_event_matches, interval_seconds, schedule_trigger,
        take_due_pending,
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
            schedule_trigger(&store, workflow, &interval, initial, false)
                .expect("schedule should initialize"),
            0
        );
        assert_eq!(
            schedule_trigger(
                &store,
                workflow,
                &interval,
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
            workflow.clone(),
            trigger.clone(),
            json!({ "paths": ["one.txt"] }),
            now + std::time::Duration::from_secs(1),
        );
        coalesce_pending(
            &mut pending,
            "sample/incoming".to_string(),
            workflow,
            trigger,
            json!({ "paths": ["two.txt"] }),
            now + std::time::Duration::from_secs(2),
        );
        assert!(take_due_pending(&mut pending, now + std::time::Duration::from_secs(1)).is_empty());
        assert_eq!(
            take_due_pending(&mut pending, now + std::time::Duration::from_secs(2)).len(),
            1
        );
    }
}
