//! Process lifecycle tests against real Unix processes.

#![cfg(unix)]

use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

use super::{
    BackgroundTaskService, CallerScope, StartRequest, StopReason, TaskState, now_ms,
    os::ProcessTable, store::TaskRecord,
};

struct Fixture {
    _directory: tempfile::TempDir,
    database: PathBuf,
    logs: PathBuf,
    workspace: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".tmp");
        std::fs::create_dir_all(&root).unwrap();
        let directory = tempfile::Builder::new()
            .prefix("background-tasks-")
            .tempdir_in(root)
            .unwrap();
        let workspace = directory.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        Self {
            database: directory.path().join("sessions.sqlite3"),
            logs: directory.path().join("logs"),
            workspace,
            _directory: directory,
        }
    }

    /// A fresh service over the same registry, as after a runtime restart.
    fn service(&self) -> Arc<BackgroundTaskService> {
        BackgroundTaskService::open(&self.database, &self.logs).unwrap()
    }

    fn request(&self, command: &str) -> StartRequest {
        StartRequest {
            name: "test task".to_owned(),
            command: command.to_owned(),
            cwd: self.workspace.clone(),
            endpoints: Vec::new(),
            environment: std::env::vars().collect(),
        }
    }
}

fn owner(id: &str) -> CallerScope {
    CallerScope {
        owner: id.to_owned(),
        run_id: None,
        turn_id: Some("turn-1".to_owned()),
        call_id: Some("call-1".to_owned()),
    }
}

async fn live_processes(group: i32, start: &str) -> Vec<i32> {
    ProcessTable::capture()
        .await
        .unwrap()
        .task_processes(group, start)
        .all()
}

async fn eventually(mut condition: impl AsyncFnMut() -> bool) {
    for _ in 0..100 {
        if condition().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("condition was not reached within 10 seconds");
}

fn pid_file(fixture: &Fixture, name: &str) -> PathBuf {
    fixture.workspace.join(name)
}

async fn read_pid(path: &PathBuf) -> i32 {
    let mut text = String::new();
    eventually(async || {
        text = std::fs::read_to_string(path).unwrap_or_default();
        !text.trim().is_empty()
    })
    .await;
    text.trim().parse().unwrap()
}

fn alive(pid: i32) -> bool {
    nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_ok()
}

#[tokio::test]
async fn a_task_outlives_its_call_and_stop_terminates_the_whole_tree() {
    let fixture = Fixture::new();
    let service = fixture.service();
    let scope = owner("owner-a");
    let child_pid = pid_file(&fixture, "child.pid");
    // The shell starts a grandchild and keeps writing to its log.
    let command = format!(
        "sleep 300 & echo $! > {}; while true; do echo tick; sleep 0.2; done",
        child_pid.display()
    );
    let task = service
        .start(&scope, fixture.request(&command))
        .await
        .unwrap();
    assert_eq!(task.state, "running");
    let grandchild = read_pid(&child_pid).await;
    assert!(alive(grandchild));

    // Still running after the starting call returned, with its output captured.
    tokio::time::sleep(Duration::from_millis(500)).await;
    service.observe().await;
    let detail = service
        .inspect_for(&scope, &task.task_id, 4096)
        .await
        .unwrap();
    assert_eq!(detail.task.state, "running");
    assert!(detail.log_tail.contains("tick"));

    let stopped = service
        .stop_for(&scope, &task.task_id, Some(Duration::from_secs(2)))
        .await
        .unwrap();
    assert_eq!(stopped.state, "stopped");
    assert_eq!(stopped.stop_reason.as_deref(), Some("requested"));
    eventually(async || !alive(grandchild)).await;
}

#[tokio::test]
async fn stop_reaches_descendants_that_left_the_process_group() {
    let fixture = Fixture::new();
    let service = fixture.service();
    let scope = owner("owner-a");
    let escaped_pid = pid_file(&fixture, "escaped.pid");
    // Job control puts the background job in its own process group, still parented by the shell.
    // Bash, because dash ignores `set -m` without a terminal.
    let command = format!(
        "bash -c 'set -m; sleep 300 & echo $! > {}; wait'",
        escaped_pid.display()
    );
    let task = service
        .start(&scope, fixture.request(&command))
        .await
        .unwrap();
    let escaped = read_pid(&escaped_pid).await;
    let table = ProcessTable::capture().await.unwrap();
    let processes = table.task_processes(
        i32::try_from(task.process_id.unwrap()).unwrap(),
        table
            .start_of(i32::try_from(task.process_id.unwrap()).unwrap())
            .unwrap(),
    );
    assert!(processes.descendants.contains(&escaped));

    let stopped = service.stop_for(&scope, &task.task_id, None).await.unwrap();
    assert_eq!(stopped.state, "stopped");
    eventually(async || !alive(escaped)).await;
}

#[tokio::test]
async fn stop_keeps_escalating_to_descendants_orphaned_by_the_first_signal() {
    let fixture = Fixture::new();
    let service = fixture.service();
    let scope = owner("owner-a");
    let escaped_pid = pid_file(&fixture, "stubborn.pid");
    // The escaped job ignores SIGTERM; the shell that parents it dies of SIGTERM, so the job is
    // reparented and can no longer be found through parent links.
    let command = format!(
        r#"bash -c 'set -m; sh -c "trap \"\" TERM; exec sleep 300" & echo $! > {}; wait'"#,
        escaped_pid.display()
    );
    let task = service
        .start(&scope, fixture.request(&command))
        .await
        .unwrap();
    let escaped = read_pid(&escaped_pid).await;

    let stopped = service
        .stop_for(&scope, &task.task_id, Some(Duration::from_millis(500)))
        .await
        .unwrap();
    assert_eq!(stopped.state, "stopped");
    eventually(async || !alive(escaped)).await;
}

#[tokio::test]
async fn a_natural_exit_is_recorded_with_its_code() {
    let fixture = Fixture::new();
    let service = fixture.service();
    let scope = owner("owner-a");
    let task = service
        .start(&scope, fixture.request("echo done; exit 7"))
        .await
        .unwrap();
    eventually(async || {
        service.observe().await;
        service.record(&task.task_id).unwrap().state == TaskState::Exited
    })
    .await;
    let record = service.record(&task.task_id).unwrap();
    assert_eq!(record.exit_code, Some(7));
    let detail = service
        .inspect_for(&scope, &task.task_id, 4096)
        .await
        .unwrap();
    assert!(detail.log_tail.contains("done"));
}

#[tokio::test]
async fn a_failed_launch_is_recorded_and_not_left_launching() {
    let fixture = Fixture::new();
    let service = fixture.service();
    let mut request = fixture.request("true");
    request.cwd = fixture.workspace.join("missing");
    let error = service.start(&owner("owner-a"), request).await.unwrap_err();
    assert!(error.contains("does not exist"));
    assert!(service.list_for(&owner("owner-a")).unwrap().is_empty());
}

#[tokio::test]
async fn ownership_comes_from_the_caller_scope() {
    let fixture = Fixture::new();
    let service = fixture.service();
    let task = service
        .start(&owner("owner-a"), fixture.request("sleep 300"))
        .await
        .unwrap();
    let other = owner("owner-b");
    assert!(service.list_for(&other).unwrap().is_empty());
    assert!(service.inspect_for(&other, &task.task_id, 0).await.is_err());
    assert!(service.stop_for(&other, &task.task_id, None).await.is_err());

    // A native run inside the owner sees only the tasks it started itself.
    let run = CallerScope {
        run_id: Some("run-1".to_owned()),
        ..owner("owner-a")
    };
    assert!(service.list_for(&run).unwrap().is_empty());
    let run_task = service
        .start(&run, fixture.request("sleep 300"))
        .await
        .unwrap();
    assert_eq!(run_task.origin_run_id.as_deref(), Some("run-1"));
    assert_eq!(service.list_for(&run).unwrap().len(), 1);
    // The owner session sees both.
    assert_eq!(service.list_for(&owner("owner-a")).unwrap().len(), 2);

    for id in [&task.task_id, &run_task.task_id] {
        service.stop(id, StopReason::Requested, None).await.unwrap();
    }
}

#[tokio::test]
async fn owner_deletion_fences_launches_terminates_tasks_and_keeps_orphan_records() {
    let fixture = Fixture::new();
    let service = fixture.service();
    let scope = owner("owner-a");
    let task = service
        .start(&scope, fixture.request("sleep 300"))
        .await
        .unwrap();
    let group = i32::try_from(task.process_id.unwrap()).unwrap();

    service.begin_owner_deletion("owner-a").await;
    // Nothing is signalled before the deletion commits.
    assert!(alive(group));
    let refused = service
        .start(&scope, fixture.request("sleep 300"))
        .await
        .unwrap_err();
    assert!(refused.contains("being deleted"));

    let cleanup = service
        .finish_owner_deletion("owner-a", true)
        .await
        .unwrap()
        .await
        .unwrap();
    assert_eq!(cleanup.stopped, vec![task.task_id.clone()]);
    assert!(cleanup.incomplete.is_empty());
    assert!(!alive(group));
    let inventory = service.inventory(None).unwrap();
    assert_eq!(inventory.tasks.len(), 1);
    assert_eq!(
        inventory.tasks[0].stop_reason.as_deref(),
        Some("owner_deleted")
    );
    assert!(inventory.tasks[0].owner_deleted_at_ms.is_some());
}

#[tokio::test]
async fn a_failed_deletion_lifts_the_launch_fence() {
    let fixture = Fixture::new();
    let service = fixture.service();
    let running = service
        .start(&owner("owner-a"), fixture.request("sleep 300"))
        .await
        .unwrap();
    service.begin_owner_deletion("owner-a").await;
    assert!(
        service
            .finish_owner_deletion("owner-a", false)
            .await
            .is_none()
    );
    // The surviving session keeps its task, unmarked.
    let kept = service.inventory(None).unwrap().tasks;
    assert_eq!(kept[0].state, "running");
    assert!(kept[0].owner_deleted_at_ms.is_none());
    service
        .stop(&running.task_id, StopReason::Requested, None)
        .await
        .unwrap();
    let task = service
        .start(&owner("owner-a"), fixture.request("sleep 300"))
        .await
        .unwrap();
    service
        .stop(&task.task_id, StopReason::Requested, None)
        .await
        .unwrap();
}

#[tokio::test]
async fn a_restart_stops_tasks_whose_owner_deletion_did_not_settle() {
    let fixture = Fixture::new();
    let first = fixture.service();
    let task = first
        .start(&owner("owner-a"), fixture.request("sleep 300"))
        .await
        .unwrap();
    let group = i32::try_from(task.process_id.unwrap()).unwrap();
    // The runtime stops after marking the owner deleted but before cleaning up.
    first.begin_owner_deletion("owner-a").await;
    drop(first);

    let restarted = fixture.service();
    assert!(restarted.reconcile().await);
    for _ in 0..100 {
        if !alive(group) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(!alive(group));
    let record = restarted.record(&task.task_id).unwrap();
    assert_eq!(record.stop_reason.as_deref(), Some("owner_deleted"));
}

#[tokio::test]
async fn a_restarted_runtime_adopts_live_tasks_and_can_stop_them() {
    let fixture = Fixture::new();
    let first = fixture.service();
    let task = first
        .start(&owner("owner-a"), fixture.request("sleep 300"))
        .await
        .unwrap();
    let group = i32::try_from(task.process_id.unwrap()).unwrap();
    // The first runtime goes away without stopping anything.
    drop(first);

    let restarted = fixture.service();
    restarted.reconcile().await;
    let record = restarted.record(&task.task_id).unwrap();
    assert_eq!(record.state, TaskState::Running);
    assert_eq!(record.supervision, "adopted");

    let stopped = restarted
        .stop(&task.task_id, StopReason::Requested, None)
        .await
        .unwrap();
    assert_eq!(stopped.state, "stopped");
    eventually(async || !alive(group)).await;
}

#[tokio::test]
async fn a_task_that_ended_while_the_runtime_was_down_is_exited_not_running() {
    let fixture = Fixture::new();
    let first = fixture.service();
    let task = first
        .start(&owner("owner-a"), fixture.request("sleep 0.2"))
        .await
        .unwrap();
    drop(first);
    tokio::time::sleep(Duration::from_millis(700)).await;

    let restarted = fixture.service();
    restarted.reconcile().await;
    let record = restarted.record(&task.task_id).unwrap();
    assert_eq!(record.state, TaskState::Exited);
    assert!(record.detail.unwrap().contains("not supervising"));
}

fn bare_record(fixture: &Fixture, task_id: &str, state: TaskState) -> TaskRecord {
    TaskRecord {
        task_id: task_id.to_owned(),
        name: "recovered".to_owned(),
        owner_session_id: "owner-a".to_owned(),
        origin_run_id: None,
        origin_turn_id: None,
        origin_call_id: None,
        command: "sleep 300".to_owned(),
        cwd: fixture.workspace.display().to_string(),
        machine: "test".to_owned(),
        state,
        supervision: "child".to_owned(),
        process_id: None,
        process_start: None,
        exit_code: None,
        exit_signal: None,
        detail: None,
        stop_reason: None,
        stop_requested_at_ms: None,
        owner_deleted_at_ms: None,
        endpoints: Vec::new(),
        log_path: fixture
            .logs
            .join(format!("{task_id}.log"))
            .display()
            .to_string(),
        created_at_ms: now_ms(),
        started_at_ms: None,
        ended_at_ms: None,
        updated_at_ms: now_ms(),
    }
}

/// A gated leader exactly as a launch creates it, before its release line.
fn spawn_gated(fixture: &Fixture, command: &str) -> std::process::Child {
    use std::os::unix::process::CommandExt as _;
    let (program, arguments) = super::gated_shell(command, &HashMap::new());
    std::process::Command::new(program)
        .args(arguments)
        .current_dir(&fixture.workspace)
        .stdin(std::process::Stdio::piped())
        .process_group(0)
        .spawn()
        .unwrap()
}

#[tokio::test]
async fn an_unreleased_launch_never_runs_its_command() {
    let fixture = Fixture::new();
    let witness = fixture.workspace.join("ran");
    let mut gated = spawn_gated(&fixture, &format!("touch {}", witness.display()));
    // The runtime dies before releasing: its end of the gate closes.
    drop(gated.stdin.take());
    let status = gated.wait().unwrap();
    assert_eq!(status.code(), Some(super::UNRELEASED_EXIT_CODE));
    assert!(!witness.exists());
}

#[tokio::test]
async fn interrupted_launches_are_resolved_explicitly_after_restart() {
    let fixture = Fixture::new();
    let service = fixture.service();
    // Interrupted before the identity was recorded: the command cannot have run.
    service
        .with_store(|store| {
            store.insert(&bare_record(
                &fixture,
                "bgt_unrecorded",
                TaskState::Launching,
            ))
        })
        .unwrap();
    // Interrupted after release but before the running state was saved: still alive.
    let mut released = spawn_gated(&fixture, "sleep 300");
    {
        use std::io::Write as _;
        released.stdin.take().unwrap().write_all(b"\n").unwrap();
    }
    let pid = i32::try_from(released.id()).unwrap();
    let mut record = bare_record(&fixture, "bgt_released", TaskState::Launching);
    record.process_id = Some(pid);
    eventually(async || {
        record.process_start = ProcessTable::capture()
            .await
            .unwrap()
            .start_of(pid)
            .map(str::to_owned);
        record.process_start.is_some()
    })
    .await;
    service.with_store(|store| store.insert(&record)).unwrap();

    service.reconcile().await;
    let unrecorded = service.record("bgt_unrecorded").unwrap();
    assert_eq!(unrecorded.state, TaskState::FailedToStart);
    assert!(unrecorded.detail.unwrap().contains("never ran"));
    let adopted = service.record("bgt_released").unwrap();
    assert_eq!(adopted.state, TaskState::Running);
    assert_eq!(adopted.supervision, "adopted");

    let stopped = service
        .stop("bgt_released", StopReason::Requested, None)
        .await
        .unwrap();
    assert_eq!(stopped.state, "stopped");
    let _ = released.wait();
}

#[tokio::test]
async fn an_interrupted_stop_resumes_after_restart() {
    let fixture = Fixture::new();
    let first = fixture.service();
    let task = first
        .start(&owner("owner-a"), fixture.request("sleep 300"))
        .await
        .unwrap();
    let group = i32::try_from(task.process_id.unwrap()).unwrap();
    // Persist the stop intent and "crash" before any signal is sent.
    let mut record = first.record(&task.task_id).unwrap();
    record.state = TaskState::Stopping;
    record.stop_reason = Some("owner_deleted".to_owned());
    first.save(&mut record).unwrap();
    drop(first);

    let restarted = fixture.service();
    restarted.reconcile().await;
    eventually(async || restarted.record(&task.task_id).unwrap().state == TaskState::Stopped).await;
    let record = restarted.record(&task.task_id).unwrap();
    assert_eq!(record.stop_reason.as_deref(), Some("owner_deleted"));
    assert!(!alive(group));
}

#[tokio::test]
async fn a_reused_process_id_is_never_signalled() {
    let fixture = Fixture::new();
    let service = fixture.service();
    // An unrelated process occupies the recorded id, but with a different start time.
    let mut unrelated = std::process::Command::new("sleep")
        .arg("300")
        .spawn()
        .unwrap();
    let pid = i32::try_from(unrelated.id()).unwrap();
    let mut record = bare_record(&fixture, "bgt_reused", TaskState::Running);
    record.process_id = Some(pid);
    record.process_start = Some("Mon Jan  1 00:00:00 2001".to_owned());
    service.with_store(|store| store.insert(&record)).unwrap();

    service.reconcile().await;
    assert_eq!(
        service.record("bgt_reused").unwrap().state,
        TaskState::Exited
    );
    service
        .stop("bgt_reused", StopReason::Requested, None)
        .await
        .unwrap();
    assert!(alive(pid), "an unrelated process must survive");
    assert!(
        live_processes(pid, "Mon Jan  1 00:00:00 2001")
            .await
            .is_empty()
    );
    unrelated.kill().unwrap();
    let _ = unrelated.wait();
}

#[tokio::test]
async fn logs_are_bounded_and_include_the_archived_generation() {
    let fixture = Fixture::new();
    std::fs::create_dir_all(&fixture.logs).unwrap();
    let log = fixture.logs.join("bounded.log");
    std::fs::write(crate::service_log::archive_path(&log), "older\n").unwrap();
    std::fs::write(&log, "newer\n").unwrap();
    let (tail, truncated) = super::read_log_tail(&log, 1024);
    assert_eq!(tail, "older\nnewer\n");
    assert!(!truncated);
    let (tail, truncated) = super::read_log_tail(&log, 4);
    assert_eq!(tail, "wer\n");
    assert!(truncated);
}

#[test]
fn endpoint_addresses_are_host_and_port() {
    assert_eq!(
        super::endpoint_address("http://127.0.0.1:3000/health").as_deref(),
        Some("127.0.0.1:3000")
    );
    assert_eq!(
        super::endpoint_address("https://localhost").as_deref(),
        Some("localhost:443")
    );
    assert_eq!(
        super::endpoint_address("localhost:5432").as_deref(),
        Some("localhost:5432")
    );
    assert_eq!(super::endpoint_address("not an endpoint"), None);
}
