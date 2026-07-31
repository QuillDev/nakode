use std::{collections::HashMap, ffi::OsString, path::PathBuf, process::Stdio, sync::Arc};

#[cfg(unix)]
use std::os::unix::process::CommandExt;

use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::{Child, Command},
    sync::{Mutex, mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::tools::{MAX_TOOL_OUTPUT_BYTES, truncate_output};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ShellEvent {
    Output {
        id: String,
        output: String,
    },
    Finished {
        id: String,
        output: String,
        exit_code: Option<i32>,
        interrupted: bool,
    },
    Failed {
        id: String,
        message: String,
    },
}

pub struct ShellProcesses {
    pub events: mpsc::Receiver<ShellEvent>,
    event_tx: mpsc::Sender<ShellEvent>,
    cancellation: CancellationToken,
    tasks: HashMap<String, ShellProcess>,
}

struct ShellProcess {
    cancellation: CancellationToken,
    task: JoinHandle<()>,
}

impl ShellProcesses {
    pub fn new() -> Self {
        let (event_tx, events) = mpsc::channel(128);
        Self {
            events,
            event_tx,
            cancellation: CancellationToken::new(),
            tasks: HashMap::new(),
        }
    }

    pub fn spawn(&mut self, workspace: PathBuf, id: String, command: String) {
        let events = self.event_tx.clone();
        let cancellation = self.cancellation.child_token();
        let task_id = id.clone();
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            if let Err(message) = run_shell_command(
                workspace,
                task_id.clone(),
                command,
                events.clone(),
                task_cancellation,
            )
            .await
            {
                let _ = events
                    .send(ShellEvent::Failed {
                        id: task_id,
                        message,
                    })
                    .await;
            }
        });
        self.tasks.insert(id, ShellProcess { cancellation, task });
    }

    pub fn cancel(&self, id: &str) -> bool {
        self.tasks.get(id).is_some_and(|process| {
            process.cancellation.cancel();
            true
        })
    }

    pub fn complete(&mut self, id: &str) {
        self.tasks.remove(id);
    }

    pub async fn shutdown(&mut self) {
        self.cancellation.cancel();
        for (_, process) in self.tasks.drain() {
            let mut task = process.task;
            loop {
                tokio::select! {
                    _ = &mut task => break,
                    _ = self.events.recv() => {}
                }
            }
        }
    }
}

async fn run_shell_command(
    workspace: PathBuf,
    id: String,
    command_text: String,
    events: mpsc::Sender<ShellEvent>,
    cancellation: CancellationToken,
) -> Result<(), String> {
    let (program, arguments) = shell_command(&command_text);
    let mut command = Command::new(program);
    command
        .args(arguments)
        .current_dir(workspace)
        .kill_on_drop(true)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_process_group(&mut command);
    let mut child = command
        .spawn()
        .map_err(|error| format!("failed to start shell command: {error}"))?;
    let process_id = child.id();
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "shell command stdout was not captured".to_owned())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "shell command stderr was not captured".to_owned())?;
    let output = Arc::new(Mutex::new(Vec::new()));
    let stdout_task = tokio::spawn(stream_output(
        stdout,
        id.clone(),
        Arc::clone(&output),
        events.clone(),
    ));
    let stderr_task = tokio::spawn(stream_output(
        stderr,
        id.clone(),
        Arc::clone(&output),
        events.clone(),
    ));

    let mut interrupted = false;
    let status = tokio::select! {
        status = child.wait() => status.map_err(|error| format!("failed to wait for shell command: {error}"))?,
        () = cancellation.cancelled() => {
            interrupted = true;
            terminate_process_tree(&mut child, process_id)?;
            child.wait().await.map_err(|error| format!("failed to reap interrupted shell command: {error}"))?
        }
    };
    stdout_task
        .await
        .map_err(|error| format!("shell stdout reader failed: {error}"))??;
    stderr_task
        .await
        .map_err(|error| format!("shell stderr reader failed: {error}"))??;
    let output = output_text(&output).await;
    let _ = events
        .send(ShellEvent::Finished {
            id,
            output,
            exit_code: status.code(),
            interrupted,
        })
        .await;
    Ok(())
}

async fn stream_output(
    mut stream: impl AsyncRead + Unpin,
    id: String,
    output: Arc<Mutex<Vec<u8>>>,
    events: mpsc::Sender<ShellEvent>,
) -> Result<(), String> {
    let mut buffer = [0_u8; 8 * 1024];
    loop {
        let read = stream
            .read(&mut buffer)
            .await
            .map_err(|error| format!("failed to read shell output: {error}"))?;
        if read == 0 {
            return Ok(());
        }
        {
            let mut output = output.lock().await;
            let remaining = MAX_TOOL_OUTPUT_BYTES
                .saturating_add(1)
                .saturating_sub(output.len());
            if remaining == 0 {
                continue;
            }
            output.extend_from_slice(&buffer[..read.min(remaining)]);
            let snapshot = truncate_output(output.clone());
            if events
                .send(ShellEvent::Output {
                    id: id.clone(),
                    output: snapshot,
                })
                .await
                .is_err()
            {
                return Ok(());
            }
        }
    }
}

async fn output_text(output: &Mutex<Vec<u8>>) -> String {
    let output = output.lock().await;
    if output.is_empty() {
        "(no output)".to_owned()
    } else {
        truncate_output(output.clone())
    }
}

#[cfg(unix)]
fn shell_command(command: &str) -> (&'static str, Vec<OsString>) {
    ("sh", vec!["-lc".into(), command.into()])
}

#[cfg(windows)]
fn shell_command(command: &str) -> (&'static str, Vec<OsString>) {
    (
        "cmd.exe",
        vec!["/D".into(), "/S".into(), "/C".into(), command.into()],
    )
}

#[cfg(unix)]
fn configure_process_group(command: &mut Command) {
    command.as_std_mut().process_group(0);
}

#[cfg(not(unix))]
fn configure_process_group(_command: &mut Command) {}

#[cfg(unix)]
fn terminate_process_tree(child: &mut Child, process_id: Option<u32>) -> Result<(), String> {
    use nix::{sys::signal, unistd::Pid};

    let Some(process_id) = process_id.and_then(|id| i32::try_from(id).ok()) else {
        return child
            .start_kill()
            .map_err(|error| format!("failed to interrupt shell command: {error}"));
    };
    signal::killpg(Pid::from_raw(process_id), signal::Signal::SIGKILL)
        .map_err(|error| format!("failed to interrupt shell process group: {error}"))
}

#[cfg(not(unix))]
fn terminate_process_tree(child: &mut Child, _process_id: Option<u32>) -> Result<(), String> {
    child
        .start_kill()
        .map_err(|error| format!("failed to interrupt shell command: {error}"))
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    #[cfg(unix)]
    fn failing_command() -> &'static str {
        "printf first; printf second >&2; exit 7"
    }

    #[cfg(windows)]
    fn failing_command() -> &'static str {
        "echo first & echo second 1>&2 & exit /b 7"
    }

    #[cfg(unix)]
    fn long_running_command() -> &'static str {
        "sleep 30"
    }

    #[cfg(windows)]
    fn long_running_command() -> &'static str {
        "ping -n 30 127.0.0.1 > nul"
    }

    #[tokio::test]
    async fn shell_process_streams_output_and_reports_exit_status() {
        let workspace = tempdir().expect("workspace");
        let mut processes = super::ShellProcesses::new();
        processes.spawn(
            workspace.path().to_path_buf(),
            "shell:1".to_owned(),
            failing_command().to_owned(),
        );

        let mut streamed = false;
        loop {
            match processes.events.recv().await.expect("shell event") {
                super::ShellEvent::Output { output, .. } => {
                    streamed = true;
                    assert!(output.contains("first") || output.contains("second"));
                }
                super::ShellEvent::Finished {
                    output, exit_code, ..
                } => {
                    assert!(streamed);
                    assert!(output.contains("first"));
                    assert!(output.contains("second"));
                    assert_eq!(exit_code, Some(7));
                    break;
                }
                super::ShellEvent::Failed { message, .. } => panic!("shell failed: {message}"),
            }
        }
        processes.shutdown().await;
    }

    #[tokio::test]
    async fn one_supervised_shell_process_can_be_cancelled_by_id() {
        let workspace = tempdir().expect("workspace");
        let mut processes = super::ShellProcesses::new();
        processes.spawn(
            workspace.path().to_path_buf(),
            "shell:cancel".to_owned(),
            long_running_command().to_owned(),
        );

        assert!(processes.cancel("shell:cancel"));
        let event = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                match processes.events.recv().await.expect("shell event") {
                    event @ super::ShellEvent::Finished { .. } => break event,
                    super::ShellEvent::Output { .. } => {}
                    super::ShellEvent::Failed { message, .. } => panic!("shell failed: {message}"),
                }
            }
        })
        .await
        .expect("cancelled shell exits promptly");
        assert!(matches!(
            event,
            super::ShellEvent::Finished {
                id,
                interrupted: true,
                ..
            } if id == "shell:cancel"
        ));
        processes.complete("shell:cancel");
        processes.shutdown().await;
    }
}
