use std::{collections::HashMap, ffi::OsString, path::Path, process::Stdio, time::Duration};

#[cfg(unix)]
use std::os::unix::process::CommandExt;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
};
use tokio_util::sync::CancellationToken;

use super::truncate_output;

pub struct ProcessRequest<'a> {
    pub program: &'a str,
    pub arguments: &'a [OsString],
    pub input: Option<&'a str>,
    pub environment: Option<&'a HashMap<String, String>>,
    pub timeout: Option<Duration>,
}

pub struct ProcessResult {
    pub output: String,
    pub success: bool,
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    pub interrupted: bool,
}

pub struct CapturedProcessResult {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub success: bool,
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    pub interrupted: bool,
}

pub async fn run_process(
    workspace: &Path,
    request: ProcessRequest<'_>,
    cancellation: &CancellationToken,
) -> Result<ProcessResult, String> {
    let captured = capture_process(workspace, request, cancellation).await?;
    let mut output = captured.stdout;
    output.extend(captured.stderr);
    Ok(ProcessResult {
        output: truncate_output(output),
        success: captured.success,
        exit_code: captured.exit_code,
        timed_out: captured.timed_out,
        interrupted: captured.interrupted,
    })
}

pub async fn capture_process(
    workspace: &Path,
    request: ProcessRequest<'_>,
    cancellation: &CancellationToken,
) -> Result<CapturedProcessResult, String> {
    let mut command = Command::new(request.program);
    command
        .args(request.arguments)
        .current_dir(workspace)
        .kill_on_drop(true)
        .stdin(if request.input.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(environment) = request.environment {
        command.envs(environment);
    }
    configure_process_group(&mut command);
    let mut child = command
        .spawn()
        .map_err(|error| format!("failed to start {}: {error}", request.program))?;
    let process_id = child.id();
    if let Some(input) = request.input
        && let Some(mut stdin) = child.stdin.take()
    {
        stdin
            .write_all(input.as_bytes())
            .await
            .map_err(|error| format!("failed to write process input: {error}"))?;
    }
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "process stdout was not captured".to_owned())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "process stderr was not captured".to_owned())?;
    let stdout_task = tokio::spawn(read_stream(stdout));
    let stderr_task = tokio::spawn(read_stream(stderr));
    let mut timed_out = false;
    let mut interrupted = false;
    let status = tokio::select! {
        status = child.wait() => status.map_err(|error| format!("failed to wait for process: {error}"))?,
        () = cancellation.cancelled() => {
            interrupted = true;
            terminate_process_tree(&mut child, process_id, "interrupt")?;
            child.wait().await.map_err(|error| format!("failed to reap interrupted process: {error}"))?
        }
        () = timeout(request.timeout) => {
            timed_out = true;
            terminate_process_tree(&mut child, process_id, "stop timed-out")?;
            child.wait().await.map_err(|error| format!("failed to reap timed-out process: {error}"))?
        }
    };
    let stdout = stdout_task
        .await
        .map_err(|error| format!("stdout reader failed: {error}"))??;
    let stderr = stderr_task
        .await
        .map_err(|error| format!("stderr reader failed: {error}"))??;
    Ok(CapturedProcessResult {
        stdout,
        stderr,
        success: status.success() && !timed_out && !interrupted,
        exit_code: status.code(),
        timed_out,
        interrupted,
    })
}

async fn timeout(duration: Option<Duration>) {
    match duration {
        Some(duration) => tokio::time::sleep(duration).await,
        None => std::future::pending().await,
    }
}

#[cfg(unix)]
fn configure_process_group(command: &mut Command) {
    command.as_std_mut().process_group(0);
}

#[cfg(not(unix))]
fn configure_process_group(_command: &mut Command) {}

#[cfg(unix)]
fn terminate_process_tree(
    child: &mut tokio::process::Child,
    process_id: Option<u32>,
    operation: &str,
) -> Result<(), String> {
    use nix::{sys::signal, unistd::Pid};

    let Some(process_id) = process_id.and_then(|id| i32::try_from(id).ok()) else {
        return child
            .start_kill()
            .map_err(|error| format!("failed to {operation} process: {error}"));
    };
    signal::killpg(Pid::from_raw(process_id), signal::Signal::SIGKILL)
        .map_err(|error| format!("failed to {operation} process group: {error}"))
}

#[cfg(not(unix))]
fn terminate_process_tree(
    child: &mut tokio::process::Child,
    _process_id: Option<u32>,
    operation: &str,
) -> Result<(), String> {
    child
        .start_kill()
        .map_err(|error| format!("failed to {operation} process: {error}"))
}

async fn read_stream(mut stream: impl tokio::io::AsyncRead + Unpin) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    stream
        .read_to_end(&mut bytes)
        .await
        .map_err(|error| format!("failed to read process output: {error}"))?;
    Ok(bytes)
}
