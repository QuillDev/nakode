use std::{
    io::{self, Write},
    path::Path,
    time::Duration,
};

use crossterm::event::EventStream;
use futures_util::StreamExt;
use nakode_sdk::{HydratedSession, NakodeClient, SdkError, SessionAttachment, Watch, v1 as api};
use thiserror::Error;
use tokio::time::MissedTickBehavior;

use crate::{
    api_projection, clipboard,
    config::Config,
    native_client, render,
    terminal::{TerminalSession, Tui},
    tui_client::TuiClientState,
};

#[derive(Debug, Error)]
pub enum AppError {
    #[error("terminal error: {0}")]
    Terminal(#[from] io::Error),
    #[error("failed to locate the running Nakode executable: {0}")]
    CurrentExecutable(#[source] io::Error),
    #[error("failed to start the native Nakode client: {0}")]
    NativeClientStart(String),
    #[error(transparent)]
    Sdk(#[from] SdkError),
    #[error("Nakode API returned an invalid projection: {0}")]
    Projection(String),
}

/// Runs the interactive renderer until the user exits or a subsystem fails.
///
/// # Errors
/// Returns an error when SDK connection, signal handling, projection, or
/// terminal ownership fails.
#[allow(clippy::large_futures)]
pub async fn run(config: Config) -> Result<(), AppError> {
    let nakode_executable = std::env::current_exe().map_err(AppError::CurrentExecutable)?;
    let client = native_client::connect(&config)
        .await
        .map_err(|error| AppError::NativeClientStart(error.to_string()))?;
    let (workspace, session) = prepare(&client, &config).await?;
    let workspace_id = workspace.workspace_id.clone();
    let session_id = session.state.id.clone();
    let mut workspace_updates = client.watch_workspace(workspace_id);
    let mut session_updates = client.watch_attached_hydrated_session(
        session_id,
        config.scrollback,
        SessionAttachment::default(),
    );

    let mut bootstrap = api_projection::workspace(workspace).map_err(AppError::Projection)?;
    bootstrap.active_session = None;
    let mut host = TuiClientState::new(client.clone(), bootstrap, config.scrollback);
    host.install_session(session)
        .map_err(AppError::Projection)?;
    let mut signals = ShutdownSignals::install().map_err(AppError::Terminal)?;
    let mut terminal = TerminalSession::enter().map_err(AppError::Terminal)?;
    let mut image_renderer = crate::terminal_image::TerminalImageRenderer::detect(
        host.projection().terminal_image_mode(),
    );
    host.projection_mut()
        .set_image_previews_enabled(image_renderer.is_some());
    let mut herdr = crate::herdr::Reporter::from_environment();

    let loop_result = run_loop(RunLoopContext {
        terminal: terminal.terminal_mut(),
        host: &mut host,
        client: &client,
        workspace_updates: &mut workspace_updates,
        session_updates: &mut session_updates,
        signals: &mut signals,
        image_renderer: image_renderer.as_mut(),
        herdr: herdr.as_mut(),
        scrollback: config.scrollback,
    })
    .await;

    if let Some(reporter) = herdr {
        reporter.shutdown().await;
    }
    let resume_session = host.resumable_session_id();
    let restore_result = terminal.restore();
    loop_result?;
    restore_result.map_err(AppError::Terminal)?;
    write_resume_hint(
        &mut io::stdout().lock(),
        &nakode_executable,
        resume_session.as_deref(),
    )
    .map_err(AppError::Terminal)
}

async fn prepare(
    client: &NakodeClient,
    config: &Config,
) -> Result<(api::WorkspaceState, HydratedSession), AppError> {
    let mut workspace = client
        .get_workspace(config.workspace.to_string_lossy(), None)
        .await?;
    let session_id = if let Some(requested) = config.resume.clone() {
        client.open_session(requested).await?
    } else {
        client
            .create_session_in_directory(
                workspace.workspace_id.clone(),
                None,
                config.workspace.to_string_lossy(),
            )
            .await?
    };
    if let Some(model_id) = &config.model {
        let reasoning_effort = workspace
            .models
            .iter()
            .find(|model| model.id == *model_id)
            .and_then(|model| model.configuration.as_ref())
            .filter(|configuration| {
                configuration
                    .reasoning_efforts
                    .contains(&config.openai_reasoning_effort.as_str().to_owned())
            })
            .map(|_| config.openai_reasoning_effort.as_str().to_owned());
        client
            .select_model(api::SelectModelRequest {
                mutation: None,
                target: Some(api::ModelTarget {
                    target: Some(api::model_target::Target::SessionId(session_id.clone())),
                }),
                model_id: model_id.clone(),
                options: Some(api::ModelOptions {
                    reasoning_effort,
                    fast_mode: false,
                }),
            })
            .await?;
    }
    let session = client
        .get_hydrated_session(session_id, config.scrollback)
        .await?;
    workspace.active_session = None;
    Ok((workspace, session))
}

fn write_resume_hint(
    output: &mut impl Write,
    executable: &Path,
    session_id: Option<&str>,
) -> io::Result<()> {
    let Some(session_id) = session_id else {
        return Ok(());
    };
    writeln!(output, "\nResume this session with:")?;
    writeln!(
        output,
        "  {} --tui --resume {session_id}",
        quote_command_argument(executable),
    )
}

fn quote_command_argument(argument: &Path) -> String {
    let argument = argument.to_string_lossy();
    if argument
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || "_@%+=:,./-".contains(character))
    {
        return argument.into_owned();
    }
    #[cfg(unix)]
    {
        format!("'{}'", argument.replace('\'', "'\\''"))
    }
    #[cfg(not(unix))]
    {
        format!("\"{}\"", argument.replace('"', "\\\""))
    }
}

struct RunLoopContext<'a> {
    terminal: &'a mut Tui,
    host: &'a mut TuiClientState,
    client: &'a NakodeClient,
    workspace_updates: &'a mut Watch<api::WorkspaceState>,
    session_updates: &'a mut Watch<HydratedSession>,
    signals: &'a mut ShutdownSignals,
    image_renderer: Option<&'a mut crate::terminal_image::TerminalImageRenderer>,
    herdr: Option<&'a mut crate::herdr::Reporter>,
    scrollback: usize,
}

async fn run_loop(context: RunLoopContext<'_>) -> Result<(), AppError> {
    let RunLoopContext {
        terminal,
        host,
        client,
        workspace_updates,
        session_updates,
        signals,
        mut image_renderer,
        mut herdr,
        scrollback,
    } = context;
    let mut input = EventStream::new();
    let mut render_tick = tokio::time::interval(Duration::from_millis(33));
    render_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut dirty = true;
    let mut workspace_watch_open = true;
    let mut session_watch_open = true;
    if let Some(reporter) = &mut herdr {
        reporter.sync(host.projection());
    }

    loop {
        tokio::select! {
            input_event = input.next() => match input_event {
                Some(Ok(event)) => { host.handle_terminal(event); flush_pending_clipboard(terminal, host); dirty = true; }
                Some(Err(error)) => return Err(AppError::Terminal(error)),
                None => break,
            },
            update = workspace_updates.next(), if workspace_watch_open => match update {
                Some(Ok(workspace)) => { if host.install_workspace(workspace).map_err(AppError::Projection)? { crate::terminal::ring_bell(terminal.backend_mut())?; } dirty = true; }
                Some(Err(error)) => { host.connection_status(error.to_string()); dirty = true; }
                None => {
                    workspace_watch_open = false;
                    host.connection_status("Workspace service disconnected.".to_owned());
                    dirty = true;
                }
            },
            update = session_updates.next(), if session_watch_open => match update {
                Some(Ok(session)) => { if host.install_session(session).map_err(AppError::Projection)? { crate::terminal::ring_bell(terminal.backend_mut())?; } dirty = true; }
                Some(Err(error)) => { host.connection_status(error.to_string()); dirty = true; }
                None => {
                    session_watch_open = false;
                    host.connection_status("Session service disconnected.".to_owned());
                    dirty = true;
                }
            },
            () = signals.recv() => break,
            _ = render_tick.tick() => {
                if dirty || host.projection().is_busy() {
                    terminal.draw(|frame| render::draw_with_images(frame, host.projection_mut(), image_renderer.as_deref_mut()))?;
                    dirty = false;
                }
            }
        }
        dirty |= host.drain_command_results();
        if let Some(session_id) = host.take_requested_session() {
            let hydrated = client
                .get_hydrated_session(session_id.to_string(), scrollback)
                .await?;
            host.install_session(hydrated)
                .map_err(AppError::Projection)?;
            *session_updates = client.watch_attached_hydrated_session(
                session_id.to_string(),
                scrollback,
                SessionAttachment::default(),
            );
            session_watch_open = true;
            dirty = true;
        }
        if let Some(reporter) = &mut herdr {
            reporter.sync(host.projection());
        }
        if host.should_quit() {
            break;
        }
    }
    Ok(())
}

fn flush_pending_clipboard(terminal: &mut Tui, host: &mut TuiClientState) {
    let Some(text) = host.take_pending_clipboard() else {
        return;
    };
    let inside_tmux = std::env::var_os("TMUX").is_some();
    match clipboard::write_osc52(terminal.backend_mut(), &text, inside_tmux) {
        Ok(bytes) => {
            host.connection_status(format!("Copied selection to clipboard ({bytes} bytes)."));
        }
        Err(error) => host.connection_status(format!("Could not copy selection: {error}")),
    }
}

#[cfg(unix)]
struct ShutdownSignals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
    hangup: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl ShutdownSignals {
    fn install() -> io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
            hangup: signal(SignalKind::hangup())?,
        })
    }
    async fn recv(&mut self) {
        tokio::select! { _ = self.interrupt.recv() => {}, _ = self.terminate.recv() => {}, _ = self.hangup.recv() => {} }
    }
}

#[cfg(not(unix))]
struct ShutdownSignals;

#[cfg(not(unix))]
impl ShutdownSignals {
    fn install() -> io::Result<Self> {
        Ok(Self)
    }
    async fn recv(&mut self) {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn exit_hint_includes_full_session_id() {
        let mut output = Vec::new();
        super::write_resume_hint(
            &mut output,
            std::path::Path::new("/opt/Nakode/nakode"),
            Some("session-1"),
        )
        .expect("write hint");
        let output = String::from_utf8(output).expect("UTF-8");
        assert!(output.contains("--tui --resume session-1"));
        assert!(!output.contains("--workspace"));
    }
}
