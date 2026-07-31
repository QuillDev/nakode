//! TUI controller over the public high-level SDK.
//!
//! This type owns only terminal presentation mechanics. Canonical state,
//! command idempotency, reconnect, paging, hydration, and execution remain in
//! the server/SDK boundary.

use std::{future::Future, pin::Pin};

use crossterm::event::Event;
use futures_util::{FutureExt, StreamExt, stream::FuturesUnordered};
use nakode_protocol::SessionId;
use nakode_sdk::{HydratedSession, NakodeClient, SdkError, v1 as api};

use crate::{
    api_projection::{self, TuiAction},
    tui_input::{CommandFollowup, CommandIntent, DeviceIntent},
    tui_state::{ComposerDraft, TuiState},
};

type PendingCommand =
    Pin<Box<dyn Future<Output = (RetainedCommand, Result<api::MutationResult, SdkError>)> + Send>>;

struct RetainedCommand {
    command: TuiAction,
    restore: Option<ComposerDraft>,
    followup: Option<CommandFollowup>,
}

pub(crate) struct TuiClientState {
    client: NakodeClient,
    bootstrap: nakode_protocol::BootstrapView,
    projection: TuiState,
    pending: FuturesUnordered<PendingCommand>,
    requested_session: Option<SessionId>,
    pending_clipboard: Option<String>,
    local_status: Option<String>,
    should_quit: bool,
}

impl TuiClientState {
    #[must_use]
    pub(crate) fn new(
        client: NakodeClient,
        bootstrap: nakode_protocol::BootstrapView,
        scrollback: usize,
    ) -> Self {
        Self {
            client,
            projection: TuiState::from_bootstrap(&bootstrap, scrollback),
            bootstrap,
            pending: FuturesUnordered::new(),
            requested_session: None,
            pending_clipboard: None,
            local_status: None,
            should_quit: false,
        }
    }

    #[must_use]
    pub(crate) const fn projection(&self) -> &TuiState {
        &self.projection
    }

    pub(crate) const fn projection_mut(&mut self) -> &mut TuiState {
        &mut self.projection
    }

    #[must_use]
    pub(crate) const fn should_quit(&self) -> bool {
        self.should_quit
    }

    #[must_use]
    pub(crate) fn resumable_session_id(&self) -> Option<String> {
        let active = self.bootstrap.active_session.as_ref()?;
        self.bootstrap
            .sessions
            .iter()
            .any(|session| session.id == active.id && session.updated_at_ms > 0)
            .then(|| active.id.to_string())
    }

    #[must_use]
    pub(crate) fn workspace_path(&self) -> &str {
        &self.bootstrap.workspace_path
    }

    /// Replaces the workspace projection from one authoritative SDK snapshot.
    pub(crate) fn install_workspace(
        &mut self,
        workspace: api::WorkspaceState,
    ) -> Result<bool, String> {
        let mut next = api_projection::workspace(workspace)?;
        next.active_session = self.bootstrap.active_session.take();
        let should_chime = turn_finished(&self.bootstrap, &next);
        self.bootstrap = next;
        self.local_status = None;
        self.projection.install_bootstrap(&self.bootstrap);
        Ok(should_chime)
    }

    /// Replaces the session projection and installs SDK-hydrated artifacts.
    pub(crate) fn install_session(&mut self, hydrated: HydratedSession) -> Result<bool, String> {
        let artifacts = hydrated
            .artifacts
            .into_values()
            .map(api_projection::artifact)
            .collect::<Vec<_>>();
        let session = api_projection::session(hydrated.state)?;
        let mut next = self.bootstrap.clone();
        next.active_session = Some(session.clone());
        let should_chime = turn_finished(&self.bootstrap, &next);
        self.bootstrap = next;
        self.local_status = None;
        self.projection.install_bootstrap(&self.bootstrap);
        for entry in &session.transcript.entries {
            let entry_artifacts = artifacts
                .iter()
                .filter(|artifact| entry.artifacts.contains(&artifact.id))
                .cloned()
                .collect::<Vec<_>>();
            if !entry_artifacts.is_empty() {
                self.projection.install_session_entry_artifacts(
                    &session.id,
                    &entry.id,
                    &entry_artifacts,
                );
            }
        }
        for run in &session.runs {
            for entry in &run.transcript.entries {
                let entry_artifacts = artifacts
                    .iter()
                    .filter(|artifact| entry.artifacts.contains(&artifact.id))
                    .cloned()
                    .collect::<Vec<_>>();
                if !entry_artifacts.is_empty() {
                    self.projection.install_run_entry_artifacts(
                        &session.id,
                        &run.id,
                        &entry.id,
                        &entry_artifacts,
                    );
                }
            }
        }
        Ok(should_chime)
    }

    pub(crate) fn connection_status(&mut self, message: impl Into<String>) {
        self.local_status = Some(message.into());
        self.apply_local_status();
    }

    pub(crate) fn handle_terminal(&mut self, event: Event) {
        let previous_status = self.projection.status_message.clone();
        let outcome =
            crate::tui_input::handle_terminal(&mut self.projection, &self.bootstrap, event);
        let status = (self.projection.status_message != previous_status)
            .then(|| self.projection.status_message.clone());
        self.should_quit |= outcome.quit;
        if let Some(status) = status {
            self.local_status = Some(status);
        }
        self.apply_local_status();
        self.dispatch(outcome.commands);
        for device in outcome.devices {
            match device {
                DeviceIntent::OpenUrl(url) => self.open_url(&url),
                DeviceIntent::Copy(text) => self.pending_clipboard = Some(text),
            }
        }
    }

    pub(crate) fn drain_command_results(&mut self) -> bool {
        let mut changed = false;
        while let Some((retained, result)) = self.pending.next().now_or_never().flatten() {
            self.apply_command_result(retained, result);
            changed = true;
        }
        changed
    }

    pub(crate) fn take_requested_session(&mut self) -> Option<SessionId> {
        self.requested_session.take()
    }

    pub(crate) fn take_pending_clipboard(&mut self) -> Option<String> {
        self.pending_clipboard.take()
    }

    fn apply_command_result(
        &mut self,
        retained: RetainedCommand,
        result: Result<api::MutationResult, SdkError>,
    ) {
        match result {
            Ok(receipt) => {
                if let Some(resource_id) = receipt.resource_id {
                    match retained.followup {
                        Some(CommandFollowup::SelectResourceSession) => {
                            self.requested_session = Some(SessionId::from(resource_id));
                        }
                        Some(CommandFollowup::AgentSaved) => {
                            self.connection_status(format!("Saved agent {resource_id}."));
                        }
                        None => {}
                    }
                }
            }
            Err(error) => {
                if let Some(draft) = retained.restore {
                    self.restore_draft(draft);
                }
                self.connection_status(error.to_string());
            }
        }
    }

    fn restore_draft(&mut self, draft: ComposerDraft) {
        if self.projection.client.editor.is_blank() {
            self.projection.client.restore_composer(draft);
        }
    }

    fn dispatch(&mut self, commands: Vec<CommandIntent>) {
        for intent in commands {
            let retained = RetainedCommand {
                command: intent.command,
                restore: intent.restore,
                followup: intent.followup,
            };
            let client = self.client.clone();
            let command = retained.command.clone();
            self.pending.push(Box::pin(async move {
                let result = api_projection::execute_command(&client, command).await;
                (retained, result)
            }));
        }
    }

    fn open_url(&mut self, url: &str) {
        match open::that(url) {
            Ok(()) => self.connection_status("Opened the authentication page."),
            Err(error) => self.connection_status(format!("Could not open the page: {error}")),
        }
    }

    fn apply_local_status(&mut self) {
        if let Some(status) = &self.local_status {
            self.projection.set_status(status);
        }
    }
}

fn turn_finished(
    before: &nakode_protocol::BootstrapView,
    after: &nakode_protocol::BootstrapView,
) -> bool {
    let before = before
        .active_session
        .as_ref()
        .and_then(|session| session.active_turn.as_ref());
    let after = after
        .active_session
        .as_ref()
        .and_then(|session| session.active_turn.as_ref());
    before.is_some_and(|turn| {
        matches!(
            turn.status,
            nakode_protocol::TurnStatus::Starting
                | nakode_protocol::TurnStatus::Running
                | nakode_protocol::TurnStatus::Cancelling
        )
    }) && after.is_none_or(|turn| {
        !matches!(
            turn.status,
            nakode_protocol::TurnStatus::Starting
                | nakode_protocol::TurnStatus::Running
                | nakode_protocol::TurnStatus::Cancelling
        )
    })
}
