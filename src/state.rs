use std::collections::{HashMap, VecDeque};

pub use crate::backend::ApprovalDecision;

use crate::{
    backend::{
        ApprovalRequest, BackendCapabilities, BackendCommand, BackendEvent, BackendOperation,
        CODEX_PROVIDER, DeltaKind, ItemKind, ItemStatus, ModelInfo, NormalizedItem,
        SessionHistoryItem, TurnOutcome,
    },
    editor::EditorState,
    selection::{ScreenPoint, ScreenSnapshot, TextSelection},
    session::{ProviderRecord, SessionRecord},
    transcript::{EntryKind, EntryStatus, Transcript},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectionState {
    Starting,
    Ready { server: String },
    Failed(String),
    Disconnected(String),
}

impl ConnectionState {
    #[must_use]
    pub fn label(&self) -> &str {
        match self {
            Self::Starting => "connecting",
            Self::Ready { .. } => "ready",
            Self::Failed(_) => "failed",
            Self::Disconnected(_) => "disconnected",
        }
    }

    #[must_use]
    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready { .. })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActiveTurn {
    pub id: String,
    pub model: Option<String>,
    pub cancelling: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueuedPrompt {
    pub id: String,
    pub text: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OutgoingPrompt {
    id: String,
    text: String,
    model: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingSteer {
    id: String,
    text: String,
    turn_id: String,
    editor_revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelPicker {
    pub filter: String,
    pub selected: usize,
}

#[derive(Clone, Debug)]
pub struct SessionPicker {
    pub sessions: Vec<SessionRecord>,
    pub selected: usize,
    pub loading: bool,
}

#[derive(Clone, Debug)]
pub struct ProviderPicker {
    pub providers: Vec<ProviderRecord>,
    pub selected: usize,
    pub loading: bool,
}

#[derive(Clone, Debug)]
struct ProviderContext {
    name: String,
    capabilities: BackendCapabilities,
    connection: ConnectionState,
    provider_session_id: Option<String>,
    session_id: Option<String>,
}

#[derive(Clone, Debug)]
pub enum Effect {
    Backend(BackendCommand),
    ListSessions,
    ListProviders,
    SetProviderEnabled {
        provider: String,
        enabled: bool,
    },
    ResolveSession(String),
    PersistSession {
        provider: String,
        provider_session_id: String,
        workspace: String,
        title: String,
        model: Option<String>,
    },
    PersistModels {
        provider: String,
        models: Vec<ModelInfo>,
    },
    UpdateSessionModel {
        session_id: String,
        model: Option<String>,
    },
    TouchSession(String),
    Quit,
}

#[derive(Debug)]
pub struct AppState {
    pub connection: ConnectionState,
    pub workspace: String,
    pub backend_provider: String,
    pub backend_name: String,
    pub backend_capabilities: BackendCapabilities,
    provider_contexts: HashMap<String, ProviderContext>,
    pub provider_session_id: Option<String>,
    pub session_id: Option<String>,
    pub active_turn: Option<ActiveTurn>,
    pub editor: EditorState,
    pub transcript: Transcript,
    pub queue: VecDeque<QueuedPrompt>,
    pub queue_selection: Option<usize>,
    pub models: Vec<ModelInfo>,
    pub selected_model: Option<String>,
    pub model_picker: Option<ModelPicker>,
    pub session_picker: Option<SessionPicker>,
    pub provider_picker: Option<ProviderPicker>,
    pending_model_picker: Option<()>,
    pub show_help: bool,
    pub text_selection: Option<TextSelection>,
    pub approvals: VecDeque<ApprovalRequest>,
    pub scroll_from_bottom: usize,
    pub status_message: String,
    pub diagnostic_count: usize,
    pub should_quit: bool,
    creating_session: Option<()>,
    pending_session_prompt: Option<OutgoingPrompt>,
    starting_turn: Option<OutgoingPrompt>,
    pending_steer: Option<PendingSteer>,
    resuming_session: Option<SessionRecord>,
    startup_resume: Option<String>,
    item_turns: HashMap<String, String>,
    initial_model: Option<String>,
    next_local_id: u64,
    screen_snapshot: Option<ScreenSnapshot>,
    pending_clipboard: Option<String>,
}

impl AppState {
    pub fn set_status(&mut self, message: &str) {
        self.status_message.clear();
        self.status_message.push_str(message);
    }

    pub fn new(
        workspace: impl Into<String>,
        initial_model: Option<String>,
        scrollback: usize,
    ) -> Self {
        Self::new_for_backend(
            workspace,
            initial_model,
            scrollback,
            CODEX_PROVIDER,
            "Codex",
        )
    }

    pub fn new_for_backend(
        workspace: impl Into<String>,
        initial_model: Option<String>,
        scrollback: usize,
        provider: impl Into<String>,
        backend_name: impl Into<String>,
    ) -> Self {
        let backend_name = backend_name.into();
        let mut transcript = Transcript::new(scrollback);
        transcript.upsert(
            "flock:startup",
            EntryKind::System,
            "FLOCK",
            format!("Starting {backend_name}…"),
            EntryStatus::Running,
        );
        let provider = provider.into();
        let mut provider_contexts = HashMap::new();
        provider_contexts.insert(
            provider.clone(),
            ProviderContext {
                name: backend_name.clone(),
                capabilities: BackendCapabilities::default(),
                connection: ConnectionState::Starting,
                provider_session_id: None,
                session_id: None,
            },
        );
        Self {
            connection: ConnectionState::Starting,
            workspace: workspace.into(),
            backend_provider: provider,
            backend_name: backend_name.clone(),
            backend_capabilities: BackendCapabilities::default(),
            provider_contexts,
            provider_session_id: None,
            session_id: None,
            active_turn: None,
            editor: EditorState::default(),
            transcript,
            queue: VecDeque::new(),
            queue_selection: None,
            models: Vec::new(),
            selected_model: initial_model.clone(),
            model_picker: None,
            session_picker: None,
            provider_picker: None,
            pending_model_picker: None,
            show_help: false,
            text_selection: None,
            approvals: VecDeque::new(),
            scroll_from_bottom: 0,
            status_message: format!("Connecting to {backend_name}…"),
            diagnostic_count: 0,
            should_quit: false,
            creating_session: None,
            pending_session_prompt: None,
            starting_turn: None,
            pending_steer: None,
            resuming_session: None,
            startup_resume: None,
            item_turns: HashMap::new(),
            initial_model,
            next_local_id: 1,
            screen_snapshot: None,
            pending_clipboard: None,
        }
    }

    pub fn set_startup_resume(&mut self, session_id: Option<String>) {
        self.startup_resume = session_id;
    }

    pub fn install_sessions(&mut self, sessions: Vec<SessionRecord>) {
        let picker = self.session_picker.get_or_insert(SessionPicker {
            sessions: Vec::new(),
            selected: 0,
            loading: false,
        });
        picker.sessions = sessions;
        picker.selected = 0;
        picker.loading = false;
        self.status_message = if picker.sessions.is_empty() {
            "No saved sessions for this workspace.".to_owned()
        } else {
            format!("{} saved session(s).", picker.sessions.len())
        };
    }

    pub fn session_store_failed(&mut self, message: impl Into<String>) {
        if let Some(picker) = &mut self.session_picker {
            picker.loading = false;
        }
        self.resuming_session = None;
        self.status_message = format!("Session error: {}", message.into());
    }

    pub fn install_providers(&mut self, providers: Vec<ProviderRecord>) {
        let picker = self.provider_picker.get_or_insert(ProviderPicker {
            providers: Vec::new(),
            selected: 0,
            loading: false,
        });
        picker.providers = providers;
        picker.selected = 0;
        picker.loading = false;
    }

    pub fn provider_picker_move(&mut self, delta: isize) {
        let Some(picker) = &mut self.provider_picker else {
            return;
        };
        if picker.providers.is_empty() {
            return;
        }
        picker.selected = offset_index(picker.selected, picker.providers.len(), delta);
    }

    pub fn close_provider_picker(&mut self) {
        self.provider_picker = None;
        self.set_status("Provider settings closed.");
    }

    pub fn toggle_provider(&mut self) -> Vec<Effect> {
        let Some(picker) = &mut self.provider_picker else {
            return Vec::new();
        };
        let Some(provider) = picker.providers.get_mut(picker.selected) else {
            return Vec::new();
        };
        provider.enabled = !provider.enabled;
        self.status_message = format!(
            "{} {}.",
            provider.display_name,
            if provider.enabled {
                "enabled"
            } else {
                "disabled"
            }
        );
        vec![Effect::SetProviderEnabled {
            provider: provider.provider.clone(),
            enabled: provider.enabled,
        }]
    }

    pub fn session_persisted(&mut self, session: &SessionRecord) {
        self.session_id = Some(session.id.clone());
        self.status_message = format!("Session {} started.", short_id(&session.id));
    }

    pub fn session_picker_move(&mut self, delta: isize) {
        let Some(picker) = &mut self.session_picker else {
            return;
        };
        if picker.sessions.is_empty() {
            return;
        }
        picker.selected = picker
            .selected
            .saturating_add_signed(delta)
            .min(picker.sessions.len() - 1);
    }

    pub fn close_session_picker(&mut self) {
        self.session_picker = None;
        self.set_status("Session selection cancelled.");
    }

    pub fn select_session(&mut self) -> Vec<Effect> {
        let session = self
            .session_picker
            .as_ref()
            .and_then(|picker| picker.sessions.get(picker.selected))
            .cloned();
        let Some(session) = session else {
            self.set_status("No session is selected.");
            return Vec::new();
        };
        self.begin_resume(session)
    }

    pub fn begin_resume(&mut self, session: SessionRecord) -> Vec<Effect> {
        if self.is_busy() {
            self.set_status("Cannot switch sessions while a turn is active.");
            return Vec::new();
        }
        if session.workspace != self.workspace {
            self.set_status("That session belongs to a different workspace.");
            return Vec::new();
        }
        if !self.activate_provider(&session.provider) {
            return Vec::new();
        }
        if !self.backend_capabilities.resume.is_supported() {
            self.status_message = format!("{} does not support session resume.", self.backend_name);
            return Vec::new();
        }
        let old_provider_session = self.provider_session_id.clone();
        self.resuming_session = Some(session.clone());
        self.session_picker = None;
        self.status_message = format!("Resuming session {}…", short_id(&session.id));
        let mut effects = Vec::new();
        if let Some(provider_session_id) =
            old_provider_session.filter(|current| current != &session.provider_session_id)
        {
            effects.push(Effect::Backend(BackendCommand::UnsubscribeSession {
                provider_session_id,
            }));
        }
        effects.push(Effect::Backend(BackendCommand::ResumeSession {
            provider_session_id: session.provider_session_id,
        }));
        effects
    }

    pub fn begin_text_selection(&mut self, point: ScreenPoint) {
        self.text_selection = Some(TextSelection::new(point));
        self.pending_clipboard = None;
    }

    pub fn update_text_selection(&mut self, point: ScreenPoint) {
        if let Some(selection) = &mut self.text_selection {
            selection.update(point);
        }
    }

    pub fn finish_text_selection(&mut self, point: ScreenPoint) {
        self.update_text_selection(point);
        self.pending_clipboard = self
            .text_selection
            .filter(|selection| selection.is_range())
            .and_then(|selection| {
                self.screen_snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.selected_text(selection))
            });
    }

    pub fn clear_text_selection(&mut self) {
        self.text_selection = None;
        self.pending_clipboard = None;
    }

    pub fn set_screen_snapshot(&mut self, snapshot: ScreenSnapshot) {
        self.screen_snapshot = Some(snapshot);
    }

    pub fn take_pending_clipboard(&mut self) -> Option<String> {
        self.pending_clipboard.take()
    }

    pub fn clipboard_copied(&mut self, bytes: usize) {
        self.status_message = format!("Copied selection to clipboard ({bytes} bytes).");
    }

    pub fn clipboard_failed(&mut self, error: &str) {
        self.status_message = format!("Could not copy selection: {error}");
    }

    #[must_use]
    pub fn is_busy(&self) -> bool {
        self.creating_session.is_some()
            || self.starting_turn.is_some()
            || self.active_turn.is_some()
    }

    pub fn submit_editor(&mut self) -> Vec<Effect> {
        if self.editor.is_blank() {
            self.set_status("Write a message before sending.");
            return Vec::new();
        }
        if !self.connection.is_ready() {
            self.set_status("The backend is not ready; the draft was preserved.");
            return Vec::new();
        }

        let command = self.editor.text().trim().to_owned();
        if command == "/new" {
            self.editor.clear();
            return self.new_session();
        }
        if command == "/providers" {
            self.editor.clear();
            self.provider_picker = Some(ProviderPicker {
                providers: Vec::new(),
                selected: 0,
                loading: true,
            });
            self.set_status("Loading providers…");
            return vec![Effect::ListProviders];
        }
        if command == "/reload" {
            self.editor.clear();
            return self.reload_backend();
        }
        if command == "/resume" {
            if self.is_busy() {
                self.set_status("Cannot switch sessions while a turn is active.");
                return Vec::new();
            }
            self.editor.clear();
            self.session_picker = Some(SessionPicker {
                sessions: Vec::new(),
                selected: 0,
                loading: true,
            });
            self.set_status("Loading sessions…");
            return vec![Effect::ListSessions];
        }
        if let Some(id) = command.strip_prefix("/resume ").map(str::trim)
            && !id.is_empty()
        {
            if self.is_busy() {
                self.set_status("Cannot switch sessions while a turn is active.");
                return Vec::new();
            }
            self.editor.clear();
            self.status_message = format!("Looking up session {id}…");
            return vec![Effect::ResolveSession(id.to_owned())];
        }

        if self.is_busy() {
            self.enqueue_editor()
        } else {
            let prompt = self.take_editor_prompt();
            self.begin_prompt(prompt)
        }
    }

    fn reload_backend(&mut self) -> Vec<Effect> {
        if self.is_busy() {
            self.set_status("Cannot reload while a turn is active.");
            return Vec::new();
        }
        if self
            .backend_capabilities
            .models_require_session
            .is_supported()
            && self.provider_session_id.is_none()
        {
            self.creating_session = Some(());
        }
        self.status_message = format!("Reloading {} metadata…", self.backend_name);
        vec![Effect::Backend(BackendCommand::Reload {
            session_id: self.provider_session_id.clone(),
        })]
    }

    fn new_session(&mut self) -> Vec<Effect> {
        if self.is_busy() {
            self.set_status("Cannot start a new session while a turn is active.");
            return Vec::new();
        }
        let previous = self.provider_session_id.take();
        self.session_id = None;
        self.active_turn = None;
        self.creating_session = None;
        self.pending_session_prompt = None;
        self.starting_turn = None;
        self.pending_steer = None;
        self.resuming_session = None;
        self.item_turns.clear();
        self.approvals.clear();
        self.queue.clear();
        self.queue_selection = None;
        self.transcript.clear();
        self.transcript.push(
            EntryKind::System,
            "FLOCK",
            "New session. Send a message to begin.",
            EntryStatus::Complete,
        );
        self.set_status("New session ready.");
        previous
            .map(|provider_session_id| {
                vec![Effect::Backend(BackendCommand::UnsubscribeSession {
                    provider_session_id,
                })]
            })
            .unwrap_or_default()
    }

    pub fn enqueue_editor(&mut self) -> Vec<Effect> {
        if self.editor.is_blank() {
            self.set_status("Write a message before queueing.");
            return Vec::new();
        }
        if !self.is_busy() {
            return self.submit_editor();
        }

        let prompt = self.take_editor_prompt();
        self.queue.push_back(prompt);
        self.queue_selection = Some(self.queue.len() - 1);
        self.status_message = format!("Queued message {}.", self.queue.len());
        Vec::new()
    }

    pub fn steer_editor(&mut self) -> Vec<Effect> {
        if self.editor.is_blank() {
            self.set_status("Write steering guidance first.");
            return Vec::new();
        }
        if !self.backend_capabilities.steering.is_supported() {
            self.status_message = format!("{} does not support steering.", self.backend_name);
            return Vec::new();
        }
        if self.pending_steer.is_some() {
            self.set_status("A steer request is already awaiting the backend.");
            return Vec::new();
        }
        let Some(active) = self.active_turn.as_ref() else {
            self.set_status("There is no active turn to steer.");
            return Vec::new();
        };
        if active.cancelling {
            self.set_status("The active turn is being cancelled.");
            return Vec::new();
        }
        let turn_id = active.id.clone();
        let Some(provider_session_id) = self.provider_session_id.clone() else {
            self.set_status("The active provider session is unavailable.");
            return Vec::new();
        };

        let id = self.next_id("steer");
        let text = self.editor.text();
        self.pending_steer = Some(PendingSteer {
            id: id.clone(),
            text: text.clone(),
            turn_id: turn_id.clone(),
            editor_revision: self.editor.revision(),
        });
        self.set_status("Sending steering guidance…");
        vec![Effect::Backend(BackendCommand::SteerTurn {
            session_id: provider_session_id,
            turn_id,
            client_id: id,
            prompt: text,
        })]
    }

    pub fn cancel_or_quit(&mut self) -> Vec<Effect> {
        let Some(active) = self.active_turn.as_mut() else {
            self.should_quit = true;
            return vec![Effect::Backend(BackendCommand::Shutdown), Effect::Quit];
        };
        if !self.backend_capabilities.interruption.is_supported() {
            self.status_message = format!("{} does not support interruption.", self.backend_name);
            return Vec::new();
        }
        if active.cancelling {
            self.should_quit = true;
            return vec![Effect::Backend(BackendCommand::Shutdown), Effect::Quit];
        }
        let Some(provider_session_id) = self.provider_session_id.clone() else {
            self.set_status("Cannot cancel: the provider session id is unavailable.");
            return Vec::new();
        };

        active.cancelling = true;
        self.status_message.clear();
        self.status_message
            .push_str("Cancelling… Press Ctrl+C again to exit Flock.");
        vec![Effect::Backend(BackendCommand::InterruptTurn {
            session_id: provider_session_id,
            turn_id: active.id.clone(),
        })]
    }

    pub fn request_quit(&mut self) -> Vec<Effect> {
        if self.is_busy() {
            self.set_status("A turn is active. Cancel it with Ctrl+C before exiting.");
            Vec::new()
        } else {
            self.should_quit = true;
            vec![Effect::Backend(BackendCommand::Shutdown), Effect::Quit]
        }
    }

    pub fn install_cached_models(&mut self, models: Vec<ModelInfo>) {
        if models.is_empty() {
            return;
        }
        self.install_models(models);
        self.status_message = format!("Loaded cached {} models.", self.backend_name);
    }

    pub fn open_model_picker(&mut self) -> Vec<Effect> {
        if self.pending_model_picker.is_some()
            || (self.creating_session.is_some() && self.provider_session_id.is_none())
        {
            self.status_message = format!("Loading {} models…", self.backend_name);
            return Vec::new();
        }
        if !self.models.is_empty() {
            self.show_model_picker();
            return Vec::new();
        }
        if !self.backend_capabilities.model_catalog.is_supported() {
            self.status_message = format!("{} does not expose model selection.", self.backend_name);
            return Vec::new();
        }
        self.pending_model_picker = Some(());
        self.status_message = format!("Loading {} models…", self.backend_name);
        if self
            .backend_capabilities
            .models_require_session
            .is_supported()
            && self.provider_session_id.is_none()
        {
            self.creating_session = Some(());
        }
        vec![Effect::Backend(BackendCommand::Reload {
            session_id: self.provider_session_id.clone(),
        })]
    }

    fn show_model_picker(&mut self) {
        let selected = self
            .selected_model
            .as_ref()
            .and_then(|selected| {
                self.models
                    .iter()
                    .position(|model| &model.qualified_id() == selected)
            })
            .unwrap_or(0);
        self.model_picker = Some(ModelPicker {
            filter: String::new(),
            selected,
        });
        self.pending_model_picker = None;
    }

    pub fn picker_insert(&mut self, character: char) {
        if let Some(picker) = &mut self.model_picker
            && !character.is_control()
        {
            picker.filter.push(character);
            picker.selected = 0;
        }
    }

    pub fn picker_backspace(&mut self) {
        if let Some(picker) = &mut self.model_picker {
            picker.filter.pop();
            picker.selected = 0;
        }
    }

    pub fn picker_move(&mut self, delta: isize) {
        let count = self.filtered_models().len();
        let Some(picker) = &mut self.model_picker else {
            return;
        };
        if count == 0 {
            picker.selected = 0;
            return;
        }
        picker.selected = offset_index(picker.selected, count, delta);
    }

    pub fn picker_select(&mut self) -> Vec<Effect> {
        let filtered = self.filtered_models();
        let selected = self
            .model_picker
            .as_ref()
            .and_then(|picker| filtered.get(picker.selected))
            .copied()
            .cloned();
        if let Some(selected) = selected {
            if !self.activate_provider(&selected.provider) {
                return Vec::new();
            }
            let active = self.active_turn.is_some();
            let qualified = selected.qualified_id();
            self.selected_model = Some(qualified.clone());
            self.status_message = if active {
                format!("Next model: {qualified}. The active turn is unchanged.")
            } else {
                format!("Selected model: {qualified}.")
            };
            self.model_picker = None;
            if self
                .backend_capabilities
                .session_model_config
                .is_supported()
                && let Some(session_id) = self.provider_session_id.clone()
            {
                return vec![Effect::Backend(BackendCommand::SetSessionModel {
                    session_id,
                    model: selected.id,
                })];
            }
            if let Some(session_id) = self.session_id.clone() {
                return vec![Effect::UpdateSessionModel {
                    session_id,
                    model: Some(qualified),
                }];
            }
        }
        Vec::new()
    }

    #[must_use]
    pub fn filtered_models(&self) -> Vec<&ModelInfo> {
        let Some(picker) = &self.model_picker else {
            return self.models.iter().collect();
        };
        let filter = picker.filter.to_lowercase();
        self.models
            .iter()
            .filter(|model| {
                filter.is_empty() || model.qualified_id().to_lowercase().contains(&filter)
            })
            .collect()
    }

    pub fn close_model_picker(&mut self) {
        self.model_picker = None;
        self.set_status("Model selection cancelled.");
    }

    pub fn move_queue_selection(&mut self, delta: isize) {
        if self.queue.is_empty() {
            self.queue_selection = None;
            return;
        }
        let current = self.queue_selection.unwrap_or(0);
        self.queue_selection = Some(offset_index(current, self.queue.len(), delta));
    }

    pub fn remove_selected_queue_item(&mut self) {
        let Some(index) = self.queue_selection else {
            return;
        };
        if let Some(prompt) = self.queue.remove(index) {
            self.status_message = format!("Removed queued message {}.", prompt.id);
        }
        self.queue_selection = if self.queue.is_empty() {
            None
        } else {
            Some(index.min(self.queue.len() - 1))
        };
    }

    pub fn resolve_approval(&mut self, decision: ApprovalDecision) -> Vec<Effect> {
        let Some(approval) = self.approvals.pop_front() else {
            return Vec::new();
        };
        let decision_name = match decision {
            ApprovalDecision::AcceptOnce => "accepted",
            ApprovalDecision::AcceptForSession => "accepted for this session",
            ApprovalDecision::Decline => "declined",
        };

        self.transcript.push(
            EntryKind::System,
            "APPROVAL",
            format!("{}: {decision_name}", approval.title),
            EntryStatus::Complete,
        );
        self.status_message = format!("Approval {decision_name}.");
        vec![Effect::Backend(BackendCommand::ResolveApproval {
            id: approval.id,
            decision,
        })]
    }

    pub fn handle_provider_backend(&mut self, provider: &str, event: BackendEvent) -> Vec<Effect> {
        match &event {
            BackendEvent::Ready(identity) => {
                self.provider_contexts.insert(
                    provider.to_owned(),
                    ProviderContext {
                        name: identity.display_name.clone(),
                        capabilities: identity.capabilities.clone(),
                        connection: ConnectionState::Ready {
                            server: identity.display_name.clone(),
                        },
                        provider_session_id: self
                            .provider_contexts
                            .get(provider)
                            .and_then(|context| context.provider_session_id.clone()),
                        session_id: self
                            .provider_contexts
                            .get(provider)
                            .and_then(|context| context.session_id.clone()),
                    },
                );
            }
            BackendEvent::Models(models) => {
                let mut models = models.clone();
                for model in &mut models {
                    provider.clone_into(&mut model.provider);
                }
                if !models.is_empty() {
                    self.install_models(models.clone());
                }
                if self.pending_model_picker.is_some() && !self.models.is_empty() {
                    self.show_model_picker();
                }
                return vec![Effect::PersistModels {
                    provider: provider.to_owned(),
                    models,
                }];
            }
            BackendEvent::SessionCreated {
                provider_session_id,
                ..
            }
            | BackendEvent::SessionResumed {
                provider_session_id,
                ..
            }
            | BackendEvent::SessionObserved {
                provider_session_id,
            } if provider != self.backend_provider => {
                if let Some(context) = self.provider_contexts.get_mut(provider) {
                    context.provider_session_id = Some(provider_session_id.clone());
                }
                return Vec::new();
            }
            BackendEvent::Warning(message) if provider != self.backend_provider => {
                self.diagnostic_count += 1;
                self.transcript.push(
                    EntryKind::System,
                    format!("{provider} WARNING"),
                    message,
                    EntryStatus::Complete,
                );
                self.status_message.clone_from(message);
                return Vec::new();
            }
            _ => {}
        }

        if provider != self.backend_provider {
            return Vec::new();
        }
        let effects = self.handle_backend(event);
        self.sync_active_provider_context();
        effects
    }

    fn sync_active_provider_context(&mut self) {
        let context = self
            .provider_contexts
            .entry(self.backend_provider.clone())
            .or_insert_with(|| ProviderContext {
                name: self.backend_name.clone(),
                capabilities: self.backend_capabilities.clone(),
                connection: self.connection.clone(),
                provider_session_id: None,
                session_id: None,
            });
        context.name.clone_from(&self.backend_name);
        context.capabilities = self.backend_capabilities.clone();
        context.connection = self.connection.clone();
        context
            .provider_session_id
            .clone_from(&self.provider_session_id);
        context.session_id.clone_from(&self.session_id);
    }

    fn activate_provider(&mut self, provider: &str) -> bool {
        if provider == self.backend_provider {
            return true;
        }
        if self.is_busy() {
            self.set_status("Cannot change provider while a turn is active.");
            return false;
        }
        self.sync_active_provider_context();
        let Some(context) = self.provider_contexts.get(provider).cloned() else {
            self.status_message = format!("Provider {provider} is not available.");
            return false;
        };
        provider.clone_into(&mut self.backend_provider);
        self.backend_name = context.name;
        self.backend_capabilities = context.capabilities;
        self.connection = context.connection;
        self.provider_session_id = context.provider_session_id;
        self.session_id = context.session_id;
        true
    }

    pub fn handle_backend(&mut self, event: BackendEvent) -> Vec<Effect> {
        match event {
            BackendEvent::Ready(identity) => return self.handle_ready(identity),
            BackendEvent::Models(models) => return self.handle_models(models),
            BackendEvent::SessionCreated {
                provider_session_id,
                model,
            } => {
                return self.handle_session_created(provider_session_id, &model);
            }
            BackendEvent::SessionResumed {
                provider_session_id,
                model,
                history,
            } => {
                return self.handle_session_resumed(provider_session_id, &model, history);
            }
            BackendEvent::SessionUnsubscribed => {}
            BackendEvent::SessionObserved {
                provider_session_id,
            } => self.observe_session(provider_session_id),
            BackendEvent::TurnAccepted { turn_id } | BackendEvent::TurnStarted { turn_id } => {
                if turn_id.is_empty() {
                    return self.protocol_problem("turn event returned an empty turn id");
                }
                self.observe_turn_started(turn_id);
            }
            BackendEvent::TurnCompleted {
                turn_id,
                outcome,
                error,
            } => return self.complete_turn(&turn_id, outcome, error),
            BackendEvent::ItemStarted { turn_id, item } => {
                self.observe_item(turn_id, item, false);
            }
            BackendEvent::ItemCompleted { turn_id, item } => {
                self.observe_item(turn_id, item, true);
            }
            BackendEvent::ItemDelta {
                turn_id,
                item_id,
                kind,
                delta,
            } => self.observe_delta(turn_id, item_id, kind, &delta),
            BackendEvent::TurnDiff { turn_id, diff } => {
                self.observe_turn_artifact(&turn_id, diff, EntryKind::Diff, "TURN DIFF", "diff");
            }
            BackendEvent::TurnPlan { turn_id, plan } => {
                self.observe_turn_artifact(&turn_id, plan, EntryKind::Reasoning, "PLAN", "plan");
            }
            BackendEvent::ApprovalRequested(approval) => {
                self.status_message = format!("Approval required: {}", approval.title);
                self.approvals.push_back(approval);
            }
            BackendEvent::ApprovalResolved { request_id } => {
                self.resolve_external_approval(&request_id);
            }
            BackendEvent::SteerAccepted { turn_id } => self.handle_steer_accepted(&turn_id),
            BackendEvent::InterruptAccepted => {
                self.set_status("Interrupt accepted; waiting for the turn to stop…");
            }
            BackendEvent::ModelRerouted { turn_id, from, to } => {
                self.handle_model_rerouted(&turn_id, &from, &to);
            }
            BackendEvent::Warning(message) => self.handle_warning(message),
            BackendEvent::TurnError {
                turn_id,
                message,
                will_retry,
            } => self.handle_turn_error(&turn_id, message, will_retry),
            BackendEvent::RequestFailed {
                operation,
                code,
                message,
            } => return self.request_failed(operation, code, message),
            BackendEvent::ProtocolDiagnostic(message) => {
                self.diagnostic_count += 1;
                self.status_message = format!("Protocol diagnostic: {message}");
            }
            BackendEvent::SessionClosed {
                provider_session_id,
            } => self.handle_session_closed(&provider_session_id),
            BackendEvent::Disconnected { reason } => self.handle_disconnected(reason),
        }
        Vec::new()
    }

    fn handle_ready(&mut self, identity: crate::backend::BackendIdentity) -> Vec<Effect> {
        self.backend_provider = identity.provider;
        self.backend_name = identity.display_name;
        self.backend_capabilities = identity.capabilities;
        self.connection = ConnectionState::Ready {
            server: self.backend_name.clone(),
        };
        self.transcript.upsert(
            "flock:startup",
            EntryKind::System,
            "FLOCK",
            format!("Connected to {}.", self.backend_name),
            EntryStatus::Complete,
        );
        self.set_status("Ready.");
        self.startup_resume
            .take()
            .map_or_else(Vec::new, |id| vec![Effect::ResolveSession(id)])
    }

    fn observe_session(&mut self, provider_session_id: String) {
        if self.provider_session_id.is_none() && !provider_session_id.is_empty() {
            self.provider_session_id = Some(provider_session_id);
        }
    }

    fn resolve_external_approval(&mut self, request_id: &serde_json::Value) {
        if let Some(index) = self
            .approvals
            .iter()
            .position(|approval| &approval.id == request_id)
        {
            self.approvals.remove(index);
            self.set_status("Approval was resolved by another client.");
        }
    }

    fn handle_warning(&mut self, message: String) {
        self.transcript.push(
            EntryKind::Warning,
            "BACKEND WARNING",
            &message,
            EntryStatus::Complete,
        );
        self.status_message = message;
    }

    fn handle_models(&mut self, models: Vec<ModelInfo>) -> Vec<Effect> {
        if models.is_empty() {
            self.pending_model_picker = None;
            if self.models.is_empty() {
                self.install_models(models);
            } else {
                self.set_status("Model refresh returned no choices; kept the cached catalog.");
            }
            return Vec::new();
        }
        let cached = models.clone();
        self.install_models(models);
        if self.pending_model_picker.is_some() {
            self.show_model_picker();
        }
        let mut effects = vec![Effect::PersistModels {
            provider: self.backend_provider.clone(),
            models: cached,
        }];
        if let (Some(session_id), Some(model)) =
            (self.session_id.clone(), self.selected_model.clone())
        {
            effects.push(Effect::UpdateSessionModel {
                session_id,
                model: Some(model),
            });
        }
        effects
    }

    fn handle_session_created(&mut self, provider_session_id: String, model: &str) -> Vec<Effect> {
        if provider_session_id.is_empty() {
            return self.protocol_problem("session creation returned an empty provider id");
        }
        self.provider_session_id = Some(provider_session_id.clone());
        self.creating_session = None;
        if !model.is_empty() {
            let qualified = self.qualify_active_model(model);
            if self.selected_model.as_deref() != Some(qualified.as_str()) {
                self.selected_model = Some(qualified.clone());
                self.status_message = format!("{} selected model {qualified}.", self.backend_name);
            }
        }
        let Some(prompt) = self.pending_session_prompt.take() else {
            return Vec::new();
        };
        let mut effects = vec![Effect::PersistSession {
            provider: self.backend_provider.clone(),
            provider_session_id: provider_session_id.clone(),
            workspace: self.workspace.clone(),
            title: prompt.text.clone(),
            model: self.selected_model.clone(),
        }];
        effects.extend(self.start_prompt_on_session(prompt, provider_session_id));
        effects
    }

    fn handle_session_resumed(
        &mut self,
        provider_session_id: String,
        model: &str,
        history: Vec<SessionHistoryItem>,
    ) -> Vec<Effect> {
        let Some(session) = self.resuming_session.take() else {
            return self.protocol_problem("received an unexpected session resume response");
        };
        if provider_session_id.is_empty() {
            return self.protocol_problem("session resume returned an empty provider id");
        }
        self.provider_session_id = Some(provider_session_id);
        self.session_id = Some(session.id.clone());
        if !model.is_empty() {
            self.selected_model = Some(self.qualify_active_model(model));
        }
        self.install_history(history);
        self.status_message = format!("Resumed session {}.", short_id(&session.id));
        vec![Effect::TouchSession(session.id)]
    }

    fn observe_turn_artifact(
        &mut self,
        turn_id: &str,
        body: String,
        kind: EntryKind,
        title: &str,
        suffix: &str,
    ) {
        if self.turn_is_current(turn_id) {
            self.transcript.upsert(
                format!("turn:{turn_id}:{suffix}"),
                kind,
                title,
                body,
                EntryStatus::Running,
            );
        }
    }

    fn handle_steer_accepted(&mut self, turn_id: &str) {
        let Some(pending) = self.pending_steer.take() else {
            self.set_status("A late steer response arrived after the turn ended.");
            return;
        };
        if pending.turn_id != turn_id || !self.turn_is_current(turn_id) {
            self.set_status("A late steer response was ignored; the draft was preserved.");
            return;
        }
        if self.editor.revision() == pending.editor_revision {
            self.editor.clear();
        }
        self.transcript.push(
            EntryKind::Steering,
            format!("STEER · {}", pending.id),
            pending.text,
            EntryStatus::Complete,
        );
        self.set_status("Steering guidance accepted.");
    }

    fn handle_model_rerouted(&mut self, turn_id: &str, from: &str, to: &str) {
        let Some(active) = self
            .active_turn
            .as_mut()
            .filter(|active| active.id == turn_id)
        else {
            self.diagnostic_count += 1;
            return;
        };
        active.model = Some(to.to_owned());
        self.transcript.push(
            EntryKind::Warning,
            "MODEL REROUTED",
            format!(
                "{} changed this turn from {from} to {to}.",
                self.backend_name
            ),
            EntryStatus::Complete,
        );
        self.status_message = format!("Model rerouted to {to}.");
    }

    fn handle_turn_error(&mut self, turn_id: &str, message: String, will_retry: bool) {
        let body = if will_retry {
            format!("{message}\n{} will retry.", self.backend_name)
        } else {
            message.clone()
        };
        let status = if will_retry {
            EntryStatus::Running
        } else {
            EntryStatus::Failed
        };
        self.transcript
            .push(EntryKind::Error, "BACKEND ERROR", body, status);
        self.status_message = if will_retry {
            format!("{} error on {turn_id}; retrying…", self.backend_name)
        } else {
            message
        };
    }

    fn handle_session_closed(&mut self, provider_session_id: &str) {
        if self.provider_session_id.as_deref() != Some(provider_session_id) {
            return;
        }
        let pending_prompt = self
            .pending_session_prompt
            .take()
            .or_else(|| self.starting_turn.take());
        self.provider_session_id = None;
        self.active_turn = None;
        self.creating_session = None;
        self.pending_steer = None;
        self.approvals.clear();
        self.set_status("The provider session was closed.");
        if let Some(prompt) = pending_prompt {
            self.restore_failed_prompt(&prompt);
        }
    }

    fn handle_disconnected(&mut self, reason: String) {
        let pending_prompt = self
            .pending_session_prompt
            .take()
            .or_else(|| self.starting_turn.take());
        self.connection = ConnectionState::Disconnected(reason.clone());
        self.active_turn = None;
        self.creating_session = None;
        self.pending_steer = None;
        self.transcript.push(
            EntryKind::Error,
            "BACKEND DISCONNECTED",
            &reason,
            EntryStatus::Failed,
        );
        self.status_message = reason;
        if let Some(prompt) = pending_prompt {
            self.restore_failed_prompt(&prompt);
        }
    }

    fn take_editor_prompt(&mut self) -> QueuedPrompt {
        let prompt = QueuedPrompt {
            id: self.next_id("msg"),
            text: self.editor.text(),
        };
        self.editor.clear();
        prompt
    }

    fn begin_prompt(&mut self, prompt: QueuedPrompt) -> Vec<Effect> {
        let prompt = OutgoingPrompt {
            id: prompt.id,
            text: prompt.text,
            model: self
                .backend_capabilities
                .model_catalog
                .is_supported()
                .then(|| self.selected_model_for_active_provider())
                .flatten(),
        };
        self.transcript.upsert(
            format!("user:{}", prompt.id),
            EntryKind::User,
            format!("YOU · {}", prompt.id),
            &prompt.text,
            EntryStatus::Complete,
        );
        self.scroll_from_bottom = 0;

        if let Some(provider_session_id) = self.provider_session_id.clone() {
            let persist = self.session_id.is_none().then(|| Effect::PersistSession {
                provider: self.backend_provider.clone(),
                provider_session_id: provider_session_id.clone(),
                workspace: self.workspace.clone(),
                title: prompt.text.clone(),
                model: self.selected_model.clone(),
            });
            let mut effects = self.start_prompt_on_session(prompt, provider_session_id);
            if let Some(persist) = persist {
                effects.insert(0, persist);
            }
            effects
        } else {
            self.creating_session = Some(());
            self.pending_session_prompt = Some(prompt.clone());
            self.status_message = format!("Creating a {} session…", self.backend_name);
            vec![Effect::Backend(BackendCommand::StartSession {
                model: prompt.model,
            })]
        }
    }

    fn start_prompt_on_session(
        &mut self,
        prompt: OutgoingPrompt,
        provider_session_id: String,
    ) -> Vec<Effect> {
        self.starting_turn = Some(prompt.clone());
        self.set_status("Starting turn…");
        vec![Effect::Backend(BackendCommand::StartTurn {
            session_id: provider_session_id,
            client_id: prompt.id,
            prompt: prompt.text,
            model: prompt.model,
        })]
    }

    fn observe_turn_started(&mut self, turn_id: String) {
        if let Some(active) = &self.active_turn {
            if active.id == turn_id {
                return;
            }
            self.diagnostic_count += 1;
            return;
        }
        let model = self
            .starting_turn
            .take()
            .and_then(|prompt| prompt.model)
            .or_else(|| self.selected_model.clone());
        self.active_turn = Some(ActiveTurn {
            id: turn_id,
            model,
            cancelling: false,
        });
        self.status_message = format!("{} is working…", self.backend_name);
    }

    fn complete_turn(
        &mut self,
        turn_id: &str,
        outcome: TurnOutcome,
        error: Option<String>,
    ) -> Vec<Effect> {
        if self.active_turn.is_none() && self.starting_turn.is_some() {
            self.observe_turn_started(turn_id.to_owned());
        }
        if !self.turn_is_current(turn_id) {
            self.diagnostic_count += 1;
            self.status_message = format!("Ignored completion for inactive turn {turn_id}.");
            return Vec::new();
        }

        let final_item_status = match outcome {
            TurnOutcome::Completed => EntryStatus::Complete,
            TurnOutcome::Interrupted => EntryStatus::Interrupted,
            TurnOutcome::Failed => EntryStatus::Failed,
        };
        let item_ids = self
            .item_turns
            .iter()
            .filter(|(_, item_turn_id)| item_turn_id.as_str() == turn_id)
            .map(|(item_id, _)| item_id.clone())
            .collect::<Vec<_>>();
        for item_id in item_ids {
            self.transcript.set_status(&item_id, final_item_status);
        }
        self.transcript
            .set_status(&format!("turn:{turn_id}:diff"), final_item_status);
        self.transcript
            .set_status(&format!("turn:{turn_id}:plan"), final_item_status);
        self.item_turns
            .retain(|_, item_turn_id| item_turn_id != turn_id);

        self.active_turn = None;
        self.starting_turn = None;
        if self
            .pending_steer
            .as_ref()
            .is_some_and(|pending| pending.turn_id == turn_id)
        {
            self.pending_steer = None;
            self.set_status("Steer was too late; the draft was preserved.");
        }

        match outcome {
            TurnOutcome::Completed => {
                self.set_status("Turn completed.");
            }
            TurnOutcome::Interrupted => {
                self.transcript.push(
                    EntryKind::System,
                    "TURN INTERRUPTED",
                    "The active turn was cancelled.",
                    EntryStatus::Interrupted,
                );
                self.set_status("Turn interrupted.");
            }
            TurnOutcome::Failed => {
                let message = error.unwrap_or_else(|| "The turn failed.".to_owned());
                self.transcript.push(
                    EntryKind::Error,
                    "TURN FAILED",
                    &message,
                    EntryStatus::Failed,
                );
                self.status_message = message;
            }
        }

        self.drain_queue()
    }

    fn drain_queue(&mut self) -> Vec<Effect> {
        if !self.connection.is_ready() || self.is_busy() {
            return Vec::new();
        }
        let Some(prompt) = self.queue.pop_front() else {
            self.queue_selection = None;
            return Vec::new();
        };
        self.queue_selection = if self.queue.is_empty() { None } else { Some(0) };
        self.begin_prompt(prompt)
    }

    fn install_history(&mut self, history: Vec<SessionHistoryItem>) {
        self.transcript.clear();
        self.item_turns.clear();
        for history_item in history {
            let item = history_item.item;
            self.item_turns
                .insert(item.id.clone(), history_item.turn_id);
            self.transcript.upsert(
                item.id,
                entry_kind(item.kind),
                item.title,
                item.body,
                entry_status(item.status),
            );
        }
        if self.transcript.entries().is_empty() {
            self.transcript.push(
                EntryKind::System,
                "FLOCK",
                "Resumed session has no visible history.",
                EntryStatus::Complete,
            );
        }
        self.scroll_from_bottom = 0;
    }

    fn observe_item(&mut self, turn_id: String, item: NormalizedItem, completed: bool) {
        if item.kind == ItemKind::User || !self.turn_is_current(&turn_id) {
            return;
        }
        self.item_turns.insert(item.id.clone(), turn_id);
        let status = if completed {
            entry_status(item.status)
        } else {
            EntryStatus::Running
        };
        self.transcript.upsert(
            item.id,
            entry_kind(item.kind),
            item.title,
            item.body,
            status,
        );
        if self.scroll_from_bottom == 0 {
            self.scroll_from_bottom = 0;
        }
    }

    fn observe_delta(&mut self, turn_id: String, item_id: String, kind: DeltaKind, delta: &str) {
        if !self.turn_is_current(&turn_id) {
            self.diagnostic_count += 1;
            return;
        }
        self.item_turns.insert(item_id.clone(), turn_id);
        let (entry_kind, title) = match kind {
            DeltaKind::Assistant => (EntryKind::Assistant, "ASSISTANT"),
            DeltaKind::Plan => (EntryKind::Reasoning, "PLAN"),
            DeltaKind::Reasoning => (EntryKind::Reasoning, "REASONING"),
            DeltaKind::Tool => (EntryKind::Tool, "TOOL OUTPUT"),
        };
        self.transcript
            .append_delta(item_id, entry_kind, title, delta);
    }

    fn request_failed(
        &mut self,
        operation: BackendOperation,
        code: i64,
        message: String,
    ) -> Vec<Effect> {
        let display = format!("{} failed ({code}): {message}", operation.label());
        self.transcript.push(
            EntryKind::Error,
            "REQUEST FAILED",
            &display,
            EntryStatus::Failed,
        );
        self.status_message = display;

        match operation {
            BackendOperation::Initialize => {
                self.connection = ConnectionState::Failed(message);
            }
            BackendOperation::ModelList
            | BackendOperation::SetSessionModel
            | BackendOperation::UnsubscribeSession => {}
            BackendOperation::Reload => {
                self.creating_session = None;
                self.pending_model_picker = None;
            }
            BackendOperation::ResumeSession => {
                self.resuming_session = None;
            }
            BackendOperation::StartSession => {
                if code == -32001 {
                    self.set_status(
                        "Session start timed out; waiting for a definitive backend event.",
                    );
                } else {
                    self.creating_session = None;
                    if let Some(prompt) = self.pending_session_prompt.take() {
                        self.restore_failed_prompt(&prompt);
                    }
                }
            }
            BackendOperation::StartTurn => {
                if code == -32001 {
                    self.set_status(
                        "Turn start timed out; waiting for a definitive backend event.",
                    );
                } else {
                    if let Some(prompt) = self.starting_turn.take() {
                        self.restore_failed_prompt(&prompt);
                    }
                    return self.drain_queue();
                }
            }
            BackendOperation::SteerTurn => {
                self.pending_steer = None;
            }
            BackendOperation::InterruptTurn => {
                if let Some(active) = &mut self.active_turn {
                    active.cancelling = false;
                }
            }
        }
        Vec::new()
    }

    fn restore_failed_prompt(&mut self, prompt: &OutgoingPrompt) {
        self.transcript
            .set_status(&format!("user:{}", prompt.id), EntryStatus::Failed);
        if self.editor.is_blank() {
            self.editor.set_text(&prompt.text);
            self.status_message.push_str(" Draft restored.");
        } else {
            self.status_message
                .push_str(" The original text remains in the transcript.");
        }
    }

    fn selected_model_for_active_provider(&self) -> Option<String> {
        self.selected_model.as_ref().and_then(|qualified| {
            self.models
                .iter()
                .find(|model| {
                    model.provider == self.backend_provider && model.qualified_id() == *qualified
                })
                .map(|model| model.id.clone())
        })
    }

    fn qualify_active_model(&self, model: &str) -> String {
        if model.contains('/') {
            model.to_owned()
        } else {
            format!("{}/{}", self.backend_provider, model)
        }
    }

    fn install_models(&mut self, models: Vec<ModelInfo>) {
        let providers: std::collections::HashSet<_> =
            models.iter().map(|model| model.provider.clone()).collect();
        self.models
            .retain(|model| !providers.contains(&model.provider));
        self.models.extend(models);
        self.models.sort_by_key(ModelInfo::qualified_id);
        if self.models.is_empty() {
            self.status_message = format!("{} returned an empty model catalog.", self.backend_name);
            self.pending_model_picker = None;
            return;
        }

        if let Some(initial) = self.initial_model.clone() {
            let initial_provider = initial.split_once('/').map(|(provider, _)| provider);
            if initial_provider.is_none_or(|provider| providers.contains(provider)) {
                self.initial_model = None;
                if self
                    .models
                    .iter()
                    .any(|model| model.qualified_id() == initial)
                {
                    self.selected_model = Some(initial);
                } else {
                    let fallback = self.default_model();
                    self.selected_model.clone_from(&fallback);
                    self.status_message = match fallback {
                        Some(fallback) => {
                            format!("Model {initial} is unavailable; using {fallback}.")
                        }
                        None => format!("Model {initial} is unavailable."),
                    };
                }
            }
        } else if self
            .backend_capabilities
            .session_model_config
            .is_supported()
            || self.selected_model.as_ref().is_none_or(|selected| {
                !self
                    .models
                    .iter()
                    .any(|model| &model.qualified_id() == selected)
            })
        {
            self.selected_model = self.default_model();
        }
    }

    fn default_model(&self) -> Option<String> {
        self.models
            .iter()
            .filter(|model| model.provider == self.backend_provider)
            .find(|model| model.is_default)
            .or_else(|| {
                self.models
                    .iter()
                    .find(|model| model.provider == self.backend_provider)
            })
            .map(ModelInfo::qualified_id)
    }

    fn protocol_problem(&mut self, message: &str) -> Vec<Effect> {
        self.diagnostic_count += 1;
        message.clone_into(&mut self.status_message);
        self.transcript.push(
            EntryKind::Error,
            "PROTOCOL ERROR",
            message,
            EntryStatus::Failed,
        );
        Vec::new()
    }

    fn turn_is_current(&self, turn_id: &str) -> bool {
        self.active_turn
            .as_ref()
            .is_some_and(|active| active.id == turn_id)
    }

    fn next_id(&mut self, kind: &str) -> String {
        let id = format!("flock-{kind}-{:06}", self.next_local_id);
        self.next_local_id = self.next_local_id.wrapping_add(1);
        id
    }
}

fn offset_index(index: usize, len: usize, delta: isize) -> usize {
    debug_assert!(len > 0);
    let distance = delta.unsigned_abs() % len;
    if delta.is_negative() {
        (index + len - distance) % len
    } else {
        (index + distance) % len
    }
}

fn short_id(id: &str) -> &str {
    id.get(..8).unwrap_or(id)
}

fn entry_kind(kind: ItemKind) -> EntryKind {
    match kind {
        ItemKind::User => EntryKind::User,
        ItemKind::Assistant => EntryKind::Assistant,
        ItemKind::Reasoning => EntryKind::Reasoning,
        ItemKind::Tool => EntryKind::Tool,
        ItemKind::Diff => EntryKind::Diff,
        ItemKind::System => EntryKind::System,
    }
}

fn entry_status(status: ItemStatus) -> EntryStatus {
    match status {
        ItemStatus::Running => EntryStatus::Running,
        ItemStatus::Complete => EntryStatus::Complete,
        ItemStatus::Failed => EntryStatus::Failed,
        ItemStatus::Declined => EntryStatus::Interrupted,
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        backend::{
            ApprovalKind, ApprovalRequest, BackendCapabilities, BackendCommand, BackendEvent,
            BackendIdentity, BackendOperation, CODEX_PROVIDER, CapabilitySupport, DEVIN_PROVIDER,
            ItemKind, ItemStatus, ModelInfo, NormalizedItem, SessionHistoryItem, TurnOutcome,
        },
        session::SessionRecord,
    };

    use super::{AppState, ApprovalDecision, Effect};

    fn ready_state() -> AppState {
        let mut state = AppState::new("/tmp/project", None, 100);
        state.handle_backend(BackendEvent::Ready(BackendIdentity {
            provider: CODEX_PROVIDER.to_owned(),
            display_name: "codex-test".to_owned(),
            version: None,
            capabilities: BackendCapabilities {
                resume: CapabilitySupport::Supported,
                steering: CapabilitySupport::Supported,
                interruption: CapabilitySupport::Supported,
                model_catalog: CapabilitySupport::Supported,
                models_require_session: CapabilitySupport::Unsupported,
                session_model_config: CapabilitySupport::Unsupported,
                approvals: CapabilitySupport::Supported,
                native_tools: CapabilitySupport::Supported,
                mcp: CapabilitySupport::Supported,
                close_session: CapabilitySupport::Supported,
            },
        }));
        state.handle_backend(BackendEvent::Models(vec![ModelInfo {
            provider: CODEX_PROVIDER.to_owned(),
            id: "model-a".to_owned(),
            is_default: true,
        }]));
        state
    }

    #[test]
    fn unsupported_backend_capabilities_are_not_simulated() {
        let mut state = AppState::new_for_backend(
            "/tmp/project",
            None,
            100,
            crate::backend::DEVIN_PROVIDER,
            "Devin",
        );
        state.handle_backend(BackendEvent::Ready(BackendIdentity {
            provider: crate::backend::DEVIN_PROVIDER.to_owned(),
            display_name: "Devin".to_owned(),
            version: None,
            capabilities: BackendCapabilities {
                interruption: CapabilitySupport::Supported,
                native_tools: CapabilitySupport::Supported,
                approvals: CapabilitySupport::Supported,
                ..BackendCapabilities::default()
            },
        }));
        state.provider_session_id = Some("devin-session".to_owned());
        state.active_turn = Some(super::ActiveTurn {
            id: "turn-1".to_owned(),
            model: None,
            cancelling: false,
        });
        state.editor.set_text("steer");

        assert!(state.steer_editor().is_empty());
        assert_eq!(state.editor.text(), "steer");
        assert!(state.status_message.contains("does not support steering"));
    }

    #[test]
    fn devin_model_picker_lazily_creates_session_and_applies_selection() {
        let mut state = AppState::new_for_backend(
            "/tmp/project",
            None,
            100,
            crate::backend::DEVIN_PROVIDER,
            "Devin",
        );
        state.handle_backend(BackendEvent::Ready(BackendIdentity {
            provider: crate::backend::DEVIN_PROVIDER.to_owned(),
            display_name: "Devin".to_owned(),
            version: None,
            capabilities: BackendCapabilities {
                model_catalog: CapabilitySupport::Supported,
                models_require_session: CapabilitySupport::Supported,
                session_model_config: CapabilitySupport::Supported,
                interruption: CapabilitySupport::Supported,
                native_tools: CapabilitySupport::Supported,
                approvals: CapabilitySupport::Supported,
                ..BackendCapabilities::default()
            },
        }));

        assert!(matches!(
            state.open_model_picker().as_slice(),
            [Effect::Backend(BackendCommand::Reload { session_id: None })]
        ));
        assert!(state.status_message.contains("Loading Devin models"));
        assert!(state.open_model_picker().is_empty());
        state.handle_backend(BackendEvent::SessionCreated {
            provider_session_id: "devin-session".to_owned(),
            model: "model-a".to_owned(),
        });
        state.handle_backend(BackendEvent::Models(vec![
            ModelInfo {
                provider: DEVIN_PROVIDER.to_owned(),
                id: "model-a".to_owned(),
                is_default: true,
            },
            ModelInfo {
                provider: DEVIN_PROVIDER.to_owned(),
                id: "model-b".to_owned(),
                is_default: false,
            },
        ]));
        assert!(state.model_picker.is_some());
        state.picker_move(1);
        assert!(matches!(
            state.picker_select().as_slice(),
            [Effect::Backend(BackendCommand::SetSessionModel { session_id, model })]
                if session_id == "devin-session" && model == "model-b"
        ));

        state.editor.set_text("/reload");
        assert!(matches!(
            state.submit_editor().as_slice(),
            [Effect::Backend(BackendCommand::Reload { session_id: Some(session_id) })]
                if session_id == "devin-session"
        ));

        state.editor.set_text("first real prompt");
        let effects = state.submit_editor();
        assert!(matches!(
            effects.as_slice(),
            [
                Effect::PersistSession { provider_session_id, model, .. },
                Effect::Backend(BackendCommand::StartTurn { .. })
            ] if provider_session_id == "devin-session" && model.as_deref() == Some("devin-acp/model-b")
        ));
    }

    #[test]
    fn empty_model_refresh_keeps_cached_catalog() {
        let mut state = ready_state();
        let cached = state.models.clone();

        assert!(
            state
                .handle_backend(BackendEvent::Models(Vec::new()))
                .is_empty()
        );
        assert_eq!(state.models, cached);
        assert!(state.status_message.contains("kept the cached catalog"));
    }

    #[test]
    fn queue_drains_fifo_after_terminal_turn_event() {
        let mut state = ready_state();
        state.provider_session_id = Some("thread-1".to_owned());
        state.session_id = Some("flock-session-1".to_owned());
        state.editor.set_text("first");
        let first = state.submit_editor();
        assert!(matches!(first.as_slice(), [Effect::Backend(_)]));
        state.handle_backend(BackendEvent::TurnAccepted {
            turn_id: "turn-1".to_owned(),
        });

        state.editor.set_text("second");
        state.submit_editor();
        state.editor.set_text("third");
        state.submit_editor();
        assert_eq!(state.queue.len(), 2);

        let effects = state.handle_backend(BackendEvent::TurnCompleted {
            turn_id: "turn-1".to_owned(),
            outcome: TurnOutcome::Completed,
            error: None,
        });
        assert_eq!(state.queue.len(), 1);
        assert!(matches!(effects.as_slice(), [Effect::Backend(_)]));
    }

    #[test]
    fn steer_clears_only_after_acceptance() {
        let mut state = ready_state();
        state.provider_session_id = Some("thread-1".to_owned());
        state.active_turn = Some(super::ActiveTurn {
            id: "turn-1".to_owned(),
            model: Some("model-a".to_owned()),
            cancelling: false,
        });
        state.editor.set_text("focus on tests");

        let effects = state.steer_editor();
        assert!(matches!(effects.as_slice(), [Effect::Backend(_)]));
        assert_eq!(state.editor.text(), "focus on tests");

        state.handle_backend(BackendEvent::SteerAccepted {
            turn_id: "turn-1".to_owned(),
        });
        assert!(state.editor.is_blank());
    }

    #[test]
    fn completed_turn_wins_race_with_late_steer_response() {
        let mut state = ready_state();
        state.provider_session_id = Some("thread-1".to_owned());
        state.active_turn = Some(super::ActiveTurn {
            id: "turn-1".to_owned(),
            model: Some("model-a".to_owned()),
            cancelling: false,
        });
        state.editor.set_text("too late");
        state.steer_editor();

        state.handle_backend(BackendEvent::TurnCompleted {
            turn_id: "turn-1".to_owned(),
            outcome: TurnOutcome::Completed,
            error: None,
        });
        state.handle_backend(BackendEvent::SteerAccepted {
            turn_id: "turn-1".to_owned(),
        });

        assert_eq!(state.editor.text(), "too late");
        assert!(state.status_message.contains("late steer"));
    }

    #[test]
    fn session_start_timeout_preserves_the_pending_prompt() {
        let mut state = ready_state();
        state.editor.set_text("first");
        state.submit_editor();

        let effects = state.handle_backend(BackendEvent::RequestFailed {
            operation: BackendOperation::StartSession,
            code: -32001,
            message: "timeout".to_owned(),
        });
        assert!(effects.is_empty());
        assert!(state.is_busy());
        assert!(state.editor.is_blank());

        let effects = state.handle_backend(BackendEvent::SessionCreated {
            provider_session_id: "thread-late".to_owned(),
            model: "model-a".to_owned(),
        });
        assert!(matches!(
            effects.as_slice(),
            [Effect::PersistSession { .. }, Effect::Backend(_)]
        ));
    }

    #[test]
    fn rejected_session_start_restores_the_draft() {
        let mut state = ready_state();
        state.editor.set_text("first");
        state.submit_editor();

        state.handle_backend(BackendEvent::RequestFailed {
            operation: BackendOperation::StartSession,
            code: -32602,
            message: "rejected".to_owned(),
        });

        assert!(!state.is_busy());
        assert_eq!(state.editor.text(), "first");
        let user = state
            .transcript
            .entries()
            .iter()
            .find(|entry| entry.kind == crate::transcript::EntryKind::User)
            .expect("failed user entry");
        assert_eq!(user.status, crate::transcript::EntryStatus::Failed);
    }

    #[test]
    fn backend_prompt_failure_completion_clears_active_turn() {
        let mut state = ready_state();
        state.provider_session_id = Some("session-1".to_owned());
        state.editor.set_text("fail prompt");
        state.submit_editor();
        state.handle_backend(BackendEvent::TurnAccepted {
            turn_id: "turn-failed".to_owned(),
        });
        state.handle_backend(BackendEvent::RequestFailed {
            operation: BackendOperation::StartTurn,
            code: -32602,
            message: "prompt failed".to_owned(),
        });
        state.handle_backend(BackendEvent::TurnCompleted {
            turn_id: "turn-failed".to_owned(),
            outcome: TurnOutcome::Failed,
            error: Some("prompt failed".to_owned()),
        });

        assert!(!state.is_busy());
        assert_eq!(state.status_message, "prompt failed");
    }

    #[test]
    fn start_turn_timeout_does_not_launch_the_next_queued_prompt() {
        let mut state = ready_state();
        state.provider_session_id = Some("thread-1".to_owned());
        state.session_id = Some("flock-session-1".to_owned());
        state.editor.set_text("first");
        state.submit_editor();
        state.editor.set_text("second");
        state.submit_editor();

        let effects = state.handle_backend(BackendEvent::RequestFailed {
            operation: BackendOperation::StartTurn,
            code: -32001,
            message: "timeout".to_owned(),
        });
        assert!(effects.is_empty());
        assert!(state.is_busy());
        assert_eq!(state.queue.len(), 1);

        state.handle_backend(BackendEvent::TurnStarted {
            turn_id: "turn-late".to_owned(),
        });
        let effects = state.handle_backend(BackendEvent::TurnCompleted {
            turn_id: "turn-late".to_owned(),
            outcome: TurnOutcome::Completed,
            error: None,
        });
        assert!(matches!(effects.as_slice(), [Effect::Backend(_)]));
    }

    #[test]
    fn session_close_clears_all_busy_state() {
        let mut state = ready_state();
        state.provider_session_id = Some("thread-1".to_owned());
        state.editor.set_text("first");
        state.submit_editor();
        assert!(state.is_busy());

        state.handle_backend(BackendEvent::SessionClosed {
            provider_session_id: "thread-1".to_owned(),
        });

        assert!(!state.is_busy());
        assert!(state.provider_session_id.is_none());
    }

    #[test]
    fn steer_ack_does_not_clear_a_draft_edited_back_to_the_same_text() {
        let mut state = ready_state();
        state.provider_session_id = Some("thread-1".to_owned());
        state.active_turn = Some(super::ActiveTurn {
            id: "turn-1".to_owned(),
            model: Some("model-a".to_owned()),
            cancelling: false,
        });
        state.editor.set_text("focus");
        state.steer_editor();
        state.editor.insert_char('x');
        state.editor.backspace();

        state.handle_backend(BackendEvent::SteerAccepted {
            turn_id: "turn-1".to_owned(),
        });

        assert_eq!(state.editor.text(), "focus");
    }

    #[test]
    fn approval_decisions_are_provider_neutral() {
        let mut state = ready_state();
        state.approvals.push_back(ApprovalRequest {
            id: serde_json::json!("v2-request"),
            method: "item/commandExecution/requestApproval".to_owned(),
            kind: ApprovalKind::Command,
            title: "command".to_owned(),
            detail: "cargo test".to_owned(),
        });
        let effects = state.resolve_approval(ApprovalDecision::AcceptOnce);
        let [Effect::Backend(BackendCommand::ResolveApproval { decision, .. })] =
            effects.as_slice()
        else {
            panic!("expected approval response");
        };
        assert_eq!(*decision, ApprovalDecision::AcceptOnce);

        state.approvals.push_back(ApprovalRequest {
            id: serde_json::json!("legacy-request"),
            method: "execCommandApproval".to_owned(),
            kind: ApprovalKind::Command,
            title: "command".to_owned(),
            detail: "cargo test".to_owned(),
        });
        let effects = state.resolve_approval(ApprovalDecision::AcceptForSession);
        let [Effect::Backend(BackendCommand::ResolveApproval { decision, .. })] =
            effects.as_slice()
        else {
            panic!("expected approval response");
        };
        assert_eq!(*decision, ApprovalDecision::AcceptForSession);
    }

    #[test]
    fn startup_entry_is_replaced_after_connection() {
        let state = ready_state();
        let startup = state
            .transcript
            .entries()
            .iter()
            .filter(|entry| entry.title == "FLOCK")
            .collect::<Vec<_>>();
        assert_eq!(startup.len(), 1);
        assert!(startup[0].body.contains("Connected to codex-test"));
    }

    #[test]
    fn turn_completion_finalizes_running_item_entries() {
        let mut state = ready_state();
        state.provider_session_id = Some("thread-1".to_owned());
        state.active_turn = Some(super::ActiveTurn {
            id: "turn-1".to_owned(),
            model: Some("model-a".to_owned()),
            cancelling: false,
        });
        state.handle_backend(BackendEvent::ItemStarted {
            turn_id: "turn-1".to_owned(),
            item: NormalizedItem {
                id: "item-1".to_owned(),
                kind: ItemKind::Tool,
                title: "TOOL".to_owned(),
                body: "running".to_owned(),
                status: ItemStatus::Running,
            },
        });
        state.handle_backend(BackendEvent::TurnCompleted {
            turn_id: "turn-1".to_owned(),
            outcome: TurnOutcome::Completed,
            error: None,
        });

        let item = state
            .transcript
            .entries()
            .iter()
            .find(|entry| entry.key.as_deref() == Some("item-1"))
            .expect("tool transcript entry");
        assert_eq!(item.status, crate::transcript::EntryStatus::Complete);
    }

    #[test]
    fn slash_commands_are_not_sent_as_prompts() {
        let mut state = ready_state();
        state.editor.set_text("/resume");
        assert!(matches!(
            state.submit_editor().as_slice(),
            [Effect::ListSessions]
        ));
        assert!(state.session_picker.is_some());

        state.close_session_picker();
        state.editor.set_text("/reload");
        assert!(matches!(
            state.submit_editor().as_slice(),
            [Effect::Backend(BackendCommand::Reload { session_id: None })]
        ));

        state.editor.set_text("/resume abc123");
        assert!(matches!(
            state.submit_editor().as_slice(),
            [Effect::ResolveSession(id)] if id == "abc123"
        ));
    }

    #[test]
    fn resumed_session_rebuilds_transcript_and_touches_metadata() {
        let mut state = ready_state();
        let session = SessionRecord {
            id: "01950000-0000-7000-8000-000000000000".to_owned(),
            provider: CODEX_PROVIDER.to_owned(),
            provider_session_id: "thread-resumed".to_owned(),
            workspace: "/tmp/project".to_owned(),
            title: "Previous work".to_owned(),
            model: Some("model-a".to_owned()),
            created_at: 1,
            updated_at: 2,
        };
        assert!(matches!(
            state.begin_resume(session.clone()).as_slice(),
            [Effect::Backend(BackendCommand::ResumeSession { .. })]
        ));

        let effects = state.handle_backend(BackendEvent::SessionResumed {
            provider_session_id: "thread-resumed".to_owned(),
            model: "model-a".to_owned(),
            history: vec![SessionHistoryItem {
                turn_id: "turn-1".to_owned(),
                item: NormalizedItem {
                    id: "user-1".to_owned(),
                    kind: ItemKind::User,
                    title: "YOU".to_owned(),
                    body: "hello".to_owned(),
                    status: ItemStatus::Complete,
                },
            }],
        });

        assert!(matches!(effects.as_slice(), [Effect::TouchSession(id)] if id == &session.id));
        assert_eq!(state.session_id.as_deref(), Some(session.id.as_str()));
        assert_eq!(state.provider_session_id.as_deref(), Some("thread-resumed"));
        assert_eq!(state.transcript.entries()[0].body, "hello");
    }

    #[test]
    fn model_switch_does_not_mutate_active_turn() {
        let mut state = ready_state();
        state.models.push(ModelInfo {
            provider: CODEX_PROVIDER.to_owned(),
            id: "model-b".to_owned(),
            is_default: false,
        });
        state.active_turn = Some(super::ActiveTurn {
            id: "turn-1".to_owned(),
            model: Some("model-a".to_owned()),
            cancelling: false,
        });
        let _ = state.open_model_picker();
        state.picker_move(1);
        let _ = state.picker_select();

        assert_eq!(
            state.selected_model.as_deref(),
            Some("openai-codex/model-b")
        );
        assert_eq!(
            state
                .active_turn
                .as_ref()
                .and_then(|turn| turn.model.as_deref()),
            Some("model-a")
        );
    }

    #[test]
    fn model_picker_merges_provider_qualified_catalogs_and_routes_selection() {
        let mut state = AppState::new("/tmp/project", None, 100);
        for (provider, name) in [(CODEX_PROVIDER, "Codex"), (DEVIN_PROVIDER, "Devin")] {
            state.handle_provider_backend(
                provider,
                BackendEvent::Ready(BackendIdentity {
                    provider: provider.to_owned(),
                    display_name: name.to_owned(),
                    version: None,
                    capabilities: BackendCapabilities {
                        model_catalog: CapabilitySupport::Supported,
                        ..BackendCapabilities::default()
                    },
                }),
            );
        }
        state.handle_provider_backend(
            CODEX_PROVIDER,
            BackendEvent::Models(vec![ModelInfo {
                provider: String::new(),
                id: "shared".to_owned(),
                is_default: true,
            }]),
        );
        state.handle_provider_backend(
            DEVIN_PROVIDER,
            BackendEvent::Models(vec![ModelInfo {
                provider: String::new(),
                id: "shared".to_owned(),
                is_default: true,
            }]),
        );

        let _ = state.open_model_picker();
        assert_eq!(
            state
                .filtered_models()
                .iter()
                .map(|model| model.qualified_id())
                .collect::<Vec<_>>(),
            vec!["devin-acp/shared", "openai-codex/shared"]
        );
        state.picker_move(-1);
        let _ = state.picker_select();
        assert_eq!(state.backend_provider, DEVIN_PROVIDER);
        assert_eq!(state.selected_model.as_deref(), Some("devin-acp/shared"));
    }
}
