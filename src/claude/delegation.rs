//! Server-routed Nakode delegation for Claude sessions.
//!
//! Claude's `mcp__nakode__delegate` tool runs inside the Node bridge. The bridge only names the
//! requested agent, title and task, plus the Claude turn and tool-use ids of the call. This module
//! binds that request to the logical owner session and active parent run recorded from the
//! control plane's own `StartSession`/`ResumeSession` commands, then sends it through the same
//! `NativeAgentRequest::Delegate` route that every in-process provider uses. The workspace server
//! stays the only owner of run creation, policy, nesting limits, persistence and attribution.
//! Nothing here starts a process.

use std::collections::HashMap;

use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::backend::{BackendCommand, NativeAgentRequest, NativeDelegationRequest};

/// Bridge request event emitted by the Claude `delegate` tool.
pub(crate) const DELEGATION_REQUEST_EVENT: &str = "delegation_request";
/// Bridge event emitted when Claude aborts a pending `delegate` tool call.
pub(crate) const DELEGATION_CANCELLED_EVENT: &str = "delegation_cancelled";

const COMPLETION_CAPACITY: usize = 32;

/// The logical Nakode identity a Claude backend acts for, taken only from control-plane commands.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct DelegationScope {
    owner_session_id: Option<String>,
    /// Active delegated run owning this provider session, absent for primary sessions.
    parent_run_id: Option<String>,
}

impl DelegationScope {
    /// Follows the session attachment commands. A server-issued resume is a primary resume
    /// (delegated runs are restored as interrupted), matching the in-process providers.
    pub(crate) fn observe(&mut self, command: &BackendCommand) {
        match command {
            BackendCommand::StartSession {
                owner_session_id,
                parent_run_id,
                ..
            } => {
                self.owner_session_id.clone_from(owner_session_id);
                self.parent_run_id.clone_from(parent_run_id);
            }
            BackendCommand::ResumeSession {
                owner_session_id, ..
            } => {
                self.owner_session_id.clone_from(owner_session_id);
                self.parent_run_id = None;
            }
            BackendCommand::UnsubscribeSession { .. } => *self = Self::default(),
            _ => {}
        }
    }
}

impl DelegationScope {
    /// The authenticated background-task caller for one bridge tool call, if this Claude session
    /// acts for a logical owner. Identity in the bridge payload is never consulted.
    pub(crate) fn background_task_caller(
        &self,
        turn_id: String,
        call_id: String,
    ) -> Option<crate::background_tasks::CallerScope> {
        Some(crate::background_tasks::CallerScope {
            owner: self.owner_session_id.clone()?,
            run_id: self.parent_run_id.clone(),
            turn_id: Some(turn_id).filter(|id| !id.is_empty()),
            call_id: Some(call_id).filter(|id| !id.is_empty()),
        })
    }
}

/// The terminal outcome of one delegation, ready to send back to the bridge.
#[derive(Debug)]
pub(crate) struct DelegationCompletion {
    pub(crate) id: String,
    pub(crate) result: Result<String, String>,
}

impl DelegationCompletion {
    pub(crate) fn bridge_reply(&self) -> Value {
        let (output, failed) = match &self.result {
            Ok(output) => (output.as_str(), false),
            Err(error) => (error.as_str(), true),
        };
        json!({
            "method": "resolve_delegation",
            "id": self.id,
            "output": output,
            "failed": failed,
        })
    }
}

struct PendingDelegation {
    turn_id: String,
    cancellation: CancellationToken,
}

/// Delegations a Claude backend has handed to the workspace server and not yet resolved.
pub(crate) struct ClaudeDelegations {
    route: Option<mpsc::Sender<NativeAgentRequest>>,
    pending: HashMap<String, PendingDelegation>,
    completions_tx: mpsc::Sender<DelegationCompletion>,
    completions_rx: mpsc::Receiver<DelegationCompletion>,
}

impl ClaudeDelegations {
    pub(crate) fn new(route: Option<mpsc::Sender<NativeAgentRequest>>) -> Self {
        let (completions_tx, completions_rx) = mpsc::channel(COMPLETION_CAPACITY);
        Self {
            route,
            pending: HashMap::new(),
            completions_tx,
            completions_rx,
        }
    }

    /// Starts one delegation from a bridge `delegation_request`.
    ///
    /// Returns an immediate completion when the request cannot be dispatched. Otherwise the
    /// terminal result arrives later through [`Self::next_completion`].
    pub(crate) fn start(
        &mut self,
        message: &Value,
        scope: &DelegationScope,
    ) -> Option<DelegationCompletion> {
        let id = text(message, "id");
        if id.is_empty() {
            return None;
        }
        let refuse = |error: &str| {
            Some(DelegationCompletion {
                id: id.clone(),
                result: Err(error.to_owned()),
            })
        };
        let Some(route) = self.route.clone() else {
            return refuse(
                "native delegation is unavailable because this Claude backend has no workspace server route",
            );
        };
        let Some(owner_session_id) = scope.owner_session_id.clone() else {
            return refuse(
                "native delegation is unavailable because this Claude session has no logical Nakode owner",
            );
        };
        let turn_id = text(message, "turnId");
        let call_id = text(message, "callId");
        if turn_id.is_empty() || call_id.is_empty() {
            return refuse("native delegation requires the invoking Claude turn and tool call");
        }
        if self.pending.contains_key(&id) {
            return refuse("native delegation request id is already in flight");
        }

        let cancellation = CancellationToken::new();
        let (respond, response) = oneshot::channel();
        let request = NativeAgentRequest::Delegate(NativeDelegationRequest {
            owner_session_id,
            parent_run_id: scope.parent_run_id.clone(),
            invocation_turn_id: turn_id.clone(),
            invocation_call_id: call_id,
            agent: text(message, "agent"),
            title: text(message, "title"),
            task: text(message, "task"),
            image_references: Vec::new(),
            cancellation: cancellation.clone(),
            respond,
        });
        self.pending.insert(
            id.clone(),
            PendingDelegation {
                turn_id,
                cancellation: cancellation.clone(),
            },
        );
        let completions = self.completions_tx.clone();
        tokio::spawn(async move {
            let result = dispatch(route, request, response, &cancellation).await;
            let _ = completions.send(DelegationCompletion { id, result }).await;
        });
        None
    }

    /// The next terminal result that still has a waiting bridge tool call.
    pub(crate) async fn next_completion(&mut self) -> DelegationCompletion {
        loop {
            let completion = self
                .completions_rx
                .recv()
                .await
                .expect("the delegation registry owns a completion sender");
            if self.pending.remove(&completion.id).is_some() {
                return completion;
            }
        }
    }

    /// Cancels one pending call; the server then interrupts its attributed child run.
    pub(crate) fn cancel(&mut self, id: &str) {
        if let Some(pending) = self.pending.remove(id) {
            pending.cancellation.cancel();
        }
    }

    pub(crate) fn cancel_turn(&mut self, turn_id: &str) {
        let ids = self
            .pending
            .iter()
            .filter(|(_, pending)| pending.turn_id == turn_id)
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        for id in ids {
            self.cancel(&id);
        }
    }

    pub(crate) fn cancel_all(&mut self) {
        for (_, pending) in self.pending.drain() {
            pending.cancellation.cancel();
        }
    }

    /// Applies the cancellation implied by a backend command before it reaches the bridge.
    pub(crate) fn observe_command(&mut self, command: &BackendCommand) {
        match command {
            BackendCommand::InterruptTurn { turn_id, .. } => self.cancel_turn(turn_id),
            BackendCommand::UnsubscribeSession { .. } | BackendCommand::Shutdown => {
                self.cancel_all();
            }
            _ => {}
        }
    }

    #[cfg(test)]
    fn pending_count(&self) -> usize {
        self.pending.len()
    }
}

async fn dispatch(
    route: mpsc::Sender<NativeAgentRequest>,
    request: NativeAgentRequest,
    response: oneshot::Receiver<Result<String, String>>,
    cancellation: &CancellationToken,
) -> Result<String, String> {
    tokio::select! {
        sent = route.send(request) => {
            if sent.is_err() {
                return Err("native delegation request channel closed".to_owned());
            }
        }
        () = cancellation.cancelled() => {
            return Err("native delegation cancelled before dispatch".to_owned());
        }
    }
    tokio::select! {
        result = response => match result {
            Ok(terminal) => terminal,
            Err(_) => Err("native delegation ended without a terminal result".to_owned()),
        },
        () = cancellation.cancelled() => Err(
            "native delegation cancelled; the workspace server will interrupt the attributed child run"
                .to_owned(),
        ),
    }
}

fn text(message: &Value, key: &str) -> String {
    message
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn start_session(owner: Option<&str>, parent: Option<&str>) -> BackendCommand {
        BackendCommand::StartSession {
            model: None,
            instructions: None,
            owner_session_id: owner.map(str::to_owned),
            parent_run_id: parent.map(str::to_owned),
            enabled_skill_ids: Vec::new(),
            external_tools: Vec::new(),
            replace_builtin_tools: false,
            code_mode: false,
            allowed_builtin_tools: None,
            max_turns: None,
            finalization_reserve_turns: 0,
            timeout_seconds: None,
        }
    }

    fn resume_session(owner: Option<&str>) -> BackendCommand {
        BackendCommand::ResumeSession {
            provider_session_id: "claude-session".to_owned(),
            owner_session_id: owner.map(str::to_owned),
            enabled_skill_ids: Vec::new(),
            external_tools: Vec::new(),
            replace_builtin_tools: false,
            code_mode: false,
            allowed_builtin_tools: None,
            max_turns: None,
            timeout_seconds: None,
        }
    }

    /// A bridge request that also tries to smuggle identity fields the server must ignore.
    fn bridge_request(id: &str, turn: &str) -> Value {
        json!({
            "event": DELEGATION_REQUEST_EVENT,
            "id": id,
            "turnId": turn,
            "callId": "toolu_proof",
            "agent": "repo-explorer",
            "title": "Read delegation proof marker",
            "task": "Read README.md and report the marker.",
            "ownerSessionId": "forged-owner",
            "parentRunId": "forged-parent",
        })
    }

    #[test]
    fn scope_follows_control_plane_attachment_commands() {
        let mut scope = DelegationScope::default();
        scope.observe(&start_session(Some("owner"), Some("run-parent")));
        assert_eq!(scope.owner_session_id.as_deref(), Some("owner"));
        assert_eq!(scope.parent_run_id.as_deref(), Some("run-parent"));

        // Ordinary turn commands never change the bound identity.
        scope.observe(&BackendCommand::InterruptTurn {
            provider_session_id: "claude-session".to_owned(),
            turn_id: "turn".to_owned(),
        });
        assert_eq!(scope.parent_run_id.as_deref(), Some("run-parent"));

        scope.observe(&resume_session(Some("owner")));
        assert_eq!(scope.owner_session_id.as_deref(), Some("owner"));
        assert_eq!(scope.parent_run_id, None);

        scope.observe(&BackendCommand::UnsubscribeSession {
            provider_session_id: "claude-session".to_owned(),
        });
        assert_eq!(scope, DelegationScope::default());
    }

    #[tokio::test]
    async fn request_uses_server_bound_identity_and_returns_the_terminal_result() {
        let (route, mut requests) = mpsc::channel(1);
        let mut delegations = ClaudeDelegations::new(Some(route));
        let mut scope = DelegationScope::default();
        scope.observe(&start_session(Some("owner"), Some("run-parent")));

        assert!(
            delegations
                .start(&bridge_request("call-1", "turn-1"), &scope)
                .is_none()
        );
        let Some(NativeAgentRequest::Delegate(request)) = requests.recv().await else {
            panic!("expected a delegation request");
        };
        assert_eq!(request.owner_session_id, "owner");
        assert_eq!(request.parent_run_id.as_deref(), Some("run-parent"));
        assert_eq!(request.invocation_turn_id, "turn-1");
        assert_eq!(request.invocation_call_id, "toolu_proof");
        assert_eq!(request.agent, "repo-explorer");
        assert_eq!(request.title, "Read delegation proof marker");
        assert_eq!(request.task, "Read README.md and report the marker.");
        assert!(request.image_references.is_empty());

        request
            .respond
            .send(Ok(
                "[Subagent Result] [agent-1] [repo-explorer]\nmarker".to_owned()
            ))
            .unwrap();
        let completion = delegations.next_completion().await;
        assert_eq!(completion.id, "call-1");
        assert_eq!(
            completion.bridge_reply(),
            json!({
                "method": "resolve_delegation",
                "id": "call-1",
                "output": "[Subagent Result] [agent-1] [repo-explorer]\nmarker",
                "failed": false,
            })
        );
        assert_eq!(delegations.pending_count(), 0);
    }

    #[tokio::test]
    async fn server_policy_rejection_reaches_the_bridge_as_a_failure() {
        let (route, mut requests) = mpsc::channel(1);
        let mut delegations = ClaudeDelegations::new(Some(route));
        let mut scope = DelegationScope::default();
        scope.observe(&start_session(Some("owner"), None));

        delegations.start(&bridge_request("call-1", "turn-1"), &scope);
        let Some(NativeAgentRequest::Delegate(request)) = requests.recv().await else {
            panic!("expected a delegation request");
        };
        request
            .respond
            .send(Err("agent \"repo-explorer\" is disabled".to_owned()))
            .unwrap();
        let reply = delegations.next_completion().await.bridge_reply();
        assert_eq!(reply["failed"], true);
        assert_eq!(reply["output"], "agent \"repo-explorer\" is disabled");
    }

    #[tokio::test]
    async fn requests_without_route_or_owner_fail_without_dispatch() {
        let mut scope = DelegationScope::default();
        scope.observe(&start_session(Some("owner"), None));
        let mut unrouted = ClaudeDelegations::new(None);
        let refused = unrouted
            .start(&bridge_request("call-1", "turn-1"), &scope)
            .expect("immediate refusal");
        assert!(
            refused
                .result
                .unwrap_err()
                .contains("no workspace server route")
        );

        let (route, mut requests) = mpsc::channel(1);
        let mut ownerless = ClaudeDelegations::new(Some(route));
        let refused = ownerless
            .start(
                &bridge_request("call-1", "turn-1"),
                &DelegationScope::default(),
            )
            .expect("immediate refusal");
        assert!(
            refused
                .result
                .unwrap_err()
                .contains("no logical Nakode owner")
        );
        assert!(requests.try_recv().is_err(), "nothing reached the server");
        assert_eq!(ownerless.pending_count(), 0);
    }

    #[tokio::test]
    async fn interrupting_the_turn_cancels_the_server_run_and_drops_the_reply() {
        let (route, mut requests) = mpsc::channel(1);
        let mut delegations = ClaudeDelegations::new(Some(route));
        let mut scope = DelegationScope::default();
        scope.observe(&start_session(Some("owner"), None));

        delegations.start(&bridge_request("call-1", "turn-1"), &scope);
        delegations.start(&bridge_request("call-2", "turn-2"), &scope);
        let Some(NativeAgentRequest::Delegate(first)) = requests.recv().await else {
            panic!("expected first request");
        };
        let Some(NativeAgentRequest::Delegate(second)) = requests.recv().await else {
            panic!("expected second request");
        };

        delegations.observe_command(&BackendCommand::InterruptTurn {
            provider_session_id: "claude-session".to_owned(),
            turn_id: "turn-1".to_owned(),
        });
        assert!(first.cancellation.is_cancelled());
        assert!(!second.cancellation.is_cancelled());
        assert_eq!(delegations.pending_count(), 1);

        delegations.cancel("call-2");
        assert!(second.cancellation.is_cancelled());
        assert_eq!(delegations.pending_count(), 0);
    }

    #[tokio::test]
    async fn shutdown_cancels_every_pending_delegation() {
        let (route, mut requests) = mpsc::channel(2);
        let mut delegations = ClaudeDelegations::new(Some(route));
        let mut scope = DelegationScope::default();
        scope.observe(&start_session(Some("owner"), None));
        delegations.start(&bridge_request("call-1", "turn-1"), &scope);
        let Some(NativeAgentRequest::Delegate(request)) = requests.recv().await else {
            panic!("expected request");
        };
        delegations.observe_command(&BackendCommand::Shutdown);
        assert!(request.cancellation.is_cancelled());
        assert_eq!(delegations.pending_count(), 0);
    }
}
