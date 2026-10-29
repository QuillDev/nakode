//! Retained transcript images for Claude sessions: `list_child_materials` and `prepare_image`.
//!
//! The bridge names only the tool and its arguments. The owner session and delegated run come from
//! the control plane's own session commands, and the request travels the same
//! `NativeAgentRequest` route the native tools use, so the workspace server authorizes every
//! source and resolves every reference exactly as it does for them. An inspected preview is
//! attached by the backend loop under the turn's image bounds, and never for a turn that ended.

use std::collections::{HashMap, HashSet};

use serde_json::Value;
use tokio::sync::mpsc;

use super::delegation::DelegationScope;
use crate::backend::NativeAgentRequest;
use crate::tools::child_materials::MaterialRequester;
use crate::tools::prepare_image::Prepared;

/// Bridge request event emitted by the Claude material tools.
pub(crate) const MATERIAL_REQUEST_EVENT: &str = "material_request";

const COMPLETION_CAPACITY: usize = 16;

/// What one material call answered.
pub(crate) enum MaterialOutcome {
    Listed(String),
    Prepared(Prepared),
}

pub(crate) struct MaterialCompletion {
    pub(crate) id: String,
    pub(crate) turn_id: String,
    pub(crate) model: String,
    pub(crate) result: Result<MaterialOutcome, String>,
}

/// Material calls a Claude backend has handed to the workspace server and not yet answered.
pub(crate) struct ClaudeMaterials {
    route: Option<mpsc::Sender<NativeAgentRequest>>,
    /// Bridge call id to its turn.
    pending: HashMap<String, String>,
    /// Turns that were interrupted or ended: a request still queued for one is never started.
    closed_turns: HashSet<String>,
    completions_tx: mpsc::Sender<MaterialCompletion>,
    completions_rx: mpsc::Receiver<MaterialCompletion>,
}

impl ClaudeMaterials {
    pub(crate) fn new(route: Option<mpsc::Sender<NativeAgentRequest>>) -> Self {
        let (completions_tx, completions_rx) = mpsc::channel(COMPLETION_CAPACITY);
        Self {
            route,
            pending: HashMap::new(),
            closed_turns: HashSet::new(),
            completions_tx,
            completions_rx,
        }
    }

    /// Starts one call from a bridge `material_request`; its answer arrives through
    /// [`Self::next_completion`].
    pub(crate) fn start(&mut self, message: &Value, scope: &DelegationScope) {
        let id = text(message, "id");
        if id.is_empty() {
            return;
        }
        let turn_id = text(message, "turnId");
        if self.closed_turns.contains(&turn_id) {
            return;
        }
        let model = text(message, "model");
        let tool = text(message, "tool");
        let arguments = message.get("arguments").cloned().unwrap_or(Value::Null);
        let (owner_session_id, run_id) = scope.identity();
        let route = self.route.clone();
        self.pending.insert(id.clone(), turn_id.clone());
        let completions = self.completions_tx.clone();
        tokio::spawn(async move {
            let requester = MaterialRequester {
                route: route.as_ref(),
                owner_session_id,
                run_id,
            };
            let result = run(&requester, &tool, arguments).await;
            let _ = completions
                .send(MaterialCompletion {
                    id,
                    turn_id,
                    model,
                    result,
                })
                .await;
        });
    }

    /// The next answer whose call still waits: answers for interrupted or ended turns are dropped.
    pub(crate) async fn next_completion(&mut self) -> MaterialCompletion {
        loop {
            let completion = self
                .completions_rx
                .recv()
                .await
                .expect("the material registry owns a completion sender");
            if self.pending.remove(&completion.id).is_some() {
                return completion;
            }
        }
    }

    pub(crate) fn forget_turn(&mut self, turn_id: &str) {
        self.pending.retain(|_, turn| turn != turn_id);
        self.closed_turns.insert(turn_id.to_owned());
    }

    pub(crate) fn forget_all(&mut self) {
        self.pending.clear();
    }
}

async fn run(
    requester: &MaterialRequester<'_>,
    tool: &str,
    arguments: Value,
) -> Result<MaterialOutcome, String> {
    match tool {
        "list_child_materials" => crate::tools::child_materials::list(requester, arguments)
            .await
            .map(MaterialOutcome::Listed),
        "prepare_image" => crate::tools::prepare_image::prepare(requester, arguments)
            .await
            .map(MaterialOutcome::Prepared),
        other => Err(format!("unknown material tool {other:?}")),
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
    use serde_json::json;

    use super::*;
    use crate::backend::NativeMaterialOperation;
    use crate::claude::delegation::tests::start_session;

    fn bound_scope() -> DelegationScope {
        let mut scope = DelegationScope::default();
        scope.observe(&start_session(Some("owner"), Some("run-parent")));
        scope
    }

    /// A bridge request that also tries to smuggle identity fields the server must ignore.
    fn bridge_request(id: &str, turn: &str, tool: &str, arguments: &Value) -> Value {
        json!({
            "event": MATERIAL_REQUEST_EVENT,
            "id": id,
            "turnId": turn,
            "model": "claude-test",
            "tool": tool,
            "arguments": arguments,
            "ownerSessionId": "forged-owner",
            "parentRunId": "forged-parent",
        })
    }

    fn artifact(reference: String) -> nakode_protocol::ArtifactView {
        nakode_protocol::ArtifactView {
            id: nakode_protocol::ArtifactId::from(reference),
            label: "shot.png".to_owned(),
            media_type: "image/png".to_owned(),
            byte_length: 4,
            data: vec![1, 2, 3, 4],
            width: Some(2),
            height: Some(2),
        }
    }

    #[tokio::test]
    async fn requests_carry_the_server_bound_identity_not_the_payload() {
        let (route, mut requests) = mpsc::channel(1);
        let mut materials = ClaudeMaterials::new(Some(route));
        materials.start(
            &bridge_request(
                "call-1",
                "turn-1",
                "prepare_image",
                &json!({"image_reference": "artifact-1", "inspect": true}),
            ),
            &bound_scope(),
        );

        let Some(NativeAgentRequest::Image(request)) = requests.recv().await else {
            panic!("expected an image request");
        };
        assert_eq!(request.owner_session_id, "owner");
        assert_eq!(request.requester_run_id.as_deref(), Some("run-parent"));
        assert_eq!(request.reference, "artifact-1");
        request
            .respond
            .send(Ok(artifact(request.reference)))
            .unwrap();

        let completion = materials.next_completion().await;
        assert_eq!(completion.id, "call-1");
        assert_eq!(completion.turn_id, "turn-1");
        let Ok(MaterialOutcome::Prepared(prepared)) = completion.result else {
            panic!("expected a prepared image");
        };
        assert!(prepared.preview.is_some());
        let output: Value = serde_json::from_str(&prepared.output).unwrap();
        assert_eq!(output["image_reference"], "artifact-1");
        assert_eq!(output["preview_attached"], true);
    }

    #[tokio::test]
    async fn server_refusals_reach_the_model_unchanged() {
        for refusal in [
            "authorized material source is unavailable",
            "material source is outside this delegated run",
        ] {
            let (route, mut requests) = mpsc::channel(1);
            let mut materials = ClaudeMaterials::new(Some(route));
            materials.start(
                &bridge_request(
                    "call-1",
                    "turn-1",
                    "list_child_materials",
                    &json!({"source": {"session_id": "child"}, "limit": 8}),
                ),
                &bound_scope(),
            );
            let Some(NativeAgentRequest::Material(request)) = requests.recv().await else {
                panic!("expected a material request");
            };
            assert_eq!(request.owner_session_id, "owner");
            assert_eq!(request.requester_run_id.as_deref(), Some("run-parent"));
            assert_eq!(request.source.session_id.as_str(), "child");
            assert!(matches!(
                request.operation,
                NativeMaterialOperation::List {
                    after: None,
                    limit: 8
                }
            ));
            request.respond.send(Err(refusal.to_owned())).unwrap();

            let completion = materials.next_completion().await;
            assert_eq!(completion.result.err().as_deref(), Some(refusal));
        }
    }

    #[tokio::test]
    async fn calls_without_a_bound_owner_or_route_are_refused() {
        let (route, _requests) = mpsc::channel(1);
        let mut unbound = ClaudeMaterials::new(Some(route));
        unbound.start(
            &bridge_request(
                "call-1",
                "turn-1",
                "list_child_materials",
                &json!({"source": {"session_id": "child"}, "limit": 8}),
            ),
            &DelegationScope::default(),
        );
        assert_eq!(
            unbound.next_completion().await.result.err().as_deref(),
            Some("material tool has no authoritative session")
        );

        let mut unrouted = ClaudeMaterials::new(None);
        unrouted.start(
            &bridge_request(
                "call-2",
                "turn-1",
                "prepare_image",
                &json!({"image_reference": "artifact-1"}),
            ),
            &bound_scope(),
        );
        assert_eq!(
            unrouted.next_completion().await.result.err().as_deref(),
            Some("image service route is unavailable")
        );
    }

    #[tokio::test]
    async fn answers_for_a_forgotten_turn_are_dropped() {
        let (route, mut requests) = mpsc::channel(2);
        let mut materials = ClaudeMaterials::new(Some(route));
        let scope = bound_scope();
        let prepare = json!({"image_reference": "artifact-1"});
        materials.start(
            &bridge_request("old", "turn-1", "prepare_image", &prepare),
            &scope,
        );
        materials.forget_turn("turn-1");
        materials.start(
            &bridge_request("new", "turn-2", "prepare_image", &prepare),
            &scope,
        );
        for _ in 0..2 {
            let Some(NativeAgentRequest::Image(request)) = requests.recv().await else {
                panic!("expected an image request");
            };
            request
                .respond
                .send(Ok(artifact(request.reference)))
                .unwrap();
        }
        assert_eq!(materials.next_completion().await.id, "new");
    }

    #[tokio::test]
    async fn a_request_queued_behind_its_turn_ending_is_never_started() {
        let (route, mut requests) = mpsc::channel(1);
        let mut materials = ClaudeMaterials::new(Some(route));
        materials.forget_turn("turn-1");
        materials.start(
            &bridge_request(
                "late",
                "turn-1",
                "prepare_image",
                &json!({"image_reference": "artifact-1"}),
            ),
            &bound_scope(),
        );
        assert!(materials.pending.is_empty());
        assert!(requests.try_recv().is_err());
    }
}
