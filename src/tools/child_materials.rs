//! Session-bound discovery over the same canonical material service used by public SDK clients.
use nakode_protocol::{ArtifactId, MaterialSource, QueryResult};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{Tool, ToolContext, ToolFuture, ToolResult};
use crate::backend::{NativeAgentRequest, NativeMaterialOperation, NativeMaterialRequest};
use crate::runtime::ToolDefinition;

pub struct ListChildMaterialsTool;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Arguments {
    source: MaterialSource,
    after_artifact_id: Option<ArtifactId>,
    limit: u32,
}

impl Tool for ListChildMaterialsTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "list_child_materials",
            description: "List one metadata-only page of retained transcript images from an authorized durable child session or an exact native delegated run. Source requires its canonical session_id and optional run_id. Owner/parent is server-bound, never a tool argument. Use prepare_image with the same source and a selected artifact_id as image_reference to inspect/attach it. Limit 1–64. No filesystem Gallery or cross-runtime source support yet; unavailable sources refuse. Does not load image bytes into model context.",
            parameters: json!({"type":"object","properties":{
                "source":source_schema(),
                "after_artifact_id":{"type":"string","minLength":1,"maxLength":200},
                "limit":{"type":"integer","minimum":1,"maximum":64}
            },"required":["source","limit"],"additionalProperties":false}),
        }
    }

    fn summarize(&self, _arguments: &Value) -> String {
        "List child material metadata".to_owned()
    }

    fn execute<'a>(
        &'a self,
        context: ToolContext<'a>,
        arguments: Value,
        cancellation: &'a CancellationToken,
    ) -> ToolFuture<'a> {
        Box::pin(async move {
            let requester = MaterialRequester::of(&context);
            let read = list(&requester, arguments);
            tokio::select! {
                result = read => match result { Ok(output) => ToolResult::success(output), Err(error) => ToolResult::failure(error) },
                () = cancellation.cancelled() => ToolResult::failure("material discovery cancelled"),
            }
        })
    }
}

pub(super) fn source_schema() -> Value {
    json!({"type":"object","properties":{
        "session_id":{"type":"string","minLength":1,"maxLength":200},
        "run_id":{"type":"string","minLength":1,"maxLength":200}
    },"required":["session_id"],"additionalProperties":false})
}

/// The identity a material read acts for, always bound by the runtime and never by tool
/// arguments: the logical owner session and, inside a delegated run, that run.
pub(crate) struct MaterialRequester<'a> {
    pub route: Option<&'a mpsc::Sender<NativeAgentRequest>>,
    pub owner_session_id: Option<String>,
    pub run_id: Option<String>,
}

impl<'a> MaterialRequester<'a> {
    pub(super) fn of(context: &ToolContext<'a>) -> Self {
        Self {
            route: context.delegation,
            owner_session_id: context.session.owner_session_id.clone(),
            run_id: context.session.parent_run_id.clone(),
        }
    }
}

/// `list_child_materials`: one metadata-only page from an authorized source.
pub(crate) async fn list(
    requester: &MaterialRequester<'_>,
    arguments: Value,
) -> Result<String, String> {
    let args: Arguments = serde_json::from_value(arguments).map_err(|error| error.to_string())?;
    let result = request(
        requester,
        args.source,
        NativeMaterialOperation::List {
            after: args.after_artifact_id,
            limit: args.limit,
        },
    )
    .await?;
    match result {
        QueryResult::ChildMaterials(page) => {
            serde_json::to_string(&page).map_err(|error| error.to_string())
        }
        _ => Err("unexpected material service response".to_owned()),
    }
}

pub(super) async fn request(
    requester: &MaterialRequester<'_>,
    source: MaterialSource,
    operation: NativeMaterialOperation,
) -> Result<QueryResult, String> {
    let owner_session_id = requester
        .owner_session_id
        .clone()
        .ok_or("material tool has no authoritative session")?;
    let route = requester.route.ok_or("material service is unavailable")?;
    let (respond, response) = tokio::sync::oneshot::channel();
    route
        .send(NativeAgentRequest::Material(NativeMaterialRequest {
            owner_session_id,
            requester_run_id: requester.run_id.clone(),
            source,
            operation,
            respond,
        }))
        .await
        .map_err(|_| "material service route closed")?;
    response
        .await
        .map_err(|_| "material service did not respond")?
}
