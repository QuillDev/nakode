//! Session-bound discovery over the same canonical material service used by public SDK clients.
use nakode_protocol::{ArtifactId, MaterialSource, QueryResult};
use serde::Deserialize;
use serde_json::{Value, json};
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
            let read = async {
                let args: Arguments =
                    serde_json::from_value(arguments).map_err(|error| error.to_string())?;
                let result = request(
                    &context,
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
            };
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

pub(super) async fn request(
    context: &ToolContext<'_>,
    source: MaterialSource,
    operation: NativeMaterialOperation,
) -> Result<QueryResult, String> {
    let owner_session_id = context
        .session
        .owner_session_id
        .clone()
        .ok_or("material tool has no authoritative session")?;
    let route = context
        .delegation
        .ok_or("material service is unavailable")?;
    let (respond, response) = tokio::sync::oneshot::channel();
    route
        .send(NativeAgentRequest::Material(NativeMaterialRequest {
            owner_session_id,
            requester_run_id: context.session.parent_run_id.clone(),
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
