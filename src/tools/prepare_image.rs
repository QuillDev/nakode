use serde::Deserialize;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::{Tool, ToolContext, ToolFuture, ToolResult};
use crate::{
    backend::{
        BackendEvent, NativeAgentRequest, NativeImageRequest, PromptAttachment, PromptImage,
    },
    image_handoff::{Crop, Recipe},
    runtime::{ReturnedImage, ToolDefinition},
};

pub struct PrepareImageTool;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Arguments {
    image_reference: String,
    source: Option<nakode_protocol::MaterialSource>,
    crop: Option<Crop>,
    max_width: Option<u32>,
    max_height: Option<u32>,
    #[serde(default)]
    inspect: bool,
}

impl Tool for PrepareImageTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "prepare_image",
            description: "Inspect image metadata or explicitly crop/downscale a conversation image before handoff. Uses authoritative image references, never file paths. Returns a reusable reference, dimensions, format, bytes and crop provenance; inspect attaches a preview to the transcript. Originals remain unchanged. PNG/JPEG transforms, PNG/JPEG/GIF/WebP originals, 5 MiB, 40 megapixels, 16384 pixels/side. Resizing preserves aspect ratio and never enlarges. Prefer focused crops for small text; visual token savings depend on the provider.",
            parameters: json!({"type":"object","properties":{
                "source":super::child_materials::source_schema(),
                "image_reference":{"type":"string","minLength":1,"description":"Original reference from Nakode Image References, or a selected artifact_id from list_child_materials. Child materials require the same explicit source; omitted source addresses only this conversation/run."},
                "crop":{"type":"object","properties":{"x":{"type":"integer","minimum":0},"y":{"type":"integer","minimum":0},"width":{"type":"integer","minimum":1},"height":{"type":"integer","minimum":1}},"required":["x","y","width","height"],"additionalProperties":false},
                "max_width":{"type":"integer","minimum":1,"maximum":16384},
                "max_height":{"type":"integer","minimum":1,"maximum":16384},
                "inspect":{"type":"boolean","description":"Attach the resolved image to the transcript for visual inspection; defaults false."}
            },"required":["image_reference"],"additionalProperties":false}),
        }
    }

    fn summarize(&self, _arguments: &Value) -> String {
        "Prepare conversation image".to_owned()
    }

    fn execute<'a>(
        &'a self,
        context: ToolContext<'a>,
        arguments: Value,
        cancellation: &'a CancellationToken,
    ) -> ToolFuture<'a> {
        Box::pin(async move {
            let result = async {
                let args: Arguments =
                    serde_json::from_value(arguments).map_err(|error| error.to_string())?;
                let recipe =
                    (args.crop.is_some() || args.max_width.is_some() || args.max_height.is_some())
                        .then_some(Recipe {
                            source: args.image_reference.clone(),
                            crop: args.crop,
                            max_width: args.max_width,
                            max_height: args.max_height,
                        });
                let reference = match &recipe {
                    Some(recipe) => recipe.reference()?,
                    None => args.image_reference,
                };
                let (artifact, source) = resolve_image(&context, reference, args.source).await?;
                let output = json!({
                    "image_reference": artifact.id,
                    "source": source.as_ref().map(|origin| &origin.source),
                    "origin": source,
                    "width": artifact.width,
                    "height": artifact.height,
                    "format": artifact.media_type,
                    "bytes": artifact.byte_length,
                    "provenance": Recipe::parse(artifact.id.as_str())?,
                    "preview_attached": args.inspect,
                })
                .to_string();
                if args.inspect {
                    let returned = context
                        .session
                        .returned_images
                        .values()
                        .filter(|image| image.turn_id == context.turn_id)
                        .collect::<Vec<_>>();
                    let bytes: usize = returned.iter().map(|image| image.image_bytes()).sum();
                    let count: usize = returned.iter().map(|image| image.image_count()).sum();
                    if count >= 8 || bytes.saturating_add(artifact.data.len()) > 20 * 1024 * 1024 {
                        return Err("preview limit is eight images / 20 MiB per turn".to_owned());
                    }
                    let image = ReturnedImage {
                        id: format!("{}:image:{}", context.turn_id, context.call_id),
                        turn_id: context.turn_id.to_owned(),
                        provider_id: context.session.provider_id.clone(),
                        model_id: context.session.model.clone(),
                        history_index: context.session.history_position(),
                        sequence: context.session.returned_images.len(),
                        attachment: PromptAttachment {
                            label: artifact.label,
                            path: None,
                            image: Some(PromptImage {
                                mime_type: artifact.media_type,
                                data: artifact.data,
                            }),
                        },
                        more_attachments: Vec::new(),
                    };
                    context
                        .backend_events
                        .send(BackendEvent::ImageReturned(image.clone()))
                        .await
                        .map_err(|_| "image transcript receiver closed")?;
                    context
                        .session
                        .returned_images
                        .insert(context.call_id.to_owned(), image);
                }
                Ok::<_, String>(output)
            };
            tokio::select! {
                result = result => match result { Ok(output) => ToolResult::success(output), Err(error) => ToolResult::failure(error) },
                () = cancellation.cancelled() => ToolResult::failure("image preparation cancelled"),
            }
        })
    }
}

async fn resolve_image(
    context: &ToolContext<'_>,
    reference: String,
    source: Option<nakode_protocol::MaterialSource>,
) -> Result<
    (
        nakode_protocol::ArtifactView,
        Option<nakode_protocol::MaterialScope>,
    ),
    String,
> {
    if let Some(source) = source {
        let value = super::child_materials::request(
            context,
            source,
            crate::backend::NativeMaterialOperation::Image { reference },
        )
        .await?;
        let nakode_protocol::QueryResult::ChildMaterial(mut material) = value else {
            return Err("unexpected material image response".to_owned());
        };
        let task = material
            .run_title
            .as_deref()
            .unwrap_or(&material.session_title);
        material.artifact.label = format!(
            "{} · {}: {}",
            task, material.scope.source.session_id, material.artifact.label
        );
        return Ok((material.artifact, Some(material.scope)));
    }
    let owner_session_id = context
        .session
        .owner_session_id
        .clone()
        .ok_or("image tool has no authoritative session")?;
    let route = context
        .delegation
        .ok_or("image service route is unavailable")?;
    let (respond, response) = tokio::sync::oneshot::channel();
    route
        .send(NativeAgentRequest::Image(NativeImageRequest {
            owner_session_id,
            requester_run_id: context.session.parent_run_id.clone(),
            reference,
            respond,
        }))
        .await
        .map_err(|_| "image service route closed")?;
    let artifact = response
        .await
        .map_err(|_| "image service did not respond")??;
    Ok((artifact, None))
}
