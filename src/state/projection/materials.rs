//! Metadata discovery never clones image bytes, decodes images, or loads material into model context.
use nakode_protocol::{
    ArtifactId, ChildMaterial, EntryId, ErrorCode, MAX_MATERIAL_PAGE_SIZE, MaterialMetadata,
    MaterialPage, MaterialScope, Query, QueryResult, ServiceError,
};

use super::{DomainState, DomainTranscript, session_title, transcript_artifact_id};

pub(crate) fn project(state: &DomainState, query: Query) -> Result<QueryResult, ServiceError> {
    match query {
        Query::ListChildMaterials {
            scope,
            after,
            limit,
        } => list(state, scope, after.as_ref(), limit).map(QueryResult::ChildMaterials),
        Query::GetChildMaterial {
            scope,
            image_reference,
            transform,
        } => {
            let (transcript, run_title) = source(state, &scope)?;
            if image_reference.is_empty() || image_reference.len() > 4096 {
                return Err(failure(
                    ErrorCode::InvalidRequest,
                    "image reference must contain 1–4096 bytes",
                ));
            }
            let reference = match transform {
                Some(transform) => crate::image_handoff::Recipe {
                    source: image_reference,
                    crop: transform.crop,
                    max_width: transform.max_width,
                    max_height: transform.max_height,
                }
                .reference()
                .map_err(invalid)?,
                None => image_reference,
            };
            let recipe = crate::image_handoff::Recipe::parse(&reference).map_err(invalid)?;
            let original = recipe
                .as_ref()
                .map_or(reference.as_str(), |recipe| recipe.source.as_str());
            let exists = transcript.entries().iter().any(|entry| {
                transcript
                    .image_artifacts(entry)
                    .enumerate()
                    .any(|(index, _)| transcript_artifact_id(&entry.id, index).as_str() == original)
            });
            if !exists {
                return Err(failure(
                    ErrorCode::NotFound,
                    "material is unavailable in this exact source",
                ));
            }
            let artifact = crate::image_handoff::resolve_transcript(transcript, &reference)
                .map_err(invalid)?;
            Ok(QueryResult::ChildMaterial(ChildMaterial {
                session_title: bounded(&session_title(state, &[])),
                run_title,
                scope,
                artifact,
            }))
        }
        _ => Err(failure(ErrorCode::InvalidRequest, "not a material query")),
    }
}

fn source<'a>(
    state: &'a DomainState,
    scope: &MaterialScope,
) -> Result<(&'a DomainTranscript, Option<String>), ServiceError> {
    if scope.source.session_id.as_str() != state.nakode_session_id {
        return Err(failure(
            ErrorCode::NotFound,
            "material source session is unavailable",
        ));
    }
    match &scope.source.run_id {
        None => Ok((&state.transcript, None)),
        Some(id) => {
            let run = state
                .subagents
                .iter()
                .find(|run| run.id == id.as_str())
                .ok_or_else(|| failure(ErrorCode::NotFound, "run is not in this source session"))?;
            let chat = state.subagent_chats.get(id.as_str()).ok_or_else(|| {
                failure(ErrorCode::NotFound, "run material history is unavailable")
            })?;
            Ok((
                &chat.transcript,
                run.observability.title.as_deref().map(bounded),
            ))
        }
    }
}

fn list(
    state: &DomainState,
    scope: MaterialScope,
    after: Option<&ArtifactId>,
    limit: u32,
) -> Result<MaterialPage, ServiceError> {
    if !(1..=MAX_MATERIAL_PAGE_SIZE).contains(&limit) {
        return Err(failure(
            ErrorCode::InvalidRequest,
            "material page size must be 1–64",
        ));
    }
    let (transcript, run_title) = source(state, &scope)?;
    let mut cursor_seen = after.is_none();
    let mut items = Vec::new();
    let mut has_more = false;
    'entries: for entry in transcript.entries() {
        for (index, (label, image)) in transcript.image_artifacts(entry).enumerate() {
            let artifact_id = transcript_artifact_id(&entry.id, index);
            if !cursor_seen {
                cursor_seen = Some(&artifact_id) == after;
                continue;
            }
            if items.len() == limit as usize {
                has_more = true;
                break 'entries;
            }
            items.push(MaterialMetadata {
                artifact_id,
                entry_id: EntryId::from(entry.id.clone()),
                label: bounded(label),
                media_type: bounded(&image.mime_type),
                byte_length: image.data.len() as u64,
            });
        }
    }
    if !cursor_seen {
        return Err(failure(
            ErrorCode::NotFound,
            "material cursor is missing or belongs to another source; refresh discovery",
        ));
    }
    let next_after = if has_more {
        items.last().map(|item| item.artifact_id.clone())
    } else {
        None
    };
    Ok(MaterialPage {
        session_title: bounded(&session_title(state, &[])),
        scope,
        run_title,
        items,
        next_after,
    })
}

fn bounded(value: &str) -> String {
    value.chars().take(512).collect()
}
fn invalid(message: String) -> ServiceError {
    ServiceError {
        code: ErrorCode::InvalidRequest,
        message,
        retryable: false,
    }
}
fn failure(code: ErrorCode, message: &str) -> ServiceError {
    ServiceError {
        code,
        message: message.to_owned(),
        retryable: false,
    }
}
