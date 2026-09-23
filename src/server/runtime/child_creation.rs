//! Logical children are durable sessions, not native delegated runs. Creation checkpoints the
//! session, profile, optional bridge and parent link before runtime publication or provider work.
use super::{Effect, ErrorCode, ServerCore, ServiceError, SessionError, SessionRepository};
use crate::session::{SessionCreationContext, pending_provider_session_id};
use nakode_protocol::SessionId;

pub(super) fn persist_child_creation(
    core: &mut ServerCore,
    child: &SessionId,
    parent: &SessionId,
    title: Option<&str>,
    sessions: &dyn SessionRepository,
    effects: &mut Vec<Effect>,
) -> Result<(), ServiceError> {
    let state = core
        .engine_for_mut(child)
        .ok_or_else(|| ServiceError {
            code: ErrorCode::Internal,
            message: "created child session is unavailable".to_owned(),
            retryable: false,
        })?
        .state_mut();
    let bridge = effects.iter().find_map(|effect| match effect {
        Effect::PersistSessionBridge(bridge) => Some(bridge),
        _ => None,
    });
    let skills = state.enabled_skill_ids();
    let tools = state.session_tool_configuration();
    let record = sessions
        .create_with_account_id_and_skill_profile(
            child.as_str(),
            state.active_provider_id(),
            state.provider_account_id.as_deref(),
            &pending_provider_session_id(child.as_str()),
            &state.workspace,
            &state.working_directory,
            title.unwrap_or("New session"),
            state.selected_model.as_deref(),
            &state.selected_model_options(),
            Some(&skills),
            state.skill_profile_id(),
            Some(tools.code_mode),
            Some(&tools),
            None,
            SessionCreationContext {
                initial_instructions: state.initial_client_instructions(),
                parent_session_id: Some(parent.as_str()),
                bridge,
            },
        )
        .map_err(|error| match error {
            SessionError::ChildRelationship(error) => error,
            error => ServiceError {
                code: ErrorCode::Internal,
                message: format!("durable child creation failed: {error}"),
                retryable: true,
            },
        })?;
    state.session_persisted(&record);
    // This bridge was committed with the child, not as a later, separately failing write.
    effects.retain(|effect| !matches!(effect, Effect::PersistSessionBridge(_)));
    core.sessions.push(record);
    Ok(())
}
