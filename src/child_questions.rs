//! Parent-side views and answers reuse the child's canonical interaction. No question or owner
//! prompt is inserted into the parent, and reading retained children never restores a provider.
use nakode_protocol::{
    ChildQuestionAvailability, ChildQuestionSnapshot, ChildQuestions, Command, ErrorCode,
    InteractionId, InteractionKind, InteractionResolution, InteractionStatus, QuestionResponse,
    ServiceError, SessionId,
};

use crate::{child_reports::ReportStore, server::ServerCore, state::projection};

pub(crate) fn snapshot(
    store: &ReportStore,
    core: &ServerCore,
    parent: &SessionId,
) -> Result<ChildQuestionSnapshot, ServiceError> {
    let children = store
        .question_children(parent.as_str())?
        .into_iter()
        .map(|(id, title, closed)| {
            let id = SessionId::from(id);
            let engine = core.engine_for(&id);
            let availability = if closed {
                ChildQuestionAvailability::Closed
            } else if engine.is_some_and(|engine| engine.state().connection.is_ready()) {
                ChildQuestionAvailability::Live
            } else {
                ChildQuestionAvailability::Unavailable
            };
            let interactions = if availability == ChildQuestionAvailability::Live {
                engine
                    .map(|engine| projection::interactions(engine.state(), engine.revision()))
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|item| {
                        item.kind == InteractionKind::Question
                            && item.status == InteractionStatus::Pending
                    })
                    .collect()
            } else {
                Vec::new()
            };
            ChildQuestions {
                child_session_id: id,
                child_title: title,
                availability,
                interactions,
            }
        })
        .collect();
    Ok(ChildQuestionSnapshot {
        parent_session_id: parent.clone(),
        children,
    })
}

/// Called inside the serialized runtime request actor, immediately before the ordinary interaction
/// command. Parent, child and ask identity must ALL match. The child's normal validator owns the
/// full answer semantics and single-winner removal; this route never answers approvals.
pub(crate) fn answer_command(
    store: &ReportStore,
    core: &ServerCore,
    parent: &SessionId,
    child: &SessionId,
    interaction: &InteractionId,
    answers: Vec<QuestionResponse>,
) -> Result<Command, ServiceError> {
    let view = snapshot(store, core, parent)?;
    let linked = view
        .children
        .iter()
        .find(|item| &item.child_session_id == child)
        .ok_or_else(|| {
            refusal(
                ErrorCode::Conflict,
                "session is not an authorized child of this parent",
            )
        })?;
    if linked.availability != ChildQuestionAvailability::Live {
        return Err(refusal(
            ErrorCode::Conflict,
            "child is closed or unavailable; no answer was delivered",
        ));
    }
    if !linked
        .interactions
        .iter()
        .any(|item| &item.id == interaction)
    {
        return Err(refusal(
            ErrorCode::NotFound,
            "this exact child question is no longer pending; refresh before answering",
        ));
    }
    Ok(Command::ResolveInteraction {
        interaction_id: interaction.clone(),
        resolution: InteractionResolution::AnswerQuestions { answers },
    })
}

fn refusal(code: ErrorCode, message: &str) -> ServiceError {
    ServiceError {
        code,
        message: message.to_owned(),
        retryable: false,
    }
}
