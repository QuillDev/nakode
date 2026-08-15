use futures_util::StreamExt;
use nakode_protocol::{RunOutcome, RunStatus, RunView};
use thiserror::Error;

use crate::{api_projection, config::Config, native_client};

#[derive(Debug, Error)]
pub enum AgentCliError {
    #[error("failed to start the native Nakode client: {0}")]
    NativeClientStart(String),
    #[error(transparent)]
    Sdk(#[from] nakode_sdk::SdkError),
    #[error("Nakode agent protocol error: {0}")]
    Protocol(String),
}

pub struct AgentCommandResult {
    pub output: String,
    pub success: bool,
}

/// Delegates one predefined agent task through the native server without a TUI.
///
/// # Errors
/// Returns service, protocol, transport, or command rejection failures.
pub async fn run(
    config: &Config,
    agent_slug: String,
    session_id: String,
    task: String,
    parent_run_id: Option<String>,
) -> Result<AgentCommandResult, AgentCliError> {
    let client = native_client::connect(config)
        .await
        .map_err(|error| AgentCliError::NativeClientStart(error.to_string()))?;
    delegate_and_wait(&client, agent_slug, session_id, task, parent_run_id).await
}

async fn delegate_and_wait(
    client: &nakode_sdk::NakodeClient,
    agent_slug: String,
    session_id: String,
    task: String,
    parent_run_id: Option<String>,
) -> Result<AgentCommandResult, AgentCliError> {
    client.get_session(session_id.clone()).await?;
    let run_id = client
        .delegate_attributed(session_id, agent_slug, task, parent_run_id, None)
        .await?;
    let mut updates = client.watch_run(run_id.clone());
    while let Some(update) = updates.next().await {
        let run = api_projection::run(update?).map_err(AgentCliError::Protocol)?;
        if let Some(result) = terminal_run_result(&run)? {
            return Ok(result);
        }
    }
    Err(AgentCliError::Protocol(format!(
        "run watch for {run_id} closed"
    )))
}

fn terminal_run_result(run: &RunView) -> Result<Option<AgentCommandResult>, AgentCliError> {
    let (body, success) = match (run.status, run.outcome.as_ref()) {
        (RunStatus::Starting | RunStatus::Working, None) => return Ok(None),
        (RunStatus::Completed, Some(RunOutcome::Completed { body })) => (body, true),
        (RunStatus::Failed, Some(RunOutcome::Failed { reason }))
        | (RunStatus::Interrupted, Some(RunOutcome::Interrupted { reason })) => (reason, false),
        (RunStatus::Starting | RunStatus::Working, Some(_)) => {
            return Err(AgentCliError::Protocol(format!(
                "non-terminal run {} included a terminal outcome",
                run.id
            )));
        }
        (RunStatus::Completed | RunStatus::Interrupted | RunStatus::Failed, None) => {
            return Err(AgentCliError::Protocol(format!(
                "terminal run {} omitted its semantic outcome",
                run.id
            )));
        }
        (status, Some(outcome)) => {
            return Err(AgentCliError::Protocol(format!(
                "run {} status {status:?} disagrees with outcome {outcome:?}",
                run.id
            )));
        }
    };
    Ok(Some(AgentCommandResult {
        output: format!(
            "[Subagent Result] [{}] [{}]\n{body}",
            run.id, run.agent_slug
        ),
        success,
    }))
}

#[cfg(test)]
mod tests {
    use nakode_protocol::{
        EntryId, ProviderId, RunId, RunOutcome, RunStatus, RunView, TranscriptEntryKind,
        TranscriptEntryStatus, TranscriptEntryView, TranscriptPage,
    };

    use super::terminal_run_result;

    #[test]
    fn completed_run_returns_the_server_result_as_success() {
        let mut run = run(RunStatus::Completed);
        run.outcome = Some(RunOutcome::Completed {
            body: "Validated the migration.".to_owned(),
        });
        run.result = Some("Stale legacy result.".to_owned());
        run.transcript.entries.push(entry(
            TranscriptEntryKind::Assistant,
            "Stale transcript result.",
            TranscriptEntryStatus::Complete,
        ));

        let result = terminal_run_result(&run)
            .expect("valid terminal outcome")
            .expect("completed run is terminal");

        assert!(result.success);
        assert_eq!(
            result.output,
            "[Subagent Result] [run-7] [reviewer]\nValidated the migration."
        );
    }

    #[test]
    fn failed_run_uses_the_server_owned_reason() {
        let mut run = run(RunStatus::Failed);
        run.outcome = Some(RunOutcome::Failed {
            reason: "Provider authentication expired.".to_owned(),
        });
        run.result = Some("Partial response before the provider failed.".to_owned());
        run.transcript.entries.extend([
            entry(
                TranscriptEntryKind::Assistant,
                "Partial response before the provider failed.",
                TranscriptEntryStatus::Complete,
            ),
            entry(
                TranscriptEntryKind::Error,
                "Stale transcript error.",
                TranscriptEntryStatus::Failed,
            ),
        ]);

        let result = terminal_run_result(&run)
            .expect("valid terminal outcome")
            .expect("failed run is terminal");

        assert!(!result.success);
        assert_eq!(
            result.output,
            "[Subagent Result] [run-7] [reviewer]\nProvider authentication expired."
        );
    }

    #[test]
    fn interrupted_run_uses_the_server_owned_reason() {
        let mut run = run(RunStatus::Interrupted);
        run.outcome = Some(RunOutcome::Interrupted {
            reason: "Interrupted by a client.".to_owned(),
        });

        let result = terminal_run_result(&run)
            .expect("valid terminal outcome")
            .expect("interrupted run is terminal");

        assert!(!result.success);
        assert_eq!(
            result.output,
            "[Subagent Result] [run-7] [reviewer]\nInterrupted by a client."
        );
    }

    #[test]
    fn terminal_run_without_an_outcome_is_a_protocol_error() {
        let run = run(RunStatus::Completed);

        let Err(super::AgentCliError::Protocol(message)) = terminal_run_result(&run) else {
            panic!("terminal run without an outcome should be rejected");
        };

        assert_eq!(message, "terminal run run-7 omitted its semantic outcome");
    }

    fn run(status: RunStatus) -> RunView {
        RunView {
            id: RunId::from("run-7"),
            parent_run_id: None,
            agent_slug: "reviewer".to_owned(),
            archetype_purpose: "Review focused changes".to_owned(),
            provider_id: ProviderId::from("openai-codex"),
            model_id: None,
            reasoning_effort: None,
            fast_mode: false,
            started_at_ms: 0,
            ended_at_ms: None,
            duration_ms: None,
            termination_kind: None,
            termination_detail: None,
            objective_mismatch_handoff: None,
            policy: nakode_protocol::RunPolicyView::default(),
            tool_denials: Vec::new(),
            tool_denials_retained_total: 0,
            native_session_id: None,
            usage: nakode_protocol::TokenUsageView {
                input_tokens: 0,
                output_tokens: 0,
                cached_input_tokens: 0,
                cache_write_tokens: 0,
            },
            objective: "Review the migration".to_owned(),
            objective_start_byte: 0,
            objective_total_bytes: 20,
            status,
            latest_activity: "Finished".to_owned(),
            latest_activity_start_byte: 0,
            latest_activity_total_bytes: 8,
            outcome: None,
            outcome_start_byte: 0,
            outcome_total_bytes: 0,
            result: None,
            result_start_byte: 0,
            result_total_bytes: 0,
            transcript: TranscriptPage {
                entries: Vec::new(),
                has_earlier: false,
                stream_active: false,
                stream_label: "reviewer".to_owned(),
            },
        }
    }

    fn entry(
        kind: TranscriptEntryKind,
        body: &str,
        status: TranscriptEntryStatus,
    ) -> TranscriptEntryView {
        TranscriptEntryView {
            id: EntryId::from(format!("entry-{body}")),
            kind,
            title: String::new(),
            body: body.to_owned(),
            body_start_byte: 0,
            body_total_bytes: u64::try_from(body.len()).unwrap_or(u64::MAX),
            status,
            artifacts: Vec::new(),
            provider_id: None,
            model_id: None,
            owner_turn_id: None,
            resolved_reasoning_effort: None,
            resolved_fast_mode: None,
            tool_audit_json: None,
            created_at_ms: None,
        }
    }
}
