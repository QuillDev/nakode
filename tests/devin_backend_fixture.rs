use std::{error::Error, ffi::OsString, io, path::PathBuf, time::Duration};

use flock::{
    backend::{
        ApprovalDecision, ApprovalKind, BackendCommand, BackendEvent, BackendHandle,
        BackendOperation, DEVIN_PROVIDER, DeltaKind, TurnOutcome,
    },
    devin::{self, BackendConfig},
};
use tokio::time::timeout;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[tokio::test]
async fn devin_acp_streams_turn_tools_permissions_and_resume_history() -> TestResult {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let fixture = manifest.join("tests/fixtures/fake_devin.py");
    let config = BackendConfig {
        program: PathBuf::from("python3"),
        args: vec![OsString::from(fixture)],
        workspace: manifest,
    };
    let mut backend = devin::spawn(config).await?;

    let identity = match next_event(&mut backend).await? {
        BackendEvent::Ready(identity) => identity,
        event => panic!("unexpected handshake event: {event:?}"),
    };
    assert_eq!(identity.provider, DEVIN_PROVIDER);
    assert_eq!(identity.display_name, "Fake Devin");
    assert!(identity.capabilities.resume);
    assert!(identity.capabilities.interruption);
    assert!(identity.capabilities.approvals);
    assert!(!identity.capabilities.steering);
    assert!(identity.capabilities.model_catalog);
    assert!(identity.capabilities.models_require_session);
    assert!(identity.capabilities.session_model_config);

    backend
        .commands
        .send(BackendCommand::Reload { session_id: None })
        .await?;
    let session_id = match next_event(&mut backend).await? {
        BackendEvent::SessionCreated {
            provider_session_id,
            model,
        } => {
            assert_eq!(model, "devin-fixture-model");
            provider_session_id
        }
        event => panic!("unexpected session event: {event:?}"),
    };
    assert_eq!(session_id, "devin-session-fixture");
    match next_event(&mut backend).await? {
        BackendEvent::Models(models) => {
            assert_eq!(models.len(), 2);
            assert_eq!(models[0].id, "devin-fixture-model");
            assert!(models[0].is_default);
        }
        event => panic!("unexpected model catalog event: {event:?}"),
    }

    backend
        .commands
        .send(BackendCommand::SetSessionModel {
            session_id: session_id.clone(),
            model: "devin-second-model".to_owned(),
        })
        .await?;
    match next_event(&mut backend).await? {
        BackendEvent::Models(models) => {
            assert!(
                models
                    .iter()
                    .any(|model| model.id == "devin-second-model" && model.is_default)
            );
        }
        event => panic!("unexpected model selection event: {event:?}"),
    }

    backend
        .commands
        .send(BackendCommand::Reload {
            session_id: Some(session_id.clone()),
        })
        .await?;
    assert!(matches!(
        next_event(&mut backend).await?,
        BackendEvent::Models(models) if models.len() == 2
    ));

    backend
        .commands
        .send(BackendCommand::StartTurn {
            session_id: session_id.clone(),
            client_id: "devin-turn-1".to_owned(),
            prompt: "hello devin".to_owned(),
            model: None,
        })
        .await?;

    let mut streamed = String::new();
    let mut saw_tool = false;
    let mut saw_tool_result = false;
    let mut completed = false;
    while !completed {
        match next_event(&mut backend).await? {
            BackendEvent::TurnAccepted { turn_id } => assert_eq!(turn_id, "devin-turn-1"),
            BackendEvent::ItemDelta {
                turn_id,
                item_id,
                kind,
                delta,
            } => {
                assert_eq!(turn_id, "devin-turn-1");
                assert_eq!(item_id, "devin-agent-message");
                assert_eq!(kind, DeltaKind::Assistant);
                streamed.push_str(&delta);
            }
            BackendEvent::ItemStarted { turn_id, item } => {
                assert_eq!(turn_id, "devin-turn-1");
                assert_eq!(item.id, "devin-tool");
                saw_tool = true;
            }
            BackendEvent::ApprovalRequested(approval) => {
                assert_eq!(approval.id, "devin-permission");
                assert_eq!(approval.kind, ApprovalKind::Command);
                assert!(approval.detail.contains("cargo test"));
                backend
                    .commands
                    .send(BackendCommand::ResolveApproval {
                        id: approval.id,
                        decision: ApprovalDecision::AcceptOnce,
                    })
                    .await?;
            }
            BackendEvent::ItemCompleted { turn_id, item } => {
                assert_eq!(turn_id, "devin-turn-1");
                assert_eq!(item.body, "tests passed");
                saw_tool_result = true;
            }
            BackendEvent::TurnCompleted {
                turn_id,
                outcome,
                error,
            } => {
                assert_eq!(turn_id, "devin-turn-1");
                assert_eq!(outcome, TurnOutcome::Completed);
                assert_eq!(error, None);
                completed = true;
            }
            event => panic!("unexpected turn event: {event:?}"),
        }
    }
    assert_eq!(streamed, "hello from Devin");
    assert!(saw_tool);
    assert!(saw_tool_result);

    backend
        .commands
        .send(BackendCommand::StartTurn {
            session_id: session_id.clone(),
            client_id: "devin-turn-failed".to_owned(),
            prompt: "fail prompt".to_owned(),
            model: None,
        })
        .await?;
    assert!(matches!(
        next_event(&mut backend).await?,
        BackendEvent::TurnAccepted { .. }
    ));
    assert!(matches!(
        next_event(&mut backend).await?,
        BackendEvent::RequestFailed {
            operation: BackendOperation::StartTurn,
            ..
        }
    ));
    match next_event(&mut backend).await? {
        BackendEvent::TurnCompleted {
            turn_id,
            outcome: TurnOutcome::Failed,
            error: Some(error),
        } => {
            assert_eq!(turn_id, "devin-turn-failed");
            assert!(error.contains("fixture prompt failure"));
        }
        event => panic!("unexpected failed-turn event: {event:?}"),
    }

    backend
        .commands
        .send(BackendCommand::SteerTurn {
            session_id: session_id.clone(),
            turn_id: "devin-turn-1".to_owned(),
            client_id: "steer".to_owned(),
            prompt: "unsupported".to_owned(),
        })
        .await?;
    match next_event(&mut backend).await? {
        BackendEvent::RequestFailed {
            operation: BackendOperation::SteerTurn,
            code: -32601,
            ..
        } => {}
        event => panic!("unexpected steer result: {event:?}"),
    }

    backend
        .commands
        .send(BackendCommand::ResumeSession {
            provider_session_id: session_id.clone(),
        })
        .await?;
    match next_event(&mut backend).await? {
        BackendEvent::SessionResumed {
            provider_session_id,
            history,
            ..
        } => {
            assert_eq!(provider_session_id, session_id);
            assert_eq!(history.len(), 2);
            assert_eq!(history[0].item.body, "saved ACP prompt");
            assert_eq!(history[1].item.body, "saved ACP response");
        }
        event => panic!("unexpected resume result: {event:?}"),
    }
    assert!(matches!(
        next_event(&mut backend).await?,
        BackendEvent::Models(models) if models.len() == 2
    ));

    backend
        .commands
        .send(BackendCommand::StartTurn {
            session_id: session_id.clone(),
            client_id: "devin-turn-cancel".to_owned(),
            prompt: "wait for cancel".to_owned(),
            model: None,
        })
        .await?;
    assert!(matches!(
        next_event(&mut backend).await?,
        BackendEvent::TurnAccepted { .. }
    ));
    backend
        .commands
        .send(BackendCommand::InterruptTurn {
            session_id: session_id.clone(),
            turn_id: "devin-turn-cancel".to_owned(),
        })
        .await?;
    let mut interrupted = false;
    while !interrupted {
        match next_event(&mut backend).await? {
            BackendEvent::InterruptAccepted => {}
            BackendEvent::TurnCompleted { outcome, .. } => {
                assert_eq!(outcome, TurnOutcome::Interrupted);
                interrupted = true;
            }
            event => panic!("unexpected cancellation event: {event:?}"),
        }
    }

    backend
        .commands
        .send(BackendCommand::UnsubscribeSession {
            provider_session_id: session_id,
        })
        .await?;
    assert!(matches!(
        next_event(&mut backend).await?,
        BackendEvent::SessionUnsubscribed
    ));

    backend.commands.send(BackendCommand::Shutdown).await?;
    timeout(Duration::from_secs(5), backend.join()).await?;
    Ok(())
}

#[tokio::test]
async fn cached_model_selection_is_applied_before_first_prompt() -> TestResult {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let fixture = manifest.join("tests/fixtures/fake_devin.py");
    let mut backend = devin::spawn(BackendConfig {
        program: PathBuf::from("python3"),
        args: vec![OsString::from(fixture)],
        workspace: manifest,
    })
    .await?;
    assert!(matches!(
        next_event(&mut backend).await?,
        BackendEvent::Ready(_)
    ));

    backend
        .commands
        .send(BackendCommand::StartSession {
            model: Some("devin-second-model".to_owned()),
        })
        .await?;
    match next_event(&mut backend).await? {
        BackendEvent::SessionCreated { model, .. } => {
            assert_eq!(model, "devin-second-model");
        }
        event => panic!("unexpected configured session event: {event:?}"),
    }
    assert!(matches!(
        next_event(&mut backend).await?,
        BackendEvent::Models(models)
            if models.iter().any(|model| model.id == "devin-second-model" && model.is_default)
    ));

    backend.commands.send(BackendCommand::Shutdown).await?;
    timeout(Duration::from_secs(5), backend.join()).await?;
    Ok(())
}

async fn next_event(backend: &mut BackendHandle) -> TestResult<BackendEvent> {
    let event = timeout(Duration::from_secs(5), backend.events.recv()).await?;
    event.ok_or_else(|| {
        io::Error::new(io::ErrorKind::UnexpectedEof, "Devin fixture stream ended").into()
    })
}
