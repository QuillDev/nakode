use std::{error::Error, ffi::OsString, io, path::PathBuf, time::Duration};

#[cfg(unix)]
use std::{fs::OpenOptions, io::Write, process::Command};

use flock::{
    backend::{
        ApprovalDecision, ApprovalKind, BackendCommand, BackendEvent, BackendHandle, DeltaKind,
    },
    codex::{self, BackendConfig},
};
use tokio::time::timeout;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[tokio::test]
async fn codex_client_completes_handshake_turn_stream_and_approval() -> TestResult {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let fixture = manifest.join("tests/fixtures/fake_codex.py");
    let config = BackendConfig {
        program: PathBuf::from("python3"),
        args: vec![OsString::from(fixture)],
        workspace: manifest,
    };
    let mut backend = codex::spawn(config).await?;

    let mut ready = false;
    let mut models = false;
    while !(ready && models) {
        match next_event(&mut backend).await? {
            BackendEvent::Ready(identity) => {
                assert_eq!(identity.display_name, "fake-codex/0.144.5");
                ready = true;
            }
            BackendEvent::Models(catalog) => {
                assert_eq!(catalog.len(), 1);
                assert_eq!(catalog[0].id, "fixture-model");
                models = true;
            }
            event => panic!("unexpected handshake event: {event:?}"),
        }
    }

    backend
        .commands
        .send(BackendCommand::Reload { session_id: None })
        .await?;
    assert!(matches!(
        next_event(&mut backend).await?,
        BackendEvent::Models(models) if models.len() == 1
    ));

    backend
        .commands
        .send(BackendCommand::StartSession {
            model: Some("fixture-model".to_owned()),
        })
        .await?;

    let provider_session_id = loop {
        match next_event(&mut backend).await? {
            BackendEvent::SessionCreated {
                provider_session_id,
                model,
            } => {
                assert_eq!(model, "fixture-model");
                break provider_session_id;
            }
            BackendEvent::SessionObserved { .. } => {}
            event => panic!("unexpected thread event: {event:?}"),
        }
    };
    assert_eq!(provider_session_id, "thread-fixture");

    backend
        .commands
        .send(BackendCommand::StartTurn {
            session_id: provider_session_id,
            client_id: "client-fixture".to_owned(),
            prompt: "hello fixture".to_owned(),
            model: Some("fixture-model".to_owned()),
        })
        .await?;

    let (streamed, final_text, steer_accepted) = observe_codex_turn(&mut backend).await?;

    assert_eq!(streamed, "hello world");
    assert_eq!(final_text.as_deref(), Some("hello world"));
    assert!(steer_accepted);
    backend.commands.send(BackendCommand::Shutdown).await?;
    timeout(Duration::from_secs(5), backend.join()).await?;
    Ok(())
}

async fn observe_codex_turn(
    backend: &mut BackendHandle,
) -> TestResult<(String, Option<String>, bool)> {
    let mut streamed = String::new();
    let mut final_text = None;
    let mut steer_accepted = false;
    loop {
        match next_event(backend).await? {
            BackendEvent::SessionObserved {
                provider_session_id,
            } => {
                assert_eq!(provider_session_id, "thread-fixture");
            }
            BackendEvent::TurnAccepted { turn_id } | BackendEvent::TurnStarted { turn_id } => {
                assert_eq!(turn_id, "turn-fixture");
            }
            BackendEvent::ItemStarted { turn_id, item } => {
                assert_eq!(turn_id, "turn-fixture");
                assert_eq!(item.id, "assistant-fixture");
            }
            BackendEvent::ItemDelta {
                turn_id,
                item_id,
                kind,
                delta,
            } => {
                assert_eq!(turn_id, "turn-fixture");
                assert_eq!(item_id, "assistant-fixture");
                assert_eq!(kind, DeltaKind::Assistant);
                streamed.push_str(&delta);
            }
            BackendEvent::ItemCompleted { turn_id, item } => {
                assert_eq!(turn_id, "turn-fixture");
                final_text = Some(item.body);
            }
            BackendEvent::ApprovalRequested(approval) => {
                assert_eq!(approval.kind, ApprovalKind::Command);
                assert!(approval.detail.contains("cargo test"));
                backend
                    .commands
                    .send(BackendCommand::SteerTurn {
                        session_id: "thread-fixture".to_owned(),
                        turn_id: "turn-fixture".to_owned(),
                        client_id: "steer-fixture".to_owned(),
                        prompt: "steer fixture".to_owned(),
                    })
                    .await?;
                backend
                    .commands
                    .send(BackendCommand::ResolveApproval {
                        id: approval.id,
                        decision: ApprovalDecision::AcceptOnce,
                    })
                    .await?;
            }
            BackendEvent::SteerAccepted { turn_id } => {
                assert_eq!(turn_id, "turn-fixture");
                steer_accepted = true;
            }
            BackendEvent::TurnCompleted { turn_id, error, .. } => {
                assert_eq!(turn_id, "turn-fixture");
                assert_eq!(error, None);
                break;
            }
            event => panic!("unexpected turn event: {event:?}"),
        }
    }
    Ok((streamed, final_text, steer_accepted))
}

#[tokio::test]
async fn codex_client_resumes_history_and_unsubscribes() -> TestResult {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let fixture = manifest.join("tests/fixtures/fake_codex.py");
    let config = BackendConfig {
        program: PathBuf::from("python3"),
        args: vec![OsString::from(fixture)],
        workspace: manifest,
    };
    let mut backend = codex::spawn(config).await?;

    let mut ready = false;
    let mut models = false;
    while !(ready && models) {
        match next_event(&mut backend).await? {
            BackendEvent::Ready { .. } => ready = true,
            BackendEvent::Models(_) => models = true,
            event => panic!("unexpected handshake event: {event:?}"),
        }
    }

    backend
        .commands
        .send(BackendCommand::ResumeSession {
            provider_session_id: "thread-fixture".to_owned(),
        })
        .await?;
    match next_event(&mut backend).await? {
        BackendEvent::SessionResumed {
            provider_session_id,
            model,
            history,
        } => {
            assert_eq!(provider_session_id, "thread-fixture");
            assert_eq!(model, "fixture-model");
            assert_eq!(history.len(), 2);
            assert_eq!(history[0].item.body, "saved prompt");
            assert_eq!(history[1].item.body, "saved response");
        }
        event => panic!("unexpected resume event: {event:?}"),
    }

    backend
        .commands
        .send(BackendCommand::UnsubscribeSession {
            provider_session_id: "thread-fixture".to_owned(),
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

#[cfg(unix)]
#[tokio::test]
async fn command_sent_before_initialize_is_deferred_not_dropped() -> TestResult {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let fixture = manifest.join("tests/fixtures/fake_codex.py");
    let gate_dir = tempfile::tempdir()?;
    let gate = gate_dir.path().join("initialize.fifo");
    let status = Command::new("mkfifo").arg(&gate).status()?;
    assert!(status.success());
    let config = BackendConfig {
        program: PathBuf::from("python3"),
        args: vec![
            OsString::from(fixture),
            OsString::from("--init-gate"),
            gate.clone().into_os_string(),
        ],
        workspace: manifest,
    };
    let mut backend = codex::spawn(config).await?;

    backend
        .commands
        .send(BackendCommand::StartSession {
            model: Some("fixture-model".to_owned()),
        })
        .await?;
    let mut gate_writer = OpenOptions::new().write(true).open(&gate)?;
    gate_writer.write_all(b"1")?;
    drop(gate_writer);

    let mut thread_created = false;
    while !thread_created {
        match next_event(&mut backend).await? {
            BackendEvent::Ready { .. }
            | BackendEvent::Models(_)
            | BackendEvent::SessionObserved { .. } => {}
            BackendEvent::SessionCreated {
                provider_session_id,
                ..
            } => {
                assert_eq!(provider_session_id, "thread-fixture");
                thread_created = true;
            }
            event => panic!("unexpected deferred-command event: {event:?}"),
        }
    }

    backend.commands.send(BackendCommand::Shutdown).await?;
    timeout(Duration::from_secs(5), backend.join()).await?;
    Ok(())
}

async fn next_event(backend: &mut BackendHandle) -> TestResult<BackendEvent> {
    let event = timeout(Duration::from_secs(5), backend.events.recv()).await?;
    event.ok_or_else(|| {
        io::Error::new(io::ErrorKind::UnexpectedEof, "Codex fixture stream ended").into()
    })
}
