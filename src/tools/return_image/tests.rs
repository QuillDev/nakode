use super::*;
use crate::runtime::{ConversationItem, QuestionBroker, RuntimeSession, RuntimeSessionStore};

#[cfg(unix)]
#[tokio::test]
async fn image_open_rejects_a_parent_replaced_with_a_symlink_after_authorization() {
    let workspace = tempfile::tempdir().expect("workspace");
    let outside = tempfile::tempdir().expect("outside");
    let directory = workspace.path().join("images");
    std::fs::create_dir(&directory).expect("directory");
    std::fs::write(directory.join("image.png"), b"inside").expect("inside image");
    std::fs::write(outside.path().join("image.png"), b"outside").expect("outside image");
    let authorized = directory
        .join("image.png")
        .canonicalize()
        .expect("authorized path");
    std::fs::rename(&directory, workspace.path().join("original")).expect("move parent");
    std::os::unix::fs::symlink(outside.path(), &directory).expect("replacement");
    assert!(open_image(authorized).await.is_err());
}

#[tokio::test]
async fn returned_image_is_durable_and_does_not_put_bytes_in_provider_tool_output() {
    let workspace = tempfile::tempdir().expect("workspace");
    let data = b"\x89PNG\r\n\x1a\n";
    tokio::fs::write(workspace.path().join("image.png"), data)
        .await
        .expect("image");
    let mut session = RuntimeSession::new("test/model".into(), String::new());
    session.history.push(ConversationItem::User {
        text: String::new(),
        attachments: vec![PromptAttachment {
            label: "image.png".into(),
            path: None,
            image: Some(PromptImage {
                mime_type: "image/png".into(),
                data: data.to_vec(),
            }),
        }],
    });
    let (events, mut receiver) = tokio::sync::mpsc::channel(2);
    let questions = QuestionBroker::default();
    let result = ReturnImageTool
        .execute(
            ToolContext {
                workspace: workspace.path(),
                session: &mut session,
                backend_events: &events,
                turn_id: "turn",
                call_id: "image-call",
                questions: &questions,
                delegation: None,
            },
            json!({"path":"image.png"}),
            &CancellationToken::new(),
        )
        .await;
    assert!(!result.failed, "{}", result.output);
    assert!(!result.output.contains("base64"));
    let BackendEvent::ImageReturned(image) = receiver.recv().await.expect("event") else {
        panic!("image event");
    };
    assert_eq!(image.attachment.image.as_ref().expect("bytes").data, data);
    session.history.push(ConversationItem::ToolResult {
        call_id: "image-call".into(),
        name: Some("return_image".into()),
        arguments: None,
        audit_kind: None,
        title: None,
        output: result.output,
        model_output: None,
        failed: false,
        denied: false,
        denial_reason: None,
        duration_ms: None,
    });

    let database = workspace.path().join("history.sqlite");
    let _repository = crate::session::SqliteSessionRepository::open(&database).expect("migrate");
    let store = RuntimeSessionStore::new(database, "test");
    store.save(&session).expect("persist");
    tokio::fs::remove_file(workspace.path().join("image.png"))
        .await
        .expect("delete source");
    let restored = store.load(&session.id).expect("load").expect("session");
    let mut nested_history = restored.clone();
    nested_history.history.pop(); // Nested code-mode calls need not have their own ToolResult.
    assert!(
        nested_history
            .normalized_history()
            .iter()
            .any(|entry| entry.item.id == image.id)
    );
    let history = restored.normalized_history();
    assert_eq!(history[0].item.kind, crate::backend::ItemKind::User);
    assert!(history[0].item.body.is_empty());
    assert_eq!(
        history[0].attachments[0]
            .image
            .as_ref()
            .expect("owner bytes")
            .data,
        data
    );
    let assistant = history
        .iter()
        .find(|item| item.item.id == image.id)
        .expect("assistant image");
    assert_eq!(assistant.item.kind, crate::backend::ItemKind::Assistant);
    assert_eq!(
        assistant.attachments[0]
            .image
            .as_ref()
            .expect("reply bytes")
            .data,
        data
    );
}

#[tokio::test]
async fn return_image_refuses_escape_unsupported_and_oversized_files() {
    let workspace = tempfile::tempdir().expect("workspace");
    let outside = tempfile::tempdir().expect("outside");
    tokio::fs::write(outside.path().join("private.png"), b"\x89PNG\r\n\x1a\n")
        .await
        .expect("outside image");
    tokio::fs::write(workspace.path().join("active.svg"), b"<svg><script/></svg>")
        .await
        .expect("svg");
    tokio::fs::write(
        workspace.path().join("huge.png"),
        vec![0; usize::try_from(MAX_BYTES).expect("image limit fits usize") + 1],
    )
    .await
    .expect("large");
    let mut session = RuntimeSession::new("test/model".into(), String::new());
    let (events, mut receiver) = tokio::sync::mpsc::channel(2);
    let questions = QuestionBroker::default();
    for path in [
        outside.path().join("private.png"),
        workspace.path().join("active.svg"),
        workspace.path().join("huge.png"),
    ] {
        let result = ReturnImageTool
            .execute(
                ToolContext {
                    workspace: workspace.path(),
                    session: &mut session,
                    backend_events: &events,
                    turn_id: "turn",
                    call_id: "call",
                    questions: &questions,
                    delegation: None,
                },
                json!({"path":path}),
                &CancellationToken::new(),
            )
            .await;
        assert!(result.failed, "{path:?}");
    }
    assert!(session.returned_images.is_empty());
    assert!(receiver.try_recv().is_err());
}
