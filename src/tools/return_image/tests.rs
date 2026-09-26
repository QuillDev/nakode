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

#[cfg(unix)]
#[tokio::test]
async fn a_gallery_image_is_returnable_by_absolute_path_and_nothing_else_outside_the_workspace() {
    let workspace = tempfile::tempdir().expect("workspace");
    let stack = tempfile::tempdir().expect("stack");
    let gallery = stack.path().join(".tmp-gallery").join("run");
    std::fs::create_dir_all(&gallery).expect("gallery");
    let png = b"\x89PNG\r\n\x1a\n";
    std::fs::write(gallery.join("shot.png"), png).expect("gallery image");
    std::fs::write(stack.path().join("secret.png"), png).expect("outside image");
    // An image in a gallery is returned by its absolute path.
    let loaded =
        load_returnable_image(workspace.path(), gallery.join("shot.png").to_str().unwrap())
            .await
            .expect("gallery image");
    assert_eq!(loaded.label, "shot.png");
    assert_eq!(loaded.mime_type, "image/png");
    // Outside both the workspace and any gallery, it is refused.
    let outside = load_returnable_image(
        workspace.path(),
        stack.path().join("secret.png").to_str().unwrap(),
    )
    .await;
    assert!(outside.is_err());
    // A gallery symlink to a file elsewhere is judged by where it leads.
    std::os::unix::fs::symlink(stack.path().join("secret.png"), gallery.join("link.png"))
        .expect("symlink");
    let escaped =
        load_returnable_image(workspace.path(), gallery.join("link.png").to_str().unwrap()).await;
    assert!(escaped.is_err());
}

#[test]
fn a_call_names_one_path_or_up_to_eight_distinct_paths() {
    assert_eq!(
        requested_image_paths(&json!({"path":"a.png"})).unwrap(),
        vec!["a.png".to_owned()]
    );
    assert_eq!(
        requested_image_paths(&json!({"paths":["a.png","b.png"]})).unwrap(),
        vec!["a.png".to_owned(), "b.png".to_owned()]
    );
    for bad in [
        json!({}),
        json!({"path":"a.png","paths":["b.png"]}),
        json!({"paths":[]}),
        json!({"paths":["a.png","a.png"]}),
        json!({"paths":[1]}),
        json!({"paths":["1","2","3","4","5","6","7","8","9"]}),
    ] {
        assert!(requested_image_paths(&bad).is_err(), "{bad}");
    }
}

async fn call(
    workspace: &std::path::Path,
    session: &mut RuntimeSession,
    events: &tokio::sync::mpsc::Sender<BackendEvent>,
    questions: &QuestionBroker,
    call_id: &str,
    arguments: Value,
) -> ToolResult {
    let context = ToolContext {
        workspace,
        session,
        backend_events: events,
        turn_id: "turn",
        call_id,
        questions,
        delegation: None,
    };
    ReturnImageTool
        .execute(context, arguments, &CancellationToken::new())
        .await
}

#[tokio::test]
async fn several_images_attach_in_order_in_one_call_or_not_at_all() {
    let workspace = tempfile::tempdir().expect("workspace");
    let png = b"\x89PNG\r\n\x1a\n";
    for name in ["one.png", "two.png"] {
        std::fs::write(workspace.path().join(name), png).expect("image");
    }
    std::fs::write(workspace.path().join("notes.txt"), b"not an image").expect("text");
    let mut session = RuntimeSession::new("test/model".into(), String::new());
    let (events, mut receiver) = tokio::sync::mpsc::channel(8);
    let questions = QuestionBroker::default();
    // One unreadable image stops the whole call before anything is attached.
    let failed = call(
        workspace.path(),
        &mut session,
        &events,
        &questions,
        "mixed",
        json!({"paths":["one.png","notes.txt"]}),
    )
    .await;
    assert!(failed.failed);
    assert!(failed.output.contains("notes.txt"), "{}", failed.output);
    assert!(receiver.try_recv().is_err());
    let result = call(
        workspace.path(),
        &mut session,
        &events,
        &questions,
        "pair",
        json!({"paths":["one.png","two.png"]}),
    )
    .await;
    assert!(!result.failed, "{}", result.output);
    assert_eq!(
        result.output,
        "2 images attached to the assistant transcript."
    );
    // One call is one message: a single reply carries both images, in the order named.
    let Ok(BackendEvent::ImageReturned(image)) = receiver.try_recv() else {
        panic!("one image reply");
    };
    assert!(receiver.try_recv().is_err());
    let labels: Vec<_> = image.attachments().map(|a| a.label.clone()).collect();
    assert_eq!(labels, vec!["one.png".to_owned(), "two.png".to_owned()]);
    assert_eq!(image.history_item().attachments.len(), 2);
    assert_eq!(session.returned_images.len(), 1);
}
