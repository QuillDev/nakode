use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use nakode::{
    backend::{BackendEvent, PromptAttachment},
    runtime::{
        AgentRuntime, ConversationItem, InferenceEvent, InferenceFuture, InferenceOutput,
        InferenceProvider, InferenceRequest, RuntimeSession, ToolCall,
    },
    tools::ToolResult,
};
use serde_json::json;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

struct CodeModeProvider {
    calls: AtomicUsize,
}

impl InferenceProvider for CodeModeProvider {
    fn infer(
        &self,
        request: InferenceRequest,
        _events: mpsc::Sender<InferenceEvent>,
        _cancellation: CancellationToken,
    ) -> InferenceFuture<'_> {
        Box::pin(async move {
            assert_eq!(request.tools.len(), 1);
            assert_eq!(request.tools[0].name, "codemode");
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                return Ok(InferenceOutput {
                    tool_calls: vec![ToolCall {
                        id: "outer-call".to_owned(),
                        name: "codemode".to_owned(),
                        arguments: json!({
                            "code": concat!(
                                "const mutation = await tools.write({path:'created.txt', content:'native mutation'});",
                                "const client = await tools.client_lookup({query:'client'});",
                                "const mcp = await tools['mcp__catalogue__lookup']({query:'mcp'});",
                                "return {mutation, client, mcp};"
                            )
                        }),
                    }],
                    ..InferenceOutput::default()
                });
            }

            let result = request.history.iter().rev().find_map(|item| match item {
                ConversationItem::ToolResult {
                    name: Some(name),
                    output,
                    failed,
                    ..
                } if name == "codemode" => Some((output, failed)),
                _ => None,
            });
            let (output, failed) =
                result.expect("Code Mode result reaches the next inference round");
            assert!(!failed, "outer Code Mode call should succeed: {output}");
            assert!(output.contains("client-result"));
            assert!(output.contains("mcp-result"));
            Ok(InferenceOutput {
                text: "complete".to_owned(),
                ..InferenceOutput::default()
            })
        })
    }
}

struct CancellationProvider;

impl InferenceProvider for CancellationProvider {
    fn infer(
        &self,
        request: InferenceRequest,
        _events: mpsc::Sender<InferenceEvent>,
        _cancellation: CancellationToken,
    ) -> InferenceFuture<'_> {
        Box::pin(async move {
            assert_eq!(request.tools.len(), 1);
            Ok(InferenceOutput {
                tool_calls: vec![ToolCall {
                    id: "cancelled-outer".to_owned(),
                    name: "codemode".to_owned(),
                    arguments: json!({
                        "code": "return await tools.mcp__catalogue__lookup({query:'wait'});"
                    }),
                }],
                ..InferenceOutput::default()
            })
        })
    }
}

#[tokio::test]
async fn cancelling_code_mode_interrupts_the_pending_host_call_and_worker() {
    let workspace = tempfile::tempdir().expect("workspace");
    let runtime = AgentRuntime::new(
        workspace.path().to_path_buf(),
        Arc::new(CancellationProvider),
    )
    .with_code_mode_worker_executable(env!("CARGO_BIN_EXE_nakode").into());
    let mut session = RuntimeSession::new("test-model".to_owned(), String::new());
    session.id = "cancelled-code-mode".to_owned();
    runtime
        .configure_external_tools(
            &session.id,
            vec![nakode_protocol::ExternalToolDefinition {
                name: "mcp__catalogue__lookup".to_owned(),
                description: "Pending MCP callback".to_owned(),
                input_schema_json: r#"{"type":"object"}"#.to_owned(),
            }],
            false,
            true,
            None,
            None,
            0,
            None,
        )
        .await
        .expect("configure Code Mode");
    let (event_tx, mut event_rx) = mpsc::channel(32);
    let cancellation = CancellationToken::new();
    let turn_cancellation = cancellation.clone();
    let turn_runtime = runtime.clone();
    let turn = tokio::spawn(async move {
        turn_runtime
            .run_turn(
                &mut session,
                "turn-cancel",
                "wait for MCP".to_owned(),
                Vec::new(),
                &event_tx,
                turn_cancellation,
            )
            .await
    });
    let request = loop {
        let event = tokio::time::timeout(std::time::Duration::from_secs(10), event_rx.recv())
            .await
            .expect("pending host call deadline")
            .expect("runtime event");
        if let BackendEvent::ExternalToolRequested(request) = event {
            break request;
        }
    };

    cancellation.cancel();
    let result = tokio::time::timeout(std::time::Duration::from_secs(10), turn)
        .await
        .expect("cancelled turn deadline")
        .expect("turn task");
    assert!(
        result.is_err(),
        "cancelled turn must not continue inference"
    );
    assert!(
        !runtime
            .resolve_external_tool(&request.id, ToolResult::success("late MCP result"))
            .await,
        "a late nested completion must not find a pending callback"
    );
}

#[tokio::test]
async fn code_mode_worker_composes_native_mutation_client_and_mcp_callbacks() {
    let workspace = tempfile::tempdir().expect("workspace");
    let provider = Arc::new(CodeModeProvider {
        calls: AtomicUsize::new(0),
    });
    let runtime = AgentRuntime::new(workspace.path().to_path_buf(), provider)
        .with_code_mode_worker_executable(env!("CARGO_BIN_EXE_nakode").into());
    let mut session = RuntimeSession::new("test-model".to_owned(), String::new());
    session.id = "code-mode-integration".to_owned();
    runtime
        .configure_external_tools(
            &session.id,
            vec![
                nakode_protocol::ExternalToolDefinition {
                    name: "client_lookup".to_owned(),
                    description: "Client callback".to_owned(),
                    input_schema_json: r#"{"type":"object"}"#.to_owned(),
                },
                nakode_protocol::ExternalToolDefinition {
                    name: "mcp__catalogue__lookup".to_owned(),
                    description: "Server-owned MCP callback".to_owned(),
                    input_schema_json: r#"{"type":"object"}"#.to_owned(),
                },
            ],
            false,
            true,
            None,
            None,
            0,
            None,
        )
        .await
        .expect("configure Code Mode");

    let (event_tx, mut event_rx) = mpsc::channel(64);
    let turn_runtime = runtime.clone();
    let turn = tokio::spawn(async move {
        let result = turn_runtime
            .run_turn(
                &mut session,
                "turn-1",
                "compose authorized tools".to_owned(),
                Vec::<PromptAttachment>::new(),
                &event_tx,
                CancellationToken::new(),
            )
            .await;
        (session, result)
    });

    let mut callbacks = 0;
    while callbacks < 2 {
        let event = tokio::time::timeout(std::time::Duration::from_secs(10), event_rx.recv())
            .await
            .expect("external callback deadline")
            .expect("runtime event");
        if let BackendEvent::ExternalToolRequested(request) = event {
            let output = match request.name.as_str() {
                "client_lookup" => "client-result",
                "mcp__catalogue__lookup" => "mcp-result",
                name => panic!("unexpected external tool request: {name}"),
            };
            assert!(
                runtime
                    .resolve_external_tool(&request.id, ToolResult::success(output))
                    .await
            );
            callbacks += 1;
        }
    }

    let (session, result) = turn.await.expect("turn task");
    result.expect("Code Mode turn completes");
    assert_eq!(
        std::fs::read_to_string(workspace.path().join("created.txt"))
            .expect("native mutation output"),
        "native mutation"
    );
    assert!(session.history.iter().any(|item| matches!(
        item,
        ConversationItem::ToolResult { name: Some(name), .. } if name == "codemode"
    )));
}
