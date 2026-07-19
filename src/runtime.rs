use std::{future::Future, path::PathBuf, pin::Pin, sync::Arc};

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::backend::{
    BackendEvent, DeltaKind, ItemKind, ItemStatus, NormalizedItem, SessionHistoryItem, TodoPhase,
};
use crate::tools::ToolRegistry;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ConversationItem {
    User {
        text: String,
    },
    Assistant {
        text: String,
        reasoning: String,
        tool_calls: Vec<ToolCall>,
        #[serde(default)]
        signature: Option<String>,
        #[serde(default)]
        provider_state: Vec<Value>,
    },
    ToolResult {
        call_id: String,
        #[serde(default)]
        title: Option<String>,
        output: String,
        failed: bool,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: Value,
}

#[derive(Clone, Debug)]
pub struct InferenceRequest {
    pub session_id: String,
    pub model: String,
    pub instructions: String,
    pub history: Vec<ConversationItem>,
    pub tools: Vec<ToolDefinition>,
}

#[derive(Clone, Debug, Default)]
pub struct InferenceOutput {
    pub text: String,
    pub reasoning: String,
    pub tool_calls: Vec<ToolCall>,
    pub response_id: Option<String>,
    pub signature: Option<String>,
    pub provider_state: Vec<Value>,
}

#[derive(Clone, Debug)]
pub enum InferenceEvent {
    TextDelta(String),
    ReasoningDelta(String),
}

#[derive(Clone, Debug)]
pub struct ToolDefinition {
    pub name: &'static str,
    pub description: &'static str,
    pub parameters: Value,
}

pub type InferenceFuture<'a> =
    Pin<Box<dyn Future<Output = Result<InferenceOutput, String>> + Send + 'a>>;

pub trait InferenceProvider: Send + Sync {
    fn infer(
        &self,
        request: InferenceRequest,
        events: mpsc::Sender<InferenceEvent>,
        cancellation: CancellationToken,
    ) -> InferenceFuture<'_>;
}

#[derive(Clone)]
pub struct AgentRuntime {
    workspace: PathBuf,
    provider: Arc<dyn InferenceProvider>,
    tools: ToolRegistry,
    questions: Arc<QuestionBroker>,
}

impl AgentRuntime {
    #[must_use]
    pub fn new(workspace: PathBuf, provider: Arc<dyn InferenceProvider>) -> Self {
        Self {
            workspace,
            provider,
            tools: ToolRegistry::base(),
            questions: Arc::new(QuestionBroker::default()),
        }
    }

    pub async fn resolve_question(&self, id: &str, answer: String) -> bool {
        self.questions.resolve(id, answer).await
    }

    /// Runs one user turn through inference and any requested local tools.
    ///
    /// # Errors
    ///
    /// Returns an error when inference, tool execution, cancellation, or event delivery fails.
    pub async fn run_turn(
        &self,
        session: &mut RuntimeSession,
        turn_id: &str,
        prompt: String,
        backend_events: &mpsc::Sender<BackendEvent>,
        cancellation: CancellationToken,
    ) -> Result<(), String> {
        session
            .history
            .push(ConversationItem::User { text: prompt });
        backend_events
            .send(BackendEvent::TurnStarted {
                turn_id: turn_id.to_owned(),
            })
            .await
            .map_err(|_| "backend event receiver closed".to_owned())?;

        loop {
            if cancellation.is_cancelled() {
                return Err("turn interrupted".to_owned());
            }
            let (inference_tx, mut inference_rx) = mpsc::channel(256);
            let provider = Arc::clone(&self.provider);
            let request = InferenceRequest {
                session_id: session.id.clone(),
                model: session.model.clone(),
                instructions: session.instructions.clone(),
                history: session.history.clone(),
                tools: self.tools.definitions(),
            };
            let inference_cancellation = cancellation.clone();
            let inference = tokio::spawn(async move {
                provider
                    .infer(request, inference_tx, inference_cancellation)
                    .await
            });
            while let Some(event) = inference_rx.recv().await {
                let (item_id, kind, delta) = match event {
                    InferenceEvent::TextDelta(delta) => {
                        (format!("{turn_id}:assistant"), DeltaKind::Assistant, delta)
                    }
                    InferenceEvent::ReasoningDelta(delta) => {
                        (format!("{turn_id}:reasoning"), DeltaKind::Reasoning, delta)
                    }
                };
                backend_events
                    .send(BackendEvent::ItemDelta {
                        turn_id: turn_id.to_owned(),
                        item_id,
                        kind,
                        delta,
                    })
                    .await
                    .map_err(|_| "backend event receiver closed".to_owned())?;
            }
            let output = inference
                .await
                .map_err(|error| format!("inference task failed: {error}"))??;
            session.last_response_id.clone_from(&output.response_id);
            session.history.push(ConversationItem::Assistant {
                text: output.text,
                reasoning: output.reasoning,
                tool_calls: output.tool_calls.clone(),
                signature: output.signature,
                provider_state: output.provider_state,
            });
            if output.tool_calls.is_empty() {
                return Ok(());
            }
            self.execute_tool_calls(
                session,
                turn_id,
                output.tool_calls,
                backend_events,
                &cancellation,
            )
            .await?;
        }
    }

    async fn execute_tool_calls(
        &self,
        session: &mut RuntimeSession,
        turn_id: &str,
        tool_calls: Vec<ToolCall>,
        backend_events: &mpsc::Sender<BackendEvent>,
        cancellation: &CancellationToken,
    ) -> Result<(), String> {
        for tool_call in tool_calls {
            let item_id = format!("{turn_id}:tool:{}", tool_call.id);
            let Some(tool) = self.tools.find(&tool_call.name) else {
                session.history.push(ConversationItem::ToolResult {
                    call_id: tool_call.id,
                    title: Some(format!("{} · unavailable", tool_call.name)),
                    output: format!("unknown tool {}", tool_call.name),
                    failed: true,
                });
                continue;
            };
            let title = format!(
                "{} · {}",
                tool_call.name,
                tool.summarize(&tool_call.arguments)
            );
            backend_events
                .send(BackendEvent::ItemStarted {
                    turn_id: turn_id.to_owned(),
                    item: NormalizedItem {
                        id: item_id.clone(),
                        kind: ItemKind::Tool,
                        title: title.clone(),
                        body: String::new(),
                        status: ItemStatus::Running,
                    },
                })
                .await
                .map_err(|_| "backend event receiver closed".to_owned())?;
            let result = tool
                .execute(
                    crate::tools::ToolContext {
                        workspace: &self.workspace,
                        session,
                        backend_events,
                        turn_id,
                        questions: &self.questions,
                    },
                    tool_call.arguments,
                    cancellation,
                )
                .await;
            let body = result.output;
            let failed = result.failed;
            backend_events
                .send(BackendEvent::ItemCompleted {
                    turn_id: turn_id.to_owned(),
                    item: NormalizedItem {
                        id: item_id,
                        kind: ItemKind::Tool,
                        title: title.clone(),
                        body: body.clone(),
                        status: if failed {
                            ItemStatus::Failed
                        } else {
                            ItemStatus::Complete
                        },
                    },
                })
                .await
                .map_err(|_| "backend event receiver closed".to_owned())?;
            session.history.push(ConversationItem::ToolResult {
                call_id: tool_call.id,
                title: Some(title),
                output: body,
                failed,
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RuntimeSession {
    pub id: String,
    pub model: String,
    pub instructions: String,
    pub history: Vec<ConversationItem>,
    pub last_response_id: Option<String>,
    #[serde(default)]
    pub todos: Vec<TodoPhase>,
}

impl RuntimeSession {
    #[must_use]
    pub fn new(model: String, instructions: String) -> Self {
        Self {
            id: Uuid::now_v7().to_string(),
            model,
            instructions,
            history: Vec::new(),
            last_response_id: None,
            todos: Vec::new(),
        }
    }

    #[must_use]
    pub fn normalized_history(&self) -> Vec<SessionHistoryItem> {
        self.history
            .iter()
            .enumerate()
            .flat_map(|(index, item)| normalize_history_item(&self.id, index, item))
            .collect()
    }
}

#[derive(Default)]
pub struct QuestionBroker {
    pending:
        tokio::sync::Mutex<std::collections::HashMap<String, tokio::sync::oneshot::Sender<String>>>,
}

impl QuestionBroker {
    /// Publishes a question and waits for its answer or turn cancellation.
    ///
    /// # Errors
    ///
    /// Returns an error when the UI disconnects, dismisses the question, or the turn is cancelled.
    pub async fn ask(
        &self,
        request: crate::backend::QuestionRequest,
        events: &mpsc::Sender<BackendEvent>,
        cancellation: &CancellationToken,
    ) -> Result<String, String> {
        let (answer_tx, answer_rx) = tokio::sync::oneshot::channel();
        self.pending
            .lock()
            .await
            .insert(request.id.clone(), answer_tx);
        events
            .send(BackendEvent::QuestionRequested(request.clone()))
            .await
            .map_err(|_| "backend event receiver closed".to_owned())?;
        let answer = tokio::select! {
            answer = answer_rx => answer.map_err(|_| "question was dismissed".to_owned()),
            () = cancellation.cancelled() => Err("question interrupted".to_owned()),
        };
        self.pending.lock().await.remove(&request.id);
        answer
    }

    async fn resolve(&self, id: &str, answer: String) -> bool {
        self.pending
            .lock()
            .await
            .remove(id)
            .is_some_and(|sender| sender.send(answer).is_ok())
    }
}

fn normalize_history_item(
    session_id: &str,
    index: usize,
    item: &ConversationItem,
) -> Vec<SessionHistoryItem> {
    let turn_id = format!("{session_id}:history:{index}");
    let item_id = |suffix: &str| format!("{turn_id}:{suffix}");
    let normalized = |kind, title: &str, body: String, suffix: &str| SessionHistoryItem {
        turn_id: turn_id.clone(),
        item: NormalizedItem {
            id: item_id(suffix),
            kind,
            title: title.to_owned(),
            body,
            status: ItemStatus::Complete,
        },
    };
    match item {
        ConversationItem::User { text } => {
            vec![normalized(ItemKind::User, "You", text.clone(), "user")]
        }
        ConversationItem::Assistant {
            text, reasoning, ..
        } => {
            let mut items = Vec::new();
            if !reasoning.is_empty() {
                items.push(normalized(
                    ItemKind::Reasoning,
                    "Reasoning",
                    reasoning.clone(),
                    "reasoning",
                ));
            }
            if !text.is_empty() {
                items.push(normalized(
                    ItemKind::Assistant,
                    "Assistant",
                    text.clone(),
                    "assistant",
                ));
            }
            items
        }
        ConversationItem::ToolResult {
            title,
            output,
            failed,
            ..
        } => vec![SessionHistoryItem {
            turn_id: turn_id.clone(),
            item: NormalizedItem {
                id: item_id("tool"),
                kind: ItemKind::Tool,
                title: title.clone().unwrap_or_else(|| "Tool result".to_owned()),
                body: output.clone(),
                status: if *failed {
                    ItemStatus::Failed
                } else {
                    ItemStatus::Complete
                },
            },
        }],
    }
}

#[derive(Clone, Debug)]
pub struct RuntimeSessionStore {
    database: PathBuf,
    provider: String,
}

impl RuntimeSessionStore {
    #[must_use]
    pub fn new(database: PathBuf, provider: impl Into<String>) -> Self {
        Self {
            database,
            provider: provider.into(),
        }
    }

    /// Persists a native runtime session.
    ///
    /// # Errors
    /// Returns an error when serialization or `SQLite` persistence fails.
    pub fn save(&self, session: &RuntimeSession) -> Result<(), String> {
        let payload = serde_json::to_string(session)
            .map_err(|error| format!("failed to serialize native session: {error}"))?;
        self.connection()?
            .execute(
                "INSERT INTO native_runtime_sessions
                   (provider, session_id, session_json, updated_at)
                 VALUES (?1, ?2, ?3, unixepoch())
                 ON CONFLICT(provider, session_id) DO UPDATE SET
                   session_json = excluded.session_json,
                   updated_at = excluded.updated_at",
                params![self.provider, session.id, payload],
            )
            .map_err(|error| format!("failed to save native session: {error}"))?;
        Ok(())
    }

    /// Loads a persisted native runtime session.
    ///
    /// # Errors
    /// Returns an error when `SQLite` access or deserialization fails.
    pub fn load(&self, session_id: &str) -> Result<Option<RuntimeSession>, String> {
        let payload = self
            .connection()?
            .query_row(
                "SELECT session_json FROM native_runtime_sessions
                 WHERE provider = ?1 AND session_id = ?2",
                params![self.provider, session_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| format!("failed to load native session: {error}"))?;
        payload
            .map(|payload| {
                serde_json::from_str(&payload)
                    .map_err(|error| format!("invalid persisted native session: {error}"))
            })
            .transpose()
    }

    fn connection(&self) -> Result<Connection, String> {
        Connection::open(&self.database)
            .map_err(|error| format!("failed to open native session store: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use serde_json::json;

    use super::{
        AgentRuntime, InferenceFuture, InferenceOutput, InferenceProvider, InferenceRequest,
        QuestionBroker, RuntimeSession, RuntimeSessionStore, ToolCall,
    };
    use crate::backend::{BackendEvent, ItemKind, QuestionOption, QuestionRequest};
    use crate::session::SqliteSessionRepository;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    struct RepeatingToolProvider {
        calls: AtomicUsize,
        tool_rounds: usize,
    }

    impl InferenceProvider for RepeatingToolProvider {
        fn infer(
            &self,
            _request: InferenceRequest,
            _events: mpsc::Sender<super::InferenceEvent>,
            _cancellation: CancellationToken,
        ) -> InferenceFuture<'_> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if call < self.tool_rounds {
                    Ok(InferenceOutput {
                        tool_calls: vec![ToolCall {
                            id: format!("call-{call}"),
                            name: "todo".to_owned(),
                            arguments: json!({"op": "view"}),
                        }],
                        ..InferenceOutput::default()
                    })
                } else {
                    Ok(InferenceOutput {
                        text: "finished".to_owned(),
                        ..InferenceOutput::default()
                    })
                }
            })
        }
    }

    #[tokio::test]
    async fn turn_can_continue_beyond_the_legacy_tool_round_limit() {
        let directory = tempfile::tempdir().expect("workspace");
        let provider = Arc::new(RepeatingToolProvider {
            calls: AtomicUsize::new(0),
            tool_rounds: 65,
        });
        let runtime = AgentRuntime::new(directory.path().to_path_buf(), provider.clone());
        let mut session = RuntimeSession::new("test-model".to_owned(), "Test.".to_owned());
        let (events, _receiver) = mpsc::channel(512);

        runtime
            .run_turn(
                &mut session,
                "turn-1",
                "Keep working.".to_owned(),
                &events,
                CancellationToken::new(),
            )
            .await
            .expect("turn completes after more than 64 tool rounds");

        assert_eq!(provider.calls.load(Ordering::SeqCst), 66);
        let tool_items = session
            .normalized_history()
            .into_iter()
            .filter(|history| history.item.kind == ItemKind::Tool)
            .collect::<Vec<_>>();
        assert_eq!(tool_items.len(), 65);
        assert!(
            tool_items
                .iter()
                .all(|history| history.item.title == "todo · view")
        );
    }

    #[test]
    fn legacy_tool_history_omits_opaque_call_ids_when_resumed() {
        let tool_result: super::ConversationItem = serde_json::from_str(
            r#"{"ToolResult":{"call_id":"call_opaque","output":"ok","failed":false}}"#,
        )
        .expect("deserialize legacy tool result");
        let mut session = RuntimeSession::new("test-model".to_owned(), "Test.".to_owned());
        session.history.push(tool_result);

        let history = session.normalized_history();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].item.title, "Tool result");
        assert!(!history[0].item.title.contains("call_opaque"));
    }

    #[test]
    fn native_sessions_survive_provider_restarts() {
        let directory = tempfile::tempdir().expect("session directory");
        let database = directory.path().join("sessions.sqlite3");
        let _repository = SqliteSessionRepository::open(&database).expect("session repository");
        let store = RuntimeSessionStore::new(database, "test-provider");
        let session = RuntimeSession::new("test-model".to_owned(), "Be concise.".to_owned());

        store.save(&session).expect("save native session");
        let restored = store
            .load(&session.id)
            .expect("load native session")
            .expect("stored session");

        assert_eq!(restored.id, session.id);
        assert_eq!(restored.model, "test-model");
    }

    #[tokio::test]
    async fn question_broker_round_trips_an_interactive_answer() {
        let broker = Arc::new(QuestionBroker::default());
        let request = QuestionRequest {
            id: "question-1".to_owned(),
            title: "Direction".to_owned(),
            question: "Which path?".to_owned(),
            options: vec![
                QuestionOption {
                    label: "Direct".to_owned(),
                    description: None,
                },
                QuestionOption {
                    label: "Flexible".to_owned(),
                    description: None,
                },
            ],
            multi: false,
            recommended: None,
        };
        let (events, mut receiver) = mpsc::channel(1);
        let waiting_broker = Arc::clone(&broker);
        let waiting_request = request.clone();
        let answer = tokio::spawn(async move {
            waiting_broker
                .ask(waiting_request, &events, &CancellationToken::new())
                .await
        });

        assert!(
            matches!(receiver.recv().await, Some(BackendEvent::QuestionRequested(event_request)) if event_request == request)
        );
        assert!(broker.resolve("question-1", "Direct".to_owned()).await);
        assert_eq!(
            answer.await.expect("question task"),
            Ok("Direct".to_owned())
        );
    }
}
