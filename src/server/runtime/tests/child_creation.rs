use super::*;
mod materials;
use crate::child_reports::ReportStore;
use crate::session::{SessionCreationContext, SessionRepository, pending_provider_session_id};
use nakode_protocol::{CommandAccepted, ServiceError, SessionBridgeIntent};

struct Harness {
    runtime: NativeServerRuntime,
    parent: SessionId,
    _handle: super::super::NativeServerHandle,
    directory: tempfile::TempDir,
}

async fn harness() -> Harness {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(".tmp");
    std::fs::create_dir_all(&root).unwrap();
    let directory = tempfile::tempdir_in(root).unwrap();
    let workspace = directory.path();
    let (persistence, _) = test_persistence(workspace);
    let mut state = DomainState::new_for_backend(
        workspace.to_string_lossy(),
        None,
        100,
        CODEX_PROVIDER,
        "Codex",
    );
    state.set_agent_directory(workspace.join("agents"));
    let parent = SessionId::from(state.nakode_session_id.clone());
    let record = persistence
        .sessions
        .create_with_account_id_and_skill_profile(
            parent.as_str(),
            CODEX_PROVIDER,
            None,
            "native-parent",
            &state.workspace,
            &state.workspace,
            "Parent Chat",
            None,
            &BackendModelOptions::default(),
            None,
            Some("profile"),
            None,
            None,
            None,
            SessionCreationContext::default(),
        )
        .unwrap();
    state.session_persisted(&record);
    state.set_skill_profile(Some("profile".to_owned()));
    let effects = EffectExecutor::new(empty_registry(workspace).await, persistence);
    let (runtime, handle) = NativeServerRuntime::from_parts(
        ServiceEngine::new(state),
        Vec::new(),
        vec![record],
        effects,
        mpsc::channel(1).1,
    );
    Harness {
        runtime,
        parent,
        _handle: handle,
        directory,
    }
}

fn creation(runtime: &NativeServerRuntime, parent: &SessionId) -> Command {
    Command::CreateSession {
        workspace_id: crate::state::projection::workspace_id(
            &runtime.core.engine().state().workspace,
        ),
        parent_session_id: Some(parent.clone()),
        working_directory: None,
        title: Some("Durable implementation".to_owned()),
        model_id: None,
        options: ModelOptions::default(),
        tools: None,
        initial_instructions: Some("Inert parent context, not new consent.".to_owned()),
        bridge: Some(SessionBridgeIntent {
            kind: OrchestratorKind::Agent,
            lifecycle: BridgeLifecycle::Open,
            display_title: "Durable implementation".to_owned(),
        }),
        mcp_grant: None,
        profile_id: Some("profile".to_owned()),
        disabled_skill_ids: Vec::new(),
        account_id: None,
    }
}

async fn send(
    runtime: &mut NativeServerRuntime,
    key: &str,
    command: Command,
    replay_only: bool,
) -> Result<CommandAccepted, ServiceError> {
    let endpoint = runtime.endpoint.clone();
    let request = endpoint.execute_command(
        ClientId::from("supervisor"),
        IdempotencyKey::from(key),
        None,
        replay_only,
        command,
    );
    let serve = async {
        let request = runtime.requests.recv().await.unwrap();
        runtime.handle_request(request).await;
    };
    let (result, ()) = tokio::join!(request, serve);
    result
}

fn children(runtime: &NativeServerRuntime, parent: &SessionId) -> Vec<(String, String, bool)> {
    ReportStore::open(&runtime.effects.persistence.database)
        .unwrap()
        .question_children(parent.as_str())
        .unwrap()
}

fn assert_parent_projection(runtime: &NativeServerRuntime, child: &str, parent: &SessionId) {
    let child_id = SessionId::from(child);
    assert_eq!(
        runtime
            .core
            .session_view(&child_id)
            .unwrap()
            .parent_session_id,
        Some(parent.clone())
    );
    let summaries = runtime.core.workspace_bootstrap().sessions;
    assert_eq!(
        summaries
            .iter()
            .find(|session| session.id == child_id)
            .unwrap()
            .parent_session_id,
        Some(parent.clone())
    );
    assert!(
        summaries
            .iter()
            .find(|session| session.id == *parent)
            .unwrap()
            .parent_session_id
            .is_none()
    );
}

#[tokio::test]
async fn parent_creation_persists_before_acceptance_and_replays_original_parent_identity() {
    let mut h = harness().await;
    let command = creation(&h.runtime, &h.parent);
    let accepted = send(&mut h.runtime, "create-child", command.clone(), false)
        .await
        .unwrap();
    let child = accepted.resource_id.as_deref().unwrap();
    assert_eq!(
        children(&h.runtime, &h.parent),
        vec![(child.to_owned(), "Durable implementation".to_owned(), false)]
    );
    let sessions = &h.runtime.effects.persistence.sessions;
    let saved = sessions.find(child).unwrap().unwrap();
    assert_eq!(saved.parent_session_id.as_deref(), Some(h.parent.as_str()));
    assert_parent_projection(&h.runtime, child, &h.parent);
    assert_eq!(
        saved.provider_session_id,
        pending_provider_session_id(child)
    );
    assert!(saved.owner_prompts.is_empty());
    assert_eq!(
        saved.initial_instructions.as_deref(),
        Some("Inert parent context, not new consent.")
    );
    assert_eq!(
        sessions.session_skill_profile(child).unwrap().as_deref(),
        Some("profile")
    );
    assert_eq!(sessions.list_session_bridges_all().unwrap().len(), 1);
    assert!(h.runtime.effects.backends.session_commands.is_empty());
    let cursor = h.runtime.endpoint.cursor();
    assert_eq!(
        send(&mut h.runtime, "create-child", command.clone(), true)
            .await
            .unwrap(),
        accepted
    );
    assert_eq!(h.runtime.endpoint.cursor(), cursor);
    assert_eq!(children(&h.runtime, &h.parent).len(), 1);
    let mut conflicting = command.clone();
    if let Command::CreateSession {
        parent_session_id, ..
    } = &mut conflicting
    {
        *parent_session_id = Some(SessionId::from("different-parent"));
    }
    assert_eq!(
        send(&mut h.runtime, "create-child", conflicting, false)
            .await
            .unwrap_err()
            .code,
        ErrorCode::Conflict
    );

    assert_eq!(
        send(&mut h.runtime, "missing-receipt", command, true)
            .await
            .unwrap_err()
            .code,
        ErrorCode::Conflict,
    );
    assert_eq!(children(&h.runtime, &h.parent).len(), 1);

    // Reopen an idle draft from storage, without inventing a first task or a native provider id.
    let repository =
        SqliteSessionRepository::open(&h.runtime.effects.persistence.database).unwrap();
    let restored = repository.find(child).unwrap().unwrap();
    assert_eq!(
        restored.parent_session_id.as_deref(),
        Some(h.parent.as_str())
    );
    let records = repository.list_recent_all().unwrap();
    assert_eq!(
        records
            .iter()
            .find(|record| record.id == child)
            .unwrap()
            .parent_session_id,
        restored.parent_session_id
    );
    let mut state =
        DomainState::new_for_backend(&restored.workspace, None, 100, CODEX_PROVIDER, "Codex");
    state.connection = crate::state::ConnectionState::Ready {
        server: "test".to_owned(),
    };
    assert!(state.begin_resume(restored).is_empty());
    let projection = crate::state::projection::bootstrap(&state, 1, &[], &records);
    assert_eq!(
        projection.active_session.unwrap().parent_session_id,
        Some(h.parent.clone())
    );
    assert_eq!(state.session_id.as_deref(), Some(child));
    assert!(state.provider_session_id.is_none());
    let effects = state
        .submit_prompt("The first real task".to_owned(), Vec::new())
        .unwrap();
    assert!(effects.iter().any(|effect| matches!(
        effect,
        crate::state::Effect::Backend(BackendCommand::StartSession { .. })
    )));
    assert_eq!(state.session_id.as_deref(), Some(child));
}

#[tokio::test]
async fn retained_parent_projection_survives_archive_and_reopen_without_guessing_standalone_links()
{
    let mut h = harness().await;
    let command = creation(&h.runtime, &h.parent);
    let accepted = send(&mut h.runtime, "linked-projection", command.clone(), false)
        .await
        .unwrap();
    let child = accepted.resource_id.unwrap();
    let mut standalone = command;
    if let Command::CreateSession {
        parent_session_id, ..
    } = &mut standalone
    {
        *parent_session_id = None;
    }
    let manual = send(&mut h.runtime, "standalone-projection", standalone, false)
        .await
        .unwrap()
        .resource_id
        .unwrap();
    let repository =
        SqliteSessionRepository::open(&h.runtime.effects.persistence.database).unwrap();
    // Parentless idle creation keeps the existing lazy persistence behavior.
    assert!(
        h.runtime
            .core
            .session_view(&SessionId::from(manual))
            .unwrap()
            .parent_session_id
            .is_none()
    );
    // The standalone parent fixture is already durable and must not gain a guessed link.
    let standalone = repository.find(h.parent.as_str()).unwrap().unwrap();
    let retained = h
        .runtime
        .core
        .query_retained_session(
            Query::GetSession {
                session_id: h.parent.clone(),
            },
            &standalone,
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
    let QueryResult::Session(standalone_view) = retained else {
        panic!("expected retained standalone session")
    };
    assert!(standalone_view.parent_session_id.is_none());
    let mut bridge = repository
        .list_session_bridges_all()
        .unwrap()
        .into_iter()
        .find(|bridge| bridge.session_id == child)
        .unwrap();
    for lifecycle in [BridgeLifecycle::Archived, BridgeLifecycle::Open] {
        bridge.lifecycle = lifecycle;
        bridge.revision += 1;
        repository.save_session_bridge(&bridge).unwrap();
        let record = repository.find(&child).unwrap().unwrap();
        let result = h
            .runtime
            .core
            .query_retained_session(
                Query::GetSession {
                    session_id: SessionId::from(child.clone()),
                },
                &record,
                Vec::new(),
                Vec::new(),
                Vec::new(),
            )
            .unwrap();
        let QueryResult::Session(view) = result else {
            panic!("expected retained session")
        };
        assert_eq!(view.parent_session_id, Some(h.parent.clone()));
        assert!(view.runs.is_empty());
    }
    assert!(h.runtime.effects.backends.session_commands.is_empty());
}

#[tokio::test]
async fn parent_creation_failure_rolls_back_rows_bridge_core_receipt_and_publication() {
    let mut h = harness().await;
    let connection = rusqlite::Connection::open(&h.runtime.effects.persistence.database).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER fail_child_link BEFORE INSERT ON session_child_links
         BEGIN SELECT RAISE(ABORT, 'injected child link failure'); END;",
        )
        .unwrap();
    let command = creation(&h.runtime, &h.parent);
    let cursor = h.runtime.endpoint.cursor();
    let error = send(
        &mut h.runtime,
        "retry-after-rollback",
        command.clone(),
        false,
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, ErrorCode::Internal);
    assert!(error.message.contains("injected child link failure"));
    assert_eq!(h.runtime.endpoint.cursor(), cursor);
    assert_eq!(h.runtime.core.sessions_by_id.len(), 1);
    assert!(h.runtime.effects.backends.session_commands.is_empty());
    for (table, expected) in [
        ("sessions", 1),
        ("session_skill_profiles", 1),
        ("session_bridges", 0),
        ("session_child_links", 0),
    ] {
        let count: i64 = connection
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, expected, "{table}");
    }
    connection
        .execute_batch("DROP TRIGGER fail_child_link;")
        .unwrap();
    assert!(
        send(&mut h.runtime, "retry-after-rollback", command, false)
            .await
            .is_ok()
    );
    assert_eq!(children(&h.runtime, &h.parent).len(), 1);
}

#[tokio::test]
async fn parent_creation_refuses_foreign_missing_closed_nested_and_overflow_parents() {
    let mut h = harness().await;
    for (key, profile, parent) in [
        ("foreign", "foreign-profile", h.parent.clone()),
        ("missing", "profile", SessionId::from("missing-parent")),
    ] {
        let mut command = creation(&h.runtime, &parent);
        if let Command::CreateSession { profile_id, .. } = &mut command {
            *profile_id = Some(profile.to_owned());
        }
        assert_eq!(
            send(&mut h.runtime, key, command, false)
                .await
                .unwrap_err()
                .code,
            ErrorCode::Conflict
        );
        assert_eq!(h.runtime.core.sessions_by_id.len(), 1);
    }
    let command = creation(&h.runtime, &h.parent);
    let child = send(&mut h.runtime, "valid", command, false)
        .await
        .unwrap()
        .resource_id
        .unwrap();
    let nested = creation(&h.runtime, &SessionId::from(child.clone()));
    assert!(
        send(&mut h.runtime, "nested", nested, false)
            .await
            .unwrap_err()
            .message
            .contains("nested")
    );
    for index in 1..32 {
        let command = creation(&h.runtime, &h.parent);
        send(&mut h.runtime, &format!("child-{index}"), command, false)
            .await
            .unwrap();
    }
    let command = creation(&h.runtime, &h.parent);
    assert!(
        send(&mut h.runtime, "overflow", command, false)
            .await
            .unwrap_err()
            .message
            .contains("32")
    );
    assert_eq!(h.runtime.core.sessions_by_id.len(), 33);
    assert_eq!(children(&h.runtime, &h.parent).len(), 32);
    let mut bridge = h
        .runtime
        .effects
        .persistence
        .sessions
        .list_session_bridges_all()
        .unwrap()
        .remove(0);
    // Archive the real parent, not one of its children.
    bridge.session_id = h.parent.to_string();
    bridge.lifecycle = BridgeLifecycle::Archived;
    h.runtime
        .effects
        .persistence
        .sessions
        .save_session_bridge(&bridge)
        .unwrap();
    let command = creation(&h.runtime, &h.parent);
    assert!(
        send(&mut h.runtime, "closed", command, false)
            .await
            .unwrap_err()
            .message
            .contains("closed")
    );
}

#[tokio::test]
async fn parent_creation_accepts_same_profile_other_workspace_without_a_bridge() {
    let mut h = harness().await;
    let connection = rusqlite::Connection::open(&h.runtime.effects.persistence.database).unwrap();
    connection
        .execute(
            "UPDATE sessions SET workspace = '/other-parent-workspace' WHERE id = ?1",
            [h.parent.as_str()],
        )
        .unwrap();
    let mut command = creation(&h.runtime, &h.parent);
    if let Command::CreateSession { bridge, .. } = &mut command {
        *bridge = None;
    }
    send(&mut h.runtime, "cross-workspace", command.clone(), false)
        .await
        .unwrap();
    assert_eq!(children(&h.runtime, &h.parent).len(), 1);
    assert!(
        h.runtime
            .effects
            .persistence
            .sessions
            .list_session_bridges_all()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        h.runtime
            .effects
            .persistence
            .sessions
            .find(h.parent.as_str())
            .unwrap()
            .unwrap()
            .provider_session_id,
        "native-parent"
    );

    // Without governing profiles, legacy same-workspace ownership rules still apply.
    connection
        .execute(
            "DELETE FROM session_skill_profiles WHERE session_id = ?1",
            [h.parent.as_str()],
        )
        .unwrap();
    if let Command::CreateSession { profile_id, .. } = &mut command {
        *profile_id = None;
    }
    assert_eq!(
        send(&mut h.runtime, "unbound-cross-workspace", command, false)
            .await
            .unwrap_err()
            .code,
        ErrorCode::Conflict
    );
    assert_eq!(h.runtime.core.sessions_by_id.len(), 2);
}

#[tokio::test]
async fn durable_children_and_direct_native_archetypes_keep_distinct_identities() {
    let mut h = harness().await;
    let directory = h.directory.path().join("agents");
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(directory.join("reviewer.toml"),
        "slug = 'reviewer'\ndescription = 'Review a bounded change'\nsystem_prompt = 'Review only'\nfirst_message = 'Reviewing'\nmodel = 'openai-codex/gpt-5'\nenabled = true\n",
    ).unwrap();
    let command = creation(&h.runtime, &h.parent);
    let child = SessionId::from(
        send(&mut h.runtime, "durable", command.clone(), false)
            .await
            .unwrap()
            .resource_id
            .unwrap(),
    );
    let mut run_ids = Vec::new();
    // Exercise the same canonical native delegation contract in both durable sessions. Do not
    // execute its provider effects: this test checks identity/lifecycle coexistence, not inference.
    for owner in [&h.parent, &child] {
        let (accepted, effects) = h
            .runtime
            .core
            .delegate_command(
                owner,
                "reviewer",
                "Review scope",
                "Read the scoped diff",
                None,
                &[],
            )
            .unwrap();
        assert!(!effects.is_empty());
        run_ids.push(accepted.resource_id.unwrap());
        assert_eq!(
            h.runtime
                .core
                .engine_for(owner)
                .unwrap()
                .state()
                .subagents
                .len(),
            1
        );
    }
    assert_ne!(run_ids[0], run_ids[1]);
    assert!(run_ids.iter().all(|id| id != child.as_str()));
    assert_eq!(h.runtime.core.sessions_by_id.len(), 2);
    assert_eq!(children(&h.runtime, &h.parent).len(), 1);
    let mut bare = h.runtime.core.clone();
    bare.durable_child_creation = false;
    assert!(
        bare.try_execute_command(command)
            .unwrap_err()
            .to_string()
            .contains("persistence runtime")
    );
}
