use super::*;

fn parent(
    h: &mut Harness,
    title: &str,
    call: &str,
    previous: Option<&str>,
    revision: u64,
) -> SessionId {
    let workspace = h.state().workspace.clone();
    let mut state = DomainState::new_for_backend(&workspace, None, 100, CODEX_PROVIDER, "Codex");
    let record = h
        .runtime
        .effects
        .persistence
        .sessions
        .create_with_id(
            &state.nakode_session_id,
            CODEX_PROVIDER,
            title,
            &workspace,
            &workspace,
            title,
            None,
            &BackendModelOptions::default(),
            None,
        )
        .unwrap();
    h.runtime
        .effects
        .persistence
        .sessions
        .bind_session_skill_profile(&record.id, "profile")
        .unwrap();
    state.session_persisted(&record);
    let mut arguments = serde_json::json!({"sessionId": h.session.as_str(), "expectedRelationshipRevision": revision});
    if let Some(previous) = previous {
        arguments["expectedParentSessionId"] = previous.into();
    }
    state
        .external_tool_calls
        .push(crate::backend::ExternalToolRequest {
            id: call.into(),
            name: if previous.is_some() {
                "TransferAgent"
            } else {
                "ClaimAgent"
            }
            .into(),
            arguments_json: arguments.to_string(),
        });
    let id = SessionId::from(record.id);
    h.runtime
        .core
        .sessions_by_id
        .insert(id.clone(), ServiceEngine::new(state));
    id
}

#[tokio::test]
async fn relationship_calls_authenticate_exact_source_and_preserve_running_turn_and_projection() {
    let mut h = Harness::new().await;
    h.runtime
        .effects
        .persistence
        .sessions
        .bind_session_skill_profile(h.session.as_str(), "profile")
        .unwrap();
    let old = parent(&mut h, "old", "claim", None, 0);
    let claim = Command::ReparentChildSession {
        source_session_id: old.clone(),
        source_call_id: "claim".into(),
        child_session_id: h.session.clone(),
        expected_parent_session_id: None,
        expected_relationship_revision: 0,
        transfer: false,
    };
    let mut forged = claim.clone();
    if let Command::ReparentChildSession {
        expected_relationship_revision,
        ..
    } = &mut forged
    {
        *expected_relationship_revision = 8;
    }
    assert!(
        h.command("forged", None, false, forged)
            .await
            .unwrap_err()
            .message
            .contains("exact pending")
    );
    h.command("claim", None, false, claim.clone())
        .await
        .unwrap();
    let new = parent(&mut h, "new", "transfer", Some(old.as_str()), 1);
    h.enqueue("start-child").await;
    h.runtime.dispatch_followups().await;
    let _ = h.start();
    let turn = h
        .runtime
        .core
        .session_view(&h.session)
        .unwrap()
        .active_turn
        .unwrap()
        .id;
    let before = h
        .runtime
        .core
        .engine_for(&h.session)
        .unwrap()
        .state()
        .transcript
        .entries()
        .to_vec();
    let transfer = Command::ReparentChildSession {
        source_session_id: new.clone(),
        source_call_id: "transfer".into(),
        child_session_id: h.session.clone(),
        expected_parent_session_id: Some(old.clone()),
        expected_relationship_revision: 1,
        transfer: true,
    };
    h.command("transfer", None, false, transfer.clone())
        .await
        .unwrap();
    assert!(
        h.commands.try_recv().is_err(),
        "no cancellation, restart or duplicated prompt"
    );
    assert_eq!(
        h.runtime
            .core
            .engine_for(&h.session)
            .unwrap()
            .state()
            .transcript
            .entries(),
        before
    );
    let snapshot = h.runtime.core.session_view(&h.session).unwrap();
    assert_eq!(snapshot.parent_session_id, Some(new.clone()));
    assert_eq!(snapshot.relationship_revision, Some(2));
    assert_eq!(snapshot.active_turn.unwrap().id.as_str(), turn.as_str());
    let record = h
        .runtime
        .effects
        .persistence
        .sessions
        .find(h.session.as_str())
        .unwrap()
        .unwrap();
    assert_eq!(record.parent_session_id.as_deref(), Some(new.as_str()));
    replay_receipts(&mut h, old, new, claim, transfer).await;
}

async fn replay_receipts(
    h: &mut Harness,
    old: SessionId,
    new: SessionId,
    claim: Command,
    transfer: Command,
) {
    h.runtime
        .core
        .engine_for_mut(&new)
        .unwrap()
        .state_mut()
        .external_tool_calls
        .clear();
    h.command("transfer", None, true, transfer).await.unwrap();
    h.runtime
        .core
        .engine_for_mut(&old)
        .unwrap()
        .state_mut()
        .external_tool_calls
        .clear();
    h.command("claim", None, true, claim).await.unwrap();
    assert_eq!(
        h.runtime
            .core
            .session_view(&h.session)
            .unwrap()
            .parent_session_id,
        Some(new)
    );
}
