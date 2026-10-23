use super::*;

fn install_call(h: &mut Harness, name: &str, arguments: &str) {
    h.runtime
        .core
        .engine_for_mut(&h.session)
        .unwrap()
        .state_mut()
        .external_tool_calls = vec![crate::backend::ExternalToolRequest {
        id: "clear-call".into(),
        name: name.into(),
        arguments_json: arguments.into(),
    }];
}

fn prepare_chat(h: &mut Harness) {
    h.runtime
        .effects
        .persistence
        .sessions
        .bind_session_skill_profile(h.session.as_str(), "profile")
        .unwrap();
    let connection = rusqlite::Connection::open(&h.runtime.effects.persistence.database).unwrap();
    connection.execute("INSERT INTO session_bridges (session_id, workspace, kind, lifecycle, display_title, revision, updated_at_ms) VALUES (?1, ?2, 'chat', 'open', 'Disposable Chat', 1, 1) ON CONFLICT(session_id) DO UPDATE SET kind = 'chat', lifecycle = 'open'", rusqlite::params![h.session.as_str(), h.state().workspace]).unwrap();
    h.runtime.refresh_catalogs();
    install_clear_turn(h);
}

fn install_clear_turn(h: &mut Harness) {
    h.runtime
        .core
        .engine_for_mut(&h.session)
        .unwrap()
        .state_mut()
        .active_turn = Some(crate::state::ActiveTurn {
        id: "disposable-turn".into(),
        model: None,
        options: BackendModelOptions::default(),
        cancelling: false,
    });
    install_call(h, "ClearSelf", "{}");
}

fn command(h: &Harness, call: &str) -> Command {
    Command::ClearSelf {
        session_id: h.session.clone(),
        source_call_id: call.into(),
    }
}

#[tokio::test]
async fn clear_self_removes_only_authenticated_childless_chat_and_returns_receipt() {
    let mut h = Harness::new().await;
    prepare_chat(&mut h);
    let id = h.session.clone();
    let clear = command(&h, "clear-call");
    let receipt = h
        .command("clear", None, false, clear.clone())
        .await
        .unwrap();
    assert_eq!(receipt.resource_id.as_deref(), Some(id.as_str()));
    assert!(h.runtime.core.engine_for(&id).is_none());
    assert!(
        h.runtime
            .effects
            .persistence
            .sessions
            .find(id.as_str())
            .unwrap()
            .is_none()
    );
    let replay = h.command("clear", None, true, clear).await.unwrap();
    assert_eq!(replay.resource_id, receipt.resource_id);
}

#[tokio::test]
async fn clear_self_refuses_spoofed_target_call_and_arguments_and_incomplete_inventory() {
    let mut h = Harness::new().await;
    prepare_chat(&mut h);
    let wrong_call = command(&h, "other-call");
    assert!(
        h.command("wrong-call", None, false, wrong_call)
            .await
            .is_err()
    );
    install_call(&mut h, "DeleteAgent", "{}");
    let clear = command(&h, "clear-call");
    assert!(
        h.command("wrong-tool", None, false, clear.clone())
            .await
            .is_err()
    );
    install_call(&mut h, "ClearSelf", "{\"sessionId\":\"other\"}");
    assert!(
        h.command("wrong-args", None, false, clear.clone())
            .await
            .is_err()
    );
    install_call(&mut h, "ClearSelf", "{}");
    h.runtime.core.session_inventory_complete = false;
    assert!(h.command("unknown", None, false, clear).await.is_err());
    assert!(h.runtime.core.engine_for(&h.session).is_some());
}

#[tokio::test]
async fn clear_self_retains_chat_with_unsettled_inbox() {
    let mut h = Harness::new().await;
    prepare_chat(&mut h);
    h.enqueue("pending-instruction").await;
    let clear = command(&h, "clear-call");
    assert!(h.command("blocked", None, false, clear).await.is_err());
    assert!(
        h.runtime
            .core
            .engine_for(&h.session)
            .unwrap()
            .state()
            .active_turn
            .is_some()
    );

    assert!(
        h.runtime
            .effects
            .persistence
            .sessions
            .find(h.session.as_str())
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn clear_self_refuses_stale_call_after_turn_or_backend_loss() {
    for lost_backend in [false, true] {
        let mut h = Harness::new().await;
        prepare_chat(&mut h);
        let state = h
            .runtime
            .core
            .engine_for_mut(&h.session)
            .unwrap()
            .state_mut();
        if lost_backend {
            state.connection =
                crate::state::ConnectionState::Disconnected("disposable backend ended".to_owned());
        } else {
            state.active_turn = None;
        }
        let clear = command(&h, "clear-call");
        assert!(h.command("stale", None, false, clear).await.is_err());
        assert!(h.runtime.core.engine_for(&h.session).is_some());
    }
}

#[tokio::test]
async fn clear_self_refuses_all_durable_child_lifecycles_before_stopping_caller() {
    for lifecycle in ["idle", "working", "closed"] {
        let mut h = Harness::new().await;
        prepare_chat(&mut h);
        let child = h
            .runtime
            .effects
            .persistence
            .sessions
            .create(
                CODEX_PROVIDER,
                lifecycle,
                &h.state().workspace,
                lifecycle,
                None,
            )
            .unwrap();
        let connection =
            rusqlite::Connection::open(&h.runtime.effects.persistence.database).unwrap();
        connection.execute("PRAGMA foreign_keys = ON", []).unwrap();
        connection.execute("INSERT INTO session_child_links (child_id, parent_id, child_title) VALUES (?1, ?2, ?3)", rusqlite::params![child.id, h.session.as_str(), lifecycle]).unwrap();
        if lifecycle == "closed" {
            connection.execute("INSERT INTO session_bridges (session_id, workspace, kind, lifecycle, display_title, revision, updated_at_ms) VALUES (?1, ?2, 'agent', 'archived', 'Retained child', 1, 1)", rusqlite::params![child.id, h.state().workspace]).unwrap();
        }
        h.runtime.refresh_catalogs();
        let clear = command(&h, "clear-call");
        assert!(h.command("linked", None, false, clear).await.is_err());
        assert!(
            h.runtime
                .core
                .engine_for(&h.session)
                .unwrap()
                .state()
                .active_turn
                .is_some()
        );
        assert_eq!(
            h.runtime
                .effects
                .persistence
                .sessions
                .find(&child.id)
                .unwrap()
                .unwrap()
                .parent_session_id
                .as_deref(),
            Some(h.session.as_str())
        );
    }
}

fn remove_fixture_bridge(h: &mut Harness) {
    let connection = rusqlite::Connection::open(&h.runtime.effects.persistence.database).unwrap();
    connection
        .execute(
            "DELETE FROM session_bridges WHERE session_id = ?1",
            [h.session.as_str()],
        )
        .unwrap();
    h.runtime.refresh_catalogs();
}

fn adopt_chat(h: &Harness, profile: &str) -> Command {
    Command::OpenSession {
        session_id: h.session.clone(),
        bridge: Some(nakode_protocol::SessionBridgeIntent {
            kind: nakode_protocol::OrchestratorKind::Chat,
            lifecycle: BridgeLifecycle::Open,
            display_title: "Hosted Chat".to_owned(),
        }),
        profile_id: Some(profile.to_owned()),
        tools: None,
        mcp_grant: None,
        enabled_skill_ids: Vec::new(),
        account_id: None,
    }
}

#[tokio::test]
async fn legacy_hosted_chat_requires_public_profile_and_bridge_adoption() {
    let mut h = Harness::new().await;
    // Match the old Host CreateSession path: neither profile nor bridge was supplied.
    remove_fixture_bridge(&mut h);
    install_clear_turn(&mut h);
    let clear = command(&h, "clear-call");
    let error = h
        .command("missing-metadata", None, false, clear.clone())
        .await
        .unwrap_err();
    assert!(
        error
            .message
            .contains("self-clear requires an owned open Chat with no unsettled coordination")
    );
    let adopt = adopt_chat(&h, "profile");
    h.command("adopt", None, false, adopt).await.unwrap();
    // Adoption preserves the invoking turn and exact pending call instead of synthesizing work.
    assert_eq!(
        h.state().active_turn.as_ref().unwrap().id,
        "disposable-turn"
    );
    assert_eq!(h.state().external_tool_calls.len(), 1);
    assert_eq!(h.state().external_tool_calls[0].id, "clear-call");
    h.command("clear-adopted", None, false, clear)
        .await
        .unwrap();
    assert!(h.runtime.core.engine_for(&h.session).is_none());
}

#[tokio::test]
async fn bridge_adoption_preserves_established_profile_kind_and_archived_lifecycle() {
    for mismatch in ["profile", "kind", "lifecycle"] {
        let mut h = Harness::new().await;
        prepare_chat(&mut h);
        let connection =
            rusqlite::Connection::open(&h.runtime.effects.persistence.database).unwrap();
        match mismatch {
            "kind" => {
                connection
                    .execute(
                        "UPDATE session_bridges SET kind = 'agent' WHERE session_id = ?1",
                        [h.session.as_str()],
                    )
                    .unwrap();
            }
            "lifecycle" => {
                connection
                    .execute(
                        "UPDATE session_bridges SET lifecycle = 'archived' WHERE session_id = ?1",
                        [h.session.as_str()],
                    )
                    .unwrap();
            }
            _ => {}
        }
        h.runtime.refresh_catalogs();
        let adopt = adopt_chat(
            &h,
            if mismatch == "profile" {
                "other"
            } else {
                "profile"
            },
        );
        assert!(h.command("mismatch", None, false, adopt).await.is_err());
        assert!(h.runtime.core.engine_for(&h.session).is_some());
    }
}

#[tokio::test]
async fn public_adoption_does_not_remove_unsettled_coordination() {
    let mut h = Harness::new().await;
    h.enqueue("pending-instruction").await;
    remove_fixture_bridge(&mut h);
    let adopt = adopt_chat(&h, "profile");
    h.command("adopt", None, false, adopt).await.unwrap();
    install_clear_turn(&mut h);
    let clear = command(&h, "clear-call");
    assert!(
        h.command("still-pending", None, false, clear)
            .await
            .is_err()
    );
    assert!(h.runtime.core.engine_for(&h.session).is_some());
}

#[tokio::test]
async fn adopted_chat_still_refuses_other_native_work_and_cancelling_turns() {
    for cancelling in [false, true] {
        let mut h = Harness::new().await;
        remove_fixture_bridge(&mut h);
        install_clear_turn(&mut h);
        let adopt = adopt_chat(&h, "profile");
        h.command("adopt-live", None, false, adopt).await.unwrap();
        let state = h
            .runtime
            .core
            .engine_for_mut(&h.session)
            .unwrap()
            .state_mut();
        if cancelling {
            state.active_turn.as_mut().unwrap().cancelling = true;
        } else {
            // Record supervised work in the isolated state without executing its shell effect.
            let _effects = state.run_shell_command("fixture-only".to_owned()).unwrap();
        }
        let clear = command(&h, "clear-call");
        assert!(
            h.command("protected-work", None, false, clear)
                .await
                .is_err()
        );
        assert!(h.runtime.core.engine_for(&h.session).is_some());
    }
}
