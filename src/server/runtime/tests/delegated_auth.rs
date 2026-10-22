use super::*;
use crate::backend::{BackendOperation, TurnOutcome};

mod transport;
use transport::{AuthTransport, TokenVersion};

const BROKER_ACCOUNT: &str = "cloud-default";
const LOCAL_ACCOUNT: &str = "old-local";
const MARKER: &str = "DELEGATE-LIVE-QUARTZ-8264";

fn brokered_credential(token: &str) -> serde_json::Value {
    serde_json::json!({
        "access_token": token,
        "refresh_token": "",
        "expires_at_ms": u64::MAX,
        "account_id": "cloud-provider-account",
        "brokered": true,
    })
}

async fn registry_with_accounts(workspace: &Path, origin: &str) -> BackendRegistry {
    SqliteSessionRepository::open(workspace.join("sessions.sqlite3")).expect("session schema");
    let mut registry = empty_registry(workspace).await;
    registry.codex_test_origin = Some(origin.to_owned());
    let automatic = nakode_protocol::ProviderAccountRoutingMode::Automatic;
    registry.provider_accounts.insert(
        CODEX_PROVIDER.to_owned(),
        vec![
            routing_account(
                CODEX_PROVIDER,
                BROKER_ACCOUNT,
                "Cloud",
                true,
                true,
                automatic,
            ),
            routing_account(
                CODEX_PROVIDER,
                LOCAL_ACCOUNT,
                "Local",
                true,
                false,
                automatic,
            ),
        ],
    );
    registry.set_provider_account_credential(
        CODEX_PROVIDER,
        BROKER_ACCOUNT,
        brokered_credential("synthetic-current"),
    );
    registry.set_provider_account_credential(
        CODEX_PROVIDER,
        LOCAL_ACCOUNT,
        serde_json::json!({
            "access_token": "synthetic-expired",
            "refresh_token": "synthetic-spent",
            "expires_at_ms": 0,
            "account_id": "local-provider-account",
        }),
    );
    registry
        .start_provider(CODEX_PROVIDER)
        .await
        .expect("provider");
    registry
}

fn start_session(owner: &SessionId, run: Option<&str>) -> BackendCommand {
    BackendCommand::StartSession {
        model: Some("gpt-native".to_owned()),
        instructions: None,
        owner_session_id: Some(owner.to_string()),
        parent_run_id: run.map(str::to_owned),
        enabled_skill_ids: Vec::new(),
        external_tools: Vec::new(),
        replace_builtin_tools: false,
        code_mode: false,
        allowed_builtin_tools: Some(Vec::new()),
        max_turns: Some(1),
        finalization_reserve_turns: 0,
        timeout_seconds: Some(10),
    }
}

async fn next_event(registry: &mut BackendRegistry) -> (BackendSource, BackendEvent) {
    let event = tokio::time::timeout(Duration::from_secs(5), registry.events.recv())
        .await
        .expect("bounded native event")
        .expect("native event");
    // The brokered path must not publish credentials into run events or diagnostics.
    assert!(!format!("{event:?}").contains("synthetic-"));
    event
}

async fn session_created(registry: &mut BackendRegistry, run: Option<&str>) -> String {
    loop {
        let (source, event) = next_event(registry).await;
        let matches = match (&source, run) {
            (BackendSource::Primary { account_id, .. }, None) => account_id == BROKER_ACCOUNT,
            (BackendSource::Subagent(id), Some(run)) => id == run,
            _ => false,
        };
        if !matches {
            continue;
        }
        match event {
            BackendEvent::SessionCreated {
                provider_session_id,
                ..
            } => return provider_session_id,
            BackendEvent::RequestFailed { message, .. } => {
                panic!("native startup failed: {message}")
            }
            _ => {}
        }
    }
}

async fn complete_delegate_turn(registry: &mut BackendRegistry, run: &str, native_id: &str) {
    assert!(
        registry
            .send_subagent(
                run,
                BackendCommand::StartTurn {
                    provider_session_id: native_id.to_owned(),
                    client_id: "fixture-client".to_owned(),
                    prompt: "Return the fixture marker.".to_owned(),
                    attachments: Vec::new(),
                    model: None,
                    skill_catalogue: SkillCatalog::default(),
                }
            )
            .await
    );
    let mut text = String::new();
    loop {
        let (source, event) = next_event(registry).await;
        if !matches!(source, BackendSource::Subagent(ref id) if id == run) {
            continue;
        }
        match event {
            BackendEvent::ItemDelta { delta, .. } => text.push_str(&delta),
            BackendEvent::TurnCompleted { outcome, error, .. } => {
                assert_eq!(outcome, TurnOutcome::Completed, "{error:?}");
                assert!(text.contains(MARKER));
                return;
            }
            BackendEvent::RequestFailed { message, .. } => panic!("native turn failed: {message}"),
            _ => {}
        }
    }
}

#[tokio::test]
async fn native_delegates_use_same_default_as_fresh_brokered_sessions() {
    for parent_provider in [CLAUDE_PROVIDER, CODEX_PROVIDER] {
        for cloud_archetype in [true, false] {
            let workspace = tempfile::tempdir().expect("workspace");
            let transport = AuthTransport::start().await;
            let mut registry = registry_with_accounts(workspace.path(), &transport.origin).await;
            let fresh = SessionId::from("fresh-codex");
            // FStack supplies this exact account for fresh durable sessions.
            let selected = registry
                .send_session(
                    &fresh,
                    CODEX_PROVIDER,
                    Some(BROKER_ACCOUNT),
                    workspace.path(),
                    start_session(&fresh, None),
                )
                .await
                .expect("fresh session selection");
            assert_eq!(selected.account_id, BROKER_ACCOUNT);
            session_created(&mut registry, None).await;
            // Keep the healthy account loaded. An unbound least-load selection would choose local.
            assert_eq!(
                registry
                    .select_account(CODEX_PROVIDER, None)
                    .unwrap()
                    .account_id,
                LOCAL_ACCOUNT
            );
            let parent = if parent_provider == CODEX_PROVIDER {
                fresh.clone()
            } else {
                let parent = SessionId::from("claude-parent");
                registry.session_accounts.insert(
                    (parent.clone(), CLAUDE_PROVIDER.to_owned()),
                    "claude-only-account".to_owned(),
                );
                parent
            };
            registry
                .spawn_subagent(
                    parent.clone(),
                    "run".to_owned(),
                    CODEX_PROVIDER,
                    workspace.path(),
                    cloud_archetype,
                )
                .await
                .expect("delegate admission");
            assert!(
                registry
                    .send_subagent("run", start_session(&parent, Some("run")))
                    .await
            );
            let native_id = session_created(&mut registry, Some("run")).await;
            assert_eq!(
                registry.subagent_account("run"),
                Some((CODEX_PROVIDER.to_owned(), BROKER_ACCOUNT.to_owned()))
            );
            assert_eq!(registry.subagent_parents.get("run"), Some(&parent));
            complete_delegate_turn(&mut registry, "run", &native_id).await;
            let requests = transport.observations();
            assert_eq!(requests.len(), 3);
            assert!(
                requests
                    .iter()
                    .all(|request| request.cloud_account && request.token == TokenVersion::Current)
            );
            assert!(requests.iter().all(|request| !request.refresh));
            registry.stop_subagent("run").await.expect("stop child");
            registry.stop_session(&fresh).await.expect("stop fresh");
            registry.stop_provider_control(CODEX_PROVIDER).await;
        }
    }
}

#[tokio::test]
async fn cross_provider_default_is_fail_closed_before_any_auth_transport() {
    for cloud_archetype in [true, false] {
        for unavailable in ["missing", "disabled", "cooldown"] {
            let workspace = tempfile::tempdir().expect("workspace");
            let transport = AuthTransport::start().await;
            let mut registry = registry_with_accounts(workspace.path(), &transport.origin).await;
            match unavailable {
                "missing" => {
                    registry
                        .provider_account_credentials
                        .remove(&(CODEX_PROVIDER.to_owned(), BROKER_ACCOUNT.to_owned()));
                }
                "disabled" => {
                    registry.provider_accounts.get_mut(CODEX_PROVIDER).unwrap()[0].enabled = false;
                }
                "cooldown" => {
                    registry.provider_cooldowns.insert(
                        (CODEX_PROVIDER.to_owned(), BROKER_ACCOUNT.to_owned()),
                        super::super::ProviderCooldown {
                            until: Instant::now() + Duration::from_secs(60),
                            reason: "fixture unavailable".to_owned(),
                        },
                    );
                }
                _ => unreachable!(),
            }
            let error = registry
                .spawn_subagent(
                    SessionId::from("claude-parent"),
                    "refused".to_owned(),
                    CODEX_PROVIDER,
                    workspace.path(),
                    cloud_archetype,
                )
                .await
                .expect_err("no sibling fallback")
                .to_string();
            assert!(error.contains(BROKER_ACCOUNT), "{error}");
            assert!(!error.contains("synthetic-"));
            assert!(!registry.subagent_commands.contains_key("refused"));
            assert!(transport.observations().is_empty());
            registry.stop_provider_control(CODEX_PROVIDER).await;
        }
    }
}

#[tokio::test]
async fn expired_brokered_delegate_requests_sync_without_contacting_oauth_or_siblings() {
    let workspace = tempfile::tempdir().expect("workspace");
    let transport = AuthTransport::start().await;
    let mut registry = registry_with_accounts(workspace.path(), &transport.origin).await;
    let mut expired = brokered_credential("synthetic-expired-broker");
    expired["expires_at_ms"] = 0.into();
    registry
        .replace_provider_account_credential(CODEX_PROVIDER, BROKER_ACCOUNT, expired)
        .await;
    registry
        .spawn_subagent(
            SessionId::from("claude-parent"),
            "expired".to_owned(),
            CODEX_PROVIDER,
            workspace.path(),
            true,
        )
        .await
        .expect("delegate handle");
    loop {
        let (source, event) = next_event(&mut registry).await;
        if matches!(source, BackendSource::Subagent(ref id) if id == "expired")
            && let BackendEvent::RequestFailed {
                operation, message, ..
            } = event
        {
            assert_eq!(operation, BackendOperation::Authenticate);
            assert!(message.starts_with(crate::backend::CREDENTIAL_REFRESH_REQUIRED));
            assert!(message.contains("broker must supply a new access token"));
            break;
        }
    }
    assert!(transport.observations().is_empty());
    assert_eq!(
        registry.subagent_account("expired"),
        Some((CODEX_PROVIDER.to_owned(), BROKER_ACCOUNT.to_owned()))
    );
    registry.stop_subagent("expired").await.expect("stop child");
    registry.stop_provider_control(CODEX_PROVIDER).await;
}

#[tokio::test]
async fn live_delegate_uses_broker_replacement_and_revocation_without_local_refresh() {
    let workspace = tempfile::tempdir().expect("workspace");
    let transport = AuthTransport::start().await;
    let mut registry = registry_with_accounts(workspace.path(), &transport.origin).await;
    let parent = SessionId::from("claude-parent");
    registry
        .spawn_subagent(
            parent.clone(),
            "live".to_owned(),
            CODEX_PROVIDER,
            workspace.path(),
            true,
        )
        .await
        .expect("delegate");
    assert!(
        registry
            .send_subagent("live", start_session(&parent, Some("live")))
            .await
    );
    let native_id = session_created(&mut registry, Some("live")).await;
    registry
        .replace_provider_account_credential(
            CODEX_PROVIDER,
            BROKER_ACCOUNT,
            brokered_credential("synthetic-replacement"),
        )
        .await;
    complete_delegate_turn(&mut registry, "live", &native_id).await;
    let requests = transport.observations();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].token, TokenVersion::Current);
    assert_eq!(requests[1].token, TokenVersion::Replacement);
    assert!(
        requests
            .iter()
            .all(|request| request.cloud_account && !request.refresh)
    );
    registry
        .clear_provider_account_credential(CODEX_PROVIDER, BROKER_ACCOUNT)
        .await;
    assert!(
        registry
            .send_subagent(
                "live",
                BackendCommand::Reload {
                    provider_session_id: None
                }
            )
            .await
    );
    loop {
        let (source, event) = next_event(&mut registry).await;
        if matches!(source, BackendSource::Subagent(ref id) if id == "live")
            && let BackendEvent::RequestFailed {
                operation, message, ..
            } = event
        {
            assert_eq!(operation, BackendOperation::Reload);
            assert!(message.contains("not authenticated"));
            break;
        }
    }
    assert_eq!(transport.observations().len(), 2);
    registry.stop_subagent("live").await.expect("stop child");
    registry.stop_provider_control(CODEX_PROVIDER).await;
}
