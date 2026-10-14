use super::*;

fn bind_delegate(registry: &mut BackendRegistry, run: &str, provider: &str, account: Option<&str>) {
    registry
        .subagent_providers
        .insert(run.to_owned(), provider.to_owned());
    registry
        .subagent_accounts
        .insert(run.to_owned(), account.map(str::to_owned));
}

#[tokio::test]
async fn delegate_refresh_is_saved_to_launch_account_not_current_default() {
    let workspace = tempfile::tempdir().expect("workspace");
    let mut registry = empty_registry(workspace.path()).await;
    bind_delegate(&mut registry, "run-a", CODEX_PROVIDER, Some("account-a"));
    registry.provider_accounts.insert(
        CODEX_PROVIDER.to_owned(),
        vec![routing_account(
            CODEX_PROVIDER,
            "account-b",
            "new default",
            true,
            true,
            nakode_protocol::ProviderAccountRoutingMode::Automatic,
        )],
    );
    let event = BackendEvent::AuthenticationCompleted {
        kind: "chatgpt_oauth".to_owned(),
        metadata: serde_json::json!({"fixture": "rotated"}),
    };
    assert!(
        matches!(registry.subagent_credential_effect("run-a", &event),
        Some(Effect::SaveProviderAccountCredential { provider, account_id, kind, metadata })
        if provider == CODEX_PROVIDER && account_id == "account-a"
            && kind == "chatgpt_oauth" && metadata["fixture"] == "rotated")
    );
    assert!(
        registry
            .subagent_credential_effect("unknown", &event)
            .is_none()
    );
    bind_delegate(&mut registry, "legacy", CODEX_PROVIDER, None);
    assert!(matches!(
        registry.subagent_credential_effect("legacy", &event),
        Some(Effect::SaveProviderCredential { .. })
    ));
    registry.stop_subagent("run-a").await.expect("stop");
    assert!(
        registry
            .subagent_credential_effect("run-a", &event)
            .is_none()
    );
}

#[tokio::test]
async fn delegate_failure_cools_down_its_bound_account_only() {
    let workspace = tempfile::tempdir().expect("workspace");
    let mut registry = empty_registry(workspace.path()).await;
    bind_delegate(&mut registry, "run-a", CODEX_PROVIDER, Some("account-a"));
    let observed = registry.observe_provider_event(
        &BackendSource::Subagent("run-a".to_owned()),
        &BackendEvent::ProviderFailure {
            classification: ProviderFailureClassification::RateLimit,
            retry_after: Some(Duration::from_secs(10)),
            message: "fixture rate limit".to_owned(),
        },
    );
    assert!(matches!(observed, Some((provider, account, _))
        if provider == CODEX_PROVIDER && account == "account-a"));
    assert!(
        registry
            .active_cooldown(CODEX_PROVIDER, "account-a")
            .is_some()
    );
    assert!(
        registry
            .active_cooldown(CODEX_PROVIDER, "account-b")
            .is_none()
    );
}

#[tokio::test]
async fn delegate_credential_replacement_and_revocation_stay_account_scoped() {
    let workspace = tempfile::tempdir().expect("workspace");
    let mut registry = empty_registry(workspace.path()).await;
    let mut receivers = Vec::new();
    for (run, provider, account) in [
        ("matching", CODEX_PROVIDER, "account-a"),
        ("other-account", CODEX_PROVIDER, "account-b"),
        ("other-provider", CLAUDE_PROVIDER, "account-a"),
    ] {
        bind_delegate(&mut registry, run, provider, Some(account));
        let (commands, receiver) = mpsc::channel(2);
        registry.subagent_commands.insert(run.to_owned(), commands);
        receivers.push(receiver);
    }
    registry
        .update_session_credentials(
            CODEX_PROVIDER,
            "account-a",
            Some(serde_json::json!({"fixture": "replacement"})),
        )
        .await;
    assert!(matches!(
        receivers[0].try_recv(),
        Ok(BackendCommand::UpdateCredential {
            credential: Some(_)
        })
    ));
    assert!(receivers[1].try_recv().is_err());
    assert!(receivers[2].try_recv().is_err());
    registry
        .update_session_credentials(CODEX_PROVIDER, "account-a", None)
        .await;
    assert!(matches!(
        receivers[0].try_recv(),
        Ok(BackendCommand::UpdateCredential { credential: None })
    ));
    assert!(receivers[1].try_recv().is_err());
    assert!(receivers[2].try_recv().is_err());
}
