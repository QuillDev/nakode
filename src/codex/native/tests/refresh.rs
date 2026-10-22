use super::*;

#[tokio::test]
async fn rejected_refresh_reports_reconnect_without_retry_or_credential_leakage() {
    let (url, server) = serve_sequence(vec![(
        401,
        "application/json",
        r#"{"error":"synthetic-secret-response"}"#,
    )])
    .await;
    let config = BackendConfig::native(PathBuf::from("."))
        .with_auth_urls(&url)
        .with_base_url(url);
    let (events, mut received) = mpsc::channel(4);
    assert!(
        refresh_if_needed(&config, Some(expired("synthetic-revoked")), &events)
            .await
            .is_none()
    );
    let event = received.try_recv().expect("authentication failure");
    assert!(matches!(event, BackendEvent::RequestFailed {
        operation: BackendOperation::Authenticate, ref message, ..
    } if message.contains("401 Unauthorized") && message.contains("reconnect this Codex account")));
    assert!(!format!("{event:?}").contains("synthetic-"));
    assert!(received.try_recv().is_err());
    let requests = server.await.expect("refresh server");
    assert_eq!(requests.len(), 1);
    assert!(requests[0].starts_with("POST /oauth/token "));
}

const ROTATED: &str = r#"{"access_token":"header.eyJodHRwczovL2FwaS5vcGVuYWkuY29tL2F1dGgiOnsiY2hhdGdwdF9hY2NvdW50X2lkIjoiYWNjb3VudC1icm93c2VyIn19.signature","refresh_token":"synthetic-rotated","expires_in":3600}"#;

fn expired(refresh_token: &str) -> CodexCredential {
    CodexCredential {
        access_token: "synthetic-expired".to_owned(),
        refresh_token: refresh_token.to_owned(),
        expires_at_ms: 0,
        account_id: "account-browser".to_owned(),
        email: None,
        brokered: false,
    }
}

#[tokio::test]
async fn concurrent_startups_refresh_once_and_both_publish_the_rotation() {
    let (url, server) = serve_once("application/json", ROTATED).await;
    let coordinator = RefreshCoordinator::default();
    let first = BackendConfig::native(PathBuf::from("."))
        .with_auth_urls(&url)
        .with_base_url(url.clone())
        .with_refresh_coordinator(coordinator.clone());
    let second = BackendConfig::native(PathBuf::from("."))
        .with_auth_urls(&url)
        .with_base_url(url)
        .with_refresh_coordinator(coordinator);
    let (events_a, mut received_a) = mpsc::channel(4);
    let (events_b, mut received_b) = mpsc::channel(4);
    let (a, b) = tokio::join!(
        refresh_if_needed(&first, Some(expired("synthetic-old")), &events_a),
        refresh_if_needed(&second, Some(expired("synthetic-old")), &events_b),
    );
    assert_eq!(a.expect("first refresh").refresh_token, "synthetic-rotated");
    assert_eq!(
        b.expect("coalesced refresh").refresh_token,
        "synthetic-rotated"
    );
    for event in [
        received_a.try_recv().expect("first event"),
        received_b.try_recv().expect("second event"),
    ] {
        assert!(
            matches!(event, BackendEvent::AuthenticationCompleted { metadata, .. }
            if metadata["refresh_token"] == "synthetic-rotated")
        );
    }
    let request = server.await.expect("one refresh server");
    assert!(request.starts_with("POST /oauth/token "));
}

#[tokio::test]
async fn replacement_credentials_do_not_reuse_another_sign_ins_cached_rotation() {
    let (url, server) = serve_sequence(vec![
        (200, "application/json", ROTATED),
        (200, "application/json", ROTATED),
    ])
    .await;
    let config = BackendConfig::native(PathBuf::from("."))
        .with_auth_urls(&url)
        .with_base_url(url);
    let (events, _received) = mpsc::channel(4);
    for token in ["synthetic-first-sign-in", "synthetic-replacement-sign-in"] {
        assert!(
            refresh_if_needed(&config, Some(expired(token)), &events)
                .await
                .is_some()
        );
    }
    let requests = timeout(Duration::from_secs(2), server)
        .await
        .expect("both requests sent")
        .expect("server");
    assert_eq!(requests.len(), 2);
    assert!(requests[0].contains("synthetic-first-sign-in"));
    assert!(requests[1].contains("synthetic-replacement-sign-in"));
}

#[tokio::test]
async fn refresh_coordinators_are_isolated_between_accounts() {
    let (url, server) = serve_sequence(vec![
        (200, "application/json", ROTATED),
        (200, "application/json", ROTATED),
    ])
    .await;
    let (events, _received) = mpsc::channel(4);
    for _ in 0..2 {
        let config = BackendConfig::native(PathBuf::from("."))
            .with_auth_urls(&url)
            .with_base_url(url.clone());
        assert!(
            refresh_if_needed(&config, Some(expired("synthetic-old")), &events)
                .await
                .is_some()
        );
    }
    let requests = timeout(Duration::from_secs(2), server)
        .await
        .expect("isolated requests")
        .expect("server");
    assert_eq!(requests.len(), 2);
}
