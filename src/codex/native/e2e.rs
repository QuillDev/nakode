//! Opt-in native wire fixture. Compiled out of ordinary/release builds.
use super::{BackendConfig, BackendError, CODEX_PROVIDER};

pub(super) fn configure(mut config: BackendConfig) -> Result<BackendConfig, BackendError> {
    let Some(value) = std::env::var_os("NAKODE_E2E_CODEX_NATIVE_URL") else {
        return Ok(config);
    };
    let invalid = || BackendError::BridgeSetup {
        provider: CODEX_PROVIDER.to_owned(),
        detail: "native E2E fixture requires an explicit loopback HTTP origin".to_owned(),
    };
    let url = reqwest::Url::parse(&value.to_string_lossy()).map_err(|_| invalid())?;
    validate_origin(&url).map_err(|()| invalid())?;
    config.base_url = url.to_string();
    // Never forward actual account credentials, proxies or redirect requests to the fixture.
    config.client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| invalid())?;
    config.credential = Some(serde_json::json!({
        "access_token": "isolated-fixture-only",
        "refresh_token": "not-a-refresh-token",
        "account_id": "isolated-fixture",
        "expires_at_ms": u64::MAX,
    }));
    Ok(config)
}

fn validate_origin(url: &reqwest::Url) -> Result<(), ()> {
    let loopback = url.host_str().is_some_and(|host| {
        host.trim_matches(['[', ']'])
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback())
    });
    if url.scheme() != "http"
        || !loopback
        || url.port().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_origin;

    #[test]
    fn native_fixture_refuses_remote_or_ambiguous_origins() {
        for value in [
            "https://127.0.0.1:9000",
            "http://example.com:9000",
            "http://localhost:9000",
            "http://127.0.0.1",
            "http://user:password@127.0.0.1:9000",
            "http://127.0.0.1:9000/path",
            "http://127.0.0.1:9000/?token=secret",
        ] {
            assert!(validate_origin(&reqwest::Url::parse(value).unwrap()).is_err());
        }
        for value in ["http://127.0.0.1:9000", "http://[::1]:9000"] {
            assert!(validate_origin(&reqwest::Url::parse(value).unwrap()).is_ok());
        }
    }
}
