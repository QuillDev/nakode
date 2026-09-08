use crate::{config::RemoteAction, remote};

/// Execute remote listener administration for either standalone or embedded services.
///
/// # Errors
/// Returns configuration, authentication, or connectivity failures.
pub async fn run(action: &RemoteAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        RemoteAction::Enable {
            bind,
            allow_public_listen,
            endpoint,
        } => {
            if bind.ip().is_unspecified() && !allow_public_listen {
                return Err("wildcard remote listeners require --allow-public-listen".into());
            }
            let endpoint = remote::enrollment_endpoint(*bind, endpoint.as_deref())?;
            let configured = remote::enable(*bind)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&remote::public_connection(
                    &configured,
                    Some(&endpoint)
                ))?
            );
            eprintln!("Restart Nakode to apply the remote listener configuration.");
        }
        RemoteAction::Descriptor { endpoint } => {
            let configured = remote::load()?.ok_or(remote::RemoteConfigError::NotConfigured)?;
            let endpoint = remote::enrollment_endpoint(configured.bind, endpoint.as_deref())?;
            println!(
                "{}",
                serde_json::to_string_pretty(&remote::public_connection(
                    &configured,
                    Some(&endpoint)
                ))?
            );
        }
        RemoteAction::Check { endpoint } => check_remote(endpoint.as_deref()).await?,
        RemoteAction::Disable => {
            remote::disable()?;
            println!("Nakode remote access disabled. Restart Nakode to apply.");
        }
        RemoteAction::RegenerateKey { endpoint } => {
            let existing = remote::load()?.ok_or(remote::RemoteConfigError::NotConfigured)?;
            let endpoint = remote::enrollment_endpoint(existing.bind, endpoint.as_deref())?;
            let configured = remote::regenerate_key()?;
            println!(
                "{}",
                serde_json::to_string_pretty(&remote::public_connection(
                    &configured,
                    Some(&endpoint)
                ))?
            );
            eprintln!("Restart Nakode to revoke the previous key.");
        }
        RemoteAction::RotateCredentials { endpoint } => {
            let existing = remote::load()?.ok_or(remote::RemoteConfigError::NotConfigured)?;
            let endpoint = remote::enrollment_endpoint(existing.bind, endpoint.as_deref())?;
            let configured = remote::rotate_credentials()?;
            println!(
                "{}",
                serde_json::to_string_pretty(&remote::public_connection(
                    &configured,
                    Some(&endpoint)
                ))?
            );
            eprintln!("Restart Nakode to activate the replacement key and TLS certificate.");
        }
        RemoteAction::Status { json } => {
            let configured = remote::load()?;
            if *json {
                let value = configured.as_ref().map_or_else(
                    || serde_json::json!({"enabled": false}),
                    |value| {
                        serde_json::json!({
                            "enabled": value.enabled,
                            "bind": value.bind,
                            "server_id": value.server_id,
                            "tls_server_name": remote::TLS_SERVER_NAME,
                        })
                    },
                );
                println!("{}", serde_json::to_string(&value)?);
            } else if let Some(value) = configured {
                println!(
                    "Nakode remote access: {} at {} (server {})",
                    if value.enabled { "enabled" } else { "disabled" },
                    value.bind,
                    value.server_id
                );
            } else {
                println!("Nakode remote access: not configured");
            }
        }
    }
    Ok(())
}

async fn check_remote(endpoint: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let configured = remote::load()?.ok_or(remote::RemoteConfigError::NotConfigured)?;
    if !configured.enabled {
        return Err("remote access is disabled".into());
    }
    let endpoint = remote::enrollment_endpoint(configured.bind, endpoint)?;
    let client = nakode_sdk::NakodeClient::connect_remote(
        &endpoint,
        configured.certificate_pem.as_bytes(),
        remote::TLS_SERVER_NAME,
        &configured.api_key,
    )
    .await?;
    let info = client.get_server_info().await?;
    let missing_capabilities = [
        "WorkspacePathInspection",
        "Subscriptions",
        "SessionWorkingDirectories",
        "ExternalTools",
        "InitialSessionTools",
        "BuiltinToolAllowlists",
    ]
    .into_iter()
    .filter(|required| !info.capabilities.iter().any(|value| value == *required))
    .collect::<Vec<_>>();
    if info.api_version != "nakode.v1"
        || info.server_id != configured.server_id
        || !missing_capabilities.is_empty()
    {
        return Err(format!(
            "remote compatibility mismatch at {endpoint}: expected nakode.v1 server {} with Ticket Agent capabilities; got {} server {} missing {}",
            configured.server_id,
            info.api_version,
            info.server_id,
            missing_capabilities.join(", ")
        )
        .into());
    }
    println!(
        "Nakode remote endpoint verified at {endpoint} (server {}, build {}).",
        info.server_id,
        info.build_revision.as_deref().unwrap_or("unknown")
    );
    Ok(())
}
