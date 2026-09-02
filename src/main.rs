use clap::CommandFactory;
use nakode::{
    activation, agent_cli, app,
    config::{Config, NakodeCommand, RemoteAction, UpdateOptions},
    diagnostics, purge, remote, remote_update, service_cli, tui_eval, update,
};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("nakode: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    // The confined worker must start under `env_clear()` without loading config, credentials,
    // agents, persistence, or any service endpoint. Its internal launcher always uses this exact
    // two-argument process shape.
    let mut arguments = std::env::args_os();
    let _executable = arguments.next();
    if arguments.next().as_deref() == Some(std::ffi::OsStr::new("codemode-worker"))
        && arguments.next().is_none()
    {
        nakode::codemode_worker::run()?;
        return Ok(());
    }

    let config = Config::load()?;
    let update_options = requested_update_options(&config);
    if let Some(options) = update_options {
        update::run(&options)?;
        return Ok(());
    }
    let Some(command) = config.command.clone() else {
        if config.tui {
            return Box::pin(app::run(config)).await.map_err(Into::into);
        }
        // Nakode is the service. Without a command it starts nothing and shows
        // what it can do, including the client behind `--tui`.
        Config::command().print_long_help()?;
        println!();
        return Ok(());
    };

    // The deprecated `nakode service <action>` spellings stay functional. Each
    // announces its replacement on standard error and then runs the command it
    // was replaced by, leaving standard output untouched for connectors.
    let command = match command {
        NakodeCommand::Service { action } => {
            service_cli::report_deprecation(action.deprecated_spelling(), action.replacement());
            action.into_command()
        }
        command => command,
    };

    match command {
        NakodeCommand::Run => service_cli::run(config).await?,
        NakodeCommand::Start => service_cli::start(&config).await?,
        NakodeCommand::Stop => service_cli::stop(&config).await?,
        NakodeCommand::Restart => service_cli::restart(&config).await?,
        NakodeCommand::Status { json } => service_cli::status(&config, json).await?,
        NakodeCommand::Logs { follow, lines } => service_cli::logs(&config, follow, lines).await?,
        NakodeCommand::Endpoint => service_cli::endpoint(&config).await?,
        NakodeCommand::ActivationEndpoint => service_cli::activation_endpoint(&config).await?,
        NakodeCommand::ActivationHelper => activation::run_helper(config).await?,
        NakodeCommand::Remote { action } => run_remote(&action).await?,
        NakodeCommand::Diagnostics {
            days,
            sessions,
            provider,
            json,
        } => {
            let output = diagnostics::run(
                &config,
                &diagnostics::DiagnosticsOptions {
                    days,
                    session_limit: usize::from(sessions),
                    provider,
                    json,
                },
            )
            .await?;
            println!("{output}");
        }
        NakodeCommand::Agent {
            agent_slug,
            session_id,
            task,
            parent_run_id,
        } => {
            let result =
                agent_cli::run(&config, agent_slug, session_id, task, parent_run_id).await?;
            println!("{}", result.output);
            if !result.success {
                return Err("agent invocation failed".into());
            }
        }
        NakodeCommand::CodemodeWorker => nakode::codemode_worker::run()?,
        NakodeCommand::TuiEval {
            scenario,
            width,
            height,
        } => tui_eval::run(&tui_eval::Options {
            workspace: config.workspace,
            scenario,
            width,
            height,
        })?,
        NakodeCommand::PurgeUnsafe => {
            purge::run().await?;
        }
        NakodeCommand::RestartStale => {
            service_cli::restart_stale().await?;
        }
        NakodeCommand::RemoteUpdateHelper { state, attempt } => {
            remote_update::run_helper(&state, &attempt)?;
        }
        NakodeCommand::Service { .. } => {
            unreachable!("deprecated service actions are rewritten before dispatch")
        }
        NakodeCommand::Update(_) => unreachable!("update commands return before dispatch"),
    }
    Ok(())
}

fn requested_update_options(config: &Config) -> Option<UpdateOptions> {
    if config.update {
        Some(UpdateOptions::default())
    } else {
        match config.command.as_ref() {
            Some(NakodeCommand::Update(options)) => Some(options.clone()),
            _ => None,
        }
    }
}

async fn run_remote(action: &RemoteAction) -> Result<(), Box<dyn std::error::Error>> {
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
