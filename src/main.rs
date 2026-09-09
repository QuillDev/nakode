use clap::CommandFactory;
use nakode::{
    activation, agent_cli, app,
    config::{Config, NakodeCommand, UpdateOptions},
    diagnostics, purge, remote_update, service_cli, tui_eval, update,
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
        NakodeCommand::RestartWhenIdle => service_cli::restart_when_idle(&config).await?,
        NakodeCommand::Status { json } => service_cli::status(&config, json).await?,
        NakodeCommand::Logs { follow, lines } => service_cli::logs(&config, follow, lines).await?,
        NakodeCommand::Endpoint => service_cli::endpoint(&config).await?,
        NakodeCommand::ActivationEndpoint => service_cli::activation_endpoint(&config).await?,
        NakodeCommand::ActivationHelper => activation::run_helper(config).await?,
        NakodeCommand::Remote { action } => nakode::remote_cli::run(&action).await?,
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
            title,
            task,
            parent_run_id,
        } => {
            let result =
                agent_cli::run(&config, agent_slug, session_id, title, task, parent_run_id).await?;
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
        NakodeCommand::RestartStale => service_cli::restart_stale().await?,
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
