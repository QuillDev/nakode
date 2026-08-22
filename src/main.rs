use clap::CommandFactory;
use nakode::{
    agent_cli, app,
    config::{Config, NakodeCommand},
    diagnostics, purge, service_cli, tui_eval, update,
};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("nakode: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load()?;
    if config.update || matches!(config.command.as_ref(), Some(NakodeCommand::Update)) {
        update::run()?;
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
        NakodeCommand::Service { .. } => {
            unreachable!("deprecated service actions are rewritten before dispatch")
        }
        NakodeCommand::Update => unreachable!("update commands return before dispatch"),
    }
    Ok(())
}
