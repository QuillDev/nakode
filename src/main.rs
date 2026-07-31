use nakode::{
    agent_cli, app,
    config::{Config, NakodeCommand, ServiceAction},
    control_service, diagnostics, tui_eval, update,
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
        control_service::shutdown_service(&config.workspace).await?;
        return Ok(());
    }
    match config.command.clone() {
        Some(NakodeCommand::Diagnostics {
            days,
            sessions,
            provider,
            json,
        }) => {
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
        Some(NakodeCommand::Agent {
            agent_slug,
            session_id,
            task,
        }) => {
            let result = agent_cli::run(&config, agent_slug, session_id, task).await?;
            println!("{}", result.output);
            if !result.success {
                return Err("agent invocation failed".into());
            }
        }
        Some(NakodeCommand::Service {
            action: ServiceAction::Run,
        }) => control_service::run_service(config).await?,
        Some(NakodeCommand::Service {
            action: ServiceAction::Shutdown,
        }) => control_service::shutdown_service(&config.workspace).await?,
        Some(NakodeCommand::Service {
            action: ServiceAction::Endpoint,
        }) => {
            let executable = std::env::current_exe()?;
            let endpoint = control_service::frontend_api_endpoint(&executable, &config).await?;
            let descriptor = serde_json::json!({
                "version": 1,
                "transport": "grpc+unix",
                "workspace": config.workspace,
                "endpoint": endpoint,
            });
            println!("{}", serde_json::to_string(&descriptor)?);
        }
        Some(NakodeCommand::TuiEval {
            scenario,
            width,
            height,
        }) => tui_eval::run(&tui_eval::Options {
            workspace: config.workspace,
            scenario,
            width,
            height,
        })?,
        Some(NakodeCommand::Update) => unreachable!("update commands return before dispatch"),
        None => Box::pin(app::run(config)).await?,
    }
    Ok(())
}
