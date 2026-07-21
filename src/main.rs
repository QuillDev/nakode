use nakode::{
    app,
    config::{Config, NakodeCommand, ServiceAction},
    control, diagnostics, update,
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
        control::shutdown_service().await?;
        return Ok(());
    }
    match config.command.clone() {
        Some(NakodeCommand::Diagnostics {
            days,
            sessions,
            provider,
            json,
        }) => {
            let output = diagnostics::run(&diagnostics::DiagnosticsOptions {
                days,
                session_limit: usize::from(sessions),
                provider,
                json,
            })?;
            println!("{output}");
        }
        Some(NakodeCommand::Agent {
            agent_slug,
            session_id,
            task,
        }) => {
            let response = control::invoke_via_service(
                &config.workspace,
                &control::AgentInvocation {
                    agent: agent_slug,
                    session_id,
                    task,
                },
            )
            .await?;
            println!("{}", response.result);
            if !response.success {
                return Err("agent invocation failed".into());
            }
        }
        Some(NakodeCommand::Service {
            action: ServiceAction::Run,
        }) => control::run_service().await?,
        Some(NakodeCommand::Service {
            action: ServiceAction::Shutdown,
        }) => control::shutdown_service().await?,
        Some(NakodeCommand::Update) => unreachable!("update commands return before dispatch"),
        None => app::run(config).await?,
    }
    Ok(())
}
