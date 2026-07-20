use nakode::{
    app,
    config::{Config, NakodeCommand, ServiceAction},
    control,
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
    match config.command.clone() {
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
        None => app::run(config).await?,
    }
    Ok(())
}
