use nako_agent::{
    app,
    config::{Config, NakoAgentCommand},
    control,
};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("nako-agent: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load()?;
    if let Some(NakoAgentCommand::Agent {
        agent_slug,
        session_id,
        task,
    }) = config.command.clone()
    {
        let response = control::invoke(
            &control::socket_path(&config.workspace),
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
    } else {
        app::run(config).await?;
    }
    Ok(())
}
