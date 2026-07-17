use flock::{app, config::Config};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("flock: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load()?;
    app::run(config).await?;
    Ok(())
}
