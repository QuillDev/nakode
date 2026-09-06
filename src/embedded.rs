//! Headless service entrypoint for applications embedding the Nakode engine.
use crate::{
    activation,
    config::{Config, NakodeCommand},
    service_cli,
};
use clap::Parser;
use std::{error::Error, ffi::OsString, sync::OnceLock};

static BUILD_REVISION: OnceLock<Option<String>> = OnceLock::new();

pub(crate) fn build_revision() -> Option<&'static str> {
    BUILD_REVISION
        .get()
        .and_then(|value| value.as_deref())
        .or(crate::BUILD_REVISION)
}

pub(crate) fn is_embedded() -> bool {
    BUILD_REVISION.get().is_some()
}

/// Run the service command in the containing application's independently supervised process.
/// Existing Nakode homes, credentials, endpoint identity and databases retain their ownership.
/// `prefix` is replayed before internal service/helper commands in the containing executable.
///
/// # Errors
/// Returns invalid configuration, unsupported standalone operations, or service lifecycle failures.
pub async fn run(
    arguments: Vec<OsString>,
    prefix: &[OsString],
    build_revision: Option<String>,
) -> Result<(), Box<dyn Error>> {
    // Confined workers must not parse environment/configuration or open persistence.
    if arguments.as_slice() == [OsString::from("codemode-worker")] {
        crate::codemode_worker::run()?;
        return Ok(());
    }
    if let Some(revision) = &build_revision
        && (revision.len() != 40
            || !revision
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
    {
        return Err("embedded runtime revision must be an immutable lowercase Git SHA".into());
    }
    BUILD_REVISION
        .set(build_revision)
        .map_err(|_| "embedded runtime was already initialized")?;
    crate::executable::initialize(prefix)?;
    let config =
        match Config::try_parse_from(std::iter::once(OsString::from("nakode")).chain(arguments)) {
            Ok(mut config) => {
                config.apply_legacy_environment();
                config.validated()?
            }
            Err(error)
                if matches!(
                    error.kind(),
                    clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
                ) =>
            {
                error.print()?;
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
    if config.tui || config.update {
        return Err("the embedding application owns installation and its frontend".into());
    }
    let command = match config.command.clone().unwrap_or(NakodeCommand::Run) {
        NakodeCommand::Service { action } => action.into_command(),
        command => command,
    };
    match command {
        NakodeCommand::Remote { action } => crate::remote_cli::run(&action).await?,
        NakodeCommand::RestartStale => service_cli::restart_stale().await?,
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
        NakodeCommand::CodemodeWorker => crate::codemode_worker::run()?,
        _ => return Err("this command is not part of the embedded service; use the containing application's controls".into()),
    }
    Ok(())
}
