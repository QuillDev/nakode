//! Built-in frontend connector for the public generated SDK.

use std::{io, path::PathBuf};

use thiserror::Error;

use crate::{config::Config, control_service::ControlError};

#[derive(Debug, Error)]
pub(crate) enum NativeClientStartError {
    #[error("failed to locate the running Nakode executable: {0}")]
    CurrentExecutable(#[source] io::Error),
    #[error(transparent)]
    Control(#[from] ControlError),
    #[error(transparent)]
    Sdk(#[from] nakode_sdk::SdkError),
}

pub(crate) struct NativeClientConnection {
    pub(crate) client: nakode_sdk::NakodeClient,
    pub(crate) authoritative_workspace: PathBuf,
}

pub(crate) async fn connect(
    config: &Config,
) -> Result<nakode_sdk::NakodeClient, NativeClientStartError> {
    Ok(connect_report(config).await?.client)
}

pub(crate) async fn connect_report(
    config: &Config,
) -> Result<NativeClientConnection, NativeClientStartError> {
    let executable = std::env::current_exe().map_err(NativeClientStartError::CurrentExecutable)?;
    let endpoint =
        crate::control_service::frontend_api_endpoint_report(&executable, config).await?;
    Ok(NativeClientConnection {
        client: nakode_sdk::NakodeClient::connect_unix(endpoint.endpoint).await?,
        authoritative_workspace: endpoint.workspace,
    })
}
