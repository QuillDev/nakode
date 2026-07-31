//! Built-in frontend connector for the public generated SDK.

use std::io;

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

pub(crate) async fn connect(
    config: &Config,
) -> Result<nakode_sdk::NakodeClient, NativeClientStartError> {
    let executable = std::env::current_exe().map_err(NativeClientStartError::CurrentExecutable)?;
    let endpoint = crate::control_service::frontend_api_endpoint(&executable, config).await?;
    Ok(nakode_sdk::NakodeClient::connect_unix(endpoint).await?)
}
