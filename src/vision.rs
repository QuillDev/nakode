use std::{future::Future, pin::Pin, sync::Arc};

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::backend::PromptImage;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VisionConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Independent of the calling agent's model options.
    #[serde(default = "default_reasoning_effort")]
    pub reasoning_effort: String,
}

fn default_reasoning_effort() -> String {
    "low".to_owned()
}

impl Default for VisionConfig {
    fn default() -> Self {
        Self {
            model: None,
            reasoning_effort: default_reasoning_effort(),
        }
    }
}

impl VisionConfig {
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.model.as_ref().is_some_and(|model| !model.is_empty())
    }

    #[must_use]
    pub fn provider(&self) -> Option<&str> {
        self.model
            .as_deref()?
            .split_once('/')
            .map(|(provider, _)| provider)
    }

    #[must_use]
    pub fn model_id(&self) -> Option<&str> {
        self.model
            .as_deref()?
            .split_once('/')
            .map(|(_, model)| model)
    }
}

pub type VisionFuture<'a> = Pin<Box<dyn Future<Output = Result<String, String>> + Send + 'a>>;

pub trait VisionService: Send + Sync {
    fn analyze<'a>(
        &'a self,
        prompt: &'a str,
        images: Vec<PromptImage>,
        cancellation: &'a CancellationToken,
    ) -> VisionFuture<'a>;
}

pub type SharedVisionService = Arc<dyn VisionService>;

#[cfg(test)]
mod tests {
    use super::VisionConfig;

    #[test]
    fn legacy_configuration_keeps_low_effort() {
        let config: VisionConfig =
            serde_json::from_str(r#"{"model":"openai-codex/gpt-5.4"}"#).unwrap();
        assert_eq!(config.reasoning_effort, "low");
    }

    #[test]
    fn parses_provider_qualified_model() {
        let config = VisionConfig {
            model: Some("openai-codex/gpt-5.4".to_owned()),
            ..VisionConfig::default()
        };

        assert!(config.is_enabled());
        assert_eq!(config.provider(), Some("openai-codex"));
        assert_eq!(config.model_id(), Some("gpt-5.4"));
    }
}
