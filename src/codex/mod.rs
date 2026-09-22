#[cfg(feature = "codex-process-adapter")]
mod compatibility;
mod image_tokens;
mod native;

pub(crate) use image_tokens::estimate as estimate_image_tokens;
#[cfg(feature = "codex-process-adapter")]
pub mod protocol;

#[cfg(feature = "codex-process-adapter")]
pub use compatibility::{
    BackendConfig as CompatibilityBackendConfig, spawn as spawn_compatibility,
};
pub use native::{BackendConfig, spawn, vision_service};
#[cfg(feature = "codex-process-adapter")]
pub use protocol::{RpcError, RpcMessage};

fn discovered_model_capabilities(
    efforts: Option<&serde_json::Value>,
    effort_key: &str,
) -> crate::backend::ModelCapabilities {
    let mut reasoning_efforts = Vec::new();
    for entry in efforts
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(effort) = entry.get(effort_key).and_then(serde_json::Value::as_str) else {
            continue;
        };
        if !effort.is_empty() && !reasoning_efforts.iter().any(|existing| existing == effort) {
            reasoning_efforts.push(effort.to_owned());
        }
    }
    crate::backend::ModelCapabilities { reasoning_efforts }
}

// Synthetic capabilities for shared runtime tests, never a production catalogue.
#[cfg(test)]
pub(crate) fn model_capabilities() -> crate::backend::ModelCapabilities {
    crate::backend::ModelCapabilities {
        reasoning_efforts: ["none", "low", "medium", "high", "xhigh", "max"]
            .into_iter()
            .map(str::to_owned)
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    #[test]
    fn capabilities_preserve_provider_efforts_without_a_local_allowlist() {
        let capabilities = super::discovered_model_capabilities(
            Some(&json!([
                {"effort": "low"}, {"effort": "max"}, {"effort": "ultra"},
                {"effort": "future-effort"}, {"effort": "low"},
                {"effort": ""}, {"effort": null}, {}
            ])),
            "effort",
        );
        assert_eq!(
            capabilities.reasoning_efforts,
            ["low", "max", "ultra", "future-effort"]
        );
    }

    #[test]
    fn missing_or_empty_metadata_does_not_invent_supported_efforts() {
        for efforts in [None, Some(json!([])), Some(json!(null)), Some(json!({}))] {
            assert!(
                super::discovered_model_capabilities(efforts.as_ref(), "effort")
                    .reasoning_efforts
                    .is_empty()
            );
        }
    }
}
