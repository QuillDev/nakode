#[cfg(feature = "codex-process-adapter")]
mod compatibility;
mod native;
#[cfg(feature = "codex-process-adapter")]
pub mod protocol;

#[cfg(feature = "codex-process-adapter")]
pub use compatibility::{
    BackendConfig as CompatibilityBackendConfig, spawn as spawn_compatibility,
};
pub use native::{BackendConfig, spawn, vision_service};
#[cfg(feature = "codex-process-adapter")]
pub use protocol::{RpcError, RpcMessage};

pub(crate) fn model_capabilities() -> crate::backend::ModelCapabilities {
    crate::backend::ModelCapabilities {
        reasoning_efforts: ["none", "low", "medium", "high", "xhigh", "max"]
            .into_iter()
            .map(str::to_owned)
            .collect(),
    }
}
