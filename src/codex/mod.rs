mod client;
mod native;
pub mod protocol;

pub use client::{BackendConfig as CompatibilityBackendConfig, spawn as spawn_compatibility};
pub use native::{BackendConfig, spawn};
pub use protocol::{RpcError, RpcMessage};
