//! Embed Nakode's authoritative runtime without its CLI or TUI dependencies.
//! The embedding application supplies the executable prefix and supervises its service process.
pub use nakode::BUILD_REVISION;
pub use nakode::embedded::run;
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
