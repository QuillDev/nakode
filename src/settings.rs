use serde::{Deserialize, Serialize};

/// Persisted policy for rendering image artifacts in terminal frontends.
///
/// The server owns this preference as semantic settings state. Each terminal
/// frontend decides how to detect and render the selected mode.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalImageMode {
    #[default]
    Auto,
    On,
    Off,
}

impl TerminalImageMode {
    pub const ALL: [Self; 3] = [Self::Auto, Self::On, Self::Off];
}
