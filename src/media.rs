use serde::{Deserialize, Serialize};

/// Opaque image bytes shared by device presentation and provider-neutral
/// runtime contracts.
///
/// This type deliberately lives outside backend adapters so a renderer never
/// needs to import an execution subsystem merely to display an artifact.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ImageData {
    pub mime_type: String,
    pub data: Vec<u8>,
}
