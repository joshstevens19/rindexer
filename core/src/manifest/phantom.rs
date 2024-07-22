use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PhantomOverlay {
    pub api_key: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Phantom {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overlay: Option<PhantomOverlay>,
}

impl Phantom {
    pub fn overlay_enabled(&self) -> bool {
        self.overlay.is_some()
    }
}
