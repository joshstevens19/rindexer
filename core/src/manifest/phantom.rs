use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PhantomShadow {
    pub api_key: String,
    pub fork_id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PhantomDyrpc {
    pub api_key: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Phantom {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dyrpc: Option<PhantomDyrpc>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow: Option<PhantomShadow>,
}

impl Phantom {
    pub fn dyrpc_enabled(&self) -> bool {
        self.dyrpc.is_some()
    }

    pub fn shadow_enabled(&self) -> bool {
        self.shadow.is_some()
    }
}
