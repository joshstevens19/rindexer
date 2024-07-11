use serde::{Deserialize, Serialize};

fn default_port() -> u16 {
    3001
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct GraphQLSettings {
    #[serde(default = "default_port")]
    pub port: u16,

    #[serde(default)]
    pub disable_advanced_filters: bool,

    #[serde(default)]
    pub filter_only_on_indexed_columns: bool,
}

impl GraphQLSettings {
    pub fn set_port(&mut self, port: u16) {
        self.port = port;
    }
}
