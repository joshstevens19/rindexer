use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum StringOrArray {
    Single(String),
    Multiple(Vec<String>),
}

impl From<String> for StringOrArray {
    fn from(s: String) -> Self {
        Self::Single(s)
    }
}
