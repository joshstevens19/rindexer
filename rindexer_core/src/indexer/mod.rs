mod fetch_logs;
mod progress;

use crate::manifest::yaml::Contract;
pub use progress::IndexingEventProgressStatus;
use serde::{Deserialize, Serialize};

pub mod no_code;
mod reorg;
pub mod start;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Indexer {
    pub name: String,

    pub contracts: Vec<Contract>,
}
