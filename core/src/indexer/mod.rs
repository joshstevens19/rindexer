mod process;
mod progress;

pub use progress::{IndexingEventProgressStatus, IndexingEventsProgressState};
use serde::{Deserialize, Serialize};

pub mod tables;
mod dependency;
pub use dependency::ContractEventDependenciesMapFromRelationshipsError;
mod fetch_logs;
pub use fetch_logs::FetchLogsResult;
mod last_synced;
pub mod native_transfer;
pub mod no_code;
mod reorg;
pub mod start;
pub mod task_tracker;

pub use dependency::{ContractEventDependencies, EventDependencies, EventsDependencyTree};

use crate::manifest::{contract::Contract, native_transfer::NativeTransfers};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Indexer {
    pub name: String,

    pub contracts: Vec<Contract>,

    pub native_transfers: NativeTransfers,
}
