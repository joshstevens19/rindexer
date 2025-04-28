use alloy::primitives::{Address, BlockHash, TxHash, U256, U64};
use serde::{Deserialize, Serialize};

/// Metadata inside a log
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogMeta {
    /// Address from which this log originated
    pub address: Address,

    /// The block in which the log was emitted
    pub block_number: U64,

    /// The block hash in which the log was emitted
    pub block_hash: BlockHash,

    /// The transaction hash in which the log was emitted
    pub transaction_hash: TxHash,

    /// Transactions index position log was created from
    pub transaction_index: U64,

    /// Log index position in the block
    pub log_index: U256,
}
