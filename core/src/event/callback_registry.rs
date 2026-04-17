use alloy::consensus::Transaction;
use alloy::network::{AnyRpcTransaction, TransactionResponse};
use alloy::{
    primitives::{Address, BlockHash, Bytes, TxHash, B256, U256, U64},
    rpc::types::{
        trace::parity::{CallAction, LocalizedTransactionTrace},
        Log,
    },
};
use chrono::Utc;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::{any::Any, sync::Arc, time::Duration};
use tokio::sync::broadcast::Sender;
use tokio::time::sleep;
use tracing::{debug, error, info};

use crate::indexer::tables::TableRuntime;
use crate::manifest::core::Constants;
use crate::provider::ChainProvider;
use crate::streams::StreamsClients;
use crate::{
    event::contract_setup::{ContractInformation, NetworkContract, TraceInformation},
    indexer::start::ProcessedNetworkContract,
    is_running, ReorgEvent,
};

pub type Decoder = Arc<dyn Fn(Vec<TxHash>, Bytes) -> Arc<dyn Any + Send + Sync> + Send + Sync>;

pub fn noop_decoder() -> Decoder {
    Arc::new(move |_topics: Vec<TxHash>, _data: Bytes| {
        Arc::new(String::new()) as Arc<dyn Any + Send + Sync>
    }) as Decoder
}

/// The [`CallbackResult`] enum has two core variants, a Trace and an Event. We implement shared
/// callback logic to sink or stream these "results".
///
/// Since each event is different, and we want `rust` project consumers to not worry about manually
/// mapping their [`EventResult`] into a [`CallbackResult`], we handle this for them internally and
/// this struct allows us to do this behind the scenes.
#[derive(Clone)]
pub enum CallbackResult {
    Event(Vec<EventResult>),
    Trace(Vec<TraceResult>),
}

impl CallbackResult {
    /// return the (from_block, to_block, network) entry regardless of variant
    pub fn first_metadata(&self) -> Option<(U64, U64, String)> {
        match self {
            Self::Event(events) => events.first().map(|e| {
                (
                    e.found_in_request.from_block,
                    e.found_in_request.to_block,
                    e.tx_information.network.clone(),
                )
            }),
            Self::Trace(traces) => traces.first().map(|t| {
                let (fir, tx) = match t {
                    TraceResult::NativeTransfer { found_in_request, tx_information, .. } => {
                        (found_in_request, tx_information)
                    }
                    TraceResult::Block { found_in_request, tx_information, .. } => {
                        (found_in_request, tx_information)
                    }
                };
                (fir.from_block, fir.to_block, tx.network.clone())
            }),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TxInformation {
    pub chain_id: u64,
    pub network: String,
    pub address: Address,
    pub block_hash: BlockHash,
    pub block_number: u64,
    pub block_timestamp: Option<U256>,
    pub transaction_hash: TxHash,
    pub log_index: U256,
    pub transaction_index: u64,
}

impl TxInformation {
    pub fn block_timestamp_to_datetime(&self) -> Option<chrono::DateTime<Utc>> {
        if let Some(timestamp) = self.block_timestamp {
            let timestamp = timestamp.to::<i64>();
            Some(chrono::DateTime::from_timestamp(timestamp, 0).expect("invalid timestamp"))
        } else {
            None
        }
    }
}

/// Define a trait over any entity that has attached transaction information. This is very useful
/// when working with on-chain generics over many event types.
pub trait HasTxInformation {
    /// Return the transaction information associated with an event.
    fn tx_information(&self) -> &TxInformation;
}

#[derive(Debug, Clone)]
pub struct LogFoundInRequest {
    pub from_block: U64,
    pub to_block: U64,
}

#[derive(Debug, Clone)]
pub struct EventResult {
    pub log: Log,
    pub decoded_data: Arc<dyn Any + Send + Sync>,
    pub tx_information: TxInformation,
    pub found_in_request: LogFoundInRequest,
}

impl EventResult {
    pub fn new(
        network_contract: Arc<NetworkContract>,
        log: Log,
        start_block: U64,
        end_block: U64,
    ) -> Self {
        let log_address = log.inner.address;
        Self {
            log: log.clone(),
            decoded_data: network_contract.decode_log(log.inner),
            tx_information: TxInformation {
                chain_id: network_contract.cached_provider.chain().id(),
                network: network_contract.network.to_string(),
                address: log_address,
                block_hash: log.block_hash.expect("log should contain block_hash"),
                block_number: log.block_number.expect("log should contain block_number"),
                block_timestamp: log.block_timestamp.map(U256::from),
                transaction_hash: log
                    .transaction_hash
                    .expect("log should contain transaction_hash"),
                transaction_index: log
                    .transaction_index
                    .expect("log should contain transaction_index"),
                log_index: U256::from(log.log_index.expect("log should contain log_index")),
            },
            found_in_request: LogFoundInRequest { from_block: start_block, to_block: end_block },
        }
    }
}

pub type EventCallbackResult<T> = Result<T, String>;

pub type EventCallbackType =
    Arc<dyn Fn(Vec<EventResult>) -> BoxFuture<'static, EventCallbackResult<()>> + Send + Sync>;
pub type TraceCallbackType =
    Arc<dyn Fn(Vec<TraceResult>) -> BoxFuture<'static, EventCallbackResult<()>> + Send + Sync>;

#[derive(Clone)]
pub struct ReorgNotification {
    pub network: String,
    pub fork_block: u64,
    pub detection_block: u64,
    pub invalidated_tx_hashes: Vec<TxHash>,
}

pub type OnReorgCallback = Arc<dyn Fn(ReorgNotification) -> BoxFuture<'static, ()> + Send + Sync>;

pub struct EventCallbackRegistryInformation {
    pub id: String,
    pub indexer_name: String,
    pub topic_id: B256,
    pub event_name: String,
    pub index_event_in_order: bool,
    pub contract: ContractInformation,
    pub callback: EventCallbackType,
    /// Derived/custom tables for this event (for reorg cleanup).
    pub tables: Arc<Vec<TableRuntime>>,
    /// Broadcast sender for reorg events (code-gen mode).
    pub reorg_sender: Option<Sender<ReorgEvent>>,
    /// Streams clients for reorg retraction.
    pub streams_clients: Arc<Option<StreamsClients>>,
    /// RPC providers by network, required for replaying table operations with view calls.
    pub providers: Arc<HashMap<String, Arc<dyn ChainProvider>>>,
    /// Manifest constants used by table expressions.
    pub constants: Arc<Constants>,
    /// Multicall address overrides by network.
    pub multicall_addresses: Arc<HashMap<String, Option<String>>>,
}

impl EventCallbackRegistryInformation {
    pub fn info_log_name(&self) -> String {
        format!("{}::{}", self.contract.name, self.event_name)
    }

    pub fn is_factory_filter_event(&self) -> bool {
        self.contract.details.iter().all(|d| {
            // it's a factory contract if the factory filter matches the contract name and event name
            matches!(
                d.indexing_contract_setup.factory_details(),
                Some(f) if f.contract_name == self.contract.name && f.event.name == self.event_name
            )
        })
    }
}

impl Clone for EventCallbackRegistryInformation {
    fn clone(&self) -> Self {
        EventCallbackRegistryInformation {
            id: self.id.clone(),
            indexer_name: self.indexer_name.clone(),
            topic_id: self.topic_id,
            event_name: self.event_name.clone(),
            index_event_in_order: self.index_event_in_order,
            contract: self.contract.clone(),
            callback: Arc::clone(&self.callback),
            tables: self.tables.clone(),
            reorg_sender: self.reorg_sender.clone(),
            streams_clients: self.streams_clients.clone(),
            providers: self.providers.clone(),
            constants: self.constants.clone(),
            multicall_addresses: self.multicall_addresses.clone(),
        }
    }
}

#[derive(Clone)]
pub struct EventCallbackRegistry {
    pub events: Vec<EventCallbackRegistryInformation>,
    pub on_reorg: Vec<OnReorgCallback>,
}

impl Default for EventCallbackRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl EventCallbackRegistry {
    pub fn new() -> Self {
        EventCallbackRegistry { events: Vec::new(), on_reorg: Vec::new() }
    }

    pub fn find_event(&self, id: &String) -> Option<&EventCallbackRegistryInformation> {
        self.events.iter().find(|e| e.id == *id)
    }

    pub fn register_event(&mut self, event: EventCallbackRegistryInformation) {
        self.events.push(event);
    }

    pub fn register_on_reorg(&mut self, callback: OnReorgCallback) {
        self.on_reorg.push(callback);
    }

    pub async fn fire_on_reorg(&self, notification: ReorgNotification) {
        for callback in &self.on_reorg {
            callback(notification.clone()).await;
        }
    }

    pub async fn trigger_event(&self, id: &String, data: Vec<EventResult>) -> Result<(), String> {
        if let Some(event_information) = self.find_event(id) {
            trigger_event(
                id,
                data,
                |d| (event_information.callback)(d),
                || event_information.info_log_name(),
                &event_information.topic_id.to_string(),
            )
            .await
        } else {
            let message = format!(
                "EventCallbackRegistry: No event found for id: {}. Data: {:?}",
                id,
                data.first()
            );
            error!(message);
            Err(message)
        }
    }

    pub fn complete(&self) -> Arc<Self> {
        Arc::new(self.clone())
    }

    pub fn reapply_after_historic(
        &mut self,
        processed_network_contracts: Vec<ProcessedNetworkContract>,
    ) -> Arc<EventCallbackRegistry> {
        self.events.iter_mut().for_each(|e| {
            e.contract.details.iter_mut().for_each(|d| {
                if d.end_block.is_none() {
                    if let Some(processed_block) =
                        processed_network_contracts.iter().find(|c| c.id == d.id)
                    {
                        d.start_block = Some(processed_block.processed_up_to);
                    }
                }
            });
        });

        // Retain only the details with `end_block.is_none()`
        self.events.iter_mut().for_each(|e| {
            e.contract.details.retain(|d| d.end_block.is_none());
        });

        // Retain only the events that have details with `end_block.is_none()`
        self.events.retain(|e| !e.contract.details.is_empty());

        self.complete()
    }
}

// --------------------------------
// "Native" Trace Callback Registry
// --------------------------------

#[derive(Debug, Clone)]
pub enum TraceResult {
    NativeTransfer {
        from: Address,
        to: Address,
        value: U256,
        tx_information: TxInformation,
        found_in_request: LogFoundInRequest,
    },
    Block {
        block: Box<alloy::network::AnyRpcBlock>,
        tx_information: TxInformation,
        found_in_request: LogFoundInRequest,
    },
}

impl TraceResult {
    /// Create a "NativeTransfer" TraceResult for sinking and streaming.
    pub fn new_debug_native_transfer(
        action: &CallAction,
        trace: &LocalizedTransactionTrace,
        network: &str,
        chain_id: u64,
        start_block: U64,
        end_block: U64,
    ) -> Self {
        if trace.block_number.is_none() {
            error!(
                "Unexpected block trace None for `block_number` in {} - {}",
                start_block, end_block
            );
        }

        Self::NativeTransfer {
            from: action.from,
            to: action.to,
            value: action.value,
            tx_information: TxInformation {
                chain_id,
                network: network.to_string(),
                address: Address::ZERO,
                // TODO: Unclear in what situation this would be `None`.
                block_number: trace.block_number.unwrap_or(0),
                block_timestamp: None,
                transaction_hash: trace.transaction_hash.unwrap_or(TxHash::ZERO),
                block_hash: trace.block_hash.unwrap_or(BlockHash::ZERO),
                transaction_index: trace.transaction_position.unwrap_or(0),
                log_index: U256::from(0),
            },
            found_in_request: LogFoundInRequest { from_block: start_block, to_block: end_block },
        }
    }

    /// Create a "NativeTransfer" TraceResult from a `eth_getBlockByNumber` Transaction.
    pub fn new_native_transfer(
        tx: AnyRpcTransaction,
        ts: u64,
        to: Address,
        network: &str,
        chain_id: u64,
        start_block: U64,
        end_block: U64,
    ) -> Self {
        Self::NativeTransfer {
            to,
            from: tx.from(),
            value: tx.value(),
            tx_information: TxInformation {
                chain_id,
                network: network.to_string(),
                address: Address::ZERO,
                block_number: tx.block_number.expect("block_number should be present"),
                block_timestamp: Some(U256::from(ts)),
                transaction_hash: tx.tx_hash(),
                block_hash: tx.block_hash.expect("block_hash should be present"),
                transaction_index: tx
                    .transaction_index
                    .expect("transaction_index should be present"),
                log_index: U256::from(0),
            },
            found_in_request: LogFoundInRequest { from_block: start_block, to_block: end_block },
        }
    }

    /// Create a "Block" TraceResult for block events.
    pub fn new_block(
        block: alloy::network::AnyRpcBlock,
        network: &str,
        chain_id: u64,
        start_block: U64,
        end_block: U64,
    ) -> Self {
        Self::Block {
            tx_information: TxInformation {
                chain_id,
                block_timestamp: Some(U256::from(block.header.timestamp)),
                network: network.to_string(),
                block_number: block.header.number,
                block_hash: block.header.hash,

                // Invalid fields for a block event.
                address: Address::ZERO,
                transaction_hash: TxHash::ZERO,
                transaction_index: 0,
                log_index: U256::from(0),
            },
            block: Box::new(block),
            found_in_request: LogFoundInRequest { from_block: start_block, to_block: end_block },
        }
    }
}

pub type TraceCallbackResult<T> = Result<T, String>;

#[derive(Clone)]
pub struct TraceCallbackRegistryInformation {
    pub id: String,
    pub indexer_name: String,
    pub event_name: String,
    pub contract_name: String,
    pub trace_information: TraceInformation,
    pub callback: TraceCallbackType,
    /// Streams clients for reorg retraction. Shared with the callback params so the
    /// reorg coordinator can publish rollback notifications to the same stream the
    /// native-transfer pipeline writes to.
    pub streams_clients: Arc<Option<StreamsClients>>,
}

impl TraceCallbackRegistryInformation {
    pub fn info_log_name(&self) -> String {
        format!("{}::{}", self.indexer_name, self.event_name)
    }
}

#[derive(Clone, Default)]
pub struct TraceCallbackRegistry {
    pub events: Vec<TraceCallbackRegistryInformation>,
}

impl TraceCallbackRegistry {
    pub fn new() -> Self {
        TraceCallbackRegistry { events: Vec::new() }
    }

    pub fn find_event(&self, id: &String) -> Option<&TraceCallbackRegistryInformation> {
        self.events.iter().find(|e| e.id == *id)
    }

    pub fn register_event(&mut self, event: TraceCallbackRegistryInformation) {
        self.events.push(event);
    }

    pub async fn trigger_event(&self, id: &String, data: Vec<TraceResult>) -> Result<(), String> {
        if let Some(event_information) = self.find_event(id) {
            trigger_event(
                id,
                data,
                |d| (event_information.callback)(d),
                || event_information.info_log_name(),
                &event_information.event_name,
            )
            .await
        } else {
            let message = format!("TraceCallbackRegistry: No event found for id: {id}");
            error!("TraceCallbackRegistry: No event found for id: {}", id);
            Err(message)
        }
    }

    pub fn complete(&self) -> Arc<Self> {
        Arc::new(self.clone())
    }
}

async fn trigger_event<T>(
    id: &String,
    data: Vec<T>,
    callback: impl Fn(Vec<T>) -> BoxFuture<'static, EventCallbackResult<()>>,
    info_log_name: impl Fn() -> String,
    event_identifier: &str,
) -> Result<(), String>
where
    T: Clone,
{
    let mut attempts = 0;
    let mut delay = Duration::from_millis(100);

    let len = data.len();
    debug!("{} - Pushed {} events", len, info_log_name());

    loop {
        if !is_running() {
            info!("Detected shutdown, stopping event trigger");
            return Err("Detected shutdown, stopping event trigger".to_string());
        }

        match callback(data.clone()).await {
            Ok(_) => {
                debug!(
                    "Event processing succeeded for id: {} - topic_id: {}",
                    id, event_identifier
                );
                return Ok(());
            }
            Err(e) => {
                if !is_running() {
                    info!("Detected shutdown, stopping event trigger");
                    return Err(e);
                }
                attempts += 1;
                error!(
                    "{} Event processing failed - id: {} - topic_id: {}. Retrying... (attempt {}). Error: {}",
                    info_log_name(), id, event_identifier, attempts, e
                );

                delay = (delay * 2).min(Duration::from_secs(15));

                sleep(delay).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::network::AnyRpcBlock;

    fn test_tx_information(network: &str) -> TxInformation {
        TxInformation {
            chain_id: 31337,
            network: network.to_string(),
            address: Address::ZERO,
            block_hash: BlockHash::ZERO,
            block_number: 0,
            block_timestamp: None,
            transaction_hash: TxHash::ZERO,
            transaction_index: 0,
            log_index: U256::ZERO,
        }
    }

    fn test_found_in_request(from_block: u64, to_block: u64) -> LogFoundInRequest {
        LogFoundInRequest { from_block: U64::from(from_block), to_block: U64::from(to_block) }
    }

    //Minimal valid json for alloy::network::AnyRpcBlock deserialization
    const TEST_BLOCK_JSON: &str = r#"{
        "number": "0x0",
        "hash": "0x0000000000000000000000000000000000000000000000000000000000000000",
        "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
        "sha3Uncles": "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
        "logsBloom": "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
        "transactionsRoot": "0x0000000000000000000000000000000000000000000000000000000000000000",
        "stateRoot": "0x0000000000000000000000000000000000000000000000000000000000000000",
        "receiptsRoot": "0x0000000000000000000000000000000000000000000000000000000000000000",
        "miner": "0x0000000000000000000000000000000000000000",
        "difficulty": "0x0",
        "extraData": "0x",
        "size": "0x0",
        "gasLimit": "0x0",
        "gasUsed": "0x0",
        "timestamp": "0x0",
        "transactions": [],
        "uncles": []
    }"#;

    const TEST_LOG_JSON: &str = r#"{
        "address": "0x0000000000000000000000000000000000000000",
        "topics": [],
        "data": "0x",
        "blockHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
        "blockNumber": "0x0",
        "transactionHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
        "transactionIndex": "0x0",
        "logIndex": "0x0",
        "removed": false
    }"#;

    fn test_block() -> AnyRpcBlock {
        serde_json::from_str(TEST_BLOCK_JSON).expect("test block JSON should deserialize")
    }

    fn test_log() -> Log {
        serde_json::from_str(TEST_LOG_JSON).expect("test log JSON should deserialize")
    }

    #[test]
    fn first_metadata_event_returns_fields_from_first_entry() {
        let event = EventResult {
            log: test_log(),
            decoded_data: Arc::new(()),
            tx_information: test_tx_information("mainnet"),
            found_in_request: test_found_in_request(10, 20),
        };
        let result = CallbackResult::Event(vec![event]);
        let (from_block, to_block, network) = result.first_metadata().expect("non-empty batch");
        assert_eq!(from_block, U64::from(10));
        assert_eq!(to_block, U64::from(20));
        assert_eq!(network, "mainnet");
    }

    #[test]
    fn first_metadata_trace_native_transfer_returns_fields() {
        let nt = TraceResult::NativeTransfer {
            from: Address::ZERO,
            to: Address::ZERO,
            value: U256::ZERO,
            tx_information: test_tx_information("anvil"),
            found_in_request: test_found_in_request(5, 15),
        };
        let result = CallbackResult::Trace(vec![nt]);
        let (from_block, to_block, network) = result.first_metadata().expect("non-empty batch");
        assert_eq!(from_block, U64::from(5));
        assert_eq!(to_block, U64::from(15));
        assert_eq!(network, "anvil");
    }

    //native_transfer_block_consumer always fires trigger_event with a Block-only batch before firing the
    // NativeTransfer batch, so first_metadata must
    // extract cleanly from a TraceResult::Block without panicking
    #[test]
    fn first_metadata_trace_block_returns_fields() {
        let b = TraceResult::Block {
            block: Box::new(test_block()),
            tx_information: test_tx_information("polygon"),
            found_in_request: test_found_in_request(100, 200),
        };
        let result = CallbackResult::Trace(vec![b]);
        let (from_block, to_block, network) = result.first_metadata().expect("non-empty batch");
        assert_eq!(from_block, U64::from(100));
        assert_eq!(to_block, U64::from(200));
        assert_eq!(network, "polygon");
    }

    #[test]
    fn first_metadata_empty_event_returns_none() {
        let result = CallbackResult::Event(vec![]);
        assert!(result.first_metadata().is_none());
    }

    #[test]
    fn first_metadata_empty_trace_returns_none() {
        let result = CallbackResult::Trace(vec![]);
        assert!(result.first_metadata().is_none());
    }
}
