use std::{any::Any, sync::Arc, time::Duration};

use ethers::{
    addressbook::Address,
    contract::LogMeta,
    types::{Bytes, Call, Log, Trace, H256, U256, U64},
};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use tracing::{debug, error, info};

use crate::{
    event::contract_setup::{ContractInformation, NetworkContract, TraceInformation},
    indexer::start::ProcessedNetworkContract,
    is_running,
    provider::WrappedLog,
};

pub type Decoder = Arc<dyn Fn(Vec<H256>, Bytes) -> Arc<dyn Any + Send + Sync> + Send + Sync>;

pub fn noop_decoder() -> Decoder {
    Arc::new(move |_topics: Vec<H256>, _data: Bytes| {
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TxInformation {
    pub network: String,
    pub address: Address,
    pub block_hash: H256,
    pub block_number: U64,
    pub block_timestamp: Option<U256>,
    pub transaction_hash: H256,
    pub log_index: U256,
    pub transaction_index: U64,
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
        log: WrappedLog,
        start_block: U64,
        end_block: U64,
    ) -> Self {
        let log_meta = LogMeta::from(&log.inner);
        let log_address = log.inner.address;
        Self {
            log: log.inner.clone(),
            decoded_data: network_contract.decode_log(log.inner),
            tx_information: TxInformation {
                network: network_contract.network.to_string(),
                address: log_address,
                block_hash: log_meta.block_hash,
                block_number: log_meta.block_number,
                block_timestamp: log.block_timestamp,
                transaction_hash: log_meta.transaction_hash,
                transaction_index: log_meta.transaction_index,
                log_index: log_meta.log_index,
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

pub struct EventCallbackRegistryInformation {
    pub id: String,
    pub indexer_name: String,
    pub topic_id: H256,
    pub event_name: String,
    pub index_event_in_order: bool,
    pub contract: ContractInformation,
    pub callback: EventCallbackType,
}

impl EventCallbackRegistryInformation {
    pub fn info_log_name(&self) -> String {
        format!("{}::{}", self.contract.name, self.event_name)
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
        }
    }
}

#[derive(Clone)]
pub struct EventCallbackRegistry {
    pub events: Vec<EventCallbackRegistryInformation>,
}

impl Default for EventCallbackRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl EventCallbackRegistry {
    pub fn new() -> Self {
        EventCallbackRegistry { events: Vec::new() }
    }

    pub fn find_event(&self, id: &String) -> Option<&EventCallbackRegistryInformation> {
        self.events.iter().find(|e| e.id == *id)
    }

    pub fn register_event(&mut self, event: EventCallbackRegistryInformation) {
        self.events.push(event);
    }

    pub async fn trigger_event(&self, id: &String, data: Vec<EventResult>) {
        if let Some(event_information) = self.find_event(id) {
            trigger_event(
                id,
                data,
                |d| (event_information.callback)(d),
                || event_information.info_log_name(),
                &event_information.topic_id.to_string(),
            )
            .await;
        } else {
            error!("EventCallbackRegistry: No event found for id: {}", id);
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
pub struct TraceResult {
    pub from: Address,
    pub to: Address,
    pub value: U256,
    pub tx_information: TxInformation,
    pub found_in_request: LogFoundInRequest,
}

impl TraceResult {
    /// Create a "NativeTransfer" TraceResult for sinking and streaming.
    pub fn new_native_transfer(
        action: &Call,
        trace: &Trace,
        network: &str,
        start_block: U64,
        end_block: U64,
    ) -> Self {
        Self {
            from: action.from,
            to: action.to,
            value: action.value,
            tx_information: TxInformation {
                network: network.to_string(),
                address: Address::zero(),
                block_number: U64::from(trace.block_number),
                block_timestamp: None,
                transaction_hash: trace.transaction_hash.expect("checked prior"),
                block_hash: trace.block_hash,
                transaction_index: U64::from(trace.transaction_position.unwrap_or(0)),
                log_index: U256::from(0),
            },
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

    pub async fn trigger_event(&self, id: &String, data: Vec<TraceResult>) {
        if let Some(event_information) = self.find_event(id) {
            trigger_event(
                id,
                data,
                |d| (event_information.callback)(d),
                || event_information.info_log_name(),
                &event_information.event_name,
            )
            .await;
        } else {
            error!("EventCallbackRegistry: No event found for id: {}", id);
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
) where
    T: Clone,
{
    let mut attempts = 0;
    let mut delay = Duration::from_millis(100);

    let len = data.len();
    debug!("{} - Pushed {} events", len, info_log_name());

    loop {
        if !is_running() {
            info!("Detected shutdown, stopping event trigger");
            break;
        }

        match callback(data.clone()).await {
            Ok(_) => {
                debug!(
                    "Event processing succeeded for id: {} - topic_id: {}",
                    id, event_identifier
                );
                break;
            }
            Err(e) => {
                if !is_running() {
                    info!("Detected shutdown, stopping event trigger");
                    break;
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
