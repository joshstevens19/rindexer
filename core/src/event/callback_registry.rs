use std::{any::Any, sync::Arc, time::Duration};

use ethers::{
    addressbook::Address,
    contract::LogMeta,
    types::{Bytes, Log, H256, U256, U64},
};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use tracing::{debug, error, info};

use crate::{
    event::contract_setup::{ContractInformation, NetworkContract},
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
        let mut attempts = 0;
        let mut delay = Duration::from_millis(100);

        if let Some(event_information) = self.find_event(id) {
            debug!("{} - Pushed {} events", data.len(), event_information.info_log_name());

            loop {
                if !is_running() {
                    info!("Detected shutdown, stopping event trigger");
                    break;
                }

                match (event_information.callback)(data.clone()).await {
                    Ok(_) => {
                        debug!(
                            "Event processing succeeded for id: {} - topic_id: {}",
                            id, event_information.topic_id
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
                            event_information.info_log_name(), id, event_information.topic_id, attempts, e
                        );

                        delay = (delay * 2).min(Duration::from_secs(15));

                        sleep(delay).await;
                    }
                }
            }
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
