use std::{any::Any, sync::Arc};

use futures::future::BoxFuture;

use ethers::prelude::RetryClient;
use ethers::{
    providers::{Http, Provider},
    types::{Bytes, Log, H256},
};

type Decoder = Arc<dyn Fn(Vec<H256>, Bytes) -> Arc<dyn Any + Send + Sync> + Send + Sync>;

#[derive(Clone)]
pub struct NetworkContract {
    pub network: String,

    pub address: String,

    pub provider: &'static Arc<Provider<RetryClient<Http>>>,

    pub decoder: Decoder,

    pub start_block: Option<u64>,

    pub end_block: Option<u64>,

    pub polling_every: Option<u64>,
}

impl NetworkContract {
    pub fn decode_log(&self, log: Log) -> Arc<dyn Any + Send + Sync> {
        (self.decoder)(log.topics, log.data)
    }
}

#[derive(Clone)]
pub struct ContractInformation {
    pub name: String,
    pub details: Vec<NetworkContract>,
    pub abi: String,
}

pub struct EventInformation {
    pub topic_id: &'static str,
    pub contract: ContractInformation,
    pub callback: Arc<
        dyn Fn(Vec<Arc<dyn Any + Send + Sync>>, String) -> BoxFuture<'static, ()> + Send + Sync,
    >,
}

impl Clone for EventInformation {
    fn clone(&self) -> Self {
        EventInformation {
            topic_id: self.topic_id,
            contract: self.contract.clone(),
            callback: Arc::clone(&self.callback),
        }
    }
}

#[derive(Clone)]
pub struct EventCallbackRegistry {
    pub events: Vec<EventInformation>,
}

impl EventCallbackRegistry {
    pub fn new() -> Self {
        EventCallbackRegistry { events: Vec::new() }
    }

    pub fn find_event(&self, topic_id: &'static str) -> Option<&EventInformation> {
        self.events.iter().find(|e| e.topic_id == topic_id)
    }

    pub fn register_event(&mut self, event: EventInformation) {
        self.events.push(event);
    }

    pub async fn trigger_event(
        &self,
        topic_id: &'static str,
        network: String,
        data: Vec<Arc<dyn Any + Send + Sync>>,
    ) {
        if let Some(callback) = self.find_event(topic_id).map(|e| &e.callback) {
            callback(data, network).await;
        } else {
            println!(
                "EventCallbackRegistry: No event found for topic_id: {}",
                topic_id
            );
        }
    }

    pub fn complete(&self) -> Arc<Self> {
        Arc::new(self.clone())
    }
}
