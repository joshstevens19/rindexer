use ethers::{
    abi::Address,
    providers::Middleware,
    types::{Filter, H256},
};

use crate::generator::event_callback_registry::EventCallbackRegistry;

pub async fn start(registry: EventCallbackRegistry) -> () {
    // 1. Get the current block numbers for networks
    // 2. Get the logs between blocks for each event and resync back
    // 3. When hits the head block, start listening to new blocks and pushing activity to check for new logs
    // 4. Use blooms to filter out logs that are not relevant

    for event in &registry.events {
        let filter: Filter = Filter::new()
            .address(event.source.address.parse::<Address>().unwrap())
            .topic0(event.topic_id.parse::<H256>().unwrap())
            .from_block(event.source.start_block.unwrap());
        // .to_block(6175245);

        let result = event.provider.get_logs(&filter).await.unwrap();

        for log in result {
            let decoded = event.decode_log(log);
            registry.trigger_event(event.topic_id, decoded)
        }
    }
}
