use ethers::{
    abi::Address,
    providers::Middleware,
    types::{Filter, H256},
};

use crate::generator::event_callback_registry::EventCallbackRegistry;

pub async fn start(registry: EventCallbackRegistry) -> Result<(), Box<dyn std::error::Error>> {
    // 1. Get the current block numbers for networks
    // 2. Get the logs between blocks for each event and resync back
    // 3. When hits the head block, start listening to new blocks and pushing activity to check for new logs
    // 4. Use blooms to filter out logs that are not relevant

    let max_block_range = 2000;

    for event in &registry.events {
        let snapshot_block = event.provider.get_block_number().await?.as_u64();
        let start_block = event.source.start_block.unwrap_or(snapshot_block);
        let end_block = event.source.end_block.unwrap();

        let mut current_block = start_block;

        while current_block < end_block && current_block < snapshot_block {
            let next_block = std::cmp::min(current_block + max_block_range, end_block);
            println!("current_block: {}", current_block);
            println!("next_block: {}", next_block);
            let filter: Filter = Filter::new()
                .address(event.source.address.parse::<Address>()?)
                .topic0(event.topic_id.parse::<H256>()?)
                .from_block(current_block)
                .to_block(next_block);

            let logs = event.provider.get_logs(&filter).await?;

            println!("logs: {}", logs.len());

            for log in logs {
                let decoded = event.decode_log(log);
                registry.trigger_event(event.topic_id, decoded)
            }

            current_block = next_block + 1;
        }
    }

    Ok(())

    // for event in &registry.events {
    //     let filter: Filter = Filter::new()
    //         .address(event.source.address.parse::<Address>().unwrap())
    //         .topic0(event.topic_id.parse::<H256>().unwrap())
    //         .from_block(event.source.start_block.unwrap());
    //         //.to_block(49929866);

    //     let result = event.provider.get_logs(&filter).await.unwrap();

    //     for log in result {
    //         let decoded = event.decode_log(log);
    //         registry.trigger_event(event.topic_id, decoded)
    //     }
    // }
}
