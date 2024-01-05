use crate::generator::event_callback_registry::EventCallbackRegistry;

pub fn start(registry: EventCallbackRegistry) -> () {
    // registry.trigger_event(
    //     "0xc906270cebe7667882104effe64262a73c422ab9176a111e05ea837b021065fc",
    //     &(1),
    // );

    // 1. Get the current block numbers for networks
    // 2. Get the logs between blocks for each event and resync back
    // 3. When hits the head block, start listening to new blocks and pushing activity to check for new logs
    // 4. Use blooms to filter out logs that are not relevant
}
