# Structure

## Cli flow

The CLI Represents the entrypoint for most no-code projects. When the CLI `start` command is run it will go 
through the following flow:

```
cli (start) -> `fn setup_no_code` -> fn `start_rindexer`
```

This parses the manifest, ensures the dependencies are available and configured, and includes the dependencies in 
a particular format made available to the code. Specifically into an `EventRegistry` and `NetworkProvider` struct that
allows the code to easily reference "providers" and the events.

The `fn process_events` in `no_code.rs` file takes the defined contract events and associates all metadata, including stream
publishing metadata (postgres + csv, and any other defined stream info) to a single struct made available ot the program.

This simplifies knowledge of the "Events" we care about in the rindexer program.

Eventually once this parsing is complete the `fn start_rindexer` is called, which kicks of indexing.

## Start Rindexer

Once the `fn start_rindexer` has been called, the actual services are started. This includes graphql (if enabled), the 
postgres setup process, and of course the actual `indexing` process.

There is a multi-stage indexing flow, meaning the core `start_indexing` is called twice here.

1. The first step, is to historically index events, dropping postgres relationships and indexes if required to speed this up
2. The next goal is to re-apply indexes and relationships and call `start_indexing` again for live-indexing.

## Start Indexing

We process events based on two branching paths. The first is again, if there are dependency events, and the second is if 
we just care about the single event directly (i.e. no dependent events).

The dependent events flow is more complicated, so for now let's stick to simple "Single Event Indexing".

1. We first configure a `semaphore` to limit the amount of concurrency in fetching logs. This is currently manually set to `100`.
2. Associate each "event" we care to index with `Streams` we will publish to, and other network metadata.
3. Get the latest network block, and ensure the YAML start-end block range is valid and offset by reorg-safe distance if required.

The function returns a `ProcessedNetworkContract` struct which contains a network name and the last block it indexed. This 
is to support the previously mentioned flow about "Historical Indexing" running first, and the "Live indexing".

The Live indexing takes the `ProcessedNetworkContract` information returned from the last function execution and resumes 
"live indexing" from the "last processed block" onwards.

It is this final live `fn start_indexing` that will hold the process and run indefinitely from now on. You can think of this as 
running the same code except `start_block` is now set to last known block and `end_block` becomes None. 

If it has already reached the user-defined `end_block` it will simply not continue indexing.

### Processing Events

We now have the `EventProcessingConfig` struct prepared for live indexing and will pass that to `fn process_event`.

For every contract-event that we define in the config, we push a handle to tokio and await completion. Which for live-indexing
is never (only on shutdown or error will it complete).

The `fn process_event` routes through to `fn process_event_logs`.

In this process we will fetch logs in a stream (`fn fetch_logs_stream`) and process the logs into structured data.

### Fetch logs stream

This generator-style function is responsible for looping through blocks whilst respecting provider limitations and 
emitting the filtered logs from the blocks.

Since we are focused on live indexing, lets skip over the "historical" loop aspect of this function, and instead discuss
the `fn live_indexing_stream` that is called. This function accepts the `transmitter` for the channel. The `receiver` is  
then passed to the `UnboundedReceiverStream` and returned by the `fn fetch_logs_stream` to be consumed by whatever
log-handlers are required (will be discussed later).

You can think of the `fetch_logs_stream` as the `LogFetch` actor which is responsible for accepting "messages" related to the
network, block range, and other filters it should process. And providing a steam of those logs to the caller.

**Live indexing (log fetcher)**

Live indexing runs in a direct `loop {}` configured with 200ms poll intervals. It will avoid getting too close to head if
reorg safety is enabled.

We continually poll for the `latest_block`.We first ensure there is a matching contract (if contract is present) with Bloom 
filters and then use a range of all blocks between `last_known` and `latest` when fetching a batch of logs from a 
network rpc provider.

Once we have the logs we forward a message (recall the actor model analogy) back to the `LogHandler`... And the loop continues!

**Live indexing (log handler)**

The `fn handle_logs_result` receives the messages passed by the log fetcher. This too runs in a "loop" consuming the available
messages from the channel.

The log handler parses the logs based on the contract-event `Abi`, associated transaction metadata and passes it to the 
`EventCallback`. This callback function accepts a generic event and returns a Future.

The `EventCallback` is configured in the very beginning of the `no_code` setup in the CLI. Callback include 
- Stream processors (Kafka, SQS, RabbitMQ, etc)
- Chatbot endpoints (Telegram, Discord, etc)
- Postgres writes (if enabled)
- CSV writes (if enabled)

Finally, once the callback has returned successfully, the last synced block is updated in memory, and written to the 
datastore (postgres or csv). 

This continues for the duration of the running program.