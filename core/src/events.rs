/// Events emitted during the indexing process for external consumption.
#[derive(Debug, Clone)]
pub enum RindexerEvent {
    /// The indexing process has completed indexing historical events (happens every restart of the indexer)
    HistoricalIndexingCompleted,

    /// All event processors on a chain have indexed up to this block.
    /// It does not emit per block, but rather once all events have indexed up to the block.
    BlockIndexingCompleted {
        /// The chain ID of the network (e.g., 1 for Ethereum mainnet)
        chain_id: u64,
        /// The block number that all processors have indexed up to
        block_number: u64,
    },
}

/// A handle to subscribe to indexer events
#[derive(Clone)]
pub struct RindexerEventStream {
    tx: tokio::sync::broadcast::Sender<RindexerEvent>,
}

impl Default for RindexerEventStream {
    fn default() -> Self {
        Self::new()
    }
}

impl RindexerEventStream {
    pub fn new() -> Self {
        let (tx, _) = tokio::sync::broadcast::channel(100);
        Self { tx }
    }

    /// Subscribe to indexer events
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<RindexerEvent> {
        self.tx.subscribe()
    }
}

/// A handle to subscribe to indexer events
#[derive(Clone)]
pub struct RindexerEventEmitter {
    tx: tokio::sync::broadcast::Sender<RindexerEvent>,
}

impl RindexerEventEmitter {
    pub fn from_stream(stream: RindexerEventStream) -> Self {
        Self { tx: stream.tx.clone() }
    }

    pub fn emit(&self, event: RindexerEvent) {
        let _ = self.tx.send(event);
    }
}
