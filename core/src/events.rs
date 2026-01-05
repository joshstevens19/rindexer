/// Events emitted during the indexing process for external consumption.
#[derive(Debug, Clone)]
pub enum RindexerEvent {
    /// The indexing process has completed indexing historical events (happens every restart of the indexer)
    HistoricalIndexingCompleted,
}

/// A handle to subscribe to indexer events
#[derive(Clone)]
pub struct RindexerEventStream {
    tx: tokio::sync::broadcast::Sender<RindexerEvent>,
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
