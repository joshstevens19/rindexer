//! JSON-RPC proxy between rindexer and anvil that injects empty `eth_getLogs` answers.
//!
//! Every request body is forwarded to anvil unchanged, except a single-object `eth_getLogs`
//! whose filter has the shape the shared tip-block fetcher sends (one block, no address, no
//! topics): those may be answered `[]` by the configured [`Behaviour`], which reproduces an
//! upstream that has not indexed the block yet.

use anyhow::{Context, Result};
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashSet};
use std::fmt::Display;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

/// How unfiltered `eth_getLogs` calls are answered; the test mutates it at any time.
#[derive(Debug, Clone)]
pub struct Behaviour {
    /// Empty answers injected per block number before the call is forwarded.
    pub empty_unfiltered_answers_per_block: u32,
    /// Blocks answered empty on every unfiltered call.
    pub always_empty_blocks: HashSet<u64>,
}

impl Default for Behaviour {
    fn default() -> Self {
        Self { empty_unfiltered_answers_per_block: 1, always_empty_blocks: HashSet::new() }
    }
}

#[derive(Debug, Default)]
struct Counters {
    unfiltered_calls_by_block: BTreeMap<u64, u32>,
    empty_answers_by_block: BTreeMap<u64, u32>,
    filtered_calls_total: u64,
}

/// What the proxy made of one request body.
enum Classified {
    /// `eth_getLogs` for one block with no address and no topics: the shared fetcher's call.
    UnfilteredGetLogs { id: Value, block: u64 },
    /// Any other `eth_getLogs`: a per-stream call.
    FilteredGetLogs,
    /// Everything else, batches included.
    Other,
}

struct ProxyState {
    upstream: String,
    client: reqwest::Client,
    behaviour: Mutex<Behaviour>,
    counters: Mutex<Counters>,
}

impl ProxyState {
    fn behaviour(&self) -> MutexGuard<'_, Behaviour> {
        self.behaviour.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn counters(&self) -> MutexGuard<'_, Counters> {
        self.counters.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Counts an unfiltered call for `block` and decides whether it is answered empty.
    fn inject_empty(&self, block: u64) -> bool {
        let behaviour = self.behaviour();
        let mut counters = self.counters();
        *counters.unfiltered_calls_by_block.entry(block).or_insert(0) += 1;
        let given = counters.empty_answers_by_block.entry(block).or_insert(0);
        let inject = behaviour.always_empty_blocks.contains(&block)
            || *given < behaviour.empty_unfiltered_answers_per_block;
        if inject {
            *given += 1;
        }
        inject
    }

    fn count_filtered(&self) {
        self.counters().filtered_calls_total += 1;
    }

    async fn forward(&self, body: Bytes) -> Response {
        let upstream = self
            .client
            .post(&self.upstream)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await;
        let response = match upstream {
            Ok(response) => response,
            Err(error) => return bad_gateway(error),
        };
        let status = StatusCode::from_u16(response.status().as_u16())
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        match response.bytes().await {
            Ok(bytes) => {
                (status, [(header::CONTENT_TYPE, "application/json")], bytes).into_response()
            }
            Err(error) => bad_gateway(error),
        }
    }
}

fn bad_gateway(error: impl Display) -> Response {
    warn!("rpc proxy: upstream request failed: {error}");
    (StatusCode::BAD_GATEWAY, format!("rpc proxy: upstream request failed: {error}"))
        .into_response()
}

async fn handle(State(state): State<Arc<ProxyState>>, body: Bytes) -> Response {
    match classify(&body) {
        Classified::UnfilteredGetLogs { id, block } => {
            if state.inject_empty(block) {
                debug!("rpc proxy: answered the unfiltered eth_getLogs for block {block} empty");
                let answer = serde_json::json!({ "jsonrpc": "2.0", "id": id, "result": [] });
                return (
                    StatusCode::OK,
                    [(header::CONTENT_TYPE, "application/json")],
                    answer.to_string(),
                )
                    .into_response();
            }
        }
        Classified::FilteredGetLogs => state.count_filtered(),
        Classified::Other => {}
    }
    state.forward(body).await
}

fn classify(body: &[u8]) -> Classified {
    let Ok(request) = serde_json::from_slice::<Value>(body) else {
        return Classified::Other;
    };
    let Some(object) = request.as_object() else {
        return Classified::Other;
    };
    if object.get("method").and_then(Value::as_str) != Some("eth_getLogs") {
        return Classified::Other;
    }
    let filter = object.get("params").and_then(|params| params.get(0)).and_then(Value::as_object);
    match filter.and_then(unfiltered_block) {
        Some(block) => {
            let id = object.get("id").cloned().unwrap_or(Value::Null);
            Classified::UnfilteredGetLogs { id, block }
        }
        None => Classified::FilteredGetLogs,
    }
}

/// The block of a filter with the unfiltered shape: `fromBlock == toBlock` as hex quantities,
/// no address (absent, null or an empty array) and no topics (absent, null, or every entry
/// null or an empty array; alloy's `Filter` serializer writes `"topics": []` for no topics).
fn unfiltered_block(filter: &Map<String, Value>) -> Option<u64> {
    let from = hex_quantity(filter.get("fromBlock")?)?;
    let to = hex_quantity(filter.get("toBlock")?)?;
    if from != to {
        return None;
    }
    let address_empty = match filter.get("address") {
        None | Some(Value::Null) => true,
        Some(Value::Array(addresses)) => addresses.is_empty(),
        Some(_) => false,
    };
    let topics_empty = match filter.get("topics") {
        None | Some(Value::Null) => true,
        Some(Value::Array(topics)) => topics.iter().all(unconstrained_topic),
        Some(_) => false,
    };
    (address_empty && topics_empty).then_some(from)
}

fn unconstrained_topic(topic: &Value) -> bool {
    match topic {
        Value::Null => true,
        Value::Array(values) => values.is_empty(),
        _ => false,
    }
}

fn hex_quantity(value: &Value) -> Option<u64> {
    u64::from_str_radix(value.as_str()?.strip_prefix("0x")?, 16).ok()
}

/// A running proxy; dropping it shuts the server down.
pub struct RpcProxy {
    url: String,
    state: Arc<ProxyState>,
    shutdown: Option<oneshot::Sender<()>>,
    server: Option<JoinHandle<()>>,
}

impl RpcProxy {
    /// Binds a loopback port and forwards to `upstream` (the anvil URL).
    pub async fn start(upstream: &str) -> Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.context("bind the rpc proxy")?;
        let addr: SocketAddr = listener.local_addr().context("rpc proxy local address")?;
        let state = Arc::new(ProxyState {
            upstream: upstream.to_string(),
            client: reqwest::Client::new(),
            behaviour: Mutex::new(Behaviour::default()),
            counters: Mutex::new(Counters::default()),
        });
        let app = Router::new().route("/", post(handle)).with_state(Arc::clone(&state));
        let (shutdown, shutdown_rx) = oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            let serve = axum::serve(listener, app).with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            });
            if let Err(error) = serve.await {
                warn!("rpc proxy: server error: {error}");
            }
        });
        let url = format!("http://{addr}");
        info!("rpc proxy listening on {url}, forwarding to {upstream}");
        Ok(Self { url, state, shutdown: Some(shutdown), server: Some(server) })
    }

    /// The URL rindexer's manifest points at.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Mutable access to the injection behaviour.
    pub fn behaviour(&self) -> MutexGuard<'_, Behaviour> {
        self.state.behaviour()
    }

    /// Unfiltered `eth_getLogs` calls seen so far, by block number.
    pub fn unfiltered_calls_by_block(&self) -> BTreeMap<u64, u32> {
        self.state.counters().unfiltered_calls_by_block.clone()
    }

    /// Unfiltered `eth_getLogs` calls seen so far, all blocks.
    pub fn unfiltered_calls_total(&self) -> u64 {
        self.state
            .counters()
            .unfiltered_calls_by_block
            .values()
            .map(|calls| u64::from(*calls))
            .sum()
    }

    /// `eth_getLogs` calls that did not have the unfiltered shape.
    pub fn filtered_calls_total(&self) -> u64 {
        self.state.counters().filtered_calls_total
    }

    /// Stops accepting connections and waits for the server task.
    pub async fn stop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(server) = self.server.take() {
            if tokio::time::timeout(Duration::from_secs(5), server).await.is_err() {
                warn!("rpc proxy: server did not stop within 5 s");
            }
        }
    }
}

impl Drop for RpcProxy {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(server) = self.server.take() {
            server.abort();
        }
    }
}
