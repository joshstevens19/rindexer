//! Health-gated failover across multiple RPC endpoints for a single network.
//!
//! The [`FailoverService`] is a `tower::Service<RequestPacket>` installed
//! *under* the retry layer inside `create_client`. It owns one HTTP transport
//! per configured rpc url and keeps a single "active" endpoint that serves all
//! traffic. When a request against the active endpoint fails at the transport
//! level, the request is retried against the remaining endpoints in order
//! (healthy endpoints first) and the first endpoint that answers becomes the
//! new active endpoint. Because the failover happens inside the transport, the
//! single `RpcClient`/`RootProvider` above it — including every batch call
//! site and the leaked `Arc<RindexerProvider>` handed out by generated code —
//! follows the active endpoint automatically.
//!
//! Why not alloy's `FallbackLayer`? Its default mode is *hedged parallel
//! dispatch*: every request is raced against the top N transports. For an
//! indexer that is dangerous, not just wasteful — a lagging endpoint can win
//! the race for `eth_getBlockByNumber(latest)` and hand the reorg coordinator
//! an alternating view of the chain tip, which manifests as spurious
//! parent-hash mismatches (and it multiplies compute-unit spend on `eth_getLogs`
//! volume). Its sequential mode only applies to an explicit method allow-list,
//! and `active_transport_count = 1` suffers a cold-start tie (untried and
//! all-failing transports both score 0.0, so a dead primary is never
//! demoted). A sticky active endpoint with explicit, observable switches is
//! the correct semantic here, so the rotation is implemented directly and the
//! stock `RetryBackoffLayer` stays on top for rate-limit-aware retries.
//!
//! Endpoints are lazily chain-id verified: an endpoint is checked against the
//! configured chain id right before its first use, so booting does not fail
//! when only a fallback endpoint is down. Endpoints on the wrong chain are
//! permanently excluded from the pool.
//!
//! Every failover decision is surfaced as an [`RpcEndpointEvent`] on a
//! process-wide broadcast channel. Urls are credential-redacted before they
//! enter any event or snapshot.

use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering},
        Arc, RwLock,
    },
    task::{Context, Poll},
};

use alloy::{
    primitives::U64,
    rpc::{
        client::RpcClient,
        json_rpc::{RequestPacket, ResponsePacket},
    },
    transports::{TransportError, TransportErrorKind, TransportFut},
};
use once_cell::sync::Lazy;
use serde::Serialize;
use tokio::sync::broadcast;
use tower::{Service, ServiceExt};
use tracing::{error, info, warn};
use url::Url;

/// Consecutive transport failures on an endpoint before it is marked
/// unhealthy (deprioritised until the prober or a successful request
/// restores it).
const FAILURES_BEFORE_UNHEALTHY: u64 = 3;

/// Maximum length of an error message kept in an endpoint health snapshot.
const MAX_STORED_ERROR_LEN: usize = 200;

static RPC_ENDPOINT_EVENTS: Lazy<broadcast::Sender<RpcEndpointEvent>> =
    Lazy::new(|| broadcast::channel(512).0);

/// Subscribe to the endpoint failover events emitted by every network
/// provider in this process.
pub fn subscribe_rpc_endpoint_events() -> broadcast::Receiver<RpcEndpointEvent> {
    RPC_ENDPOINT_EVENTS.subscribe()
}

/// The process-wide sender endpoint events are published on.
pub(crate) fn rpc_endpoint_event_sender() -> broadcast::Sender<RpcEndpointEvent> {
    RPC_ENDPOINT_EVENTS.clone()
}

/// Forward every process-wide endpoint event into a host-supplied sender
/// (the optional `rpc_endpoint_events` field on `StartDetails` /
/// `StartNoCodeDetails`). The task exits when the host drops all receivers.
pub(crate) fn spawn_rpc_endpoint_event_forwarder(host: broadcast::Sender<RpcEndpointEvent>) {
    let mut events = subscribe_rpc_endpoint_events();
    tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(event) => {
                    if host.send(event).is_err() {
                        break; // host dropped every receiver
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(skipped, "host rpc endpoint event forwarder lagged - events skipped");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });
}

/// A failover event for one of a network's rpc endpoints.
///
/// All urls carried by these events are credential-redacted — they are safe
/// to log and to forward to embedding hosts.
#[derive(Debug, Clone, Serialize)]
pub enum RpcEndpointEvent {
    /// The active endpoint serving a network's traffic changed.
    Switched { network: String, chain_id: u64, from: String, to: String, reason: String },
    /// An endpoint was marked unhealthy (repeated failures, lagging tip or
    /// a wrong chain id).
    Degraded { network: String, chain_id: u64, url: String, reason: String, lag: Option<u64> },
    /// A previously degraded endpoint became healthy again.
    Recovered { network: String, chain_id: u64, url: String },
}

/// Point-in-time health of a single rpc endpoint, as exposed through
/// `ChainProvider::rpc_health`. The url is credential-redacted.
#[derive(Debug, Clone, Serialize)]
pub struct EndpointHealthSnapshot {
    pub network: String,
    pub chain_id: u64,
    pub url: String,
    pub active: bool,
    pub healthy: bool,
    /// `None` until the endpoint's chain id has been verified.
    /// `Some(false)` means the endpoint answered with the wrong chain id and
    /// is permanently excluded from the failover pool.
    pub chain_id_ok: Option<bool>,
    pub consecutive_errors: u64,
    /// Latest block number observed by the background prober, if any.
    pub observed_tip: Option<u64>,
    pub last_error: Option<String>,
}

const CHAIN_UNVERIFIED: u8 = 0;
const CHAIN_OK: u8 = 1;
const CHAIN_MISMATCH: u8 = 2;

/// Shared mutable health state for one endpoint. Fed by [`RpcLoggingLayer`]
/// (which sees every request outcome), the lag prober and the failover
/// service itself.
///
/// [`RpcLoggingLayer`]: crate::layer_extensions::RpcLoggingLayer
#[derive(Debug)]
pub struct EndpointHealth {
    redacted_url: String,
    healthy: AtomicBool,
    chain_status: AtomicU8,
    consecutive_errors: AtomicU64,
    observed_tip: AtomicU64,
    has_observed_tip: AtomicBool,
    last_error: RwLock<Option<String>>,
}

impl EndpointHealth {
    pub(crate) fn new(url: &str) -> Self {
        Self {
            redacted_url: redact_rpc_url(url),
            healthy: AtomicBool::new(true),
            chain_status: AtomicU8::new(CHAIN_UNVERIFIED),
            consecutive_errors: AtomicU64::new(0),
            observed_tip: AtomicU64::new(0),
            has_observed_tip: AtomicBool::new(false),
            last_error: RwLock::new(None),
        }
    }

    /// The credential-redacted url of this endpoint.
    pub fn redacted_url(&self) -> &str {
        &self.redacted_url
    }

    /// Record a successful request against this endpoint.
    pub fn record_success(&self) {
        self.consecutive_errors.store(0, Ordering::Release);
    }

    /// Record a failed request against this endpoint.
    pub fn record_failure(&self, error: &str) {
        self.consecutive_errors.fetch_add(1, Ordering::AcqRel);
        let truncated: String = error.chars().take(MAX_STORED_ERROR_LEN).collect();
        if let Ok(mut last_error) = self.last_error.write() {
            *last_error = Some(truncated);
        }
    }

    pub(crate) fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    pub(crate) fn set_healthy(&self, healthy: bool) {
        self.healthy.store(healthy, Ordering::Release);
    }

    pub(crate) fn consecutive_errors(&self) -> u64 {
        self.consecutive_errors.load(Ordering::Acquire)
    }

    fn chain_status(&self) -> u8 {
        self.chain_status.load(Ordering::Acquire)
    }

    fn set_chain_status(&self, status: u8) {
        self.chain_status.store(status, Ordering::Release);
    }

    pub(crate) fn record_tip(&self, tip: u64) {
        self.observed_tip.store(tip, Ordering::Release);
        self.has_observed_tip.store(true, Ordering::Release);
    }

    pub(crate) fn observed_tip(&self) -> Option<u64> {
        if self.has_observed_tip.load(Ordering::Acquire) {
            Some(self.observed_tip.load(Ordering::Acquire))
        } else {
            None
        }
    }

    fn last_error(&self) -> Option<String> {
        self.last_error.read().ok().and_then(|guard| guard.clone())
    }
}

/// Book-keeping shared between the failover transport, the provider (for
/// health snapshots and switch counters) and the background prober.
#[derive(Debug)]
pub(crate) struct FailoverState {
    network: String,
    chain_id: u64,
    endpoints: Vec<Arc<EndpointHealth>>,
    active: AtomicUsize,
    switches: AtomicU64,
    events: broadcast::Sender<RpcEndpointEvent>,
}

impl FailoverState {
    pub(crate) fn new(
        network: String,
        chain_id: u64,
        endpoints: Vec<Arc<EndpointHealth>>,
        events: broadcast::Sender<RpcEndpointEvent>,
    ) -> Self {
        assert!(!endpoints.is_empty(), "failover state requires at least one endpoint");
        Self {
            network,
            chain_id,
            endpoints,
            active: AtomicUsize::new(0),
            switches: AtomicU64::new(0),
            events,
        }
    }

    /// Monotonic count of endpoint switches since boot.
    pub(crate) fn endpoint_switches(&self) -> u64 {
        self.switches.load(Ordering::Acquire)
    }

    pub(crate) fn snapshot(&self) -> Vec<EndpointHealthSnapshot> {
        let active = self.active.load(Ordering::Acquire);
        self.endpoints
            .iter()
            .enumerate()
            .map(|(index, health)| EndpointHealthSnapshot {
                network: self.network.clone(),
                chain_id: self.chain_id,
                url: health.redacted_url.clone(),
                active: index == active,
                healthy: health.is_healthy(),
                chain_id_ok: match health.chain_status() {
                    CHAIN_OK => Some(true),
                    CHAIN_MISMATCH => Some(false),
                    _ => None,
                },
                consecutive_errors: health.consecutive_errors(),
                observed_tip: health.observed_tip(),
                last_error: health.last_error(),
            })
            .collect()
    }

    /// Mark the active endpoint unhealthy and fail over to another healthy
    /// endpoint when one exists. Called from the indexing loops when signals
    /// only they can see fire (stalled chain tip, repeated
    /// `get_latest_block` errors). No-op for single-endpoint networks —
    /// there is nowhere to go and no prober to restore the flag.
    pub(crate) fn flag_active_degraded(&self, reason: &str) {
        if self.endpoints.len() < 2 {
            return;
        }

        let active = self.active.load(Ordering::Acquire);
        let health = &self.endpoints[active];
        if !health.is_healthy() {
            return;
        }

        health.set_healthy(false);
        health.record_failure(reason);
        self.emit_degraded(active, reason, None);

        if let Some(next) = self.best_alternative(active) {
            self.switch_to(next, active, reason);
        }
    }

    /// Candidate endpoints in dispatch order: rotation starting at the active
    /// endpoint, healthy endpoints first, endpoints on the wrong chain
    /// excluded. When every endpoint is unhealthy the unhealthy ones are
    /// still returned — a degraded endpoint beats no endpoint at all.
    fn candidate_order(&self) -> Vec<usize> {
        let total = self.endpoints.len();
        let active = self.active.load(Ordering::Acquire).min(total - 1);

        let mut healthy = Vec::with_capacity(total);
        let mut unhealthy = Vec::new();
        for offset in 0..total {
            let index = (active + offset) % total;
            let health = &self.endpoints[index];
            if health.chain_status() == CHAIN_MISMATCH {
                continue;
            }
            if health.is_healthy() {
                healthy.push(index);
            } else {
                unhealthy.push(index);
            }
        }
        healthy.extend(unhealthy);
        healthy
    }

    /// The first healthy, chain-verified-or-unverified endpoint other than
    /// `excluding`, if any.
    fn best_alternative(&self, excluding: usize) -> Option<usize> {
        self.candidate_order().into_iter().find(|&index| {
            index != excluding
                && self.endpoints[index].is_healthy()
                && self.endpoints[index].chain_status() != CHAIN_MISMATCH
        })
    }

    /// Move the active endpoint from `from` to `to`, emitting a switch event.
    /// Uses compare-exchange so concurrent requests racing to switch only
    /// emit a single event.
    pub(crate) fn switch_to(&self, to: usize, from: usize, reason: &str) {
        if to == from {
            return;
        }
        if self.active.compare_exchange(from, to, Ordering::AcqRel, Ordering::Acquire).is_err() {
            return;
        }
        self.switches.fetch_add(1, Ordering::AcqRel);

        let from_url = self.endpoints[from].redacted_url.clone();
        let to_url = self.endpoints[to].redacted_url.clone();
        warn!(
            network = %self.network,
            chain_id = self.chain_id,
            from = %from_url,
            to = %to_url,
            reason,
            "RPC endpoint failover"
        );
        let _ = self.events.send(RpcEndpointEvent::Switched {
            network: self.network.clone(),
            chain_id: self.chain_id,
            from: from_url,
            to: to_url,
            reason: reason.to_string(),
        });
    }

    /// Gate an endpoint once its consecutive failures (recorded by the
    /// logging layer beneath) reach the threshold.
    fn maybe_gate_after_failure(&self, index: usize) {
        let health = &self.endpoints[index];
        if health.is_healthy() && health.consecutive_errors() >= FAILURES_BEFORE_UNHEALTHY {
            health.set_healthy(false);
            self.emit_degraded(
                index,
                &format!("{} consecutive request failures", health.consecutive_errors()),
                None,
            );
        }
    }

    pub(crate) fn emit_degraded(&self, index: usize, reason: &str, lag: Option<u64>) {
        let _ = self.events.send(RpcEndpointEvent::Degraded {
            network: self.network.clone(),
            chain_id: self.chain_id,
            url: self.endpoints[index].redacted_url.clone(),
            reason: reason.to_string(),
            lag,
        });
    }

    fn emit_recovered(&self, index: usize) {
        let _ = self.events.send(RpcEndpointEvent::Recovered {
            network: self.network.clone(),
            chain_id: self.chain_id,
            url: self.endpoints[index].redacted_url.clone(),
        });
    }

    /// Fold one round of `eth_blockNumber` probe results (index-aligned with
    /// the endpoints, `None` = probe failed) into endpoint health:
    ///
    /// - an endpoint lagging more than `lag_threshold` blocks behind the
    ///   furthest-ahead peer is marked unhealthy (`Degraded { lag }`),
    /// - a previously unhealthy endpoint whose probe succeeded within the
    ///   lag threshold is restored (`Recovered`) — this is the half-open
    ///   recovery path for endpoints gated after request failures too,
    /// - a failed probe changes nothing: hard failures are gated by the
    ///   dispatch path, and a down endpoint must not "recover" here,
    ///
    /// then fails over when the active endpoint ended up unhealthy.
    pub(crate) fn apply_probe_results(&self, tips: &[Option<u64>], lag_threshold: u64) {
        debug_assert_eq!(tips.len(), self.endpoints.len());

        let Some(peer_max_tip) = tips.iter().flatten().copied().max() else {
            return; // every probe failed - nothing to compare against
        };

        for (index, (health, tip)) in self.endpoints.iter().zip(tips).enumerate() {
            if health.chain_status() == CHAIN_MISMATCH {
                continue;
            }
            let Some(tip) = tip else {
                continue;
            };
            health.record_tip(*tip);

            let lag = peer_max_tip.saturating_sub(*tip);
            if lag > lag_threshold {
                if health.is_healthy() {
                    health.set_healthy(false);
                    warn!(
                        network = %self.network,
                        url = %health.redacted_url,
                        lag,
                        lag_threshold,
                        "RPC endpoint lagging behind peers - marked degraded"
                    );
                    self.emit_degraded(
                        index,
                        &format!(
                            "lagging {lag} blocks behind the furthest-ahead endpoint (threshold {lag_threshold})"
                        ),
                        Some(lag),
                    );
                }
            } else if !health.is_healthy() {
                health.set_healthy(true);
                health.record_success();
                info!(
                    network = %self.network,
                    url = %health.redacted_url,
                    "RPC endpoint recovered"
                );
                self.emit_recovered(index);
            }
        }

        // If the probes left the active endpoint unhealthy, proactively move
        // off it rather than waiting for a request to fail.
        let active = self.active.load(Ordering::Acquire);
        if !self.endpoints[active].is_healthy() {
            if let Some(next) = self.best_alternative(active) {
                self.switch_to(next, active, "active endpoint lagging behind peers");
            }
        }
    }
}

/// How often the background prober polls `eth_blockNumber` on every endpoint
/// of a multi-endpoint network.
pub(crate) const PROBE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

/// Timeout for a single probe request.
const PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Spawn the background lag prober for a multi-endpoint network. Every
/// `interval` it polls `eth_blockNumber` on each endpoint (verifying
/// still-unverified endpoints first, so fallbacks get chain-checked in the
/// background) and feeds the results into [`FailoverState::apply_probe_results`].
///
/// Holds only a `Weak` reference to the failover state: when the provider is
/// dropped (e.g. on hot reload) the prober task exits on its next tick.
pub(crate) fn spawn_lag_prober(
    state: &Arc<FailoverState>,
    probers: Vec<RpcClient>,
    lag_threshold: u64,
    interval: std::time::Duration,
) {
    debug_assert_eq!(probers.len(), state.endpoints.len());
    let state = Arc::downgrade(state);

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;

            let Some(state) = state.upgrade() else {
                break; // provider dropped - stop probing
            };

            let mut tips: Vec<Option<u64>> = Vec::with_capacity(probers.len());
            for (index, (health, prober)) in state.endpoints.iter().zip(&probers).enumerate() {
                // Background-verify endpoints that have not served yet;
                // never probe (or trust tips from) wrong-chain endpoints.
                if health.chain_status() == CHAIN_UNVERIFIED {
                    match tokio::time::timeout(
                        PROBE_TIMEOUT,
                        verify_chain_id(prober, state.chain_id),
                    )
                    .await
                    {
                        Ok(Ok(Ok(()))) => health.set_chain_status(CHAIN_OK),
                        Ok(Ok(Err(actual))) => {
                            health.set_chain_status(CHAIN_MISMATCH);
                            health.set_healthy(false);
                            error!(
                                network = %state.network,
                                url = %health.redacted_url,
                                expected = state.chain_id,
                                actual,
                                "RPC endpoint excluded from failover pool - wrong chain id"
                            );
                            state.emit_degraded(
                                index,
                                &format!(
                                    "wrong chain id: expected {}, endpoint returned {}",
                                    state.chain_id, actual
                                ),
                                None,
                            );
                        }
                        _ => {
                            tips.push(None);
                            continue;
                        }
                    }
                }
                if health.chain_status() == CHAIN_MISMATCH {
                    tips.push(None);
                    continue;
                }

                let tip: Option<u64> = match tokio::time::timeout(PROBE_TIMEOUT, async {
                    prober.request::<_, U64>("eth_blockNumber", ()).await
                })
                .await
                {
                    Ok(Ok(tip)) => Some(tip.to::<u64>()),
                    _ => None,
                };
                tips.push(tip);
            }

            state.apply_probe_results(&tips, lag_threshold);
        }
    });
}

/// The failover transport itself. `S` is the per-endpoint transport (in
/// production `RpcLoggingService<Http<Client>>`); the per-endpoint
/// `RpcClient` verifiers reuse the same transports for lazy chain-id checks.
#[derive(Debug, Clone)]
pub(crate) struct FailoverService<S> {
    state: Arc<FailoverState>,
    transports: Arc<Vec<S>>,
    verifiers: Arc<Vec<RpcClient>>,
}

impl<S> FailoverService<S> {
    pub(crate) fn new(
        state: Arc<FailoverState>,
        transports: Vec<S>,
        verifiers: Vec<RpcClient>,
    ) -> Self {
        assert_eq!(
            state.endpoints.len(),
            transports.len(),
            "failover transports must be index-aligned with endpoint health entries"
        );
        assert_eq!(
            transports.len(),
            verifiers.len(),
            "failover verifiers must be index-aligned with transports"
        );
        Self { state, transports: Arc::new(transports), verifiers: Arc::new(verifiers) }
    }
}

/// Verify an endpoint's chain id right before its first use. Returns:
/// - `Ok(Ok(()))` when the endpoint matches the configured chain id,
/// - `Ok(Err(actual))` when it answers with a different chain id,
/// - `Err(_)` when the endpoint could not be reached.
async fn verify_chain_id(
    verifier: &RpcClient,
    expected: u64,
) -> Result<Result<(), u64>, TransportError> {
    let actual: U64 = verifier.request("eth_chainId", ()).await?;
    let actual = actual.to::<u64>();
    Ok(if actual == expected { Ok(()) } else { Err(actual) })
}

impl<S> FailoverService<S>
where
    S: Service<RequestPacket, Response = ResponsePacket, Error = TransportError>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send,
{
    async fn dispatch(self, req: RequestPacket) -> Result<ResponsePacket, TransportError> {
        let state = &self.state;
        let order = state.candidate_order();
        if order.is_empty() {
            return Err(TransportErrorKind::custom_str(&format!(
                "no usable rpc endpoints for network {} - every endpoint failed chain-id verification against chain id {}",
                state.network, state.chain_id
            )));
        }

        let active_at_start = state.active.load(Ordering::Acquire);
        let mut last_error: Option<TransportError> = None;
        let mut switch_reason: Option<String> = None;

        for index in order {
            let health = &state.endpoints[index];

            // Lazily verify the endpoint's chain id before its first use so a
            // wrong-chain endpoint never serves data and a down fallback
            // never blocks boot.
            if health.chain_status() == CHAIN_UNVERIFIED {
                match verify_chain_id(&self.verifiers[index], state.chain_id).await {
                    Ok(Ok(())) => health.set_chain_status(CHAIN_OK),
                    Ok(Err(actual)) => {
                        health.set_chain_status(CHAIN_MISMATCH);
                        health.set_healthy(false);
                        let reason = format!(
                            "wrong chain id: expected {}, endpoint returned {}",
                            state.chain_id, actual
                        );
                        error!(
                            network = %state.network,
                            url = %health.redacted_url,
                            expected = state.chain_id,
                            actual,
                            "RPC endpoint excluded from failover pool - wrong chain id"
                        );
                        state.emit_degraded(index, &reason, None);
                        last_error = Some(TransportErrorKind::custom_str(&format!(
                            "{} is on the wrong chain (expected {}, got {})",
                            health.redacted_url, state.chain_id, actual
                        )));
                        continue;
                    }
                    Err(err) => {
                        // Unreachable right now - the RpcLoggingLayer beneath the
                        // verifier already recorded the failure. Try the next one.
                        if switch_reason.is_none() {
                            switch_reason = Some(format!(
                                "{} failed chain-id verification: {}",
                                health.redacted_url,
                                truncate_error(&err)
                            ));
                        }
                        state.maybe_gate_after_failure(index);
                        last_error = Some(err);
                        continue;
                    }
                }
            }

            match self.transports[index].clone().oneshot(req.clone()).await {
                Ok(response) => {
                    if index != active_at_start {
                        let reason = switch_reason
                            .unwrap_or_else(|| "active endpoint gated unhealthy".to_string());
                        state.switch_to(index, active_at_start, &reason);
                    }
                    return Ok(response);
                }
                Err(err) => {
                    if switch_reason.is_none() {
                        switch_reason = Some(format!(
                            "{} failed: {}",
                            health.redacted_url,
                            truncate_error(&err)
                        ));
                    }
                    // The RpcLoggingLayer beneath already recorded the failure;
                    // gate the endpoint once failures pile up.
                    state.maybe_gate_after_failure(index);
                    last_error = Some(err);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            TransportErrorKind::custom_str(&format!(
                "all rpc endpoints failed for network {}",
                state.network
            ))
        }))
    }
}

impl<S> Service<RequestPacket> for FailoverService<S>
where
    S: Service<RequestPacket, Response = ResponsePacket, Error = TransportError>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send,
{
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = TransportFut<'static>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: RequestPacket) -> Self::Future {
        let this = self.clone();
        Box::pin(async move { this.dispatch(req).await })
    }
}

fn truncate_error(error: &TransportError) -> String {
    error.to_string().chars().take(MAX_STORED_ERROR_LEN).collect()
}

/// Redact credentials from an rpc url so it is safe to log, put in events and
/// hand to embedding hosts. Keeps scheme, host and port; strips userinfo and
/// replaces any path/query (where providers put API keys) with a placeholder.
/// IPC paths are returned unchanged - they are local socket paths.
pub fn redact_rpc_url(url: &str) -> String {
    if url.ends_with(".ipc") {
        return url.to_string();
    }
    match Url::parse(url) {
        Ok(parsed) => {
            let mut redacted = format!("{}://", parsed.scheme());
            redacted.push_str(parsed.host_str().unwrap_or("<unknown-host>"));
            if let Some(port) = parsed.port() {
                redacted.push_str(&format!(":{port}"));
            }
            let path = parsed.path();
            if !path.is_empty() && path != "/" {
                redacted.push_str("/***");
            }
            if parsed.query().is_some() {
                redacted.push_str("?***");
            }
            redacted
        }
        Err(_) => "<unparseable-rpc-url>".to_string(),
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use alloy::rpc::json_rpc::{Response, ResponsePayload};
    use serde_json::value::RawValue;
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};

    /// A programmable mock endpoint transport. Answers `eth_chainId` and
    /// `eth_blockNumber` with configured values and any other method with a
    /// generic success payload; fails every call when `fail` is set.
    #[derive(Debug, Clone)]
    pub(crate) struct MockEndpoint {
        pub chain_id: Arc<AtomicU64>,
        pub block_number: Arc<AtomicU64>,
        pub fail: Arc<AtomicBool>,
        pub calls: Arc<AtomicUsize>,
    }

    impl MockEndpoint {
        pub(crate) fn new(chain_id: u64) -> Self {
            Self {
                chain_id: Arc::new(AtomicU64::new(chain_id)),
                block_number: Arc::new(AtomicU64::new(0)),
                fail: Arc::new(AtomicBool::new(false)),
                calls: Arc::new(AtomicUsize::new(0)),
            }
        }

        pub(crate) fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        fn payload_for(&self, method: &str) -> ResponsePayload {
            let raw = match method {
                "eth_chainId" => {
                    format!("\"0x{:x}\"", self.chain_id.load(Ordering::SeqCst))
                }
                "eth_blockNumber" => {
                    format!("\"0x{:x}\"", self.block_number.load(Ordering::SeqCst))
                }
                _ => "\"0x1\"".to_string(),
            };
            ResponsePayload::Success(RawValue::from_string(raw).expect("valid raw json"))
        }
    }

    impl Service<RequestPacket> for MockEndpoint {
        type Response = ResponsePacket;
        type Error = TransportError;
        type Future = TransportFut<'static>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: RequestPacket) -> Self::Future {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let this = self.clone();
            Box::pin(async move {
                if this.fail.load(Ordering::SeqCst) {
                    return Err(TransportErrorKind::custom_str("mock endpoint down"));
                }
                match req {
                    RequestPacket::Single(single) => Ok(ResponsePacket::Single(Response {
                        id: single.id().clone(),
                        payload: this.payload_for(single.method()),
                    })),
                    RequestPacket::Batch(batch) => Ok(ResponsePacket::Batch(
                        batch
                            .iter()
                            .map(|request| Response {
                                id: request.id().clone(),
                                payload: this.payload_for(request.method()),
                            })
                            .collect(),
                    )),
                }
            })
        }
    }

    /// The transport used in tests: the production logging layer (which
    /// feeds endpoint health) over a mock endpoint - the same stack shape
    /// `create_client` builds over `Http<Client>`.
    pub(crate) type MockTransport = crate::layer_extensions::RpcLoggingService<MockEndpoint>;

    /// Build a failover service over the given mock endpoints, returning the
    /// service, its shared state and an event receiver.
    pub(crate) fn failover_over_mocks(
        network: &str,
        chain_id: u64,
        mocks: &[MockEndpoint],
    ) -> (FailoverService<MockTransport>, Arc<FailoverState>, broadcast::Receiver<RpcEndpointEvent>)
    {
        use crate::layer_extensions::RpcLoggingLayer;
        use tower::Layer as _;

        let (events, receiver) = broadcast::channel(64);
        let mut healths: Vec<Arc<EndpointHealth>> = Vec::with_capacity(mocks.len());
        let mut transports: Vec<MockTransport> = Vec::with_capacity(mocks.len());
        let mut verifiers: Vec<RpcClient> = Vec::with_capacity(mocks.len());
        for (index, mock) in mocks.iter().enumerate() {
            let url = format!("https://endpoint-{index}.example.com/key-{index}");
            let health = Arc::new(EndpointHealth::new(&url));
            let transport = RpcLoggingLayer::new(chain_id, url)
                .with_health(Arc::clone(&health))
                .layer(mock.clone());
            verifiers.push(RpcClient::new(transport.clone(), false));
            transports.push(transport);
            healths.push(health);
        }
        let state = Arc::new(FailoverState::new(network.to_string(), chain_id, healths, events));
        let service = FailoverService::new(Arc::clone(&state), transports, verifiers);
        (service, state, receiver)
    }

    /// Issue a simple `eth_blockNumber` request through the failover service.
    pub(crate) async fn send_request(
        service: &FailoverService<MockTransport>,
    ) -> Result<ResponsePacket, TransportError> {
        use alloy::rpc::json_rpc::{Id, Request};
        let request = Request::new("eth_blockNumber", Id::Number(1), ());
        let serialized = request.serialize().expect("serializable request");
        service.clone().oneshot(RequestPacket::Single(serialized)).await
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;

    fn drain_events(receiver: &mut broadcast::Receiver<RpcEndpointEvent>) -> Vec<RpcEndpointEvent> {
        let mut events = Vec::new();
        while let Ok(event) = receiver.try_recv() {
            events.push(event);
        }
        events
    }

    #[tokio::test]
    async fn sticky_active_endpoint_serves_all_traffic() {
        let mocks = [MockEndpoint::new(1), MockEndpoint::new(1)];
        let (service, state, mut events) = failover_over_mocks("ethereum", 1, &mocks);

        for _ in 0..3 {
            send_request(&service).await.expect("request should succeed");
        }

        // 1 chain-id verification + 3 requests on the primary, nothing on the fallback.
        assert_eq!(mocks[0].call_count(), 4);
        assert_eq!(mocks[1].call_count(), 0);
        assert_eq!(state.endpoint_switches(), 0);
        assert!(drain_events(&mut events).is_empty(), "no events for healthy primary");
    }

    #[tokio::test]
    async fn failing_primary_rotates_to_fallback_and_emits_switch() {
        let mocks = [MockEndpoint::new(1), MockEndpoint::new(1)];
        mocks[0].fail.store(true, Ordering::SeqCst);
        let (service, state, mut events) = failover_over_mocks("ethereum", 1, &mocks);

        send_request(&service).await.expect("fallback should serve the request");

        assert_eq!(state.endpoint_switches(), 1);
        let events = drain_events(&mut events);
        assert!(
            matches!(
                events.last(),
                Some(RpcEndpointEvent::Switched { network, from, to, .. })
                    if network == "ethereum"
                        && from.contains("endpoint-0")
                        && to.contains("endpoint-1")
            ),
            "expected a Switched event, got {events:?}"
        );

        // Follow-up requests go straight to the new active endpoint.
        let fallback_calls = mocks[1].call_count();
        let primary_calls = mocks[0].call_count();
        send_request(&service).await.expect("request should succeed");
        assert_eq!(mocks[0].call_count(), primary_calls, "dead primary must not be retried");
        assert_eq!(mocks[1].call_count(), fallback_calls + 1);
    }

    #[tokio::test]
    async fn gate_skips_unhealthy_endpoint_without_calling_it() {
        let mocks = [MockEndpoint::new(1), MockEndpoint::new(1)];
        let (service, state, mut events) = failover_over_mocks("ethereum", 1, &mocks);

        // The prober (or the indexing loop) marked the primary as lagging.
        state.endpoints[0].set_healthy(false);

        send_request(&service).await.expect("healthy fallback should serve the request");

        assert_eq!(mocks[0].call_count(), 0, "gated endpoint must not receive traffic");
        assert_eq!(state.endpoint_switches(), 1);
        assert!(matches!(
            drain_events(&mut events).last(),
            Some(RpcEndpointEvent::Switched { .. })
        ));
    }

    #[tokio::test]
    async fn all_endpoints_unhealthy_still_serves() {
        let mocks = [MockEndpoint::new(1), MockEndpoint::new(1)];
        let (service, state, _events) = failover_over_mocks("ethereum", 1, &mocks);

        state.endpoints[0].set_healthy(false);
        state.endpoints[1].set_healthy(false);

        send_request(&service).await.expect("an unhealthy endpoint beats no endpoint at all");
        assert_eq!(state.endpoint_switches(), 0, "active endpoint answered, no switch");
    }

    #[tokio::test]
    async fn wrong_chain_endpoint_is_excluded_from_the_pool() {
        let mocks = [MockEndpoint::new(5), MockEndpoint::new(1)];
        let (service, state, mut events) = failover_over_mocks("ethereum", 1, &mocks);

        send_request(&service).await.expect("correct-chain endpoint should serve");

        let snapshots = state.snapshot();
        assert_eq!(snapshots[0].chain_id_ok, Some(false));
        assert_eq!(snapshots[1].chain_id_ok, Some(true));
        assert!(snapshots[1].active);

        let events = drain_events(&mut events);
        assert!(
            events.iter().any(|event| matches!(
                event,
                RpcEndpointEvent::Degraded { reason, .. } if reason.contains("wrong chain id")
            )),
            "expected a wrong-chain Degraded event, got {events:?}"
        );

        // The wrong-chain endpoint saw exactly one call (the verification) and
        // is never dialled again.
        let wrong_chain_calls = mocks[0].call_count();
        assert_eq!(wrong_chain_calls, 1);
        send_request(&service).await.expect("request should succeed");
        assert_eq!(mocks[0].call_count(), wrong_chain_calls);
    }

    #[tokio::test]
    async fn all_endpoints_failing_surfaces_the_error() {
        let mocks = [MockEndpoint::new(1), MockEndpoint::new(1)];
        mocks[0].fail.store(true, Ordering::SeqCst);
        mocks[1].fail.store(true, Ordering::SeqCst);
        let (service, _state, _events) = failover_over_mocks("ethereum", 1, &mocks);

        let result = send_request(&service).await;
        assert!(result.is_err(), "errors must surface when every endpoint is down");
    }

    #[tokio::test]
    async fn repeated_failures_mark_endpoint_unhealthy_and_emit_degraded() {
        let mocks = [MockEndpoint::new(1), MockEndpoint::new(1)];
        let (service, state, mut events) = failover_over_mocks("ethereum", 1, &mocks);

        // Verify + serve one request so the primary is chain-verified.
        send_request(&service).await.expect("request should succeed");

        // With sticky failover, consecutive failures only pile up on an
        // endpoint while every endpoint is failing (a single bad endpoint is
        // rotated away from after one error).
        mocks[0].fail.store(true, Ordering::SeqCst);
        mocks[1].fail.store(true, Ordering::SeqCst);
        for _ in 0..=FAILURES_BEFORE_UNHEALTHY {
            send_request(&service).await.expect_err("every endpoint is down");
        }

        assert!(
            !state.endpoints[0].is_healthy(),
            "primary should be gated after repeated failures"
        );
        let events = drain_events(&mut events);
        assert!(
            events.iter().any(|event| matches!(
                event,
                RpcEndpointEvent::Degraded { reason, .. }
                    if reason.contains("consecutive request failures")
            )),
            "expected a Degraded event, got {events:?}"
        );

        // Gated endpoints are still tried as a last resort, so recovery of
        // the underlying endpoint restores service immediately.
        mocks[0].fail.store(false, Ordering::SeqCst);
        mocks[1].fail.store(false, Ordering::SeqCst);
        send_request(&service).await.expect("gated endpoints are still dialled as a last resort");
    }

    #[tokio::test]
    async fn probe_results_gate_a_lagging_fallback() {
        let mocks = [MockEndpoint::new(1), MockEndpoint::new(1)];
        let (_service, state, mut events) = failover_over_mocks("ethereum", 1, &mocks);

        state.apply_probe_results(&[Some(100), Some(40)], 20);

        assert!(state.endpoints[0].is_healthy());
        assert!(!state.endpoints[1].is_healthy(), "endpoint lagging 60 blocks must be gated");
        assert_eq!(state.endpoint_switches(), 0, "active endpoint is fine - no switch");
        let events = drain_events(&mut events);
        assert!(
            matches!(events.last(), Some(RpcEndpointEvent::Degraded { lag: Some(60), .. })),
            "expected a Degraded event with the observed lag, got {events:?}"
        );
    }

    #[tokio::test]
    async fn probe_results_evict_a_lagging_active_endpoint() {
        let mocks = [MockEndpoint::new(1), MockEndpoint::new(1)];
        let (_service, state, mut events) = failover_over_mocks("ethereum", 1, &mocks);

        state.apply_probe_results(&[Some(40), Some(100)], 20);

        assert!(!state.endpoints[0].is_healthy());
        assert_eq!(state.endpoint_switches(), 1, "lagging active endpoint must be evicted");
        let events = drain_events(&mut events);
        assert!(matches!(events.first(), Some(RpcEndpointEvent::Degraded { .. })));
        assert!(matches!(events.last(), Some(RpcEndpointEvent::Switched { .. })));
    }

    #[tokio::test]
    async fn probe_results_recover_a_previously_degraded_endpoint() {
        let mocks = [MockEndpoint::new(1), MockEndpoint::new(1)];
        let (_service, state, mut events) = failover_over_mocks("ethereum", 1, &mocks);

        state.apply_probe_results(&[Some(40), Some(100)], 20);
        assert!(!state.endpoints[0].is_healthy());
        drain_events(&mut events);

        // The old endpoint catches back up within the lag threshold.
        state.apply_probe_results(&[Some(98), Some(100)], 20);

        assert!(state.endpoints[0].is_healthy(), "caught-up endpoint must be restored");
        assert_eq!(
            state.endpoint_switches(),
            1,
            "recovery must not switch back - the active endpoint is sticky"
        );
        let events = drain_events(&mut events);
        assert!(
            matches!(events.last(), Some(RpcEndpointEvent::Recovered { .. })),
            "expected a Recovered event, got {events:?}"
        );
        assert_eq!(state.snapshot()[0].observed_tip, Some(98));
    }

    #[tokio::test]
    async fn failed_probes_change_nothing() {
        let mocks = [MockEndpoint::new(1), MockEndpoint::new(1)];
        let (_service, state, mut events) = failover_over_mocks("ethereum", 1, &mocks);

        // A failed probe must not gate an endpoint (hard failures are gated
        // by the dispatch path) and must not recover one either.
        state.apply_probe_results(&[None, Some(100)], 20);
        assert!(state.endpoints[0].is_healthy());

        state.endpoints[0].set_healthy(false);
        state.apply_probe_results(&[None, Some(100)], 20);
        assert!(
            !state.endpoints[0].is_healthy(),
            "a down endpoint must not recover on probe failure"
        );

        state.apply_probe_results(&[None, None], 20);
        assert_eq!(state.endpoint_switches(), 1, "eviction of the gated active endpoint only");
        drain_events(&mut events);
    }

    #[tokio::test]
    async fn lag_prober_evicts_and_recovers_in_the_background() {
        let mocks = [MockEndpoint::new(1), MockEndpoint::new(1)];
        mocks[0].block_number.store(100, Ordering::SeqCst);
        mocks[1].block_number.store(200, Ordering::SeqCst);
        let (_service, state, mut events) = failover_over_mocks("ethereum", 1, &mocks);

        let probers =
            mocks.iter().map(|mock| RpcClient::new(mock.clone(), false)).collect::<Vec<_>>();
        spawn_lag_prober(&state, probers, 20, std::time::Duration::from_millis(20));

        // Wait for the prober to verify both endpoints and evict the
        // lagging primary.
        for _ in 0..100 {
            if state.endpoint_switches() >= 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(state.endpoint_switches(), 1, "prober must evict the lagging primary");
        assert!(!state.endpoints[0].is_healthy());
        let snapshots = state.snapshot();
        assert_eq!(
            snapshots[0].chain_id_ok,
            Some(true),
            "prober verifies endpoints in the background"
        );
        assert_eq!(snapshots[1].chain_id_ok, Some(true));

        // The primary catches up - the prober restores it.
        mocks[0].block_number.store(200, Ordering::SeqCst);
        for _ in 0..100 {
            if state.endpoints[0].is_healthy() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(state.endpoints[0].is_healthy(), "prober must recover a caught-up endpoint");

        let events = drain_events(&mut events);
        assert!(events.iter().any(|event| matches!(event, RpcEndpointEvent::Degraded { .. })));
        assert!(events.iter().any(|event| matches!(event, RpcEndpointEvent::Switched { .. })));
        assert!(events.iter().any(|event| matches!(event, RpcEndpointEvent::Recovered { .. })));
    }

    #[tokio::test]
    async fn flag_active_degraded_switches_and_emits_events() {
        let mocks = [MockEndpoint::new(1), MockEndpoint::new(1)];
        let (_service, state, mut events) = failover_over_mocks("ethereum", 1, &mocks);

        state.flag_active_degraded("chain tip stalled");

        assert_eq!(state.endpoint_switches(), 1);
        assert!(!state.endpoints[0].is_healthy());
        let events = drain_events(&mut events);
        assert!(matches!(events.first(), Some(RpcEndpointEvent::Degraded { .. })));
        assert!(matches!(events.last(), Some(RpcEndpointEvent::Switched { .. })));
    }

    #[tokio::test]
    async fn flag_active_degraded_is_a_noop_for_single_endpoint() {
        let mocks = [MockEndpoint::new(1)];
        let (_service, state, mut events) = failover_over_mocks("ethereum", 1, &mocks);

        state.flag_active_degraded("chain tip stalled");

        assert!(state.endpoints[0].is_healthy(), "single endpoint must never be gated");
        assert_eq!(state.endpoint_switches(), 0);
        assert!(drain_events(&mut events).is_empty());
    }

    #[test]
    fn snapshot_reports_per_endpoint_state() {
        let (events, _receiver) = broadcast::channel(8);
        let healths = vec![
            Arc::new(EndpointHealth::new("https://user:secret@one.example.com/v2/api-key")),
            Arc::new(EndpointHealth::new("https://two.example.com")),
        ];
        healths[1].record_failure("boom");
        let state = FailoverState::new("base".to_string(), 8453, healths, events);

        let snapshots = state.snapshot();
        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0].network, "base");
        assert_eq!(snapshots[0].chain_id, 8453);
        assert!(snapshots[0].active);
        assert_eq!(snapshots[0].url, "https://one.example.com/***");
        assert_eq!(snapshots[0].chain_id_ok, None);
        assert_eq!(snapshots[1].observed_tip, None, "no prober ran yet");
        assert_eq!(snapshots[1].consecutive_errors, 1);
        assert_eq!(snapshots[1].last_error.as_deref(), Some("boom"));
    }

    #[test]
    fn redaction_strips_credentials_and_keys() {
        assert_eq!(
            redact_rpc_url(
                "https://user:password@rpc.example.com:8545/v2/super-secret-key?token=x"
            ),
            "https://rpc.example.com:8545/***?***"
        );
        assert_eq!(
            redact_rpc_url("https://eth-mainnet.g.alchemy.com/v2/abc123"),
            "https://eth-mainnet.g.alchemy.com/***"
        );
        assert_eq!(redact_rpc_url("https://rpc.example.com"), "https://rpc.example.com");
        assert_eq!(redact_rpc_url("https://rpc.example.com/"), "https://rpc.example.com");
        assert_eq!(redact_rpc_url("/var/run/reth.ipc"), "/var/run/reth.ipc");
        assert_eq!(redact_rpc_url("not a url"), "<unparseable-rpc-url>");
    }

    #[tokio::test]
    async fn forwarder_relays_events_to_a_host_sender() {
        let (host_tx, mut host_rx) = broadcast::channel(16);
        spawn_rpc_endpoint_event_forwarder(host_tx);

        // Give the forwarder task a beat to subscribe before emitting.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let healths = vec![
            Arc::new(EndpointHealth::new("https://one.example.com")),
            Arc::new(EndpointHealth::new("https://two.example.com")),
        ];
        let state = FailoverState::new(
            "forwarder-relays-test".to_string(),
            1,
            healths,
            rpc_endpoint_event_sender(),
        );
        state.flag_active_degraded("test reason");

        let mut saw_switch = false;
        for _ in 0..50 {
            match tokio::time::timeout(std::time::Duration::from_millis(100), host_rx.recv()).await
            {
                Ok(Ok(RpcEndpointEvent::Switched { network, .. }))
                    if network == "forwarder-relays-test" =>
                {
                    saw_switch = true;
                    break;
                }
                Ok(Ok(_)) => continue,
                _ => break,
            }
        }
        assert!(saw_switch, "the host sender must receive forwarded failover events");
    }

    #[tokio::test]
    async fn events_reach_the_process_wide_subscription() {
        // The production constructor wires FailoverState to the global
        // broadcast channel - emulate that here and make sure a subscriber
        // sees the event.
        let mut receiver = subscribe_rpc_endpoint_events();
        let healths = vec![
            Arc::new(EndpointHealth::new("https://one.example.com")),
            Arc::new(EndpointHealth::new("https://two.example.com")),
        ];
        let state = FailoverState::new(
            "events-reach-subscription-test".to_string(),
            1,
            healths,
            rpc_endpoint_event_sender(),
        );

        state.flag_active_degraded("test reason");

        let mut saw_switch = false;
        while let Ok(event) = receiver.try_recv() {
            if let RpcEndpointEvent::Switched { network, .. } = event {
                if network == "events-reach-subscription-test" {
                    saw_switch = true;
                }
            }
        }
        assert!(saw_switch, "global subscribers must observe failover events");
    }
}
