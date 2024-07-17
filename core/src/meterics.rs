use std::{
    net::SocketAddr,
    sync::{Arc, RwLock},
};

use metrics::{
    counter, describe_counter, describe_gauge, describe_histogram, gauge, Gauge, Histogram,
};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use metrics_util::layers::{PrefixLayer, Stack};
use prometheus::{Encoder, TextEncoder};
use prometheus_metric_storage::StorageRegistry;
use reqwest::Body;
use tokio::sync::oneshot;
use tokio_postgres::Client;

struct MetricsConfig {
    listen_address: SocketAddr,
}

trait DbMetrics {}

struct MetricsService {
    rindexer_http_port: Gauge,
    rindexer_http_requests: Gauge,
    rindexer_http_requests_duration: Histogram,
    rindexer_rpc_requests: Gauge,
    rindexer_rpc_requests_duration: Histogram,
    registry: Arc<RwLock<StorageRegistry>>,
}

impl MetricsService {
    pub fn install_recorder() -> PrometheusHandle {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        Stack::new(recorder)
            .push(PrefixLayer::new("rindexer"))
            .install()
            .expect("couldn't set metrics recorder");

        handle
    }
    pub fn setup_metrics(&self, listen_addr: SocketAddr) {
        describe_gauge!("rindexer_http_port", "rindexer HTTP port");
        describe_gauge!("rindexer_http_requests", "rindexer HTTP requests");
        describe_histogram!("rindexer_http_requests_duration", "rindexer HTTP requests duration");
        describe_gauge!("rindexer_rpc_requests", "rindexer RPC requests");
        describe_histogram!("rindexer_rpc_requests_duration", "rindexer RPC requests duration");
        let registry = Arc::new(RwLock::new(StorageRegistry::default()));
    }

    // pub fn connection_string() -> Result<

    async fn metrics_server(&self, listen_addr: SocketAddr, handle: PrometheusHandle) {
        let client = reqwest::Client::new();
        // let (tx, rx) = oneshot::channel();
        let metrics_endpoint = format!("http://localhost:{}/metrics", listen_addr.port());
        let encoder = prometheus::TextEncoder::new();
        let mut buffer = vec![];
        // tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            let metric_families = prometheus::gather();
            encoder.encode(&metric_families, &mut buffer).unwrap();
            let response =
                String::from_utf8(buffer.clone()).expect("Failed to convert metrics to string");
            buffer.clear();
        }
        //})
    }
}
