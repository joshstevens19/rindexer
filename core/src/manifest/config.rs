use serde::{Deserialize, Serialize};

/// Advanced config options for tuning rindexer beyond its default settings.
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Config {
    /// Sets the buffer of events we hold in memory per "network-event". Useful for balancing
    /// memory with event throughput on large scale backfill operations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buffer: Option<usize>,

    /// Sets the per "network-event" handler callback rate, which will increase/decrease throughput
    /// depending on the specific logic found in the handler.
    ///
    /// If `index_event_in_order` is used, this option will always be set as `1` (sequential).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub callback_concurrency: Option<usize>,

    /// Optionally configure a worst case sample rate.
    ///
    /// In cases where a batch of logs includes thousands of blocks we will not call every block for
    /// a timestamp, but will instead sample blocks and interpolate the remaining timestamps.
    ///
    /// In other cases where we are requesting a small handful of blocks in a single batch rpc this
    /// sample rate will not be applied. The sample rate should be considered a "worst case"
    /// acceptable rate.
    ///
    /// For many applications this will be `1.0` or no error tolerance. Where only loose-time ordering
    /// is required this can provide considerable speedup and RPC CU reduction at minimal accuracy loss.
    ///
    /// The default is `1.0`, which represents no sampling. A high sample rate would be `0.1` and a
    /// reasonable one would be `0.01` or below. Modern chains are suprisingly consistent in their
    /// block times so often no accuracy loss occurs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_sample_rate: Option<f32>,

    /// Maximum number of concurrent view calls (`$call()`) to RPC nodes.
    ///
    /// View calls in custom tables can generate significant RPC load. This setting limits
    /// how many view calls can be in-flight simultaneously to avoid overwhelming your RPC node.
    ///
    /// The default is `10`, which works well for most nodes including free tiers (with retries).
    /// Increase for high-capacity paid nodes, decrease if you're still seeing rate limits.
    ///
    /// Example values:
    /// - `5` - Conservative, for free/shared nodes
    /// - `10` - Default, good balance for most use cases
    /// - `20-50` - For dedicated/paid nodes with high rate limits
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_concurrent_view_calls: Option<usize>,
}

#[cfg(test)]
mod tests {
    use serde_yaml;

    use super::*;

    #[test]
    fn test_config_simple() {
        let yaml = r#"
          buffer: 4
          callback_concurrency: 2
        "#;

        let transfer: Config = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(transfer.buffer, Some(4));
        assert_eq!(transfer.callback_concurrency, Some(2));
    }

    #[test]
    fn test_config_optional() {
        let yaml = r#"
          buffer: 4
        "#;

        let transfer: Config = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(transfer.buffer, Some(4));
        assert_eq!(transfer.callback_concurrency, None);
    }
}
