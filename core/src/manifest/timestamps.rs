use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Timestamps {
    /// Include block timestamps on every event.
    ///
    /// - If set to `true`, always include block timestamps in the log and make any required RPC
    /// calls to ensure this.
    /// - If set to `false` (default), never make additional RPC calls to include timestamps. But if they are
    /// included by default in the log it will be kept.
    #[serde(default)]
    pub enabled: bool,

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
    pub sample_rate: Option<f32>,
}

#[cfg(test)]
mod tests {
    use serde_yaml;

    use super::*;

    #[test]
    fn test_no_config() {
        let yaml = r#""#;
        let transfer: Timestamps = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(transfer.enabled, false);
        assert_eq!(transfer.sample_rate, None);
    }

    #[test]
    fn test_config_simple() {
        let yaml = r#"
          enabled: true
          sample_rate: 1
        "#;

        let transfer: Timestamps = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(transfer.enabled, true);
        assert_eq!(transfer.sample_rate, Some(1.0));
    }

    #[test]
    fn test_config_optional() {
        let yaml = r#"
          sample_rate: 0.1
        "#;

        let transfer: Timestamps = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(transfer.enabled, false);
        assert_eq!(transfer.sample_rate, Some(0.1));

        let yaml = r#"
          enabled: true
        "#;

        let transfer: Timestamps = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(transfer.enabled, true);
        assert_eq!(transfer.sample_rate, None);

        let yaml = r#"
          enabled: false
        "#;

        let transfer: Timestamps = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(transfer.enabled, false);
        assert_eq!(transfer.sample_rate, None);
    }
}
