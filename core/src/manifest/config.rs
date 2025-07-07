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
    pub concurrency: Option<usize>,
}

#[cfg(test)]
mod tests {
    use serde_yaml;

    use super::*;

    #[test]
    fn test_config_simple() {
        let yaml = r#"
          config:
              buffer: 4
              concurrency: 2
        "#;

        let transfer: Config = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(transfer.buffer, Some(4));
        assert_eq!(transfer.concurrency, Some(2));
    }

    #[test]
    fn test_config_optional() {
        let yaml = r#"
          config:
              buffer: 4
        "#;

        let transfer: Config = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(transfer.buffer, Some(4));
        assert_eq!(transfer.concurrency, None);
    }
}
