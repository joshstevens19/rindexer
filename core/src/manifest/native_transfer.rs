use ethers::prelude::U64;
use serde::{Deserialize, Deserializer, Serialize};

use super::core::serialize_option_u64_as_string;
use crate::manifest::{chat::ChatConfig, stream::StreamsConfig};

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum StringOrNum {
    String(String),
    Num(u64),
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, Default, Deserialize, Serialize)]
pub enum TraceProcessingMethod {
    #[default]
    #[serde(rename = "trace_block")]
    TraceBlock,
    #[serde(rename = "debug_traceBlockByNumber")]
    DebugTraceBlockByNumber,
}

/// Deserialize a number or string into a U64. This is required for the untagged deserialize of
/// native transfers to succeed.
fn deserialize_option_u64_from_string_or_num<'de, D>(
    deserializer: D,
) -> Result<Option<U64>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<StringOrNum> = Option::deserialize(deserializer)?;

    match s {
        Some(StringOrNum::String(string)) => {
            U64::from_dec_str(&string).map(Some).map_err(serde::de::Error::custom)
        }
        Some(StringOrNum::Num(num)) => Ok(Some(U64::from(num))),
        None => Ok(None),
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct NativeTransferDetails {
    pub network: String,

    #[serde(default)]
    pub method: TraceProcessingMethod,

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_option_u64_from_string_or_num",
        serialize_with = "serialize_option_u64_as_string"
    )]
    pub start_block: Option<U64>,

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_option_u64_from_string_or_num",
        serialize_with = "serialize_option_u64_as_string"
    )]
    pub end_block: Option<U64>,
}

fn default_enabled() -> bool {
    true
}

/// The normalized 'Native Transfers' config.
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct NativeTransfers {
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// None has a special meaning of "All" networks for this case. This is because we want the
    /// functionality to be as simple to opt in to as possible.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub networks: Option<Vec<NativeTransferDetails>>,

    /// Define any stream provider, you can manually override the event name by using the Stream
    /// `alias` option, or by default we ensure the event is included in the list of `events`
    /// on the stream if none are provided to the native_transfers stream config..
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub streams: Option<StreamsConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat: Option<ChatConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generate_csv: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reorg_safe_distance: Option<bool>,
}

/// The config to enable native transfers. This can be either a "simple" opinionated enable-all, or
/// a detailed "full" option configuration. The most common live-index setup will be "simple".
///
/// # Example
///
/// ```yaml
/// # Simple opt-in to all native transfer live indexing
/// native_transfers: true
/// ```
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum NativeTransferFullOrSimple {
    Simple(bool),
    Full(NativeTransfers),
}

pub fn deserialize_native_transfers<'de, D>(deserializer: D) -> Result<NativeTransfers, D::Error>
where
    D: Deserializer<'de>,
{
    let value = NativeTransferFullOrSimple::deserialize(deserializer)?;
    let native = match value {
        NativeTransferFullOrSimple::Simple(enabled) => NativeTransfers {
            enabled,
            networks: None,
            streams: None,
            chat: None,
            generate_csv: None,
            reorg_safe_distance: None,
        },
        NativeTransferFullOrSimple::Full(transfers) => transfers,
    };

    Ok(native)
}

#[cfg(test)]
mod tests {
    use serde_yaml;

    use super::*;

    #[test]
    fn test_native_transfer_full_string() {
        let yaml = r#"
          networks:
            - network: ethereum
              start_block: "100"
              end_block: "200"
        "#;

        let transfer: NativeTransfers = serde_yaml::from_str(yaml).unwrap();
        let networks: Vec<NativeTransferDetails> = transfer.networks.unwrap().into_iter().collect();

        assert!(transfer.enabled);
        assert_eq!(networks[0].network, "ethereum");
        assert_eq!(networks[0].start_block.unwrap().as_u64(), 100);
        assert_eq!(networks[0].end_block.unwrap().as_u64(), 200);
        assert_eq!(networks[0].method, TraceProcessingMethod::TraceBlock);
    }

    #[test]
    fn test_native_transfer_full_u64() {
        let yaml = r#"
          networks:
            - network: base
              start_block: 100
        "#;

        let transfer: NativeTransfers = serde_yaml::from_str(yaml).unwrap();
        let networks: Vec<NativeTransferDetails> = transfer.networks.unwrap().into_iter().collect();

        assert!(transfer.enabled);
        assert_eq!(networks[0].network, "base");
        assert_eq!(networks[0].start_block.unwrap().as_u64(), 100);
        assert_eq!(networks[0].end_block, None);
        assert_eq!(networks[0].method, TraceProcessingMethod::TraceBlock);
    }

    #[test]
    fn test_native_transfer_full_method() {
        let yaml = r#"
          networks:
            - network: base
              start_block: 100
              method: "trace_block"
            - network: ethereum
              method: "debug_traceBlockByNumber"
        "#;

        let transfer: NativeTransfers = serde_yaml::from_str(yaml).unwrap();
        let networks: Vec<NativeTransferDetails> = transfer.networks.unwrap().into_iter().collect();

        assert!(transfer.enabled);
        assert_eq!(networks[0].network, "base");
        assert_eq!(networks[0].method, TraceProcessingMethod::TraceBlock);

        assert!(transfer.enabled);
        assert_eq!(networks[1].network, "ethereum");
        assert_eq!(networks[1].method, TraceProcessingMethod::DebugTraceBlockByNumber);
    }
}
