use ethers::prelude::U64;
use serde::{Deserialize, Deserializer, Serialize};

use super::core::{deserialize_option_u64_from_string, serialize_option_u64_as_string};
use crate::manifest::{chat::ChatConfig, stream::StreamsConfig};

#[derive(Debug, Hash, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct NativeTransferDetails {
    pub network: String,

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_option_u64_from_string",
        serialize_with = "serialize_option_u64_as_string"
    )]
    pub start_block: Option<U64>,

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_option_u64_from_string",
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

    /// None has a special meaning of "All" networks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub networks: Option<Vec<NativeTransferDetails>>,

    /// For now `NativeTokenTransfer` must be the defined "Event" name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub streams: Option<StreamsConfig>,

    /// For now `NativeTokenTransfer` must be the defined "Event" name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat: Option<ChatConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generate_csv: Option<bool>,
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

    Ok(match value {
        NativeTransferFullOrSimple::Simple(enabled) => NativeTransfers {
            enabled,
            networks: None,
            streams: None,
            chat: None,
            generate_csv: None,
        },
        NativeTransferFullOrSimple::Full(transfers) => transfers,
    })
}

#[cfg(test)]
mod tests {
    use serde_yaml;

    use super::*;

    #[test]
    fn test_native_transfer_full() {
        // TODO: Right now, "100" will pass but 100 as a numeric will not...
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
    }
}
