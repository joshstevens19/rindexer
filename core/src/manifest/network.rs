use super::core::{deserialize_option_u64_from_string, serialize_option_u64_as_string};
use alloy::primitives::U64;
use serde::de::Visitor;
use serde::{de, Deserialize, Deserializer, Serialize};
use std::fmt;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Network {
    pub name: String,

    pub chain_id: u64,

    pub rpc: String,

    /// Poll the latest block at a defined frequency. It is recommended that this frequency be a
    /// multiple faster than the networks block time to ensure fast indexing.
    ///
    /// If RPC use is a concern, this can be reduced at the cost of slower indexing of new logs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_poll_frequency: Option<BlockPollFrequency>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compute_units_per_second: Option<u64>,

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_option_u64_from_string",
        serialize_with = "serialize_option_u64_as_string"
    )]
    pub max_block_range: Option<U64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub get_logs_settings: Option<GetLogsSettings>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disable_logs_bloom_checks: Option<bool>,
}

#[derive(Debug, Serialize, Clone)]
pub enum AddressFiltering {
    InMemory,
    MaxAddressPerGetLogsRequest(usize),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AddressFilteringConfig {
    pub max_address_per_get_logs_request: usize,
}

impl<'de> Deserialize<'de> for AddressFiltering {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;

        let value = serde_yaml::Value::deserialize(deserializer)?;

        if let Some(s) = value.as_str() {
            if s == "in-memory" {
                return Ok(AddressFiltering::InMemory);
            }
        }

        // Try to deserialize as AddressFilteringConfig
        match AddressFilteringConfig::deserialize(value) {
            Ok(config) => Ok(AddressFiltering::MaxAddressPerGetLogsRequest(
                config.max_address_per_get_logs_request,
            )),
            Err(_) => Err(Error::custom("Invalid AddressFiltering format")),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GetLogsSettings {
    pub address_filtering: AddressFiltering,
}

#[derive(Debug, Serialize, Clone, Copy, PartialEq, Eq)]
pub enum BlockPollFrequency {
    /// Poll the latest block a defined rate for a network. E.g. 500ms.
    PollRateMs { millis: u64 },
    /// Poll the latest block at factor of the networks block time. E.g. 1/5th of a 2s block time.
    Division { divisor: u32 },
    /// Use very fast block polling designed to index blocks as close to realtime as possible.
    /// This will be at the expense of more RPC calls.
    Rapid,
    /// This will use an RPC balanced / optimised poll rate, it is currently defined as 1/3 of the
    /// networks block rate and is shorthand for `/3`, however may change in the future to represent
    /// use cases where we want to balance speed of indexing with rpc use automatically.
    RpcOptimized,
}

impl<'de> Deserialize<'de> for BlockPollFrequency {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct BlockPollFrequencyVisitor;

        impl<'de> Visitor<'de> for BlockPollFrequencyVisitor {
            type Value = BlockPollFrequency;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string representing block poll frequency")
            }

            fn visit_str<E>(self, value: &str) -> Result<BlockPollFrequency, E>
            where
                E: de::Error,
            {
                match value {
                    "rapid" => Ok(BlockPollFrequency::Rapid),
                    "optimized" => Ok(BlockPollFrequency::RpcOptimized),
                    _ if value.starts_with('/') => {
                        let divisor = value[1..].parse::<u32>().map_err(E::custom)?;
                        Ok(BlockPollFrequency::Division { divisor })
                    },
                    _ => {
                        let millis = value.parse::<u64>().map_err(E::custom)?;
                        Ok(BlockPollFrequency::PollRateMs { millis })
                    },
                }
            }
        }

        deserializer.deserialize_str(BlockPollFrequencyVisitor)
    }
}

#[cfg(test)]
mod tests {
    use serde_yaml;

    use super::*;

    #[test]
    fn test_network_defaults() {
        let network: Network = serde_yaml::from_str(
            r#"
            name: ethereum
            chain_id: 1
            rpc: https://mainnet.gateway.tenderly.co
            "#,
        )
        .unwrap();

        assert_eq!(network.name, "ethereum");
        assert_eq!(network.chain_id, 1);
        assert_eq!(network.rpc, "https://mainnet.gateway.tenderly.co");
        assert_eq!(network.max_block_range, None);
        assert_eq!(network.compute_units_per_second, None);
        assert_eq!(network.block_poll_frequency, None);
    }

    #[test]
    fn test_network_block_poll_frequency() {
        let network: Network = serde_yaml::from_str(
            r#"
            name: ethereum
            chain_id: 1
            rpc: https://mainnet.gateway.tenderly.co
            block_poll_frequency: "rapid"
            "#,
        )
        .unwrap();

        assert_eq!(network.block_poll_frequency, Some(BlockPollFrequency::Rapid));

        let network: Network = serde_yaml::from_str(
            r#"
            name: ethereum
            chain_id: 1
            rpc: https://mainnet.gateway.tenderly.co
            block_poll_frequency: "/3"
            "#,
        )
        .unwrap();

        assert_eq!(network.block_poll_frequency, Some(BlockPollFrequency::Division { divisor: 3 }));

        let network: Network = serde_yaml::from_str(
            r#"
            name: ethereum
            chain_id: 1
            rpc: https://mainnet.gateway.tenderly.co
            block_poll_frequency: "1500"
            "#,
        )
        .unwrap();

        assert_eq!(
            network.block_poll_frequency,
            Some(BlockPollFrequency::PollRateMs { millis: 1500 })
        );

        let network: Network = serde_yaml::from_str(
            r#"
            name: ethereum
            chain_id: 1
            rpc: https://mainnet.gateway.tenderly.co
            block_poll_frequency: 100
            "#,
        )
        .unwrap();

        assert_eq!(
            network.block_poll_frequency,
            Some(BlockPollFrequency::PollRateMs { millis: 100 })
        );
    }
}
