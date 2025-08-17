use std::str::FromStr;

use alloy::{primitives::U64, transports::http::reqwest::header::HeaderMap};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_yaml::Value;

use crate::event::contract_setup::ContractEventMapping;
use crate::helpers::to_pascal_case;
use crate::manifest::config::Config;
use crate::manifest::contract::{
    ContractDetails, DependencyEventTreeYaml, FactoryDetailsYaml, SimpleEventOrContractEvent,
};
use crate::{
    indexer::Indexer,
    manifest::{
        contract::Contract,
        global::Global,
        graphql::GraphQLSettings,
        native_transfer::{deserialize_native_transfers, NativeTransferDetails, NativeTransfers},
        network::Network,
        phantom::Phantom,
        storage::Storage,
    },
};

fn deserialize_project_type<'de, D>(deserializer: D) -> Result<ProjectType, D::Error>
where
    D: Deserializer<'de>,
{
    let value: Value = Deserialize::deserialize(deserializer)?;
    match value {
        Value::String(s) => match s.as_str() {
            "rust" => Ok(ProjectType::Rust),
            "no-code" => Ok(ProjectType::NoCode),
            _ => Err(serde::de::Error::custom(format!("Unknown project type: {s}"))),
        },
        _ => Err(serde::de::Error::custom("Invalid project type format")),
    }
}

fn serialize_project_type<S>(value: &ProjectType, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let string_value = match value {
        ProjectType::Rust => "rust",
        ProjectType::NoCode => "no-code",
    };
    serializer.serialize_str(string_value)
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(untagged)]
pub enum ProjectType {
    Rust,
    NoCode,
}

fn default_storage() -> Storage {
    Storage::default()
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Manifest {
    pub name: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository: Option<String>,

    #[serde(deserialize_with = "deserialize_project_type")]
    #[serde(serialize_with = "serialize_project_type")]
    pub project_type: ProjectType,

    #[serde(default)]
    pub config: Config,

    pub networks: Vec<Network>,

    #[serde(default = "default_storage")]
    pub storage: Storage,

    #[serde(default)]
    #[serde(deserialize_with = "deserialize_native_transfers")]
    pub native_transfers: NativeTransfers,

    pub contracts: Vec<Contract>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phantom: Option<Phantom>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub global: Option<Global>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graphql: Option<GraphQLSettings>,
}

impl Manifest {
    /// Includes both user-defined contracts and factory filter contracts
    pub fn all_contracts(&self) -> Vec<Contract> {
        self.contracts.clone().into_iter().flat_map(|contract| {
            let factory_filter_details = contract.details.iter()
                .filter_map(|detail| {
                    detail.factory.as_ref().map(|factory| (
                        factory.clone(),
                        detail.network.clone(),
                        detail.start_block,
                        detail.end_block
                    ))
                }).collect::<Vec<_>>();

            let first_factory = factory_filter_details.first().cloned();

            match first_factory {
                Some((first_factory, ..)) => {
                    let has_factory_mismatch = factory_filter_details.iter().any(|(detail, ..)| detail.name != first_factory.name || detail.abi != first_factory.abi || detail.event_name != first_factory.event_name);

                    if has_factory_mismatch {
                        panic!("Contract using factory filter must use same factory across all networks. Please raise issue in github if you need different factories across networks");
                    }

                    // suffix with factory filter details to allow having the same contract name at the `contracts` level in yaml
                    let overridden_factory_contract_name = format!("{}{}{}", first_factory.name, to_pascal_case(&first_factory.event_name), first_factory.input_names().iter().map(|v| to_pascal_case(&v)).collect::<Vec<_>>().join(""));

                    let factory_contract = Contract {
                        name: overridden_factory_contract_name.clone(),
                        details: factory_filter_details.into_iter().map(|(factory, network, start_block, end_block)| ContractDetails {
                            network,
                            start_block,
                            end_block,
                            factory: Some(FactoryDetailsYaml {
                                name: overridden_factory_contract_name.clone(),
                                ..factory
                            }),
                            address: None,
                            filter: None,
                            indexed_filters: None,
                        }).collect::<Vec<_>>(),
                        abi: first_factory.abi.clone().into(),
                        dependency_events: None,
                        include_events: Some(vec![first_factory.event_name.clone()]),
                        index_event_in_order: contract.index_event_in_order.clone(),
                        reorg_safe_distance: contract.reorg_safe_distance,
                        generate_csv: contract.generate_csv,
                        streams: None,
                        chat: None,
                    };

                    let dependency_contract = Contract {
                        dependency_events: Some(DependencyEventTreeYaml {
                            events: vec![SimpleEventOrContractEvent::ContractEvent(ContractEventMapping {
                                contract_name: factory_contract.name.clone(),
                                event_name: first_factory.event_name,
                            })],
                            then: contract.dependency_events.or_else(|| {
                                let events = contract
                                    .include_events
                                    .clone()
                                    .expect("Contract using factory filter must specify `include_events`.");

                                Some(DependencyEventTreeYaml {
                                    events: events
                                        .into_iter()
                                        .map(SimpleEventOrContractEvent::SimpleEvent)
                                        .collect(),
                                    then: None,
                                })
                            }).map(Box::new),
                        }),
                        details: contract.details.into_iter().map(|detail| ContractDetails {
                            factory: Some(FactoryDetailsYaml {
                                name: overridden_factory_contract_name.clone(),
                                ..detail.factory.expect("Factory details must be present")
                            }),
                            ..detail
                        }).collect(),
                        ..contract
                    };

                    vec![factory_contract, dependency_contract]
                }
                None => vec![contract]
            }
        }).collect()
    }

    pub fn to_indexer(&self) -> Indexer {
        Indexer {
            name: self.name.clone(),
            contracts: self.all_contracts().clone(),
            native_transfers: self.native_transfers.clone(),
        }
    }

    pub fn has_any_contracts_live_indexing(&self) -> bool {
        self.all_contracts()
            .iter()
            .filter(|c| c.details.iter().any(|p| p.end_block.is_none()))
            .count()
            > 0
    }

    /// Check if the manifest has opted-in to indexing native transfers. It is off by default.
    pub fn has_enabled_native_transfers(&self) -> bool {
        self.native_transfers.enabled
    }

    /// We allow `networks` to be None for native transfers. Which has a special semantic of meaning
    /// opt in to "all supported networks" for live indexing only.
    ///
    /// This means we can map the root `networks` object into the `native_transfers.networks` for
    /// simplicity when constructing the manifest.
    ///
    /// If the user defines a `networks` list this will take priority.
    ///
    /// # Example
    ///
    /// ```yaml
    /// project_type: no-code
    /// native_transfers: true
    /// networks:
    ///   - name: ethereum
    //      chain_id: 1
    //      rpc: https://example.rpc.org
    /// ```
    pub fn set_native_transfer_networks(&mut self) {
        if self.native_transfers.networks.is_none() {
            let root_networks = self
                .networks
                .iter()
                .cloned()
                .map(|n| NativeTransferDetails {
                    network: n.name,
                    start_block: None,
                    end_block: None,
                    method: Default::default(),
                })
                .collect::<Vec<_>>();

            self.native_transfers.networks = Some(root_networks);
        }
    }

    pub fn contract_csv_enabled(&self, contract_name: &str) -> bool {
        let contract_csv_enabled = self
            .all_contracts()
            .iter()
            .find(|c| c.name == contract_name)
            .is_some_and(|c| c.generate_csv.unwrap_or(true));

        self.storage.csv_enabled() && contract_csv_enabled
    }

    pub fn get_custom_headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if let Some(phantom) = &self.phantom {
            if let Some(shadow) = &phantom.shadow {
                headers.insert("X-SHADOW-API-KEY", shadow.api_key.parse().unwrap());
            }
        }
        headers
    }
}

pub fn deserialize_option_u64_from_string<'de, D>(deserializer: D) -> Result<Option<U64>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    match s {
        Some(string) => U64::from_str(&string).map(Some).map_err(serde::de::Error::custom),
        None => Ok(None),
    }
}

pub fn serialize_option_u64_as_string<S>(
    value: &Option<U64>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match *value {
        Some(ref u64_value) => serializer.serialize_some(&u64_value.as_limbs()[0].to_string()),
        None => serializer.serialize_none(),
    }
}

#[cfg(test)]
mod tests {
    use serde_yaml;

    use super::*;

    #[test]
    fn test_native_transfers_complex() {
        let yaml = r#"
        name: test
        project_type: no-code
        networks: []
        contracts: []
        storage:
          csv:
            enabled: true
          postgres:
            enabled: true
        native_transfers:
          networks:
            - network: ethereum
              start_block: 100
              end_block: 200
            - network: base
          streams:
            sns:
              aws_config:
                region: us-east-1
                access_key: test
                secret_key: test
                endpoint_url: http://localhost:8000
              topics:
                - topic_arn: arn:aws:sns:us-east-1:000000000000:native-transfers-test
                  networks:
                    - ethereum
                  events:
                    - event_name: NativeTransfer
        "#;

        let manifest: Manifest = serde_yaml::from_str(yaml).unwrap();

        assert!(manifest.native_transfers.enabled);
        assert_eq!(
            manifest.native_transfers.streams.unwrap().sns.unwrap().topics[0].events[0].event_name,
            "NativeTransfer"
        );

        let networks = manifest.native_transfers.networks.unwrap();

        assert_eq!(networks[0].network, "ethereum");
        assert_eq!(networks[0].start_block.unwrap().as_limbs()[0], 100);
        assert_eq!(networks[0].end_block.unwrap().as_limbs()[0], 200);

        assert_eq!(networks[1].network, "base");
        assert_eq!(networks[1].start_block, None);
        assert_eq!(networks[1].end_block, None);
    }

    #[test]
    fn test_native_transfers_simple() {
        let yaml = r#"
        name: test
        project_type: no-code
        networks: []
        contracts: []
        native_transfers: true
        "#;

        let manifest: Manifest = serde_yaml::from_str(yaml).unwrap();
        assert!(manifest.native_transfers.enabled);

        let yaml = r#"
        name: test
        project_type: no-code
        networks: []
        contracts: []
        native_transfers: false
        "#;

        let manifest: Manifest = serde_yaml::from_str(yaml).unwrap();
        assert!(!manifest.native_transfers.enabled);

        let yaml = r#"
        name: test
        project_type: no-code
        networks: []
        contracts: []
        "#;

        let manifest: Manifest = serde_yaml::from_str(yaml).unwrap();
        assert!(!manifest.native_transfers.enabled);
    }

    #[test]
    fn test_config_simple() {
        let yaml = r#"
        name: test
        project_type: no-code
        networks: []
        contracts: []
        "#;

        let manifest: Manifest = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(manifest.config.callback_concurrency, None);
        assert_eq!(manifest.config.buffer, None);
    }
}
