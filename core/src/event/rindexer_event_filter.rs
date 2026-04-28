use crate::database::DatabaseBackends;
use crate::event::contract_setup::{AddressDetails, FilterDetails};
use crate::event::factory_event_filter_sync::{
    get_known_factory_deployed_addresses, GetKnownFactoryDeployedAddressesParams,
};
use crate::manifest::storage::CsvDetails;
use alloy::rpc::types::Topic;
use alloy::{
    primitives::{Address, B256, U64},
    rpc::types::ValueOrArray,
};
use std::collections::HashSet;
use std::path::PathBuf;

#[derive(thiserror::Error, Debug)]
pub enum BuildRindexerFilterError {
    #[error("Address is valid format")]
    AddressInvalidFormat,
}

#[derive(Clone, Debug)]
pub struct SimpleEventFilter {
    pub address: Option<ValueOrArray<Address>>,
    pub topic_id: B256,
    pub topics: [Topic; 4],
    pub current_block: U64,
    pub next_block: U64,
}

impl SimpleEventFilter {
    fn set_from_block(mut self, block: U64) -> Self {
        self.current_block = block;

        self
    }

    fn set_to_block(mut self, block: U64) -> Self {
        self.next_block = block;

        self
    }

    fn contract_address(&self) -> Option<HashSet<Address>> {
        self.address.as_ref().map(|address| match address {
            ValueOrArray::Value(address) => HashSet::from([*address]),
            ValueOrArray::Array(addresses) => addresses.iter().copied().collect(),
        })
    }
}

#[derive(Clone)]
pub struct FactoryFilter {
    pub project_path: PathBuf,
    pub indexer_name: String,
    pub factory_address: ValueOrArray<Address>,
    pub factory_contract_name: String,
    pub factory_event_name: String,
    pub factory_input_name: ValueOrArray<String>,
    pub network: String,

    pub topic_id: B256,
    pub topics: [Topic; 4],

    pub databases: DatabaseBackends,
    pub csv_details: Option<CsvDetails>,

    pub current_block: U64,
    pub next_block: U64,
}

impl std::fmt::Debug for FactoryFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FactoryFilter")
            .field("project_path", &self.project_path)
            .field("indexer_name", &self.indexer_name)
            .field("factory_address", &self.factory_address)
            .field("factory_contract_name", &self.factory_contract_name)
            .field("factory_event_name", &self.factory_event_name)
            .field("factory_input_names", &self.factory_input_name)
            .field("network", &self.network)
            .field("topic_id", &self.topic_id)
            .field("current_block", &self.current_block)
            .field("next_block", &self.next_block)
            .finish()
    }
}

impl FactoryFilter {
    fn set_from_block(mut self, block: U64) -> Self {
        self.current_block = block;

        self
    }

    fn set_to_block(mut self, block: U64) -> Self {
        self.next_block = block;

        self
    }

    async fn contract_address(&self) -> Option<HashSet<Address>> {
        let input_names = match &self.factory_input_name {
            ValueOrArray::Value(name) => vec![name.clone()],
            ValueOrArray::Array(names) => names.clone(),
        };

        get_known_factory_deployed_addresses(&GetKnownFactoryDeployedAddressesParams {
            project_path: self.project_path.clone(),
            indexer_name: self.indexer_name.clone(),
            contract_name: self.factory_contract_name.clone(),
            event_name: self.factory_event_name.clone(),
            input_names,
            network: self.network.clone(),
            postgres: self.databases.postgres.clone(),
            clickhouse: self.databases.clickhouse.clone(),
            csv_details: self.csv_details.clone(),
        })
        .await
        .inspect_err(|e| tracing::error!("Failed to get known factory deployed addresses: {}", e))
        .expect("Failed to get known factory deployed addresses")
    }
}

#[derive(Debug, Clone)]
pub enum RindexerEventFilter {
    Address(SimpleEventFilter),
    Filter(SimpleEventFilter),
    Factory(Box<FactoryFilter>),
}

impl RindexerEventFilter {
    pub fn new_address_filter(
        topic_id: &B256,
        event_name: &str,
        address_details: &AddressDetails,
        current_block: U64,
        next_block: U64,
    ) -> Result<RindexerEventFilter, BuildRindexerFilterError> {
        let index_filter = address_details.indexed_filters.iter().find_map(|indexed_filters| {
            indexed_filters.iter().find(|&n| n.event_name == event_name)
        });

        Ok(RindexerEventFilter::Filter(SimpleEventFilter {
            address: Some(address_details.address.clone()),
            topic_id: *topic_id,
            topics: index_filter
                .map(|indexed_filter| indexed_filter.clone().into())
                .unwrap_or_default(),
            current_block,
            next_block,
        }))
    }

    pub fn new_filter(
        topic_id: &B256,
        _: &str,
        filter_details: &FilterDetails,
        current_block: U64,
        next_block: U64,
    ) -> Result<RindexerEventFilter, BuildRindexerFilterError> {
        Ok(RindexerEventFilter::Filter(SimpleEventFilter {
            address: None,
            topic_id: *topic_id,
            topics: filter_details
                .clone()
                .indexed_filters
                .map(|indexed_filter| indexed_filter.clone().into())
                .unwrap_or_default(),
            current_block,
            next_block,
        }))
    }

    pub fn event_signature(&self) -> B256 {
        match self {
            RindexerEventFilter::Address(filter) => filter.topic_id,
            RindexerEventFilter::Filter(filter) => filter.topic_id,
            RindexerEventFilter::Factory(filter) => filter.topic_id,
        }
    }

    pub fn topic1(&self) -> Topic {
        match self {
            RindexerEventFilter::Address(filter) => filter.topics[1].clone(),
            RindexerEventFilter::Filter(filter) => filter.topics[1].clone(),
            RindexerEventFilter::Factory(filter) => filter.topics[1].clone(),
        }
    }

    pub fn topic2(&self) -> Topic {
        match self {
            RindexerEventFilter::Address(filter) => filter.topics[2].clone(),
            RindexerEventFilter::Filter(filter) => filter.topics[2].clone(),
            RindexerEventFilter::Factory(filter) => filter.topics[2].clone(),
        }
    }

    pub fn topic3(&self) -> Topic {
        match self {
            RindexerEventFilter::Address(filter) => filter.topics[3].clone(),
            RindexerEventFilter::Filter(filter) => filter.topics[3].clone(),
            RindexerEventFilter::Factory(filter) => filter.topics[3].clone(),
        }
    }

    pub fn to_block(&self) -> U64 {
        match self {
            RindexerEventFilter::Address(filter) => filter.next_block,
            RindexerEventFilter::Filter(filter) => filter.next_block,
            RindexerEventFilter::Factory(filter) => filter.next_block,
        }
    }

    pub fn from_block(&self) -> U64 {
        match self {
            RindexerEventFilter::Address(filter) => filter.current_block,
            RindexerEventFilter::Filter(filter) => filter.current_block,
            RindexerEventFilter::Factory(filter) => filter.current_block,
        }
    }

    pub fn set_from_block<R: Into<U64>>(self, block: R) -> Self {
        let block = block.into();
        match self {
            Self::Address(filter) => Self::Address(filter.set_from_block(block)),
            Self::Filter(filter) => Self::Filter(filter.set_from_block(block)),
            Self::Factory(filter) => Self::Factory(Box::new(filter.set_from_block(block))),
        }
    }
    pub fn set_to_block<R: Into<U64>>(self, block: R) -> Self {
        match self {
            RindexerEventFilter::Address(filter) => {
                RindexerEventFilter::Address(filter.set_to_block(block.into()))
            }
            RindexerEventFilter::Filter(filter) => {
                RindexerEventFilter::Filter(filter.set_to_block(block.into()))
            }
            RindexerEventFilter::Factory(filter) => {
                RindexerEventFilter::Factory(Box::new(filter.set_to_block(block.into())))
            }
        }
    }

    pub async fn contract_addresses(&self) -> Option<HashSet<Address>> {
        match self {
            RindexerEventFilter::Address(filter) => filter.contract_address(),
            RindexerEventFilter::Filter(filter) => filter.contract_address(),
            RindexerEventFilter::Factory(filter) => filter.contract_address().await,
        }
    }

    /// Creates a minimal filter for use in tests.
    #[cfg(test)]
    pub fn empty_for_test() -> Self {
        RindexerEventFilter::Filter(SimpleEventFilter {
            address: None,
            topic_id: B256::ZERO,
            topics: Default::default(),
            current_block: U64::ZERO,
            next_block: U64::ZERO,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::contract_setup::{AddressDetails, FilterDetails};
    use alloy::primitives::{Address, B256, U64};
    use alloy::rpc::types::ValueOrArray;
    use std::str::FromStr;

    fn make_topic_id() -> B256 {
        B256::from([1u8; 32])
    }

    fn make_address() -> Address {
        Address::from_str("0xdEADbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF").unwrap()
    }

    // ---- empty_for_test ----

    #[test]
    fn empty_for_test_returns_filter_variant() {
        let f = RindexerEventFilter::empty_for_test();
        assert!(matches!(f, RindexerEventFilter::Filter(_)));
    }

    #[test]
    fn empty_for_test_has_zero_blocks() {
        let f = RindexerEventFilter::empty_for_test();
        assert_eq!(f.from_block(), U64::ZERO);
        assert_eq!(f.to_block(), U64::ZERO);
    }

    #[test]
    fn empty_for_test_has_zero_topic_id() {
        let f = RindexerEventFilter::empty_for_test();
        assert_eq!(f.event_signature(), B256::ZERO);
    }

    // ---- event_signature ----

    #[test]
    fn event_signature_returns_topic_id_for_address_variant() {
        let topic_id = make_topic_id();
        let f = RindexerEventFilter::Address(SimpleEventFilter {
            address: None,
            topic_id,
            topics: Default::default(),
            current_block: U64::ZERO,
            next_block: U64::ZERO,
        });
        assert_eq!(f.event_signature(), topic_id);
    }

    #[test]
    fn event_signature_returns_topic_id_for_filter_variant() {
        let topic_id = make_topic_id();
        let f = RindexerEventFilter::Filter(SimpleEventFilter {
            address: None,
            topic_id,
            topics: Default::default(),
            current_block: U64::ZERO,
            next_block: U64::ZERO,
        });
        assert_eq!(f.event_signature(), topic_id);
    }

    // ---- from_block / to_block ----

    #[test]
    fn from_block_returns_current_block() {
        let f = RindexerEventFilter::Filter(SimpleEventFilter {
            address: None,
            topic_id: B256::ZERO,
            topics: Default::default(),
            current_block: U64::from(42u64),
            next_block: U64::from(100u64),
        });
        assert_eq!(f.from_block(), U64::from(42u64));
    }

    #[test]
    fn to_block_returns_next_block() {
        let f = RindexerEventFilter::Filter(SimpleEventFilter {
            address: None,
            topic_id: B256::ZERO,
            topics: Default::default(),
            current_block: U64::from(42u64),
            next_block: U64::from(100u64),
        });
        assert_eq!(f.to_block(), U64::from(100u64));
    }

    // ---- set_from_block / set_to_block ----

    #[test]
    fn set_from_block_updates_current_block() {
        let f = RindexerEventFilter::empty_for_test();
        let f = f.set_from_block(U64::from(55u64));
        assert_eq!(f.from_block(), U64::from(55u64));
    }

    #[test]
    fn set_to_block_updates_next_block() {
        let f = RindexerEventFilter::empty_for_test();
        let f = f.set_to_block(U64::from(200u64));
        assert_eq!(f.to_block(), U64::from(200u64));
    }

    #[test]
    fn set_from_block_does_not_affect_to_block() {
        let f = RindexerEventFilter::Filter(SimpleEventFilter {
            address: None,
            topic_id: B256::ZERO,
            topics: Default::default(),
            current_block: U64::ZERO,
            next_block: U64::from(999u64),
        });
        let f = f.set_from_block(U64::from(10u64));
        assert_eq!(f.to_block(), U64::from(999u64));
    }

    #[test]
    fn set_to_block_does_not_affect_from_block() {
        let f = RindexerEventFilter::Filter(SimpleEventFilter {
            address: None,
            topic_id: B256::ZERO,
            topics: Default::default(),
            current_block: U64::from(7u64),
            next_block: U64::ZERO,
        });
        let f = f.set_to_block(U64::from(50u64));
        assert_eq!(f.from_block(), U64::from(7u64));
    }

    #[test]
    fn set_from_block_preserves_address_variant() {
        let f = RindexerEventFilter::Address(SimpleEventFilter {
            address: None,
            topic_id: B256::ZERO,
            topics: Default::default(),
            current_block: U64::ZERO,
            next_block: U64::ZERO,
        });
        let f = f.set_from_block(U64::from(3u64));
        assert!(matches!(f, RindexerEventFilter::Address(_)));
        assert_eq!(f.from_block(), U64::from(3u64));
    }

    // ---- topic1 / topic2 / topic3 ----

    #[test]
    fn topic1_topic2_topic3_are_none_on_empty_filter() {
        let f = RindexerEventFilter::empty_for_test();
        // Default Topic is None (no constraint)
        assert!(f.topic1().is_empty());
        assert!(f.topic2().is_empty());
        assert!(f.topic3().is_empty());
    }

    // ---- contract_addresses (Simple variant) ----

    #[tokio::test]
    async fn contract_addresses_none_when_no_address() {
        let f = RindexerEventFilter::empty_for_test();
        let addrs = f.contract_addresses().await;
        assert!(addrs.is_none());
    }

    #[tokio::test]
    async fn contract_addresses_single_address() {
        let addr = make_address();
        let f = RindexerEventFilter::Filter(SimpleEventFilter {
            address: Some(ValueOrArray::Value(addr)),
            topic_id: B256::ZERO,
            topics: Default::default(),
            current_block: U64::ZERO,
            next_block: U64::ZERO,
        });
        let addrs = f.contract_addresses().await.expect("expected Some");
        assert_eq!(addrs.len(), 1);
        assert!(addrs.contains(&addr));
    }

    #[tokio::test]
    async fn contract_addresses_multiple_addresses() {
        let addr1 = make_address();
        let addr2 = Address::from_str("0xCAfEBAbECAFEbAbEcAFEbabECAfebAbEcAFEBAbE").unwrap();
        let f = RindexerEventFilter::Filter(SimpleEventFilter {
            address: Some(ValueOrArray::Array(vec![addr1, addr2])),
            topic_id: B256::ZERO,
            topics: Default::default(),
            current_block: U64::ZERO,
            next_block: U64::ZERO,
        });
        let addrs = f.contract_addresses().await.expect("expected Some");
        assert_eq!(addrs.len(), 2);
        assert!(addrs.contains(&addr1));
        assert!(addrs.contains(&addr2));
    }

    // ---- new_filter ----

    #[test]
    fn new_filter_creates_filter_variant_without_address() {
        let topic_id = make_topic_id();
        let filter_details = FilterDetails {
            events: ValueOrArray::Value("Transfer".to_string()),
            indexed_filters: None,
        };
        let f = RindexerEventFilter::new_filter(
            &topic_id,
            "Transfer",
            &filter_details,
            U64::from(1u64),
            U64::from(100u64),
        )
        .unwrap();

        assert!(matches!(f, RindexerEventFilter::Filter(_)));
        assert_eq!(f.event_signature(), topic_id);
        assert_eq!(f.from_block(), U64::from(1u64));
        assert_eq!(f.to_block(), U64::from(100u64));
    }

    #[tokio::test]
    async fn new_filter_has_no_contract_addresses() {
        let topic_id = make_topic_id();
        let filter_details = FilterDetails {
            events: ValueOrArray::Value("Transfer".to_string()),
            indexed_filters: None,
        };
        let f = RindexerEventFilter::new_filter(
            &topic_id,
            "Transfer",
            &filter_details,
            U64::ZERO,
            U64::ZERO,
        )
        .unwrap();
        assert!(f.contract_addresses().await.is_none());
    }

    // ---- new_address_filter ----

    #[test]
    fn new_address_filter_creates_filter_variant_with_address() {
        let topic_id = make_topic_id();
        let addr = make_address();
        let address_details =
            AddressDetails { address: ValueOrArray::Value(addr), indexed_filters: None };
        let f = RindexerEventFilter::new_address_filter(
            &topic_id,
            "Transfer",
            &address_details,
            U64::from(10u64),
            U64::from(200u64),
        )
        .unwrap();

        assert!(matches!(f, RindexerEventFilter::Filter(_)));
        assert_eq!(f.event_signature(), topic_id);
        assert_eq!(f.from_block(), U64::from(10u64));
        assert_eq!(f.to_block(), U64::from(200u64));
    }

    #[tokio::test]
    async fn new_address_filter_has_contract_address() {
        let topic_id = make_topic_id();
        let addr = make_address();
        let address_details =
            AddressDetails { address: ValueOrArray::Value(addr), indexed_filters: None };
        let f = RindexerEventFilter::new_address_filter(
            &topic_id,
            "Transfer",
            &address_details,
            U64::ZERO,
            U64::ZERO,
        )
        .unwrap();
        let addrs = f.contract_addresses().await.expect("expected Some");
        assert!(addrs.contains(&addr));
    }
}
