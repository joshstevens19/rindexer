pub mod callback_registry;

pub mod config;
pub mod contract_setup;

mod rindexer_event_filter;
pub use rindexer_event_filter::{BuildRindexerFilterError, RindexerEventFilter};

mod message;
pub use message::EventMessage;

mod factory_event_filter_sync;
pub use factory_event_filter_sync::{
    get_factory_addresses_with_birth_blocks, get_known_factory_deployed_addresses,
    GetFactoryAddressesWithBirthBlocksParams, GetKnownFactoryDeployedAddressesParams,
};
mod filter;

pub use filter::ast::VariableSource;
pub use filter::evaluation::{evaluate_arithmetic, evaluate_with_table_data, ComputedValue};
pub use filter::parsing::{parse as parse_filter_expression, parse_arithmetic_expression};
pub use filter::{filter_by_expression, filter_event_data_by_conditions};
