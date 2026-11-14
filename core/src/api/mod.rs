mod generate_operations;
mod generate_schema;
#[cfg(feature = "graphql")]
mod graphql;

pub use generate_schema::generate_graphql_queries;
pub struct GraphqlOverrideSettings {
    pub enabled: bool,
    pub override_port: Option<u16>,
}

#[cfg(feature = "graphql")]
pub use graphql::{start_graphql_server, stop_graphql_server, StartGraphqlServerError};
