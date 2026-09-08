mod actor;
pub(crate) mod client;
pub(crate) mod config;
mod connector;
mod http;
pub(crate) mod identifier;
pub(crate) mod table;
mod transport;

pub(crate) fn type_mapping() -> transferia_registry::type_mapping::TypeMapping {
    transferia_registry::type_mapping::destination_mapping(
        "Evaluated by ClickHouse destination DDL resolver. Non-null columns without LowCardinality or source extensions. Nullable/LowCardinality wrappers and value constraints are validated separately.",
        table::destination_type,
    )
}

#[cfg(test)]
mod tests;

pub use actor::ClickHouseSink;
pub use config::{ClickHouseCompression, ClickHouseInsertFormat, ClickHouseSinkConfig};
pub(crate) use connector::connection_check_error;
pub use connector::ClickHouseConnectionCheck;
pub use connector::ClickHouseSinkConnector;
pub use transport::{InsertError, InsertTransport};
