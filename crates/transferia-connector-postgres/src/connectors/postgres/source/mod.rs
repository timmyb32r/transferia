mod config;
mod connector;
mod metadata;

pub use config::PostgresSourceConfig;
pub use config::UnsupportedTypePolicy;
pub(crate) use config::TableConfig;
pub use connector::PostgresSourceConnector;
pub(crate) use connector::{
    discover_table, incoming_user_schema, old_key_column_name, old_value_column_name,
    DiscoveredTable, POSTGRES_REPLICATION_SYSTEM_COLUMNS, POSTGRES_SOURCE_METADATA_COLUMNS,
};

#[cfg(test)]
mod tests;
