mod config;
mod connector;
mod reader;

pub use config::PostgresSourceConfig;
#[cfg(test)]
pub(crate) use config::TableConfig;
pub use connector::PostgresSourceConnector;
pub(crate) use connector::{old_key_column_name, old_value_column_name, DiscoveredTable};

#[cfg(test)]
mod tests;
