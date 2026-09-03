mod config;
mod connector;
mod reader;

pub use config::{
    MySqlReadProtocol, MySqlSourceConfig, TableConfig, DEFAULT_MYSQL_BATCH_TARGET_BYTES,
    DEFAULT_MYSQL_MAX_ROW_BYTES, MYSQL_SNAPSHOT_BATCH_TARGET_MAX_BYTES,
};
pub use connector::MySqlSourceConnector;
pub(crate) use connector::{
    old_value_column_name, ColumnPlan, DiscoveredTable, MYSQL_REPLICATION_SYSTEM_COLUMNS,
    MYSQL_SOURCE_METADATA_COLUMNS,
};
#[cfg(test)]
pub(crate) use connector::MySqlColumnKind;
pub(crate) use reader::optional_value_column_array;

#[cfg(test)]
mod tests;
