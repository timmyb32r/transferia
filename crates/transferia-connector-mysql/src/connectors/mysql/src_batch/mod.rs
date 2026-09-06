mod config;
mod connector;
mod reader;

pub(crate) const MYSQL_CANONICAL_SNAPSHOT_SQL_MODE: &str =
    "SET SESSION sql_mode = TRIM(BOTH ',' FROM REPLACE(CONCAT(',', @@SESSION.sql_mode, ','), ',PAD_CHAR_TO_FULL_LENGTH,', ','))";

pub use config::{
    MySqlReadProtocol, MySqlSourceConfig, NewTables, TableConfig, DEFAULT_MYSQL_BATCH_TARGET_BYTES,
    DEFAULT_MYSQL_MAX_ROW_BYTES, MYSQL_SNAPSHOT_BATCH_TARGET_MAX_BYTES,
};
pub use connector::MySqlSourceConnector;
pub(crate) use connector::{
    authoritative_table_identities, build_delivery_discovery, column_generation, column_visibility,
    discover_table, has_column_type_modifier, has_extra_modifier, mysql_column_kind,
    old_value_schema_column, parse_enum_set_values, validate_structured_column_metadata,
    ColumnPlan, DiscoveredTable, MySqlColumnKind, MYSQL_REPLICATION_SYSTEM_COLUMNS,
    MYSQL_SOURCE_METADATA_COLUMNS,
};
pub(crate) use reader::optional_value_column_array;

#[cfg(test)]
mod tests;
