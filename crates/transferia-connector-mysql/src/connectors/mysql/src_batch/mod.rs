mod config;
mod connector;
mod metadata;
mod reader;
mod sample;

pub(crate) fn type_mapping() -> transferia_registry::type_mapping::TypeMapping {
    use transferia_registry::type_mapping::{TypeMapping, TypeMappingRow};
    let mut rows = Vec::new();
    for name in ["tinyint", "smallint", "mediumint", "int", "bigint", "float", "double",
        "bit", "binary", "varbinary", "blob", "geometry", "vector", "json", "char", "varchar",
        "text", "enum", "set", "decimal", "date", "datetime", "timestamp", "time", "year"] {
        for unsigned in [false, true] {
            if unsigned && !matches!(name, "tinyint" | "smallint" | "mediumint" | "int" | "bigint") { continue; }
            rows.push(TypeMappingRow::evaluate(format!("{name}{}", if unsigned { " unsigned" } else { "" }),
                connector::mysql_column_kind(name, unsigned, Some("utf8mb4"))
                    .map(|kind| format!("{:?} · {}", kind.arrow_type(), kind.arrow_extension_name()))));
        }
    }
    rows.push(TypeMappingRow::evaluate("varchar CHARACTER SET latin1", connector::mysql_column_kind("varchar", false, Some("latin1"))
        .map(|kind| format!("{:?} · {}", kind.arrow_type(), kind.arrow_extension_name()))));
    TypeMapping { context: "Evaluated by MySQL discovery. Text examples use utf8mb4 unless specified; non-UTF character sets retain bytes. Physical type extensions preserve source metadata. Decimal and temporal values use canonical text.".to_owned(), rows }
}

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
pub(crate) use sample::sample_table;

#[cfg(test)]
mod tests;
