mod config;
mod connector;
mod identifiers;
mod metadata;
mod parquet;
mod reader;
mod sample;
mod types;

pub(crate) fn type_mapping() -> transferia_registry::type_mapping::TypeMapping {
    use transferia_registry::type_mapping::{TypeMapping, TypeMappingRow};
    TypeMapping {
        context: "Evaluated by ClickHouse discovery with unsupported_types=fail. With explicit to_string, unsupported columns become Utf8. Parameterized types below are concrete examples; nullability and source-type metadata are retained by discovery.".to_owned(),
        rows: ["Bool", "Int8", "Int16", "Int32", "Int64", "Int128", "Int256",
            "UInt8", "UInt16", "UInt32", "UInt64", "UInt128", "UInt256", "Float32", "Float64",
            "String", "FixedString(16)", "UUID", "IPv4", "IPv6", "Date", "Date32",
            "DateTime", "DateTime('UTC')", "DateTime64(3)", "DateTime64(6, 'UTC')", "DateTime64(9)",
            "Decimal(18, 4)", "Decimal(38, 9)", "Decimal(76, 18)", "Nullable(Int32)",
            "LowCardinality(String)", "Array(Int32)", "Tuple(id Int32, name String)",
            "Map(String, Int32)", "Enum8('a' = 1)", "JSON"]
            .into_iter().map(|t| TypeMappingRow::evaluate(t, types::source_column("value", t, UnsupportedTypePolicy::Fail)
                .map(|c| format!("{:?}{}", c.data_type, if c.nullable { " · nullable" } else { "" })))).collect(),
    }
}

pub use config::{
    ClickHouseParquetCompression, ClickHouseSnapshotReader, ClickHouseSourceConfig,
    UnsupportedTypePolicy,
};
pub use connector::ClickHouseSourceConnector;
pub(crate) use sample::sample_table;

#[cfg(test)]
mod tests;
