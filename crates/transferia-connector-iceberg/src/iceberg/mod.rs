mod catalog;
mod config;
mod sink;
mod source;
mod storage;

pub(crate) use source::type_mapping as source_type_mapping;
pub(crate) fn sink_type_mapping() -> transferia_registry::type_mapping::TypeMapping {
    transferia_registry::type_mapping::destination_mapping("Evaluated by the Iceberg destination schema converter. Non-null columns without source extensions; precision, nested schemas and runtime values are validated by the writer.", |c| {
        let schema = transferia_core::data::schema::DatasetSchema::new(vec![c.clone()]);
        let native = sink::iceberg_schema(&schema)?;
        Ok(native.as_struct().fields()[0].field_type.to_string())
    })
}

pub use config::{IcebergSinkConfig, IcebergSourceConfig};
pub use sink::check_connection as check_sink_connection;
pub use sink::IcebergSinkConnector;
pub use source::check_connection as check_source_connection;
pub use source::IcebergSourceConnector;

#[cfg(test)]
mod tests;
