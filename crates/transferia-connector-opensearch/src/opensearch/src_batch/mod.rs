mod config;
mod connector;
mod source;

pub(crate) fn type_mapping() -> transferia_registry::type_mapping::TypeMapping {
    use transferia_registry::type_mapping::{TypeMapping, TypeMappingRow};
    TypeMapping { context: "OpenSearch preserves a document envelope, not individual index-property types. _source contains the complete JSON document. These rows come from the production document schema.".to_owned(),
        rows: connector::document_schema().columns.into_iter().map(|c| TypeMappingRow::evaluate(c.name,
            Ok(format!("{:?}{}{}", c.data_type, if c.nullable { " · nullable" } else { "" }, c.arrow_extension_name.map_or(String::new(), |e| format!(" · {e}")))))).collect() }
}

pub use config::{IndexConfig, OpenSearchSourceConfig};
pub use connector::OpenSearchSourceConnector;

#[must_use]
pub fn initial_config() -> serde_json::Value {
    serde_json::json!({
        "hosts": [""],
        "port": 9200,
        "trusted_plaintext": false,
        "auth": { "type": "basic", "username": "", "password": "" },
        "indices": [{ "name": "index" }],
        "page_rows": 10_000,
        "read_concurrency": 2,
        "pit_keep_alive_ms": 300_000,
        "retry_initial_ms": 100,
        "retry_max_ms": 10_000,
        "retry_max_attempts": 10,
        "request_timeout_ms": 30_000,
        "max_response_bytes": 67_108_864
    })
}

#[cfg(test)]
mod tests;
