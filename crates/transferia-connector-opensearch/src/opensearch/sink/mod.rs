mod actor;
mod bulk;
mod config;
mod connector;
mod document;
mod mapping;

pub(crate) fn type_mapping() -> transferia_registry::type_mapping::TypeMapping {
    transferia_registry::type_mapping::destination_mapping("Evaluated by the OpenSearch destination mapping resolver. Plain columns without source extensions; document identity, routing and runtime values are validated separately.", mapping::destination_type)
}

pub use config::{OpenSearchSinkConfig, RoutedIdentity};
pub use connector::OpenSearchSinkConnector;

#[must_use]
pub fn initial_config() -> serde_json::Value {
    serde_json::json!({
        "hosts": [""],
        "port": 9200,
        "trusted_plaintext": false,
        "auth": { "type": "basic", "username": "", "password": "" },
        "request_timeout_ms": 30_000,
        "max_response_bytes": 67_108_864,
        "create_indices": true,
        "routed_identity": "fail",
        "bulk_target_rows": 20_000,
        "bulk_target_bytes": 16_777_216,
        "bulk_concurrency": 4,
        "flush_interval_ms": 250,
        "retry_initial_ms": 100,
        "retry_max_ms": 10_000,
        "retry_max_attempts": 10
    })
}

#[cfg(test)]
mod tests;
