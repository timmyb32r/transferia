mod config;
mod connector;
mod source;

pub use config::{IndexConfig, OpenSearchSourceConfig};
pub use connector::OpenSearchSourceConnector;

pub fn initial_config() -> serde_json::Value {
    serde_json::json!({
        "hosts": [""],
        "port": 9200,
        "trusted_plaintext": false,
        "auth": { "type": "basic", "username": "", "password": "" },
        "indices": [{ "name": "index" }],
        "page_rows": 10_000,
        "read_concurrency": 4,
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
