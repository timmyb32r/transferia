use serde::Deserialize;

use crate::parsers::ParserConfig;
use crate::providers::pqv1::config::{default_network_timeout_ms, PqV1AuthConfig};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PqV1SourceConfig {
    pub discovery_endpoint: String,
    pub topic_path: String,
    pub consumer_name: String,
    pub auth: PqV1AuthConfig,
    pub parser: ParserConfig,
    pub partition_group_ids: Vec<i64>,
    /// Bounds discovery/connect/open stages and the HTTP/2 keepalive interval/ACK wait for a
    /// live streaming session. Must be at least 100ms. An idle topic remains valid; liveness
    /// uses transport PING frames.
    #[serde(default = "default_network_timeout_ms")]
    pub network_timeout_ms: u64,
    /// Shared blocking-decompression concurrency across partition sessions.
    /// RAW messages bypass this pool because decoding only transfers ownership.
    #[serde(default = "default_decompression_concurrency")]
    pub decompression_concurrency: usize,
    #[serde(default)]
    pub benchmark_discard_before_decompression: bool,
}

const fn default_decompression_concurrency() -> usize {
    4
}
