use serde::Deserialize;

use crate::parsers::ParserConfig;

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    #[serde(rename = "type")]
    pub auth_type: String,
    pub token: Option<String>,
    pub token_file: Option<String>,
    pub sa_file: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct YdsSourceConfig {
    pub connection_string: String,
    pub topic_path: String,
    pub consumer_name: String,
    #[serde(default)]
    pub auth: AuthConfig,
    pub parser: ParserConfig,
    #[serde(default)]
    pub discovery_endpoint: Option<String>,
    #[serde(default)]
    pub partition_ids: Option<Vec<i64>>,
    #[serde(default)]
    pub drop_before_decompress: bool,
}
