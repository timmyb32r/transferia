#![allow(
    clippy::expect_used,
    reason = "the generated JsonParser schema has a compile-time-owned shape and must remain serializable"
)]

use std::borrow::Cow;
use std::time::Duration;

use futures_util::TryStreamExt as _;
use object_store::ObjectStore;
use schemars::generate::SchemaGenerator;
use schemars::{JsonSchema, Schema};
use serde::Deserialize;

use crate::connectors::s3::sink::S3CredentialsConfig;
use crate::parsers::json_parser::JsonParserConfig;
use crate::parsers::SystemColumnsConfig;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct S3SourceConfig {
    pub bucket: String,

    #[serde(default)]
    #[schemars(title = "Path prefix")]
    pub path_prefix: String,

    #[schemars(title = "Table name")]
    pub table_name: String,

    #[serde(default = "default_region")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub region: String,

    #[serde(default)]
    pub endpoint: Option<String>,

    #[serde(default)]
    pub credentials: Option<S3CredentialsConfig>,

    #[schemars(title = "Parser", extend("x-ui" = { "widget": "parser" }))]
    pub parser: S3InputParser,

    #[serde(default = "default_timeout_ms")]
    #[schemars(
        title = "Request timeout (ms)",
        extend("x-ui" = { "widget": "hidden" })
    )]
    pub timeout_ms: u64,
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum S3InputParser {
    #[schemars(
        title = "S3 Parquet parser",
        extend("x-ui" = { "capabilities": { "component": "parser", "key": "s3_parquet", "record_semantics": ["append_only"] } })
    )]
    Parquet {
        #[serde(default = "default_parquet_batch_rows")]
        #[schemars(extend("x-ui" = { "widget": "hidden" }))]
        batch_rows: usize,
    },

    #[schemars(
        title = "S3 JSON parser",
        extend("x-ui" = { "widget": "json_parser", "capabilities": { "component": "parser", "key": "s3_json", "record_semantics": ["append_only"] } })
    )]
    Json {
        #[schemars(
            title = "Parser settings",
            extend("x-ui" = { "widget": "parser_common" })
        )]
        common: S3JsonCommonConfig,

        #[schemars(with = "S3JsonParserSchema", title = "JSON parser")]
        json_parser: JsonParserConfig,
    },

    #[schemars(title = "Discard messages (for benchmarks)")]
    Discard {
        #[serde(default)]
        #[schemars(extend("x-ui" = { "widget": "hidden" }))]
        common: S3JsonCommonConfig,

        #[serde(default)]
        #[schemars(extend("x-ui" = { "widget": "hidden" }))]
        discard: EmptyDiscardConfig,
    },
}

impl S3InputParser {
    pub(super) const fn parquet_batch_rows(&self) -> Option<usize> {
        match self {
            Self::Parquet { batch_rows } => Some(*batch_rows),
            Self::Json { .. } | Self::Discard { .. } => None,
        }
    }
}

#[derive(Clone, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct S3JsonCommonConfig {
    #[serde(default)]
    #[schemars(extend("x-ui" = { "widget": "system_columns" }))]
    pub system_columns: SystemColumnsConfig,
}

#[derive(Clone, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EmptyDiscardConfig {}

struct S3JsonParserSchema;

impl JsonSchema for S3JsonParserSchema {
    fn schema_name() -> Cow<'static, str> {
        Cow::Borrowed("S3JsonParserConfig")
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        let schema = JsonParserConfig::json_schema(generator);
        let mut value =
            serde_json::to_value(schema).expect("JsonParserConfig schema must serialize to JSON");
        let framing = value
            .pointer_mut("/properties/json_framing/default")
            .expect("JsonParserConfig schema must define json_framing.default");
        *framing = serde_json::Value::String("json_lines".to_owned());
        Schema::try_from(value).expect("modified S3 JSON parser schema must remain valid")
    }
}

impl S3SourceConfig {
    pub async fn check_connection(&self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.bucket.is_empty(), "s3.bucket must not be empty");
        anyhow::ensure!(self.timeout_ms > 0, "s3.timeout_ms must be positive");
        self.validate_endpoint()?;
        let store = self.build_store()?;
        let mut listed = store.list(None);
        tokio::time::timeout(self.timeout(), listed.try_next())
            .await
            .map_err(|_| anyhow::anyhow!("S3 connection check timed out"))??;
        Ok(())
    }

    pub(super) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.bucket.is_empty(), "s3.bucket must not be empty");
        if !self.path_prefix.is_empty() {
            let path = object_store::path::Path::parse(&self.path_prefix)
                .map_err(|error| anyhow::anyhow!("invalid s3.path_prefix: {error}"))?;
            anyhow::ensure!(
                path.as_ref() == self.path_prefix,
                "s3.path_prefix must be a normalized relative path without leading or trailing slashes"
            );
        }
        anyhow::ensure!(self.timeout_ms > 0, "s3.timeout_ms must be positive");
        self.validate_endpoint()?;
        match &self.parser {
            S3InputParser::Json {
                common,
                json_parser,
            } => {
                common.system_columns.validate()?;
                json_parser.to_dataset_schema()?;
                self.validate_table_name()?;
            }
            S3InputParser::Parquet { batch_rows } => {
                self.validate_table_name()?;
                anyhow::ensure!(
                    *batch_rows > 0,
                    "s3.parser.parquet.batch_rows must be positive"
                );
            }
            S3InputParser::Discard { .. } => {}
        }
        Ok(())
    }

    pub(super) fn build_store(&self) -> anyhow::Result<std::sync::Arc<dyn ObjectStore>> {
        let mut builder = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(&self.bucket)
            .with_region(&self.region)
            .with_http_connector(super::super::http::NoRedirectConnector::new(
                self.timeout(),
            )?);
        if let Some(endpoint) = &self.endpoint {
            builder = builder
                .with_allow_http(endpoint.starts_with("http://"))
                .with_endpoint(endpoint);
        }
        if let Some(credentials) = &self.credentials {
            builder = builder
                .with_access_key_id(&credentials.access_key)
                .with_secret_access_key(&credentials.secret_key);
        }
        Ok(std::sync::Arc::new(builder.build()?))
    }
    pub(super) const fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }

    fn validate_endpoint(&self) -> anyhow::Result<()> {
        if let Some(endpoint) = &self.endpoint {
            let parsed = reqwest::Url::parse(endpoint)
                .map_err(|error| anyhow::anyhow!("invalid s3.endpoint: {error}"))?;
            anyhow::ensure!(
                matches!(parsed.scheme(), "http" | "https"),
                "s3.endpoint must use http or https"
            );
            anyhow::ensure!(
                parsed.host_str().is_some(),
                "s3.endpoint must include a host"
            );
        }
        Ok(())
    }

    fn validate_table_name(&self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.table_name.is_empty(), "s3.table_name is required");
        anyhow::ensure!(
            self.table_name == self.table_name.trim(),
            "s3.table_name must not have leading or trailing whitespace"
        );
        Ok(())
    }
}

fn default_region() -> String {
    "us-east-1".into()
}
const fn default_timeout_ms() -> u64 {
    30_000
}
const fn default_parquet_batch_rows() -> usize {
    65_536
}
