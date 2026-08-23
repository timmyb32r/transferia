use std::time::Duration;

use futures_util::TryStreamExt as _;
use object_store::ObjectStore;
use schemars::JsonSchema;
use serde::Deserialize;

use crate::connectors::s3::sink::S3CredentialsConfig;
use crate::parsers::ParserConfig;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct S3SourceConfig {
    pub bucket: String,

    #[serde(default)]
    #[schemars(title = "Path prefix")]
    pub path_prefix: String,

    #[serde(default = "default_region")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub region: String,

    #[serde(default)]
    pub endpoint: Option<String>,

    #[serde(default)]
    pub credentials: Option<S3CredentialsConfig>,

    #[schemars(title = "Data format")]
    pub format: S3InputFormat,

    #[serde(default = "default_timeout_ms")]
    #[schemars(
        title = "Request timeout (ms)",
        extend("x-ui" = { "section": "advanced" })
    )]
    pub timeout_ms: u64,
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum S3InputFormat {
    #[schemars(title = "JSON")]
    Json {
        #[schemars(
            with = "crate::parsers::config::ParserSchema",
            extend("x-ui" = { "widget": "parser" })
        )]
        parser: ParserConfig,
    },

    #[schemars(title = "Parquet")]
    Parquet {
        #[schemars(title = "Table name")]
        table_name: String,

        #[serde(default = "default_parquet_batch_rows")]
        #[schemars(extend("x-ui" = { "widget": "hidden" }))]
        batch_rows: usize,
    },
}

impl S3InputFormat {
    pub(super) fn parser(&self) -> Option<&ParserConfig> {
        match self {
            Self::Json { parser } => Some(parser),
            Self::Parquet { .. } => None,
        }
    }

    pub(super) fn parquet(&self) -> Option<(&str, usize)> {
        match self {
            Self::Parquet {
                table_name,
                batch_rows,
            } => Some((table_name, *batch_rows)),
            Self::Json { .. } => None,
        }
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
        match &self.format {
            S3InputFormat::Json { parser } => anyhow::ensure!(
                parser.parser.kind()? == "json_parser",
                "S3 JSON source supports only parser.json_parser"
            ),
            S3InputFormat::Parquet {
                table_name,
                batch_rows,
            } => {
                anyhow::ensure!(!table_name.is_empty(), "s3.format.parquet.table_name is required");
                anyhow::ensure!(*batch_rows > 0, "s3.format.parquet.batch_rows must be positive");
            }
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
            anyhow::ensure!(parsed.host_str().is_some(), "s3.endpoint must include a host");
        }
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
