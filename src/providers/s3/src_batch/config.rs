use std::time::Duration;

use object_store::ObjectStore;
use schemars::JsonSchema;
use serde::Deserialize;

use crate::parsers::ParserConfig;
use crate::providers::s3::sink::S3CredentialsConfig;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct S3SourceConfig {
    pub bucket: String,
    #[serde(default)]
    pub prefix: String,
    #[serde(default = "default_region")]
    pub region: String,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub allow_http: bool,
    #[serde(default)]
    pub credentials: Option<S3CredentialsConfig>,
    #[schemars(with = "crate::parsers::config::JsonParserSchema")]
    pub parser: ParserConfig,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

impl S3SourceConfig {
    pub(super) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.bucket.is_empty(), "s3.bucket must not be empty");
        if !self.prefix.is_empty() {
            let path = object_store::path::Path::parse(&self.prefix)
                .map_err(|error| anyhow::anyhow!("invalid s3.prefix: {error}"))?;
            anyhow::ensure!(
                path.as_ref() == self.prefix,
                "s3.prefix must be a normalized relative path without leading or trailing slashes"
            );
        }
        anyhow::ensure!(self.timeout_ms > 0, "s3.timeout_ms must be positive");
        self.validate_custom_address()?;
        anyhow::ensure!(
            self.parser.parser.kind()? == "json_parser",
            "S3 source currently supports only parser.json_parser"
        );
        Ok(())
    }

    pub(super) fn build_store(&self) -> anyhow::Result<std::sync::Arc<dyn ObjectStore>> {
        let mut builder = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(&self.bucket)
            .with_region(&self.region)
            .with_allow_http(self.allow_http);
        if let Some(endpoint) = self.custom_endpoint() {
            builder = builder.with_endpoint(endpoint);
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

    fn validate_custom_address(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.host.is_some() == self.port.is_some(),
            "s3.host and s3.port must be configured together"
        );
        if let (Some(host), Some(port)) = (&self.host, self.port) {
            crate::providers::address::validate_host("s3.host", host)?;
            crate::providers::address::validate_port("s3.port", port)?;
        }
        Ok(())
    }

    fn custom_endpoint(&self) -> Option<String> {
        self.host.as_ref().zip(self.port).map(|(host, port)| {
            crate::providers::address::url(
                if self.allow_http { "http" } else { "https" },
                host,
                port,
            )
        })
    }
}

fn default_region() -> String {
    "us-east-1".into()
}
const fn default_timeout_ms() -> u64 {
    30_000
}
