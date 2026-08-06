use alloc::sync::Arc;

use futures_util::future::BoxFuture;
use serde::Deserialize;
use serde_yaml::Value;
use object_store::aws::AmazonS3Builder;

use crate::pipeline::sink::Sink;
use crate::providers::s3::sink::writer::S3Sink;
use crate::providers::traits::SinkProvider;
use crate::serializer::Serializer;

#[derive(Debug, Deserialize)]
#[non_exhaustive]
pub struct S3SinkConfig {
    /// S3 bucket name.
    pub bucket: String,
    /// Object key prefix (e.g. "`snapshots/my_table`/").
    #[serde(default)]
    pub prefix: String,
    /// AWS region.
    #[serde(default = "default_region")]
    pub region: String,
    /// S3 endpoint URL (for S3-compatible storage like Yandex Object Storage).
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Access key.
    #[serde(default)]
    pub access_key: Option<String>,
    /// Secret key.
    #[serde(default)]
    pub secret_key: Option<String>,
    /// Serializer type (currently only "json").
    #[serde(default = "default_serializer")]
    pub serializer_type: String,
    /// When `true`, null-valued columns are elided (absent keys) in JSON output.
    /// Default: `false` — nulls are emitted as `"col": null`.
    #[serde(default)]
    pub skip_null_columns: bool,
}

fn default_region() -> String { "us-east-1".into() }
fn default_serializer() -> String { "json".into() }

pub struct S3SinkProvider {
    store: Arc<dyn object_store::ObjectStore>,
    prefix: String,
    serializer: Arc<dyn Serializer>,
}

impl S3SinkProvider {
    pub fn from_config(value: Value) -> anyhow::Result<Self> {
        let cfg: S3SinkConfig = serde_yaml::from_value(value)
            .map_err(|e| anyhow::anyhow!("Failed to parse S3 sink config: {e}"))?;
        if cfg.bucket.is_empty() {
            anyhow::bail!("s3 sink: bucket must not be empty");
        }

        let serializer = crate::serializer::build_json_serializer(cfg.skip_null_columns);

        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&cfg.bucket)
            .with_region(&cfg.region);

        if let Some(ref ep) = cfg.endpoint {
            builder = builder.with_endpoint(ep);
            builder = builder.with_allow_http(true); // S3-compatible storage may use HTTP
        }
        if let Some(ref ak) = cfg.access_key {
            builder = builder.with_access_key_id(ak);
        }
        if let Some(ref sk) = cfg.secret_key {
            builder = builder.with_secret_access_key(sk);
        }

        let store = builder.build()
            .map_err(|e| anyhow::anyhow!("Failed to build S3 client: {e}"))?;

        tracing::info!(
            "S3 sink: bucket={} prefix={} serializer={}",
            cfg.bucket, cfg.prefix, cfg.serializer_type,
        );

        Ok(Self {
            store: Arc::new(store),
            prefix: cfg.prefix,
            serializer,
        })
    }
}

impl SinkProvider for S3SinkProvider {
    fn build_sink(&self) -> BoxFuture<'_, anyhow::Result<Arc<dyn Sink>>> {
        let sink = Arc::new(S3Sink::new(
            Arc::clone(&self.store),
            self.prefix.clone(),
            Arc::clone(&self.serializer),
        ));
        Box::pin(async move { Ok(sink as Arc<dyn Sink>) })
    }


}
