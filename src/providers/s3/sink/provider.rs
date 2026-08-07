use alloc::sync::Arc;

use futures_util::future::BoxFuture;
use serde_yaml::Value;
use object_store::aws::AmazonS3Builder;

use crate::pipeline::sink::Sink;
use crate::providers::s3::sink::writer::{S3Sink, S3SinkConfig};
use crate::providers::traits::SinkProvider;
use crate::serializer::Serializer;

pub struct S3SinkProvider {
    store: Arc<dyn object_store::ObjectStore>,
    cfg: S3SinkConfig,
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
            cfg,
            serializer,
        })
    }
}

impl SinkProvider for S3SinkProvider {
    fn build_sink(&self) -> BoxFuture<'_, anyhow::Result<Arc<dyn Sink>>> {
        let sink = Arc::new(S3Sink::new(
            self.cfg.clone(),
            Arc::clone(&self.store),
            Arc::clone(&self.serializer),
        ));
        Box::pin(async move { Ok(sink as Arc<dyn Sink>) })
    }


}