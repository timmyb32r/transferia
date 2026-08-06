use alloc::sync::Arc;

use anyhow::anyhow;
use object_store::ObjectStore;
use serde::Deserialize;

use crate::config::yaml::ParserConfig;

pub const DEFAULT_CHUNK_SIZE_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_MAX_RETRIES: u32 = 3;

#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct S3CredentialsConfig {
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Debug, Deserialize)]
#[non_exhaustive]
pub struct S3SourceConfig {
    pub bucket: String,
    pub prefix: String,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub allow_http: bool,
    #[serde(default)]
    pub credentials: Option<S3CredentialsConfig>,
    #[serde(default = "default_chunk_size")]
    pub chunk_size_bytes: usize,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    pub parser: ParserConfig,
}

const fn default_chunk_size() -> usize { DEFAULT_CHUNK_SIZE_BYTES }
const fn default_max_retries() -> u32 { DEFAULT_MAX_RETRIES }

/// Build an `ObjectStore` from S3 config. Without credentials, uses the standard
/// AWS chain (env vars, ~/.aws, IMDS). Supports custom endpoints for `MinIO` etc.
pub fn build_object_store(cfg: &S3SourceConfig) -> anyhow::Result<Arc<dyn ObjectStore>> {
    let mut builder = object_store::aws::AmazonS3Builder::new()
        .with_bucket_name(&cfg.bucket)
        .with_allow_http(cfg.allow_http);
    if let Some(region) = cfg.region.as_deref() {
        builder = builder.with_region(region);
    }
    if let Some(endpoint) = cfg.endpoint.as_deref() {
        builder = builder.with_endpoint(endpoint);
    }
    if let Some(ref creds) = cfg.credentials {
        builder = builder
            .with_access_key_id(&creds.access_key)
            .with_secret_access_key(&creds.secret_key);
    }
    Ok(Arc::new(builder.build().map_err(|e| anyhow!("Failed to build S3 object store: {e}"))?))
}
