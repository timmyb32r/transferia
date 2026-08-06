use futures_util::future::BoxFuture;
use serde_yaml::Value;
use tokio_util::sync::CancellationToken;

use crate::config::yaml::{validate_parser, ChunkSplitter, ParserConfig, SchemaConfig};
use crate::pipeline::source::Source;
use crate::providers::s3::config::{build_object_store, S3SourceConfig};
use crate::providers::s3::source::S3Source;
use crate::providers::traits::SourceProvider;

pub struct S3SourceProvider {
    cfg: S3SourceConfig,
}

impl S3SourceProvider {
    pub fn from_config(value: Value) -> anyhow::Result<Self> {
        let cfg: S3SourceConfig = serde_yaml::from_value(value)
            .map_err(|e| anyhow::anyhow!("Failed to parse S3 source config: {e}"))?;
        if cfg.bucket.is_empty() {
            anyhow::bail!("s3.bucket must not be empty");
        }
        if cfg.prefix.is_empty() {
            anyhow::bail!("s3.prefix must not be empty");
        }
        if cfg.parser.settings.chunk_splitter == ChunkSplitter::NoSplit {
            anyhow::bail!(
                "s3: chunk_splitter 'no-split' is not supported for S3 \u{2014} use 'new-line'"
            );
        }
        if cfg.parser.table_naming.kind != "from_config" {
            anyhow::bail!(
                "s3: table_naming.type must be 'from_config' (S3 has no topic path)"
            );
        }
        validate_parser(&cfg.parser, &[], &crate::parser::parser_names())?;
        Ok(Self { cfg })
    }
}

impl SourceProvider for S3SourceProvider {
    fn build_source(
        &self,
        partition_id: i64,
        _cancel_token: CancellationToken,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>> {
        let store = match build_object_store(&self.cfg) {
            Ok(s) => s,
            Err(e) => return Box::pin(async { Err(e) }),
        };
        let prefix = self.cfg.prefix.clone();
        let framer = self.cfg.parser.settings.chunk_splitter;
        let chunk_size = self.cfg.chunk_size_bytes;
        let max_retries = self.cfg.max_retries;

        Box::pin(async move {
            let src = S3Source::new(store, &prefix, framer, chunk_size, max_retries, partition_id).await?;
            Ok(Box::new(src) as Box<dyn Source>)
        })
    }

    fn discover_partitions(
        &self,
        _total_workers: u32,
        worker_index: u32,
    ) -> BoxFuture<'_, anyhow::Result<Vec<i64>>> {
        let parts = if worker_index == 0 { vec![0] } else { vec![] };
        Box::pin(async move { Ok(parts) })
    }

    fn resolve_table_name(&self) -> anyhow::Result<String> {
        self.cfg.parser.resolve_table_name(&self.cfg.prefix)
    }

    fn parser_config(&self) -> Option<&ParserConfig> {
        Some(&self.cfg.parser)
    }

    fn schema_config(&self) -> Option<&SchemaConfig> {
        Some(&self.cfg.parser.settings)
    }
}
