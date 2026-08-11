use futures_util::future::BoxFuture;
use serde_yaml::Value;
use tokio_util::sync::CancellationToken;

use crate::parsers::json_parser::{ChunkSplitter, JsonParserConfig};
use crate::parsers::ParserConfig;
use crate::pipeline::memory::PipelineMemory;
use crate::pipeline::source::Source;
use crate::providers::s3::config::{build_object_store, S3SourceConfig};
use crate::providers::s3::source::S3Source;
use crate::providers::traits::SourceProvider;
use crate::types::schema::DatasetSchema;

pub struct S3SourceProvider {
    cfg: S3SourceConfig,
    /// Cached DDL schema derived from the parser config.
    cached_schema: DatasetSchema,
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
        let parser_cfg: JsonParserConfig =
            serde_yaml::from_value(cfg.parser.parser.raw()?.clone())?;
        if parser_cfg.chunk_splitter == ChunkSplitter::OneMessageOneRow {
            anyhow::bail!(
                "s3: chunk_splitter 'no-split' is not supported for S3 \u{2014} use 'new-line'"
            );
        }
        if cfg.parser.common.table_naming.kind != "from_config" {
            anyhow::bail!("s3: table_naming.type must be 'from_config' (S3 has no topic path)");
        }
        let cached_schema = parser_cfg.to_dataset_schema()?;
        Ok(Self { cfg, cached_schema })
    }
}

impl SourceProvider for S3SourceProvider {
    fn build_source(
        &self,
        partition_id: i64,
        _cancel_token: CancellationToken,
        _memory: PipelineMemory,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>> {
        let store = match build_object_store(&self.cfg) {
            Ok(s) => s,
            Err(e) => return Box::pin(async { Err(e) }),
        };
        let cfg = self.cfg.clone();

        Box::pin(async move {
            let src = S3Source::new(cfg, store, partition_id).await?;
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

    fn schema(&self) -> Option<&DatasetSchema> {
        Some(&self.cached_schema)
    }
}
