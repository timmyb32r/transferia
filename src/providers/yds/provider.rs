use alloc::sync::Arc;
use futures_util::future::BoxFuture;
use serde_yaml::Value;
use tokio_util::sync::CancellationToken;

use crate::compatibility::{ColumnDescriptor, EndpointDescriptor, SourceDescriptor, SourceFraming};
use crate::metrics::{MetricsRegistry, SourceCounters};
use crate::parsers::json_parser::{JsonParser, JsonParserConfig};
use crate::parsers::ParserConfig;
use crate::pipeline::memory::PipelineMemory;
use crate::pipeline::source::Source;
use crate::providers::traits::SourceProvider;
use crate::providers::yds::config::YdsSourceConfig;
use crate::providers::yds::credentials::build_credentials_with_token;
use crate::providers::yds::pq_v1::{parse_endpoint, partition_to_group, PqV1Client, PqV1Source};
use crate::types::schema::DatasetSchema;

pub struct YdsSourceProvider {
    cfg: YdsSourceConfig,
    cached_schema: DatasetSchema,
    metrics_registry: Arc<MetricsRegistry>,
    framing: SourceFraming,
}

impl YdsSourceProvider {
    pub fn from_config(
        value: Value,
        metrics_registry: Arc<MetricsRegistry>,
    ) -> anyhow::Result<Self> {
        let cfg: YdsSourceConfig = serde_yaml::from_value(value)
            .map_err(|e| anyhow::anyhow!("Failed to parse YDS source config: {e}"))?;
        if cfg.connection_string.is_empty() {
            anyhow::bail!("pqv1.connection_string must not be empty");
        }
        if cfg.topic_path.is_empty() {
            anyhow::bail!("pqv1.topic_path must not be empty");
        }
        if cfg.consumer_name.is_empty() {
            anyhow::bail!("pqv1.consumer_name must not be empty");
        }
        // "none" parser ⇒ no columns, no JsonParserConfig (which requires
        // `columns`). DDL is skipped by main for no-parser mode.
        let parser_kind = cfg.parser.parser.kind()?;
        let (cached_schema, framing) = if parser_kind == "none" {
            (
                DatasetSchema::default(),
                SourceFraming::MultipleRowsPerMessage,
            )
        } else {
            let parser_cfg: JsonParserConfig =
                serde_yaml::from_value(cfg.parser.parser.raw()?.clone())?;
            drop(JsonParser::new(
                &parser_cfg,
                &cfg.parser.common.system_columns,
                Arc::from("__config_validation__"),
            )?);
            let framing = if parser_cfg.chunk_splitter
                == crate::parsers::json_parser::ChunkSplitter::OneMessageOneRow
            {
                SourceFraming::OneMessageOneRow
            } else {
                SourceFraming::MultipleRowsPerMessage
            };
            (parser_cfg.to_dataset_schema()?, framing)
        };
        Ok(Self {
            cfg,
            cached_schema,
            metrics_registry,
            framing,
        })
    }
}

impl SourceProvider for YdsSourceProvider {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::PqV1(SourceDescriptor {
            framing: self.framing,
            system_columns: self.cfg.parser.common.system_columns.enabled().collect(),
            columns: self
                .cached_schema
                .columns
                .iter()
                .map(|column| ColumnDescriptor {
                    name: column.name.clone(),
                    data_type: column.data_type.clone(),
                    nullable: column.nullable,
                })
                .collect(),
        })
    }
    fn build_source(
        &self,
        partition_id: i64,
        cancel_token: CancellationToken,
        memory: PipelineMemory,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>> {
        let cfg = self.cfg.clone();
        let metrics_registry = Arc::clone(&self.metrics_registry);

        Box::pin(async move {
            let source_counters = Arc::new(SourceCounters::new());
            metrics_registry.register_source(partition_id, Arc::clone(&source_counters));
            let (_, raw_token) = build_credentials_with_token(&cfg.auth)?;
            let token =
                raw_token.ok_or_else(|| anyhow::anyhow!("PQv1 requires access_token auth"))?;
            let (scheme, host, _) = parse_endpoint(&cfg.connection_string)?;
            let endpoint = format!("{scheme}://{host}");
            let pg_id = partition_to_group(partition_id);
            let (client, mut queues) = PqV1Client::connect(
                &endpoint,
                &cfg.topic_path,
                &cfg.consumer_name,
                &token,
                &[pg_id],
                Arc::clone(&source_counters),
                cancel_token,
                cfg.drop_before_decompress,
                memory,
            )
            .await?;
            let rx = queues
                .remove(&partition_id)
                .ok_or_else(|| anyhow::anyhow!("No queue for partition {partition_id}"))?;
            Ok(Box::new(PqV1Source::new(client, rx, partition_id, cfg)) as Box<dyn Source>)
        })
    }

    fn discover_partitions(
        &self,
        total_workers: u32,
        worker_index: u32,
    ) -> BoxFuture<'_, anyhow::Result<Vec<i64>>> {
        let cfg = self.cfg.clone();

        Box::pin(async move {
            let (_, raw_token) = build_credentials_with_token(&cfg.auth)?;
            let token =
                raw_token.ok_or_else(|| anyhow::anyhow!("PQv1 requires access_token auth"))?;
            let parts = if let Some(ref static_ids) = cfg.partition_ids {
                static_ids
                    .iter()
                    .filter(|id| id.unsigned_abs() as u32 % total_workers == worker_index)
                    .copied()
                    .collect()
            } else {
                let (scheme, host, _) = parse_endpoint(&cfg.connection_string)?;
                let endpoint = format!("{scheme}://{host}");
                PqV1Client::discover_partitions(
                    &endpoint,
                    &cfg.topic_path,
                    &cfg.consumer_name,
                    &token,
                )
                .await?
                .into_iter()
                .filter(|id| id.unsigned_abs() as u32 % total_workers == worker_index)
                .collect()
            };
            Ok(parts)
        })
    }

    fn resolve_table_name(&self) -> anyhow::Result<String> {
        self.cfg.parser.resolve_table_name(&self.cfg.topic_path)
    }

    fn parser_config(&self) -> Option<&ParserConfig> {
        Some(&self.cfg.parser)
    }

    fn schema(&self) -> Option<&DatasetSchema> {
        Some(&self.cached_schema)
    }
}
