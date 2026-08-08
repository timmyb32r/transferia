use alloc::sync::Arc;
use futures_util::future::BoxFuture;
use serde::Deserialize;
use serde_yaml::Value;
use tokio_util::sync::CancellationToken;

use crate::config::yaml::{AuthConfig, ParserConfig, SchemaConfig};
use crate::metrics::{MetricsRegistry, SourceCounters};
use crate::parsers::json_parser::JsonParserConfig;
use crate::pipeline::source::Source;
use crate::providers::traits::SourceProvider;
use crate::providers::yds::credentials::{build_credentials, build_credentials_with_token};
use crate::providers::yds::pq_v1::{parse_endpoint, partition_to_group, PqV1Client, PqV1Source};
use crate::providers::yds::ydb_topic::YdbTopicSource;

#[derive(Debug, Clone, Deserialize)]
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
}

pub struct YdsSourceProvider {
    cfg: YdsSourceConfig,
    kind: String, // "topic" or "pqv1"
    /// Cached DDL schema derived from the parser config.
    cached_schema: SchemaConfig,
    metrics_registry: Arc<MetricsRegistry>,
}

impl YdsSourceProvider {
    pub fn from_config(
        value: Value,
        kind: &str,
        metrics_registry: Arc<MetricsRegistry>,
    ) -> anyhow::Result<Self> {
        let cfg: YdsSourceConfig = serde_yaml::from_value(value)
            .map_err(|e| anyhow::anyhow!("Failed to parse YDS source config: {e}"))?;
        if cfg.connection_string.is_empty() {
            anyhow::bail!("{kind}.connection_string must not be empty");
        }
        if cfg.topic_path.is_empty() {
            anyhow::bail!("{kind}.topic_path must not be empty");
        }
        if cfg.consumer_name.is_empty() {
            anyhow::bail!("{kind}.consumer_name must not be empty");
        }
        // "none" parser ⇒ no columns, no JsonParserConfig (which requires
        // `columns`). DDL is skipped by main for no-parser mode.
        let parser_kind = cfg.parser.parser.kind()?;
        let cached_schema = if parser_kind == "none" {
            SchemaConfig::default()
        } else {
            let parser_cfg: JsonParserConfig = serde_yaml::from_value(
                cfg.parser.parser.raw()?.clone(),
            )?;
            parser_cfg.to_schema_config()
        };
        Ok(Self { cfg, kind: kind.to_string(), cached_schema, metrics_registry })
    }
}

impl SourceProvider for YdsSourceProvider {
    fn build_source(
        &self,
        partition_id: i64,
        cancel_token: CancellationToken,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>> {
        let cfg = self.cfg.clone();
        let kind = self.kind.clone();
        let metrics_registry = Arc::clone(&self.metrics_registry);

        Box::pin(async move {
            // Per-partition source counters — registered so the stats reporter
            // can read them. For pqv1 they are filled by the bg task + read_batch;
            // for "topic" they stay zero (the ydb SDK abstracts decompression —
            // duty metrics for the topic kind are out of scope).
            let source_counters = Arc::new(SourceCounters::new());
            metrics_registry.register_source(partition_id, Arc::clone(&source_counters));
            match kind.as_str() {
                "topic" => {
                    let creds = build_credentials(&cfg.auth)?;
                    let src = YdbTopicSource::new(cfg, partition_id, creds).await?;
                    Ok(Box::new(src) as Box<dyn Source>)
                }
                "pqv1" => {
                    let (_, raw_token) = build_credentials_with_token(&cfg.auth)?;
                    let token = raw_token.ok_or_else(|| anyhow::anyhow!("PQv1 requires access_token auth"))?;
                    let (scheme, host, _) = parse_endpoint(&cfg.connection_string)?;
                    let endpoint = format!("{scheme}://{host}");
                    let pg_id = partition_to_group(partition_id);
                    let (client, mut queues) = PqV1Client::connect(&endpoint, &cfg.topic_path, &cfg.consumer_name, &token, &[pg_id], Arc::clone(&source_counters), cancel_token).await?;
                    let rx = queues.remove(&partition_id)
                        .ok_or_else(|| anyhow::anyhow!("No queue for partition {partition_id}"))?;
                    Ok(Box::new(PqV1Source::new(client, rx, partition_id, cfg, source_counters)) as Box<dyn Source>)
                }
                _ => anyhow::bail!("Unknown YDS kind: {kind}"),
            }
        })
    }

    fn discover_partitions(
        &self,
        total_workers: u32,
        worker_index: u32,
    ) -> BoxFuture<'_, anyhow::Result<Vec<i64>>> {
        let cfg = self.cfg.clone();
        let kind = self.kind.clone();

        Box::pin(async move {
            match kind.as_str() {
                "pqv1" => {
                    let (_, raw_token) = build_credentials_with_token(&cfg.auth)?;
                    let token = raw_token.ok_or_else(|| anyhow::anyhow!("PQv1 requires access_token auth"))?;
                    let parts = if let Some(ref static_ids) = cfg.partition_ids {
                        static_ids.iter()
                            .filter(|id| id.unsigned_abs() as u32 % total_workers == worker_index)
                            .copied()
                            .collect()
                    } else {
                        let (scheme, host, _) = parse_endpoint(&cfg.connection_string)?;
                        let endpoint = format!("{scheme}://{host}");
                        PqV1Client::discover_partitions(&endpoint, &cfg.topic_path, &cfg.consumer_name, &token)
                            .await?
                            .into_iter()
                            .filter(|id| id.unsigned_abs() as u32 % total_workers == worker_index)
                            .collect()
                    };
                    Ok(parts)
                }
                "topic" => {
                    let creds = build_credentials(&cfg.auth)?;
                    let mut builder = ydb::ClientBuilder::new_from_connection_string(&cfg.connection_string)?
                        .with_credentials(creds);
                    if let Some(ref ep) = cfg.discovery_endpoint {
                        let discovery = ydb::StaticDiscovery::new_from_str(ep.as_str())
                            .map_err(|e| anyhow::anyhow!("StaticDiscovery: {e}"))?;
                        builder = builder.with_discovery(discovery);
                    }
                    let client = builder.client()?;
                    let mut topic_client = client.topic_client();
                    let parts = crate::partition::discover_my_partitions(
                        &mut topic_client, &cfg.topic_path, total_workers, worker_index,
                    ).await?;
                    Ok(parts)
                }
                _ => anyhow::bail!("Unknown YDS kind: {kind}"),
            }
        })
    }

    fn resolve_table_name(&self) -> anyhow::Result<String> {
        self.cfg.parser.resolve_table_name(&self.cfg.topic_path)
    }

    fn parser_config(&self) -> Option<&ParserConfig> {
        Some(&self.cfg.parser)
    }

    fn schema_config(&self) -> Option<&SchemaConfig> {
        Some(&self.cached_schema)
    }
}