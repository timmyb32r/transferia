use alloc::sync::Arc;
use futures_util::future::BoxFuture;
use serde_yaml::Value;
use tokio::sync::{OnceCell, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::compatibility::{
    ColumnDescriptor, EndpointDescriptor, SourceBehavior, SourceDescriptor,
};
use crate::metrics::{MetricsRegistry, SourceCounters};
use crate::parsers::json_parser::{JsonParser, JsonParserConfig};
use crate::parsers::ParserConfig;
use crate::pipeline::memory::PipelineMemory;
use crate::pipeline::source::Source;
use crate::providers::pqv1::config::PqV1SourceConfig;
use crate::providers::pqv1::credentials::load_access_token;
use crate::providers::pqv1::pq_v1::{parse_endpoint, PqV1Client, PqV1Source};
use crate::providers::traits::SourceProvider;
use crate::types::schema::DatasetSchema;

pub struct PqV1SourceProvider {
    cfg: PqV1SourceConfig,
    cached_schema: DatasetSchema,
    metrics_registry: Arc<MetricsRegistry>,
    behavior: SourceBehavior,
    proxy: OnceCell<String>,
    decompression_slots: Arc<Semaphore>,
}

impl PqV1SourceProvider {
    pub fn from_config(
        value: Value,
        metrics_registry: Arc<MetricsRegistry>,
    ) -> anyhow::Result<Self> {
        let cfg: PqV1SourceConfig = serde_yaml::from_value(value)
            .map_err(|e| anyhow::anyhow!("Failed to parse PQv1 source config: {e}"))?;
        if cfg.connection_string.is_empty() {
            anyhow::bail!("pqv1.connection_string must not be empty");
        }
        parse_endpoint(&cfg.connection_string)?;
        if cfg.topic_path.is_empty() {
            anyhow::bail!("pqv1.topic_path must not be empty");
        }
        if cfg.consumer_name.is_empty() {
            anyhow::bail!("pqv1.consumer_name must not be empty");
        }
        anyhow::ensure!(
            cfg.network_timeout_ms > 0,
            "pqv1.network_timeout_ms must be positive"
        );
        anyhow::ensure!(
            cfg.decompression_concurrency > 0,
            "pqv1.decompression_concurrency must be positive"
        );
        cfg.auth.validate()?;
        anyhow::ensure!(
            !cfg.partition_ids.is_empty(),
            "pqv1.partition_ids must not be empty"
        );
        let mut unique = std::collections::HashSet::with_capacity(cfg.partition_ids.len());
        for &partition_id in &cfg.partition_ids {
            anyhow::ensure!(
                partition_id >= 0,
                "pqv1.partition_ids must be nonnegative, got {partition_id}"
            );
            anyhow::ensure!(
                unique.insert(partition_id),
                "pqv1.partition_ids contains duplicate partition {partition_id}"
            );
        }
        // Benchmark discard ⇒ no columns and no `JsonParserConfig` (which
        // requires `columns`). DDL is skipped for benchmark-discard mode.
        let parser_kind = cfg.parser.parser.kind()?;
        anyhow::ensure!(
            !cfg.benchmark_discard_before_decompression || parser_kind == "benchmark_discard",
            "pqv1.benchmark_discard_before_decompression requires parser.benchmark_discard"
        );
        let behavior = if parser_kind == "benchmark_discard" {
            SourceBehavior::BenchmarkDiscard
        } else {
            SourceBehavior::ProducesRows
        };
        let cached_schema = if parser_kind == "benchmark_discard" {
            let _: crate::parsers::benchmark_discard::BenchmarkDiscardConfig =
                serde_yaml::from_value(cfg.parser.parser.raw()?.clone())?;
            DatasetSchema::default()
        } else {
            let parser_cfg: JsonParserConfig =
                serde_yaml::from_value(cfg.parser.parser.raw()?.clone())?;
            drop(JsonParser::new(
                &parser_cfg,
                &cfg.parser.common.system_columns,
                Arc::from("__config_validation__"),
            )?);
            parser_cfg.to_dataset_schema()?
        };
        let decompression_slots = Arc::new(Semaphore::new(cfg.decompression_concurrency));
        Ok(Self {
            cfg,
            cached_schema,
            metrics_registry,
            behavior,
            proxy: OnceCell::new(),
            decompression_slots,
        })
    }
}

impl SourceProvider for PqV1SourceProvider {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::PqV1(SourceDescriptor {
            behavior: self.behavior,
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
        let proxy = &self.proxy;
        let decompression_slots = Arc::clone(&self.decompression_slots);

        Box::pin(async move {
            let source_counters = Arc::new(SourceCounters::new());
            metrics_registry.register_source(partition_id, Arc::clone(&source_counters));
            let token = load_access_token(&cfg.auth)?;
            let network_timeout = core::time::Duration::from_millis(cfg.network_timeout_ms);
            let proxy = proxy
                .get_or_try_init(|| {
                    PqV1Client::resolve_proxy(
                        &cfg.connection_string,
                        &token,
                        network_timeout,
                        &cancel_token,
                    )
                })
                .await?
                .clone();
            let (client, mut queues) = PqV1Client::connect(
                &proxy,
                &cfg.topic_path,
                &cfg.consumer_name,
                &token,
                &[partition_id],
                Arc::clone(&source_counters),
                cancel_token,
                cfg.benchmark_discard_before_decompression,
                memory,
                network_timeout,
                decompression_slots,
            )
            .await?;
            let rx = queues
                .remove(&partition_id)
                .ok_or_else(|| anyhow::anyhow!("No queue for partition {partition_id}"))?;
            Ok(Box::new(PqV1Source::new(
                client,
                rx,
                partition_id,
                Arc::from(cfg.topic_path),
            )) as Box<dyn Source>)
        })
    }

    fn partitions_for_worker(
        &self,
        total_workers: u32,
        worker_index: u32,
    ) -> BoxFuture<'_, anyhow::Result<Vec<i64>>> {
        let cfg = self.cfg.clone();

        Box::pin(async move {
            anyhow::ensure!(total_workers > 0, "total_workers must be positive");
            anyhow::ensure!(
                worker_index < total_workers,
                "worker_index {worker_index} must be less than total_workers {total_workers}"
            );
            let total_workers = u64::from(total_workers);
            let worker_index = u64::from(worker_index);
            let parts = cfg
                .partition_ids
                .iter()
                .filter(|&&id| u64::try_from(id).is_ok_and(|id| id % total_workers == worker_index))
                .copied()
                .collect();
            Ok(parts)
        })
    }

    fn resolve_table_name(&self) -> anyhow::Result<String> {
        self.cfg.parser.resolve_table_name(&self.cfg.topic_path)
    }

    fn parser_config(&self) -> &ParserConfig {
        &self.cfg.parser
    }

    fn schema(&self) -> &DatasetSchema {
        &self.cached_schema
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider(config: &str) -> anyhow::Result<PqV1SourceProvider> {
        let value = serde_yaml::from_str(config)?;
        PqV1SourceProvider::from_config(value, Arc::new(MetricsRegistry::new()))
    }

    fn config(partition_ids: &str, extra: &str) -> String {
        format!(
            "connection_string: grpc://localhost\ntopic_path: topic\nconsumer_name: consumer\nauth: {{ type: access_token, token: test }}\n{partition_ids}{extra}parser:\n  common:\n    table_naming: {{ type: from_config, name: events }}\n  benchmark_discard: {{}}\n"
        )
    }

    fn json_config(extra: &str) -> String {
        format!(
            "connection_string: grpc://localhost\ntopic_path: topic\nconsumer_name: consumer\nauth: {{ type: access_token, token: test }}\npartition_ids: [0]\n{extra}parser:\n  common:\n    table_naming: {{ type: from_config, name: events }}\n  json_parser:\n    chunk_splitter: one-message-one-row\n    columns:\n      - jsonpath: $.id\n        column_name: id\n        arrow_type: Int64\n        nullable: false\n"
        )
    }

    #[test]
    fn validates_static_partition_ids() {
        for (ids, expected) in [
            ("partition_ids: [-1]\n", "must be nonnegative"),
            ("partition_ids: [1, 1]\n", "duplicate partition 1"),
            ("partition_ids: []\n", "must not be empty"),
        ] {
            let error = provider(&config(ids, "")).err().expect("config must fail");
            assert!(error.to_string().contains(expected), "{error:#}");
        }
    }

    #[test]
    fn rejects_zero_network_timeout() {
        let error = provider(&config("partition_ids: [0]\n", "network_timeout_ms: 0\n"))
            .err()
            .expect("zero network timeout must fail");

        assert!(
            error
                .to_string()
                .contains("network_timeout_ms must be positive"),
            "{error:#}"
        );
    }

    #[test]
    fn rejects_zero_decompression_concurrency() {
        let error = provider(&config(
            "partition_ids: [0]\n",
            "decompression_concurrency: 0\n",
        ))
        .err()
        .expect("zero decompression concurrency must fail");

        assert!(
            error
                .to_string()
                .contains("decompression_concurrency must be positive"),
            "{error:#}"
        );
    }

    #[test]
    fn reports_benchmark_discard_behavior() {
        for cfg in [
            config("partition_ids: [0]\n", ""),
            config(
                "partition_ids: [0]\n",
                "benchmark_discard_before_decompression: true\n",
            ),
        ] {
            let source = provider(&cfg).unwrap();
            let endpoint = source.compatibility();
            let EndpointDescriptor::PqV1(descriptor) = &endpoint else {
                panic!("expected PQv1 descriptor")
            };
            assert_eq!(descriptor.behavior, SourceBehavior::BenchmarkDiscard);
            assert!(crate::compatibility::validate_pipeline(
                &endpoint,
                &EndpointDescriptor::ClickHouse,
                false,
            )
            .ensure_valid()
            .is_err());
        }

        let source = provider(&json_config("")).unwrap();
        let EndpointDescriptor::PqV1(descriptor) = source.compatibility() else {
            panic!("expected PQv1 descriptor")
        };
        assert_eq!(descriptor.behavior, SourceBehavior::ProducesRows);
    }

    #[test]
    fn payload_discard_requires_the_discard_parser() {
        let error = provider(&json_config(
            "benchmark_discard_before_decompression: true\n",
        ))
        .err()
        .expect("payload discard with a row parser must fail");
        assert!(error
            .to_string()
            .contains("requires parser.benchmark_discard"));
    }

    #[test]
    fn missing_partition_ids_fails_during_provider_construction() {
        let error = provider(&config("", ""))
            .err()
            .expect("missing partition_ids must fail");
        assert!(error.to_string().contains("missing field `partition_ids`"));
    }

    #[tokio::test]
    async fn static_partitions_are_split_without_truncating_ids() {
        let source = provider(&config("partition_ids: [0, 1, 4294967297]\n", "")).unwrap();
        assert_eq!(
            source.partitions_for_worker(2, 1).await.unwrap(),
            vec![1, 4_294_967_297]
        );
    }
}
