use alloc::sync::Arc;
use futures_util::future::BoxFuture;
use serde_yaml::Value;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex as AsyncMutex, OnceCell, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::compatibility::{
    ColumnDescriptor, EndpointDescriptor, SourceBehavior, SourceDescriptor,
};
use crate::metrics::{MetricsRegistry, SourceCounters};
use crate::parsers::ParserPlan;
use crate::pipeline::memory::PipelineMemory;
use crate::pipeline::source::Source;
use crate::pipeline::PipelineFailure;
use crate::providers::pqv1::config::PqV1SourceConfig;
use crate::providers::pqv1::credentials::load_access_token;
use crate::providers::pqv1::pq_v1::{parse_endpoint, PqV1Client, PqV1Source};
use crate::providers::traits::SourceProvider;

const MIN_NETWORK_TIMEOUT_MS: u64 = 100;
const ENDPOINT_CACHE_TTL: Duration = Duration::from_secs(30);
const ENDPOINT_REFRESH_BACKOFF: Duration = Duration::from_secs(1);

#[derive(Clone)]
struct CachedEndpoints {
    fetched_at: Instant,
    refresh_retry_at: Option<Instant>,
    main_host: String,
    endpoints: Vec<crate::Ydb::discovery::EndpointInfo>,
}

impl CachedEndpoints {
    fn should_refresh(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.fetched_at) >= ENDPOINT_CACHE_TTL
            && self.refresh_retry_at.is_none_or(|retry_at| now >= retry_at)
    }

    fn defer_refresh(&mut self, now: Instant) {
        self.refresh_retry_at = Some(now + ENDPOINT_REFRESH_BACKOFF);
    }
}

fn connection_failure(
    partition_id: i64,
    errors: &[String],
    fatal_error: Option<anyhow::Error>,
) -> anyhow::Error {
    fatal_error.unwrap_or_else(|| {
        anyhow::anyhow!(
            "PQv1 could not connect partition {partition_id} to any endpoint: {}",
            errors.join("; ")
        )
    })
}

pub struct PqV1SourceProvider {
    cfg: PqV1SourceConfig,
    parser_plan: ParserPlan,
    metrics_registry: Arc<MetricsRegistry>,
    behavior: SourceBehavior,
    source_counters: Mutex<HashMap<i64, Arc<SourceCounters>>>,
    endpoint_attempts: Mutex<HashMap<i64, usize>>,
    decompression_slots: Arc<Semaphore>,
    token: Arc<OnceCell<Arc<str>>>,
    endpoint_cache: Arc<AsyncMutex<Option<CachedEndpoints>>>,
}

impl PqV1SourceProvider {
    fn counters_for_partition(&self, partition_id: i64) -> Arc<SourceCounters> {
        let mut counters = self
            .source_counters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(
            counters
                .entry(partition_id)
                .or_insert_with(|| Arc::new(SourceCounters::new())),
        )
    }

    fn next_endpoint_attempt(&self, partition_id: i64) -> usize {
        let mut attempts = self
            .endpoint_attempts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let attempt = attempts.entry(partition_id).or_default();
        let current = *attempt;
        *attempt = attempt.wrapping_add(1);
        drop(attempts);
        current
    }

    pub fn from_config(
        value: Value,
        metrics_registry: Arc<MetricsRegistry>,
    ) -> anyhow::Result<Self> {
        let cfg: PqV1SourceConfig = serde_yaml::from_value(value)
            .map_err(|e| anyhow::anyhow!("Failed to parse PQv1 source config: {e}"))?;
        if cfg.discovery_endpoint.is_empty() {
            anyhow::bail!("pqv1.discovery_endpoint must not be empty");
        }
        parse_endpoint(&cfg.discovery_endpoint)?;
        if cfg.topic_path.is_empty() {
            anyhow::bail!("pqv1.topic_path must not be empty");
        }
        if cfg.consumer_name.is_empty() {
            anyhow::bail!("pqv1.consumer_name must not be empty");
        }
        anyhow::ensure!(
            cfg.network_timeout_ms >= MIN_NETWORK_TIMEOUT_MS,
            "pqv1.network_timeout_ms must be at least {MIN_NETWORK_TIMEOUT_MS}ms"
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
        let parser_plan = ParserPlan::from_config(&cfg.parser, &cfg.topic_path)?;
        let decompression_slots = Arc::new(Semaphore::new(cfg.decompression_concurrency));
        Ok(Self {
            cfg,
            parser_plan,
            metrics_registry,
            behavior,
            source_counters: Mutex::new(HashMap::new()),
            endpoint_attempts: Mutex::new(HashMap::new()),
            decompression_slots,
            token: Arc::new(OnceCell::new()),
            endpoint_cache: Arc::new(AsyncMutex::new(None)),
        })
    }
}

async fn resolve_proxies_cached(
    cache: &AsyncMutex<Option<CachedEndpoints>>,
    discovery_endpoint: &str,
    token: &str,
    network_timeout: Duration,
    cancellation: &CancellationToken,
    partition_id: i64,
) -> anyhow::Result<Vec<String>> {
    let mut cache = tokio::select! {
        biased;
        () = cancellation.cancelled() => anyhow::bail!("PQv1 endpoint discovery cancelled"),
        cache = cache.lock() => cache,
    };
    let fallback_main = parse_endpoint(discovery_endpoint)?;
    let now = Instant::now();
    let refresh = cache
        .as_ref()
        .is_none_or(|cached| cached.should_refresh(now));
    if refresh {
        match PqV1Client::discover_endpoints(
            discovery_endpoint,
            token,
            network_timeout,
            cancellation,
        )
        .await
        {
            Ok((main_host, endpoints)) => {
                *cache = Some(CachedEndpoints {
                    fetched_at: Instant::now(),
                    refresh_retry_at: None,
                    main_host,
                    endpoints,
                });
            }
            Err(error) => {
                if error
                    .downcast_ref::<PipelineFailure>()
                    .is_some_and(|failure| !failure.is_retryable())
                {
                    return Err(error);
                }
                if cache.is_none() {
                    tracing::warn!("PQv1 proxy discovery failed: {error}. Using main endpoint.");
                    *cache = Some(CachedEndpoints {
                        fetched_at: Instant::now(),
                        refresh_retry_at: None,
                        main_host: fallback_main,
                        endpoints: Vec::new(),
                    });
                } else {
                    if let Some(cached) = cache.as_mut() {
                        cached.defer_refresh(Instant::now());
                    }
                    tracing::warn!(
                        "PQv1 proxy discovery refresh failed: {error}. Using stale endpoint cache."
                    );
                }
            }
        }
    }
    let cached = cache
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("PQv1 endpoint cache is unexpectedly empty"))?;
    let proxies = PqV1Client::order_proxies(
        cached.main_host.clone(),
        cached.endpoints.clone(),
        partition_id,
    );
    drop(cache);
    Ok(proxies)
}

impl SourceProvider for PqV1SourceProvider {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::PqV1(SourceDescriptor {
            behavior: self.behavior,
            system_columns: self.cfg.parser.common.system_columns.enabled().collect(),
            columns: self
                .parser_plan
                .dataset_schema()
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
        let source_counters = self.counters_for_partition(partition_id);
        let endpoint_attempt = self.next_endpoint_attempt(partition_id);
        let decompression_slots = Arc::clone(&self.decompression_slots);
        let token = Arc::clone(&self.token);
        let endpoint_cache = Arc::clone(&self.endpoint_cache);

        Box::pin(async move {
            anyhow::ensure!(
                cfg.partition_ids.contains(&partition_id),
                "partition {partition_id} is not declared in pqv1.partition_ids"
            );
            metrics_registry.register_source(partition_id, Arc::clone(&source_counters));
            // Token rotation is intentionally not supported yet. Load lazily
            // so config validation needs no runtime secret, then share the
            // value across all partition starts and retries.
            let token = token
                .get_or_try_init(|| async {
                    load_access_token(&cfg.auth).map(Into::<Arc<str>>::into)
                })
                .await?;
            let network_timeout = core::time::Duration::from_millis(cfg.network_timeout_ms);
            let mut proxies = resolve_proxies_cached(
                endpoint_cache.as_ref(),
                &cfg.discovery_endpoint,
                token.as_ref(),
                network_timeout,
                &cancel_token,
                partition_id,
            )
            .await?;
            anyhow::ensure!(
                !proxies.is_empty(),
                "PQv1 endpoint resolver returned no endpoints"
            );
            let proxy_count = proxies.len();
            proxies.rotate_left(endpoint_attempt % proxy_count);
            let mut connected = None;
            let mut errors = Vec::new();
            let mut fatal_error = None;
            for proxy in proxies {
                match PqV1Client::connect(
                    &proxy,
                    &cfg.topic_path,
                    &cfg.consumer_name,
                    token.as_ref(),
                    partition_id,
                    Arc::clone(&source_counters),
                    cancel_token.clone(),
                    cfg.benchmark_discard_before_decompression,
                    memory.clone(),
                    network_timeout,
                    Arc::clone(&decompression_slots),
                )
                .await
                {
                    Ok(connection) => {
                        connected = Some(connection);
                        break;
                    }
                    Err(error) => {
                        tracing::warn!(
                            partition_id,
                            proxy,
                            "PQv1 proxy connection failed: {error}"
                        );
                        if error
                            .downcast_ref::<PipelineFailure>()
                            .is_some_and(|failure| !failure.is_retryable())
                        {
                            fatal_error.get_or_insert(error);
                        } else {
                            errors.push(format!("{proxy}: {error}"));
                        }
                    }
                }
            }
            if connected.is_none() {
                if let Some(error) = fatal_error {
                    return Err(error);
                }
            }
            let (client, rx) =
                connected.ok_or_else(|| connection_failure(partition_id, &errors, fatal_error))?;
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

    fn parser_plan(&self) -> &ParserPlan {
        &self.parser_plan
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider(config: &str) -> anyhow::Result<PqV1SourceProvider> {
        let value = serde_yaml::from_str(config)?;
        PqV1SourceProvider::from_config(value, Arc::new(MetricsRegistry::new()))
    }

    #[test]
    fn endpoint_refresh_failure_is_backed_off_without_refreshing_stale_data() {
        let now = Instant::now();
        let mut cached = CachedEndpoints {
            fetched_at: now
                .checked_sub(ENDPOINT_CACHE_TTL + Duration::from_secs(1))
                .expect("test clock has enough history"),
            refresh_retry_at: None,
            main_host: "localhost:2135".into(),
            endpoints: Vec::new(),
        };
        assert!(cached.should_refresh(now));
        cached.defer_refresh(now);
        assert!(!cached.should_refresh(now));
        assert!(cached.should_refresh(now + ENDPOINT_REFRESH_BACKOFF));
        assert!(cached.fetched_at < now);
    }

    #[test]
    fn proxy_failure_aggregation_preserves_a_fatal_disposition() {
        let fatal = PipelineFailure::fatal(anyhow::anyhow!("invalid credentials"));
        let error = connection_failure(7, &["proxy: timed out".into()], Some(fatal.into()));
        let failure = error
            .downcast_ref::<PipelineFailure>()
            .expect("fatal disposition must survive endpoint aggregation");
        assert!(!failure.is_retryable());
    }

    fn config(partition_ids: &str, extra: &str) -> String {
        format!(
            "discovery_endpoint: grpc://localhost\ntopic_path: topic\nconsumer_name: consumer\nauth: {{ type: access_token, token: test }}\n{partition_ids}{extra}parser:\n  common:\n    table_naming: {{ type: from_config, name: events }}\n  benchmark_discard: {{}}\n"
        )
    }

    fn json_config(extra: &str) -> String {
        format!(
            "discovery_endpoint: grpc://localhost\ntopic_path: topic\nconsumer_name: consumer\nauth: {{ type: access_token, token: test }}\npartition_ids: [0]\n{extra}parser:\n  common:\n    table_naming: {{ type: from_config, name: events }}\n  json_parser:\n    chunk_splitter: one-message-one-row\n    columns:\n      - jsonpath: $.id\n        column_name: id\n        arrow_type: Int64\n        nullable: false\n"
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
    fn rejects_unreasonably_short_network_timeout() {
        let error = provider(&config("partition_ids: [0]\n", "network_timeout_ms: 99\n"))
            .err()
            .expect("a timeout that makes keepalive self-thrash must fail");

        assert!(
            error
                .to_string()
                .contains("network_timeout_ms must be at least 100ms"),
            "{error:#}"
        );
    }

    #[test]
    fn rejects_removed_connection_string_name() {
        let legacy = config("partition_ids: [0]\n", "").replacen(
            "discovery_endpoint:",
            "connection_string:",
            1,
        );
        assert!(provider(&legacy).is_err());
    }

    #[tokio::test]
    async fn rejects_builds_for_undeclared_partitions_before_network_io() {
        let source = provider(&config("partition_ids: [0]\n", "")).unwrap();
        let error = source
            .build_source(1, CancellationToken::new(), PipelineMemory::new(1 << 20))
            .await
            .err()
            .expect("undeclared partition must fail locally");
        assert!(error.to_string().contains("not declared"), "{error:#}");
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

    #[test]
    fn retries_reuse_partition_source_counters() {
        let source = provider(&config("partition_ids: [0, 1]\n", "")).unwrap();
        let first = source.counters_for_partition(0);
        let retry = source.counters_for_partition(0);
        let other = source.counters_for_partition(1);

        assert!(Arc::ptr_eq(&first, &retry));
        assert!(!Arc::ptr_eq(&first, &other));
    }

    #[test]
    fn retries_advance_the_endpoint_failover_cursor_per_partition() {
        let source = provider(&config("partition_ids: [0, 1]\n", "")).unwrap();

        assert_eq!(source.next_endpoint_attempt(0), 0);
        assert_eq!(source.next_endpoint_attempt(0), 1);
        assert_eq!(source.next_endpoint_attempt(1), 0);
    }

    #[test]
    fn cached_endpoint_order_remains_partition_specific() {
        let main = "main.test:2135".to_string();
        let endpoints = vec![
            crate::Ydb::discovery::EndpointInfo {
                address: "a.test".into(),
                port: 2135,
                load_factor: 0.0,
                ..Default::default()
            },
            crate::Ydb::discovery::EndpointInfo {
                address: "b.test".into(),
                port: 2135,
                load_factor: 0.0,
                ..Default::default()
            },
        ];

        let first = PqV1Client::order_proxies(main.clone(), endpoints.clone(), 7);
        assert_eq!(first, PqV1Client::order_proxies(main, endpoints, 7));
        assert!(first.contains(&"main.test:2135".to_string()));
    }
}
