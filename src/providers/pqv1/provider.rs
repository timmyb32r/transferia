use alloc::sync::Arc;
use futures_util::future::BoxFuture;
use serde_yaml::Value;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::sync::{Notify, OnceCell, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::compatibility::{EndpointDescriptor, SourceBehavior, SourceDescriptor};
use crate::delivery::{DeliveryDiscovery, DeliveryDiscoveryRequest};
use crate::metrics::{MetricsRegistry, SourceCounters};
use crate::parsers::ParserPlan;
use crate::pipeline::memory::PipelineMemory;
use crate::pipeline::source::Source;
use crate::pipeline::PipelineFailure;
use crate::providers::pqv1::config::PqV1SourceConfig;
use crate::providers::pqv1::credentials::load_access_token;
use crate::providers::pqv1::pq_v1::{parse_endpoint, PqV1Client, PqV1Source};
use crate::providers::traits::SourceProvider;
use crate::Ydb::pers_queue::v1::{AutoPartitioningStrategy, TopicSettings};

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

#[derive(Default)]
struct EndpointCacheState {
    cached: Option<CachedEndpoints>,
    refreshing: bool,
}

#[derive(Default)]
struct EndpointCache {
    state: Mutex<EndpointCacheState>,
    refreshed: Notify,
}

impl EndpointCache {
    fn replace(&self, cached: CachedEndpoints) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.cached = Some(cached);
        state.refreshing = false;
        drop(state);
        self.refreshed.notify_waiters();
    }

    fn finish_refresh(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.refreshing = false;
        drop(state);
        self.refreshed.notify_waiters();
    }
}

struct EndpointRefreshGuard(Arc<EndpointCache>);

impl Drop for EndpointRefreshGuard {
    fn drop(&mut self) {
        self.0.finish_refresh();
    }
}

enum EndpointCacheDecision {
    Return(CachedEndpoints),
    Refresh,
    Wait,
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
    endpoint_cache: Arc<EndpointCache>,
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
        validate_partition_group_ids(&cfg.partition_group_ids)?;
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
            endpoint_cache: Arc::new(EndpointCache::default()),
        })
    }

    fn configured_delivery_discovery(
        &self,
        request: DeliveryDiscoveryRequest,
    ) -> anyhow::Result<DeliveryDiscovery> {
        // PQ delivers opaque message bytes, not an Arrow row schema. DescribeTopic
        // validates the remote topic topology and consumer below; row schemas must
        // therefore remain the projection declared by the configured parser.
        DeliveryDiscovery::parser_projection(
            Arc::from(self.cfg.topic_path.as_str()),
            self.cfg.partition_group_ids.clone(),
            &self.parser_plan,
            request,
        )
    }
}

fn validate_partition_group_ids(partition_group_ids: &[i64]) -> anyhow::Result<()> {
    anyhow::ensure!(
        !partition_group_ids.is_empty(),
        "pqv1.partition_group_ids must not be empty"
    );
    let mut unique = std::collections::HashSet::with_capacity(partition_group_ids.len());
    for &partition_group_id in partition_group_ids {
        anyhow::ensure!(
            partition_group_id >= 0,
            "pqv1.partition_group_ids must be nonnegative, got {partition_group_id}"
        );
        anyhow::ensure!(
            unique.insert(partition_group_id),
            "pqv1.partition_group_ids contains duplicate group {partition_group_id}"
        );
    }
    Ok(())
}

fn validate_topic_metadata(
    settings: &TopicSettings,
    consumer_name: &str,
    partition_group_ids: &[i64],
) -> anyhow::Result<()> {
    validate_partition_group_ids(partition_group_ids)?;
    anyhow::ensure!(
        settings.partitions_count > 0,
        "PQv1 topic reports invalid partitions_count {}",
        settings.partitions_count
    );
    if let Some(auto_partitioning) = settings.auto_partitioning_settings.as_ref() {
        let strategy =
            AutoPartitioningStrategy::try_from(auto_partitioning.strategy).map_err(|_| {
                anyhow::anyhow!(
                    "PQv1 topic reports unknown auto-partitioning strategy {}",
                    auto_partitioning.strategy
                )
            })?;
        anyhow::ensure!(
            matches!(
                strategy,
                AutoPartitioningStrategy::Disabled | AutoPartitioningStrategy::Paused
            ),
            "pqv1.partition_group_ids requires a fixed topic topology, but topic auto-partitioning strategy is {}",
            strategy.as_str_name()
        );
    }

    let partition_count = i64::from(settings.partitions_count);
    for &partition_group_id in partition_group_ids {
        anyhow::ensure!(
            partition_group_id < partition_count,
            "pqv1.partition_group_ids contains group {partition_group_id}, but topic has groups in range 0..{partition_count}"
        );
    }
    anyhow::ensure!(
        settings
            .read_rules
            .iter()
            .any(|rule| rule.consumer_name == consumer_name),
        "pqv1.consumer_name '{consumer_name}' is not configured on the source topic"
    );
    Ok(())
}

async fn shared_access_token(
    token: &OnceCell<Arc<str>>,
    auth: &crate::providers::pqv1::config::PqV1AuthConfig,
) -> anyhow::Result<Arc<str>> {
    token
        .get_or_try_init(|| async { load_access_token(auth).map(Into::<Arc<str>>::into) })
        .await
        .map(Arc::clone)
}

async fn resolve_proxies_cached(
    cache: Arc<EndpointCache>,
    discovery_endpoint: &str,
    token: &str,
    network_timeout: Duration,
    cancellation: &CancellationToken,
    partition_id: i64,
) -> anyhow::Result<Vec<String>> {
    let fallback_main = parse_endpoint(discovery_endpoint)?;
    loop {
        // Register before inspecting the state so a refresh completion cannot
        // be lost between observing `refreshing` and awaiting the notification.
        let refreshed = cache.refreshed.notified();
        let decision = {
            let mut state = cache
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let now = Instant::now();
            match &state.cached {
                Some(cached) if !cached.should_refresh(now) => {
                    EndpointCacheDecision::Return(cached.clone())
                }
                Some(cached) if state.refreshing => EndpointCacheDecision::Return(cached.clone()),
                None if state.refreshing => EndpointCacheDecision::Wait,
                _ => {
                    state.refreshing = true;
                    EndpointCacheDecision::Refresh
                }
            }
        };

        match decision {
            EndpointCacheDecision::Return(cached) => {
                return Ok(PqV1Client::order_proxies(
                    cached.main_host,
                    cached.endpoints,
                    partition_id,
                ));
            }
            EndpointCacheDecision::Wait => {
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => anyhow::bail!("PQv1 endpoint discovery cancelled"),
                    () = refreshed => {}
                }
            }
            EndpointCacheDecision::Refresh => {
                let refresh_guard = EndpointRefreshGuard(Arc::clone(&cache));
                let result = PqV1Client::discover_endpoints(
                    discovery_endpoint,
                    token,
                    network_timeout,
                    cancellation,
                )
                .await;
                match result {
                    Ok((main_host, endpoints)) => cache.replace(CachedEndpoints {
                        fetched_at: Instant::now(),
                        refresh_retry_at: None,
                        main_host,
                        endpoints,
                    }),
                    Err(error) => {
                        if error
                            .downcast_ref::<PipelineFailure>()
                            .is_some_and(|failure| !failure.is_retryable())
                        {
                            return Err(error);
                        }
                        let mut state = cache
                            .state
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        if let Some(cached) = state.cached.as_mut() {
                            cached.defer_refresh(Instant::now());
                            tracing::warn!(
                                "PQv1 proxy discovery refresh failed: {error}. Using stale endpoint cache."
                            );
                        } else {
                            tracing::warn!(
                                "PQv1 proxy discovery failed: {error}. Using main endpoint."
                            );
                            state.cached = Some(CachedEndpoints {
                                fetched_at: Instant::now(),
                                refresh_retry_at: None,
                                main_host: fallback_main.clone(),
                                endpoints: Vec::new(),
                            });
                        }
                    }
                }
                drop(refresh_guard);
            }
        }
    }
}

impl SourceProvider for PqV1SourceProvider {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::PqV1(SourceDescriptor {
            behavior: self.behavior,
        })
    }

    fn delivery_discovery(
        &self,
        request: DeliveryDiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, anyhow::Result<DeliveryDiscovery>> {
        let cfg = self.cfg.clone();
        let token = Arc::clone(&self.token);
        let endpoint_cache = Arc::clone(&self.endpoint_cache);
        let configured = self.configured_delivery_discovery(request);

        Box::pin(async move {
            let discovery = configured?;
            validate_partition_group_ids(&cfg.partition_group_ids)?;
            let token = shared_access_token(token.as_ref(), &cfg.auth).await?;
            let network_timeout = core::time::Duration::from_millis(cfg.network_timeout_ms);
            let endpoints = PqV1Client::discover_endpoints(
                &cfg.discovery_endpoint,
                token.as_ref(),
                network_timeout,
                &cancellation,
            );
            let topic = PqV1Client::describe_topic(
                &cfg.discovery_endpoint,
                &cfg.topic_path,
                token.as_ref(),
                network_timeout,
                &cancellation,
            );
            let ((main_host, endpoints), topic_settings) = tokio::try_join!(endpoints, topic)
                .map_err(|error| error.context("PQv1 delivery discovery failed"))?;
            validate_topic_metadata(
                &topic_settings,
                &cfg.consumer_name,
                &cfg.partition_group_ids,
            )?;

            endpoint_cache.replace(CachedEndpoints {
                fetched_at: Instant::now(),
                refresh_retry_at: None,
                main_host,
                endpoints,
            });
            Ok(discovery)
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
                cfg.partition_group_ids.contains(&partition_id),
                "partition group {partition_id} is not declared in pqv1.partition_group_ids"
            );
            metrics_registry.register_source(partition_id, Arc::clone(&source_counters));
            // Token rotation is intentionally not supported yet. Load lazily
            // so config validation needs no runtime secret, then share the
            // value across all partition starts and retries.
            let token = shared_access_token(token.as_ref(), &cfg.auth).await?;
            let network_timeout = core::time::Duration::from_millis(cfg.network_timeout_ms);
            let mut proxies = resolve_proxies_cached(
                Arc::clone(&endpoint_cache),
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
                .partition_group_ids
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

    fn topic_settings(partitions_count: i32, consumers: &[&str]) -> TopicSettings {
        TopicSettings {
            partitions_count,
            read_rules: consumers
                .iter()
                .map(
                    |consumer_name| crate::Ydb::pers_queue::v1::topic_settings::ReadRule {
                        consumer_name: (*consumer_name).to_owned(),
                        ..Default::default()
                    },
                )
                .collect(),
            ..Default::default()
        }
    }

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

    fn config(partition_group_ids: &str, extra: &str) -> String {
        format!(
            "discovery_endpoint: grpc://localhost\ntopic_path: topic\nconsumer_name: consumer\nauth: {{ type: access_token, token: test }}\n{partition_group_ids}{extra}parser:\n  common:\n    table_naming: {{ type: from_config, name: events }}\n  benchmark_discard: {{}}\n"
        )
    }

    fn json_config(extra: &str) -> String {
        format!(
            "discovery_endpoint: grpc://localhost\ntopic_path: topic\nconsumer_name: consumer\nauth: {{ type: access_token, token: test }}\npartition_group_ids: [0]\n{extra}parser:\n  common:\n    table_naming: {{ type: from_config, name: events }}\n  json_parser:\n    chunk_splitter: one-message-one-row\n    columns:\n      - jsonpath: $.id\n        column_name: id\n        arrow_type: Int64\n        nullable: false\n"
        )
    }

    #[test]
    fn validates_static_partition_group_ids() {
        for (ids, expected) in [
            ("partition_group_ids: [-1]\n", "must be nonnegative"),
            ("partition_group_ids: [1, 1]\n", "duplicate group 1"),
            ("partition_group_ids: []\n", "must not be empty"),
        ] {
            let error = provider(&config(ids, "")).err().expect("config must fail");
            assert!(error.to_string().contains(expected), "{error:#}");
        }
    }

    #[test]
    fn validates_discovered_topic_metadata() {
        validate_topic_metadata(&topic_settings(3, &["consumer"]), "consumer", &[0, 2]).unwrap();
    }

    #[test]
    fn discovered_topic_must_contain_the_configured_consumer() {
        let error =
            validate_topic_metadata(&topic_settings(3, &["other"]), "consumer", &[0]).unwrap_err();
        assert!(error.to_string().contains("is not configured"), "{error:#}");
    }

    #[test]
    fn discovered_topic_bounds_configured_partition_group_ids() {
        let error = validate_topic_metadata(&topic_settings(3, &["consumer"]), "consumer", &[3])
            .unwrap_err();
        assert!(
            error.to_string().contains("groups in range 0..3"),
            "{error:#}"
        );
    }

    #[test]
    fn discovered_topic_requires_a_stable_partition_topology() {
        for strategy in [
            AutoPartitioningStrategy::Unspecified,
            AutoPartitioningStrategy::ScaleUp,
            AutoPartitioningStrategy::ScaleUpAndDown,
        ] {
            let mut settings = topic_settings(3, &["consumer"]);
            settings.auto_partitioning_settings =
                Some(crate::Ydb::pers_queue::v1::AutoPartitioningSettings {
                    strategy: strategy as i32,
                    ..Default::default()
                });
            let error = validate_topic_metadata(&settings, "consumer", &[0]).unwrap_err();
            assert!(
                error.to_string().contains(strategy.as_str_name()),
                "{error:#}"
            );
        }

        for strategy in [
            AutoPartitioningStrategy::Disabled,
            AutoPartitioningStrategy::Paused,
        ] {
            let mut settings = topic_settings(3, &["consumer"]);
            settings.auto_partitioning_settings =
                Some(crate::Ydb::pers_queue::v1::AutoPartitioningSettings {
                    strategy: strategy as i32,
                    ..Default::default()
                });
            validate_topic_metadata(&settings, "consumer", &[0]).unwrap();
        }
    }

    #[test]
    fn rejects_unreasonably_short_network_timeout() {
        let error = provider(&config(
            "partition_group_ids: [0]\n",
            "network_timeout_ms: 99\n",
        ))
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
    fn rejects_unknown_discovery_endpoint_field() {
        let invalid = config("partition_group_ids: [0]\n", "").replacen(
            "discovery_endpoint:",
            "discovery_endpoint_typo:",
            1,
        );
        assert!(provider(&invalid).is_err());
    }

    #[tokio::test]
    async fn rejects_builds_for_undeclared_partitions_before_network_io() {
        let source = provider(&config("partition_group_ids: [0]\n", "")).unwrap();
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
            "partition_group_ids: [0]\n",
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
            config("partition_group_ids: [0]\n", ""),
            config(
                "partition_group_ids: [0]\n",
                "benchmark_discard_before_decompression: true\n",
            ),
        ] {
            let source = provider(&cfg).unwrap();
            let endpoint = source.compatibility();
            let EndpointDescriptor::PqV1(descriptor) = &endpoint else {
                panic!("expected PQv1 descriptor")
            };
            assert_eq!(descriptor.behavior, SourceBehavior::BenchmarkDiscard);
            let discovery = source
                .configured_delivery_discovery(DeliveryDiscoveryRequest {
                    keep_system_columns: false,
                })
                .unwrap();
            assert!(crate::compatibility::validate_pipeline(
                &endpoint,
                &EndpointDescriptor::ClickHouse,
                &discovery,
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
    fn configured_discovery_uses_the_parser_projection() -> anyhow::Result<()> {
        let source = provider(&json_config(""))?;
        let discovery = source.configured_delivery_discovery(DeliveryDiscoveryRequest {
            keep_system_columns: false,
        })?;

        assert_eq!(
            discovery.schema_origin,
            crate::delivery::SchemaOrigin::ParserProjection
        );
        assert_eq!(discovery.source_name.as_ref(), "topic");
        assert_eq!(discovery.source_partitions, [0]);
        assert_eq!(discovery.datasets.len(), 2);
        assert_eq!(
            discovery
                .dataset(crate::delivery::DatasetRole::Main)?
                .name
                .as_ref(),
            "events"
        );
        assert_eq!(
            discovery
                .dataset(crate::delivery::DatasetRole::DeadLetterQueue)?
                .name
                .as_ref(),
            "events_dlq"
        );
        Ok(())
    }

    #[test]
    fn benchmark_discovery_has_no_row_datasets() -> anyhow::Result<()> {
        let source = provider(&config("partition_group_ids: [0]\n", ""))?;
        let discovery = source.configured_delivery_discovery(DeliveryDiscoveryRequest {
            keep_system_columns: false,
        })?;
        assert!(discovery.datasets.is_empty());
        Ok(())
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
    fn missing_partition_group_ids_fails_during_provider_construction() {
        let error = provider(&config("", ""))
            .err()
            .expect("missing partition_group_ids must fail");
        assert!(error
            .to_string()
            .contains("missing field `partition_group_ids`"));
    }

    #[tokio::test]
    async fn static_partitions_are_split_without_truncating_ids() {
        let source = provider(&config("partition_group_ids: [0, 1, 4294967297]\n", "")).unwrap();
        assert_eq!(
            source.partitions_for_worker(2, 1).await.unwrap(),
            vec![1, 4_294_967_297]
        );
    }

    #[test]
    fn retries_reuse_partition_source_counters() {
        let source = provider(&config("partition_group_ids: [0, 1]\n", "")).unwrap();
        let first = source.counters_for_partition(0);
        let retry = source.counters_for_partition(0);
        let other = source.counters_for_partition(1);

        assert!(Arc::ptr_eq(&first, &retry));
        assert!(!Arc::ptr_eq(&first, &other));
    }

    #[test]
    fn retries_advance_the_endpoint_failover_cursor_per_partition() {
        let source = provider(&config("partition_group_ids: [0, 1]\n", "")).unwrap();

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
