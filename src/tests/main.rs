use std::sync::atomic::{AtomicBool, Ordering};

use super::*;

fn configured_discovery(
    source: &dyn SourceProvider,
    keep_system_columns: bool,
) -> anyhow::Result<DeliveryDiscovery> {
    DeliveryDiscovery::parser_projection(
        Arc::from("configured-source"),
        vec![0],
        source.parser_plan(),
        DeliveryDiscoveryRequest {
            keep_system_columns,
        },
    )
}

struct RecordingLimits {
    called: AtomicBool,
}

impl SinkLimits for RecordingLimits {
    fn description(&self) -> transferia::delivery::SinkLimitsDescription {
        transferia::delivery::SinkLimitsDescription {
            sink: "test",
            dataset_name: None,
            column_name: None,
            supported_arrow_types: Vec::new(),
            object_key: None,
        }
    }

    fn validate_discovery(&self, _discovery: &DeliveryDiscovery) -> anyhow::Result<()> {
        self.called.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[test]
fn semantic_errors_short_circuit_sink_limit_validation() {
    let limits = RecordingLimits {
        called: AtomicBool::new(false),
    };
    let discovery = DeliveryDiscovery {
        source_name: Arc::from("topic"),
        source_partitions: vec![0],
        schema_origin: transferia::delivery::SchemaOrigin::ParserProjection,
        keep_system_columns: false,
        datasets: Vec::new(),
    };
    let source = transferia::compatibility::EndpointDescriptor::PqV1(
        transferia::compatibility::SourceDescriptor {
            behavior: transferia::compatibility::SourceBehavior::BenchmarkDiscard,
        },
    );

    assert!(validate_discovered_pipeline(
        &source,
        &transferia::compatibility::EndpointDescriptor::ClickHouse,
        &limits,
        &discovery,
        false,
    )
    .is_err());
    assert!(!limits.called.load(Ordering::SeqCst));
}

#[test]
fn rejects_invalid_worker_assignment_before_partitioning() {
    let mut cli = Cli {
        config: Some("unused".into()),
        server: false,
        bind: "127.0.0.1:8080".parse().unwrap(),
        state_dir: ".transferia-server".into(),
        total_workers: 0,
        worker_index: 0,
    };
    assert!(validate_worker_assignment(&cli).is_err());
    cli.total_workers = 2;
    cli.worker_index = 2;
    assert!(validate_worker_assignment(&cli).is_err());
    cli.worker_index = 1;
    assert!(validate_worker_assignment(&cli).is_ok());
}

#[test]
fn retryable_partition_failures_use_capped_backoff_without_exhaustion() {
    let mut policy = PartitionRestartPolicy::new();

    for expected_failure in 1..=100 {
        let (failure, delay) = policy.record_failure(false);
        assert_eq!(failure, expected_failure);
        assert!(delay <= MAX_PARTITION_RESTART_DELAY);
    }
    assert_eq!(policy.next_delay, MAX_PARTITION_RESTART_DELAY);
}

#[test]
fn durable_progress_resets_failure_streak_and_backoff() {
    let mut policy = PartitionRestartPolicy::new();
    for _ in 0..10 {
        policy.record_failure(false);
    }

    let (failure, delay) = policy.record_failure(true);

    assert_eq!(failure, 1);
    assert_eq!(delay, INITIAL_PARTITION_RESTART_DELAY);
    for expected_failure in 2..5 {
        let (failure, _) = policy.record_failure(false);
        assert_eq!(failure, expected_failure);
    }
}

#[test]
fn finite_source_completion_is_not_restarted() {
    assert!(classify_partition_completion(Ok(()), false, true).is_none());
    assert!(classify_partition_completion(Ok(()), false, false).is_some());
}

#[test]
fn default_registry_builds_pqv1_to_clickhouse_pipeline() -> anyhow::Result<()> {
    let registry = build_provider_registry(&Arc::new(MetricsRegistry::new()));
    let config: Config = serde_yaml::from_str(
        r"
delivery_id: pqv1-clickhouse-test
durable_storage: { type: local_file, path: /tmp/transferia-test-state }
source:
  pqv1:
    discovery_endpoint: grpc://localhost
    topic_path: topic
    consumer_name: consumer
    partition_group_ids: [0]
    auth: { type: access_token, token: test }
    parser:
      common:
        table_naming: { type: from_config, name: events }
      json_parser:
        conversion_error: dlq
        unknown_fields: { action: fail }
        columns:
          - { jsonpath: $.id, column_name: id, json_data_type: integer, arrow_type: Int64, nullable: false }
sink:
  clickhouse:
    endpoint: localhost:9000
    trusted_plaintext: true
    database: default
",
    )?;
    let source = registry.build_source(config.source.kind()?, config.source.raw()?.clone())?;
    let sink = registry.build_sink(config.sink.kind()?, config.sink.raw()?.clone())?;
    assert!(matches!(
        sink.compatibility(),
        transferia::compatibility::EndpointDescriptor::ClickHouse
    ));
    let discovery = configured_discovery(source.as_ref(), false)?;
    validate_discovered_pipeline(
        &source.compatibility(),
        &sink.compatibility(),
        sink.limits(),
        &discovery,
        false,
    )?;
    Ok(())
}

#[test]
fn every_benchmark_config_matches_registered_provider_shapes() -> anyhow::Result<()> {
    let registry = build_provider_registry(&Arc::new(MetricsRegistry::new()));
    for relative_path in [
        "benchmarks/config_bench_pqv1_json_parser_to_discard.yaml",
        "benchmarks/config_bench_pqv1_decompress_to_discard.yaml",
        "benchmarks/config_bench_pqv1_network_to_discard.yaml",
        "benchmarks/config_bench_pqv1_json_parser_to_clickhouse.yaml",
        "benchmarks/config_bench_pqv1_json_parser_to_s3.yaml",
    ] {
        let path = format!("{}/{relative_path}", env!("CARGO_MANIFEST_DIR"));
        let config =
            Config::from_file(&path).with_context(|| format!("failed to load {relative_path}"))?;
        let source = registry
            .build_source(config.source.kind()?, config.source.raw()?.clone())
            .with_context(|| format!("invalid source in {relative_path}"))?;
        let sink = registry
            .build_sink(config.sink.kind()?, config.sink.raw()?.clone())
            .with_context(|| format!("invalid sink in {relative_path}"))?;
        sink.validate_pipeline_memory_limit(config.pipeline_memory_limit_bytes)
            .with_context(|| format!("invalid memory limits in {relative_path}"))?;
        let discovery = configured_discovery(source.as_ref(), config.keep_system_columns_in_sink)?;
        validate_discovered_pipeline(
            &source.compatibility(),
            &sink.compatibility(),
            sink.limits(),
            &discovery,
            config.keep_system_columns_in_sink,
        )
        .with_context(|| format!("incompatible providers in {relative_path}"))?;
    }
    Ok(())
}

#[test]
fn root_example_config_matches_registered_provider_shapes() -> anyhow::Result<()> {
    let raw = std::fs::read_to_string(format!("{}/config.yaml", env!("CARGO_MANIFEST_DIR")))?
        .replace("${HOME}", "/tmp")
        .replace("${S3_ACCESS_KEY}", "test-access-key")
        .replace("${S3_SECRET_KEY}", "test-secret-key");
    let config: Config = serde_yaml::from_str(&raw)?;
    let registry = build_provider_registry(&Arc::new(MetricsRegistry::new()));
    let source = registry.build_source(config.source.kind()?, config.source.raw()?.clone())?;
    let sink = registry.build_sink(config.sink.kind()?, config.sink.raw()?.clone())?;
    sink.validate_pipeline_memory_limit(config.pipeline_memory_limit_bytes)?;
    let discovery = configured_discovery(source.as_ref(), config.keep_system_columns_in_sink)?;
    validate_discovered_pipeline(
        &source.compatibility(),
        &sink.compatibility(),
        sink.limits(),
        &discovery,
        config.keep_system_columns_in_sink,
    )?;
    Ok(())
}

#[test]
fn postgres_pipeline_examples_match_registered_provider_shapes() -> anyhow::Result<()> {
    let registry = build_provider_registry(&Arc::new(MetricsRegistry::new()));
    for relative_path in [
        "examples/postgres-to-clickhouse.yaml",
        "examples/postgres-to-s3.yaml",
    ] {
        let path = format!("{}/{relative_path}", env!("CARGO_MANIFEST_DIR"));
        let config = Config::from_file(&path)?;
        let source = registry.build_source(config.source.kind()?, config.source.raw()?.clone())?;
        let sink = registry.build_sink(config.sink.kind()?, config.sink.raw()?.clone())?;
        assert!(matches!(
            source.compatibility(),
            transferia::compatibility::EndpointDescriptor::Postgres(_)
        ));
        assert!(matches!(
            sink.compatibility(),
            transferia::compatibility::EndpointDescriptor::ClickHouse
                | transferia::compatibility::EndpointDescriptor::S3(_)
        ));
    }
    Ok(())
}

#[test]
fn ytsaurus_examples_match_registered_provider_shapes() -> anyhow::Result<()> {
    let registry = build_provider_registry(&Arc::new(MetricsRegistry::new()));
    for relative_path in [
        "config_ytsaurus_source_to_clickhouse.yaml",
        "config_ytsaurus_sink.yaml",
    ] {
        let config = Config::from_file(&format!("{}/{relative_path}", env!("CARGO_MANIFEST_DIR")))?;
        let source = registry.build_source(config.source.kind()?, config.source.raw()?.clone())?;
        let sink = registry.build_sink(config.sink.kind()?, config.sink.raw()?.clone())?;
        if relative_path.contains("source") {
            assert!(matches!(
                source.compatibility(),
                transferia::compatibility::EndpointDescriptor::YTsaurus(_)
            ));
        } else {
            assert!(matches!(
                sink.compatibility(),
                transferia::compatibility::EndpointDescriptor::YTsaurusSink
            ));
        }
    }
    Ok(())
}
