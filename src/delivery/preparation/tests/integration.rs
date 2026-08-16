use std::sync::atomic::{AtomicBool, Ordering};

use super::*;
use crate::delivery::SinkLimits;
use crate::providers::catalog::build_provider_catalog;

fn configured_discovery(
    source: &dyn SourceProvider,
    keep_system_columns: bool,
) -> anyhow::Result<DeliveryDiscovery> {
    DeliveryDiscovery::parser_projection(
        Arc::from("configured-source"),
        crate::delivery::SourceTopology::StaticPartitions(vec![0]),
        source.parser_plan(),
        DeliveryDiscoveryRequest {
            keep_system_columns,
        },
    )
}

async fn build_resolved_endpoints(
    config: &Config,
) -> anyhow::Result<(Box<dyn SourceProvider>, Box<dyn SinkProvider>)> {
    let transferia = Transferia::public()?;
    let catalog = transferia::providers::catalog::build_provider_catalog_with(
        &transferia,
        &Arc::new(MetricsRegistry::new()),
    )?;
    let source_kind = config.source.kind()?;
    let sink_kind = config.sink.kind()?;
    let source_config = transferia
        .registry()
        .resolve(
            source_kind,
            transferia::extension::EndpointRole::Source,
            config.source.raw()?.clone(),
            CancellationToken::new(),
        )
        .await?;
    let sink_config = transferia
        .registry()
        .resolve(
            sink_kind,
            transferia::extension::EndpointRole::Sink,
            config.sink.raw()?.clone(),
            CancellationToken::new(),
        )
        .await?;
    Ok((
        catalog.build_source(source_kind, source_config)?,
        catalog.build_sink(sink_kind, sink_config)?,
    ))
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
        source_topology: crate::delivery::SourceTopology::StaticPartitions(vec![0]),
        schema_origin: transferia::delivery::SchemaOrigin::ParserProjection,
        keep_system_columns: false,
        datasets: Vec::new(),
    };
    let source = transferia::compatibility::EndpointDescriptor::Logbroker(
        transferia::compatibility::SourceDescriptor {
            behavior: transferia::compatibility::SourceBehavior::BenchmarkDiscard,
            delivery_modes: transferia::compatibility::SourceDeliveryModes::STREAM,
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
fn default_registry_builds_logbroker_pqv1_driver_to_clickhouse_pipeline() -> anyhow::Result<()> {
    let registry = build_provider_catalog(&Arc::new(MetricsRegistry::new()))?;
    let config: Config = serde_yaml::from_str(
        r"
delivery_id: pqv1-clickhouse-test
delivery_type: stream
durable_storage: { type: local_file, path: /tmp/transferia-test-state }
source:
  logbroker:
    host: localhost
    port: 2135
    topics: [{ path: topic, partitions: [0] }]
    consumer_name: consumer
    auth: { type: token, token: test }
    driver: pqv1
    trusted_plaintext: true
    parser:
      common:
        table_naming: { type: from_config, name: events }
      json_parser:
        conversion_error: dlq
        unknown_fields: { action: fail }
        columns:
          - { jsonpath: $.id, column_name: id, json_data_type: number, arrow_type: Int64, nullable: false }
sink:
  clickhouse:
    hosts: [localhost]
    port: 9000
    trusted_plaintext: true
    database: default
    username: default
",
    )?;
    let source = registry.build_source(config.source.kind()?, config.source.raw()?.clone())?;
    let sink = registry.build_sink(config.sink.kind()?, config.sink.raw()?.clone())?;
    assert!(matches!(
        sink.compatibility(),
        transferia::compatibility::EndpointDescriptor::ClickHouse
    ));
    let discovery = configured_discovery(source.as_ref(), true)?;
    validate_discovered_pipeline(
        &source.compatibility(),
        &sink.compatibility(),
        sink.limits(),
        &discovery,
        true,
    )?;
    Ok(())
}

#[tokio::test]
async fn every_benchmark_config_matches_registered_provider_shapes() -> anyhow::Result<()> {
    for relative_path in [
        "benchmarks/config_bench_pqv1_json_parser_to_discard.yaml",
        "benchmarks/config_bench_pqv1_decompress_to_discard.yaml",
        "benchmarks/config_bench_pqv1_network_to_discard.yaml",
        "benchmarks/config_bench_pqv1_json_parser_to_clickhouse.yaml",
        "benchmarks/config_bench_pqv1_json_parser_to_s3.yaml",
    ] {
        let path = format!("{}/{relative_path}", env!("CARGO_MANIFEST_DIR"));
        let template = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to load {relative_path}"))?;
        let rendered = render_benchmark_template_defaults(&template)
            .with_context(|| format!("invalid benchmark template {relative_path}"))?;
        let config: Config = serde_yaml::from_str(&rendered)
            .with_context(|| format!("failed to parse {relative_path}"))?;
        let (source, sink) = build_resolved_endpoints(&config)
            .await
            .with_context(|| format!("invalid endpoints in {relative_path}"))?;
        sink.validate_pipeline_memory_limit(config.pipeline_memory_limit_bytes)
            .with_context(|| format!("invalid memory limits in {relative_path}"))?;
        let discovery = configured_discovery(source.as_ref(), true)?;
        validate_discovered_pipeline(
            &source.compatibility(),
            &sink.compatibility(),
            sink.limits(),
            &discovery,
            true,
        )
        .with_context(|| format!("incompatible providers in {relative_path}"))?;
    }
    Ok(())
}

fn render_benchmark_template_defaults(template: &str) -> anyhow::Result<String> {
    let mut rendered = String::with_capacity(template.len());
    let mut remaining = template;
    while let Some(start) = remaining.find("${") {
        rendered.push_str(&remaining[..start]);
        let expression = &remaining[start + 2..];
        let end = expression
            .find('}')
            .ok_or_else(|| anyhow::anyhow!("unterminated benchmark placeholder"))?;
        let placeholder = &expression[..end];
        let (_, default) = placeholder.split_once(":-").ok_or_else(|| {
            anyhow::anyhow!("benchmark placeholder '{placeholder}' has no test default")
        })?;
        rendered.push_str(default);
        remaining = &expression[end + 1..];
    }
    rendered.push_str(remaining);
    Ok(rendered)
}

#[tokio::test]
async fn root_example_config_matches_registered_provider_shapes() -> anyhow::Result<()> {
    let raw = std::fs::read_to_string(format!("{}/config.yaml", env!("CARGO_MANIFEST_DIR")))?
        .replace("${HOME}", "/tmp")
        .replace("${S3_ACCESS_KEY}", "test-access-key")
        .replace("${S3_SECRET_KEY}", "test-secret-key");
    let config: Config = serde_yaml::from_str(&raw)?;
    let (source, sink) = build_resolved_endpoints(&config).await?;
    sink.validate_pipeline_memory_limit(config.pipeline_memory_limit_bytes)?;
    let discovery = configured_discovery(source.as_ref(), true)?;
    validate_discovered_pipeline(
        &source.compatibility(),
        &sink.compatibility(),
        sink.limits(),
        &discovery,
        true,
    )?;
    Ok(())
}

#[tokio::test]
async fn postgres_pipeline_examples_match_registered_provider_shapes() -> anyhow::Result<()> {
    for relative_path in [
        "examples/postgres-to-clickhouse.yaml",
        "examples/postgres-to-s3.yaml",
    ] {
        let path = format!("{}/{relative_path}", env!("CARGO_MANIFEST_DIR"));
        let config = Config::from_file(&path)?;
        let (source, sink) = build_resolved_endpoints(&config).await?;
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

#[tokio::test]
async fn ytsaurus_examples_match_registered_provider_shapes() -> anyhow::Result<()> {
    for relative_path in [
        "config_ytsaurus_source_to_clickhouse.yaml",
        "config_ytsaurus_sink.yaml",
    ] {
        let config = Config::from_file(&format!("{}/{relative_path}", env!("CARGO_MANIFEST_DIR")))?;
        let (source, sink) = build_resolved_endpoints(&config).await?;
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
