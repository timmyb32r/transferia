use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use transferia_core::delivery::SinkLimits;

struct UnusedComposition;

struct RecordingLimits {
    called: AtomicBool,
}

impl SinkLimits for RecordingLimits {
    fn description(&self) -> transferia_core::delivery::SinkLimitsDescription {
        transferia_core::delivery::SinkLimitsDescription {
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

impl transferia_registry::Composition for UnusedComposition {
    fn fingerprint(&self) -> &'static str {
        "unused-test-composition"
    }

    fn definitions(&self) -> &[transferia_registry::ConnectorDefinition] {
        &[]
    }

    fn build_registry(
        &self,
        _metrics: &Arc<MetricsRegistry>,
    ) -> anyhow::Result<transferia_registry::Registry> {
        panic!("invalid pipeline memory must fail before building the connector registry")
    }

    fn resolve_many(
        &self,
        _connector: &str,
        _role: transferia_registry::EndpointRole,
        _raw: serde_yaml::Value,
        _cancellation: CancellationToken,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<Vec<serde_yaml::Value>>> + Send + '_>,
    > {
        Box::pin(async {
            panic!("invalid pipeline memory must fail before resolving connector configuration")
        })
    }
}

#[test]
fn semantic_errors_short_circuit_sink_limit_validation() {
    let limits = RecordingLimits {
        called: AtomicBool::new(false),
    };
    let discovery = DeliveryDiscovery {
        source_name: Arc::from("topic"),
        source_topology: transferia_core::delivery::SourceTopology::StaticPartitions(vec![0]),
        schema_origin: transferia_core::delivery::SchemaOrigin::ParserProjection,
        keep_system_columns: false,
        datasets: Vec::new(),
        performance_advice: Vec::new(),
    };
    let source = transferia_delivery_contracts::semantics::EndpointDescriptor::Logbroker(
        transferia_delivery_contracts::semantics::SourceDescriptor {
            behavior: transferia_delivery_contracts::semantics::SourceBehavior::BenchmarkDiscard,
            delivery_modes: transferia_delivery_contracts::semantics::SourceDeliveryModes::STREAM,
        },
    );

    assert!(validate_discovered_pipeline(
        &source,
        &transferia_delivery_contracts::semantics::EndpointDescriptor::ClickHouse,
        &limits,
        &discovery,
        false,
    )
    .is_err());
    assert!(!limits.called.load(Ordering::SeqCst));
}

#[test]
fn resolved_worker_document_preserves_the_exact_replay_identity() -> anyhow::Result<()> {
    let config = Config::from_yaml(
        "delivery_id: replay-test\ndurable_storage: { type: local_file, path: /tmp/replay-test }\ndelivery_type: batch\nsource: { test: {} }\nsink: { test: {} }\n",
    )?;
    let document = ResolvedConfigDocument {
        replay_identity: Some("control-plane-delivery:dtt-example:revision:7".to_owned()),
        pipelines: vec![config],
    };

    let yaml = serde_yaml::to_string(&document)?;
    let decoded: ResolvedConfigDocument = serde_yaml::from_str(&yaml)?;

    assert_eq!(
        decoded.replay_identity.as_deref(),
        Some("control-plane-delivery:dtt-example:revision:7")
    );
    Ok(())
}

#[tokio::test]
async fn plan_rejects_zero_pipeline_memory_before_discovery() -> anyhow::Result<()> {
    let yaml = r"
delivery_id: plan-test
durable_storage: { type: local_file, path: /tmp/transferia-plan-test }
delivery_type: batch
source:
  postgres:
    host: localhost
    port: 5432
    database: postgres
    username: postgres
    password: postgres
    trusted_plaintext: true
    tables: [{ schema: public, name: events }]
    batch_rows: 1
sink: { discard: {} }
pipeline_memory_limit_bytes: 0
";
    let composition = UnusedComposition;
    let error = build_delivery_plan_with(
        Config::from_yaml(yaml)?,
        CancellationToken::new(),
        &composition,
    )
    .await
    .err()
    .context("zero memory limit must fail")?;
    assert!(error
        .to_string()
        .contains("pipeline_memory_limit_bytes must be positive"));
    Ok(())
}
