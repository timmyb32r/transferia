use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use transferia_core::delivery::SinkLimits;

struct UnusedComposition;

struct RejectSecondSchema;

#[async_trait::async_trait]
impl Middleware for RejectSecondSchema {
    async fn output_schema(&self, schema: &transferia_core::DatasetSchema) -> anyhow::Result<transferia_core::DatasetSchema> {
        anyhow::ensure!(schema.columns[0].name != "unsupported", "unsupported column");
        Ok(schema.clone())
    }

    async fn process(&self, data: transferia_core::TableData) -> anyhow::Result<transferia_core::TableData> {
        Ok(data)
    }
}

#[tokio::test]
async fn middleware_validation_checks_every_selected_table_before_execution() {
    let datasets = ["supported", "unsupported"].into_iter().map(|name| {
        let schema = transferia_core::DatasetSchema::new(vec![transferia_core::SchemaColumn::new(
            name.into(), arrow::datatypes::DataType::Int64, false,
        )]);
        transferia_core::DiscoveredDataset {
            namespace: Some(Arc::from("db")),
            update_policy: transferia_core::delivery::UpdatePolicy::Strict,
            role: DatasetRole::Main,
            name: Arc::from(name),
            incoming_schema: schema.clone(),
            stored_schema: schema,
            system_columns: Vec::new(),
        }
    }).collect();
    let discovery = DeliveryDiscovery {
        source_name: Arc::from("multiple tables"),
        source_topology: transferia_core::SourceTopology::StaticPartitions(vec![0, 1]),
        schema_origin: transferia_core::SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets,
        performance_advice: Vec::new(),
    };
    let error = validate_middlewares(&[Box::new(RejectSecondSchema)], discovery).await.unwrap_err();
    assert!(error.to_string().contains("unsupported"));
}

#[test]
fn same_table_names_in_different_namespaces_fail_before_sink_validation() {
    let schema = transferia_core::DatasetSchema::new(vec![transferia_core::SchemaColumn::new(
        "id".into(), arrow::datatypes::DataType::Int64, false,
    )]);
    let discovery = DeliveryDiscovery {
        source_name: Arc::from("multiple databases"),
        source_topology: transferia_core::SourceTopology::StaticPartitions(vec![0, 1]),
        schema_origin: transferia_core::SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets: ["first", "second"].into_iter().map(|namespace| transferia_core::DiscoveredDataset {
            namespace: Some(Arc::from(namespace)),
            update_policy: transferia_core::delivery::UpdatePolicy::Strict,
            role: DatasetRole::Main,
            name: Arc::from("events"),
            incoming_schema: schema.clone(),
            stored_schema: schema.clone(),
            system_columns: Vec::new(),
        }).collect(),
        performance_advice: Vec::new(),
    };
    let limits = RecordingLimits { called: AtomicBool::new(false) };
    let endpoint = transferia_delivery_contracts::semantics::EndpointDescriptor::ClickHouse;
    let error = validate_discovered_pipeline(&endpoint, &endpoint, &limits, &discovery, false).unwrap_err();
    let message = error.to_string();
    for expected in ["same name", "events", "first", "second"] {
        assert!(message.contains(expected), "{message}");
    }
    assert!(!limits.called.load(Ordering::SeqCst));
}

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
        "delivery_id: replay-test\ndelivery_name: Test delivery\ndurable_storage: { type: local_file, path: /tmp/replay-test }\ndelivery_type: batch\nsource: { test: {} }\nsink: { test: {} }\n",
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
delivery_name: Test delivery
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
