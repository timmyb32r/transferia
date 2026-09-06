use std::future::Future;
use std::pin::Pin;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use transferia_core::{SystemColumns, TableData};
use transferia_registry::{ComponentRegistration, ConnectorDefinition, DeliveryMode, Registry, RegistryBuilder};

use super::*;

#[derive(Default)]
struct SampleComposition {
    reads: Arc<AtomicUsize>,
    resolutions: AtomicUsize,
    stall_resolution: bool,
    resolution_cancellation: std::sync::Mutex<Option<CancellationToken>>,
}

impl Composition for SampleComposition {
    fn fingerprint(&self) -> &str { "preview-test" }
    fn definitions(&self) -> &[ConnectorDefinition] { &[] }
    fn build_registry(&self, _: &Arc<transferia_delivery_contracts::metrics::MetricsRegistry>) -> anyhow::Result<Registry> {
        let mut builder = RegistryBuilder::new();
        transferia_middleware_datafusion::register(&mut builder)?;
        transferia_middleware_filter::register(&mut builder)?;
        let reads = self.reads.clone();
        builder.register(ComponentRegistration::new("sample", "Sample")
            .source::<BTreeMap<String, Value>, _, _>(vec![DeliveryMode::Batch], false,
                || serde_json::json!({}), |_| anyhow::bail!("preview must not construct a delivery source"))?
            .source_table_sampler::<Value, _, _>(move |_, table, limits, _| {
                let reads = reads.clone();
                async move {
                    reads.fetch_add(1, Ordering::SeqCst);
                    anyhow::ensure!(limits.row_limit == 20 && limits.max_bytes == 16 * 1024 * 1024 && limits.timeout_ms == 30_000, "explicit sample limits were not forwarded");
                    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![
                        Field::new("id", DataType::Int64, false).with_metadata(std::collections::HashMap::from([
                            ("ARROW:extension:name".into(), "test.exact.integer".into()),
                            ("ARROW:extension:metadata".into(), "{\"semantic\":\"original\"}".into()),
                            ("transferia.primary_key".into(), "true".into()),
                        ])),
                        Field::new("kind", DataType::Utf8, false),
                    ])), vec![Arc::new(Int64Array::from(vec![1, 3])), Arc::new(StringArray::from(vec!["drop", "keep"]))])?;
                    Ok(TableData::new(Arc::from(table.name), false, batch, SystemColumns::default()).with_namespace(Arc::from(table.namespace)))
                }
            }))?;
        Ok(builder.build())
    }
    fn resolve_many(&self, connector: &str, role: EndpointRole, raw: serde_yaml::Value, cancellation: CancellationToken)
        -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<serde_yaml::Value>>> + Send + '_>> {
        assert_eq!(connector, "sample");
        assert_eq!(role, EndpointRole::Source);
        self.resolutions.fetch_add(1, Ordering::SeqCst);
        *self.resolution_cancellation.lock().unwrap() = Some(cancellation);
        if self.stall_resolution {
            return Box::pin(std::future::pending());
        }
        Box::pin(async move { Ok(vec![raw]) })
    }
}

fn request(steps: Value, through_step: usize) -> anyhow::Result<TransformPreviewRequest> {
    Ok(serde_json::from_value(serde_json::json!({
        "middlewares": steps, "through_step":through_step,
        "source":{"connector":"sample","config":{}},
        "table":{"namespace":"public","name":"events"}, "row_limit":20,
        "max_sample_bytes":16777216,"memory_limit_bytes":268435456,"timeout_ms":30000
    }))?)
}

#[tokio::test]
async fn source_preview_executes_real_filter_then_sql_and_never_constructs_workers() -> anyhow::Result<()> {
    let composition = SampleComposition::default();
    let preview = ControlPlane::preview_transforms_with(&composition, request(serde_json::json!([
        {"filter":{"field":"kind","value":"keep"}},
        {"datafusion":{"sql":"SELECT id * 2 AS doubled FROM input"}}
    ]), 1)?, CancellationToken::new(), None).await?;
    assert_eq!(serde_json::to_value(preview.before.rows)?, serde_json::json!([{"id":"3","kind":"keep"}]));
    assert_eq!(serde_json::to_value(preview.after.rows)?, serde_json::json!([{"doubled":"6"}]));
    assert_eq!(preview.after.columns[0].arrow_type, "Int64");
    assert_eq!(preview.after.table.namespace.as_deref(), Some("public"));
    assert_eq!(composition.reads.load(Ordering::SeqCst), 1);
    assert_eq!(composition.resolutions.load(Ordering::SeqCst), 1);
    assert!(preview.applied);
    Ok(())
}

#[tokio::test]
async fn source_preview_keeps_empty_arrow_schema_between_real_transforms() -> anyhow::Result<()> {
    let preview = ControlPlane::preview_transforms_with(&SampleComposition::default(), request(serde_json::json!([
        {"filter":{"field":"kind","value":"absent"}},
        {"datafusion":{"sql":"SELECT id * 2 AS doubled FROM input"}}
    ]), 1)?, CancellationToken::new(), None).await?;
    assert!(preview.before.rows.is_empty());
    assert!(preview.after.rows.is_empty());
    assert_eq!(preview.after.columns[0].name, "doubled");
    assert_eq!(preview.after.columns[0].arrow_type, "Int64");
    Ok(())
}

#[tokio::test]
async fn source_preview_skips_excluded_table_and_its_action_column_validation() -> anyhow::Result<()> {
    let preview = ControlPlane::preview_transforms_with(&SampleComposition::default(), request(serde_json::json!([
        {"tables":{"include":"public.*","exclude":"public.events"},"filter":{"field":"missing","value":"x"}}
    ]), 0)?, CancellationToken::new(), None).await?;
    assert!(!preview.applied);
    assert_eq!(preview.before.rows, preview.after.rows);
    Ok(())
}

#[tokio::test]
async fn source_preview_rejects_malformed_action_before_resolving_or_reading_source() -> anyhow::Result<()> {
    let composition = SampleComposition::default();
    let error = ControlPlane::preview_transforms_with(&composition, request(serde_json::json!([
        {"filter":{},"datafusion":{}}
    ]), 0)?, CancellationToken::new(), None).await.err().unwrap();
    assert!(error.to_string().contains("exactly one"));
    assert_eq!(composition.reads.load(Ordering::SeqCst), 0);
    assert_eq!(composition.resolutions.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn source_preview_ignores_unfinished_steps_after_the_selected_step() -> anyhow::Result<()> {
    let preview = ControlPlane::preview_transforms_with(&SampleComposition::default(), request(serde_json::json!([
        {"datafusion":{"sql":"SELECT id FROM input"}},
        {"tables":{"include":""},"filter":{},"datafusion":{}}
    ]), 0)?, CancellationToken::new(), None).await?;
    assert_eq!(serde_json::to_value(preview.after.rows)?, serde_json::json!([{"id":"1"},{"id":"3"}]));
    Ok(())
}

#[tokio::test]
async fn source_preview_rejects_zero_budgets_before_any_source_io() -> anyhow::Result<()> {
    for field in ["max_sample_bytes", "memory_limit_bytes", "timeout_ms"] {
        let composition = SampleComposition::default();
        let mut value = serde_json::json!({
            "middlewares":[{"datafusion":{"sql":"SELECT * FROM input"}}],"through_step":0,
            "source":{"connector":"sample","config":{}},"table":{"namespace":"public","name":"events"},
            "row_limit":20,"max_sample_bytes":16777216,"memory_limit_bytes":268435456,"timeout_ms":30000
        });
        value[field] = Value::from(0);
        let error = ControlPlane::preview_transforms_with(&composition, serde_json::from_value(value)?, CancellationToken::new(), None).await.err().unwrap();
        assert!(error.to_string().contains(field));
        assert_eq!(composition.resolutions.load(Ordering::SeqCst), 0);
        assert_eq!(composition.reads.load(Ordering::SeqCst), 0);
    }
    Ok(())
}

#[tokio::test]
async fn source_preview_timeout_cancels_endpoint_resolution() -> anyhow::Result<()> {
    let composition = SampleComposition { stall_resolution: true, ..Default::default() };
    let mut request = request(serde_json::json!([{"datafusion":{"sql":"SELECT * FROM input"}}]), 0)?;
    request.timeout_ms = 1;
    let error = ControlPlane::preview_transforms_with(&composition, request, CancellationToken::new(), None).await.err().unwrap();
    assert!(error.to_string().contains("timeout_ms"));
    assert!(composition.resolution_cancellation.lock().unwrap().as_ref().unwrap().is_cancelled());
    assert_eq!(composition.reads.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn source_preview_explains_unavailable_synthetic_event_metadata() -> anyhow::Result<()> {
    let error = ControlPlane::preview_transforms_with(&SampleComposition::default(), request(serde_json::json!([
        {"datafusion":{"sql":"SELECT _system_offset FROM input"}}
    ]), 0)?, CancellationToken::new(), None).await.err().unwrap();
    assert!(error.to_string().contains("synthetic transport and CDC metadata are not available"));
    assert!(error.to_string().contains("_system_offset"));
    Ok(())
}

#[tokio::test]
async fn source_preview_preserves_exact_native_arrow_metadata_without_inventing_flags() -> anyhow::Result<()> {
    let preview = ControlPlane::preview_transforms_with(&SampleComposition::default(), request(serde_json::json!([
        {"filter":{"field":"kind","value":"keep"}},
        {"datafusion":{"sql":"SELECT * FROM input"}}
    ]), 1)?, CancellationToken::new(), None).await?;
    let metadata = &preview.after.columns[0].metadata;
    assert_eq!(metadata["ARROW:extension:name"], "test.exact.integer");
    assert_eq!(metadata["ARROW:extension:metadata"], "{\"semantic\":\"original\"}");
    assert_eq!(metadata["transferia.primary_key"], "true");
    assert!(!metadata.contains_key("transferia.low_cardinality"));
    assert!(preview.after.columns[1].metadata.is_empty());
    Ok(())
}

#[tokio::test]
async fn source_preview_executes_required_column_and_sql_validation_on_native_input() -> anyhow::Result<()> {
    for action in [
        serde_json::json!({"filter":{"field":"missing","value":"x"}}),
        serde_json::json!({"datafusion":{"sql":"SELECT missing FROM input"}}),
        serde_json::json!({"datafusion":{"sql":"DROP TABLE input"}}),
    ] {
        let error = ControlPlane::preview_transforms_with(&SampleComposition::default(), request(serde_json::json!([action]), 0)?, CancellationToken::new(), None).await.err().unwrap();
        assert!(error.to_string().contains("transform step 1"));
    }
    Ok(())
}
