use std::sync::Arc;

use transferia_delivery_contracts::metrics::MetricsRegistry;
use transferia_delivery_contracts::semantics::RecordSemantics;
use transferia_registry::{EndpointRole, RegistryBuilder};

#[test]
fn registration_exposes_only_the_bounded_throughput_tuning_surface() -> anyhow::Result<()> {
    let mut builder = RegistryBuilder::new();
    super::register(&mut builder, &Arc::new(MetricsRegistry::new()))?;
    let registry = builder.build();

    assert_eq!(
        tuning_contract(registry.tuning_parameters("opensearch", EndpointRole::Source)?)?,
        vec![
            (
                "/page_rows",
                serde_json::json!(10_000),
                vec![
                    serde_json::json!(2_500),
                    serde_json::json!(5_000),
                    serde_json::json!(10_000),
                ],
            ),
            (
                "/read_concurrency",
                serde_json::json!(4),
                vec![
                    serde_json::json!(1),
                    serde_json::json!(2),
                    serde_json::json!(4),
                    serde_json::json!(8),
                ],
            ),
        ]
    );
    assert_eq!(
        tuning_contract(registry.tuning_parameters("opensearch", EndpointRole::Sink)?)?,
        vec![
            (
                "/bulk_target_rows",
                serde_json::json!(20_000),
                vec![
                    serde_json::json!(2_500),
                    serde_json::json!(10_000),
                    serde_json::json!(20_000),
                ],
            ),
            (
                "/bulk_target_bytes",
                serde_json::json!(16_777_216),
                vec![
                    serde_json::json!(4_194_304),
                    serde_json::json!(16_777_216),
                    serde_json::json!(33_554_432),
                ],
            ),
            (
                "/bulk_concurrency",
                serde_json::json!(8),
                vec![
                    serde_json::json!(1),
                    serde_json::json!(2),
                    serde_json::json!(4),
                    serde_json::json!(8),
                ],
            ),
        ]
    );
    Ok(())
}

fn tuning_contract(
    parameters: &[transferia_registry::tuning::TuningParameter],
) -> anyhow::Result<Vec<(&str, serde_json::Value, Vec<serde_json::Value>)>> {
    parameters
        .iter()
        .map(|parameter| {
            let candidates = match parameter {
                transferia_registry::tuning::TuningParameter::UnsignedInteger {
                    candidates,
                    ..
                } => candidates.iter().copied().map(serde_json::Value::from).collect(),
                other => anyhow::bail!("unexpected OpenSearch tuning parameter: {other:?}"),
            };
            Ok((parameter.pointer(), parameter.baseline(), candidates))
        })
        .collect()
}

#[test]
fn registration_publishes_batch_source_and_append_only_sink() -> anyhow::Result<()> {
    let mut builder = RegistryBuilder::new();
    super::register(&mut builder, &Arc::new(MetricsRegistry::new()))?;
    let registry = builder.build();
    let definition = registry
        .definitions()
        .iter()
        .find(|definition| definition.key == "opensearch")
        .ok_or_else(|| anyhow::anyhow!("missing OpenSearch definition"))?;
    let source = definition
        .source
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("missing OpenSearch source"))?;
    let sink = definition
        .sink
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("missing OpenSearch sink"))?;

    assert!(source
        .record_semantics
        .contains(&RecordSemantics::AppendOnly));
    assert_eq!(sink.record_semantics, [RecordSemantics::AppendOnly]);
    assert_eq!(source.initial["auth"]["type"], "basic");
    assert_eq!(sink.initial["auth"]["type"], "basic");
    assert_eq!(
        source.schema["$defs"]["OpenSearchAuth"]["oneOf"][0]["properties"]["password"]
            ["x-ui"]["widget"],
        "password"
    );
    Ok(())
}
