use std::sync::Arc;

use transferia_delivery_contracts::metrics::MetricsRegistry;
use transferia_delivery_contracts::semantics::RecordSemantics;
use transferia_registry::RegistryBuilder;

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
