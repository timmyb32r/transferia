use std::sync::Arc;

use transferia_delivery_contracts::metrics::MetricsRegistry;
use transferia_registry::{ConnectionCheckStatus, EndpointRole, RegistryBuilder};

use super::{configured_source_paths, incomplete_entities_result, register, ytsaurus};

#[test]
fn registration_keeps_the_measured_writer_buffer_default_in_sync() -> anyhow::Result<()> {
    let mut builder = RegistryBuilder::new();
    register(&mut builder, &Arc::new(MetricsRegistry::new()))?;
    let registry = builder.build();
    let definition = registry
        .definitions()
        .iter()
        .find(|definition| definition.key == "ytsaurus")
        .ok_or_else(|| anyhow::anyhow!("missing YTsaurus definition"))?;
    let sink = definition
        .sink
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("missing YTsaurus sink"))?;
    let parameter = registry
        .tuning_parameters("ytsaurus", EndpointRole::Sink)?
        .iter()
        .find(|parameter| parameter.pointer() == "/write_row_buffer_bytes")
        .ok_or_else(|| anyhow::anyhow!("missing writer row-buffer tuning parameter"))?;

    assert_eq!(sink.initial["write_row_buffer_bytes"], 524_288);
    assert_eq!(parameter.baseline(), serde_json::json!(524_288));
    Ok(())
}

#[test]
fn empty_source_paths_cannot_be_reported_as_verified_entities() -> anyhow::Result<()> {
    let config = serde_yaml::from_str::<ytsaurus::YTsaurusSourceConfig>(
        "auth: { type: token, token: test }\nhost: localhost\nport: 8000\ntrusted_plaintext: true\ntables:\n  - path: ''\n",
    )?;

    assert!(configured_source_paths(&config).is_none());
    Ok(())
}

#[test]
fn incomplete_entity_result_is_explicitly_partial() {
    let result = incomplete_entities_result("entity access was not checked");

    assert!(matches!(
        result.status,
        ConnectionCheckStatus::NetworkReachable
    ));
    assert_eq!(
        result.message.as_deref(),
        Some("entity access was not checked")
    );
}
