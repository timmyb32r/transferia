use super::*;

#[test]
fn rejects_multiple_source_providers() -> anyhow::Result<()> {
    let config: Config =
        serde_yaml::from_str("delivery_id: test\ndelivery_type: batch\ndurable_storage: { type: local_file, path: /tmp/state }\nsource: {a: {}, b: {}}\nsink: {clickhouse: {}}\nmiddlewares: []\n")?;
    anyhow::ensure!(config.source.kind().is_err());
    Ok(())
}

#[test]
fn rejects_provider_specific_top_level_fields() {
    let result = serde_yaml::from_str::<Config>(
        "source: {pqv1: {}}\nsink: {clickhouse: {}}\nrecreate_tables: true\n",
    );
    assert!(result.is_err());
}

#[test]
fn durable_identity_and_storage_are_required_and_validated_explicitly() {
    let missing = serde_yaml::from_str::<Config>("source: {a: {}}\nsink: {b: {}}\n");
    assert!(missing.is_err());

    let invalid: Config = serde_yaml::from_str(
        "delivery_id: 'not/a/path'\ndelivery_type: batch\ndurable_storage: { type: local_file, path: /tmp/state }\nsource: {a: {}}\nsink: {b: {}}\n",
    )
    .unwrap();
    assert!(invalid.durable_storage.build(&invalid.delivery_id).is_err());
}

#[test]
fn yaml_values_are_not_silently_expanded_from_the_environment() -> anyhow::Result<()> {
    let config = Config::from_yaml(
        "delivery_id: '${TRANSFERIA_DELIVERY_ID}'\ndelivery_type: batch\ndurable_storage: { type: local_file, path: /tmp/state }\nsource: {a: {}}\nsink: {b: {}}\n",
    )?;

    assert_eq!(config.delivery_id, "${TRANSFERIA_DELIVERY_ID}");
    Ok(())
}

#[test]
fn resolved_endpoint_values_can_be_serialized_without_installation_metadata() -> anyhow::Result<()>
{
    let mut config = Config::from_yaml(
        "delivery_id: test\ndelivery_type: batch\ndurable_storage: { type: local_file, path: /tmp/state }\nsource: { a: { installation: { type: managed } } }\nsink: { b: {} }\n",
    )?;
    config.source.replace_raw(
        "a".to_owned(),
        serde_yaml::from_str("host: resolved.example\nport: 1234\n")?,
    );

    let yaml = serde_yaml::to_string(&config)?;
    assert!(yaml.contains("resolved.example"));
    assert!(!yaml.contains("installation"));
    Ok(())
}
