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
fn ydb_topic_pqv1_driver_to_s3_matches_registered_provider_shapes() -> anyhow::Result<()> {
    let config: Config = serde_yaml::from_str(
        r"
delivery_id: pqv1-s3-test
delivery_type: stream
durable_storage: { type: local_file, path: /tmp/state }
source:
  ydb_topic:
    host: localhost
    port: 2135
    topics: [{ path: topic-a, partitions: [0] }]
    consumer_name: consumer-a
    auth: { type: token, token: test }
    driver: pqv1
    trusted_plaintext: true
    parser:
      common:
        table_naming: { type: from_config, name: events }
        system_columns:
          topic: _system_topic
          partition: _system_partition
          offset: _system_offset
          message_index: _system_message_index
          write_timestamp_ms: _system_write_timestamp_ms
      json_parser:
        conversion_error: dlq
        unknown_fields: { action: fail }
        json_framing: single_document
        columns:
          - { jsonpath: $.id, column_name: id, json_data_type: integer, arrow_type: Int64, nullable: false }
sink:
  s3:
    bucket: transfer-bucket
    partitioning: { type: source }
",
    )?;
    let source: crate::providers::ydb_topic::src_stream::YdbTopicSourceConfig =
        serde_yaml::from_value(config.source.raw()?.clone())?;
    drop(serde_yaml::from_value::<
        crate::parsers::json_parser::JsonParserConfig,
    >(source.parser.parser.raw()?.clone())?);
    let sink: crate::providers::s3::sink::S3SinkConfig =
        serde_yaml::from_value(config.sink.raw()?.clone())?;
    sink.validate()?;
    Ok(())
}
