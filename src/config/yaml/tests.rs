use super::*;

#[test]
fn rejects_multiple_source_providers() -> anyhow::Result<()> {
    let config: Config =
        serde_yaml::from_str("source: {a: {}, b: {}}\nsink: {clickhouse: {}}\nmiddlewares: []\n")?;
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
fn pqv1_to_s3_config_matches_registered_provider_shapes() -> anyhow::Result<()> {
    let config: Config = serde_yaml::from_str(
        r"
source:
  pqv1:
    discovery_endpoint: grpc://localhost
    topic_path: topic-a
    consumer_name: consumer-a
    partition_group_ids: [0]
    auth: { type: access_token, token: test }
    parser:
      common:
        table_naming: { type: from_config, name: events }
        system_columns:
          topic: true
          partition: true
          offset: true
          message_index: true
          write_timestamp_ms: true
      json_parser:
        chunk_splitter: one-message-one-row
        columns:
          - { jsonpath: $.id, column_name: id, arrow_type: Int64, nullable: false }
sink:
  s3:
    bucket: transfer-bucket
    partitioning: { type: source }
keep_system_columns_in_sink: false
",
    )?;
    let source: crate::providers::pqv1::config::PqV1SourceConfig =
        serde_yaml::from_value(config.source.raw()?.clone())?;
    let _: crate::parsers::json_parser::JsonParserConfig =
        serde_yaml::from_value(source.parser.parser.raw()?.clone())?;
    let sink: crate::providers::s3::sink::S3SinkConfig =
        serde_yaml::from_value(config.sink.raw()?.clone())?;
    sink.validate()?;
    Ok(())
}
