use super::config::LogbrokerSinkConfig;
use crate::connectors::logbroker::LogbrokerDriver;

fn config(extra: &str) -> anyhow::Result<LogbrokerSinkConfig> {
    Ok(serde_yaml::from_str(&format!(
        "host: localhost\nport: 2135\ntopic: {{ type: topic, topic_path: /demo/events }}\nauth: {{ type: token, token: test }}\nserializer: {{ type: json }}\ndriver: ydb\ntrusted_plaintext: true\n{extra}"
    ))?)
}

#[test]
fn topic_routing_control_uses_the_wide_layout() {
    let schema = serde_json::to_value(schemars::schema_for!(LogbrokerSinkConfig))
        .expect("Logbroker sink schema must serialize");
    assert_eq!(
        schema.pointer("/properties/topic/x-ui/control_width"),
        Some(&serde_json::json!("routing")),
    );
}

#[test]
fn accepts_ydb_and_pqv1_drivers() -> anyhow::Result<()> {
    let ydb = config("")?;
    ydb.validate()?;
    assert_eq!(ydb.driver, LogbrokerDriver::Ydb);

    let pqv1: LogbrokerSinkConfig = serde_yaml::from_str(
        "host: localhost\nport: 2135\ntopic: { type: topic, topic_path: /demo/events }\npartition_id: 2\nauth: { type: token_file, token_file: /tmp/token }\nserializer: { type: json }\ndriver: pqv1\ntrusted_plaintext: true\n",
    )?;
    pqv1.validate()?;
    assert_eq!(pqv1.driver, LogbrokerDriver::Pqv1);
    Ok(())
}

#[test]
fn rejects_invalid_partition() -> anyhow::Result<()> {
    let value = config("partition_id: -1\n")?;
    let error = value.validate().expect_err("negative partition must fail");
    assert!(error
        .to_string()
        .contains("partition_id must be nonnegative"));
    Ok(())
}

#[test]
fn pqv1_requires_an_explicit_partition() -> anyhow::Result<()> {
    let value: LogbrokerSinkConfig = serde_yaml::from_str(
        "host: localhost\nport: 2135\ntopic: { type: topic, topic_path: /demo/events }\nauth: { type: token, token: test }\nserializer: { type: json }\ndriver: pqv1\ntrusted_plaintext: true\n",
    )?;
    let error = super::connector::build_sink_connector(value)
        .err()
        .expect("PQv1 without a partition must fail");
    assert!(error.to_string().contains("explicit partition_id"));
    Ok(())
}

#[test]
fn rejects_removed_producer_id() {
    let error = serde_yaml::from_str::<LogbrokerSinkConfig>(
        "host: localhost\nport: 2135\ntopic: { type: topic, topic_path: /demo/events }\nproducer_id: legacy\nauth: { type: token, token: test }\nserializer: { type: json }\ndriver: ydb\ntrusted_plaintext: true\n",
    )
    .err()
    .expect("producer_id must no longer be accepted");
    assert!(error.to_string().contains("producer_id"));
}

#[test]
fn topic_selection_is_strict_and_prefix_routes_by_dataset() -> anyhow::Result<()> {
    let fixed = config("")?;
    assert_eq!(fixed.topic.topic_for_table("orders"), "/demo/events");

    let prefixed: LogbrokerSinkConfig = serde_yaml::from_str(
        "host: localhost\nport: 2135\ntopic: { type: topic_prefix, topic_prefix: replica }\nauth: { type: token, token: test }\nserializer: { type: json }\ndriver: ydb\ntrusted_plaintext: true\n",
    )?;
    prefixed.validate()?;
    assert_eq!(prefixed.topic.topic_for_table("orders"), "replica.orders");

    let both = serde_yaml::from_str::<LogbrokerSinkConfig>(
        "host: localhost\nport: 2135\ntopic: { type: topic, topic_path: /demo/events, topic_prefix: replica }\nauth: { type: token, token: test }\nserializer: { type: json }\ndriver: ydb\ntrusted_plaintext: true\n",
    );
    assert!(both.is_err(), "topic and topic_prefix must not coexist");
    Ok(())
}

#[test]
fn pqv1_rejects_topic_prefix_explicitly() -> anyhow::Result<()> {
    let config: LogbrokerSinkConfig = serde_yaml::from_str(
        "host: localhost\nport: 2135\ntopic: { type: topic_prefix, topic_prefix: replica }\npartition_id: 0\nauth: { type: token, token: test }\nserializer: { type: json }\ndriver: pqv1\ntrusted_plaintext: true\n",
    )?;
    let error = super::connector::build_sink_connector(config)
        .err()
        .expect("PQv1 prefix mode must fail");
    assert!(error.to_string().contains("does not support topic_prefix"));
    Ok(())
}

#[test]
fn table_identity_is_validated_before_discovery_and_runtime_writes() -> anyhow::Result<()> {
    use std::sync::Arc;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use transferia_core::{DeliveryDiscovery, SchemaOrigin, SinkLimits, SourceTopology};

    let limits = config("")?;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )?;
    let memory = transferia_core::memory::PipelineMemory::new(1024);
    let mut output = transferia_core::SinkBatch {
        table: Arc::from("events"),
        namespace: None,
        schema: Arc::new(transferia_core::DatasetSchema::from_arrow(batch.schema().as_ref())?),
        byte_size: batch.get_array_memory_size(),
        memory: memory.reserve_transform(batch.get_array_memory_size()),
        batch,
        is_dlq: false,
        system_columns: Default::default(),
    };
    let mut discovery = DeliveryDiscovery {
        source_name: Arc::from("test"),
        source_topology: SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::ParserRuntime,
        keep_system_columns: false,
        datasets: vec![output.dataset(false)?],
        performance_advice: Vec::new(),
    };
    limits.validate_discovery(&discovery)?;
    limits.validate_batch(&discovery, &output)?;
    for invalid in [String::new(), "x".repeat(2049)] {
        output.table = Arc::from(invalid);
        discovery.datasets[0].name = Arc::clone(&output.table);
        assert!(limits.validate_discovery(&discovery).is_err());
        assert!(limits.validate_batch(&discovery, &output).is_err());
    }
    Ok(())
}
