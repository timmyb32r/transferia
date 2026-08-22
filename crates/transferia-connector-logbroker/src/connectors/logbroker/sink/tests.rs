use super::config::LogbrokerSinkConfig;
use crate::connectors::logbroker::LogbrokerDriver;

fn config(extra: &str) -> anyhow::Result<LogbrokerSinkConfig> {
    Ok(serde_yaml::from_str(&format!(
        "host: localhost\nport: 2135\ntopic: {{ type: topic, topic_path: /demo/events }}\nauth: {{ type: token, token: test }}\nserializer: {{ type: json }}\ndriver: ydb\ntrusted_plaintext: true\n{extra}"
    ))?)
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
