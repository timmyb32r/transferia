use super::config::LogbrokerSinkConfig;
use crate::providers::logbroker::LogbrokerDriver;

fn config(extra: &str) -> anyhow::Result<LogbrokerSinkConfig> {
    Ok(serde_yaml::from_str(&format!(
        "host: localhost\nport: 2135\ntopic_path: /demo/events\nproducer_id: transferia\nauth: {{ type: token, token: test }}\ndriver: ydb\ntrusted_plaintext: true\n{extra}"
    ))?)
}

#[test]
fn accepts_ydb_and_pqv1_drivers() -> anyhow::Result<()> {
    let ydb = config("")?;
    ydb.validate()?;
    assert_eq!(ydb.driver, LogbrokerDriver::Ydb);

    let pqv1: LogbrokerSinkConfig = serde_yaml::from_str(
        "host: localhost\nport: 2135\ntopic_path: /demo/events\nproducer_id: transferia\npartition_id: 2\nauth: { type: token_file, token_file: /tmp/token }\ndriver: pqv1\ntrusted_plaintext: true\n",
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
    let value = serde_yaml::from_str(
        "host: localhost\nport: 2135\ntopic_path: /demo/events\nproducer_id: transferia\nauth: { type: token, token: test }\ndriver: pqv1\ntrusted_plaintext: true\n",
    )?;
    let error = super::provider::build_sink_provider(value)
        .err()
        .expect("PQv1 without a partition must fail");
    assert!(error.to_string().contains("explicit partition_id"));
    Ok(())
}
