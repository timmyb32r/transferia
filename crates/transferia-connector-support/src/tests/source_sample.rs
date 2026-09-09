use super::*;
use crate::parsers::{ParserConfig, ParserPlan};
use transferia_core::data::message::{MessageHeader, MessageMeta};

fn message() -> Message {
    Message {
        value: bytes::Bytes::from_static(b"not-json"), tombstone: false,
        key: Some(bytes::Bytes::from_static(b"key")),
        headers: Arc::from([MessageHeader { key: Arc::from("header"), value: None }]),
        meta: MessageMeta { topic: Some(Arc::from("topic")), partition: Some(2), offset: Some(9), write_timestamp_ms: Some(123) },
    }
}
fn limits() -> TableSampleLimits {
    TableSampleLimits { row_limit: 20, max_bytes: 16 * 1024 * 1024, timeout_ms: 30_000 }
}

#[tokio::test]
async fn sample_matches_the_configured_production_parser_including_envelope() -> anyhow::Result<()> {
    let config: ParserConfig = serde_yaml::from_str("common:\n  table_naming: { type: from_config, name: authored_name }\nraw_to_table: {}\n")?;
    let plan = ParserPlan::from_config(&config, "topic")?;
    let mut input = message(); input.tombstone = true; input.value = bytes::Bytes::new();
    let expected = plan.parser().create_session(limits().max_bytes).parse_into(vec![input.clone()])?.0;
    let actual = parse_sample(plan.parser(), vec![input], limits(), CancellationToken::new()).await?;
    assert_eq!(actual.len(), 1);
    assert_eq!(&*actual[0].table, "authored_name");
    assert_eq!(actual[0].batch, expected.batch);
    Ok(())
}

#[tokio::test]
async fn sample_preserves_dlq_and_does_not_fall_back_to_detected_parsers() -> anyhow::Result<()> {
    for policy in ["fail", "dlq"] {
        let config: ParserConfig = serde_yaml::from_str(&format!(
            "common:\n  table_naming: {{type: from_config, name: events}}\ndebezium:\n  on_parse_error: {policy}\n  connection: {{url: 'http://registry.invalid', request_timeout_ms: 1000}}\n"
        ))?;
        let plan = ParserPlan::from_config(&config, "topic")?;
        let result = parse_sample(plan.parser(), vec![message()], limits(), CancellationToken::new()).await;
        if policy == "fail" { assert!(result.is_err()); }
        else {
            let tables = result?;
            assert_eq!(tables.len(), 2);
            assert_eq!(tables[0].batch.num_rows(), 0);
            assert!(tables[1].is_dlq);
            assert_eq!(tables[1].batch.num_rows(), 1);
            assert_eq!(&*tables[1].table, "events_dlq");
        }
    }
    Ok(())
}

#[tokio::test]
async fn sample_rejects_insufficient_budget_and_cancellation() -> anyhow::Result<()> {
    let config: ParserConfig = serde_yaml::from_str("common:\n  table_naming: {type: from_config, name: events}\nraw_to_table: {}\n")?;
    let plan = ParserPlan::from_config(&config, "topic")?;
    assert!(parse_sample(plan.parser(), vec![message()], TableSampleLimits { max_bytes: 1, ..limits() }, CancellationToken::new()).await.is_err());
    let cancellation = CancellationToken::new(); cancellation.cancel();
    assert!(parse_sample(plan.parser(), vec![message()], limits(), cancellation).await.is_err());
    Ok(())
}
