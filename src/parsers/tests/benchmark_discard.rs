use super::*;
use bytes::Bytes;

#[test]
fn benchmark_discard_parser_drops_all_rows() -> anyhow::Result<()> {
    let parser = BenchmarkDiscardParser::new("logs".into());
    let messages = vec![
        Message::new(Bytes::from_static(b"{\"id\":\"a\"}")),
        Message::new(Bytes::from_static(b"{\"id\":\"b\"}")),
    ];
    let mut session = Arc::new(parser).create_session();
    let (valid, dlq) = session.parse_into(messages)?;
    assert_eq!(valid.batch.num_rows(), 0);
    assert!(!valid.is_dlq);
    assert!(dlq.is_none());
    assert_eq!(valid.table.as_ref(), "logs");
    assert!(valid.system_columns.is_empty());
    Ok(())
}
