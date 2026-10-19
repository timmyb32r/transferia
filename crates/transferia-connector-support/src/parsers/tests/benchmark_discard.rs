use super::*;
use bytes::Bytes;

#[test]
fn benchmark_discard_parser_drops_all_rows() -> anyhow::Result<()> {
    let parser = BenchmarkDiscardParser::new("logs".into());
    let messages = vec![
        Message::new(Bytes::from_static(b"{\"id\":\"a\"}")),
        Message::new(Bytes::from_static(b"{\"id\":\"b\"}")),
    ];
    let mut session = Arc::new(parser).create_session(1024);
    let (valid, dlq) = session.parse_into(messages)?;
    assert_eq!(valid[0].batch.num_rows(), 0);
    assert!(!valid[0].is_dlq);
    assert!(dlq.is_none());
    assert_eq!(valid[0].table.as_ref(), "logs");
    assert!(valid[0].system_columns.is_empty());
    Ok(())
}
