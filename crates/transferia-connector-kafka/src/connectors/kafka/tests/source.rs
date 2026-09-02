use rdkafka::message::{Header, OwnedHeaders};

use super::*;

#[test]
fn source_message_preserves_binary_key_ordered_duplicate_headers_and_i64_coordinates() {
    let headers = OwnedHeaders::new()
        .insert(Header {
            key: "duplicate",
            value: Some(&[0_u8, 255][..]),
        })
        .insert(Header::<&[u8]> {
            key: "duplicate",
            value: None,
        });
    let record = OwnedMessage::new(
        Some(vec![1, 2, 3]),
        Some(vec![0, 255]),
        "account/topic".into(),
        Timestamp::CreateTime(1234),
        i32::MAX,
        i64::MAX,
        Some(headers),
    );

    let message = source_message(&record);
    assert_eq!(message.value.as_ref(), [1, 2, 3]);
    assert!(!message.tombstone);
    assert_eq!(message.key.as_deref(), Some(&[0, 255][..]));
    assert_eq!(message.headers.len(), 2);
    assert_eq!(message.headers[0].key.as_ref(), "duplicate");
    assert_eq!(message.headers[0].value.as_deref(), Some(&[0, 255][..]));
    assert_eq!(message.headers[1].key.as_ref(), "duplicate");
    assert!(message.headers[1].value.is_none());
    assert_eq!(message.meta.partition, Some(i64::from(i32::MAX)));
    assert_eq!(message.meta.offset, Some(i64::MAX));
    assert_eq!(message.meta.write_timestamp_ms, Some(1234));
}

#[test]
fn null_payload_is_an_explicit_tombstone_not_an_empty_value() {
    let record = OwnedMessage::new(
        None,
        Some(vec![1, 2, 3]),
        "account/topic".into(),
        Timestamp::NotAvailable,
        0,
        7,
        None,
    );

    let message = source_message(&record);
    assert!(message.tombstone);
    assert!(message.value.is_empty());
    assert_eq!(message.key.as_deref(), Some(&[1, 2, 3][..]));
}
