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

#[test]
fn preview_preserves_payload_coordinates_key_duplicate_headers_and_timestamp() {
    let headers = OwnedHeaders::new()
        .insert(Header {
            key: "duplicate",
            value: Some(&[1_u8, 2][..]),
        })
        .insert(Header::<&[u8]> {
            key: "duplicate",
            value: None,
        });
    let record = OwnedMessage::new(
        Some(vec![7, 8, 9]),
        Some(vec![0, 255]),
        "events".into(),
        Timestamp::LogAppendTime(1234),
        4,
        99,
        Some(headers),
    );

    let preview = preview_message(&record, 3).unwrap();
    assert_eq!(preview.payload, [7, 8, 9]);
    assert_eq!(preview.detection_payloads, [vec![7, 8, 9]]);
    assert_eq!(preview.metadata.topic, "events");
    assert_eq!(preview.metadata.partition, 4);
    assert_eq!(preview.metadata.offset, 99);
    assert_eq!(preview.metadata.sequence_number, 99);
    assert_eq!(preview.metadata.created_at_ms, None);
    assert_eq!(preview.metadata.written_at_ms, Some(1234));
    assert_eq!(preview.metadata.declared_uncompressed_size, Some(3));
    assert_eq!(preview.metadata.message_metadata.len(), 3);
    assert_eq!(preview.metadata.message_metadata[0].key, "kafka.key");
    assert_eq!(preview.metadata.message_metadata[0].value, [0, 255]);
    assert_eq!(preview.metadata.message_metadata[1].key, "duplicate");
    assert_eq!(preview.metadata.message_metadata[1].value, [1, 2]);
    assert_eq!(preview.metadata.message_metadata[2].key, "duplicate");
    assert!(preview.metadata.message_metadata[2].value.is_empty());
}

#[test]
fn preview_rejects_a_message_larger_than_the_explicit_limit() {
    let record = OwnedMessage::new(
        Some(vec![1, 2, 3]),
        None,
        "events".into(),
        Timestamp::NotAvailable,
        0,
        1,
        None,
    );

    let error = preview_message(&record, 2).err().unwrap();
    assert!(error.to_string().contains("exceeding max_bytes=2"));
}
