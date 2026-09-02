use super::*;

#[test]
fn renders_every_wire_type_nested_messages_and_repeated_tags() -> anyhow::Result<()> {
    let payload = [
        0x08, 0x96, 0x01, // field 1, varint 150
        0x08, 0x01, // field 1 repeated
        0x11, 1, 2, 3, 4, 5, 6, 7, 8, // field 2, fixed64
        0x1a, 0x02, 0x08, 0x2a, // field 3, embedded message
        0x22, 0x05, b'h', b'e', b'l', b'l', b'o', // field 4, UTF-8
        0x2b, 0x30, 0x07, 0x2c, // field 5 group containing field 6
        0x3d, 1, 2, 3, 4, // field 7, fixed32
    ];
    let detection = ProtobufWireDetector
        .try_parse(&payload)?
        .expect("valid protobuf is detected");
    let preview = &detection.preview_tabs[0].content;

    assert_eq!(
        detection.config["protobuf"]["package_type"],
        "single_message"
    );
    assert!(preview.contains("1: varint, repeated ×2 = 150"));
    assert!(preview.contains("2: fixed64 = 0x0807060504030201"));
    assert!(preview.contains("3: length-delimited (2 bytes, embedded message)"));
    assert!(preview.contains("1: varint = 42"));
    assert!(preview.contains("4: length-delimited (5 bytes, UTF-8) = \"hello\""));
    assert!(preview.contains("5: group {"));
    assert!(preview.contains("7: fixed32 = 0x04030201"));
    Ok(())
}

#[test]
fn detects_complete_protoseq_framing_and_rejects_corruption() -> anyhow::Result<()> {
    let first = [0x08, 0x01];
    let second = [0x12, 0x01, b'x'];
    let payload = protoseq(&[&first, &second]);
    let detection = ProtobufWireDetector
        .try_parse(&payload)?
        .expect("valid protoseq is detected");

    assert_eq!(detection.config["protobuf"]["package_type"], "protoseq");
    assert_eq!(detection.sampled_rows, 2);
    assert!(detection.preview_tabs[0].content.contains("message 2:"));

    let mut corrupt = payload;
    corrupt[4 + first.len()] ^= 1;
    assert!(ProtobufWireDetector.try_parse(&corrupt)?.is_none());
    Ok(())
}

#[test]
fn rejects_empty_truncated_reserved_and_mismatched_group_payloads() -> anyhow::Result<()> {
    for payload in [
        Vec::new(),
        vec![0x08, 0x80],
        vec![0x0e],
        vec![0x00],
        vec![0x0b, 0x14],
        vec![0x0c],
        vec![0x12, 0x03, 1, 2],
    ] {
        assert!(
            ProtobufWireDetector.try_parse(&payload)?.is_none(),
            "{payload:?}"
        );
    }
    Ok(())
}

#[test]
fn sample_detection_requires_one_framing_and_honors_the_row_limit() -> anyhow::Result<()> {
    let single = [0x08, 0x01];
    let protoseq = protoseq(&[&[0x08, 0x02], &[0x08, 0x03]]);
    let detection = ProtobufWireDetector
        .try_parse_samples(&[&single, &protoseq], 1)?
        .expect("the first valid framing wins");
    assert_eq!(detection.sampled_messages, 1);
    assert_eq!(detection.sampled_rows, 1);
    assert_eq!(
        detection.config["protobuf"]["package_type"],
        "single_message"
    );
    Ok(())
}

fn protoseq(messages: &[&[u8]]) -> Vec<u8> {
    let mut payload = Vec::new();
    for message in messages {
        payload.extend_from_slice(&(message.len() as u32).to_le_bytes());
        payload.extend_from_slice(message);
        payload.extend_from_slice(&PROTOSEQ_MAGIC);
    }
    payload
}
