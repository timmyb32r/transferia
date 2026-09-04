use crate::parsers::detection::{detect, detect_samples};

#[test]
fn json_detection_infers_framing_and_lossless_primitive_columns() {
    let detected = detect(
        br#"{"id":1,"name":"one"}
{"id":2,"name":null}"#,
    );
    assert_eq!(detected.len(), 1);
    let config = &detected[0].config;
    assert_eq!(config["json_parser"]["json_framing"], "json_lines");
    assert_eq!(config["json_parser"]["columns"][0]["column_name"], "id");
    assert_eq!(config["json_parser"]["columns"][0]["arrow_type"], "Int64");
    assert_eq!(config["json_parser"]["columns"][1]["nullable"], true);
    assert_eq!(
        config["json_parser"]["unknown_fields"]["action"],
        "send_to_column"
    );
    assert_eq!(detected[0].preview_tabs[0].label, "Pretty print");
    assert_eq!(detected[0].sampled_messages, 1);
    assert_eq!(detected[0].sampled_rows, 2);
    assert_eq!(detected[0].sample_rows.len(), 2);
}

#[test]
fn json_detection_aggregates_messages_until_the_explicit_row_limit() {
    let detected = detect_samples(
        &[
            br#"{"id":1,"sometimes":"present"}"#,
            br#"{"id":2}"#,
            br#"{"id":3,"ignored":"past-limit"}"#,
        ],
        2,
    );

    assert_eq!(detected.len(), 1);
    assert_eq!(detected[0].sampled_messages, 2);
    assert_eq!(detected[0].sampled_rows, 2);
    assert_eq!(detected[0].inferred_columns[0].name, "id");
    assert_eq!(detected[0].inferred_columns[1].name, "sometimes");
    assert!(detected[0].inferred_columns[1].nullable);
    assert!(!detected[0]
        .inferred_columns
        .iter()
        .any(|column| column.name == "ignored"));
}

#[test]
fn non_json_payload_has_no_detection() {
    assert!(detect(&[0xff, 0x00, 0x10]).is_empty());
}

#[test]
fn protobuf_cloud_event_is_detected_as_both_cloud_events_and_protobuf() {
    let mut payload = Vec::new();
    for (field, value) in [
        (1_u8, b"event-1".as_slice()),
        (2, b"urn:transferia:test".as_slice()),
        (3, b"1.0".as_slice()),
        (4, b"example.created".as_slice()),
        (6, &[0, 255, 65][..]),
    ] {
        payload.push((field << 3) | 2);
        payload.push(u8::try_from(value.len()).unwrap());
        payload.extend_from_slice(value);
    }

    let detected = detect(&payload);
    assert_eq!(
        detected
            .iter()
            .map(|detection| detection.key.as_str())
            .collect::<Vec<_>>(),
        ["cloud_events", "protobuf"]
    );
    assert_eq!(
        detected[0].config["protobuf"]["message_name"],
        "io.cloudevents.v1.CloudEvent"
    );
}
