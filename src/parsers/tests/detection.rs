use crate::parsers::detection::detect;

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
}

#[test]
fn non_json_payload_has_no_detection() {
    assert!(detect(&[0xff, 0x00, 0x10]).is_empty());
}
