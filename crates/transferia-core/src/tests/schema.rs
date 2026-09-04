use arrow::datatypes::DataType;

use super::{SchemaColumn, META_ARROW_EXTENSION_METADATA, META_ARROW_EXTENSION_NAME};

#[test]
fn arrow_extension_metadata_is_emitted_only_with_its_exact_extension_name() {
    let column = SchemaColumn::new("value".into(), DataType::Binary, false)
        .with_arrow_extension_metadata(
            "transferia.mysql.text_bytes",
            r#"{"version":1,"character_set":"latin1"}"#,
        );
    let metadata = column.arrow_metadata();

    assert_eq!(
        metadata.get(META_ARROW_EXTENSION_NAME).map(String::as_str),
        Some("transferia.mysql.text_bytes")
    );
    assert_eq!(
        metadata
            .get(META_ARROW_EXTENSION_METADATA)
            .map(String::as_str),
        Some(r#"{"version":1,"character_set":"latin1"}"#)
    );
}

#[test]
fn name_only_extensions_do_not_invent_an_empty_metadata_payload() {
    let metadata = SchemaColumn::new("value".into(), DataType::Utf8, false)
        .with_arrow_extension("arrow.json")
        .arrow_metadata();

    assert_eq!(
        metadata.get(META_ARROW_EXTENSION_NAME).map(String::as_str),
        Some("arrow.json")
    );
    assert!(!metadata.contains_key(META_ARROW_EXTENSION_METADATA));
}
