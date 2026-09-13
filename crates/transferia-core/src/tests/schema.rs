use arrow::datatypes::DataType;

use super::{SchemaColumn, META_ARROW_EXTENSION_METADATA, META_ARROW_EXTENSION_NAME};

#[test]
fn native_source_declaration_is_preserved_but_not_a_wire_or_compatibility_constraint() {
    let plain = SchemaColumn::new("value".into(), DataType::Utf8, false);
    let native = plain.clone().with_source_type("public.amount_domain");
    assert_eq!(native.clone().source_type.as_deref(), Some("public.amount_domain"));
    assert_eq!(plain, native);
    assert_eq!(plain.arrow_metadata(), native.arrow_metadata());
}

#[test]
fn update_presence_is_an_explicit_guarantee_independent_of_nullability() {
    let mut column = SchemaColumn::new("id".into(), DataType::Int32, true);
    let key = super::META_ALWAYS_PRESENT_ON_UPDATE;
    assert!(!column.arrow_metadata().contains_key(key));
    column.always_present_on_update = true;
    assert_eq!(column.arrow_metadata().get(key).map(String::as_str), Some("true"));
    assert!(column.nullable);
    assert!(column.clone().always_present_on_update);
}

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
