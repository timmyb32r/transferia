use arrow::datatypes::DataType;

use super::{SchemaColumn, META_ARROW_EXTENSION_METADATA, META_ARROW_EXTENSION_NAME};

#[test]
fn normalized_presence_is_separate_from_wire_presence_and_nullability() {
    use super::{ValuePresence, META_DELETE_VALUE_PRESENCE, META_UPDATE_VALUE_PRESENCE};
    let base = SchemaColumn::new("text".into(), DataType::Utf8, true);
    assert_eq!(base.update_value_presence, ValuePresence::MayBeAbsent);
    assert!(!base
        .arrow_metadata()
        .contains_key(META_UPDATE_VALUE_PRESENCE));
    let mut full = base.clone();
    full.update_value_presence = ValuePresence::Guaranteed;
    full.delete_value_presence = ValuePresence::Guaranteed;
    assert_ne!(base, full);
    assert!(full.nullable);
    assert!(!full.always_present_on_update);
    assert_eq!(full, full.clone());
    for key in [META_UPDATE_VALUE_PRESENCE, META_DELETE_VALUE_PRESENCE] {
        assert_eq!(
            full.arrow_metadata().get(key).map(String::as_str),
            Some("guaranteed")
        );
    }
}

#[test]
fn native_source_declaration_is_preserved_but_not_a_wire_or_compatibility_constraint() {
    let plain = SchemaColumn::new("value".into(), DataType::Utf8, false);
    let native = plain.clone().with_source_type("public.amount_domain");
    assert_eq!(native.source_type.as_deref(), Some("public.amount_domain"));
    assert_eq!(plain, native);
    assert_eq!(plain.arrow_metadata(), native.arrow_metadata());
}

#[test]
fn update_presence_is_an_explicit_guarantee_independent_of_nullability() {
    let mut column = SchemaColumn::new("id".into(), DataType::Int32, true);
    let key = super::META_ALWAYS_PRESENT_ON_UPDATE;
    assert!(!column.arrow_metadata().contains_key(key));
    column.always_present_on_update = true;
    assert_eq!(
        column.arrow_metadata().get(key).map(String::as_str),
        Some("true")
    );
    assert!(column.nullable);
    assert!(column.always_present_on_update);
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

#[test]
fn arrow_schema_round_trip_preserves_the_runtime_contract() -> anyhow::Result<()> {
    use std::sync::Arc;
    use arrow::datatypes::{Field, Schema};
    use super::{DatasetSchema, ValuePresence};
    let mut column = SchemaColumn::new("payload".into(), DataType::Binary, true)
        .with_constraints(true, true, Some(1024))
        .with_arrow_extension_metadata(String::from("runtime.extension"), "exact metadata")
        .with_system_role("source.version");
    column.always_present_on_update = true;
    column.update_value_presence = ValuePresence::Guaranteed;
    column.delete_value_presence = ValuePresence::Guaranteed;
    column.old_value_of = Some("current_payload".into());
    column.old_key_of = Some("current_key".into());
    let arrow = Schema::new(vec![Arc::new(
        Field::new(&column.name, column.data_type.clone(), column.nullable)
            .with_metadata(column.arrow_metadata()),
    )]);
    assert_eq!(DatasetSchema::from_arrow(&arrow)?, DatasetSchema::new(vec![column]));
    Ok(())
}

#[test]
fn malformed_arrow_contract_is_rejected_at_table_construction() {
    use std::sync::Arc;
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    for (key, value) in [
        (super::META_PRIMARY_KEY, "not-a-boolean"),
        (super::META_UPDATE_VALUE_PRESENCE, "sometimes"),
        (super::META_MAX_LENGTH, "negative"),
        (super::META_ARROW_EXTENSION_METADATA, "missing extension name"),
    ] {
        let field = Field::new("value", DataType::Int64, false)
            .with_metadata([(key.to_owned(), value.to_owned())].into());
        let batch = RecordBatch::new_empty(Arc::new(Schema::new(vec![field])));
        assert!(super::DatasetSchema::from_arrow(batch.schema().as_ref()).is_err());
        assert!(crate::TableData::new(Arc::from("events"), false, batch, crate::SystemColumns::default()).is_err());
    }
}
