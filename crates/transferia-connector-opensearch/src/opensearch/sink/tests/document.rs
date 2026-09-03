use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn, ARROW_JSON_EXTENSION_NAME};

use super::super::document::{document_shape, encode_batch, DocumentShape};
use super::super::RoutedIdentity;

fn envelope_schema() -> DatasetSchema {
    DatasetSchema::new(vec![
        SchemaColumn::new("_id".to_owned(), DataType::Utf8, false)
            .with_constraints(true, false, Some(512)),
        SchemaColumn::new("_routing".to_owned(), DataType::Utf8, true),
        SchemaColumn::new("_source".to_owned(), DataType::Utf8, false)
            .with_arrow_extension(ARROW_JSON_EXTENSION_NAME),
        SchemaColumn::new("_routing_key".to_owned(), DataType::Utf8, false)
            .with_constraints(true, false, None),
    ])
}

fn batch(fields: Vec<Field>, columns: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

#[test]
fn exact_envelope_preserves_source_bytes_and_routes_in_metadata() {
    let source = r#"{ "z": 1, "a": [true, null] }"#;
    let batch = batch(
        vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("_routing", DataType::Utf8, true),
            Field::new("_source", DataType::Utf8, false),
            Field::new("_routing_key", DataType::Utf8, false),
        ],
        vec![
            Arc::new(StringArray::from(vec!["raw-id"])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![source])),
            Arc::new(StringArray::from(vec!["raw-id"])),
        ],
    );
    assert_eq!(document_shape(&envelope_schema()), DocumentShape::Envelope);
    let actions = encode_batch("logs", &envelope_schema(), &batch, RoutedIdentity::Fail).unwrap();
    let lines = actions[0].ndjson.split(|byte| *byte == b'\n').collect::<Vec<_>>();
    let metadata: serde_json::Value = serde_json::from_slice(lines[0]).unwrap();
    assert_eq!(metadata["index"]["_id"], "raw-id");
    assert!(metadata["index"].get("routing").is_none());
    assert_eq!(lines[1], source.as_bytes());
    assert!(lines[2].is_empty(), "bulk NDJSON must end with a newline");
}

#[test]
fn routed_envelope_requires_explicit_injective_identity_encoding() {
    let batch = batch(
        vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("_routing", DataType::Utf8, true),
            Field::new("_source", DataType::Utf8, false),
            Field::new("_routing_key", DataType::Utf8, false),
        ],
        vec![
            Arc::new(StringArray::from(vec!["same", "same"])),
            Arc::new(StringArray::from(vec![Some("route-a"), Some("route-b")])),
            Arc::new(StringArray::from(vec!["{}", "{}"])),
            Arc::new(StringArray::from(vec!["route-a", "route-b"])),
        ],
    );
    let default_error = encode_batch(
        "logs",
        &envelope_schema(),
        &batch,
        RoutedIdentity::Fail,
    )
    .unwrap_err();
    assert!(default_error.to_string().contains("encode_identity"));

    let actions = encode_batch(
        "logs",
        &envelope_schema(),
        &batch,
        RoutedIdentity::EncodeIdentity,
    )
    .unwrap();
    assert_ne!(actions[0].id, actions[1].id);
    for (action, route) in actions.iter().zip(["route-a", "route-b"]) {
        assert!(action.id.len() <= 512);
        let metadata: serde_json::Value = serde_json::from_slice(
            action
                .ndjson
                .split(|byte| *byte == b'\n')
                .next()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(metadata["index"]["routing"], route);
    }
}

#[test]
fn routed_envelope_rejects_inconsistent_or_unencodable_identity_before_request() {
    for (routing_key, mode) in [
        ("not-the-route".to_owned(), RoutedIdentity::EncodeIdentity),
        ("x".repeat(512), RoutedIdentity::EncodeIdentity),
    ] {
        let batch = batch(
            vec![
                Field::new("_id", DataType::Utf8, false),
                Field::new("_routing", DataType::Utf8, true),
                Field::new("_source", DataType::Utf8, false),
                Field::new("_routing_key", DataType::Utf8, false),
            ],
            vec![
                Arc::new(StringArray::from(vec!["id"])),
                Arc::new(StringArray::from(vec![Some(
                    if routing_key == "not-the-route" {
                        "route"
                    } else {
                        routing_key.as_str()
                    },
                )])),
                Arc::new(StringArray::from(vec!["{}"])),
                Arc::new(StringArray::from(vec![routing_key])),
            ],
        );
        assert!(encode_batch("logs", &envelope_schema(), &batch, mode).is_err());
    }
}

#[test]
fn envelope_preserves_huge_json_numbers_byte_for_byte() {
    let source = concat!(
        "  {\"huge_exponent\":1e400,\"huge_integer\":",
        "123456789012345678901234567890123456789012345678901234567890}  "
    );
    let batch = batch(
        vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("_routing", DataType::Utf8, true),
            Field::new("_source", DataType::Utf8, false),
            Field::new("_routing_key", DataType::Utf8, false),
        ],
        vec![
            Arc::new(StringArray::from(vec!["raw-id"])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![source])),
            Arc::new(StringArray::from(vec!["raw-id"])),
        ],
    );
    let action = &encode_batch("logs", &envelope_schema(), &batch, RoutedIdentity::Fail).unwrap()[0];
    let lines = action.ndjson.split(|byte| *byte == b'\n').collect::<Vec<_>>();
    assert_eq!(lines[1], source.as_bytes());
}

#[test]
fn envelope_rejects_runtime_nulls_before_array_access() {
    for columns in [
        vec![
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("{}")])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("id")])) as ArrayRef,
        ],
        vec![
            Arc::new(StringArray::from(vec![Some("id")])) as ArrayRef,
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("id")])) as ArrayRef,
        ],
        vec![
            Arc::new(StringArray::from(vec![Some("id")])) as ArrayRef,
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("{}")])) as ArrayRef,
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
        ],
    ] {
        let batch = batch(
            vec![
                Field::new("_id", DataType::Utf8, true),
                Field::new("_routing", DataType::Utf8, true),
                Field::new("_source", DataType::Utf8, true),
                Field::new("_routing_key", DataType::Utf8, true),
            ],
            columns,
        );
        assert!(encode_batch("logs", &envelope_schema(), &batch, RoutedIdentity::Fail).is_err());
    }
}

#[test]
fn envelope_rejects_non_object_json_and_oversized_ids() {
    for (id, source) in [("id".to_owned(), "[]"), ("x".repeat(513), "{}")]
    {
        let batch = batch(
            vec![
                Field::new("_id", DataType::Utf8, false),
                Field::new("_routing", DataType::Utf8, true),
                Field::new("_source", DataType::Utf8, false),
                Field::new("_routing_key", DataType::Utf8, false),
            ],
            vec![
                Arc::new(StringArray::from(vec![id])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec![source])),
                Arc::new(StringArray::from(vec!["id"])),
            ],
        );
        assert!(encode_batch("logs", &envelope_schema(), &batch, RoutedIdentity::Fail).is_err());
    }
}

fn composite_schema(first: DataType, second: DataType) -> DatasetSchema {
    DatasetSchema::new(vec![
        SchemaColumn::new("a".to_owned(), first, false).with_constraints(true, false, None),
        SchemaColumn::new("b".to_owned(), second, false).with_constraints(true, false, None),
    ])
}

#[test]
fn composite_ids_are_stable_and_tuple_injective() {
    let schema = composite_schema(DataType::Utf8, DataType::Utf8);
    let fields = vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
    ];
    let one = batch(
        fields.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(StringArray::from(vec!["bc"])),
        ],
    );
    let two = batch(
        fields,
        vec![
            Arc::new(StringArray::from(vec!["ab"])),
            Arc::new(StringArray::from(vec!["c"])),
        ],
    );
    let id = encode_batch("logs", &schema, &one, RoutedIdentity::Fail).unwrap()[0].id.clone();
    assert_eq!(
        id.as_ref(),
        "AAAAAAAAAAR1dGY4AAAAAAAAAAFhAAAAAAAAAAR1dGY4AAAAAAAAAAJiYw"
    );
    assert_ne!(
        id,
        encode_batch("logs", &schema, &two, RoutedIdentity::Fail).unwrap()[0].id
    );

    let ints = batch(
        vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int64, false),
        ],
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![1])),
        ],
    );
    let reverse = batch(
        vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int32, false),
        ],
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![1])),
        ],
    );
    assert_ne!(
        encode_batch(
            "logs",
            &composite_schema(DataType::Int32, DataType::Int64),
            &ints,
            RoutedIdentity::Fail,
        )
        .unwrap()[0]
            .id,
        encode_batch(
            "logs",
            &composite_schema(DataType::Int64, DataType::Int32),
            &reverse,
            RoutedIdentity::Fail,
        )
        .unwrap()[0]
            .id
    );
}

#[test]
fn flat_rows_reject_non_finite_values_before_request() {
    let schema = DatasetSchema::new(vec![
        SchemaColumn::new("id".to_owned(), DataType::Int64, false)
            .with_constraints(true, false, None),
        SchemaColumn::new("value".to_owned(), DataType::Float64, false),
    ]);
    let batch = batch(
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ],
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Float64Array::from(vec![f64::NAN])),
        ],
    );
    assert!(encode_batch("logs", &schema, &batch, RoutedIdentity::Fail).is_err());
}

#[test]
fn flat_custom_routing_requires_explicit_composite_identity_encoding() {
    let schema = DatasetSchema::new(vec![
        SchemaColumn::new("_id".to_owned(), DataType::Utf8, false)
            .with_constraints(true, false, Some(512)),
        SchemaColumn::new("_routing".to_owned(), DataType::Utf8, true),
        SchemaColumn::new("payload".to_owned(), DataType::Utf8, false),
    ]);
    let batch = batch(
        vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("_routing", DataType::Utf8, true),
            Field::new("payload", DataType::Utf8, false),
        ],
        vec![
            Arc::new(StringArray::from(vec!["same", "same"])),
            Arc::new(StringArray::from(vec![Some("route-a"), Some("route-b")])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    );
    assert!(encode_batch("logs", &schema, &batch, RoutedIdentity::Fail).is_err());

    let actions = encode_batch(
        "logs",
        &schema,
        &batch,
        RoutedIdentity::EncodeIdentity,
    )
    .unwrap();
    assert_ne!(actions[0].id, actions[1].id);
    for (action, route) in actions.iter().zip(["route-a", "route-b"]) {
        let metadata: serde_json::Value = serde_json::from_slice(
            action
                .ndjson
                .split(|byte| *byte == b'\n')
                .next()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(metadata["index"]["routing"], route);
    }
}

#[test]
fn explicit_routed_identity_mode_uses_one_disjoint_id_domain_for_flat_rows() {
    let schema = DatasetSchema::new(vec![
        SchemaColumn::new("_id".to_owned(), DataType::Utf8, false)
            .with_constraints(true, false, Some(512)),
        SchemaColumn::new("_routing".to_owned(), DataType::Utf8, true),
    ]);
    let batch = batch(
        vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("_routing", DataType::Utf8, true),
        ],
        vec![
            Arc::new(StringArray::from(vec!["plain", "routed"])),
            Arc::new(StringArray::from(vec![None, Some("route")])),
        ],
    );
    let actions = encode_batch(
        "logs",
        &schema,
        &batch,
        RoutedIdentity::EncodeIdentity,
    )
    .unwrap();
    assert_ne!(actions[0].id.as_ref(), "plain");
    assert_ne!(actions[1].id.as_ref(), "routed");
    assert_ne!(actions[0].id, actions[1].id);
}
