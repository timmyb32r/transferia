#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test assertions intentionally fail fast"
)]

use std::sync::Arc;

use transferia_core::ChangeOperation;
use ydb_grpc::ydb_proto::r#type::{PrimitiveTypeId, Type as TypeKind};
use ydb_grpc::ydb_proto::table::ColumnMeta;
use ydb_grpc::ydb_proto::{DecimalType, OptionalType, Type};

use super::super::decoder::{YdbCdcDecoder, YdbCdcValue};
use crate::ydb::types::column_plans;

fn primitive(primitive: PrimitiveTypeId) -> Type {
    Type {
        r#type: Some(TypeKind::TypeId(primitive as i32)),
    }
}

fn decimal(precision: u32, scale: u32) -> Type {
    Type {
        r#type: Some(TypeKind::DecimalType(DecimalType { precision, scale })),
    }
}

fn optional(item: Type) -> Type {
    Type {
        r#type: Some(TypeKind::OptionalType(Box::new(OptionalType {
            item: Some(Box::new(item)),
        }))),
    }
}

fn column(name: &str, r#type: Type, not_null: Option<bool>) -> ColumnMeta {
    ColumnMeta {
        name: name.to_owned(),
        r#type: Some(r#type),
        family: String::new(),
        not_null,
        default_value: None,
    }
}

fn decoder(
    columns: Vec<ColumnMeta>,
    primary_key: &[&str],
    max_event_bytes: usize,
) -> anyhow::Result<YdbCdcDecoder> {
    let primary_key = primary_key
        .iter()
        .map(|value| (*value).to_owned())
        .collect::<Vec<_>>();
    YdbCdcDecoder::new(
        Arc::from(column_plans(columns, &primary_key)?),
        max_event_bytes,
    )
}

#[test]
fn create_preserves_composite_key_order_null_mask_and_transaction_identity(
) -> anyhow::Result<()> {
    let decoder = decoder(
        vec![
            column("payload", optional(primitive(PrimitiveTypeId::Utf8)), None),
            column("region", primitive(PrimitiveTypeId::Utf8), Some(true)),
            column("id", primitive(PrimitiveTypeId::Uint64), Some(true)),
            column("untouched", optional(primitive(PrimitiveTypeId::Int64)), None),
        ],
        &["id", "region"],
        1_024,
    )?;
    let event = decoder.decode(
        br#"{"key":[18446744073709551615,"eu"],"update":{},"newImage":{"payload":null,"untouched":42},"ts":[72623859790382856,1230066625199609624]}"#,
    )?;

    assert_eq!(event.operation, ChangeOperation::Create);
    assert_eq!(
        event.current,
        vec![
            YdbCdcValue::Null,
            YdbCdcValue::Utf8("eu".to_owned()),
            YdbCdcValue::UInt64(u64::MAX),
            YdbCdcValue::Int64(42),
        ]
    );
    assert_eq!(
        event.old,
        vec![
            YdbCdcValue::Absent,
            YdbCdcValue::Absent,
            YdbCdcValue::Absent,
            YdbCdcValue::Absent,
        ]
    );
    assert_eq!(event.changed_columns, vec![0b0000_1111]);
    assert_eq!(event.transaction.step(), 0x0102_0304_0506_0708);
    assert_eq!(event.transaction.transaction_id(), 0x1112_1314_1516_1718);
    assert_eq!(
        event.transaction.as_bytes(),
        &[
            1, 2, 3, 4, 5, 6, 7, 8, 17, 18, 19, 20, 21, 22, 23, 24,
        ]
    );
    Ok(())
}

#[test]
fn full_images_preserve_schema_order_binary_yson_and_old_values() -> anyhow::Result<()> {
    let decoder = decoder(
        vec![
            column("id", primitive(PrimitiveTypeId::Uint64), Some(true)),
            column("bytes", primitive(PrimitiveTypeId::String), Some(true)),
            column("yson", primitive(PrimitiveTypeId::Yson), Some(true)),
        ],
        &["id"],
        2_048,
    )?;
    let event = decoder.decode(
        br#"{"key":[9],"update":{},"newImage":{"yson":"AP8=","bytes":"aGVsbG8="},"oldImage":{"bytes":"b2xk","yson":"e30="},"ts":[44,55]}"#,
    )?;

    assert_eq!(event.operation, ChangeOperation::Update);
    assert_eq!(
        event.current,
        vec![
            YdbCdcValue::UInt64(9),
            YdbCdcValue::Binary(b"hello".to_vec()),
            YdbCdcValue::Binary(vec![0, 255]),
        ]
    );
    assert_eq!(
        event.old,
        vec![
            YdbCdcValue::UInt64(9),
            YdbCdcValue::Binary(b"old".to_vec()),
            YdbCdcValue::Binary(b"{}".to_vec()),
        ]
    );
    assert_eq!(event.changed_columns, vec![0b0000_0111]);
    Ok(())
}

#[test]
fn temporal_uuid_and_json_values_match_snapshot_logical_representations() -> anyhow::Result<()> {
    let decoder = decoder(
        vec![
            column("id", primitive(PrimitiveTypeId::Uint64), Some(true)),
            column("day", primitive(PrimitiveTypeId::Date32), Some(true)),
            column(
                "observed_second",
                primitive(PrimitiveTypeId::Datetime64),
                Some(true),
            ),
            column(
                "observed_micro",
                primitive(PrimitiveTypeId::Timestamp64),
                Some(true),
            ),
            column(
                "elapsed",
                primitive(PrimitiveTypeId::Interval64),
                Some(true),
            ),
            column("event_id", primitive(PrimitiveTypeId::Uuid), Some(true)),
            column("document", primitive(PrimitiveTypeId::JsonDocument), Some(true)),
        ],
        &["id"],
        4_096,
    )?;
    let event = decoder.decode(
        br#"{"key":[1],"update":{},"newImage":{"document":{"b":2,"a":[true,null]},"event_id":"12345678-1234-4abc-89ab-1234567890ab","elapsed":-9223372036854775808,"observed_micro":"1970-01-01T00:00:00.000001Z","observed_second":"1969-12-31T23:59:59.000000Z","day":"1970-01-01T00:00:00.000000Z"},"ts":[1,2]}"#,
    )?;

    assert_eq!(event.current[1], YdbCdcValue::Date32(0));
    assert_eq!(event.current[2], YdbCdcValue::TimestampSecond(-1));
    assert_eq!(event.current[3], YdbCdcValue::TimestampMicrosecond(1));
    assert_eq!(
        event.current[4],
        YdbCdcValue::DurationMicrosecond(i64::MIN)
    );
    assert_eq!(
        event.current[5],
        YdbCdcValue::Uuid(
            uuid::Uuid::parse_str("12345678-1234-4abc-89ab-1234567890ab")?.into_bytes()
        )
    );
    assert_eq!(
        event.current[6],
        YdbCdcValue::Utf8(r#"{"b":2,"a":[true,null]}"#.to_owned())
    );
    Ok(())
}

#[test]
fn strict_envelope_rejects_duplicates_unknowns_conflicts_bad_timestamp_and_oversize(
) -> anyhow::Result<()> {
    let decoder = decoder(
        vec![
            column("id", primitive(PrimitiveTypeId::Uint64), Some(true)),
            column("value", primitive(PrimitiveTypeId::Utf8), Some(true)),
        ],
        &["id"],
        128,
    )?;
    let invalid = [
        br#"{"key":[1],"update":{},"newImage":{"value":"a"},"ts":[1,2],"ts":[1,2]}"#.as_slice(),
        br#"{"key":[1],"update":{},"newImage":{"value":"a"},"unknown":0,"ts":[1,2]}"#.as_slice(),
        br#"{"key":[1],"update":{},"erase":{},"newImage":{"value":"a"},"ts":[1,2]}"#.as_slice(),
        br#"{"key":[1],"update":{},"newImage":{"value":"a"},"ts":[1]}"#.as_slice(),
        br#"{"key":[1],"update":{},"newImage":{"value":"a","value":"b"},"ts":[1,2]}"#.as_slice(),
        br#"{"key":[1],"update":{},"newImage":{"missing":"a"},"ts":[1,2]}"#.as_slice(),
        br#"{"key":[1],"update":{"value":"a"},"newImage":{"value":"a"},"ts":[1,2]}"#.as_slice(),
        br#"{"key":[1],"update":{},"ts":[1,2]}"#.as_slice(),
        br#"{"key":[1],"erase":{},"ts":[1,2]}"#.as_slice(),
        br#"{"key":[1],"newImage":{"value":"a"},"ts":[1,2]}"#.as_slice(),
    ];
    for payload in invalid {
        assert!(decoder.decode(payload).is_err(), "accepted {payload:?}");
    }
    assert!(decoder.decode(&vec![b' '; 129]).is_err());
    Ok(())
}

#[test]
fn invalid_base64_decimal_temporal_uuid_and_nested_json_fail_closed() -> anyhow::Result<()> {
    let cases = [
        (
            column("value", primitive(PrimitiveTypeId::String), Some(true)),
            r#""AB==""#,
        ),
        (
            column("value", primitive(PrimitiveTypeId::Date32), Some(true)),
            r#""2023-02-29T00:00:00.000000Z""#,
        ),
        (
            column("value", primitive(PrimitiveTypeId::Date32), Some(true)),
            r#""1970-01-01""#,
        ),
        (
            column(
                "value",
                primitive(PrimitiveTypeId::Datetime64),
                Some(true),
            ),
            r#""1970-01-01T00:00:00.000001Z""#,
        ),
        (
            column(
                "value",
                primitive(PrimitiveTypeId::Datetime64),
                Some(true),
            ),
            r#""1970-01-01T00:00:00Z""#,
        ),
        (
            column("value", primitive(PrimitiveTypeId::Timestamp64), Some(true)),
            r#""1970-01-01T00:00:00.0000001Z""#,
        ),
        (
            column("value", primitive(PrimitiveTypeId::Timestamp64), Some(true)),
            r#""1970-01-01T00:00:00.000001""#,
        ),
        (
            column("value", primitive(PrimitiveTypeId::Timestamp64), Some(true)),
            r#""1970-01-01T03:00:00.000001+03:00""#,
        ),
        (
            column("value", primitive(PrimitiveTypeId::Uuid), Some(true)),
            r#""not-a-uuid""#,
        ),
        (
            column("value", primitive(PrimitiveTypeId::Json), Some(true)),
            r#"{"duplicate":1,"duplicate":2}"#,
        ),
    ];
    for (value_column, value) in cases {
        let decoder = decoder(
            vec![
                column("id", primitive(PrimitiveTypeId::Uint64), Some(true)),
                value_column,
            ],
            &["id"],
            1_024,
        )?;
        let payload = format!(
            r#"{{"key":[1],"update":{{}},"newImage":{{"value":{value}}},"ts":[1,2]}}"#
        );
        assert!(
            decoder.decode(payload.as_bytes()).is_err(),
            "accepted {payload}"
        );
    }
    Ok(())
}

#[test]
fn nullable_json_is_rejected_before_runtime_decoding() -> anyhow::Result<()> {
    let columns = column_plans(
        vec![
            column("id", primitive(PrimitiveTypeId::Uint64), Some(true)),
            column(
                "document",
                optional(primitive(PrimitiveTypeId::JsonDocument)),
                None,
            ),
        ],
        &["id".to_owned()],
    )?;
    let error = YdbCdcDecoder::new(Arc::from(columns), 1_024)
        .err()
        .expect("nullable JSON must be rejected");
    assert!(error.to_string().contains("cannot distinguish SQL NULL from JSON null"));
    Ok(())
}

#[test]
fn decimal_is_rejected_before_runtime_decoding() -> anyhow::Result<()> {
    let columns = column_plans(
        vec![
            column("id", primitive(PrimitiveTypeId::Uint64), Some(true)),
            column("amount", decimal(22, 3), Some(true)),
        ],
        &["id".to_owned()],
    )?;
    let error = YdbCdcDecoder::new(Arc::from(columns), 1_024)
        .err()
        .expect("Decimal CDC must be rejected before streaming");
    assert!(error.to_string().contains("Decimal special values"));
    Ok(())
}

#[test]
fn physically_distinct_date_aliases_do_not_share_cdc_validation() -> anyhow::Result<()> {
    let payload = br#"{"key":[1],"update":{},"newImage":{"day":"1969-12-31T00:00:00.000000Z"},"ts":[1,2]}"#;
    let narrow = decoder(
        vec![
            column("id", primitive(PrimitiveTypeId::Uint64), Some(true)),
            column("day", primitive(PrimitiveTypeId::Date), Some(true)),
        ],
        &["id"],
        1_024,
    )?;
    assert!(narrow.decode(payload).is_err());

    let wide = decoder(
        vec![
            column("id", primitive(PrimitiveTypeId::Uint64), Some(true)),
            column("day", primitive(PrimitiveTypeId::Date32), Some(true)),
        ],
        &["id"],
        1_024,
    )?;
    assert_eq!(wide.decode(payload)?.current[1], YdbCdcValue::Date32(-1));
    Ok(())
}

#[test]
fn replacement_flag_preserves_create_and_update_identity() -> anyhow::Result<()> {
    let decoder = decoder(
        vec![
            column("id", primitive(PrimitiveTypeId::Uint64), Some(true)),
            column("value", optional(primitive(PrimitiveTypeId::Utf8)), None),
        ],
        &["id"],
        512,
    )?;
    let created = decoder.decode(
        br#"{"key":[1],"reset":{},"newImage":{"value":"created"},"ts":[1,2]}"#,
    )?;
    assert_eq!(created.operation, ChangeOperation::Create);
    assert!(created.old.iter().all(|value| matches!(value, YdbCdcValue::Absent)));

    let updated = decoder.decode(
        br#"{"key":[1],"reset":{},"newImage":{"value":"new"},"oldImage":{"value":"old"},"ts":[3,4]}"#,
    )?;
    assert_eq!(updated.operation, ChangeOperation::Update);
    assert_eq!(updated.old[1], YdbCdcValue::Utf8("old".to_owned()));
    Ok(())
}

#[test]
fn decode_admission_is_checked_and_covers_row_slots_beyond_the_payload() -> anyhow::Result<()> {
    let decoder = decoder(
        vec![
            column("id", primitive(PrimitiveTypeId::Uint64), Some(true)),
            column("value", primitive(PrimitiveTypeId::Utf8), Some(true)),
        ],
        &["id"],
        1_024,
    )?;
    assert!(decoder.decode_admission_bytes(1)? > 2 * std::mem::size_of::<YdbCdcValue>());
    assert!(decoder.decode_admission_bytes(usize::MAX).is_err());
    Ok(())
}

#[test]
fn large_unique_json_objects_decode_without_quadratic_duplicate_scans() -> anyhow::Result<()> {
    let decoder = decoder(
        vec![
            column("id", primitive(PrimitiveTypeId::Uint64), Some(true)),
            column("document", primitive(PrimitiveTypeId::JsonDocument), Some(true)),
        ],
        &["id"],
        2 * 1024 * 1024,
    )?;
    let fields = (0..10_000)
        .map(|index| format!(r#""key_{index}":{index}"#))
        .collect::<Vec<_>>()
        .join(",");
    let payload = format!(
        r#"{{"key":[1],"update":{{}},"newImage":{{"document":{{{fields}}}}},"ts":[1,2]}}"#
    );
    let event = decoder.decode(payload.as_bytes())?;
    assert_eq!(event.operation, ChangeOperation::Create);
    Ok(())
}

#[test]
fn extreme_calendar_years_fail_without_overflowing() -> anyhow::Result<()> {
    let decoder = decoder(
        vec![
            column("id", primitive(PrimitiveTypeId::Uint64), Some(true)),
            column("day", primitive(PrimitiveTypeId::Date32), Some(true)),
        ],
        &["id"],
        1_024,
    )?;
    for year in [i64::MIN, i64::MAX] {
        let payload = format!(
            r#"{{"key":[1],"update":{{}},"newImage":{{"day":"{year}-01-01T00:00:00.000000Z"}},"ts":[1,2]}}"#
        );
        assert!(decoder.decode(payload.as_bytes()).is_err());
    }
    Ok(())
}

#[test]
fn deletes_require_only_the_positional_key_and_mark_key_columns() -> anyhow::Result<()> {
    let decoder = decoder(
        vec![
            column("value", primitive(PrimitiveTypeId::Utf8), Some(true)),
            column("id", primitive(PrimitiveTypeId::Uint64), Some(true)),
        ],
        &["id"],
        256,
    )?;
    let event = decoder.decode(
        br#"{"key":[7],"erase":{},"oldImage":{"value":"old"},"ts":[8,9]}"#,
    )?;
    assert_eq!(event.operation, ChangeOperation::Delete);
    assert_eq!(
        event.current,
        vec![YdbCdcValue::Absent, YdbCdcValue::UInt64(7)]
    );
    assert_eq!(
        event.old,
        vec![
            YdbCdcValue::Utf8("old".to_owned()),
            YdbCdcValue::UInt64(7),
        ]
    );
    assert_eq!(event.changed_columns, vec![0b0000_0010]);
    Ok(())
}
