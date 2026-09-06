use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn table_sample_select_quotes_identifiers_and_limits_rows_in_database() {
    let table = transferia_registry::TableIdentity { namespace: "some\"schema".into(), name: "events\"; DROP TABLE x; --".into() };
    assert_eq!(super::sample::sample_query(&table, "\"id\"", 7).unwrap(),
        "SELECT \"id\" FROM \"some\"\"schema\".\"events\"\"; DROP TABLE x; --\" LIMIT 7");
    assert!(super::sample::sample_query(&table, "\"id\"", 0).is_err());
}

use super::copy_out::{CopyDecoder, DecodeState};
use super::reader::{
    decode_date, decode_i8, decode_timestamp, decode_timestamptz, discovered_schema_matches,
    source_column_expression, source_user_field,
};
use super::snapshot::{close_owner_with_timeout, set_snapshot_sql};
use arrow::datatypes::{DataType, TimeUnit};
use bytes::Bytes;
use tokio_postgres::types::{Kind, Type};

use crate::connectors::postgres::common::postgres_to_arrow;
use crate::connectors::postgres::source::POSTGRES_SOURCE_METADATA_COLUMNS;
use crate::connectors::postgres::PostgresCopyFormat;
use transferia_core::data::schema::{
    SchemaColumn, META_LOW_CARDINALITY, META_MAX_LENGTH, META_PRIMARY_KEY,
    SYSTEM_ROLE_EVENT_TIMESTAMP_MS, SYSTEM_ROLE_EVENT_TIMESTAMP_NS, SYSTEM_ROLE_EVENT_TIMESTAMP_US,
    SYSTEM_ROLE_SOURCE_DATABASE, SYSTEM_ROLE_SOURCE_SCHEMA, SYSTEM_ROLE_SOURCE_TABLE,
    SYSTEM_ROLE_SOURCE_TIMESTAMP_MS, SYSTEM_ROLE_SOURCE_TIMESTAMP_NS,
    SYSTEM_ROLE_SOURCE_TIMESTAMP_US, SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
};

#[test]
fn imported_snapshot_sql_accepts_a_real_postgres_snapshot_identifier() {
    assert_eq!(
        set_snapshot_sql("00000003-0000001B-1").unwrap(),
        "SET TRANSACTION SNAPSHOT '00000003-0000001B-1'"
    );
}

#[test]
fn imported_snapshot_sql_rejects_every_non_literal_identifier_shape() {
    let too_long = "a".repeat(129);
    for invalid in [
        "",
        too_long.as_str(),
        "00000003-0000001B-'1",
        " 00000003-0000001B-1",
        "00000003-0000001B-1 ",
        "00000003-0000 01B-1",
        "00000003-0000001G-1",
        "00000003/0000001B/1",
    ] {
        assert!(
            set_snapshot_sql(invalid).is_err(),
            "unsafe snapshot identifier was accepted: {invalid:?}"
        );
    }
}

#[tokio::test]
async fn hung_snapshot_owner_cleanup_is_bounded_and_force_drops_the_owner() {
    struct DropProbe(Arc<AtomicBool>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    let dropped = Arc::new(AtomicBool::new(false));
    let cleanup = close_owner_with_timeout(
        DropProbe(Arc::clone(&dropped)),
        |_| Box::pin(std::future::pending()),
        Duration::from_millis(1),
    );
    let error = tokio::time::timeout(Duration::from_secs(1), cleanup)
        .await
        .expect("snapshot owner cleanup must return within its deadline")
        .expect_err("a hung rollback must fail explicitly");

    assert!(error.to_string().contains("cleanup timed out"));
    assert!(
        dropped.load(Ordering::Acquire),
        "the connection owner must be force-dropped after the cleanup deadline"
    );
}

#[test]
fn snapshot_and_replication_share_one_stable_source_metadata_schema() {
    let expected = [
        (
            "_system_source_database",
            SYSTEM_ROLE_SOURCE_DATABASE,
            DataType::Utf8,
        ),
        (
            "_system_source_schema",
            SYSTEM_ROLE_SOURCE_SCHEMA,
            DataType::Utf8,
        ),
        (
            "_system_source_table",
            SYSTEM_ROLE_SOURCE_TABLE,
            DataType::Utf8,
        ),
        (
            "_system_source_transaction_id",
            SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
            DataType::UInt64,
        ),
        (
            "_system_source_timestamp_ms",
            SYSTEM_ROLE_SOURCE_TIMESTAMP_MS,
            DataType::Int64,
        ),
        (
            "_system_source_timestamp_us",
            SYSTEM_ROLE_SOURCE_TIMESTAMP_US,
            DataType::Int64,
        ),
        (
            "_system_source_timestamp_ns",
            SYSTEM_ROLE_SOURCE_TIMESTAMP_NS,
            DataType::Int64,
        ),
        (
            "_system_event_timestamp_ms",
            SYSTEM_ROLE_EVENT_TIMESTAMP_MS,
            DataType::Int64,
        ),
        (
            "_system_event_timestamp_us",
            SYSTEM_ROLE_EVENT_TIMESTAMP_US,
            DataType::Int64,
        ),
        (
            "_system_event_timestamp_ns",
            SYSTEM_ROLE_EVENT_TIMESTAMP_NS,
            DataType::Int64,
        ),
    ];
    let actual = POSTGRES_SOURCE_METADATA_COLUMNS
        .iter()
        .map(|column| (column.name, column.role, column.data_type.clone()))
        .collect::<Vec<_>>();

    assert_eq!(actual, expected);
}

#[test]
fn empty_snapshot_table_schema_validation_rejects_order_type_and_nullability_drift() {
    let expected = transferia_core::data::schema::DatasetSchema::new(vec![
        SchemaColumn::new("id".to_owned(), DataType::Int64, false)
            .with_constraints(true, false, None),
        SchemaColumn::new("payload".to_owned(), DataType::Utf8, true),
    ]);
    assert!(discovered_schema_matches(&expected, &expected));

    let mut reordered = expected.clone();
    reordered.columns.swap(0, 1);
    assert!(!discovered_schema_matches(&reordered, &expected));

    let mut changed_type = expected.clone();
    changed_type.columns[1].data_type = DataType::Binary;
    assert!(!discovered_schema_matches(&changed_type, &expected));

    let mut changed_nullability = expected.clone();
    changed_nullability.columns[0].nullable = true;
    assert!(!discovered_schema_matches(&changed_nullability, &expected));
}

#[test]
fn binary_copy_decoder_handles_fragmented_header_rows_nulls_and_trailer() {
    let mut wire = Vec::new();
    wire.extend_from_slice(b"PGCOPY\n\xFF\r\n\0");
    wire.extend_from_slice(&0_i32.to_be_bytes());
    wire.extend_from_slice(&0_i32.to_be_bytes());
    wire.extend_from_slice(&2_i16.to_be_bytes());
    wire.extend_from_slice(&4_i32.to_be_bytes());
    wire.extend_from_slice(&7_i32.to_be_bytes());
    wire.extend_from_slice(&(-1_i32).to_be_bytes());
    wire.extend_from_slice(&(-1_i16).to_be_bytes());

    let mut decoder = CopyDecoder::new(PostgresCopyFormat::Binary, 2);
    for chunk in wire.chunks(3) {
        decoder.push(chunk).unwrap();
    }
    let DecodeState::Row(row) = decoder.next().unwrap() else {
        panic!("expected one decoded row");
    };
    assert_eq!(
        row.fields[0].as_deref(),
        Some(7_i32.to_be_bytes().as_slice())
    );
    assert_eq!(row.fields[1], None);
    assert!(matches!(decoder.next().unwrap(), DecodeState::End));
    decoder.finish().unwrap();
}

#[test]
fn binary_copy_decoder_rejects_partial_or_malformed_frames() {
    let mut partial = CopyDecoder::new(PostgresCopyFormat::Binary, 1);
    partial.push(b"PGCOPY\n\xFF\r\n\0").unwrap();
    assert!(partial.finish().is_err());

    let mut invalid = CopyDecoder::new(PostgresCopyFormat::Binary, 1);
    let mut wire = Vec::from(b"PGCOPY\n\xFF\r\n\0".as_slice());
    wire.extend_from_slice(&1_i32.to_be_bytes());
    wire.extend_from_slice(&0_i32.to_be_bytes());
    invalid.push(&wire).unwrap();
    assert!(invalid.next().is_err());
}

#[test]
fn text_copy_decoder_preserves_escapes_and_distinguishes_null() {
    let mut decoder = CopyDecoder::new(PostgresCopyFormat::Text, 3);
    decoder.push(b"hello\\tworld\t\\N\t\\\\N\\nline").unwrap();
    assert!(matches!(decoder.next().unwrap(), DecodeState::NeedMore));
    decoder.push(b"\\\\tail\n").unwrap();
    let DecodeState::Row(row) = decoder.next().unwrap() else {
        panic!("expected one decoded text row");
    };
    assert_eq!(row.fields[0], Some(Bytes::from_static(b"hello\tworld")));
    assert_eq!(row.fields[1], None);
    assert_eq!(row.fields[2], Some(Bytes::from_static(b"\\N\nline\\tail")));
    decoder.finish().unwrap();
    assert!(matches!(decoder.next().unwrap(), DecodeState::End));
}

#[test]
fn text_copy_decoder_supports_octal_and_hex_and_rejects_bad_shape() {
    let mut decoder = CopyDecoder::new(PostgresCopyFormat::Text, 1);
    decoder.push(b"a\\101\\x42\n").unwrap();
    let DecodeState::Row(row) = decoder.next().unwrap() else {
        panic!("expected one decoded text row");
    };
    assert_eq!(row.fields[0], Some(Bytes::from_static(b"aAB")));

    let mut wrong_columns = CopyDecoder::new(PostgresCopyFormat::Text, 2);
    wrong_columns.push(b"one\n").unwrap();
    assert!(wrong_columns.next().is_err());

    let mut partial = CopyDecoder::new(PostgresCopyFormat::Text, 1);
    partial.push(b"unterminated").unwrap();
    assert!(partial.finish().is_err());
}

#[test]
fn text_copy_char_decoder_covers_the_complete_postgres_internal_char_domain() {
    for byte in u8::MIN..=u8::MAX {
        let text = match byte {
            0 => Vec::new(),
            1..=127 => vec![byte],
            _ => format!("\\{byte:03o}").into_bytes(),
        };
        assert_eq!(
            decode_i8(&text, PostgresCopyFormat::Text).unwrap(),
            i8::from_ne_bytes([byte])
        );
    }
    assert!(decode_i8(b"128", PostgresCopyFormat::Text).is_err());
    assert!(decode_i8(b"\\400", PostgresCopyFormat::Text).is_err());
}

#[test]
fn postgres_types_use_native_arrow_where_lossless_and_canonical_text_otherwise() {
    for (postgres, arrow) in [
        (Type::BOOL, DataType::Boolean),
        (Type::CHAR, DataType::Int8),
        (Type::INT2, DataType::Int16),
        (Type::INT4, DataType::Int32),
        (Type::INT8, DataType::Int64),
        (Type::OID, DataType::UInt32),
        (Type::FLOAT4, DataType::Float32),
        (Type::FLOAT8, DataType::Float64),
        (Type::BYTEA, DataType::Binary),
        (Type::TEXT, DataType::Utf8),
        (Type::VARCHAR, DataType::Utf8),
        (Type::BPCHAR, DataType::Utf8),
        (Type::NAME, DataType::Utf8),
        (Type::DATE, DataType::Date32),
        (
            Type::TIMESTAMP,
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ),
        (
            Type::TIMESTAMPTZ,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ),
    ] {
        assert_eq!(postgres_to_arrow(&postgres).unwrap(), arrow);
        assert_eq!(
            source_column_expression("mixed\"case", &postgres, crate::connectors::postgres::source::UnsupportedTypePolicy::Fail).unwrap(),
            "\"mixed\"\"case\""
        );
    }

    for postgres in [
        Type::NUMERIC,
        Type::MONEY,
        Type::TIME,
        Type::TIMETZ,
        Type::INTERVAL,
        Type::JSON,
        Type::JSONB,
        Type::XML,
        Type::UUID,
        Type::INET,
        Type::CIDR,
        Type::MACADDR,
        Type::MACADDR8,
        Type::BIT,
        Type::VARBIT,
        Type::POINT,
        Type::LINE,
        Type::LSEG,
        Type::BOX,
        Type::PATH,
        Type::POLYGON,
        Type::CIRCLE,
        Type::INT4_ARRAY,
        Type::INT4_RANGE,
        Type::INT4MULTI_RANGE,
    ] {
        assert_eq!(postgres_to_arrow(&postgres).unwrap(), DataType::Utf8);
        assert_eq!(
            source_column_expression("value", &postgres, crate::connectors::postgres::source::UnsupportedTypePolicy::Fail).unwrap(),
            "\"value\"::text AS \"value\""
        );
    }
}

#[test]
fn temporal_copy_decoders_preserve_types_epochs_offsets_and_microseconds() {
    for (text, postgres_days, unix_days) in [
        ("1970-01-01", -10_957_i32, 0_i32),
        ("2000-01-01", 0, 10_957),
        ("0001-01-01 BC", -730_485, -719_528),
    ] {
        assert_eq!(
            decode_date(text.as_bytes(), PostgresCopyFormat::Text).unwrap(),
            unix_days
        );
        assert_eq!(
            decode_date(&postgres_days.to_be_bytes(), PostgresCopyFormat::Binary).unwrap(),
            unix_days
        );
    }

    let expected = 1_704_067_200_123_456_i64;
    let postgres = expected - 946_684_800_000_000;
    assert_eq!(
        decode_timestamp(b"2024-01-01 00:00:00.123456", PostgresCopyFormat::Text,).unwrap(),
        expected
    );
    assert_eq!(
        decode_timestamp(&postgres.to_be_bytes(), PostgresCopyFormat::Binary).unwrap(),
        expected
    );
    assert_eq!(
        decode_timestamptz(b"2024-01-01 03:00:00.123456+03", PostgresCopyFormat::Text,).unwrap(),
        expected
    );
    assert_eq!(
        decode_timestamptz(&postgres.to_be_bytes(), PostgresCopyFormat::Binary).unwrap(),
        expected
    );
}

#[test]
fn temporal_copy_decoders_fail_closed_on_infinity_precision_and_bad_offsets() {
    for value in [i32::MIN, i32::MAX] {
        assert!(decode_date(&value.to_be_bytes(), PostgresCopyFormat::Binary).is_err());
    }
    for value in [i64::MIN, i64::MAX] {
        assert!(decode_timestamp(&value.to_be_bytes(), PostgresCopyFormat::Binary).is_err());
    }
    assert!(decode_date(b"infinity", PostgresCopyFormat::Text).is_err());
    assert!(decode_timestamp(b"2024-01-01 00:00:00.1234567", PostgresCopyFormat::Text,).is_err());
    assert!(decode_timestamptz(b"2024-01-01 00:00:00", PostgresCopyFormat::Text,).is_err());
}

#[test]
fn snapshot_arrow_fields_preserve_discovered_column_constraints() {
    let discovered = SchemaColumn::new("id".to_owned(), DataType::Int64, true).with_constraints(
        true,
        true,
        Some(64),
    );

    let field = source_user_field(&discovered, false);

    assert_eq!(field.name(), "id");
    assert_eq!(field.data_type(), &DataType::Int64);
    assert!(field.is_nullable());
    assert_eq!(
        field.metadata().get(META_PRIMARY_KEY).map(String::as_str),
        Some("true")
    );
    assert_eq!(
        field
            .metadata()
            .get(META_LOW_CARDINALITY)
            .map(String::as_str),
        Some("true")
    );
    assert_eq!(
        field.metadata().get(META_MAX_LENGTH).map(String::as_str),
        Some("64")
    );
}

#[test]
fn user_defined_postgres_types_are_lossless_text_and_pseudo_types_fail_closed() {
    for kind in [
        Kind::Simple,
        Kind::Enum(vec!["one".to_owned(), "two".to_owned()]),
        Kind::Array(Type::INT4),
        Kind::Range(Type::INT4),
        Kind::Multirange(Type::INT4_RANGE),
        Kind::Domain(Type::TEXT),
        Kind::Composite(Vec::new()),
    ] {
        let data_type = Type::new("custom".to_owned(), 80_000, kind, "public".to_owned());
        assert_eq!(postgres_to_arrow(&data_type).unwrap(), DataType::Utf8);
        assert_eq!(
            source_column_expression("value", &data_type, crate::connectors::postgres::source::UnsupportedTypePolicy::Fail).unwrap(),
            "\"value\"::text AS \"value\""
        );
    }

    let pseudo = Type::new(
        "custom_pseudo".to_owned(),
        80_001,
        Kind::Pseudo,
        "public".to_owned(),
    );
    assert!(postgres_to_arrow(&pseudo).is_err());
    assert!(source_column_expression("value", &pseudo, crate::connectors::postgres::source::UnsupportedTypePolicy::Fail).is_err());
    assert_eq!(source_column_expression("value", &pseudo, crate::connectors::postgres::source::UnsupportedTypePolicy::ToString).unwrap(), "\"value\"::text AS \"value\"");
}
