use super::copy_out::{CopyDecoder, DecodeState};
use super::reader::{decode_i8, source_column_expression};
use bytes::Bytes;
use arrow::datatypes::DataType;
use tokio_postgres::types::{Kind, Type};

use crate::connectors::postgres::common::postgres_to_arrow;
use crate::connectors::postgres::PostgresCopyFormat;

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
    assert_eq!(row.fields[0].as_deref(), Some(7_i32.to_be_bytes().as_slice()));
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
    ] {
        assert_eq!(postgres_to_arrow(&postgres).unwrap(), arrow);
        assert_eq!(
            source_column_expression("mixed\"case", &postgres).unwrap(),
            "\"mixed\"\"case\""
        );
    }

    for postgres in [
        Type::NUMERIC,
        Type::MONEY,
        Type::DATE,
        Type::TIME,
        Type::TIMETZ,
        Type::TIMESTAMP,
        Type::TIMESTAMPTZ,
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
            source_column_expression("value", &postgres).unwrap(),
            "\"value\"::text AS \"value\""
        );
    }
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
            source_column_expression("value", &data_type).unwrap(),
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
    assert!(source_column_expression("value", &pseudo).is_err());
}
