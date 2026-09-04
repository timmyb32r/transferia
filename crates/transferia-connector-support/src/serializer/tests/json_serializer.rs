use super::*;
use arrow::array::{
    BinaryArray, BooleanArray, Float64Array, Int64Array, Int8Array, StringArray, StringBuilder,
    UInt16Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

fn encode_batch(batch: &RecordBatch) -> anyhow::Result<Vec<u8>> {
    let encoder = JsonBatchEncoder::new(batch, |_| true)?;
    let mut output = Vec::new();
    for row in 0..batch.num_rows() {
        encoder.write_row(row, &mut output)?;
    }
    Ok(output)
}

#[test]
fn serialize_simple_batch() -> anyhow::Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("active", DataType::Boolean, true),
        Field::new("score", DataType::Float64, true),
    ]));
    let id_arr = Int64Array::from(vec![1, 2, 3]);
    let mut name_arr = StringBuilder::with_capacity(3, 64);
    name_arr.append_value("Alice");
    name_arr.append_value("Bob");
    name_arr.append_value("Charlie");
    let bool_arr = BooleanArray::from(vec![true, false, true]);
    let floats: Vec<f64> = vec![1.5, 2.5, 3.5];
    let float_arr = Float64Array::from(floats);

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_arr),
            Arc::new(name_arr.finish()),
            Arc::new(bool_arr),
            Arc::new(float_arr),
        ],
    )?;

    let text = String::from_utf8(encode_batch(&batch)?)?;

    let lines: Vec<&str> = text.lines().collect();
    anyhow::ensure!(lines.len() == 3, "3 rows \u{2192} 3 JSON lines");

    for line in &lines {
        let val: serde_json::Value = serde_json::from_str(line)?;
        anyhow::ensure!(val.get("id").is_some(), "id missing in {val}");
        anyhow::ensure!(val.get("name").is_some(), "name missing in {val}");
    }
    Ok(())
}

#[test]
fn serialize_with_nulls_default() -> anyhow::Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int64, true),
        Field::new("y", DataType::Utf8, true),
    ]));
    let x_arr = Int64Array::from(vec![1, 2]);
    let mut y_builder = StringBuilder::with_capacity(2, 32);
    y_builder.append_value("hello");
    y_builder.append_null();

    let batch = RecordBatch::try_new(schema, vec![Arc::new(x_arr), Arc::new(y_builder.finish())])?;

    let text = String::from_utf8(encode_batch(&batch)?)?;

    let lines: Vec<&str> = text.lines().collect();
    anyhow::ensure!(lines.len() == 2, "expected 2 lines, got {}", lines.len());

    let row2: serde_json::Value = serde_json::from_str(lines[1])?;
    anyhow::ensure!(
        row2.get("y").is_some(),
        "null column should be present as \"y\": null"
    );
    anyhow::ensure!(row2["y"].is_null(), "y should be null");
    Ok(())
}

#[test]
fn non_finite_floats_are_valid_json_nulls() -> anyhow::Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Float64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Float64Array::from(vec![
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            1.5,
        ]))],
    )?;
    let output = String::from_utf8(encode_batch(&batch)?)?;
    let rows = output
        .lines()
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<Result<Vec<_>, _>>()?;
    anyhow::ensure!(rows[..3].iter().all(|row| row["value"].is_null()));
    anyhow::ensure!(rows[3]["value"] == serde_json::json!(1.5));
    Ok(())
}

#[test]
fn debezium_preserves_non_finite_float_values_as_protobuf_json_strings() -> anyhow::Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Float64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Float64Array::from(vec![
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ]))],
    )?;
    let encoder = JsonBatchEncoder::projected_debezium(
        &batch,
        [JsonColumnProjection {
            output_name: "value".to_owned(),
            source_index: Some(0),
        }],
    )?;
    let mut output = Vec::new();
    for row in 0..batch.num_rows() {
        encoder.write_row(row, &mut output)?;
    }
    let values = String::from_utf8(output)?
        .lines()
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(values[0]["value"], "NaN");
    assert_eq!(values[1]["value"], "Infinity");
    assert_eq!(values[2]["value"], "-Infinity");
    Ok(())
}

#[test]
fn binary_values_use_the_kafka_connect_base64_json_representation() -> anyhow::Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "bytes",
        DataType::Binary,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(BinaryArray::from_iter_values([
            b"\x00\xff".as_slice()
        ]))],
    )?;
    let value: serde_json::Value = serde_json::from_slice(&encode_batch(&batch)?)?;
    assert_eq!(value["bytes"], "AP8=");
    Ok(())
}

#[test]
fn mysql_debezium_projects_exact_physical_values() -> anyhow::Result<()> {
    let schema = Arc::new(Schema::new(vec![
        mysql_field(
            "bit1",
            DataType::Binary,
            MYSQL_BINARY_EXTENSION_NAME,
            r#"{"version":1,"data_type":"bit","column_type":"bit(1)","unsigned":true,"numeric_precision":1}"#,
        ),
        mysql_field(
            "tinyint1",
            DataType::Int8,
            MYSQL_SIGNED_INTEGER_EXTENSION_NAME,
            r#"{"version":1,"data_type":"tinyint","column_type":"tinyint(1)","unsigned":false}"#,
        ),
        mysql_field(
            "tinyint1u",
            DataType::UInt8,
            MYSQL_UNSIGNED_INTEGER_EXTENSION_NAME,
            r#"{"version":1,"data_type":"tinyint","column_type":"tinyint(1) unsigned","unsigned":true}"#,
        ),
        mysql_field(
            "bytes",
            DataType::Binary,
            MYSQL_BINARY_EXTENSION_NAME,
            r#"{"version":1,"data_type":"varbinary","column_type":"varbinary(3)","unsigned":false}"#,
        ),
        mysql_field(
            "u64",
            DataType::UInt64,
            MYSQL_UNSIGNED_INTEGER_EXTENSION_NAME,
            r#"{"version":1,"data_type":"bigint","column_type":"bigint unsigned","unsigned":true}"#,
        ),
        mysql_field(
            "decimal",
            DataType::Utf8,
            MYSQL_DECIMAL_EXTENSION_NAME,
            r#"{"version":1,"data_type":"decimal","column_type":"decimal(65,30)","unsigned":false,"numeric_precision":65,"numeric_scale":30}"#,
        ),
        mysql_field(
            "date",
            DataType::Utf8,
            MYSQL_DATE_EXTENSION_NAME,
            r#"{"version":1,"data_type":"date","column_type":"date","unsigned":false}"#,
        ),
        mysql_field(
            "datetime",
            DataType::Utf8,
            MYSQL_DATETIME_EXTENSION_NAME,
            r#"{"version":1,"data_type":"datetime","column_type":"datetime(6)","unsigned":false,"datetime_precision":6}"#,
        ),
        mysql_field(
            "timestamp",
            DataType::Utf8,
            MYSQL_TIMESTAMP_EXTENSION_NAME,
            r#"{"version":1,"data_type":"timestamp","column_type":"timestamp(6)","unsigned":false,"datetime_precision":6}"#,
        ),
        mysql_field(
            "time",
            DataType::Utf8,
            MYSQL_TIME_EXTENSION_NAME,
            r#"{"version":1,"data_type":"time","column_type":"time(6)","unsigned":false,"datetime_precision":6}"#,
        ),
        mysql_field(
            "year",
            DataType::Utf8,
            MYSQL_YEAR_EXTENSION_NAME,
            r#"{"version":1,"data_type":"year","column_type":"year","unsigned":false}"#,
        ),
        mysql_field(
            "enum_value",
            DataType::UInt16,
            MYSQL_ENUM_EXTENSION_NAME,
            r#"{"version":1,"data_type":"enum","column_type":"enum('red','blue')","unsigned":false,"enum_set_values":["red","blue"]}"#,
        ),
        mysql_field(
            "set_value",
            DataType::UInt64,
            MYSQL_SET_EXTENSION_NAME,
            r#"{"version":1,"data_type":"set","column_type":"set('alpha','beta','gamma')","unsigned":false,"enum_set_values":["alpha","beta","gamma"]}"#,
        ),
        mysql_field(
            "latin1_value",
            DataType::Binary,
            MYSQL_TEXT_BYTES_EXTENSION_NAME,
            r#"{"version":1,"data_type":"varchar","column_type":"varchar(2)","unsigned":false,"character_set":"latin1"}"#,
        ),
        mysql_field(
            "json_value",
            DataType::Utf8,
            ARROW_JSON_EXTENSION_NAME,
            r#"{"version":1,"data_type":"json","column_type":"json","unsigned":false}"#,
        ),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(BinaryArray::from_iter_values([&[1_u8][..]])),
            Arc::new(Int8Array::from(vec![1_i8])),
            Arc::new(UInt8Array::from(vec![255_u8])),
            Arc::new(BinaryArray::from_iter_values([&[0_u8, 255, b'A'][..]])),
            Arc::new(UInt64Array::from(vec![u64::MAX])),
            Arc::new(StringArray::from(vec![
                "12345678901234567890123456789012345.123456789012345678901234567890",
            ])),
            Arc::new(StringArray::from(vec!["1970-01-02"])),
            Arc::new(StringArray::from(vec!["1970-01-01 00:00:01.123456"])),
            Arc::new(StringArray::from(vec!["2024-02-03 04:05:06.123456"])),
            Arc::new(StringArray::from(vec!["-123:27:36.123456"])),
            Arc::new(StringArray::from(vec!["2024"])),
            Arc::new(UInt16Array::from(vec![2_u16])),
            Arc::new(UInt64Array::from(vec![0b101_u64])),
            Arc::new(BinaryArray::from_iter_values([&[0x80_u8, 0xff][..]])),
            Arc::new(StringArray::from(vec![r#"{"a":1}"#])),
        ],
    )?;
    let encoder = JsonBatchEncoder::projected_debezium_mysql(
        &batch,
        schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| JsonColumnProjection {
                output_name: field.name().to_owned(),
                source_index: Some(index),
            }),
    )?;
    let mut output = Vec::new();
    encoder.write_object(0, &mut output)?;
    let value: serde_json::Value = serde_json::from_slice(&output)?;

    assert_eq!(value["bit1"], true);
    assert_eq!(value["tinyint1"], 1);
    assert_eq!(value["tinyint1u"], 255);
    assert_eq!(value["bytes"], "AP9B");
    assert_eq!(value["u64"], "AP//////////");
    assert_eq!(value["decimal"], "HgK8HpeFi9xsuVBY80JNfTp/7HsD4maOPwrS");
    assert_eq!(value["date"], 1);
    assert_eq!(value["datetime"], 1_123_456);
    assert_eq!(value["timestamp"], "2024-02-03T04:05:06.123456Z");
    assert_eq!(value["time"], -444_456_123_456_i64);
    assert_eq!(value["year"], 2024);
    assert_eq!(value["enum_value"], "blue");
    assert_eq!(value["set_value"], "alpha,gamma");
    assert_eq!(value["latin1_value"], "€ÿ");
    assert_eq!(value["json_value"], r#"{"a":1}"#);
    Ok(())
}

#[test]
fn mysql_debezium_tinyint_one_stays_numeric_across_the_full_signed_domain() -> anyhow::Result<()> {
    let field = mysql_field(
        "tinyint1",
        DataType::Int8,
        MYSQL_SIGNED_INTEGER_EXTENSION_NAME,
        r#"{"version":1,"data_type":"tinyint","column_type":"tinyint(1)","unsigned":false}"#,
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field])),
        vec![Arc::new(Int8Array::from(vec![-128_i8, 127_i8]))],
    )?;
    let encoder = JsonBatchEncoder::projected_debezium_mysql(
        &batch,
        [JsonColumnProjection {
            output_name: "tinyint1".to_owned(),
            source_index: Some(0),
        }],
    )?;
    let mut output = Vec::new();
    encoder.write_row(0, &mut output)?;
    encoder.write_row(1, &mut output)?;
    let values = String::from_utf8(output)?
        .lines()
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(values[0]["tinyint1"], -128);
    assert_eq!(values[1]["tinyint1"], 127);
    Ok(())
}

#[test]
fn mysql_debezium_temporal_precision_matrix_matches_connect_logical_units() -> anyhow::Result<()> {
    for (precision, datetime, expected_datetime, time, expected_time) in [
        (0, "1970-01-01 00:00:01", 1_000, "04:05:06", 14_706_000_000),
        (
            1,
            "1970-01-01 00:00:01.1",
            1_100,
            "04:05:06.1",
            14_706_100_000,
        ),
        (
            2,
            "1970-01-01 00:00:01.12",
            1_120,
            "04:05:06.12",
            14_706_120_000,
        ),
        (
            3,
            "1970-01-01 00:00:01.123",
            1_123,
            "04:05:06.123",
            14_706_123_000,
        ),
        (
            4,
            "1970-01-01 00:00:01.1234",
            1_123_400,
            "04:05:06.1234",
            14_706_123_400,
        ),
        (
            5,
            "1970-01-01 00:00:01.12345",
            1_123_450,
            "04:05:06.12345",
            14_706_123_450,
        ),
        (
            6,
            "1970-01-01 00:00:01.123456",
            1_123_456,
            "04:05:06.123456",
            14_706_123_456,
        ),
    ] {
        assert_eq!(
            mysql_datetime_timestamp(datetime, precision)?,
            expected_datetime
        );
        assert_eq!(mysql_time_timestamp(time, precision)?, expected_time);

        let mut timestamp = Vec::new();
        write_mysql_zoned_timestamp(&mut timestamp, datetime, precision)?;
        assert_eq!(
            String::from_utf8(timestamp)?,
            format!("\"{}T{}Z\"", &datetime[..10], &datetime[11..])
        );
    }

    assert_eq!(mysql_time_timestamp("-123:27:36.123", 3)?, -444_456_123_000);
    assert_eq!(
        mysql_time_timestamp("838:59:59.999999", 6)?,
        3_020_399_999_999
    );
    assert_eq!(
        mysql_time_timestamp("-838:59:59.999999", 6)?,
        -3_020_399_999_999
    );
    Ok(())
}

#[test]
fn mysql_debezium_rejects_noninjective_or_unsupported_physical_values() {
    for (extension, metadata, expected) in [
        (
            MYSQL_ENUM_EXTENSION_NAME,
            r#"{"version":1,"data_type":"enum","column_type":"enum('','blue')","unsigned":false,"enum_set_values":["","blue"]}"#,
            "empty member",
        ),
        (
            MYSQL_SET_EXTENSION_NAME,
            r#"{"version":1,"data_type":"set","column_type":"set('a,b','c')","unsigned":false,"enum_set_values":["a,b","c"]}"#,
            "comma-containing",
        ),
        (
            MYSQL_BINARY_EXTENSION_NAME,
            r#"{"version":1,"data_type":"geometry","column_type":"geometry","unsigned":false}"#,
            "spatial type",
        ),
        (
            MYSQL_BINARY_EXTENSION_NAME,
            r#"{"version":1,"data_type":"vector","column_type":"vector(3)","unsigned":false}"#,
            "FloatVector",
        ),
    ] {
        let data_type = if extension == MYSQL_ENUM_EXTENSION_NAME {
            DataType::UInt16
        } else if extension == MYSQL_SET_EXTENSION_NAME {
            DataType::UInt64
        } else {
            DataType::Binary
        };
        let column =
            transferia_core::data::schema::SchemaColumn::new("unsafe".to_owned(), data_type, false)
                .with_arrow_extension_metadata(extension, metadata);
        let error = validate_mysql_debezium_column(&column)
            .unwrap_err()
            .to_string();
        assert!(error.contains(expected), "{error}");
    }
}

#[test]
fn mysql_debezium_temporal_conversion_fails_closed_for_zero_or_partial_values() {
    for error in [
        mysql_date_days("0000-00-00").unwrap_err().to_string(),
        mysql_date_days("2024-00-12").unwrap_err().to_string(),
        mysql_datetime_timestamp("2024-01-00 01:02:03.123456", 6)
            .unwrap_err()
            .to_string(),
        mysql_time_timestamp("-00:00:00.000000", 6)
            .unwrap_err()
            .to_string(),
        mysql_year("0000").unwrap_err().to_string(),
    ] {
        assert!(
            error.contains("MySQL Debezium"),
            "unexpected temporal error: {error}"
        );
    }
}

#[test]
fn mysql_debezium_rejects_undefined_cp1252_bytes_at_runtime() -> anyhow::Result<()> {
    let field = mysql_field(
        "latin1_value",
        DataType::Binary,
        MYSQL_TEXT_BYTES_EXTENSION_NAME,
        r#"{"version":1,"data_type":"varchar","column_type":"varchar(1)","unsigned":false,"character_set":"latin1"}"#,
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field])),
        vec![Arc::new(BinaryArray::from_iter_values([&[0x81_u8][..]]))],
    )?;
    let encoder = JsonBatchEncoder::projected_debezium_mysql(
        &batch,
        [JsonColumnProjection {
            output_name: "latin1_value".to_owned(),
            source_index: Some(0),
        }],
    )?;
    let error = encoder
        .write_object(0, &mut Vec::new())
        .unwrap_err()
        .to_string();
    assert!(error.contains("undefined cp1252 byte 0x81"), "{error}");
    Ok(())
}

fn mysql_field(name: &str, data_type: DataType, extension: &str, metadata: &str) -> Field {
    Field::new(name, data_type, false).with_metadata(std::collections::HashMap::from([
        (META_ARROW_EXTENSION_NAME.to_owned(), extension.to_owned()),
        (
            META_ARROW_EXTENSION_METADATA.to_owned(),
            metadata.to_owned(),
        ),
    ]))
}
