use std::collections::HashMap;

use arrow::array::{
    Array, BinaryArray, Decimal128Array, DictionaryArray, Int8Array, Int16Array, Int32Array,
    ListArray, MapArray, StructArray, UInt8Array,
    TimestampMillisecondArray, TimestampSecondArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::Int32Type;

use super::*;
use super::super::parquet::{parquet_input_type, validate_parquet_input_schema};

fn second_type() -> DataType {
    DataType::Timestamp(TimeUnit::Second, Some(Arc::from("Europe/Moscow")))
}

fn milliseconds(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(TimestampMillisecondArray::from(values).with_timezone("UTC"))
}

#[test]
fn nested_list_timestamps_preserve_values_offsets_nulls_and_metadata() -> anyhow::Result<()> {
    let values = milliseconds(vec![Some(1_000), None, Some(-2_000)]);
    let actual_item = Arc::new(Field::new("item", values.data_type().clone(), true));
    let list = ListArray::try_new(
        actual_item,
        OffsetBuffer::new(vec![0_i32, 2, 3].into()),
        values,
        None,
    )?;
    let expected_item = Arc::new(
        Field::new("item", second_type(), true)
            .with_metadata(HashMap::from([("source.member".into(), "exact".into())])),
    );
    let expected_list = DataType::List(expected_item);
    let actual = StructArray::try_new(
        vec![Arc::new(Field::new("events", list.data_type().clone(), false))].into(),
        vec![Arc::new(list)],
        None,
    )?;
    let expected = DataType::Struct(
        vec![Arc::new(Field::new("events", expected_list, false))].into(),
    );
    let normalized = normalize_snapshot_array(&(Arc::new(actual) as ArrayRef), &expected, "payload", false)?;
    assert_eq!(normalized.data_type(), &expected);
    let structure = normalized.as_any().downcast_ref::<StructArray>().unwrap();
    let list = structure.column(0).as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.value_offsets(), &[0, 2, 3]);
    assert_eq!(
        list.values().as_any().downcast_ref::<TimestampSecondArray>().unwrap().iter().collect::<Vec<_>>(),
        vec![Some(1), None, Some(-2)],
    );
    Ok(())
}

#[test]
fn dictionary_timestamps_keep_keys_and_nulls() -> anyhow::Result<()> {
    let dictionary = DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![Some(1), Some(0), None]),
        milliseconds(vec![Some(-2_000), Some(1_000)]),
    )?;
    let expected = DataType::Dictionary(Box::new(DataType::Int32), Box::new(second_type()));
    let normalized = normalize_snapshot_array(&(Arc::new(dictionary) as ArrayRef), &expected, "event", false)?;
    let dictionary = normalized.as_any().downcast_ref::<DictionaryArray<Int32Type>>().unwrap();
    assert_eq!(dictionary.keys().iter().collect::<Vec<_>>(), vec![Some(1), Some(0), None]);
    assert_eq!(
        dictionary.values().as_any().downcast_ref::<TimestampSecondArray>().unwrap().iter().collect::<Vec<_>>(),
        vec![Some(-2), Some(1)],
    );
    Ok(())
}

#[test]
fn map_timestamps_keep_entry_order_and_offsets() -> anyhow::Result<()> {
    let values = milliseconds(vec![Some(2_000), Some(1_000)]);
    let entries = StructArray::try_new(
        vec![
            Arc::new(Field::new("key", DataType::Int32, false)),
            Arc::new(Field::new("value", values.data_type().clone(), true)),
        ].into(),
        vec![Arc::new(Int32Array::from(vec![2, 1])) as ArrayRef, values],
        None,
    )?;
    let map = MapArray::try_new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::new(vec![0_i32, 2].into()),
        entries,
        None,
        false,
    )?;
    let expected = DataType::Map(Arc::new(Field::new(
        "entries",
        DataType::Struct(vec![
            Arc::new(Field::new("key", DataType::Int32, false)),
            Arc::new(Field::new("value", second_type(), true)),
        ].into()),
        false,
    )), false);
    let normalized = normalize_snapshot_array(&(Arc::new(map) as ArrayRef), &expected, "events", false)?;
    let map = normalized.as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(map.value_offsets(), &[0, 2]);
    assert_eq!(map.keys().as_any().downcast_ref::<Int32Array>().unwrap().values().as_ref(), &[2, 1]);
    assert_eq!(map.values().as_any().downcast_ref::<TimestampSecondArray>().unwrap().values().as_ref(), &[2, 1]);
    Ok(())
}

#[test]
fn nested_timestamp_rejects_fractional_seconds_with_member_path() -> anyhow::Result<()> {
    let values = milliseconds(vec![Some(1_500)]);
    let list = ListArray::try_new(
        Arc::new(Field::new("item", values.data_type().clone(), true)),
        OffsetBuffer::new(vec![0_i32, 1].into()),
        values,
        None,
    )?;
    let expected = DataType::List(Arc::new(Field::new("item", second_type(), true)));
    let error = normalize_snapshot_array(&(Arc::new(list) as ArrayRef), &expected, "events", false).unwrap_err().to_string();
    assert!(error.contains("events.item"), "{error}");
    assert!(error.contains("cannot be represented losslessly"), "{error}");
    Ok(())
}

#[test]
fn native_tuple_names_require_validated_declaration() -> anyhow::Result<()> {
    let actual = StructArray::try_new(
        vec![Arc::new(Field::new("field_0", DataType::Int32, false))].into(),
        vec![Arc::new(Int32Array::from(vec![7])) as ArrayRef],
        None,
    )?;
    let expected = DataType::Struct(vec![Arc::new(Field::new("authored", DataType::Int32, false))].into());
    let array = Arc::new(actual) as ArrayRef;
    assert!(normalize_snapshot_array(&array, &expected, "tuple", false).is_err());
    let normalized = normalize_snapshot_array(&array, &expected, "tuple", true)?;
    assert_eq!(normalized.data_type(), &expected);
    Ok(())
}

#[test]
fn parquet_mapping_recurses_through_dictionary_and_list_fields() {
    let canonical = DataType::Dictionary(
        Box::new(DataType::Int32),
        Box::new(DataType::List(Arc::new(Field::new("item", second_type(), true)))),
    );
    let expected = DataType::Dictionary(
        Box::new(DataType::Int32),
        Box::new(DataType::List(Arc::new(Field::new(
            "item", DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC"))), true,
        )))),
    );
    assert_eq!(parquet_input_type(&canonical), expected);
}

#[test]
fn parquet_schema_validation_rejects_plain_integer_timestamp_hint() {
    let actual = Schema::new(vec![Field::new("event_time", DataType::Int64, false)]);
    let expected = Schema::new(vec![Field::new(
        "event_time", DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC"))), false,
    )]);
    let error = validate_parquet_input_schema(&actual, &expected).unwrap_err().to_string();
    assert!(error.contains("event_time"), "{error}");
    assert!(error.contains("schema drifted"), "{error}");
}

#[test]
fn parquet_schema_validation_rejects_decimal_scale_drift() {
    let actual = Schema::new(vec![Field::new("amount", DataType::Decimal128(18, 3), false)]);
    let expected = Schema::new(vec![Field::new("amount", DataType::Decimal128(18, 2), false)]);
    assert!(validate_parquet_input_schema(&actual, &expected).is_err());
}

#[test]
fn string_transport_rejects_invalid_utf8_instead_of_replacing_it_with_null() {
    let array = Arc::new(BinaryArray::from(vec![Some(b"valid".as_slice()), Some(&[0xff])])) as ArrayRef;
    let error = decode_snapshot_string(&array, "source.payload").unwrap_err().to_string();
    assert!(error.contains("source.payload"), "{error}");
    assert!(error.contains("UTF-8"), "{error}");
}

#[test]
fn timestamp_normalization_preserves_sliced_list_offsets() -> anyhow::Result<()> {
    let values = milliseconds(vec![Some(1_000), Some(2_000), Some(3_000)]);
    let list = ListArray::try_new(
        Arc::new(Field::new("element", values.data_type().clone(), true)),
        OffsetBuffer::new(vec![0_i32, 2, 3].into()),
        values,
        None,
    )?.slice(1, 1);
    let expected = DataType::List(Arc::new(Field::new("item", second_type(), true)));
    let normalized = normalize_snapshot_array(&(Arc::new(list) as ArrayRef), &expected, "events", false)?;
    let list = normalized.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.value_offsets(), &[2, 3]);
    let value = list.value(0);
    assert_eq!(value.as_any().downcast_ref::<TimestampSecondArray>().unwrap().values().as_ref(), &[3]);
    Ok(())
}

#[test]
fn parquet_decode_preserves_nested_metadata_and_normalizes_transport_item_names() -> anyhow::Result<()> {
    let values = milliseconds(vec![Some(1_000), Some(2_000)]);
    let list = ListArray::try_new(
        Arc::new(Field::new("element", values.data_type().clone(), true)),
        OffsetBuffer::new(vec![0_i32, 2].into()),
        values,
        None,
    )?;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("events", list.data_type().clone(), false)])),
        vec![Arc::new(list)],
    )?;
    let mut encoded = Vec::new();
    let mut writer = parquet::arrow::ArrowWriter::try_new(&mut encoded, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;

    let canonical = DataType::List(Arc::new(
        Field::new("item", second_type(), true)
            .with_metadata(HashMap::from([("source.member".into(), "exact".into())])),
    ));
    let expected = Schema::new(vec![Field::new("events", parquet_input_type(&canonical), false)]);
    let bytes = bytes::Bytes::from(encoded);
    let inferred = parquet::arrow::arrow_reader::ArrowReaderMetadata::load(
        &bytes,
        parquet::arrow::arrow_reader::ArrowReaderOptions::new().with_skip_arrow_metadata(true),
    )?;
    let hint = validate_parquet_input_schema(inferred.schema(), &expected)?;
    let metadata = parquet::arrow::arrow_reader::ArrowReaderMetadata::try_new(
        Arc::clone(inferred.metadata()),
        parquet::arrow::arrow_reader::ArrowReaderOptions::new().with_schema(hint),
    )?;
    let decoded = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::new_with_metadata(bytes, metadata)
        .build()?.next().unwrap()?;
    let normalized = normalize_snapshot_array(decoded.column(0), &canonical, "events", false)?;
    assert_eq!(normalized.data_type(), &canonical);
    let list = normalized.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.values().as_any().downcast_ref::<TimestampSecondArray>().unwrap().values().as_ref(), &[1, 2]);
    Ok(())
}

fn parquet_enum_round_trip(array: ArrayRef, declaration: &str) -> anyhow::Result<ArrayRef> {
    let declared = declaration.parse::<clickhouse_arrow::Type>()?;
    let (canonical, nullable) = super::super::types::source_arrow_type(&declared, declaration)?;
    let transport = EnumTransport::new(&declared)?;
    let expected_type = parquet_input_type(&transport.parquet_type(&canonical)?);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("status", array.data_type().clone(), nullable)])),
        vec![array],
    )?;
    let mut encoded = Vec::new();
    let mut writer = parquet::arrow::ArrowWriter::try_new(&mut encoded, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    let bytes = bytes::Bytes::from(encoded);
    let inferred = parquet::arrow::arrow_reader::ArrowReaderMetadata::load(
        &bytes,
        parquet::arrow::arrow_reader::ArrowReaderOptions::new().with_skip_arrow_metadata(true),
    )?;
    let expected = Schema::new(vec![Field::new("status", expected_type, nullable)]);
    let hint = validate_parquet_input_schema(inferred.schema(), &expected)?;
    let metadata = parquet::arrow::arrow_reader::ArrowReaderMetadata::try_new(
        Arc::clone(inferred.metadata()),
        parquet::arrow::arrow_reader::ArrowReaderOptions::new().with_schema(hint),
    )?;
    let decoded = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::new_with_metadata(bytes, metadata)
        .build()?.next().unwrap()?;
    let decoded = transport.decode(decoded.column(0), "status")?;
    normalize_snapshot_array(&decoded, &canonical, "status", false)
}

fn enum_labels(array: &ArrayRef) -> Vec<Option<String>> {
    let dictionary = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>().unwrap();
    let values = dictionary.values().as_any().downcast_ref::<StringArray>().unwrap();
    dictionary.keys().iter().map(|key| key.map(|key| values.value(usize::try_from(key).unwrap()).to_owned())).collect()
}

#[test]
fn parquet_enum8_codes_preserve_negative_values_labels_and_nulls() -> anyhow::Result<()> {
    let decoded = parquet_enum_round_trip(
        Arc::new(Int8Array::from(vec![Some(-7), None, Some(1), Some(-7)])),
        "Nullable(Enum8('negative' = -7, 'positive' = 1))",
    )?;
    assert_eq!(enum_labels(&decoded), vec![Some("negative".into()), None, Some("positive".into()), Some("negative".into())]);
    Ok(())
}

#[test]
fn parquet_nested_enum16_codes_keep_list_order() -> anyhow::Result<()> {
    let values = Arc::new(Int16Array::from(vec![Some(-32768), Some(32767), None])) as ArrayRef;
    let list = ListArray::try_new(
        Arc::new(Field::new("element", DataType::Int16, true)),
        OffsetBuffer::new(vec![0_i32, 3].into()), values, None,
    )?;
    let decoded = parquet_enum_round_trip(
        Arc::new(list), "Array(Nullable(Enum16('low' = -32768, 'high' = 32767)))",
    )?;
    let list = decoded.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(enum_labels(list.values()), vec![Some("low".into()), Some("high".into()), None]);
    Ok(())
}

#[test]
fn parquet_low_cardinality_enum_preserves_both_dictionary_layers() -> anyhow::Result<()> {
    let decoded = parquet_enum_round_trip(
        Arc::new(Int8Array::from(vec![Some(-1), Some(1), None, Some(-1)])),
        "LowCardinality(Nullable(Enum8('off' = -1, 'on' = 1)))",
    )?;
    let outer = decoded.as_any().downcast_ref::<DictionaryArray<Int32Type>>().unwrap();
    let labels = enum_labels(outer.values());
    let rows = outer.keys().iter().map(|key| key.and_then(|key| labels[usize::try_from(key).unwrap()].clone())).collect::<Vec<_>>();
    assert_eq!(rows, vec![Some("off".into()), Some("on".into()), None, Some("off".into())]);
    Ok(())
}

#[test]
fn parquet_enum_rejects_undeclared_code() {
    let error = parquet_enum_round_trip(
        Arc::new(Int16Array::from(vec![1_i16, -3_i16])), "Enum16('one' = 1)",
    ).unwrap_err().to_string();
    assert!(error.contains("status"), "{error}");
    assert!(error.contains("undeclared code -3"), "{error}");
}

#[test]
fn native_nested_decimal_precision_is_checked_before_reannotation() -> anyhow::Result<()> {
    let values = Arc::new(Decimal128Array::from(vec![1234_i128]).with_precision_and_scale(9, 2)?) as ArrayRef;
    let list = ListArray::try_new(
        Arc::new(Field::new("item", values.data_type().clone(), false)),
        OffsetBuffer::new(vec![0_i32, 1].into()), values, None,
    )?;
    let expected = DataType::List(Arc::new(Field::new("item", DataType::Decimal128(4, 2), false)));
    let array = Arc::new(list) as ArrayRef;
    assert!(normalize_snapshot_array(&array, &expected, "amounts", false).is_err());
    let normalized = normalize_snapshot_array(&array, &expected, "amounts", true)?;
    assert_eq!(normalized.data_type(), &expected);
    Ok(())
}

#[test]
fn native_decimal_rejects_value_outside_declared_precision() -> anyhow::Result<()> {
    let array = Arc::new(Decimal128Array::from(vec![10000_i128]).with_precision_and_scale(9, 2)?) as ArrayRef;
    let error = normalize_snapshot_array(&array, &DataType::Decimal128(4, 2), "amount", true).unwrap_err().to_string();
    assert!(error.contains("amount"), "{error}");
    assert!(error.contains("precision"), "{error}");
    Ok(())
}

#[test]
fn unchanged_nested_decimal_types_still_validate_precision() -> anyhow::Result<()> {
    let values = Arc::new(Decimal128Array::from(vec![10000_i128]).with_precision_and_scale(4, 2)?) as ArrayRef;
    let list = Arc::new(ListArray::try_new(
        Arc::new(Field::new("item", values.data_type().clone(), false)),
        OffsetBuffer::new(vec![0_i32, 1].into()), Arc::clone(&values), None,
    )?) as ArrayRef;
    let structure = Arc::new(StructArray::try_new(
        vec![Arc::new(Field::new("amount", values.data_type().clone(), false))].into(),
        vec![Arc::clone(&values)], None,
    )?) as ArrayRef;
    let dictionary = Arc::new(DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![0]), values,
    )?) as ArrayRef;
    for array in [list, structure, dictionary] {
        let error = normalize_snapshot_array(&array, array.data_type(), "payload", false)
            .unwrap_err().to_string();
        assert!(error.contains("payload"), "{error}");
        assert!(error.contains("precision"), "{error}");
    }

    let null_values = Arc::new(Decimal128Array::new(
        vec![10000_i128].into(), Some(arrow::buffer::NullBuffer::new_null(1)),
    ).with_precision_and_scale(4, 2)?) as ArrayRef;
    let list = Arc::new(ListArray::try_new(
        Arc::new(Field::new("item", null_values.data_type().clone(), true)),
        OffsetBuffer::new(vec![0_i32, 1].into()), null_values, None,
    )?) as ArrayRef;
    let normalized = normalize_snapshot_array(&list, list.data_type(), "payload", false)?;
    assert!(Arc::ptr_eq(&list, &normalized));
    Ok(())
}

#[test]
fn native_boolean_requires_verified_type_and_exact_zero_or_one() -> anyhow::Result<()> {
    let valid = Arc::new(UInt8Array::from(vec![Some(0), Some(1), None])) as ArrayRef;
    assert!(normalize_snapshot_array(&valid, &DataType::Boolean, "enabled", false).is_err());
    let normalized = normalize_snapshot_array(&valid, &DataType::Boolean, "enabled", true)?;
    let values = normalized.as_any().downcast_ref::<arrow::array::BooleanArray>().unwrap();
    assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some(false), Some(true), None]);
    let invalid = Arc::new(UInt8Array::from(vec![2])) as ArrayRef;
    let error = normalize_snapshot_array(&invalid, &DataType::Boolean, "enabled", true).unwrap_err().to_string();
    assert!(error.contains("enabled"), "{error}");
    assert!(error.contains("0 or 1"), "{error}");
    Ok(())
}
