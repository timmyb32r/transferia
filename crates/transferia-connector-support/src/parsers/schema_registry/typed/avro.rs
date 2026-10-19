use std::sync::Arc;

use apache_avro::{types::Value, Schema};
use arrow::array::*;
use arrow::datatypes::i256;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;

pub(in crate::parsers::schema_registry) fn record(schema: &Schema, value: &Value) -> anyhow::Result<RecordBatch> {
    let Schema::Record(record) = schema else {
        anyhow::bail!("Schema Registry Avro output requires a record schema");
    };
    let Value::Record(values) = value else {
        anyhow::bail!("Avro datum does not match its record schema");
    };
    let mut fields = Vec::with_capacity(record.fields.len());
    let mut arrays = Vec::with_capacity(record.fields.len());
    for field in &record.fields {
        let value = values.iter().find(|(name, _)| name == &field.name)
            .ok_or_else(|| anyhow::anyhow!("Avro record is missing field '{}'", field.name))?;
        let (data_type, nullable) = data_type(&field.schema)?;
        fields.push(Field::new(&field.name, data_type, nullable));
        arrays.push(array(&field.schema, &value.1)?);
    }
    Ok(RecordBatch::try_new_with_options(Arc::new(ArrowSchema::new(fields)), arrays, &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(1)))?)
}

fn non_null_schema(schema: &Schema) -> anyhow::Result<(&Schema, bool)> {
    if let Schema::Union(union) = schema {
        let mut variants = union.variants().iter().filter(|schema| !matches!(schema, Schema::Null));
        let inner = variants.next().ok_or_else(|| anyhow::anyhow!("Avro union has no non-null variant"))?;
        anyhow::ensure!(variants.next().is_none(), "Avro unions with multiple non-null variants have no supported Arrow representation");
        return Ok((inner, union.variants().iter().any(|schema| matches!(schema, Schema::Null))));
    }
    Ok((schema, matches!(schema, Schema::Null)))
}

fn data_type(schema: &Schema) -> anyhow::Result<(DataType, bool)> {
    let (schema, nullable) = non_null_schema(schema)?;
    let data_type = match schema {
        Schema::Null => DataType::Null,
        Schema::Boolean => DataType::Boolean,
        Schema::Int => DataType::Int32,
        Schema::Long => DataType::Int64,
        Schema::Float => DataType::Float32,
        Schema::Double => DataType::Float64,
        Schema::Bytes => DataType::Binary,
        Schema::Fixed(fixed) => DataType::FixedSizeBinary(i32::try_from(fixed.size)?),
        Schema::String | Schema::Enum(_) | Schema::Uuid => DataType::Utf8,
        Schema::Date => DataType::Date32,
        Schema::TimeMillis => DataType::Time32(TimeUnit::Millisecond),
        Schema::TimeMicros => DataType::Time64(TimeUnit::Microsecond),
        Schema::TimestampMillis => DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        Schema::TimestampMicros => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        Schema::TimestampNanos => DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        Schema::LocalTimestampMillis => DataType::Timestamp(TimeUnit::Millisecond, None),
        Schema::LocalTimestampMicros => DataType::Timestamp(TimeUnit::Microsecond, None),
        Schema::LocalTimestampNanos => DataType::Timestamp(TimeUnit::Nanosecond, None),
        Schema::Decimal(decimal) => {
            let precision = u8::try_from(decimal.precision)?;
            let scale = i8::try_from(decimal.scale)?;
            anyhow::ensure!(precision <= 76, "Avro decimal precision exceeds Arrow Decimal256 precision");
            if precision <= 38 { DataType::Decimal128(precision, scale) } else { DataType::Decimal256(precision, scale) }
        }
        Schema::Array(array) => {
            let (data_type, nullable) = data_type(&array.items)?;
            DataType::List(Arc::new(Field::new("item", data_type, nullable)))
        }
        Schema::Map(map) => {
            let (data_type, nullable) = data_type(&map.types)?;
            DataType::Map(Arc::new(Field::new("entries", DataType::Struct(vec![
                Field::new("key", DataType::Utf8, false), Field::new("value", data_type, nullable),
            ].into()), false)), false)
        }
        Schema::Record(record) => DataType::Struct(record.fields.iter().map(|field| {
            let (data_type, nullable) = data_type(&field.schema)?;
            Ok(Field::new(&field.name, data_type, nullable))
        }).collect::<anyhow::Result<Vec<_>>>()?.into()),
        other => anyhow::bail!("Avro type has no supported lossless Arrow representation: {:?}", apache_avro::schema::SchemaKind::from(other)),
    };
    Ok((data_type, nullable))
}

fn array(schema: &Schema, value: &Value) -> anyhow::Result<ArrayRef> {
    let (data_type, nullable) = data_type(schema)?;
    let value = if let Value::Union(_, value) = value { value.as_ref() } else { value };
    if matches!(value, Value::Null) {
        anyhow::ensure!(nullable, "Avro null is not permitted by its schema");
        return Ok(new_null_array(&data_type, 1));
    }
    let (schema, _) = non_null_schema(schema)?;
    macro_rules! scalar { ($array:ident, $value:expr) => { Arc::new($array::from(vec![$value])) as ArrayRef }; }
    let result: ArrayRef = match (schema, value) {
        (Schema::Boolean, Value::Boolean(value)) => scalar!(BooleanArray, *value),
        (Schema::Int, Value::Int(value)) => scalar!(Int32Array, *value),
        (Schema::Long, Value::Long(value)) => scalar!(Int64Array, *value),
        (Schema::Float, Value::Float(value)) => scalar!(Float32Array, *value),
        (Schema::Double, Value::Double(value)) => scalar!(Float64Array, *value),
        (Schema::String, Value::String(value)) | (Schema::Enum(_), Value::Enum(_, value)) => scalar!(StringArray, value.as_str()),
        (Schema::Uuid, Value::Uuid(value)) => scalar!(StringArray, value.to_string().as_str()),
        (Schema::Bytes, Value::Bytes(value)) => scalar!(BinaryArray, value.as_slice()),
        (Schema::Fixed(fixed), Value::Fixed(_, value)) => {
            Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size([Some(value.as_slice())].into_iter(), i32::try_from(fixed.size)?)?)
        }
        (Schema::Date, Value::Date(value)) => scalar!(Date32Array, *value),
        (Schema::TimeMillis, Value::TimeMillis(value)) => scalar!(Time32MillisecondArray, *value),
        (Schema::TimeMicros, Value::TimeMicros(value)) => scalar!(Time64MicrosecondArray, *value),
        (Schema::TimestampMillis, Value::TimestampMillis(value)) => Arc::new(TimestampMillisecondArray::from(vec![*value]).with_timezone("UTC")),
        (Schema::TimestampMicros, Value::TimestampMicros(value)) => Arc::new(TimestampMicrosecondArray::from(vec![*value]).with_timezone("UTC")),
        (Schema::TimestampNanos, Value::TimestampNanos(value)) => Arc::new(TimestampNanosecondArray::from(vec![*value]).with_timezone("UTC")),
        (Schema::LocalTimestampMillis, Value::LocalTimestampMillis(value)) => scalar!(TimestampMillisecondArray, *value),
        (Schema::LocalTimestampMicros, Value::LocalTimestampMicros(value)) => scalar!(TimestampMicrosecondArray, *value),
        (Schema::LocalTimestampNanos, Value::LocalTimestampNanos(value)) => scalar!(TimestampNanosecondArray, *value),
        (Schema::Decimal(decimal), Value::Decimal(value)) => {
            let bytes = Vec::<u8>::try_from(value)?;
            anyhow::ensure!(bytes.len() <= 32, "Avro decimal value exceeds Decimal256 storage");
            let mut padded = [if bytes.first().is_some_and(|byte| byte & 0x80 != 0) { 0xff } else { 0 }; 32];
            padded[32 - bytes.len()..].copy_from_slice(&bytes);
            let value = i256::from_be_bytes(padded);
            let precision = u8::try_from(decimal.precision)?;
            let scale = i8::try_from(decimal.scale)?;
            if precision <= 38 {
                let value = value.to_i128().ok_or_else(|| anyhow::anyhow!("Avro decimal overflows Decimal128"))?;
                let array = Decimal128Array::from(vec![value]).with_precision_and_scale(precision, scale)?;
                array.validate_decimal_precision(precision)?;
                Arc::new(array)
            } else {
                let array = Decimal256Array::from(vec![value]).with_precision_and_scale(precision, scale)?;
                array.validate_decimal_precision(precision)?;
                Arc::new(array)
            }
        }
        (Schema::Array(items), Value::Array(values)) => {
            let DataType::List(field) = data_type else { unreachable!() };
            super::list(field, values.iter().map(|value| array(&items.items, value)).collect::<anyhow::Result<_>>()?)?
        }
        (Schema::Map(map), Value::Map(values)) => {
            let DataType::Map(field, _) = data_type else { unreachable!() };
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            super::string_map(field, entries.into_iter().map(|(key, value)| Ok((key.as_str(), array(&map.types, value)?))).collect::<anyhow::Result<_>>()?)?
        }
        (Schema::Record(_), Value::Record(_)) => {
            let record = record(schema, value)?;
            Arc::new(StructArray::from(record))
        }
        _ => anyhow::bail!("Avro value does not match its declared type"),
    };
    Ok(result)
}
