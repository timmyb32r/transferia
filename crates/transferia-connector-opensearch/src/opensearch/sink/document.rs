use std::sync::Arc;

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array, Decimal256Array,
    DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
    DurationSecondArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine as _;
use serde_json::value::RawValue;
use serde_json::{Map, Number, Value};
use transferia_core::data::schema::{DatasetSchema, ARROW_JSON_EXTENSION_NAME};

use super::config::RoutedIdentity;

const MAX_DOCUMENT_ID_BYTES: usize = 512;

#[derive(Clone, Debug)]
pub(super) struct BulkAction {
    pub(super) id: Arc<str>,

    pub(super) ndjson: Arc<[u8]>,

    pub(super) bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DocumentShape {
    Envelope,
    Flat,
}

pub(super) fn document_shape(schema: &DatasetSchema) -> DocumentShape {
    if schema.columns.len() == 4
        && schema.columns[0].name == "_id"
        && schema.columns[0].data_type == DataType::Utf8
        && !schema.columns[0].nullable
        && schema.columns[0].primary_key
        && schema.columns[0].max_length == Some(MAX_DOCUMENT_ID_BYTES)
        && schema.columns[0].arrow_extension_name.is_none()
        && schema.columns[1].name == "_routing"
        && schema.columns[1].data_type == DataType::Utf8
        && schema.columns[1].nullable
        && !schema.columns[1].primary_key
        && schema.columns[1].max_length.is_none()
        && schema.columns[1].arrow_extension_name.is_none()
        && schema.columns[2].name == "_source"
        && schema.columns[2].data_type == DataType::Utf8
        && !schema.columns[2].nullable
        && !schema.columns[2].primary_key
        && schema.columns[2].max_length.is_none()
        && schema.columns[2].arrow_extension_name == Some(ARROW_JSON_EXTENSION_NAME)
        && schema.columns[3].name == "_routing_key"
        && schema.columns[3].data_type == DataType::Utf8
        && !schema.columns[3].nullable
        && schema.columns[3].primary_key
        && schema.columns[3].max_length.is_none()
        && schema.columns[3].arrow_extension_name.is_none()
    {
        DocumentShape::Envelope
    } else {
        DocumentShape::Flat
    }
}

pub(super) fn encode_batch(
    index: &str,
    schema: &DatasetSchema,
    batch: &RecordBatch,
    routed_identity: RoutedIdentity,
) -> anyhow::Result<Vec<BulkAction>> {
    match document_shape(schema) {
        DocumentShape::Envelope => encode_envelope(index, batch, routed_identity),
        DocumentShape::Flat => encode_flat(index, schema, batch, routed_identity),
    }
}

fn encode_envelope(
    index: &str,
    batch: &RecordBatch,
    routed_identity: RoutedIdentity,
) -> anyhow::Result<Vec<BulkAction>> {
    let ids = downcast::<StringArray>(batch.column(0).as_ref())?;
    let routing = downcast::<StringArray>(batch.column(1).as_ref())?;
    let sources = downcast::<StringArray>(batch.column(2).as_ref())?;
    let routing_keys = downcast::<StringArray>(batch.column(3).as_ref())?;
    let mut actions = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        anyhow::ensure!(!ids.is_null(row), "OpenSearch _id must not be null");
        anyhow::ensure!(
            !sources.is_null(row),
            "OpenSearch _source must not be null"
        );
        anyhow::ensure!(
            !routing_keys.is_null(row),
            "OpenSearch _routing_key must not be null"
        );
        let id = ids.value(row);
        validate_document_id(id)?;
        let route = (!routing.is_null(row)).then(|| routing.value(row));
        let routing_key = routing_keys.value(row);
        anyhow::ensure!(
            route.unwrap_or(id) == routing_key,
            "OpenSearch _routing_key for _id '{id}' does not match its effective routing key"
        );
        let destination_id = match routed_identity {
            RoutedIdentity::Fail => {
                anyhow::ensure!(
                    route.is_none(),
                    "OpenSearch source document _id '{id}' uses custom routing; select routed_identity=encode_identity explicitly to preserve its composite identity"
                );
                id.to_owned()
            }
            RoutedIdentity::EncodeIdentity => routed_document_id(id, routing_key)?,
        };
        let source = sources.value(row);
        let parsed: Box<RawValue> = serde_json::from_str(source)
            .map_err(|error| anyhow::anyhow!("OpenSearch _source for _id '{id}' is invalid JSON: {error}"))?;
        anyhow::ensure!(
            parsed
                .get()
                .bytes()
                .find(|byte| !byte.is_ascii_whitespace())
                == Some(b'{'),
            "OpenSearch _source for _id '{id}' must be a JSON object"
        );
        actions.push(build_action(
            index,
            &destination_id,
            route,
            source.as_bytes(),
        )?);
    }
    Ok(actions)
}

fn routed_document_id(id: &str, routing_key: &str) -> anyhow::Result<String> {
    let mut encoded = Vec::with_capacity(id.len().saturating_add(routing_key.len()).saturating_add(64));
    append_bytes(&mut encoded, b"transferia:opensearch:routed-identity:v1")?;
    append_bytes(&mut encoded, id.as_bytes())?;
    append_bytes(&mut encoded, routing_key.as_bytes())?;
    let destination_id = URL_SAFE_NO_PAD.encode(encoded);
    validate_document_id(&destination_id)?;
    Ok(destination_id)
}

fn encode_flat(
    index: &str,
    schema: &DatasetSchema,
    batch: &RecordBatch,
    routed_identity: RoutedIdentity,
) -> anyhow::Result<Vec<BulkAction>> {
    let primary_keys = schema
        .columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| column.primary_key.then_some(index))
        .collect::<Vec<_>>();
    let direct_id = schema
        .columns
        .iter()
        .position(|column| column.name == "_id");
    let routing = schema
        .columns
        .iter()
        .position(|column| column.name == "_routing");
    let mut actions = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let source_id = if let Some(position) = direct_id {
            let array = downcast::<StringArray>(batch.column(position).as_ref())?;
            anyhow::ensure!(!array.is_null(row), "OpenSearch _id must not be null");
            array.value(row).to_owned()
        } else {
            composite_document_id(batch, schema, &primary_keys, row)?
        };
        validate_document_id(&source_id)?;
        let route = routing
            .map(|position| -> anyhow::Result<Option<&str>> {
                let array = downcast::<StringArray>(batch.column(position).as_ref())?;
                Ok((!array.is_null(row)).then(|| array.value(row)))
            })
            .transpose()?
            .flatten();
        let destination_id = match routed_identity {
            RoutedIdentity::Fail => {
                anyhow::ensure!(
                    route.is_none(),
                    "OpenSearch row _id '{source_id}' uses custom routing; select routed_identity=encode_identity explicitly to preserve its composite identity"
                );
                source_id
            }
            RoutedIdentity::EncodeIdentity => {
                routed_document_id(&source_id, route.unwrap_or(&source_id))?
            }
        };
        let mut object = Map::with_capacity(schema.columns.len());
        for (position, column) in schema.columns.iter().enumerate() {
            if column.name == "_id" || column.name == "_routing" {
                continue;
            }
            let value = arrow_json_value(batch.column(position).as_ref(), row, column.arrow_extension_name)
                .map_err(|error| anyhow::anyhow!("cannot encode OpenSearch field '{}': {error}", column.name))?;
            object.insert(column.name.clone(), value);
        }
        let source = serde_json::to_vec(&Value::Object(object))?;
        actions.push(build_action(index, &destination_id, route, &source)?);
    }
    Ok(actions)
}

fn build_action(
    index: &str,
    id: &str,
    routing: Option<&str>,
    source: &[u8],
) -> anyhow::Result<BulkAction> {
    let mut metadata = Map::new();
    metadata.insert("_index".to_owned(), Value::String(index.to_owned()));
    metadata.insert("_id".to_owned(), Value::String(id.to_owned()));
    if let Some(routing) = routing {
        metadata.insert("routing".to_owned(), Value::String(routing.to_owned()));
    }
    let mut operation = Map::new();
    operation.insert("index".to_owned(), Value::Object(metadata));
    let metadata = serde_json::to_vec(&Value::Object(operation))?;
    let mut ndjson = Vec::with_capacity(metadata.len() + source.len() + 2);
    ndjson.extend_from_slice(&metadata);
    ndjson.push(b'\n');
    ndjson.extend_from_slice(source);
    ndjson.push(b'\n');
    let bytes = ndjson.len();
    Ok(BulkAction {
        id: Arc::from(id),
        ndjson: Arc::from(ndjson),
        bytes,
    })
}

fn composite_document_id(
    batch: &RecordBatch,
    schema: &DatasetSchema,
    primary_keys: &[usize],
    row: usize,
) -> anyhow::Result<String> {
    let mut encoded = Vec::new();
    for position in primary_keys {
        let array = batch.column(*position).as_ref();
        anyhow::ensure!(
            !array.is_null(row),
            "OpenSearch primary-key column '{}' must not be null",
            batch.schema().field(*position).name()
        );
        let type_tag = stable_type_tag(
            &schema.columns[*position].data_type,
            schema.columns[*position].arrow_extension_name,
        )?;
        append_bytes(&mut encoded, type_tag.as_bytes())?;
        let value = stable_scalar_bytes(array, row, schema.columns[*position].arrow_extension_name)?;
        append_bytes(&mut encoded, &value)?;
    }
    Ok(URL_SAFE_NO_PAD.encode(encoded))
}

#[allow(
    clippy::too_many_lines,
    reason = "stable primary-key bytes cover each supported scalar explicitly"
)]
fn stable_scalar_bytes(
    array: &dyn Array,
    row: usize,
    extension: Option<&str>,
) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(
        extension.is_none(),
        "OpenSearch primary-key Arrow extensions are not supported"
    );
    macro_rules! bytes {
        ($array:ty) => {
            downcast::<$array>(array)?.value(row).to_be_bytes().to_vec()
        };
    }
    Ok(match array.data_type() {
        DataType::Boolean => vec![u8::from(downcast::<BooleanArray>(array)?.value(row))],
        DataType::Int8 => downcast::<Int8Array>(array)?.value(row).to_be_bytes().to_vec(),
        DataType::Int16 => bytes!(Int16Array),
        DataType::Int32 => bytes!(Int32Array),
        DataType::Int64 => bytes!(Int64Array),
        DataType::UInt8 => downcast::<UInt8Array>(array)?.value(row).to_be_bytes().to_vec(),
        DataType::UInt16 => bytes!(UInt16Array),
        DataType::UInt32 => bytes!(UInt32Array),
        DataType::UInt64 => bytes!(UInt64Array),
        DataType::Float32 => {
            let value = downcast::<Float32Array>(array)?.value(row);
            anyhow::ensure!(value.is_finite(), "non-finite primary-key value {value}");
            value.to_bits().to_be_bytes().to_vec()
        }
        DataType::Float64 => {
            let value = downcast::<Float64Array>(array)?.value(row);
            anyhow::ensure!(value.is_finite(), "non-finite primary-key value {value}");
            value.to_bits().to_be_bytes().to_vec()
        }
        DataType::Utf8 => downcast::<StringArray>(array)?.value(row).as_bytes().to_vec(),
        DataType::LargeUtf8 => downcast::<LargeStringArray>(array)?.value(row).as_bytes().to_vec(),
        DataType::Binary => downcast::<BinaryArray>(array)?.value(row).to_vec(),
        DataType::LargeBinary => downcast::<LargeBinaryArray>(array)?.value(row).to_vec(),
        DataType::FixedSizeBinary(_) => {
            downcast::<FixedSizeBinaryArray>(array)?.value(row).to_vec()
        }
        DataType::Decimal128(_, _) => bytes!(Decimal128Array),
        DataType::Decimal256(_, _) => bytes!(Decimal256Array),
        DataType::Date32 => bytes!(Date32Array),
        DataType::Date64 => bytes!(Date64Array),
        DataType::Timestamp(unit, _) => timestamp_value(array, row, unit)?.to_be_bytes().to_vec(),
        DataType::Duration(unit) => duration_value(array, row, unit)?.to_be_bytes().to_vec(),
        other => anyhow::bail!("unsupported primary-key Arrow type {other:?}"),
    })
}

pub(super) fn stable_type_tag(
    data_type: &DataType,
    extension: Option<&str>,
) -> anyhow::Result<String> {
    let base = match data_type {
        DataType::Boolean => "bool".to_owned(),
        DataType::Int8 => "i8".to_owned(),
        DataType::Int16 => "i16".to_owned(),
        DataType::Int32 => "i32".to_owned(),
        DataType::Int64 => "i64".to_owned(),
        DataType::UInt8 => "u8".to_owned(),
        DataType::UInt16 => "u16".to_owned(),
        DataType::UInt32 => "u32".to_owned(),
        DataType::UInt64 => "u64".to_owned(),
        DataType::Float32 => "f32".to_owned(),
        DataType::Float64 => "f64".to_owned(),
        DataType::Utf8 => "utf8".to_owned(),
        DataType::LargeUtf8 => "large_utf8".to_owned(),
        DataType::Binary => "binary".to_owned(),
        DataType::LargeBinary => "large_binary".to_owned(),
        DataType::FixedSizeBinary(width) => format!("fixed_binary:{width}"),
        DataType::Decimal128(precision, scale) => format!("decimal128:{precision}:{scale}"),
        DataType::Decimal256(precision, scale) => format!("decimal256:{precision}:{scale}"),
        DataType::Date32 => "date32".to_owned(),
        DataType::Date64 => "date64".to_owned(),
        DataType::Timestamp(unit, timezone) => format!(
            "timestamp:{}:{}",
            stable_time_unit(unit),
            timezone.as_deref().unwrap_or("")
        ),
        DataType::Duration(unit) => format!("duration:{}", stable_time_unit(unit)),
        other => anyhow::bail!("unsupported primary-key Arrow type {other:?}"),
    };
    Ok(match extension {
        Some(extension) => format!("{base}:extension:{extension}"),
        None => base,
    })
}

const fn stable_time_unit(unit: &TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Second => "s",
        TimeUnit::Millisecond => "ms",
        TimeUnit::Microsecond => "us",
        TimeUnit::Nanosecond => "ns",
    }
}

fn append_bytes(output: &mut Vec<u8>, value: &[u8]) -> anyhow::Result<()> {
    let length = u64::try_from(value.len())?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value);
    Ok(())
}

pub(super) fn validate_document_id(id: &str) -> anyhow::Result<()> {
    anyhow::ensure!(!id.is_empty(), "OpenSearch document _id must not be empty");
    anyhow::ensure!(
        id.len() <= MAX_DOCUMENT_ID_BYTES,
        "OpenSearch document _id is {} bytes, exceeding the 512-byte limit",
        id.len()
    );
    Ok(())
}

#[allow(clippy::too_many_lines, reason = "all supported Arrow scalars are validated at one lossless boundary")]
fn arrow_json_value(
    array: &dyn Array,
    row: usize,
    extension: Option<&str>,
) -> anyhow::Result<Value> {
    if array.is_null(row) {
        return Ok(Value::Null);
    }
    if extension == Some(ARROW_JSON_EXTENSION_NAME) {
        let text = match array.data_type() {
            DataType::Utf8 => downcast::<StringArray>(array)?.value(row),
            DataType::LargeUtf8 => downcast::<LargeStringArray>(array)?.value(row),
            other => anyhow::bail!("arrow.json extension requires Utf8, got {other:?}"),
        };
        return serde_json::from_str(text)
            .map_err(|error| anyhow::anyhow!("arrow.json value is invalid: {error}"));
    }
    anyhow::ensure!(extension.is_none(), "unsupported Arrow extension '{:?}'", extension);
    macro_rules! integer {
        ($array:ty) => {
            Value::Number(Number::from(downcast::<$array>(array)?.value(row)))
        };
    }
    Ok(match array.data_type() {
        DataType::Boolean => Value::Bool(downcast::<BooleanArray>(array)?.value(row)),
        DataType::Int8 => integer!(Int8Array),
        DataType::Int16 => integer!(Int16Array),
        DataType::Int32 => integer!(Int32Array),
        DataType::Int64 => integer!(Int64Array),
        DataType::UInt8 => integer!(UInt8Array),
        DataType::UInt16 => integer!(UInt16Array),
        DataType::UInt32 => integer!(UInt32Array),
        DataType::UInt64 => integer!(UInt64Array),
        DataType::Float32 => finite_number(f64::from(downcast::<Float32Array>(array)?.value(row)))?,
        DataType::Float64 => finite_number(downcast::<Float64Array>(array)?.value(row))?,
        DataType::Utf8 => Value::String(downcast::<StringArray>(array)?.value(row).to_owned()),
        DataType::LargeUtf8 => {
            Value::String(downcast::<LargeStringArray>(array)?.value(row).to_owned())
        }
        DataType::Binary => Value::String(STANDARD.encode(downcast::<BinaryArray>(array)?.value(row))),
        DataType::LargeBinary => {
            Value::String(STANDARD.encode(downcast::<LargeBinaryArray>(array)?.value(row)))
        }
        DataType::FixedSizeBinary(_) => {
            Value::String(STANDARD.encode(downcast::<FixedSizeBinaryArray>(array)?.value(row)))
        }
        DataType::Decimal128(_, scale) => Value::String(decimal_text(
            &downcast::<Decimal128Array>(array)?.value(row).to_string(),
            *scale,
        )),
        DataType::Decimal256(_, scale) => Value::String(decimal_text(
            &downcast::<Decimal256Array>(array)?.value(row).to_string(),
            *scale,
        )),
        DataType::Date32 => integer!(Date32Array),
        DataType::Date64 => integer!(Date64Array),
        DataType::Timestamp(unit, _) => Value::Number(Number::from(timestamp_value(array, row, unit)?)),
        DataType::Duration(unit) => Value::Number(Number::from(duration_value(array, row, unit)?)),
        other => anyhow::bail!("unsupported Arrow type {other:?}"),
    })
}

fn finite_number(value: f64) -> anyhow::Result<Value> {
    anyhow::ensure!(value.is_finite(), "non-finite floating-point value {value}");
    Ok(Value::Number(
        Number::from_f64(value).ok_or_else(|| anyhow::anyhow!("invalid floating-point value"))?,
    ))
}

fn timestamp_value(array: &dyn Array, row: usize, unit: &TimeUnit) -> anyhow::Result<i64> {
    Ok(match unit {
        TimeUnit::Second => downcast::<TimestampSecondArray>(array)?.value(row),
        TimeUnit::Millisecond => downcast::<TimestampMillisecondArray>(array)?.value(row),
        TimeUnit::Microsecond => downcast::<TimestampMicrosecondArray>(array)?.value(row),
        TimeUnit::Nanosecond => downcast::<TimestampNanosecondArray>(array)?.value(row),
    })
}

fn duration_value(array: &dyn Array, row: usize, unit: &TimeUnit) -> anyhow::Result<i64> {
    Ok(match unit {
        TimeUnit::Second => downcast::<DurationSecondArray>(array)?.value(row),
        TimeUnit::Millisecond => downcast::<DurationMillisecondArray>(array)?.value(row),
        TimeUnit::Microsecond => downcast::<DurationMicrosecondArray>(array)?.value(row),
        TimeUnit::Nanosecond => downcast::<DurationNanosecondArray>(array)?.value(row),
    })
}

fn decimal_text(unscaled: &str, scale: i8) -> String {
    if scale == 0 {
        return unscaled.to_owned();
    }
    if scale < 0 {
        return format!("{unscaled}{}", "0".repeat(usize::from(scale.unsigned_abs())));
    }
    let negative = unscaled.starts_with('-');
    let digits = unscaled.strip_prefix('-').unwrap_or(unscaled);
    let scale = usize::from(scale.unsigned_abs());
    let mut result = String::new();
    if negative {
        result.push('-');
    }
    if digits.len() <= scale {
        result.push_str("0.");
        result.push_str(&"0".repeat(scale - digits.len()));
        result.push_str(digits);
    } else {
        let split = digits.len() - scale;
        result.push_str(&digits[..split]);
        result.push('.');
        result.push_str(&digits[split..]);
    }
    result
}

fn downcast<T: 'static>(array: &dyn Array) -> anyhow::Result<&T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        anyhow::anyhow!(
            "Arrow array implementation does not match declared type {:?}",
            array.data_type()
        )
    })
}
