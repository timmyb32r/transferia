use std::sync::Arc;

use arrow::array::*;
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use prost_reflect::{FieldDescriptor, Kind, MapKey, MessageDescriptor, ReflectMessage, Value, DynamicMessage};

pub(in crate::parsers::schema_registry) fn record(message: &DynamicMessage) -> anyhow::Result<RecordBatch> {
    anyhow::ensure!(message.unknown_fields().next().is_none(), "Protobuf message contains fields absent from its Registry schema");
    let descriptor = message.descriptor();
    let fields = message_fields(&descriptor, &mut Vec::new())?;
    let arrays = descriptor.fields().zip(&fields).map(|(field, expected)| field_array(message, &field, expected)).collect::<anyhow::Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), arrays, &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(1)))?)
}

fn message_fields(message: &MessageDescriptor, ancestors: &mut Vec<String>) -> anyhow::Result<Vec<Field>> {
    anyhow::ensure!(!ancestors.iter().any(|name| name == message.full_name()), "recursive Protobuf types have no finite Arrow schema");
    ancestors.push(message.full_name().to_owned());
    let fields = message.fields().map(|field| {
        let data_type = if field.is_map() {
            let Kind::Message(entry) = field.kind() else { anyhow::bail!("Protobuf map is not a message") };
            DataType::Map(Arc::new(Field::new("entries", DataType::Struct(message_fields(&entry, ancestors)?.into()), false)), false)
        } else if field.is_list() {
            DataType::List(Arc::new(Field::new("item", kind_type(&field.kind(), ancestors)?, false)))
        } else { kind_type(&field.kind(), ancestors)? };
        Ok(Field::new(field.name(), data_type, field.supports_presence() && !field.is_required()))
    }).collect::<anyhow::Result<Vec<_>>>()?;
    ancestors.pop();
    Ok(fields)
}

fn kind_type(kind: &Kind, ancestors: &mut Vec<String>) -> anyhow::Result<DataType> {
    Ok(match kind {
        Kind::Bool => DataType::Boolean,
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 | Kind::Enum(_) => DataType::Int32,
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => DataType::Int64,
        Kind::Uint32 | Kind::Fixed32 => DataType::UInt32,
        Kind::Uint64 | Kind::Fixed64 => DataType::UInt64,
        Kind::Float => DataType::Float32,
        Kind::Double => DataType::Float64,
        Kind::String => DataType::Utf8,
        Kind::Bytes => DataType::Binary,
        Kind::Message(message) => DataType::Struct(message_fields(message, ancestors)?.into()),
    })
}

fn field_array(message: &DynamicMessage, field: &FieldDescriptor, expected: &Field) -> anyhow::Result<ArrayRef> {
    if field.supports_presence() && !message.has_field(field) {
        anyhow::ensure!(!field.is_required(), "required Protobuf field '{}' is absent", field.name());
        return Ok(new_null_array(expected.data_type(), 1));
    }
    let value = message.get_field(field);
    if field.is_list() {
        let Value::List(values) = value.as_ref() else { anyhow::bail!("Protobuf repeated field is not a list") };
        let DataType::List(item) = expected.data_type() else { anyhow::bail!("Arrow repeated field is not a list") };
        return super::list(Arc::clone(item), values.iter().map(|value| array(&field.kind(), value)).collect::<anyhow::Result<_>>()?);
    }
    if field.is_map() {
        let Value::Map(values) = value.as_ref() else { anyhow::bail!("Protobuf map value is not a map") };
        let Kind::Message(entry) = field.kind() else { anyhow::bail!("Protobuf map descriptor is not a message") };
        let key = entry.map_entry_key_field();
        let value_field = entry.map_entry_value_field();
        let DataType::Map(entries, _) = expected.data_type() else { anyhow::bail!("Arrow map field is not a map") };
        let DataType::Struct(fields) = entries.data_type() else { anyhow::bail!("Arrow map entries are not a struct") };
        let mut values = values.iter().collect::<Vec<_>>();
        values.sort_by(|left, right| left.0.cmp(right.0));
        let keys = values.iter().map(|(value, _)| {
            let value = match value {
                MapKey::Bool(value) => Value::Bool(*value),
                MapKey::I32(value) => Value::I32(*value), MapKey::I64(value) => Value::I64(*value),
                MapKey::U32(value) => Value::U32(*value), MapKey::U64(value) => Value::U64(*value),
                MapKey::String(value) => Value::String(value.clone()),
            };
            array(&key.kind(), &value)
        }).collect::<anyhow::Result<Vec<_>>>()?;
        let arrays = values.iter().map(|(_, value)| array(&value_field.kind(), value)).collect::<anyhow::Result<Vec<_>>>()?;
        let keys = super::concatenate(fields[0].data_type(), &keys)?;
        let arrays = super::concatenate(fields[1].data_type(), &arrays)?;
        let len = i32::try_from(values.len())?;
        let arrays = StructArray::try_new(fields.clone(), vec![keys, arrays], None)?;
        return Ok(Arc::new(MapArray::try_new(Arc::clone(entries), OffsetBuffer::new(vec![0, len].into()), arrays, None, false)?));
    }
    array(&field.kind(), value.as_ref())
}

fn array(kind: &Kind, value: &Value) -> anyhow::Result<ArrayRef> {
    macro_rules! scalar { ($array:ident, $value:expr) => { Arc::new($array::from(vec![$value])) as ArrayRef }; }
    Ok(match (kind, value) {
        (Kind::Bool, Value::Bool(value)) => scalar!(BooleanArray, *value),
        (Kind::Int32 | Kind::Sint32 | Kind::Sfixed32, Value::I32(value)) => scalar!(Int32Array, *value),
        (Kind::Int64 | Kind::Sint64 | Kind::Sfixed64, Value::I64(value)) => scalar!(Int64Array, *value),
        (Kind::Uint32 | Kind::Fixed32, Value::U32(value)) => scalar!(UInt32Array, *value),
        (Kind::Uint64 | Kind::Fixed64, Value::U64(value)) => scalar!(UInt64Array, *value),
        (Kind::Enum(_), Value::EnumNumber(value)) => scalar!(Int32Array, *value),
        (Kind::Float, Value::F32(value)) => scalar!(Float32Array, *value),
        (Kind::Double, Value::F64(value)) => scalar!(Float64Array, *value),
        (Kind::String, Value::String(value)) => scalar!(StringArray, value.as_str()),
        (Kind::Bytes, Value::Bytes(value)) => scalar!(BinaryArray, value.as_ref()),
        (Kind::Message(_), Value::Message(message)) => Arc::new(StructArray::from(record(message)?)),
        _ => anyhow::bail!("Protobuf value differs from its declared type"),
    })
}
