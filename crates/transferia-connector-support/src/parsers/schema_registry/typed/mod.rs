mod avro;
mod json;
mod protobuf;

use std::sync::Arc;

use arrow::array::{new_empty_array, Array, ArrayRef, ListArray, MapArray, StringArray, StructArray};
use arrow::buffer::OffsetBuffer;
use arrow::compute::concat;
use arrow::datatypes::{DataType, Field};

pub(super) use avro::record as avro_record;
pub(super) use json::{record as json_record, parse as parse_json};
pub(super) use protobuf::record as protobuf_record;

fn concatenate(data_type: &DataType, arrays: &[ArrayRef]) -> anyhow::Result<ArrayRef> {
    if arrays.is_empty() {
        return Ok(new_empty_array(data_type));
    }
    Ok(concat(&arrays.iter().map(AsRef::as_ref).collect::<Vec<_>>())?)
}

fn list(field: Arc<Field>, values: Vec<ArrayRef>) -> anyhow::Result<ArrayRef> {
    let len = i32::try_from(values.len())?;
    let values = concatenate(field.data_type(), &values)?;
    Ok(Arc::new(ListArray::try_new(field, OffsetBuffer::new(vec![0, len].into()), values, None)?))
}

fn string_map(field: Arc<Field>, entries: Vec<(&str, ArrayRef)>) -> anyhow::Result<ArrayRef> {
    let DataType::Struct(fields) = field.data_type() else {
        anyhow::bail!("Arrow map entries must be a struct");
    };
    let keys = Arc::new(StringArray::from_iter_values(entries.iter().map(|(key, _)| *key)));
    let values = entries.iter().map(|(_, value)| Arc::clone(value)).collect::<Vec<_>>();
    let values = concatenate(fields[1].data_type(), &values)?;
    let entries = StructArray::try_new(fields.clone(), vec![keys, values], None)?;
    let len = i32::try_from(entries.len())?;
    Ok(Arc::new(MapArray::try_new(field, OffsetBuffer::new(vec![0, len].into()), entries, None, false)?))
}
