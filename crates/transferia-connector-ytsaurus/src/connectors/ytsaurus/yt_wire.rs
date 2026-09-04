#![allow(
    clippy::expect_used,
    clippy::float_cmp,
    clippy::needless_pass_by_value,
    clippy::unnecessary_wraps,
    reason = "wire widths are checked before conversion, exact float round-trips enforce losslessness, and builders share a fallible API"
)]

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, BooleanBuilder, Date32Array,
    Date32Builder, Date64Array, Date64Builder, Float32Array, Float32Builder, Float64Array,
    Float64Builder, Int16Array, Int16Builder, Int32Array, Int32Builder, Int64Array, Int64Builder,
    Int8Array, Int8Builder, LargeBinaryArray, LargeStringArray, StringArray, StringBuilder,
    TimestampMicrosecondArray, TimestampMicrosecondBuilder, TimestampSecondArray, UInt16Array,
    UInt16Builder, UInt32Array, UInt32Builder, UInt64Array, UInt64Builder, UInt8Array,
    UInt8Builder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use transferia_core::data::schema::DatasetSchema;

use super::schema::{YT_DATE_UPPER_BOUND_DAYS, YT_TIMESTAMP_UPPER_BOUND_MICROSECONDS};

pub(super) const VALUE_INT64: u8 = 0x03;
pub(super) const VALUE_UINT64: u8 = 0x04;
pub(super) const VALUE_DOUBLE: u8 = 0x05;
pub(super) const VALUE_BOOLEAN: u8 = 0x06;
pub(super) const VALUE_STRING: u8 = 0x10;
const VALUE_NULL: u8 = 0x02;
const MAX_WIRE_VALUE_BYTES: usize = 16 * 1024 * 1024;

pub(super) struct EncodedWireBatch {
    pub column_names: Vec<String>,
    pub payload: Bytes,
}

pub(super) fn encode_wire_batch(batch: &RecordBatch) -> anyhow::Result<EncodedWireBatch> {
    anyhow::ensure!(
        u16::try_from(batch.num_columns()).is_ok(),
        "YTsaurus wire rowset has too many columns"
    );
    let columns = batch
        .columns()
        .iter()
        .map(|array| WireColumn::new(array.as_ref()))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let header_bytes = batch
        .num_rows()
        .checked_mul(8 + batch.num_columns() * 8)
        .ok_or_else(|| anyhow::anyhow!("YTsaurus wire rowset size overflow"))?;
    let capacity = batch
        .get_array_memory_size()
        .checked_add(header_bytes)
        .and_then(|size| size.checked_add(8))
        .ok_or_else(|| anyhow::anyhow!("YTsaurus wire rowset size overflow"))?;
    let mut output = Vec::with_capacity(capacity);
    output.extend_from_slice(&u64::try_from(batch.num_rows())?.to_le_bytes());
    for row in 0..batch.num_rows() {
        output.extend_from_slice(&u64::try_from(columns.len())?.to_le_bytes());
        for (id, column) in columns.iter().enumerate() {
            column.append(u16::try_from(id)?, row, &mut output)?;
        }
    }
    Ok(EncodedWireBatch {
        column_names: batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect(),
        payload: Bytes::from(output),
    })
}

enum WireColumn<'a> {
    Int8(&'a Int8Array),
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    UInt8(&'a UInt8Array),
    UInt16(&'a UInt16Array),
    UInt32(&'a UInt32Array),
    UInt64(&'a UInt64Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Boolean(&'a BooleanArray),
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Binary(&'a BinaryArray),
    LargeBinary(&'a LargeBinaryArray),
    Date32(&'a Date32Array),
    Date64(&'a Date64Array),
    TimestampSecond(&'a TimestampSecondArray),
    TimestampMicrosecond(&'a TimestampMicrosecondArray),
}

macro_rules! downcast_wire {
    ($array:expr_2021, $ty:ty, $variant:ident) => {
        Self::$variant(
            $array
                .as_any()
                .downcast_ref::<$ty>()
                .ok_or_else(|| anyhow::anyhow!("Arrow array type does not match schema"))?,
        )
    };
}

impl<'a> WireColumn<'a> {
    fn new(array: &'a dyn Array) -> anyhow::Result<Self> {
        Ok(match array.data_type() {
            DataType::Int8 => downcast_wire!(array, Int8Array, Int8),
            DataType::Int16 => downcast_wire!(array, Int16Array, Int16),
            DataType::Int32 => downcast_wire!(array, Int32Array, Int32),
            DataType::Int64 => downcast_wire!(array, Int64Array, Int64),
            DataType::UInt8 => downcast_wire!(array, UInt8Array, UInt8),
            DataType::UInt16 => downcast_wire!(array, UInt16Array, UInt16),
            DataType::UInt32 => downcast_wire!(array, UInt32Array, UInt32),
            DataType::UInt64 => downcast_wire!(array, UInt64Array, UInt64),
            DataType::Float32 => downcast_wire!(array, Float32Array, Float32),
            DataType::Float64 => downcast_wire!(array, Float64Array, Float64),
            DataType::Boolean => downcast_wire!(array, BooleanArray, Boolean),
            DataType::Utf8 => downcast_wire!(array, StringArray, Utf8),
            DataType::LargeUtf8 => downcast_wire!(array, LargeStringArray, LargeUtf8),
            DataType::Binary => downcast_wire!(array, BinaryArray, Binary),
            DataType::LargeBinary => downcast_wire!(array, LargeBinaryArray, LargeBinary),
            DataType::Date32 => downcast_wire!(array, Date32Array, Date32),
            DataType::Date64 => downcast_wire!(array, Date64Array, Date64),
            DataType::Timestamp(TimeUnit::Second, None) => {
                downcast_wire!(array, TimestampSecondArray, TimestampSecond)
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                downcast_wire!(array, TimestampMicrosecondArray, TimestampMicrosecond)
            }
            other => anyhow::bail!("YTsaurus wire encoder does not support Arrow type {other:?}"),
        })
    }

    fn append(&self, id: u16, row: usize, output: &mut Vec<u8>) -> anyhow::Result<()> {
        if self.is_null(row) {
            write_wire_header(output, id, VALUE_NULL, 0);
            return Ok(());
        }
        match self {
            Self::Int8(array) => {
                write_wire_scalar(output, id, VALUE_INT64, i64::from(array.value(row)) as u64);
            }
            Self::Int16(array) => {
                write_wire_scalar(output, id, VALUE_INT64, i64::from(array.value(row)) as u64);
            }
            Self::Int32(array) => {
                write_wire_scalar(output, id, VALUE_INT64, i64::from(array.value(row)) as u64);
            }
            Self::Int64(array) => {
                write_wire_scalar(output, id, VALUE_INT64, array.value(row) as u64);
            }
            Self::UInt8(array) => {
                write_wire_scalar(output, id, VALUE_UINT64, u64::from(array.value(row)));
            }
            Self::UInt16(array) => {
                write_wire_scalar(output, id, VALUE_UINT64, u64::from(array.value(row)));
            }
            Self::UInt32(array) => {
                write_wire_scalar(output, id, VALUE_UINT64, u64::from(array.value(row)));
            }
            Self::UInt64(array) => write_wire_scalar(output, id, VALUE_UINT64, array.value(row)),
            Self::Float32(array) => write_wire_scalar(
                output,
                id,
                VALUE_DOUBLE,
                f64::from(array.value(row)).to_bits(),
            ),
            Self::Float64(array) => {
                write_wire_scalar(output, id, VALUE_DOUBLE, array.value(row).to_bits());
            }
            Self::Boolean(array) => {
                write_wire_scalar(output, id, VALUE_BOOLEAN, u64::from(array.value(row)));
            }
            Self::Utf8(array) => write_wire_bytes(output, id, array.value(row).as_bytes())?,
            Self::LargeUtf8(array) => write_wire_bytes(output, id, array.value(row).as_bytes())?,
            Self::Binary(array) => write_wire_bytes(output, id, array.value(row))?,
            Self::LargeBinary(array) => write_wire_bytes(output, id, array.value(row))?,
            Self::Date32(array) => {
                let days = array.value(row);
                anyhow::ensure!(
                    (0..YT_DATE_UPPER_BOUND_DAYS).contains(&days),
                    "YTsaurus date must be in the supported [0, {YT_DATE_UPPER_BOUND_DAYS}) day range"
                );
                let value = u64::try_from(days)?;
                write_wire_scalar(output, id, VALUE_UINT64, value);
            }
            Self::Date64(array) => {
                let milliseconds = array.value(row);
                anyhow::ensure!(
                    milliseconds >= 0 && milliseconds % 1_000 == 0,
                    "YTsaurus datetime requires a non-negative whole-second value"
                );
                write_wire_scalar(
                    output,
                    id,
                    VALUE_UINT64,
                    u64::try_from(milliseconds / 1_000)?,
                );
            }
            Self::TimestampSecond(array) => {
                let microseconds = array.value(row).checked_mul(1_000_000).ok_or_else(|| {
                    anyhow::anyhow!(
                        "YTsaurus timestamp seconds value cannot be widened to microseconds"
                    )
                })?;
                anyhow::ensure!(
                    (0..YT_TIMESTAMP_UPPER_BOUND_MICROSECONDS).contains(&microseconds),
                    "YTsaurus timestamp is outside the supported microsecond range"
                );
                write_wire_scalar(output, id, VALUE_UINT64, u64::try_from(microseconds)?);
            }
            Self::TimestampMicrosecond(array) => {
                let microseconds = array.value(row);
                anyhow::ensure!(
                    (0..YT_TIMESTAMP_UPPER_BOUND_MICROSECONDS).contains(&microseconds),
                    "YTsaurus timestamp is outside the supported microsecond range"
                );
                write_wire_scalar(output, id, VALUE_UINT64, u64::try_from(microseconds)?);
            }
        }
        Ok(())
    }

    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Int8(array) => array.is_null(row),
            Self::Int16(array) => array.is_null(row),
            Self::Int32(array) => array.is_null(row),
            Self::Int64(array) => array.is_null(row),
            Self::UInt8(array) => array.is_null(row),
            Self::UInt16(array) => array.is_null(row),
            Self::UInt32(array) => array.is_null(row),
            Self::UInt64(array) => array.is_null(row),
            Self::Float32(array) => array.is_null(row),
            Self::Float64(array) => array.is_null(row),
            Self::Boolean(array) => array.is_null(row),
            Self::Utf8(array) => array.is_null(row),
            Self::LargeUtf8(array) => array.is_null(row),
            Self::Binary(array) => array.is_null(row),
            Self::LargeBinary(array) => array.is_null(row),
            Self::Date32(array) => array.is_null(row),
            Self::Date64(array) => array.is_null(row),
            Self::TimestampSecond(array) => array.is_null(row),
            Self::TimestampMicrosecond(array) => array.is_null(row),
        }
    }
}

fn write_wire_header(output: &mut Vec<u8>, id: u16, value_type: u8, length: u32) {
    output.extend_from_slice(&id.to_le_bytes());
    output.push(value_type);
    output.push(0);
    output.extend_from_slice(&length.to_le_bytes());
}

fn write_wire_scalar(output: &mut Vec<u8>, id: u16, value_type: u8, value: u64) {
    write_wire_header(output, id, value_type, 0);
    output.extend_from_slice(&value.to_le_bytes());
}

fn write_wire_bytes(output: &mut Vec<u8>, id: u16, value: &[u8]) -> anyhow::Result<()> {
    anyhow::ensure!(
        value.len() <= MAX_WIRE_VALUE_BYTES,
        "YTsaurus wire value is {} bytes, exceeding the protocol limit of {MAX_WIRE_VALUE_BYTES}",
        value.len()
    );
    write_wire_header(output, id, VALUE_STRING, u32::try_from(value.len())?);
    output.extend_from_slice(value);
    let padding = (8 - value.len() % 8) % 8;
    output.resize(output.len() + padding, 0);
    Ok(())
}

pub(super) struct YtWireDecoder {
    dataset_schema: DatasetSchema,
    arrow_schema: Arc<Schema>,
    name_table: Vec<Option<usize>>,
}

impl YtWireDecoder {
    pub(super) fn new(schema: &DatasetSchema) -> Self {
        let fields = schema
            .columns
            .iter()
            .map(|column| {
                Field::new(&column.name, column.data_type.clone(), column.nullable)
                    .with_metadata(column.arrow_metadata())
            })
            .collect::<Vec<_>>();
        Self {
            dataset_schema: schema.clone(),
            arrow_schema: Arc::new(Schema::new(fields)),
            name_table: Vec::new(),
        }
    }

    pub(super) fn decode(
        &mut self,
        name_table_entries: &[String],
        payload: Bytes,
    ) -> anyhow::Result<RecordBatch> {
        self.extend_name_table(name_table_entries)?;
        let mut cursor = WireCursor::new(&payload);
        let row_count = cursor.read_count("row count")?;
        let mut builders = self
            .dataset_schema
            .columns
            .iter()
            .map(|column| ColumnBuilder::new(&column.data_type, row_count))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let mut seen = vec![false; builders.len()];

        for row_index in 0..row_count {
            seen.fill(false);
            let value_count = cursor.read_count("row value count")?;
            for _ in 0..value_count {
                let header = cursor.read_u64("value header")?;
                let name_table_id = usize::from((header & 0xffff) as u16);
                let value_type = ((header >> 16) & 0xff) as u8;
                let flags = ((header >> 24) & 0xff) as u8;
                let string_length = usize::try_from(header >> 32)?;
                anyhow::ensure!(
                    flags == 0,
                    "YTsaurus wire value uses unsupported flags {flags:#x}"
                );
                let schema_index = self
                    .name_table
                    .get(name_table_id)
                    .copied()
                    .flatten()
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "YTsaurus wire value refers to unknown name-table id {name_table_id}"
                        )
                    })?;
                anyhow::ensure!(
                    !seen[schema_index],
                    "YTsaurus wire row {row_index} repeats column '{}'",
                    self.dataset_schema.columns[schema_index].name
                );
                seen[schema_index] = true;
                match value_type {
                    VALUE_NULL => builders[schema_index].append_null()?,
                    VALUE_STRING => {
                        let value = cursor.read_aligned(string_length, "string value")?;
                        builders[schema_index].append_string(value)?;
                    }
                    VALUE_INT64 | VALUE_UINT64 | VALUE_DOUBLE | VALUE_BOOLEAN => {
                        let value = cursor.read_u64("fixed-width value")?;
                        builders[schema_index].append_fixed(value_type, value)?;
                    }
                    other => anyhow::bail!(
                        "YTsaurus wire value uses unsupported physical type {other:#x}"
                    ),
                }
            }
            for (index, present) in seen.iter().copied().enumerate() {
                if !present {
                    anyhow::ensure!(
                        self.dataset_schema.columns[index].nullable,
                        "YTsaurus wire row {row_index} omits required column '{}'",
                        self.dataset_schema.columns[index].name
                    );
                    builders[index].append_null()?;
                }
            }
        }
        cursor.finish()?;
        let arrays = builders
            .into_iter()
            .map(ColumnBuilder::finish)
            .collect::<Vec<_>>();
        Ok(RecordBatch::try_new(
            Arc::clone(&self.arrow_schema),
            arrays,
        )?)
    }

    fn extend_name_table(&mut self, entries: &[String]) -> anyhow::Result<()> {
        for name in entries {
            let schema_index = self
                .dataset_schema
                .columns
                .iter()
                .position(|column| column.name == *name)
                .ok_or_else(|| {
                    anyhow::anyhow!("YTsaurus wire descriptor contains unknown column '{name}'")
                })?;
            self.name_table.push(Some(schema_index));
        }
        Ok(())
    }
}

pub(super) fn count_wire_rows(payload: &Bytes) -> anyhow::Result<u64> {
    anyhow::ensure!(
        payload.len() >= 8,
        "YTsaurus wire rowset is shorter than its row count"
    );
    Ok(u64::from_le_bytes(
        payload[..8].try_into().expect("eight checked bytes"),
    ))
}

struct WireCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> WireCursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_count(&mut self, what: &str) -> anyhow::Result<usize> {
        let value = self.read_u64(what)?;
        anyhow::ensure!(value != u64::MAX, "YTsaurus wire {what} is null");
        usize::try_from(value).map_err(Into::into)
    }

    fn read_u64(&mut self, what: &str) -> anyhow::Result<u64> {
        let end = self
            .offset
            .checked_add(8)
            .ok_or_else(|| anyhow::anyhow!("YTsaurus wire {what} offset overflow"))?;
        anyhow::ensure!(end <= self.bytes.len(), "YTsaurus wire {what} is truncated");
        let value = u64::from_le_bytes(
            self.bytes[self.offset..end]
                .try_into()
                .expect("eight checked bytes"),
        );
        self.offset = end;
        Ok(value)
    }

    fn read_aligned(&mut self, length: usize, what: &str) -> anyhow::Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| anyhow::anyhow!("YTsaurus wire {what} offset overflow"))?;
        anyhow::ensure!(end <= self.bytes.len(), "YTsaurus wire {what} is truncated");
        let value = &self.bytes[self.offset..end];
        let aligned = end
            .checked_add(7)
            .map(|value| value & !7)
            .ok_or_else(|| anyhow::anyhow!("YTsaurus wire {what} alignment overflow"))?;
        anyhow::ensure!(
            aligned <= self.bytes.len(),
            "YTsaurus wire {what} padding is truncated"
        );
        self.offset = aligned;
        Ok(value)
    }

    fn finish(self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.offset == self.bytes.len(),
            "YTsaurus wire rowset contains {} trailing bytes",
            self.bytes.len() - self.offset
        );
        Ok(())
    }
}

pub(super) enum ColumnBuilder {
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    UInt8(UInt8Builder),
    UInt16(UInt16Builder),
    UInt32(UInt32Builder),
    UInt64(UInt64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
    Date32(Date32Builder),
    Date64(Date64Builder),
    TimestampMicrosecond(TimestampMicrosecondBuilder),
}

impl ColumnBuilder {
    pub(super) fn new(data_type: &DataType, capacity: usize) -> anyhow::Result<Self> {
        Ok(match data_type {
            DataType::Int8 => Self::Int8(Int8Builder::with_capacity(capacity)),
            DataType::Int16 => Self::Int16(Int16Builder::with_capacity(capacity)),
            DataType::Int32 => Self::Int32(Int32Builder::with_capacity(capacity)),
            DataType::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            DataType::UInt8 => Self::UInt8(UInt8Builder::with_capacity(capacity)),
            DataType::UInt16 => Self::UInt16(UInt16Builder::with_capacity(capacity)),
            DataType::UInt32 => Self::UInt32(UInt32Builder::with_capacity(capacity)),
            DataType::UInt64 => Self::UInt64(UInt64Builder::with_capacity(capacity)),
            DataType::Float32 => Self::Float32(Float32Builder::with_capacity(capacity)),
            DataType::Float64 => Self::Float64(Float64Builder::with_capacity(capacity)),
            DataType::Boolean => Self::Boolean(BooleanBuilder::with_capacity(capacity)),
            DataType::Utf8 => Self::Utf8(StringBuilder::new()),
            DataType::Binary => Self::Binary(BinaryBuilder::new()),
            DataType::Date32 => Self::Date32(Date32Builder::with_capacity(capacity)),
            DataType::Date64 => Self::Date64(Date64Builder::with_capacity(capacity)),
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                Self::TimestampMicrosecond(TimestampMicrosecondBuilder::with_capacity(capacity))
            }
            other => anyhow::bail!("YTsaurus wire decoder does not support Arrow type {other:?}"),
        })
    }

    pub(super) fn append_null(&mut self) -> anyhow::Result<()> {
        match self {
            Self::Int8(builder) => builder.append_null(),
            Self::Int16(builder) => builder.append_null(),
            Self::Int32(builder) => builder.append_null(),
            Self::Int64(builder) => builder.append_null(),
            Self::UInt8(builder) => builder.append_null(),
            Self::UInt16(builder) => builder.append_null(),
            Self::UInt32(builder) => builder.append_null(),
            Self::UInt64(builder) => builder.append_null(),
            Self::Float32(builder) => builder.append_null(),
            Self::Float64(builder) => builder.append_null(),
            Self::Boolean(builder) => builder.append_null(),
            Self::Utf8(builder) => builder.append_null(),
            Self::Binary(builder) => builder.append_null(),
            Self::Date32(builder) => builder.append_null(),
            Self::Date64(builder) => builder.append_null(),
            Self::TimestampMicrosecond(builder) => builder.append_null(),
        }
        Ok(())
    }

    pub(super) fn append_fixed(&mut self, physical_type: u8, raw: u64) -> anyhow::Result<()> {
        let signed = i64::from_le_bytes(raw.to_le_bytes());
        match self {
            Self::Int8(builder) if physical_type == VALUE_INT64 => builder.append_value(signed.try_into()?),
            Self::Int16(builder) if physical_type == VALUE_INT64 => builder.append_value(signed.try_into()?),
            Self::Int32(builder) if physical_type == VALUE_INT64 => builder.append_value(signed.try_into()?),
            Self::Int64(builder) if physical_type == VALUE_INT64 => builder.append_value(signed),
            Self::UInt8(builder) if physical_type == VALUE_UINT64 => builder.append_value(raw.try_into()?),
            Self::UInt16(builder) if physical_type == VALUE_UINT64 => builder.append_value(raw.try_into()?),
            Self::UInt32(builder) if physical_type == VALUE_UINT64 => builder.append_value(raw.try_into()?),
            Self::UInt64(builder) if physical_type == VALUE_UINT64 => builder.append_value(raw),
            Self::Float32(builder) if physical_type == VALUE_DOUBLE => {
                let value = f64::from_bits(raw);
                let narrowed = value as f32;
                anyhow::ensure!(
                    (f64::from(narrowed) == value) || (narrowed.is_nan() && value.is_nan()),
                    "YTsaurus float value cannot be represented losslessly as Float32"
                );
                builder.append_value(narrowed);
            }
            Self::Float64(builder) if physical_type == VALUE_DOUBLE => {
                builder.append_value(f64::from_bits(raw));
            }
            Self::Boolean(builder) if physical_type == VALUE_BOOLEAN => {
                anyhow::ensure!(raw <= 1, "YTsaurus boolean payload is neither 0 nor 1");
                builder.append_value(raw == 1);
            }
            Self::Date32(builder) if physical_type == VALUE_UINT64 => {
                builder.append_value(raw.try_into()?);
            }
            Self::Date64(builder) if physical_type == VALUE_UINT64 => {
                let seconds = i64::try_from(raw)?;
                builder.append_value(seconds.checked_mul(1_000).ok_or_else(|| {
                    anyhow::anyhow!("YTsaurus datetime milliseconds overflow")
                })?);
            }
            Self::TimestampMicrosecond(builder) if physical_type == VALUE_UINT64 => {
                builder.append_value(raw.try_into()?);
            }
            _ => anyhow::bail!(
                "YTsaurus physical value type {physical_type:#x} does not match the discovered Arrow column"
            ),
        }
        Ok(())
    }

    pub(super) fn append_string(&mut self, value: &[u8]) -> anyhow::Result<()> {
        match self {
            Self::Utf8(builder) => builder.append_value(std::str::from_utf8(value)?),
            Self::Binary(builder) => builder.append_value(value),
            _ => anyhow::bail!("YTsaurus string value does not match the discovered Arrow column"),
        }
        Ok(())
    }

    pub(super) fn finish(self) -> ArrayRef {
        match self {
            Self::Int8(mut builder) => Arc::new(builder.finish()),
            Self::Int16(mut builder) => Arc::new(builder.finish()),
            Self::Int32(mut builder) => Arc::new(builder.finish()),
            Self::Int64(mut builder) => Arc::new(builder.finish()),
            Self::UInt8(mut builder) => Arc::new(builder.finish()),
            Self::UInt16(mut builder) => Arc::new(builder.finish()),
            Self::UInt32(mut builder) => Arc::new(builder.finish()),
            Self::UInt64(mut builder) => Arc::new(builder.finish()),
            Self::Float32(mut builder) => Arc::new(builder.finish()),
            Self::Float64(mut builder) => Arc::new(builder.finish()),
            Self::Boolean(mut builder) => Arc::new(builder.finish()),
            Self::Utf8(mut builder) => Arc::new(builder.finish()),
            Self::Binary(mut builder) => Arc::new(builder.finish()),
            Self::Date32(mut builder) => Arc::new(builder.finish()),
            Self::Date64(mut builder) => Arc::new(builder.finish()),
            Self::TimestampMicrosecond(mut builder) => Arc::new(builder.finish()),
        }
    }
}
