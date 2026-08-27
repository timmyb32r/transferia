use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Date64Builder, Float32Builder,
    Float64Builder, Int8Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder, UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use transferia_core::data::schema::DatasetSchema;

pub(super) const VALUE_INT64: u8 = 0x03;
pub(super) const VALUE_UINT64: u8 = 0x04;
pub(super) const VALUE_DOUBLE: u8 = 0x05;
pub(super) const VALUE_BOOLEAN: u8 = 0x06;
pub(super) const VALUE_STRING: u8 = 0x10;
const VALUE_NULL: u8 = 0x02;

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
        Ok(RecordBatch::try_new(Arc::clone(&self.arrow_schema), arrays)?)
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
    anyhow::ensure!(payload.len() >= 8, "YTsaurus wire rowset is shorter than its row count");
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
        anyhow::ensure!(aligned <= self.bytes.len(), "YTsaurus wire {what} padding is truncated");
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
            DataType::Timestamp(TimeUnit::Microsecond, None) => Self::TimestampMicrosecond(
                TimestampMicrosecondBuilder::with_capacity(capacity),
            ),
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
