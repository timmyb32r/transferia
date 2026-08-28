#![allow(
    clippy::struct_field_names,
    reason = "protobuf fields intentionally preserve upstream YTsaurus wire-schema names"
)]

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use prost::Message as _;
use transferia_core::data::schema::DatasetSchema;

use super::yt_wire::{ColumnBuilder, VALUE_BOOLEAN, VALUE_DOUBLE, VALUE_INT64, VALUE_UINT64};

pub(super) const EXTENSION_MISC: i32 = 0;
pub(super) const EXTENSION_DATA_BLOCK_META: i32 = 51;
pub(super) const EXTENSION_COLUMN_META: i32 = 58;

const CODEC_NONE: i32 = 0;
const CODEC_LZ4: i32 = 4;
const CODEC_LZ4_HIGH_COMPRESSION: i32 = 5;
const LZ4_SIGNATURE_V1: u32 = (1 << 30) + 1;
const LZ4_SIGNATURE_V2: u32 = (1 << 30) + 2;
const SERIALIZATION_ALIGNMENT: usize = 8;

pub(super) struct ColumnarChunkDecoder {
    arrow_schema: Arc<Schema>,
    dataset_schema: DatasetSchema,
    column_meta: ColumnMetaExt,
    compression_codec: i32,
}

impl ColumnarChunkDecoder {
    pub(super) fn from_meta(meta: &ChunkMeta, schema: &DatasetSchema) -> anyhow::Result<Self> {
        validate_direct_schema(schema)?;
        let misc = MiscExt::decode(required_extension(meta, EXTENSION_MISC)?)?;
        anyhow::ensure!(
            misc.compression_dictionary_id.is_none(),
            "direct YTsaurus reads do not support dictionary-compressed chunks"
        );
        let column_meta = ColumnMetaExt::decode(required_extension(meta, EXTENSION_COLUMN_META)?)?;
        anyhow::ensure!(
            column_meta.columns.len() == schema.columns.len(),
            "YTsaurus chunk metadata has {} columns, but discovery reported {}",
            column_meta.columns.len(),
            schema.columns.len()
        );
        let arrow_schema = Arc::new(Schema::new(
            schema
                .columns
                .iter()
                .map(|column| {
                    Field::new(&column.name, column.data_type.clone(), column.nullable)
                        .with_metadata(column.arrow_metadata())
                })
                .collect::<Vec<_>>(),
        ));
        Ok(Self {
            arrow_schema,
            dataset_schema: schema.clone(),
            column_meta,
            compression_codec: misc.compression_codec.unwrap_or(CODEC_NONE),
        })
    }

    pub(super) fn decode_block(
        &self,
        block_index: i32,
        compressed: &[u8],
        lower_row_index: i64,
        upper_row_index: i64,
    ) -> anyhow::Result<(RecordBatch, usize)> {
        anyhow::ensure!(
            0 <= lower_row_index && lower_row_index <= upper_row_index,
            "invalid YTsaurus block row range [{lower_row_index}, {upper_row_index})"
        );
        let block = decompress_block(self.compression_codec, compressed)?;
        let decoded_bytes = block.len();
        let row_count = usize::try_from(upper_row_index - lower_row_index)?;
        let mut arrays = Vec::with_capacity(self.dataset_schema.columns.len());
        for (column, meta) in self
            .dataset_schema
            .columns
            .iter()
            .zip(&self.column_meta.columns)
        {
            let mut builder = ColumnBuilder::new(&column.data_type, row_count)?;
            let mut appended = 0_usize;
            for segment in meta
                .segments
                .iter()
                .filter(|segment| segment.block_index == block_index)
            {
                let segment_start = segment
                    .chunk_row_count
                    .checked_sub(segment.row_count)
                    .ok_or_else(|| anyhow::anyhow!("YTsaurus segment row range underflow"))?;
                let start = segment_start.max(lower_row_index);
                let end = segment.chunk_row_count.min(upper_row_index);
                if start >= end {
                    continue;
                }
                anyhow::ensure!(
                    start == lower_row_index + i64::try_from(appended)?,
                    "YTsaurus column '{}' has a gap or overlap at row {start}",
                    column.name
                );
                let data = segment_data(&block, segment)?;
                append_segment(
                    &mut builder,
                    &column.data_type,
                    segment,
                    data,
                    usize::try_from(start - segment_start)?,
                    usize::try_from(end - segment_start)?,
                )?;
                appended = appended
                    .checked_add(usize::try_from(end - start)?)
                    .ok_or_else(|| anyhow::anyhow!("YTsaurus decoded row count overflow"))?;
            }
            anyhow::ensure!(
                appended == row_count,
                "YTsaurus column '{}' produced {appended} rows for a {row_count}-row block range",
                column.name
            );
            arrays.push(builder.finish());
        }
        Ok((
            RecordBatch::try_new(Arc::clone(&self.arrow_schema), arrays)?,
            decoded_bytes,
        ))
    }
}

pub(super) fn validate_direct_schema(schema: &DatasetSchema) -> anyhow::Result<()> {
    for column in &schema.columns {
        anyhow::ensure!(
            matches!(
                &column.data_type,
                DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Boolean
                    | DataType::Utf8
                    | DataType::Binary
                    | DataType::Date32
                    | DataType::Date64
                    | DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
            ),
            "direct YTsaurus data-node reader does not support column '{}' with Arrow type {:?}",
            column.name,
            column.data_type,
        );
    }
    Ok(())
}

pub(super) fn data_block_meta(meta: &ChunkMeta) -> anyhow::Result<DataBlockMetaExt> {
    DataBlockMetaExt::decode(required_extension(meta, EXTENSION_DATA_BLOCK_META)?)
        .map_err(Into::into)
}

pub(super) fn has_extension(meta: &ChunkMeta, tag: i32) -> bool {
    extension(meta, tag).is_some()
}

fn required_extension(meta: &ChunkMeta, tag: i32) -> anyhow::Result<Bytes> {
    extension(meta, tag)
        .ok_or_else(|| anyhow::anyhow!("YTsaurus chunk metadata has no extension {tag}"))
}

fn extension(meta: &ChunkMeta, tag: i32) -> Option<Bytes> {
    meta.extensions
        .as_ref()?
        .extensions
        .iter()
        .find(|extension| extension.tag == tag)
        .map(|extension| Bytes::copy_from_slice(&extension.data))
}

fn segment_data<'a>(block: &'a [u8], segment: &SegmentMeta) -> anyhow::Result<&'a [u8]> {
    let start = usize::try_from(segment.offset)?;
    let end = start
        .checked_add(usize::try_from(segment.size)?)
        .ok_or_else(|| anyhow::anyhow!("YTsaurus segment byte range overflow"))?;
    anyhow::ensure!(
        end <= block.len(),
        "YTsaurus segment exceeds its data block"
    );
    Ok(&block[start..end])
}

fn append_segment(
    builder: &mut ColumnBuilder,
    data_type: &DataType,
    meta: &SegmentMeta,
    data: &[u8],
    start: usize,
    end: usize,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        meta.version == 0,
        "unsupported YTsaurus segment version {}",
        meta.version
    );
    anyhow::ensure!(
        end <= usize::try_from(meta.row_count)?,
        "YTsaurus segment slice exceeds its row count"
    );
    match data_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            append_integer(builder, meta, data, start, end, true)
        }
        DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Date32
        | DataType::Date64
        | DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            append_integer(builder, meta, data, start, end, false)
        }
        DataType::Float32 => append_floating::<4>(builder, data, start, end),
        DataType::Float64 => append_floating::<8>(builder, data, start, end),
        DataType::Boolean => append_boolean(builder, data, start, end),
        DataType::Utf8 | DataType::Binary => append_string(builder, meta, data, start, end),
        other => {
            anyhow::bail!("direct YTsaurus chunk reader does not support Arrow type {other:?}")
        }
    }
}

fn append_integer(
    builder: &mut ColumnBuilder,
    meta: &SegmentMeta,
    data: &[u8],
    start: usize,
    end: usize,
    signed: bool,
) -> anyhow::Result<()> {
    let base = meta
        .integer_segment_meta
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("YTsaurus integer segment has no integer metadata"))?
        .min_value;
    let mut cursor = 0_usize;
    match meta.segment_type {
        3 => {
            let values = PackedVector::read(data, &mut cursor)?;
            anyhow::ensure!(values.len() == usize::try_from(meta.row_count)?);
            let nulls = Bitmap::read(data, &mut cursor, values.len())?;
            ensure_consumed(data, cursor)?;
            for index in start..end {
                append_integer_value(builder, signed, base, values.get(index)?, nulls.get(index))?;
            }
        }
        1 => {
            let dictionary = PackedVector::read(data, &mut cursor)?;
            let ids = PackedVector::read(data, &mut cursor)?;
            anyhow::ensure!(ids.len() == usize::try_from(meta.row_count)?);
            ensure_consumed(data, cursor)?;
            for index in start..end {
                let id = usize::try_from(ids.get(index)?)?;
                if id == 0 {
                    builder.append_null()?;
                } else {
                    let delta = dictionary.get(id - 1)?;
                    append_integer_value(builder, signed, base, delta, false)?;
                }
            }
        }
        2 => {
            let values = PackedVector::read(data, &mut cursor)?;
            let nulls = Bitmap::read(data, &mut cursor, values.len())?;
            let rows = PackedVector::read(data, &mut cursor)?;
            validate_runs(&rows, values.len(), usize::try_from(meta.row_count)?)?;
            ensure_consumed(data, cursor)?;
            append_rle(start, end, &rows, |run| {
                append_integer_value(builder, signed, base, values.get(run)?, nulls.get(run))
            })?;
        }
        0 => {
            let dictionary = PackedVector::read(data, &mut cursor)?;
            let ids = PackedVector::read(data, &mut cursor)?;
            let rows = PackedVector::read(data, &mut cursor)?;
            validate_runs(&rows, ids.len(), usize::try_from(meta.row_count)?)?;
            ensure_consumed(data, cursor)?;
            append_rle(start, end, &rows, |run| {
                let id = usize::try_from(ids.get(run)?)?;
                if id == 0 {
                    builder.append_null()
                } else {
                    append_integer_value(builder, signed, base, dictionary.get(id - 1)?, false)
                }
            })?;
        }
        other => anyhow::bail!("unsupported YTsaurus integer segment type {other}"),
    }
    Ok(())
}

fn append_integer_value(
    builder: &mut ColumnBuilder,
    signed: bool,
    base: u64,
    delta: u64,
    is_null: bool,
) -> anyhow::Result<()> {
    if is_null {
        return builder.append_null();
    }
    let encoded = base
        .checked_add(delta)
        .ok_or_else(|| anyhow::anyhow!("YTsaurus integer value overflow"))?;
    if signed {
        let value = (encoded >> 1).cast_signed() ^ -(encoded & 1).cast_signed();
        builder.append_fixed(VALUE_INT64, value as u64)
    } else {
        builder.append_fixed(VALUE_UINT64, encoded)
    }
}

fn append_string(
    builder: &mut ColumnBuilder,
    meta: &SegmentMeta,
    data: &[u8],
    start: usize,
    end: usize,
) -> anyhow::Result<()> {
    let expected_length = meta
        .string_segment_meta
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("YTsaurus string segment has no string metadata"))?
        .expected_length;
    let mut cursor = 0_usize;
    match meta.segment_type {
        3 => {
            let offsets = PackedVector::read(data, &mut cursor)?;
            anyhow::ensure!(offsets.len() == usize::try_from(meta.row_count)?);
            let nulls = Bitmap::read(data, &mut cursor, offsets.len())?;
            let strings = &data[cursor..];
            let boundaries = string_boundaries(&offsets, expected_length, strings.len())?;
            for index in start..end {
                append_string_value(builder, strings, &boundaries, index, nulls.get(index))?;
            }
        }
        1 => {
            let ids = PackedVector::read(data, &mut cursor)?;
            anyhow::ensure!(ids.len() == usize::try_from(meta.row_count)?);
            let offsets = PackedVector::read(data, &mut cursor)?;
            let strings = &data[cursor..];
            let boundaries = string_boundaries(&offsets, expected_length, strings.len())?;
            for index in start..end {
                let id = usize::try_from(ids.get(index)?)?;
                if id == 0 {
                    builder.append_null()?;
                } else {
                    append_string_value(builder, strings, &boundaries, id - 1, false)?;
                }
            }
        }
        2 => {
            let rows = PackedVector::read(data, &mut cursor)?;
            let offsets = PackedVector::read(data, &mut cursor)?;
            validate_runs(&rows, offsets.len(), usize::try_from(meta.row_count)?)?;
            let nulls = Bitmap::read(data, &mut cursor, offsets.len())?;
            let strings = &data[cursor..];
            let boundaries = string_boundaries(&offsets, expected_length, strings.len())?;
            append_rle(start, end, &rows, |run| {
                append_string_value(builder, strings, &boundaries, run, nulls.get(run))
            })?;
        }
        0 => {
            let rows = PackedVector::read(data, &mut cursor)?;
            let ids = PackedVector::read(data, &mut cursor)?;
            validate_runs(&rows, ids.len(), usize::try_from(meta.row_count)?)?;
            let offsets = PackedVector::read(data, &mut cursor)?;
            let strings = &data[cursor..];
            let boundaries = string_boundaries(&offsets, expected_length, strings.len())?;
            append_rle(start, end, &rows, |run| {
                let id = usize::try_from(ids.get(run)?)?;
                if id == 0 {
                    builder.append_null()
                } else {
                    append_string_value(builder, strings, &boundaries, id - 1, false)
                }
            })?;
        }
        other => anyhow::bail!("unsupported YTsaurus string segment type {other}"),
    }
    Ok(())
}

fn append_string_value(
    builder: &mut ColumnBuilder,
    strings: &[u8],
    boundaries: &[usize],
    index: usize,
    is_null: bool,
) -> anyhow::Result<()> {
    if is_null {
        return builder.append_null();
    }
    let end = *boundaries
        .get(index)
        .ok_or_else(|| anyhow::anyhow!("YTsaurus string index {index} is out of range"))?;
    let start = index
        .checked_sub(1)
        .and_then(|previous| boundaries.get(previous).copied())
        .unwrap_or(0);
    builder.append_string(&strings[start..end])
}

fn string_boundaries(
    offsets: &PackedVector<'_>,
    expected_length: u32,
    data_len: usize,
) -> anyhow::Result<Vec<usize>> {
    let mut boundaries = Vec::with_capacity(offsets.len());
    let mut previous = 0_i64;
    for index in 0..offsets.len() {
        let encoded = u32::try_from(offsets.get(index)?)?;
        let difference = (encoded >> 1).cast_signed() ^ -(encoded & 1).cast_signed();
        let expected = i64::from(expected_length)
            .checked_mul(i64::try_from(index + 1)?)
            .ok_or_else(|| anyhow::anyhow!("YTsaurus string offset overflow"))?;
        let boundary = expected
            .checked_add(i64::from(difference))
            .ok_or_else(|| anyhow::anyhow!("YTsaurus string offset overflow"))?;
        anyhow::ensure!(
            previous <= boundary && boundary <= i64::try_from(data_len)?,
            "invalid YTsaurus string boundary {boundary}"
        );
        boundaries.push(usize::try_from(boundary)?);
        previous = boundary;
    }
    anyhow::ensure!(
        boundaries.last().copied().unwrap_or(0) == data_len,
        "YTsaurus string segment has trailing or missing bytes"
    );
    Ok(boundaries)
}

fn append_floating<const WIDTH: usize>(
    builder: &mut ColumnBuilder,
    data: &[u8],
    start: usize,
    end: usize,
) -> anyhow::Result<()> {
    let mut cursor = 0_usize;
    let count = usize::try_from(read_u64(data, &mut cursor)?)?;
    anyhow::ensure!(end <= count, "YTsaurus floating-point segment is too short");
    let values_len = count
        .checked_mul(WIDTH)
        .ok_or_else(|| anyhow::anyhow!("YTsaurus floating-point vector overflow"))?;
    let values_end = cursor
        .checked_add(values_len)
        .ok_or_else(|| anyhow::anyhow!("YTsaurus floating-point vector overflow"))?;
    anyhow::ensure!(
        values_end <= data.len(),
        "truncated YTsaurus floating-point vector"
    );
    let values = &data[cursor..values_end];
    cursor = values_end;
    let nulls = Bitmap::read(data, &mut cursor, count)?;
    ensure_consumed(data, cursor)?;
    for index in start..end {
        if nulls.get(index) {
            builder.append_null()?;
        } else if WIDTH == 4 {
            let offset = index * WIDTH;
            let value = f32::from_le_bytes(values[offset..offset + WIDTH].try_into()?);
            builder.append_fixed(VALUE_DOUBLE, f64::from(value).to_bits())?;
        } else {
            let offset = index * WIDTH;
            let value = f64::from_le_bytes(values[offset..offset + WIDTH].try_into()?);
            builder.append_fixed(VALUE_DOUBLE, value.to_bits())?;
        }
    }
    Ok(())
}

fn append_boolean(
    builder: &mut ColumnBuilder,
    data: &[u8],
    start: usize,
    end: usize,
) -> anyhow::Result<()> {
    let mut cursor = 0_usize;
    let count = usize::try_from(read_u64(data, &mut cursor)?)?;
    anyhow::ensure!(end <= count, "YTsaurus boolean segment is too short");
    let values = Bitmap::read(data, &mut cursor, count)?;
    let nulls = Bitmap::read(data, &mut cursor, count)?;
    ensure_consumed(data, cursor)?;
    for index in start..end {
        if nulls.get(index) {
            builder.append_null()?;
        } else {
            builder.append_fixed(VALUE_BOOLEAN, u64::from(values.get(index)))?;
        }
    }
    Ok(())
}

fn append_rle(
    start: usize,
    end: usize,
    rows: &PackedVector<'_>,
    mut append_run: impl FnMut(usize) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let mut run = rows.partition_point(start)?;
    for row in start..end {
        while run + 1 < rows.len() && usize::try_from(rows.get(run + 1)?)? <= row {
            run += 1;
        }
        append_run(run)?;
    }
    Ok(())
}

fn validate_runs(
    rows: &PackedVector<'_>,
    value_count: usize,
    row_count: usize,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        rows.len() == value_count,
        "YTsaurus RLE value/index count mismatch"
    );
    anyhow::ensure!(
        rows.len() > 0 && rows.get(0)? == 0,
        "YTsaurus RLE rows must start at zero"
    );
    let mut previous = 0_usize;
    for index in 0..rows.len() {
        let row = usize::try_from(rows.get(index)?)?;
        anyhow::ensure!(row < row_count, "YTsaurus RLE row index is out of range");
        anyhow::ensure!(
            index == 0 || row > previous,
            "YTsaurus RLE row indexes are not increasing"
        );
        previous = row;
    }
    Ok(())
}

fn ensure_consumed(data: &[u8], cursor: usize) -> anyhow::Result<()> {
    anyhow::ensure!(
        cursor == data.len(),
        "YTsaurus segment contains {} trailing bytes",
        data.len() - cursor
    );
    Ok(())
}

fn read_u64(data: &[u8], cursor: &mut usize) -> anyhow::Result<u64> {
    let end = cursor
        .checked_add(8)
        .ok_or_else(|| anyhow::anyhow!("YTsaurus vector offset overflow"))?;
    anyhow::ensure!(end <= data.len(), "truncated YTsaurus vector header");
    let value = u64::from_le_bytes(data[*cursor..end].try_into()?);
    *cursor = end;
    Ok(value)
}

struct PackedVector<'a> {
    data: &'a [u8],
    size: usize,
    width: usize,
}

impl<'a> PackedVector<'a> {
    fn read(data: &'a [u8], cursor: &mut usize) -> anyhow::Result<Self> {
        let header = read_u64(data, cursor)?;
        let size = usize::try_from(header & ((1_u64 << 56) - 1))?;
        let width = usize::try_from(header >> 56)?;
        anyhow::ensure!(width <= 64, "invalid YTsaurus packed-vector width {width}");
        let words = width
            .checked_mul(size)
            .and_then(|bits| bits.checked_add(63))
            .map(|bits| bits / 64)
            .ok_or_else(|| anyhow::anyhow!("YTsaurus packed-vector size overflow"))?;
        let byte_len = words
            .checked_mul(8)
            .ok_or_else(|| anyhow::anyhow!("YTsaurus packed-vector size overflow"))?;
        let end = cursor
            .checked_add(byte_len)
            .ok_or_else(|| anyhow::anyhow!("YTsaurus packed-vector size overflow"))?;
        anyhow::ensure!(end <= data.len(), "truncated YTsaurus packed vector");
        let packed = Self {
            data: &data[*cursor..end],
            size,
            width,
        };
        *cursor = end;
        Ok(packed)
    }

    const fn len(&self) -> usize {
        self.size
    }

    fn get(&self, index: usize) -> anyhow::Result<u64> {
        anyhow::ensure!(
            index < self.size,
            "YTsaurus packed-vector index {index} is out of range"
        );
        if self.width == 0 {
            return Ok(0);
        }
        let bit_index = index
            .checked_mul(self.width)
            .ok_or_else(|| anyhow::anyhow!("YTsaurus packed-vector bit index overflow"))?;
        let word_index = bit_index / 64;
        let offset = bit_index % 64;
        let first = read_word(self.data, word_index)? >> offset;
        let value = if offset + self.width > 64 {
            let second_width = offset + self.width - 64;
            let second = read_word(self.data, word_index + 1)? & low_mask(second_width);
            first | (second << (64 - offset))
        } else {
            first & low_mask(self.width)
        };
        Ok(value)
    }

    fn partition_point(&self, row: usize) -> anyhow::Result<usize> {
        let mut left = 0_usize;
        let mut right = self.size;
        while left < right {
            let middle = left + (right - left) / 2;
            if usize::try_from(self.get(middle)?)? <= row {
                left = middle + 1;
            } else {
                right = middle;
            }
        }
        Ok(left.saturating_sub(1))
    }
}

fn read_word(data: &[u8], index: usize) -> anyhow::Result<u64> {
    let start = index
        .checked_mul(8)
        .ok_or_else(|| anyhow::anyhow!("YTsaurus packed-vector word offset overflow"))?;
    let end = start
        .checked_add(8)
        .ok_or_else(|| anyhow::anyhow!("YTsaurus packed-vector word offset overflow"))?;
    anyhow::ensure!(end <= data.len(), "truncated YTsaurus packed-vector word");
    Ok(u64::from_le_bytes(data[start..end].try_into()?))
}

const fn low_mask(width: usize) -> u64 {
    if width == 64 {
        u64::MAX
    } else if width == 0 {
        0
    } else {
        (1_u64 << width) - 1
    }
}

struct Bitmap<'a> {
    data: &'a [u8],
    size: usize,
}

impl<'a> Bitmap<'a> {
    fn read(data: &'a [u8], cursor: &mut usize, size: usize) -> anyhow::Result<Self> {
        let byte_len = size
            .checked_add(7)
            .map(|bits| bits / 8)
            .ok_or_else(|| anyhow::anyhow!("YTsaurus bitmap size overflow"))?;
        let aligned = align_up(byte_len, SERIALIZATION_ALIGNMENT)?;
        let end = cursor
            .checked_add(aligned)
            .ok_or_else(|| anyhow::anyhow!("YTsaurus bitmap size overflow"))?;
        anyhow::ensure!(end <= data.len(), "truncated YTsaurus bitmap");
        let bitmap = Self {
            data: &data[*cursor..*cursor + byte_len],
            size,
        };
        *cursor = end;
        Ok(bitmap)
    }

    fn get(&self, index: usize) -> bool {
        debug_assert!(index < self.size);
        self.data[index / 8] & (1 << (index % 8)) != 0
    }
}

fn align_up(value: usize, alignment: usize) -> anyhow::Result<usize> {
    value
        .checked_add(alignment - 1)
        .map(|value| value & !(alignment - 1))
        .ok_or_else(|| anyhow::anyhow!("YTsaurus alignment overflow"))
}

fn decompress_block(codec: i32, input: &[u8]) -> anyhow::Result<Vec<u8>> {
    match codec {
        CODEC_NONE => Ok(input.to_vec()),
        CODEC_LZ4 | CODEC_LZ4_HIGH_COMPRESSION => decompress_lz4(input),
        other => anyhow::bail!("direct YTsaurus reads do not support compression codec {other}"),
    }
}

fn decompress_lz4(input: &[u8]) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(
        input.len() >= 8,
        "YTsaurus LZ4 block is shorter than its header"
    );
    let first = u32::from_le_bytes(input[0..4].try_into()?);
    let second = u32::from_le_bytes(input[4..8].try_into()?);
    let (mut cursor, expected_size) = match first {
        LZ4_SIGNATURE_V1 => (8_usize, Some(u64::from(second))),
        LZ4_SIGNATURE_V2 => {
            anyhow::ensure!(input.len() >= 16, "truncated YTsaurus LZ4 v2 header");
            (16_usize, Some(u64::from_le_bytes(input[8..16].try_into()?)))
        }
        _ => (0_usize, None),
    };
    let mut output =
        Vec::with_capacity(expected_size.map(usize::try_from).transpose()?.unwrap_or(0));
    while cursor < input.len() {
        let header_end = cursor
            .checked_add(8)
            .ok_or_else(|| anyhow::anyhow!("YTsaurus LZ4 block offset overflow"))?;
        anyhow::ensure!(
            header_end <= input.len(),
            "truncated YTsaurus LZ4 block header"
        );
        let compressed_size =
            usize::try_from(u32::from_le_bytes(input[cursor..cursor + 4].try_into()?))?;
        let uncompressed_size = usize::try_from(u32::from_le_bytes(
            input[cursor + 4..header_end].try_into()?,
        ))?;
        cursor = header_end;
        let compressed_end = cursor
            .checked_add(compressed_size)
            .ok_or_else(|| anyhow::anyhow!("YTsaurus LZ4 block size overflow"))?;
        anyhow::ensure!(
            compressed_end <= input.len(),
            "truncated YTsaurus LZ4 payload"
        );
        let output_start = output.len();
        output.resize(
            output_start
                .checked_add(uncompressed_size)
                .ok_or_else(|| anyhow::anyhow!("YTsaurus LZ4 output size overflow"))?,
            0,
        );
        let written = lz4_flex::block::decompress_into(
            &input[cursor..compressed_end],
            &mut output[output_start..],
        )?;
        anyhow::ensure!(
            written == uncompressed_size,
            "YTsaurus LZ4 block size mismatch"
        );
        cursor = compressed_end;
    }
    if let Some(expected_size) = expected_size {
        anyhow::ensure!(
            u64::try_from(output.len())? == expected_size,
            "YTsaurus LZ4 total size mismatch"
        );
    }
    Ok(output)
}

#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct Extension {
    #[prost(int32, required, tag = "1")]
    pub(super) tag: i32,

    #[prost(bytes = "vec", required, tag = "2")]
    pub(super) data: Vec<u8>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct ExtensionSet {
    #[prost(message, repeated, tag = "1")]
    pub(super) extensions: Vec<Extension>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct ChunkMeta {
    #[prost(int32, required, tag = "1")]
    pub(super) chunk_type: i32,

    #[prost(message, optional, tag = "2")]
    pub(super) extensions: Option<ExtensionSet>,

    #[prost(int32, required, tag = "3")]
    pub(super) format: i32,
}

#[derive(Clone, PartialEq, prost::Message)]
struct MiscExt {
    #[prost(int32, optional, tag = "3")]
    compression_codec: Option<i32>,

    #[prost(message, optional, tag = "25")]
    compression_dictionary_id: Option<ProtoGuid>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ProtoGuid {
    #[prost(fixed64, required, tag = "1")]
    first: u64,

    #[prost(fixed64, required, tag = "2")]
    second: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct DataBlockMeta {
    #[prost(int32, required, tag = "1")]
    pub(super) row_count: i32,

    #[prost(int64, optional, tag = "2")]
    pub(super) uncompressed_size: Option<i64>,

    #[prost(int64, required, tag = "3")]
    pub(super) chunk_row_count: i64,

    #[prost(int32, required, tag = "7")]
    pub(super) block_index: i32,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct DataBlockMetaExt {
    #[prost(message, repeated, tag = "1")]
    pub(super) data_blocks: Vec<DataBlockMeta>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ColumnMetaExt {
    #[prost(message, repeated, tag = "1")]
    columns: Vec<ColumnMeta>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ColumnMeta {
    #[prost(message, repeated, tag = "1")]
    segments: Vec<SegmentMeta>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct SegmentMeta {
    #[prost(int32, required, tag = "1")]
    version: i32,

    #[prost(int32, required, tag = "2")]
    segment_type: i32,

    #[prost(int64, required, tag = "3")]
    row_count: i64,

    #[prost(int32, required, tag = "4")]
    block_index: i32,

    #[prost(int64, required, tag = "5")]
    offset: i64,

    #[prost(int64, required, tag = "6")]
    chunk_row_count: i64,

    #[prost(int64, required, tag = "7")]
    size: i64,

    #[prost(message, optional, tag = "101")]
    integer_segment_meta: Option<IntegerSegmentMeta>,

    #[prost(message, optional, tag = "122")]
    string_segment_meta: Option<StringSegmentMeta>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct IntegerSegmentMeta {
    #[prost(uint64, required, tag = "1")]
    min_value: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct StringSegmentMeta {
    #[prost(uint32, required, tag = "1")]
    expected_length: u32,
}

#[cfg(test)]
#[path = "tests/columnar_chunk.rs"]
mod tests;
