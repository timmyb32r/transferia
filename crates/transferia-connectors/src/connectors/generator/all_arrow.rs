//! Deterministic type-coverage data, not a model of a production distribution.
//! Arrow Null is deliberately excluded: it has no typed values to exercise.
//! Every column contains a representative non-null value; `id` is the unique
//! absolute row index. Parameterized families use concrete examples.
use std::sync::Arc;

use arrow::array::*;
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{
    DataType, Field, Int32Type, IntervalUnit, Schema, TimeUnit, UnionMode,
};
use arrow::record_batch::RecordBatch;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};

// Fixed logical benchmark width, conservatively above the generated buffers per
// row (including offsets and take indices). It is not a serialized wire size.
pub(super) const LOGICAL_ROW_BYTES: u64 = 4096;
// Array objects, alignment and one-row seed arrays remain even in tiny batches.
pub(super) const BATCH_OVERHEAD_BYTES: u64 = 32 * 1024;

use transferia_registry::arrow_examples::types;

pub(super) fn schema() -> DatasetSchema {
    DatasetSchema::new(
        types()
            .into_iter()
            .map(|(name, data_type)| {
                SchemaColumn::new(name.to_owned(), data_type, false).with_constraints(
                    name == "id",
                    false,
                    None,
                )
            })
            .collect(),
    )
}

pub(super) fn batch(start: u64, rows: u64) -> anyhow::Result<RecordBatch> {
    let end = start
        .checked_add(rows)
        .ok_or_else(|| anyhow::anyhow!("generator row range overflows u64"))?;
    let rows = usize::try_from(rows)?;
    let indices = UInt32Array::from(vec![0; rows]);
    let schema = schema();
    let fields = schema
        .columns
        .iter()
        .map(|column| {
            Field::new(&column.name, column.data_type.clone(), column.nullable)
                .with_metadata(column.arrow_metadata())
        })
        .collect::<Vec<_>>();
    let arrays = schema
        .columns
        .iter()
        .map(|column| {
            if column.name == "id" {
                return Ok(Arc::new(UInt64Array::from_iter_values(start..end)) as ArrayRef);
            }
            Ok(arrow::compute::take(
                sample(&column.data_type)?.as_ref(),
                &indices,
                None,
            )?)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
}

fn sample(data_type: &DataType) -> anyhow::Result<ArrayRef> {
    let integers = || Arc::new(Int32Array::from(vec![42])) as ArrayRef;
    let strings = || Arc::new(StringArray::from(vec!["Arrow ✓"])) as ArrayRef;
    let array: ArrayRef = match data_type {
        DataType::Boolean => Arc::new(BooleanArray::from(vec![true])),
        DataType::Time32(TimeUnit::Second) => Arc::new(Time32SecondArray::from(vec![0])),
        DataType::Time32(TimeUnit::Millisecond) => Arc::new(Time32MillisecondArray::from(vec![0])),
        DataType::Time64(TimeUnit::Microsecond) => Arc::new(Time64MicrosecondArray::from(vec![0])),
        DataType::Time64(TimeUnit::Nanosecond) => Arc::new(Time64NanosecondArray::from(vec![0])),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            arrow::compute::cast(strings().as_ref(), data_type)?
        }
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => {
            arrow::compute::cast(&BinaryArray::from(vec![b"data".as_slice()]), data_type)?
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            Arc::new(IntervalYearMonthArray::from(vec![1]))
        }
        DataType::Interval(IntervalUnit::DayTime) => Arc::new(IntervalDayTimeArray::from(vec![
            arrow::datatypes::IntervalDayTime::new(1, 2),
        ])),
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            Arc::new(IntervalMonthDayNanoArray::from(vec![
                arrow::datatypes::IntervalMonthDayNano::new(1, 2, 3),
            ]))
        }
        DataType::List(field) => Arc::new(ListArray::try_new(
            Arc::clone(field),
            OffsetBuffer::new(vec![0, 1].into()),
            integers(),
            None,
        )?),
        DataType::LargeList(field) => Arc::new(LargeListArray::try_new(
            Arc::clone(field),
            OffsetBuffer::new(vec![0, 1].into()),
            integers(),
            None,
        )?),
        DataType::ListView(field) => Arc::new(ListViewArray::try_new(
            Arc::clone(field),
            vec![0].into(),
            vec![1].into(),
            integers(),
            None,
        )?),
        DataType::LargeListView(field) => Arc::new(LargeListViewArray::try_new(
            Arc::clone(field),
            vec![0].into(),
            vec![1].into(),
            integers(),
            None,
        )?),
        DataType::FixedSizeList(field, size) => Arc::new(FixedSizeListArray::try_new(
            Arc::clone(field),
            *size,
            integers(),
            None,
        )?),
        DataType::Struct(fields) => Arc::new(StructArray::try_new(
            fields.clone(),
            vec![integers()],
            None,
        )?),
        DataType::Union(fields, mode) => Arc::new(UnionArray::try_new(
            fields.clone(),
            ScalarBuffer::from(vec![0_i8]),
            (*mode == UnionMode::Dense).then(|| vec![0].into()),
            vec![integers()],
        )?),
        DataType::Dictionary(_, _) => Arc::new(DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0]),
            strings(),
        )?),
        DataType::Map(field, sorted) => {
            let DataType::Struct(fields) = field.data_type() else {
                anyhow::bail!("map entries must be a struct")
            };
            let entries = StructArray::try_new(fields.clone(), vec![strings(), integers()], None)?;
            Arc::new(MapArray::try_new(
                Arc::clone(field),
                OffsetBuffer::new(vec![0, 1].into()),
                entries,
                None,
                *sorted,
            )?)
        }
        DataType::RunEndEncoded(_, _) => Arc::new(RunArray::<Int32Type>::try_new(
            &Int32Array::from(vec![1]),
            integers().as_ref(),
        )?),
        // Zero is exactly representable in all numeric and temporal families,
        // including Date64's whole-day invariant and all decimal scales.
        _ => arrow::compute::cast(&Int64Array::from(vec![0]), data_type)?,
    };
    Ok(array)
}
