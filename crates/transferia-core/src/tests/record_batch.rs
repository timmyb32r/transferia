use alloc::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use super::{compact_record_batch, compact_record_batch_chunks};

#[test]
fn compacts_short_slices_that_retain_large_transport_buffers() -> anyhow::Result<()> {
    const PARENT_ROWS: i64 = 4_096;
    let parent = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("payload", DataType::Utf8, false),
            Field::new("sequence", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from_iter_values(
                (0..PARENT_ROWS).map(|row| format!("payload-{row:010}-{}", "x".repeat(512))),
            )) as ArrayRef,
            Arc::new(Int64Array::from_iter_values(0..PARENT_ROWS)) as ArrayRef,
        ],
    )?;
    let retained = parent.slice(320, 64);
    let retained_bytes = retained.get_array_memory_size();

    let compact = compact_record_batch(retained)?;
    let compact_bytes = compact.get_array_memory_size();

    assert_eq!(compact.num_rows(), 64);
    assert_eq!(
        compact
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string column")
            .value(3),
        format!("payload-{:010}-{}", 323, "x".repeat(512)),
    );
    assert!(
        compact_bytes * 4 < retained_bytes,
        "compacted batch uses {compact_bytes} bytes, retained slice uses {retained_bytes} bytes"
    );
    Ok(())
}

#[test]
fn keeps_right_sized_batches_zero_copy() -> anyhow::Result<()> {
    let values: ArrayRef = Arc::new(StringArray::from(vec!["one", "two", "three"]));
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::clone(&values)],
    )?;

    let compact = compact_record_batch(input)?;

    assert!(Arc::ptr_eq(compact.column(0), &values));
    Ok(())
}

#[test]
fn chunks_oversized_batches_without_retaining_parent_buffers() -> anyhow::Result<()> {
    const ROWS: usize = if cfg!(miri) { 32_768 } else { 262_144 };
    const CHUNK_ROWS: usize = ROWS / 4;
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "sequence",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from_iter_values(
            0..i64::try_from(ROWS)?,
        ))],
    )?;
    let parent_bytes = input.get_array_memory_size();

    let chunks = compact_record_batch_chunks(input, CHUNK_ROWS)?;
    let chunk_bytes = chunks
        .iter()
        .map(RecordBatch::get_array_memory_size)
        .sum::<usize>();

    assert_eq!(chunks.len(), 4);
    assert!(chunks.iter().all(|batch| batch.num_rows() == CHUNK_ROWS));
    assert!(
        chunk_bytes < parent_bytes * 2,
        "independent chunks use {chunk_bytes} bytes, parent uses {parent_bytes} bytes"
    );
    Ok(())
}
