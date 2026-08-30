use arrow::array::UInt64Array;
use arrow::compute::take;
use arrow::record_batch::RecordBatch;

const MIN_COMPACTION_SAVINGS_BYTES: usize = 1024 * 1024;

/// Materializes the visible rows when an Arrow batch retains substantially
/// larger backing buffers.
///
/// Readers commonly expose short slices of transport or decoder allocations.
/// Keeping those allocations alive distorts pipeline memory accounting and
/// causes byte-targeted sinks to flush undersized logical batches. A copy is
/// worthwhile only when it releases at least 1 MiB and more than 25% of the
/// visible payload; already right-sized batches remain zero-copy.
pub fn compact_record_batch(batch: RecordBatch) -> anyhow::Result<RecordBatch> {
    if batch.num_rows() == 0 || !materially_overallocated(&batch)? {
        return Ok(batch);
    }
    materialize_range(&batch, 0, batch.num_rows())
}

/// Splits an Arrow batch at `max_rows` and materializes every resulting view
/// that would otherwise retain materially larger backing buffers.
pub fn compact_record_batch_chunks(
    batch: RecordBatch,
    max_rows: usize,
) -> anyhow::Result<Vec<RecordBatch>> {
    anyhow::ensure!(max_rows > 0, "record batch row limit must be positive");
    if batch.num_rows() <= max_rows {
        return compact_record_batch(batch).map(|batch| vec![batch]);
    }

    let mut batches = Vec::with_capacity(batch.num_rows().div_ceil(max_rows));
    let mut offset = 0;
    while offset < batch.num_rows() {
        let rows = max_rows.min(batch.num_rows() - offset);
        batches.push(materialize_range(&batch, offset, rows)?);
        offset += rows;
    }
    Ok(batches)
}

fn materially_overallocated(batch: &RecordBatch) -> anyhow::Result<bool> {
    let retained_bytes = batch.get_array_memory_size();
    let visible_bytes = batch.columns().iter().try_fold(0_usize, |total, column| {
        Ok::<_, arrow::error::ArrowError>(
            total.saturating_add(column.to_data().get_slice_memory_size()?),
        )
    })?;
    Ok(
        retained_bytes.saturating_sub(visible_bytes) > MIN_COMPACTION_SAVINGS_BYTES
            && retained_bytes > visible_bytes.saturating_add(visible_bytes / 4),
    )
}

fn materialize_range(
    batch: &RecordBatch,
    offset: usize,
    rows: usize,
) -> anyhow::Result<RecordBatch> {
    let start = u64::try_from(offset)?;
    let end = u64::try_from(offset + rows)?;
    let indices = UInt64Array::from_iter_values(start..end);
    let columns = batch
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &indices, None))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

#[cfg(test)]
#[path = "../tests/record_batch.rs"]
mod tests;
