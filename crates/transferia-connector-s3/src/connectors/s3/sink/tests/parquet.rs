use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::*;

#[test]
fn parquet_round_trip_preserves_arrow_rows() -> anyhow::Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("alpha"), None, Some("gamma")])),
        ],
    )?;
    let encoded = encode_parquet(
        batch.clone(),
        Compression::SNAPPY,
        &ParquetRowGroupConfig {
            max_rows: 2,
            max_bytes: super::super::config::ByteSize(1024 * 1024),
        },
    )?;

    let decoded = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(encoded))?
        .build()?
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(decoded.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    assert_eq!(decoded[0].schema(), schema);
    assert_eq!(arrow::compute::concat_batches(&schema, &decoded)?, batch);
    Ok(())
}

#[test]
fn object_rotation_splits_batches_without_copying_or_losing_rows() -> anyhow::Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "id",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]))],
    )?;

    let chunks = split_for_object_limits(batch.clone(), 2, usize::MAX);

    assert_eq!(chunks.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(), [2, 2, 1]);
    assert_eq!(arrow::compute::concat_batches(&schema, &chunks)?, batch);
    Ok(())
}
