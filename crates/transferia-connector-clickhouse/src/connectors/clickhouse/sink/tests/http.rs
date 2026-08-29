use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::*;

fn batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("one"), None, Some("three")])),
        ],
    )
    .expect("test batch")
}

#[test]
fn parquet_encoder_preserves_all_batches() -> anyhow::Result<()> {
    let batch = batch();
    let encoded = encode_parquet(ClickHouseCompression::Lz4, 2, &[batch.clone(), batch])?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(encoded))?
        .with_batch_size(16)
        .build()?;
    let rows = reader
        .map(|batch| batch.map(|batch| batch.num_rows()))
        .sum::<Result<usize, _>>()?;
    assert_eq!(rows, 6);
    Ok(())
}

#[test]
fn arrow_stream_encoder_preserves_all_batches() -> anyhow::Result<()> {
    let batch = batch();
    let encoded = encode_arrow_stream(ClickHouseCompression::Zstd, &[batch.clone(), batch])?;
    let reader = StreamReader::try_new(Cursor::new(encoded), None)?;
    let rows = reader
        .map(|batch| batch.map(|batch| batch.num_rows()))
        .sum::<Result<usize, _>>()?;
    assert_eq!(rows, 6);
    Ok(())
}
