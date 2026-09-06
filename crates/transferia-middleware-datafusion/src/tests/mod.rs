use std::sync::Arc;

use arrow::array::{AsArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use super::DataFusionMiddleware;
use transferia_core::{DatasetSchema, SchemaColumn, SystemColumns, TableData};
use transferia_delivery_contracts::middleware::Middleware;
use transferia_delivery_contracts::middleware::MiddlewarePreviewContext;

fn input() -> anyhow::Result<TableData> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["one", "two", "three"])),
        ],
    )?;
    Ok(TableData::new(
        Arc::from("events"),
        false,
        batch,
        SystemColumns::default(),
    ))
}

#[tokio::test]
async fn sql_projects_filters_and_derives_the_output_schema() -> anyhow::Result<()> {
    let middleware = DataFusionMiddleware::new(
        "SELECT id * 10 AS scaled_id, upper(name) AS label FROM input WHERE id >= 2".into(),
    )?;
    let schema = DatasetSchema::new(vec![
        SchemaColumn::new("id".into(), DataType::Int64, false),
        SchemaColumn::new("name".into(), DataType::Utf8, false),
    ]);
    let output_schema = middleware.output_schema(&schema).await?;
    assert_eq!(output_schema.columns.len(), 2);
    assert_eq!(output_schema.columns[0].name, "scaled_id");
    assert_eq!(output_schema.columns[0].data_type, DataType::Int64);

    let output = middleware.process(input()?).await?;
    assert_eq!(output.batch.num_rows(), 2);
    assert_eq!(
        output
            .batch
            .column(0)
            .as_primitive::<arrow::datatypes::Int64Type>()
            .values(),
        &[20, 30]
    );
    assert_eq!(
        output
            .batch
            .column(1)
            .as_string::<i32>()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("TWO"), Some("THREE")]
    );
    Ok(())
}

#[tokio::test]
async fn sql_rejects_ddl_and_unknown_input_columns() -> anyhow::Result<()> {
    let schema = DatasetSchema::new(vec![SchemaColumn::new("id".into(), DataType::Int64, false)]);
    assert!(DataFusionMiddleware::new("DROP TABLE input".into())?
        .output_schema(&schema)
        .await
        .is_err());
    assert!(
        DataFusionMiddleware::new("SELECT missing FROM input".into())?
            .output_schema(&schema)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn preview_hash_aggregate_uses_the_configured_execution_memory_pool() -> anyhow::Result<()> {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from_iter_values(0..10_000))],
    )?;
    let memory_limit_bytes = batch.get_array_memory_size() + 1024;
    let input = TableData::new(Arc::from("events"), false, batch, SystemColumns::default());
    let middleware = DataFusionMiddleware::new("SELECT id, COUNT(*) AS n FROM input GROUP BY id".into())?;
    let error = middleware.preview(input, MiddlewarePreviewContext { memory_limit_bytes }).await.err()
        .expect("aggregation must not allocate outside the configured memory pool");
    let diagnostic = format!("{error:#}");
    assert!(diagnostic.contains("Resources exhausted") && diagnostic.contains("SpillPool"), "{diagnostic}");
    Ok(())
}

#[tokio::test]
async fn schema_planning_does_not_execute_failing_data_expressions() -> anyhow::Result<()> {
    let middleware = DataFusionMiddleware::new("SELECT id / 0 AS ratio FROM input".into())?;
    let frame = middleware.plan(input()?.batch, datafusion::execution::context::SessionContext::new()).await?;
    assert_eq!(frame.schema().as_arrow().field(0).name(), "ratio");
    assert!(frame.collect().await.is_err(), "executing this expression must fail; planning must not execute it");
    Ok(())
}
