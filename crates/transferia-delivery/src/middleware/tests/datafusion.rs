use std::sync::Arc;

use arrow::array::{AsArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use super::super::datafusion::DataFusionMiddleware;
use transferia_core::{DatasetSchema, SchemaColumn, SystemColumns, TableData};
use transferia_delivery_contracts::middleware::Middleware;

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
