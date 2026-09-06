use std::sync::Arc;

use arrow::array::{ArrayRef, Decimal128Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use transferia_core::{SystemColumns, DatasetSchema, DiscoveredDataset};

use super::*;

const BUDGET: MiddlewarePreviewContext = MiddlewarePreviewContext { memory_limit_bytes: 1024 * 1024 };

fn sample(values: Vec<i64>) -> TableData {
    TableData::new(
        Arc::from("events"), false,
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(values))],
        ).unwrap(),
        SystemColumns::default(),
    ).with_namespace(Arc::from("public"))
}

struct RemoveRows;

#[async_trait]
impl Middleware for RemoveRows {
    async fn output_schema(&self, schema: &DatasetSchema) -> anyhow::Result<DatasetSchema> {
        Ok(schema.clone())
    }
    async fn process(&self, data: TableData) -> anyhow::Result<TableData> {
        Ok(TableData { batch: data.batch.slice(0, 0), ..data })
    }
}

struct Identity;

#[async_trait]
impl Middleware for Identity {
    async fn output_schema(&self, schema: &DatasetSchema) -> anyhow::Result<DatasetSchema> {
        anyhow::ensure!(schema.columns[0].data_type == DataType::Int64);
        Ok(schema.clone())
    }
    async fn process(&self, data: TableData) -> anyhow::Result<TableData> {
        Ok(data)
    }
}

struct Rename;

struct CancelDuringExecution {
    cancellation: CancellationToken,
    execution_dropped: CancellationToken,
}

#[async_trait]
impl Middleware for CancelDuringExecution {
    async fn output_schema(&self, schema: &DatasetSchema) -> anyhow::Result<DatasetSchema> {
        Ok(schema.clone())
    }

    async fn process(&self, _: TableData) -> anyhow::Result<TableData> {
        let _execution_guard = self.execution_dropped.clone().drop_guard();
        self.cancellation.cancel();
        std::future::pending().await
    }
}

#[async_trait]
impl Middleware for Rename {
    async fn output_dataset(&self, dataset: &DiscoveredDataset) -> anyhow::Result<DiscoveredDataset> {
        Ok(DiscoveredDataset { name: Arc::from("renamed"), ..dataset.clone() })
    }
    async fn output_schema(&self, schema: &DatasetSchema) -> anyhow::Result<DatasetSchema> {
        Ok(schema.clone())
    }
    async fn process(&self, data: TableData) -> anyhow::Result<TableData> {
        Ok(TableData { table: Arc::from("renamed"), ..data })
    }
}

#[tokio::test]
async fn preview_matches_the_current_identity_after_the_previous_step_renames_it() -> anyhow::Result<()> {
    use transferia_registry::{MiddlewareRegistration, RegistryBuilder};
    use crate::middleware::{build_middlewares, MiddlewareEntry};

    let mut registry = RegistryBuilder::new();
    registry.register_middleware(MiddlewareRegistration::new::<BTreeMap<String, String>, _, _>(
        "rename", "Rename", || serde_json::json!({}), |_| Ok(Box::new(Rename)),
    )?)?;
    registry.register_middleware(MiddlewareRegistration::new::<BTreeMap<String, String>, _, _>(
        "empty", "Empty", || serde_json::json!({}), |_| Ok(Box::new(RemoveRows)),
    )?)?;
    let entries: Vec<MiddlewareEntry> = serde_json::from_value(serde_json::json!([
        {"rename":{}},
        {"tables":{"include":"public.renamed"},"empty":{}}
    ]))?;
    let steps = build_middlewares(&registry.build(), &entries)?;
    let preview = preview_chain(&steps, sample(vec![1]), 1, BUDGET, CancellationToken::new()).await?;
    assert_eq!(preview.before.table.as_ref(), "renamed");
    assert!(preview.applied);
    assert_eq!(preview.after.batch.num_rows(), 0);
    Ok(())
}

#[tokio::test]
async fn zero_rows_keep_the_native_schema_for_the_next_step() -> anyhow::Result<()> {
    let steps: Vec<Box<dyn Middleware>> = vec![Box::new(RemoveRows), Box::new(Identity)];
    let preview = preview_chain(&steps, sample(vec![1]), 1, BUDGET, CancellationToken::new()).await?;
    assert_eq!(preview.before.batch.num_rows(), 0);
    assert_eq!(preview.after.batch.num_rows(), 0);
    assert_eq!(preview.after.batch.schema().field(0).data_type(), &DataType::Int64);
    assert_eq!(preview.after.namespace.as_deref(), Some("public"));
    Ok(())
}

#[tokio::test]
async fn selected_before_frame_contains_all_previous_step_results() -> anyhow::Result<()> {
    let steps: Vec<Box<dyn Middleware>> = vec![Box::new(Identity), Box::new(RemoveRows)];
    let preview = preview_chain(&steps, sample(vec![1, 2]), 1, BUDGET, CancellationToken::new()).await?;
    assert_eq!(preview.before.batch.num_rows(), 2);
    assert_eq!(preview.after.batch.num_rows(), 0);
    assert!(preview.applied);
    Ok(())
}

#[tokio::test]
async fn cancellation_prevents_preview_execution() {
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let steps: Vec<Box<dyn Middleware>> = vec![Box::new(Identity)];
    let error = preview_chain(&steps, sample(vec![1]), 0, BUDGET, cancellation).await.err().unwrap();
    assert!(format!("{error:#}").contains("cancelled"));
}

#[tokio::test]
async fn cancellation_drops_the_current_transform_execution() {
    let cancellation = CancellationToken::new();
    let execution_dropped = CancellationToken::new();
    let steps: Vec<Box<dyn Middleware>> = vec![Box::new(CancelDuringExecution {
        cancellation: cancellation.clone(),
        execution_dropped: execution_dropped.clone(),
    })];
    let error = preview_chain(&steps, sample(vec![1]), 0, BUDGET, cancellation)
        .await.err().unwrap();
    assert!(format!("{error:#}").contains("cancelled"));
    assert!(execution_dropped.is_cancelled(), "the running transform must not survive cancellation");
}

#[test]
fn display_keeps_full_integer_and_decimal_precision_as_text() -> anyhow::Result<()> {
    assert_eq!(serde_json::to_value(display_rows(&sample(vec![i64::MAX]))?)?, serde_json::json!([{"id":"9223372036854775807"}]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("amount", DataType::Decimal128(38, 9), true)])),
        vec![Arc::new(Decimal128Array::from(vec![Some(12345678901234567890123456789_i128), None]).with_precision_and_scale(38, 9)?) as ArrayRef],
    )?;
    let data = TableData { batch, ..sample(vec![]) };
    assert_eq!(serde_json::to_value(display_rows(&data)?)?, serde_json::json!([{"amount":"12345678901234567890.123456789"}, {"amount":null}]));
    Ok(())
}

#[tokio::test]
async fn physical_table_columns_are_not_hidden_based_on_metadata_like_names() -> anyhow::Result<()> {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("_system_offset", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![123]))],
    )?;
    let input = TableData { batch, ..sample(vec![]) };
    let steps: Vec<Box<dyn Middleware>> = vec![Box::new(Identity)];
    let preview = preview_chain(&steps, input, 0, BUDGET, CancellationToken::new()).await?;
    assert_eq!(serde_json::to_value(display_rows(&preview.after)?)?, serde_json::json!([{"_system_offset":"123"}]));
    Ok(())
}
