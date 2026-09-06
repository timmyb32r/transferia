use std::sync::Arc;

use anyhow::Context;
use arrow::array::new_empty_array;
use arrow::compute::concat_batches;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::execution::context::{SQLOptions, SessionContext};
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{DataFrame, SessionConfig};
use futures_util::TryStreamExt;
use schemars::JsonSchema;
use serde::Deserialize;

use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::data::system_columns::{SystemColumn, SystemColumns};
use transferia_core::data::table_data::TableData;
use transferia_delivery_contracts::middleware::{Middleware, MiddlewarePreviewContext};
use transferia_registry::{MiddlewareRegistration, RegistryBuilder};

const INPUT_TABLE: &str = "input";

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DataFusionConfig {
    #[schemars(
        title = "SQL",
        description = "One SELECT query over the input table",
        extend("x-ui" = { "widget": "sql" })
    )]
    pub sql: String,
}

pub struct DataFusionMiddleware {
    sql: String,
}

impl DataFusionMiddleware {
    pub fn new(sql: String) -> anyhow::Result<Self> {
        anyhow::ensure!(!sql.trim().is_empty(), "DataFusion SQL must not be empty");
        Ok(Self { sql })
    }

    async fn plan(&self, batch: RecordBatch, context: SessionContext) -> anyhow::Result<DataFrame> {
        context
            .register_batch(INPUT_TABLE, batch)
            .context("register DataFusion input batch")?;
        let options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);
        context
            .sql_with_options(&self.sql, options)
            .await
            .context("plan DataFusion SQL")
    }

    async fn execute(&self, batch: RecordBatch, memory_limit_bytes: Option<usize>) -> anyhow::Result<RecordBatch> {
        let pool: Option<Arc<dyn MemoryPool>> = memory_limit_bytes.map(|limit| {
            Arc::new(GreedyMemoryPool::new(limit)) as Arc<dyn MemoryPool>
        });
        let mut reservation = pool.as_ref().map(|pool| MemoryConsumer::new("preview input and output").register(pool));
        if let Some(reservation) = &mut reservation {
            reservation.try_grow(batch.get_array_memory_size()).context("preview input exceeds memory_limit_bytes")?;
        }
        let context = match pool {
            Some(pool) => SessionContext::new_with_config_rt(
                SessionConfig::new(),
                Arc::new(RuntimeEnvBuilder::new()
                    .with_memory_pool(pool)
                    .with_disk_manager_builder(DiskManagerBuilder::default()
                        .with_mode(DiskManagerMode::Disabled)
                        .with_max_temp_directory_size(0))
                    .build()?),
            ),
            None => SessionContext::new(),
        };
        let frame = self.plan(batch, context).await?;
        let schema = Arc::new(frame.schema().as_arrow().clone());
        if reservation.is_none() {
            let batches = frame.collect().await.context("execute DataFusion SQL")?;
            return concat_batches(&schema, &batches).context("combine DataFusion output batches");
        }
        let mut stream = frame.execute_stream().await.context("execute DataFusion SQL")?;
        let mut batches = Vec::new();
        let mut output_bytes = 0usize;
        while let Some(batch) = stream.try_next().await.context("execute DataFusion SQL")? {
            let bytes = batch.get_array_memory_size();
            if let Some(reservation) = &mut reservation {
                reservation.try_grow(bytes).context("preview output exceeds memory_limit_bytes")?;
            }
            output_bytes = output_bytes.checked_add(bytes).context("preview output memory accounting overflow")?;
            batches.push(batch);
        }
        if batches.len() == 1 {
            return Ok(batches.pop().expect("one batch"));
        }
        if let Some(reservation) = &mut reservation {
            reservation.try_grow(output_bytes).context("preview output combination exceeds memory_limit_bytes")?;
        }
        concat_batches(&schema, &batches).context("combine DataFusion output batches")
    }

    async fn process_with_budget(&self, data: TableData, memory_limit_bytes: Option<usize>) -> anyhow::Result<TableData> {
        let batch = self.execute(data.batch, memory_limit_bytes).await?;
        let system_columns = SystemColumns::new(data.system_columns.iter().filter_map(|column| {
            batch.schema().index_of(&column.name).ok().map(|index| SystemColumn {
                kind: column.kind,
                index,
                name: column.name.clone(),
            })
        }).collect::<Vec<_>>());
        Ok(TableData { batch, system_columns, ..data })
    }

}

#[async_trait]
impl Middleware for DataFusionMiddleware {
    async fn output_schema(&self, schema: &DatasetSchema) -> anyhow::Result<DatasetSchema> {
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
        let columns = arrow_schema
            .fields()
            .iter()
            .map(|field| new_empty_array(field.data_type()))
            .collect::<Vec<_>>();
        let output = self
            .plan(RecordBatch::try_new(arrow_schema, columns)?, SessionContext::new())
            .await?;
        Ok(DatasetSchema::new(
            output
                .schema().as_arrow()
                .fields()
                .iter()
                .map(|field| {
                    let column = SchemaColumn::new(
                        field.name().clone(),
                        field.data_type().clone(),
                        field.is_nullable(),
                    );
                    if let Some(input) = schema.columns.iter().find(|input| {
                        input.name.as_str() == field.name() && input.data_type == *field.data_type()
                    }) {
                        let mut column = column.with_constraints(
                            input.primary_key,
                            input.low_cardinality,
                            input.max_length,
                        );
                        if let Some(extension_name) = input.arrow_extension_name {
                            column = column.with_arrow_extension(extension_name);
                        }
                        column
                    } else {
                        column
                    }
                })
                .collect(),
        ))
    }

    async fn process(&self, data: TableData) -> anyhow::Result<TableData> {
        self.process_with_budget(data, None).await
    }

    async fn preview(&self, data: TableData, context: MiddlewarePreviewContext) -> anyhow::Result<TableData> {
        anyhow::ensure!(context.memory_limit_bytes > 0, "preview memory_limit_bytes must be positive");
        self.process_with_budget(data, Some(context.memory_limit_bytes)).await
    }
}

pub fn register(builder: &mut RegistryBuilder) -> anyhow::Result<()> {
    builder.register_middleware(MiddlewareRegistration::new::<
        DataFusionConfig,
        _,
        _,
    >(
        "datafusion",
        "DataFusion SQL",
        || serde_json::json!({ "sql": "SELECT * FROM input" }),
        |config| Ok(Box::new(DataFusionMiddleware::new(config.sql)?)),
    )?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
