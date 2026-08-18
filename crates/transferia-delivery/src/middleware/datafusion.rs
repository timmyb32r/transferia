use std::sync::Arc;

use anyhow::Context;
use arrow::array::new_empty_array;
use arrow::compute::concat_batches;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::execution::context::{SQLOptions, SessionContext};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::Value;

use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::data::system_columns::{SystemColumn, SystemColumns};
use transferia_core::data::table_data::TableData;
use transferia_delivery_contracts::middleware::Middleware;

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

    async fn execute(&self, batch: RecordBatch) -> anyhow::Result<RecordBatch> {
        let context = SessionContext::new();
        context
            .register_batch(INPUT_TABLE, batch)
            .context("register DataFusion input batch")?;
        let options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);
        let frame = context
            .sql_with_options(&self.sql, options)
            .await
            .context("plan DataFusion SQL")?;
        let schema = Arc::new(frame.schema().as_arrow().clone());
        let batches = frame.collect().await.context("execute DataFusion SQL")?;
        concat_batches(&schema, &batches).context("combine DataFusion output batches")
    }

    pub async fn execute_json_rows(
        &self,
        rows: &[Value],
    ) -> anyhow::Result<(RecordBatch, Vec<Value>)> {
        anyhow::ensure!(
            !rows.is_empty(),
            "playground sample must contain at least one row"
        );
        let schema =
            arrow::json::reader::infer_json_schema_from_iterator(rows.iter().cloned().map(Ok))?;
        let input = rows
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()?
            .join("\n");
        let mut reader = arrow::json::ReaderBuilder::new(Arc::new(schema))
            .with_batch_size(rows.len())
            .build(std::io::Cursor::new(input))?;
        let batch = reader
            .next()
            .transpose()?
            .context("playground sample produced no Arrow batch")?;
        drop(reader);
        let output = self.execute(batch).await?;
        let mut encoded = Vec::new();
        {
            let mut writer = arrow::json::LineDelimitedWriter::new(&mut encoded);
            writer.write(&output)?;
            writer.finish()?;
        }
        let values = encoded
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(serde_json::from_slice)
            .collect::<Result<Vec<_>, _>>()?;
        Ok((output, values))
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
            .execute(RecordBatch::try_new(arrow_schema, columns)?)
            .await?;
        Ok(DatasetSchema::new(
            output
                .schema()
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
        let batch = self.execute(data.batch).await?;
        let system_columns = SystemColumns::new(
            data.system_columns
                .iter()
                .filter_map(|column| {
                    batch
                        .schema()
                        .index_of(&column.name)
                        .ok()
                        .map(|index| SystemColumn {
                            kind: column.kind,
                            index,
                            name: column.name.clone(),
                        })
                })
                .collect::<Vec<_>>(),
        );
        Ok(TableData {
            batch,
            system_columns,
            ..data
        })
    }
}
