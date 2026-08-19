use std::sync::Arc;

use schemars::JsonSchema;
use serde::Serialize;
use serde_json::Value;

use transferia_core::data::message::Message;
use transferia_core::data::table_data::TableData;

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ParserDetection {
    pub key: String,

    pub label: String,

    pub config: Value,

    pub inferred_columns: Vec<InferredColumn>,

    pub sample_rows: Vec<Value>,

    pub preview_tabs: Vec<ParserPreviewTab>,

    pub sampled_messages: usize,

    pub sampled_rows: usize,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InferredColumn {
    pub name: String,

    pub source_type: String,

    pub arrow_type: String,

    pub nullable: bool,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ParserPreviewTab {
    pub key: String,

    pub label: String,

    pub content: String,

    pub truncated: bool,
}

/// Compiled parser shared by partition pipelines.
pub trait ParserFactory: Send + Sync {
    fn create_session(self: Arc<Self>, memory_limit_bytes: usize) -> Box<dyn ParserSession>;
}

/// Mutable parser state owned by exactly one partition pipeline.
pub trait ParserSession: Send {
    /// Conservative allocation estimate used for admission before builders allocate.
    fn output_memory_bound(&self, messages: &[Message]) -> usize;

    fn parse_into(
        &mut self,
        messages: Vec<Message>,
    ) -> anyhow::Result<(TableData, Option<TableData>)>;
}
