pub mod benchmark_discard;
pub mod config;
pub mod json_parser;

use std::collections::HashMap;

use alloc::sync::Arc;
use serde::Deserialize;
use serde_yaml::Value;

use crate::types::message::Message;
use crate::types::schema::DatasetSchema;
use crate::types::table_data::TableData;

pub use config::{CommonParserConfig, ParserConfig, SystemColumnsConfig, TableNaming};

/// Common parser interface. Every parser converts raw [`Message`]s into
/// Arrow [`TableData`] (valid + optional DLQ).
pub trait Parser: Send + Sync {
    fn create_session(self: Arc<Self>) -> Box<dyn ParserSession>;
}

/// Mutable parser state owned by exactly one partition parser thread.
pub trait ParserSession: Send {
    /// Conservative Arrow/DLQ allocation bound used to account transform memory
    /// before builders allocate it. The pipeline validates the materialized parser
    /// output against this bound.
    fn output_memory_bound(&self, messages: &[Message]) -> usize;

    fn parse_into(
        &mut self,
        messages: Vec<Message>,
    ) -> anyhow::Result<(TableData, Option<TableData>)>;
}

/// A parser compiled once from source configuration and shared by all partition pipelines.
/// It is the single source of truth for the parser instance and its sink-facing schemas.
pub struct ParserPlan {
    parser: Arc<dyn Parser>,
    table: Arc<str>,
    dataset_schema: DatasetSchema,
    system_columns: SystemColumnsConfig,
    parses_rows: bool,
}

impl ParserPlan {
    pub fn from_config(config: &ParserConfig, topic_path: &str) -> anyhow::Result<Self> {
        let table: Arc<str> = config.resolve_table_name(topic_path)?.into();
        let kind = config.parser.kind()?;
        let (parser, dataset_schema, parses_rows) = match kind {
            "json_parser" => {
                let parser_config: json_parser::JsonParserConfig =
                    serde_yaml::from_value(config.parser.raw()?.clone())?;
                let schema = parser_config.to_dataset_schema()?;
                let parser = Arc::new(json_parser::JsonParser::new(
                    &parser_config,
                    &config.common.system_columns,
                    Arc::clone(&table),
                )?) as Arc<dyn Parser>;
                (parser, schema, true)
            }
            "benchmark_discard" => {
                let _: benchmark_discard::BenchmarkDiscardConfig =
                    serde_yaml::from_value(config.parser.raw()?.clone())?;
                let parser = Arc::new(benchmark_discard::BenchmarkDiscardParser::new(Arc::clone(
                    &table,
                ))) as Arc<dyn Parser>;
                (parser, DatasetSchema::default(), false)
            }
            other => anyhow::bail!(
                "unknown parser '{other}'; supported parsers: json_parser, benchmark_discard"
            ),
        };
        Ok(Self {
            parser,
            table,
            dataset_schema,
            system_columns: config.common.system_columns.clone(),
            parses_rows,
        })
    }

    #[must_use]
    pub fn parser(&self) -> Arc<dyn Parser> {
        Arc::clone(&self.parser)
    }

    #[must_use]
    pub fn table(&self) -> Arc<str> {
        Arc::clone(&self.table)
    }

    #[must_use]
    pub const fn dataset_schema(&self) -> &DatasetSchema {
        &self.dataset_schema
    }

    #[must_use]
    pub const fn system_columns(&self) -> &SystemColumnsConfig {
        &self.system_columns
    }

    #[must_use]
    pub const fn parses_rows(&self) -> bool {
        self.parses_rows
    }

    #[must_use]
    pub fn sink_schema(&self, keep_system_columns: bool) -> DatasetSchema {
        json_parser::sink_dataset_schema(
            self.dataset_schema.clone(),
            &self.system_columns,
            keep_system_columns,
        )
    }

    #[must_use]
    pub fn dlq_schema(&self, keep_system_columns: bool) -> DatasetSchema {
        json_parser::dlq_dataset_schema(&self.system_columns, keep_system_columns)
    }
}

// ---------------------------------------------------------------------------
// ParserEntry — dynamic { kind: { config } } dispatch, like SourceEntry/SinkEntry
// ---------------------------------------------------------------------------

/// Parser config entry: `parser: { <kind>: { ... } }` — exactly one key.
#[derive(Debug, Clone, Deserialize)]
pub struct ParserEntry {
    #[serde(flatten)]
    pub inner: HashMap<String, Value>,
}

impl ParserEntry {
    pub fn kind(&self) -> anyhow::Result<&str> {
        let keys: Vec<&str> = self.inner.keys().map(String::as_str).collect();
        match *keys.as_slice() {
            [single] => Ok(single),
            [] => anyhow::bail!(
                "parser: no parser key found (expected 'json_parser' or 'benchmark_discard')"
            ),
            _ => anyhow::bail!("parser: expected exactly one parser key, got {keys:?}"),
        }
    }

    pub fn raw(&self) -> anyhow::Result<&Value> {
        let kind = self.kind()?;
        self.inner
            .get(kind)
            .ok_or_else(|| anyhow::anyhow!("parser: parser key '{kind}' missing from config"))
    }
}

pub fn build_parser(
    name: &str,
    raw: Value,
    table: Arc<str>,
    common: &CommonParserConfig,
) -> anyhow::Result<Arc<dyn Parser>> {
    match name {
        "json_parser" => {
            let config: crate::parsers::json_parser::JsonParserConfig =
                serde_yaml::from_value(raw)?;
            Ok(Arc::new(crate::parsers::json_parser::JsonParser::new(
                &config,
                &common.system_columns,
                table,
            )?))
        }
        "benchmark_discard" => {
            let _: crate::parsers::benchmark_discard::BenchmarkDiscardConfig =
                serde_yaml::from_value(raw)?;
            Ok(Arc::new(
                crate::parsers::benchmark_discard::BenchmarkDiscardParser::new(table),
            ))
        }
        other => anyhow::bail!(
            "unknown parser '{other}'; supported parsers: json_parser, benchmark_discard"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn common() -> CommonParserConfig {
        serde_yaml::from_str("table_naming: { type: from_config, name: events }").unwrap()
    }

    #[test]
    fn benchmark_discard_rejects_unknown_configuration() {
        assert!(build_parser(
            "benchmark_discard",
            serde_yaml::from_str("{ typo: true }").unwrap(),
            Arc::from("events"),
            &common(),
        )
        .is_err());
    }
}
