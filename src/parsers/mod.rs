pub mod benchmark_discard;
pub mod config;
pub mod json_parser;

use std::collections::HashMap;

use alloc::sync::Arc;
use serde::Deserialize;
use serde_yaml::Value;

use crate::types::message::Message;
use crate::types::table_data::TableData;

pub use crate::parsers::json_parser::ParserWorkspace;
pub use config::{CommonParserConfig, ParserConfig, SystemColumnsConfig, TableNaming};

/// Common parser interface. Every parser converts raw [`Message`]s into
/// Arrow [`TableData`] (valid + optional DLQ).
pub trait Parser: Send + Sync {
    fn parse_into(
        &self,
        messages: Vec<Message>,
        partition_id: i64,
        ws: &mut ParserWorkspace,
    ) -> anyhow::Result<(TableData, Option<TableData>)>;
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
