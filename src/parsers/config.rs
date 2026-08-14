use schemars::JsonSchema;
use serde::Deserialize;

use crate::parsers::json_parser::JsonParserConfig;
use crate::parsers::ParserEntry;
use crate::types::system_columns::SystemColumnKind;

/// Complete parser schema used by the control plane. Runtime dispatch remains
/// registry-based, while this tagged union gives JSON Schema an explicit set
/// of currently supported forms.
#[derive(JsonSchema)]
#[serde(untagged)]
pub enum ParserSchema {
    #[schemars(title = "JSON parser")]
    Json(JsonParserSchema),
    #[schemars(title = "Discard messages (benchmark)")]
    BenchmarkDiscard(BenchmarkDiscardParserSchema),
}

#[derive(JsonSchema)]
pub struct JsonParserSchema {
    pub common: CommonParserConfig,

    pub json_parser: JsonParserConfig,
}

#[derive(JsonSchema)]
pub struct BenchmarkDiscardParserSchema {
    pub common: CommonParserConfig,

    pub benchmark_discard: EmptyParserConfig,
}

#[derive(JsonSchema)]
pub struct EmptyParserConfig {}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct ParserConfig {
    pub common: CommonParserConfig,

    #[serde(flatten)]
    #[schemars(skip)]
    pub parser: ParserEntry,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CommonParserConfig {
    pub table_naming: TableNaming,

    #[serde(default)]
    pub system_columns: SystemColumnsConfig,
}

#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "these independent user-facing switches deliberately compose"
)]
pub struct SystemColumnsConfig {
    #[serde(default)]
    pub topic: bool,

    #[serde(default)]
    pub partition: bool,

    #[serde(default)]
    pub offset: bool,

    #[serde(default)]
    pub message_index: bool,

    #[serde(default)]
    pub write_timestamp_ms: bool,
}

impl SystemColumnsConfig {
    pub fn enabled(&self) -> impl Iterator<Item = SystemColumnKind> {
        [
            (self.topic, SystemColumnKind::Topic),
            (self.partition, SystemColumnKind::Partition),
            (self.offset, SystemColumnKind::Offset),
            (self.message_index, SystemColumnKind::MessageIndex),
            (self.write_timestamp_ms, SystemColumnKind::WriteTimestampMs),
        ]
        .into_iter()
        .filter_map(|(enabled, kind)| enabled.then_some(kind))
    }
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum TableNaming {
    FromConfig { name: String },
    FromTopic,
}

impl ParserConfig {
    pub fn resolve_table_name(&self, topic_path: &str) -> anyhow::Result<String> {
        match &self.common.table_naming {
            TableNaming::FromConfig { name } => {
                (!name.is_empty()).then(|| name.clone()).ok_or_else(|| {
                    anyhow::anyhow!("table_naming.name is required for type 'from_config'")
                })
            }
            TableNaming::FromTopic => Ok(topic_path.to_string()),
        }
    }
}
