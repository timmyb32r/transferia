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
    #[schemars(
        title = "Parser settings",
        extend("x-ui" = { "widget": "parser_common" })
    )]
    pub common: CommonParserConfig,

    #[schemars(title = "JSON parser")]
    pub json_parser: JsonParserConfig,
}

#[derive(JsonSchema)]
pub struct BenchmarkDiscardParserSchema {
    #[schemars(
        title = "Parser settings",
        extend("x-ui" = { "widget": "parser_common" })
    )]
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
    #[schemars(title = "Table name")]
    pub table_naming: TableNaming,

    #[serde(default)]
    #[schemars(extend("x-ui" = { "widget": "system_columns" }))]
    pub system_columns: SystemColumnsConfig,
}

#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SystemColumnsConfig {
    #[serde(default)]
    pub topic: Option<String>,

    #[serde(default)]
    pub partition: Option<String>,

    #[serde(default)]
    pub offset: Option<String>,

    #[serde(default)]
    pub message_index: Option<String>,

    #[serde(default)]
    pub write_timestamp_ms: Option<String>,
}

impl SystemColumnsConfig {
    pub fn enabled(&self) -> impl Iterator<Item = SystemColumnKind> {
        [
            (self.topic.is_some(), SystemColumnKind::Topic),
            (self.partition.is_some(), SystemColumnKind::Partition),
            (self.offset.is_some(), SystemColumnKind::Offset),
            (self.message_index.is_some(), SystemColumnKind::MessageIndex),
            (
                self.write_timestamp_ms.is_some(),
                SystemColumnKind::WriteTimestampMs,
            ),
        ]
        .into_iter()
        .filter_map(|(enabled, kind)| enabled.then_some(kind))
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        let mut names = std::collections::HashSet::new();
        for name in [
            self.topic.as_deref(),
            self.partition.as_deref(),
            self.offset.as_deref(),
            self.message_index.as_deref(),
            self.write_timestamp_ms.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            anyhow::ensure!(!name.is_empty(), "system column names must not be empty");
            anyhow::ensure!(
                names.insert(name),
                "system columns repeat column name '{name}'"
            );
        }
        Ok(())
    }

    #[must_use]
    pub fn name(&self, kind: SystemColumnKind) -> &str {
        match kind {
            SystemColumnKind::Topic => self.topic.as_deref(),
            SystemColumnKind::Partition => self.partition.as_deref(),
            SystemColumnKind::Offset => self.offset.as_deref(),
            SystemColumnKind::MessageIndex => self.message_index.as_deref(),
            SystemColumnKind::WriteTimestampMs => self.write_timestamp_ms.as_deref(),
        }
        .unwrap_or_else(|| kind.default_name())
    }
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum TableNaming {
    #[schemars(title = "From config")]
    FromConfig { name: String },

    #[schemars(title = "From topic name")]
    FromTopicName,
}

impl ParserConfig {
    pub fn resolve_table_name(&self, topic_path: &str) -> anyhow::Result<String> {
        match &self.common.table_naming {
            TableNaming::FromConfig { name } => {
                (!name.is_empty()).then(|| name.clone()).ok_or_else(|| {
                    anyhow::anyhow!("table_naming.name is required for type 'from_config'")
                })
            }
            TableNaming::FromTopicName => Ok(topic_path.to_string()),
        }
    }
}
