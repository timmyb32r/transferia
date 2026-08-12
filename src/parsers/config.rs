use serde::Deserialize;

use crate::parsers::ParserEntry;
use crate::types::system_columns::SystemColumnKind;

#[derive(Debug, Clone, Deserialize)]
pub struct ParserConfig {
    pub common: CommonParserConfig,
    #[serde(flatten)]
    pub parser: ParserEntry,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommonParserConfig {
    pub table_naming: TableNaming,
    #[serde(default)]
    pub system_columns: SystemColumnsConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "these independent user-facing switches deliberately compose"
)]
pub struct SystemColumnsConfig {
    #[serde(default)]
    pub topic_name: bool,
    #[serde(default)]
    pub partition_num: bool,
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
            (self.topic_name, SystemColumnKind::TopicName),
            (self.partition_num, SystemColumnKind::PartitionNum),
            (self.offset, SystemColumnKind::Offset),
            (self.message_index, SystemColumnKind::MessageIndex),
            (self.write_timestamp_ms, SystemColumnKind::WriteTimestampMs),
        ]
        .into_iter()
        .filter_map(|(enabled, kind)| enabled.then_some(kind))
    }
}

#[derive(Debug, Clone, Deserialize)]
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
