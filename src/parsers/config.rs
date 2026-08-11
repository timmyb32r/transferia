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

    #[must_use]
    pub fn any_enabled(&self) -> bool {
        self.enabled().next().is_some()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TableNaming {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub name: Option<String>,
}

impl ParserConfig {
    pub fn resolve_table_name(&self, topic_path: &str) -> anyhow::Result<String> {
        match self.common.table_naming.kind.as_str() {
            "from_config" => self
                .common
                .table_naming
                .name
                .clone()
                .filter(|name| !name.is_empty())
                .ok_or_else(|| {
                    anyhow::anyhow!("table_naming.name is required for type 'from_config'")
                }),
            "from_topic" => Ok(topic_path.to_string()),
            other => {
                anyhow::bail!("unknown table_naming.type '{other}' (use from_config | from_topic)")
            }
        }
    }
}
