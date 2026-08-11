use serde::Deserialize;

use crate::parsers::ParserEntry;

#[derive(Debug, Clone, Deserialize)]
pub struct ParserConfig {
    pub table_naming: TableNaming,
    #[serde(flatten)]
    pub parser: ParserEntry,
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
        match self.table_naming.kind.as_str() {
            "from_config" => self
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
