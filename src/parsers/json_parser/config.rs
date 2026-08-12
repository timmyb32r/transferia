use arrow::datatypes::{DataType, TimeUnit};
use serde::Deserialize;

use crate::types::schema::{DatasetSchema, SchemaColumn};

/// JSON parser configuration deserialized from the `json_parser:` block.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JsonParserConfig {
    pub columns: Vec<ColumnMapping>,
    /// How incoming message bytes are split into individual JSON objects.
    #[serde(default)]
    pub chunk_splitter: ChunkSplitter,
}

impl JsonParserConfig {
    /// Convert parser mappings into the sink-neutral runtime schema.
    pub fn to_dataset_schema(&self) -> anyhow::Result<DatasetSchema> {
        self.columns
            .iter()
            .map(ColumnMapping::to_schema_column)
            .collect::<anyhow::Result<Vec<_>>>()
            .map(DatasetSchema::new)
    }
}

/// Record framing policy owned by the JSON parser.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ChunkSplitter {
    #[default]
    OneMessageOneRow,
    NewLine,
}

impl ChunkSplitter {
    /// Count records without allocating a `Vec`.
    #[must_use]
    pub fn count_records(self, buf: &[u8]) -> usize {
        match self {
            Self::OneMessageOneRow => 1,
            Self::NewLine if buf.is_empty() => 0,
            Self::NewLine if !buf.contains(&b'\n') => 1,
            Self::NewLine => buf
                .split(|&byte| byte == b'\n')
                .filter(|line| !line.is_empty())
                .count(),
        }
    }
}

/// `JSONPath` mapping from an input field to one Arrow output column.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnMapping {
    pub jsonpath: String,
    pub column_name: String,
    pub arrow_type: String,
    #[serde(default)]
    pub nullable: bool,
}

impl ColumnMapping {
    #[must_use]
    pub const fn new(
        jsonpath: String,
        column_name: String,
        arrow_type: String,
        nullable: bool,
    ) -> Self {
        Self {
            jsonpath,
            column_name,
            arrow_type,
            nullable,
        }
    }

    pub fn to_schema_column(&self) -> anyhow::Result<SchemaColumn> {
        Ok(SchemaColumn::new(
            self.column_name.clone(),
            parse_arrow_type(&self.arrow_type)?,
            self.nullable,
        ))
    }
}

/// Parse the JSON parser's human-readable Arrow type syntax.
pub fn parse_arrow_type(value: &str) -> anyhow::Result<DataType> {
    match value {
        "Utf8" | "String" => Ok(DataType::Utf8),
        "LargeUtf8" | "LargeString" => Ok(DataType::LargeUtf8),
        "Int64" | "int64" => Ok(DataType::Int64),
        "Int32" | "int32" => Ok(DataType::Int32),
        "Int16" | "int16" => Ok(DataType::Int16),
        "Int8" | "int8" => Ok(DataType::Int8),
        "UInt64" | "uint64" => Ok(DataType::UInt64),
        "UInt32" | "uint32" => Ok(DataType::UInt32),
        "UInt16" | "uint16" => Ok(DataType::UInt16),
        "UInt8" | "uint8" => Ok(DataType::UInt8),
        "Float64" | "float64" => Ok(DataType::Float64),
        "Float32" | "float32" => Ok(DataType::Float32),
        "Boolean" | "bool" | "Bool" => Ok(DataType::Boolean),
        "Date32" => Ok(DataType::Date32),
        "Date64" => Ok(DataType::Date64),
        _ if value.starts_with("Timestamp(") => {
            let inner = value.trim_start_matches("Timestamp(").trim_end_matches(')');
            let parts: Vec<&str> = inner.split(',').map(str::trim).collect();
            let unit = match parts.first().copied().unwrap_or("Microsecond") {
                "Second" => TimeUnit::Second,
                "Millisecond" => TimeUnit::Millisecond,
                "Microsecond" => TimeUnit::Microsecond,
                "Nanosecond" => TimeUnit::Nanosecond,
                other => anyhow::bail!(
                    "Unsupported Timestamp unit '{other}'. Use Second, Millisecond, Microsecond, or Nanosecond."
                ),
            };
            let timezone =
                (parts.len() > 1 && parts[1] != "None").then(|| parts[1].to_string().into());
            Ok(DataType::Timestamp(unit, timezone))
        }
        other => anyhow::bail!(
            "Unsupported arrow_type: '{other}'. Supported: Utf8, String, LargeUtf8, Int64, Int32, Int16, Int8, UInt64, UInt32, UInt16, UInt8, Float64, Float32, Boolean, Date32, Date64, Timestamp(unit, tz)"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_arrow_types() -> anyhow::Result<()> {
        anyhow::ensure!(parse_arrow_type("Utf8")? == DataType::Utf8);
        anyhow::ensure!(parse_arrow_type("int64")? == DataType::Int64);
        anyhow::ensure!(parse_arrow_type("Float64")? == DataType::Float64);
        anyhow::ensure!(parse_arrow_type("bool")? == DataType::Boolean);
        anyhow::ensure!(
            parse_arrow_type("Timestamp(Millisecond, None)")?
                == DataType::Timestamp(TimeUnit::Millisecond, None)
        );
        anyhow::ensure!(
            parse_arrow_type("Timestamp(Microsecond, UTC)")?
                == DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        Ok(())
    }

    #[test]
    fn rejects_unknown_arrow_type() -> anyhow::Result<()> {
        let error = parse_arrow_type("Blob")
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected Blob to be rejected"))?;
        anyhow::ensure!(error.to_string().contains("Unsupported arrow_type"));
        Ok(())
    }

    #[test]
    fn produces_sink_neutral_schema() -> anyhow::Result<()> {
        let config: JsonParserConfig = serde_yaml::from_str(
            "columns:\n  - jsonpath: $.id\n    column_name: id\n    arrow_type: UInt64\n    nullable: false\n",
        )?;
        let schema = config.to_dataset_schema()?;
        anyhow::ensure!(schema.columns.len() == 1);
        anyhow::ensure!(schema.columns[0].name == "id");
        anyhow::ensure!(schema.columns[0].data_type == DataType::UInt64);
        anyhow::ensure!(!schema.columns[0].nullable);
        Ok(())
    }

    #[test]
    fn rejects_clickhouse_sorting_from_parser_config() {
        let result = serde_yaml::from_str::<JsonParserConfig>("columns: []\norder_by: [id]\n");
        assert!(result.is_err());
    }
}
