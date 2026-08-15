use std::collections::HashSet;

use arrow::datatypes::{DataType, TimeUnit};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::types::schema::{DatasetSchema, SchemaColumn};
use crate::types::system_columns::SystemColumnKind;

/// JSON parser configuration deserialized from the `json_parser:` block.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JsonParserConfig {
    #[schemars(
        title = "Data schema",
        extend("x-ui" = { "widget": "column_mappings", "initial_items": 1 })
    )]
    pub columns: Vec<ColumnMapping>,

    /// How incoming message bytes are split into individual JSON objects.
    #[serde(default)]
    pub chunk_splitter: ChunkSplitter,

    #[schemars(extend("x-ui" = {
        "labels": { "dlq": "Send to DLQ", "fail": "Fail delivery" }
    }))]
    pub conversion_error: ConversionErrorPolicy,

    pub unknown_fields: UnknownFieldPolicy,

    #[serde(default)]
    #[schemars(extend("x-ui" = { "section": "advanced" }))]
    pub primary_key: Vec<String>,

    #[serde(default)]
    #[schemars(extend("x-ui" = { "section": "system_columns" }))]
    pub system_column_names: SystemColumnNames,
}

impl JsonParserConfig {
    /// Validate the complete user-visible conversion contract and return its schema.
    pub fn to_dataset_schema(&self) -> anyhow::Result<DatasetSchema> {
        anyhow::ensure!(
            !self.columns.is_empty(),
            "json_parser.columns must not be empty"
        );
        let mut names = HashSet::with_capacity(self.columns.len());
        let columns = self
            .columns
            .iter()
            .map(|column| {
                anyhow::ensure!(
                    names.insert(column.column_name.as_str()),
                    "json_parser.columns repeats column_name '{}'",
                    column.column_name
                );
                column.to_schema_column(self.primary_key.contains(&column.column_name))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let mut columns = columns;
        match &self.unknown_fields {
            UnknownFieldPolicy::Fail => {}
            UnknownFieldPolicy::Rest { column_name } => {
                anyhow::ensure!(
                    !column_name.is_empty() && !names.contains(column_name.as_str()),
                    "unknown_fields.rest column_name must be non-empty and unique"
                );
                names.insert(column_name);
                columns.push(SchemaColumn::new(
                    column_name.clone(),
                    DataType::Utf8,
                    false,
                ));
            }
        }
        self.system_column_names.validate()?;
        Ok(DatasetSchema::new(columns))
    }
}

#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SystemColumnNames {
    pub topic: Option<String>,

    pub partition: Option<String>,

    pub offset: Option<String>,

    pub message_index: Option<String>,

    pub write_timestamp_ms: Option<String>,
}

impl SystemColumnNames {
    fn validate(&self) -> anyhow::Result<()> {
        let mut names = HashSet::new();
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
                "system_column_names repeats column name '{name}'"
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

/// Record framing policy owned by the JSON parser.
#[derive(Debug, Clone, Copy, Default, Deserialize, JsonSchema, PartialEq, Eq)]
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

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConversionErrorPolicy {
    Dlq,
    Fail,
}

#[derive(Debug, Clone, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum UnknownFieldPolicy {
    Fail,
    Rest { column_name: String },
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JsonDataType {
    String,
    Integer,
    UnsignedInteger,
    Number,
    Boolean,
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EpochUnit {
    Seconds,
    Milliseconds,
    Microseconds,
    Nanoseconds,
}

#[derive(Debug, Clone, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum TimeConversion {
    String { format: String },
    Epoch { unit: EpochUnit },
}

/// Explicit `JSONPath` and JSON-to-Arrow conversion for one output column.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ColumnMapping {
    pub jsonpath: String,

    pub column_name: String,

    pub json_data_type: JsonDataType,

    #[schemars(extend("x-ui" = {
        "widget": "select",
        "options": [
            "Utf8", "LargeUtf8", "Int64", "Int32", "Int16", "Int8",
            "UInt64", "UInt32", "UInt16", "UInt8", "Float64", "Float32",
            "Boolean", "Date32", "Date64", "Timestamp(Second)",
            "Timestamp(Millisecond)", "Timestamp(Microsecond)",
            "Timestamp(Nanosecond)", "Timestamp(Second, UTC)",
            "Timestamp(Millisecond, UTC)", "Timestamp(Microsecond, UTC)",
            "Timestamp(Nanosecond, UTC)"
        ]
    }))]
    pub arrow_type: String,

    #[serde(default)]
    pub nullable: bool,

    #[serde(default)]
    pub time_conversion: Option<TimeConversion>,

    #[serde(default)]
    pub low_cardinality: bool,

    #[serde(default)]
    pub max_length: Option<usize>,
}

impl ColumnMapping {
    #[cfg(test)]
    pub(super) fn new(
        jsonpath: String,
        column_name: String,
        arrow_type: String,
        nullable: bool,
    ) -> Self {
        let json_data_type = json_type_for_arrow_literal(&arrow_type);
        Self {
            jsonpath,
            column_name,
            json_data_type,
            arrow_type,
            nullable,
            time_conversion: None,
            low_cardinality: false,
            max_length: None,
        }
    }

    pub fn to_schema_column(&self, primary_key: bool) -> anyhow::Result<SchemaColumn> {
        let arrow_type = parse_arrow_type(&self.arrow_type)?;
        validate_conversion(self, &arrow_type)?;
        Ok(
            SchemaColumn::new(self.column_name.clone(), arrow_type, self.nullable)
                .with_constraints(primary_key, self.low_cardinality, self.max_length),
        )
    }
}

#[cfg(test)]
const fn json_type_for_arrow_literal(arrow_type: &str) -> JsonDataType {
    match arrow_type.as_bytes() {
        b"Utf8" | b"LargeUtf8" => JsonDataType::String,
        b"UInt8" | b"UInt16" | b"UInt32" | b"UInt64" => JsonDataType::UnsignedInteger,
        b"Float32" | b"Float64" => JsonDataType::Number,
        b"Boolean" => JsonDataType::Boolean,
        _ => JsonDataType::Integer,
    }
}

fn validate_conversion(column: &ColumnMapping, arrow_type: &DataType) -> anyhow::Result<()> {
    let is_string = matches!(arrow_type, DataType::Utf8 | DataType::LargeUtf8);
    let is_signed = matches!(
        arrow_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    );
    let is_unsigned = matches!(
        arrow_type,
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
    );
    let is_float = matches!(arrow_type, DataType::Float32 | DataType::Float64);
    let is_temporal = matches!(
        arrow_type,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(..)
    );
    let allowed = match column.json_data_type {
        JsonDataType::String => is_string || is_temporal,
        JsonDataType::Integer => is_signed || is_float || is_temporal,
        JsonDataType::UnsignedInteger => is_unsigned || is_float || is_temporal,
        JsonDataType::Number => is_float,
        JsonDataType::Boolean => *arrow_type == DataType::Boolean,
    };
    anyhow::ensure!(
        allowed,
        "column '{}': unsupported {:?} -> {:?} conversion",
        column.column_name,
        column.json_data_type,
        arrow_type
    );
    anyhow::ensure!(
        column.time_conversion.is_some() == is_temporal,
        "column '{}': temporal Arrow types require time_conversion and non-temporal types forbid it",
        column.column_name
    );
    if let Some(TimeConversion::String { format }) = &column.time_conversion {
        anyhow::ensure!(
            column.json_data_type == JsonDataType::String && !format.is_empty(),
            "column '{}': string time conversion requires json_data_type=string and non-empty format",
            column.column_name
        );
        time::format_description::parse_borrowed::<2>(format).map_err(|error| {
            anyhow::anyhow!(
                "column '{}': invalid time_conversion format: {error}",
                column.column_name
            )
        })?;
    }
    if matches!(column.time_conversion, Some(TimeConversion::Epoch { .. })) {
        anyhow::ensure!(
            matches!(
                column.json_data_type,
                JsonDataType::Integer | JsonDataType::UnsignedInteger
            ),
            "column '{}': epoch conversion requires integer JSON data",
            column.column_name
        );
    }
    anyhow::ensure!(
        !column.low_cardinality || is_string,
        "column '{}': low_cardinality is supported only for string Arrow columns",
        column.column_name
    );
    anyhow::ensure!(
        column.max_length.is_none() || is_string,
        "column '{}': max_length is supported only for string Arrow columns",
        column.column_name
    );
    anyhow::ensure!(
        column.max_length.is_none_or(|value| value > 0),
        "column '{}': max_length must be positive",
        column.column_name
    );
    Ok(())
}

/// Parse the JSON parser's human-readable Arrow type syntax.
pub fn parse_arrow_type(value: &str) -> anyhow::Result<DataType> {
    match value {
        "Utf8" => Ok(DataType::Utf8),
        "LargeUtf8" => Ok(DataType::LargeUtf8),
        "Int64" => Ok(DataType::Int64),
        "Int32" => Ok(DataType::Int32),
        "Int16" => Ok(DataType::Int16),
        "Int8" => Ok(DataType::Int8),
        "UInt64" => Ok(DataType::UInt64),
        "UInt32" => Ok(DataType::UInt32),
        "UInt16" => Ok(DataType::UInt16),
        "UInt8" => Ok(DataType::UInt8),
        "Float64" => Ok(DataType::Float64),
        "Float32" => Ok(DataType::Float32),
        "Boolean" => Ok(DataType::Boolean),
        "Date32" => Ok(DataType::Date32),
        "Date64" => Ok(DataType::Date64),
        _ if value.starts_with("Timestamp") => {
            let inner = value
                .strip_prefix("Timestamp(")
                .and_then(|inner| inner.strip_suffix(')'))
                .ok_or_else(|| anyhow::anyhow!("invalid Timestamp type '{value}'"))?;
            let parts: Vec<&str> = inner.split(',').map(str::trim).collect();
            anyhow::ensure!(
                matches!(parts.len(), 1 | 2) && parts.iter().all(|part| !part.is_empty()),
                "invalid Timestamp type '{value}'"
            );
            let unit = match parts[0] {
                "Second" => TimeUnit::Second,
                "Millisecond" => TimeUnit::Millisecond,
                "Microsecond" => TimeUnit::Microsecond,
                "Nanosecond" => TimeUnit::Nanosecond,
                other => anyhow::bail!("unsupported Timestamp unit '{other}'"),
            };
            let timezone = (parts.len() == 2).then(|| parts[1].to_string().into());
            Ok(DataType::Timestamp(unit, timezone))
        }
        other => anyhow::bail!("unsupported arrow_type '{other}'"),
    }
}

#[cfg(test)]
#[path = "tests/config.rs"]
mod tests;
