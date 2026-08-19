use std::collections::HashSet;

use arrow::datatypes::{DataType, TimeUnit};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use transferia_core::data::schema::{DatasetSchema, SchemaColumn, ARROW_JSON_EXTENSION_NAME};

/// JSON parser configuration deserialized from the `json_parser:` block.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JsonParserConfig {
    /// How incoming message bytes frame JSON records.
    #[serde(default)]
    #[schemars(title = "JSON framing", extend("x-ui" = {
        "control_width": "medium",
        "labels": {
            "single_document": "Single JSON document",
            "json_lines": "JSON Lines (JSONL)",
            "json_array": "JSON array"
        }
    }))]
    pub json_framing: JsonFramingMode,

    #[schemars(
        title = "Data schema",
        extend("x-ui" = { "widget": "column_mappings", "initial_items": 1 })
    )]
    pub columns: Vec<ColumnMapping>,

    #[schemars(title = "On Parse Error", extend("x-ui" = {
        "labels": {
            "dlq": "Send to DLQ",
            "drop": "Drop",
            "fail": "Fail delivery"
        }
    }))]
    pub conversion_error: ConversionErrorPolicy,

    #[serde(default)]
    #[schemars(title = "On Unknown Field")]
    pub unknown_fields: UnknownFieldPolicy,

    #[serde(default)]
    #[schemars(title = "Keys", extend("x-ui" = { "widget": "column_keys" }))]
    pub keys: Vec<String>,
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
                column.to_schema_column(self.keys.contains(&column.column_name))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let mut columns = columns;
        match &self.unknown_fields {
            UnknownFieldPolicy::Drop | UnknownFieldPolicy::Fail => {}
            UnknownFieldPolicy::SendToColumn { column_name } => {
                anyhow::ensure!(
                    !column_name.is_empty() && !names.contains(column_name.as_str()),
                    "unknown_fields.send_to_column column_name must be non-empty and unique"
                );
                names.insert(column_name);
                columns.push(
                    SchemaColumn::new(column_name.clone(), DataType::Utf8, false)
                        .with_arrow_extension(ARROW_JSON_EXTENSION_NAME),
                );
            }
        }
        Ok(DatasetSchema::new(columns))
    }
}

/// Record framing policy owned by the JSON parser.
#[derive(Debug, Clone, Copy, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JsonFramingMode {
    #[default]
    SingleDocument,
    JsonLines,
    JsonArray,
}

impl JsonFramingMode {
    /// Count records without allocating a `Vec`.
    #[must_use]
    pub fn count_records(self, buf: &[u8]) -> usize {
        match self {
            Self::SingleDocument => 1,
            Self::JsonLines | Self::JsonArray if buf.is_empty() => 0,
            Self::JsonLines | Self::JsonArray if !buf.contains(&b'\n') => 1,
            Self::JsonLines | Self::JsonArray => buf
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
    Drop,
    Fail,
}

#[derive(Debug, Clone, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum UnknownFieldPolicy {
    #[schemars(title = "Drop")]
    Drop,

    #[schemars(title = "Fail delivery")]
    Fail,

    #[schemars(title = "Send to a column")]
    SendToColumn {
        #[serde(default = "default_additional_properties_column")]
        #[schemars(
            title = "Column name",
            default = "default_additional_properties_column"
        )]
        column_name: String,
    },
}

impl Default for UnknownFieldPolicy {
    fn default() -> Self {
        Self::SendToColumn {
            column_name: default_additional_properties_column(),
        }
    }
}

fn default_additional_properties_column() -> String {
    "additional_properties".to_owned()
}

#[derive(Debug, Clone, Copy, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JsonDataType {
    #[schemars(title = "String")]
    #[default]
    String,

    #[schemars(title = "Number")]
    Number,

    #[schemars(title = "Boolean")]
    Boolean,

    #[schemars(title = "JSON")]
    Json,

    #[schemars(title = "Decimal (exact string)")]
    Decimal,
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

    #[serde(default)]
    #[schemars(default = "default_json_data_type", extend("x-ui" = {
        "labels": {
            "string": "String",
            "number": "Number",
            "boolean": "Boolean"
        }
    }))]
    pub json_data_type: JsonDataType,

    #[serde(default = "default_arrow_type")]
    #[schemars(default = "default_arrow_type", extend("x-ui" = {
        "widget": "select",
        "options": [
            "Utf8", "LargeUtf8", "Int64", "Int32", "Int16", "Int8",
            "UInt64", "UInt32", "UInt16", "UInt8", "Float64", "Float32",
            "Boolean", "Json", "Decimal128", "Date32", "Date64", "Timestamp(Second)",
            "Timestamp(Millisecond)", "Timestamp(Microsecond)",
            "Timestamp(Nanosecond)", "Timestamp(Second, UTC)",
            "Timestamp(Millisecond, UTC)", "Timestamp(Microsecond, UTC)",
            "Timestamp(Nanosecond, UTC)"
        ]
    }))]
    pub arrow_type: String,

    #[serde(default)]
    #[schemars(title = "Decimal precision", extend("x-ui" = { "section": "advanced" }))]
    pub decimal_precision: Option<u8>,

    #[serde(default)]
    #[schemars(title = "Decimal scale", extend("x-ui" = { "section": "advanced" }))]
    pub decimal_scale: Option<i8>,

    #[serde(default)]
    pub nullable: bool,

    #[serde(default)]
    pub time_conversion: Option<TimeConversion>,

    #[serde(default)]
    pub low_cardinality: bool,

    #[serde(default)]
    pub max_length: Option<usize>,
}

fn default_arrow_type() -> String {
    "Utf8".to_owned()
}

const fn default_json_data_type() -> JsonDataType {
    JsonDataType::String
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
            decimal_precision: None,
            decimal_scale: None,
            nullable,
            time_conversion: None,
            low_cardinality: false,
            max_length: None,
        }
    }

    pub fn to_schema_column(&self, primary_key: bool) -> anyhow::Result<SchemaColumn> {
        let arrow_type = self.data_type()?;
        validate_conversion(self, &arrow_type)?;
        let mut column = SchemaColumn::new(self.column_name.clone(), arrow_type, self.nullable)
            .with_constraints(primary_key, self.low_cardinality, self.max_length);
        if self.arrow_type == "Json" {
            column = column.with_arrow_extension(ARROW_JSON_EXTENSION_NAME);
        }
        Ok(column)
    }

    pub fn data_type(&self) -> anyhow::Result<DataType> {
        if self.arrow_type == "Decimal128" {
            let precision = self.decimal_precision.ok_or_else(|| {
                anyhow::anyhow!(
                    "column '{}': Decimal128 requires decimal_precision",
                    self.column_name
                )
            })?;
            let scale = self.decimal_scale.ok_or_else(|| {
                anyhow::anyhow!(
                    "column '{}': Decimal128 requires decimal_scale",
                    self.column_name
                )
            })?;
            anyhow::ensure!(
                (1..=38).contains(&precision),
                "column '{}': decimal_precision must be between 1 and 38",
                self.column_name
            );
            anyhow::ensure!(
                scale.unsigned_abs() <= precision,
                "column '{}': absolute decimal_scale must not exceed decimal_precision",
                self.column_name
            );
            return Ok(DataType::Decimal128(precision, scale));
        }
        anyhow::ensure!(
            self.decimal_precision.is_none() && self.decimal_scale.is_none(),
            "column '{}': decimal_precision and decimal_scale are valid only for Decimal128",
            self.column_name
        );
        parse_arrow_type(&self.arrow_type)
    }
}

#[cfg(test)]
const fn json_type_for_arrow_literal(arrow_type: &str) -> JsonDataType {
    match arrow_type.as_bytes() {
        b"Utf8" | b"LargeUtf8" => JsonDataType::String,
        b"Boolean" => JsonDataType::Boolean,
        b"Json" => JsonDataType::Json,
        b"Decimal128" => JsonDataType::Decimal,
        _ => JsonDataType::Number,
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
    let is_decimal = matches!(arrow_type, DataType::Decimal128(..));
    let is_temporal = matches!(
        arrow_type,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(..)
    );
    let allowed = match column.json_data_type {
        JsonDataType::String => is_string || is_temporal,
        JsonDataType::Number => is_signed || is_unsigned || is_float || is_temporal,
        JsonDataType::Boolean => *arrow_type == DataType::Boolean,
        JsonDataType::Json => column.arrow_type == "Json",
        JsonDataType::Decimal => is_decimal,
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
            column.json_data_type == JsonDataType::Number,
            "column '{}': epoch conversion requires numeric JSON data",
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
        "Utf8" | "Json" => Ok(DataType::Utf8),
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
