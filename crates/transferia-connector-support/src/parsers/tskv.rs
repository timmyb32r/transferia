use std::collections::{BTreeMap, HashMap};
use std::str::FromStr as _;
use std::sync::Arc;

use bytes::Bytes;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Map, Number, Value};

use transferia_core::data::message::Message;
use transferia_core::data::schema::DatasetSchema;
use transferia_core::data::table_data::TableData;
use transferia_delivery_contracts::parser::{ParserFactory, ParserSession};

use super::json_parser::{
    ColumnMapping, ConversionErrorPolicy, JsonDataType, JsonFramingMode, JsonParser,
    JsonParserConfig, TimeConversion, UnknownFieldPolicy,
};
use super::SystemColumnsConfig;

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TskvParserConfig {
    #[schemars(
        title = "Data schema",
        length(min = 1),
        extend("x-ui" = { "widget": "column_mappings", "initial_items": 1 })
    )]
    pub columns: Vec<TskvColumnMapping>,

    #[serde(default)]
    #[schemars(
        title = "On Unknown Field",
        extend("x-ui" = { "control_width": "routing" })
    )]
    pub unknown_fields: UnknownFieldPolicy,

    #[serde(default)]
    #[schemars(title = "Keys", extend("x-ui" = { "widget": "column_keys" }))]
    pub keys: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TskvColumnMapping {
    pub column_name: String,

    #[serde(default = "default_arrow_type")]
    #[schemars(default = "default_arrow_type", extend("x-ui" = {
        "widget": "select",
        "options": [
            "Utf8", "LargeUtf8", "Int64", "Int32", "Int16", "Int8",
            "UInt64", "UInt32", "UInt16", "UInt8", "Float64", "Float32",
            "Boolean", "Decimal128", "Date32", "Date64", "Timestamp(Second)",
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

impl TskvParserConfig {
    fn json_config(&self) -> JsonParserConfig {
        JsonParserConfig {
            json_framing: JsonFramingMode::SingleDocument,
            columns: self.columns.iter().map(TskvColumnMapping::json_mapping).collect(),
            conversion_error: ConversionErrorPolicy::Fail,
            unknown_fields: self.unknown_fields.clone(),
            keys: self.keys.clone(),
        }
    }

    pub fn to_dataset_schema(&self) -> anyhow::Result<DatasetSchema> {
        self.json_config().to_dataset_schema()
    }
}

impl TskvColumnMapping {
    fn json_mapping(&self) -> ColumnMapping {
        ColumnMapping {
            jsonpath: format!("$.{}", self.column_name),
            column_name: self.column_name.clone(),
            json_data_type: self.value_kind(),
            arrow_type: self.arrow_type.clone(),
            decimal_precision: self.decimal_precision,
            decimal_scale: self.decimal_scale,
            nullable: self.nullable,
            time_conversion: self.time_conversion.clone(),
            low_cardinality: self.low_cardinality,
            max_length: self.max_length,
        }
    }

    fn value_kind(&self) -> JsonDataType {
        if self.arrow_type == "Boolean" {
            JsonDataType::Boolean
        } else if self.arrow_type == "Decimal128" {
            JsonDataType::Decimal
        } else if self.arrow_type.starts_with("Int")
            || self.arrow_type.starts_with("UInt")
            || self.arrow_type.starts_with("Float")
        {
            JsonDataType::Number
        } else {
            JsonDataType::String
        }
    }
}

pub struct TskvParser {
    inner: Arc<JsonParser>,
    kinds: HashMap<String, JsonDataType>,
}

impl TskvParser {
    pub fn new(
        config: &TskvParserConfig,
        system_columns: &SystemColumnsConfig,
        table: Arc<str>,
    ) -> anyhow::Result<Self> {
        let json_config = config.json_config();
        let inner = Arc::new(JsonParser::new(&json_config, system_columns, table)?);
        let kinds = config
            .columns
            .iter()
            .map(|column| (column.column_name.clone(), column.value_kind()))
            .collect();
        Ok(Self { inner, kinds })
    }
}

struct TskvParserSession {
    inner: Box<dyn ParserSession>,
    kinds: HashMap<String, JsonDataType>,
}

impl ParserFactory for TskvParser {
    fn create_session(self: Arc<Self>, memory_limit_bytes: usize) -> Box<dyn ParserSession> {
        Box::new(TskvParserSession {
            inner: Arc::clone(&self.inner).create_session(memory_limit_bytes),
            kinds: self.kinds.clone(),
        })
    }
}

impl ParserSession for TskvParserSession {
    fn output_memory_bound(&self, messages: &[Message]) -> usize {
        let escaped_json_bound = messages.iter().fold(0_usize, |total, message| {
            total.saturating_add(message.value.len().saturating_mul(6).saturating_add(2))
        });
        self.inner
            .output_memory_bound(messages)
            .saturating_add(escaped_json_bound)
    }

    fn parse_into(
        &mut self,
        messages: Vec<Message>,
    ) -> anyhow::Result<(TableData, Option<TableData>)> {
        let messages = messages
            .into_iter()
            .map(|message| transform_message(message, &self.kinds))
            .collect::<anyhow::Result<Vec<_>>>()?;
        self.inner.parse_into(messages)
    }
}

fn transform_message(
    mut message: Message,
    kinds: &HashMap<String, JsonDataType>,
) -> anyhow::Result<Message> {
    anyhow::ensure!(!message.tombstone, "TSKV parser does not accept tombstones");
    let fields = parse_record(&message.value)?;
    let mut object = Map::with_capacity(fields.len());
    for (key, raw) in fields {
        let value = match kinds.get(&key).copied().unwrap_or(JsonDataType::String) {
            JsonDataType::String | JsonDataType::Decimal => Value::String(raw),
            JsonDataType::Boolean => match raw.as_str() {
                "true" => Value::Bool(true),
                "false" => Value::Bool(false),
                _ => anyhow::bail!("TSKV field '{key}' is not a boolean"),
            },
            JsonDataType::Number => Value::Number(
                Number::from_str(&raw)
                    .map_err(|_| anyhow::anyhow!("TSKV field '{key}' is not a JSON number"))?,
            ),
            JsonDataType::Json => anyhow::bail!("TSKV cannot produce a JSON source value"),
        };
        object.insert(key, value);
    }
    message.value = Bytes::from(serde_json::to_vec(&Value::Object(object))?);
    Ok(message)
}

pub(crate) fn parse_record(payload: &[u8]) -> anyhow::Result<BTreeMap<String, String>> {
    let text = std::str::from_utf8(payload).map_err(|_| anyhow::anyhow!("TSKV is not UTF-8"))?;
    let mut parts = text.split('\t');
    anyhow::ensure!(parts.next() == Some("tskv"), "TSKV record must start with 'tskv'");
    let mut fields = BTreeMap::new();
    for field in parts {
        let (key, value) = field
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("TSKV field is missing '='"))?;
        let key = unescape(key)?;
        anyhow::ensure!(!key.is_empty(), "TSKV field name must not be empty");
        let value = unescape(value)?;
        anyhow::ensure!(
            fields.insert(key.clone(), value).is_none(),
            "TSKV record repeats field '{key}'"
        );
    }
    Ok(fields)
}

fn unescape(value: &str) -> anyhow::Result<String> {
    let mut output = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(character) = chars.next() {
        if character != '\\' {
            output.push(character);
            continue;
        }
        let escaped = chars
            .next()
            .ok_or_else(|| anyhow::anyhow!("TSKV value ends with an escape prefix"))?;
        output.push(match escaped {
            '\\' => '\\',
            't' => '\t',
            'n' => '\n',
            'r' => '\r',
            '0' => '\0',
            other => anyhow::bail!("TSKV contains unsupported escape '\\{other}'"),
        });
    }
    Ok(output)
}

#[cfg(test)]
#[path = "tests/tskv.rs"]
mod tests;
