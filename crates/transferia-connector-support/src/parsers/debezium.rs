use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::BinaryArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::parsers::json_parser::{
    ColumnMapping, ConversionErrorPolicy, JsonDataType, JsonFramingMode, JsonParser,
    JsonParserConfig, UnknownFieldPolicy,
};
use crate::parsers::{ParserFactory, ParserSession, SystemColumnsConfig};
use crate::schema_registry::{ConfluentEnvelope, RegistryClient, SchemaRegistryConnection};
use transferia_core::data::message::Message;
use transferia_core::data::schema::{
    DatasetSchema, SchemaColumn, META_CHANGE_OPERATION, SYSTEM_ROLE_EVENT_TIMESTAMP_MS,
    SYSTEM_ROLE_EVENT_TIMESTAMP_NS, SYSTEM_ROLE_EVENT_TIMESTAMP_US, SYSTEM_ROLE_SOURCE_DATABASE,
    SYSTEM_ROLE_SOURCE_SCHEMA, SYSTEM_ROLE_SOURCE_TABLE, SYSTEM_ROLE_SOURCE_TIMESTAMP_MS,
    SYSTEM_ROLE_SOURCE_TIMESTAMP_NS, SYSTEM_ROLE_SOURCE_TIMESTAMP_US,
    SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
};
use transferia_core::data::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use transferia_core::data::table_data::TableData;

use super::SchemaDecoder;

const UNAVAILABLE_VALUE: &str = "__debezium_unavailable_value";
const NORMALIZED_OFFSET: &str = "_debezium_offset";
const NORMALIZED_OPERATION: &str = "_debezium_operation";

const ROLE_COLUMNS: [(&str, &str, &str, JsonDataType); 10] = [
    (
        "_system_source_database",
        "$.source.db",
        SYSTEM_ROLE_SOURCE_DATABASE,
        JsonDataType::String,
    ),
    (
        "_system_source_schema",
        "$.source.schema",
        SYSTEM_ROLE_SOURCE_SCHEMA,
        JsonDataType::String,
    ),
    (
        "_system_source_table",
        "$.source.table",
        SYSTEM_ROLE_SOURCE_TABLE,
        JsonDataType::String,
    ),
    (
        "_system_source_transaction_id",
        "$.source.txId",
        SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
        JsonDataType::Number,
    ),
    (
        "_system_source_timestamp_ms",
        "$.source.ts_ms",
        SYSTEM_ROLE_SOURCE_TIMESTAMP_MS,
        JsonDataType::Number,
    ),
    (
        "_system_source_timestamp_us",
        "$.source.ts_us",
        SYSTEM_ROLE_SOURCE_TIMESTAMP_US,
        JsonDataType::Number,
    ),
    (
        "_system_source_timestamp_ns",
        "$.source.ts_ns",
        SYSTEM_ROLE_SOURCE_TIMESTAMP_NS,
        JsonDataType::Number,
    ),
    (
        "_system_event_timestamp_ms",
        "$.ts_ms",
        SYSTEM_ROLE_EVENT_TIMESTAMP_MS,
        JsonDataType::Number,
    ),
    (
        "_system_event_timestamp_us",
        "$.ts_us",
        SYSTEM_ROLE_EVENT_TIMESTAMP_US,
        JsonDataType::Number,
    ),
    (
        "_system_event_timestamp_ns",
        "$.ts_ns",
        SYSTEM_ROLE_EVENT_TIMESTAMP_NS,
        JsonDataType::Number,
    ),
];

#[derive(Clone, Default, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum DebeziumInput {
    #[default]
    #[schemars(title = "JSON")]
    Json,

    #[schemars(title = "Confluent Schema Registry")]
    SchemaRegistry {
        connection: SchemaRegistryConnection,
    },
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DebeziumParserConfig {
    #[serde(default)]
    #[schemars(title = "Input format")]
    pub input: DebeziumInput,

    #[schemars(
        title = "Data schema",
        length(min = 1),
        extend("x-ui" = { "widget": "column_mappings", "initial_items": 1 })
    )]
    pub columns: Vec<ColumnMapping>,

    #[schemars(
        title = "Primary key columns",
        description = "At least one non-nullable output column that uniquely identifies a row",
        length(min = 1)
    )]
    pub keys: Vec<String>,
}

impl DebeziumParserConfig {
    pub fn schemas(&self) -> anyhow::Result<(DatasetSchema, DatasetSchema)> {
        let user = self.user_projection()?.to_dataset_schema()?;
        anyhow::ensure!(
            !self.keys.is_empty(),
            "debezium.keys must contain at least one primary-key column"
        );
        let mut keys = HashSet::with_capacity(self.keys.len());
        for key in &self.keys {
            anyhow::ensure!(keys.insert(key), "debezium.keys repeats column '{key}'");
            let column = user
                .columns
                .iter()
                .find(|column| &column.name == key)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "debezium primary-key column '{key}' is not produced by the parser"
                    )
                })?;
            anyhow::ensure!(
                !column.nullable,
                "debezium primary-key column '{key}' must be non-nullable"
            );
        }

        let mut incoming = user.clone();
        for column in &mut incoming.columns {
            if !column.primary_key {
                column.nullable = true;
            }
        }
        incoming
            .columns
            .extend(user.columns.iter().enumerate().map(|(index, column)| {
                SchemaColumn::new(old_value_column_name(index), column.data_type.clone(), true)
                    .with_old_value_of(column.name.clone())
            }));
        incoming
            .columns
            .extend(ROLE_COLUMNS.iter().map(|(name, _, role, json_type)| {
                let data_type = if *json_type == JsonDataType::String {
                    DataType::Utf8
                } else if *role == SYSTEM_ROLE_SOURCE_TRANSACTION_ID {
                    DataType::UInt64
                } else {
                    DataType::Int64
                };
                SchemaColumn::new((*name).to_owned(), data_type, false).with_system_role(*role)
            }));
        Ok((incoming, user))
    }

    fn user_projection(&self) -> anyhow::Result<JsonParserConfig> {
        let mut names = HashSet::with_capacity(self.columns.len());
        for column in &self.columns {
            anyhow::ensure!(
                column.jsonpath.starts_with('$'),
                "debezium column '{}' JSONPath must start with '$'",
                column.column_name
            );
            anyhow::ensure!(
                names.insert(column.column_name.as_str()),
                "debezium.columns repeats column_name '{}'",
                column.column_name
            );
            anyhow::ensure!(
                !column.column_name.starts_with("_system_"),
                "debezium user column '{}' conflicts with reserved control columns",
                column.column_name
            );
        }
        Ok(JsonParserConfig {
            json_framing: JsonFramingMode::SingleDocument,
            columns: self.columns.clone(),
            conversion_error: ConversionErrorPolicy::Fail,
            unknown_fields: UnknownFieldPolicy::Fail,
            keys: self.keys.clone(),
        })
    }

    fn normalized_projection(&self) -> anyhow::Result<JsonParserConfig> {
        let mut projection = self.user_projection()?;
        for (index, mapping) in projection.columns.iter_mut().enumerate() {
            mapping.jsonpath = format!("$.current[{index}]");
            if !self.keys.iter().any(|key| key == &mapping.column_name) {
                mapping.nullable = true;
            }
        }
        projection
            .columns
            .extend(self.columns.iter().enumerate().map(|(index, mapping)| {
                let mut mapping = mapping.clone();
                mapping.jsonpath = format!("$.before[{index}]");
                mapping.column_name = old_value_column_name(index);
                mapping.nullable = true;
                mapping
            }));
        projection.columns.extend(
            ROLE_COLUMNS
                .iter()
                .map(|(name, path, role, json_data_type)| {
                    primitive_mapping(
                        name,
                        path,
                        *json_data_type,
                        if *json_data_type == JsonDataType::String {
                            "Utf8"
                        } else if *role == SYSTEM_ROLE_SOURCE_TRANSACTION_ID {
                            "UInt64"
                        } else {
                            "Int64"
                        },
                    )
                }),
        );
        projection.columns.push(primitive_mapping(
            NORMALIZED_OFFSET,
            "$.source.lsn",
            JsonDataType::Number,
            "Int64",
        ));
        projection.columns.push(primitive_mapping(
            NORMALIZED_OPERATION,
            "$.op",
            JsonDataType::String,
            "Utf8",
        ));
        projection.keys.clone_from(&self.keys);
        projection.unknown_fields = UnknownFieldPolicy::Drop;
        Ok(projection)
    }
}

pub struct DebeziumParser {
    input: DebeziumInput,
    json: Arc<JsonParser>,
    incoming_schema: DatasetSchema,
    registry: Option<RegistryClient>,
    user_paths: Arc<[jsonpath_lib::Compiled]>,
    user_json_types: Arc<[JsonDataType]>,
    user_top_level_fields: Arc<[String]>,
    table: Arc<str>,
}

impl DebeziumParser {
    pub fn new(config: &DebeziumParserConfig, table: Arc<str>) -> anyhow::Result<Self> {
        let (incoming_schema, _) = config.schemas()?;
        let projection = config.normalized_projection()?;
        let registry = match &config.input {
            DebeziumInput::Json => None,
            DebeziumInput::SchemaRegistry { connection } => Some(RegistryClient::new(connection)?),
        };
        let user_paths = config
            .columns
            .iter()
            .map(|column| {
                jsonpath_lib::Compiled::compile(&column.jsonpath).map_err(|error| {
                    anyhow::anyhow!(
                        "column '{}': invalid JSONPath '{}': {error}",
                        column.column_name,
                        column.jsonpath
                    )
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let user_top_level_fields = config
            .columns
            .iter()
            .map(|column| top_level_field(&column.jsonpath).map(str::to_owned))
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(Self {
            input: config.input.clone(),
            json: Arc::new(JsonParser::new(
                &projection,
                &SystemColumnsConfig::default(),
                Arc::clone(&table),
            )?),
            incoming_schema,
            registry,
            user_paths: user_paths.into(),
            user_json_types: config
                .columns
                .iter()
                .map(|column| column.json_data_type)
                .collect::<Vec<_>>()
                .into(),
            user_top_level_fields: user_top_level_fields.into(),
            table,
        })
    }
}

impl ParserFactory for DebeziumParser {
    fn create_session(self: Arc<Self>, memory_limit_bytes: usize) -> Box<dyn ParserSession> {
        Box::new(DebeziumParserSession {
            parser: Arc::clone(&self),
            json: Arc::clone(&self.json).create_session(memory_limit_bytes),
            registry: self.registry.clone(),
            decoder: SchemaDecoder::default(),
            runtime: None,
            memory_limit_bytes,
        })
    }
}

struct DebeziumParserSession {
    parser: Arc<DebeziumParser>,
    json: Box<dyn ParserSession>,
    registry: Option<RegistryClient>,
    decoder: SchemaDecoder,
    runtime: Option<tokio::runtime::Runtime>,
    memory_limit_bytes: usize,
}

impl DebeziumParserSession {
    fn runtime(&mut self) -> anyhow::Result<&tokio::runtime::Runtime> {
        if self.runtime.is_none() {
            self.runtime = Some(
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?,
            );
        }
        self.runtime
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Debezium parser runtime was not initialized"))
    }

    fn registry_schemas(
        &mut self,
        messages: &[Message],
    ) -> anyhow::Result<HashMap<i32, crate::schema_registry::RegistrySchema>> {
        let Some(registry) = self.registry.clone() else {
            return Ok(HashMap::new());
        };
        let ids = messages
            .iter()
            .filter(|message| !message.tombstone)
            .map(|message| ConfluentEnvelope::decode(&message.value).map(|value| value.schema_id))
            .collect::<anyhow::Result<HashSet<_>>>()?;
        self.runtime()?.block_on(async move {
            let mut schemas = HashMap::with_capacity(ids.len());
            for id in ids {
                schemas.insert(id, registry.schema_by_id(id).await?);
            }
            anyhow::Ok(schemas)
        })
    }

    fn decode_value(
        &mut self,
        message: &Message,
        schemas: &HashMap<i32, crate::schema_registry::RegistrySchema>,
    ) -> anyhow::Result<Value> {
        match &self.parser.input {
            DebeziumInput::Json => Ok(serde_json::from_slice(&message.value)?),
            DebeziumInput::SchemaRegistry { .. } => {
                let envelope = ConfluentEnvelope::decode(&message.value)?;
                let schema = schemas.get(&envelope.schema_id).ok_or_else(|| {
                    anyhow::anyhow!(
                        "prefetched Schema Registry schema id {} is absent",
                        envelope.schema_id
                    )
                })?;
                self.decoder.decode(schema, envelope.payload)
            }
        }
    }
}

impl ParserSession for DebeziumParserSession {
    fn output_memory_bound(&self, messages: &[Message]) -> usize {
        let input = messages
            .iter()
            .map(|message| message.value.len())
            .sum::<usize>();
        self.json
            .output_memory_bound(messages)
            .saturating_add(input.saturating_mul(2))
    }

    fn parse_into(
        &mut self,
        messages: Vec<Message>,
    ) -> anyhow::Result<(TableData, Option<TableData>)> {
        let schemas = self.registry_schemas(&messages)?;
        let mut normalized = Vec::with_capacity(messages.len());
        let mut changed = Vec::with_capacity(messages.len());
        let mut decoded_bytes = 0_usize;
        for message in messages {
            if message.tombstone {
                anyhow::ensure!(
                    message.value.is_empty(),
                    "Debezium tombstone carries a nonempty value"
                );
                anyhow::ensure!(
                    message.key.is_some(),
                    "Debezium tombstone must carry a message key"
                );
                continue;
            }
            let decoded = self.decode_value(&message, &schemas)?;
            let envelope = unwrap_payload(decoded)?;
            let (value, mask) = normalize_envelope(
                &envelope,
                &self.parser.user_paths,
                &self.parser.user_json_types,
                &self.parser.user_top_level_fields,
            )?;
            let value = serde_json::to_vec(&value)?;
            decoded_bytes = decoded_bytes
                .checked_add(value.len())
                .ok_or_else(|| anyhow::anyhow!("decoded Debezium delivery size overflow"))?;
            anyhow::ensure!(
                decoded_bytes <= self.memory_limit_bytes,
                "decoded Debezium delivery exceeds pipeline memory budget: decoded_bytes={decoded_bytes}, pipeline_memory_limit_bytes={}",
                self.memory_limit_bytes
            );
            normalized.push(Message {
                value: Bytes::from(value),
                tombstone: false,
                key: message.key,
                headers: message.headers,
                meta: message.meta,
            });
            changed.push(mask);
        }

        let (mut main, dlq) = self.json.parse_into(normalized)?;
        anyhow::ensure!(
            dlq.is_none(),
            "Debezium parser unexpectedly produced DLQ rows"
        );
        anyhow::ensure!(
            main.batch.num_rows() == changed.len(),
            "Debezium parser changed-mask count does not match decoded rows"
        );
        let mut arrays = main.batch.columns().to_vec();
        arrays.push(Arc::new(BinaryArray::from_iter_values(
            changed.iter().map(Vec::as_slice),
        )));

        let mut fields = self
            .parser
            .incoming_schema
            .columns
            .iter()
            .map(|column| {
                Arc::new(
                    Field::new(&column.name, column.data_type.clone(), column.nullable)
                        .with_metadata(column.arrow_metadata()),
                )
            })
            .collect::<Vec<_>>();
        let system_start = fields.len();
        fields.push(Arc::new(Field::new(
            SystemColumnKind::Offset.default_name(),
            DataType::Int64,
            false,
        )));
        fields.push(Arc::new(
            Field::new(
                SystemColumnKind::ChangeOperation.default_name(),
                DataType::Utf8,
                false,
            )
            .with_metadata(HashMap::from([(
                META_CHANGE_OPERATION.to_owned(),
                "true".to_owned(),
            )])),
        ));
        fields.push(Arc::new(Field::new(
            SystemColumnKind::ChangedColumns.default_name(),
            DataType::Binary,
            false,
        )));
        main.batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?;
        main.table = Arc::clone(&self.parser.table);
        main.system_columns = SystemColumns::new(vec![
            SystemColumn {
                kind: SystemColumnKind::Offset,
                index: system_start,
                name: Arc::from(SystemColumnKind::Offset.default_name()),
            },
            SystemColumn {
                kind: SystemColumnKind::ChangeOperation,
                index: system_start + 1,
                name: Arc::from(SystemColumnKind::ChangeOperation.default_name()),
            },
            SystemColumn {
                kind: SystemColumnKind::ChangedColumns,
                index: system_start + 2,
                name: Arc::from(SystemColumnKind::ChangedColumns.default_name()),
            },
        ]);
        Ok((main, None))
    }
}

fn normalize_envelope(
    value: &Value,
    paths: &[jsonpath_lib::Compiled],
    json_types: &[JsonDataType],
    top_level_fields: &[String],
) -> anyhow::Result<(Value, Vec<u8>)> {
    let envelope = value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("Debezium payload must be a JSON object"))?;
    let operation = required_string(envelope, "op")?;
    anyhow::ensure!(
        matches!(operation, "c" | "r" | "u" | "d"),
        "unsupported Debezium operation '{operation}'"
    );
    let before = envelope.get("before").cloned().unwrap_or(Value::Null);
    let after = envelope.get("after").cloned().unwrap_or(Value::Null);
    match operation {
        "c" | "r" => {
            anyhow::ensure!(
                before.is_null(),
                "Debezium '{operation}' before must be null"
            );
            anyhow::ensure!(
                after.is_object(),
                "Debezium '{operation}' after must be an object"
            );
        }
        "u" => {
            anyhow::ensure!(
                before.is_object(),
                "Debezium update before must be an object"
            );
            anyhow::ensure!(after.is_object(), "Debezium update after must be an object");
        }
        "d" => {
            anyhow::ensure!(
                before.is_object(),
                "Debezium delete before must be an object"
            );
            anyhow::ensure!(after.is_null(), "Debezium delete after must be null");
        }
        other => anyhow::bail!("unsupported Debezium operation '{other}'"),
    }

    let current_object = if operation == "d" { &before } else { &after };
    reject_unknown_user_fields(current_object, top_level_fields)?;
    if before.is_object() {
        reject_unknown_user_fields(&before, top_level_fields)?;
    }
    let mut current = Vec::with_capacity(paths.len());
    let mut old = Vec::with_capacity(paths.len());
    let mut unavailable = Vec::with_capacity(paths.len());
    for ((path, json_type), field) in paths.iter().zip(json_types).zip(top_level_fields) {
        let value = extract_one(path, current_object, field)?;
        let is_unavailable = contains_unavailable(&value);
        current.push(if is_unavailable {
            Value::Null
        } else {
            canonicalize_value(value, *json_type)?
        });
        let old_value = extract_one(path, &before, field)?;
        old.push(if contains_unavailable(&old_value) {
            Value::Null
        } else {
            canonicalize_value(old_value, *json_type)?
        });
        unavailable.push(is_unavailable);
    }
    if matches!(operation, "c" | "r") {
        anyhow::ensure!(
            unavailable.iter().all(|value| !value),
            "Debezium create/snapshot event contains an unavailable user value"
        );
    }
    if let Some(transaction) = envelope.get("transaction") {
        anyhow::ensure!(
            transaction.is_null(),
            "non-null Debezium transaction metadata is not supported"
        );
    }
    let mut normalized = Map::with_capacity(7);
    normalized.insert("current".to_owned(), Value::Array(current));
    normalized.insert("before".to_owned(), Value::Array(old));
    normalized.insert(
        "source".to_owned(),
        envelope
            .get("source")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Debezium payload source must be present"))?,
    );
    normalized.insert("op".to_owned(), Value::String(operation.to_owned()));
    for field in ["ts_ms", "ts_us", "ts_ns"] {
        normalized.insert(
            field.to_owned(),
            envelope
                .get(field)
                .or_else(|| envelope.get(protobuf_timestamp_name(field)))
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("Debezium payload is missing '{field}'"))?,
        );
    }
    canonicalize_metadata(&mut normalized)?;
    let mut changed = vec![0_u8; paths.len().div_ceil(8)];
    for (index, unavailable) in unavailable.iter().enumerate() {
        if !unavailable {
            changed[index / 8] |= 1 << (index % 8);
        }
    }
    Ok((Value::Object(normalized), changed))
}

fn protobuf_timestamp_name(field: &str) -> &str {
    match field {
        "ts_ms" => "tsMs",
        "ts_us" => "tsUs",
        "ts_ns" => "tsNs",
        _ => field,
    }
}

fn extract_one(
    path: &jsonpath_lib::Compiled,
    object: &Value,
    field: &str,
) -> anyhow::Result<Value> {
    if object.is_null() {
        return Ok(Value::Null);
    }
    let values = path
        .select(object)
        .map_err(|error| anyhow::anyhow!("Debezium column '{field}' JSONPath failed: {error}"))?;
    anyhow::ensure!(
        values.len() <= 1,
        "Debezium column '{field}' JSONPath selected multiple values"
    );
    Ok(values.first().map_or(Value::Null, |value| (*value).clone()))
}

fn reject_unknown_user_fields(value: &Value, known: &[String]) -> anyhow::Result<()> {
    let Some(object) = value.as_object() else {
        return Ok(());
    };
    for field in object.keys() {
        anyhow::ensure!(
            known.iter().any(|known| known == field),
            "Debezium user record contains unmapped field '{field}'"
        );
    }
    Ok(())
}

fn canonicalize_value(value: Value, json_type: JsonDataType) -> anyhow::Result<Value> {
    if json_type != JsonDataType::Number || !value.is_string() {
        return Ok(value);
    }
    let raw = value
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("numeric Debezium value is not a string"))?;
    if let Ok(value) = raw.parse::<i64>() {
        return Ok(Value::from(value));
    }
    if let Ok(value) = raw.parse::<u64>() {
        return Ok(Value::from(value));
    }
    let value = raw.parse::<f64>()?;
    anyhow::ensure!(value.is_finite(), "Debezium numeric value is not finite");
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .ok_or_else(|| anyhow::anyhow!("Debezium numeric value cannot be represented as JSON"))
}

fn canonicalize_metadata(envelope: &mut Map<String, Value>) -> anyhow::Result<()> {
    for (field, protobuf_json_name) in [("ts_ms", "tsMs"), ("ts_us", "tsUs"), ("ts_ns", "tsNs")] {
        canonicalize_object_number(envelope, field, protobuf_json_name)?;
    }
    let source = envelope
        .get_mut("source")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| anyhow::anyhow!("Debezium payload source must be an object"))?;
    for (field, protobuf_json_name) in [
        ("txId", "txId"),
        ("lsn", "lsn"),
        ("ts_ms", "tsMs"),
        ("ts_us", "tsUs"),
        ("ts_ns", "tsNs"),
    ] {
        canonicalize_object_number(source, field, protobuf_json_name)?;
    }
    Ok(())
}

fn canonicalize_object_number(
    object: &mut Map<String, Value>,
    field: &str,
    protobuf_json_name: &str,
) -> anyhow::Result<()> {
    if field != protobuf_json_name {
        match (
            object.contains_key(field),
            object.remove(protobuf_json_name),
        ) {
            (false, Some(value)) => {
                object.insert(field.to_owned(), value);
            }
            (true, Some(_)) => {
                anyhow::bail!(
                    "Debezium payload contains both '{field}' and '{protobuf_json_name}'"
                );
            }
            (_, None) => {}
        }
    }
    let value = object
        .get_mut(field)
        .ok_or_else(|| anyhow::anyhow!("Debezium payload is missing numeric field '{field}'"))?;
    *value = canonicalize_value(std::mem::take(value), JsonDataType::Number)?;
    Ok(())
}

fn unwrap_payload(value: Value) -> anyhow::Result<Value> {
    let Some(object) = value.as_object() else {
        anyhow::bail!("Debezium message must be a JSON object");
    };
    if !object.contains_key("payload") {
        return Ok(value);
    }
    anyhow::ensure!(
        object
            .keys()
            .all(|key| matches!(key.as_str(), "schema" | "payload")),
        "schemaful Debezium wrapper contains unsupported fields"
    );
    object
        .get("payload")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("schemaful Debezium wrapper has no payload"))
}

fn required_string<'a>(object: &'a Map<String, Value>, key: &str) -> anyhow::Result<&'a str> {
    object
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("Debezium payload field '{key}' must be a string"))
}

fn contains_unavailable(value: &Value) -> bool {
    match value {
        Value::String(value) => value == UNAVAILABLE_VALUE,
        Value::Array(values) => values.iter().any(contains_unavailable),
        Value::Object(values) => values.values().any(contains_unavailable),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

fn primitive_mapping(
    name: &str,
    path: &str,
    json_data_type: JsonDataType,
    arrow_type: &str,
) -> ColumnMapping {
    ColumnMapping {
        jsonpath: path.to_owned(),
        column_name: name.to_owned(),
        json_data_type,
        arrow_type: arrow_type.to_owned(),
        decimal_precision: None,
        decimal_scale: None,
        nullable: false,
        time_conversion: None,
        low_cardinality: false,
        max_length: None,
    }
}

fn top_level_field(path: &str) -> anyhow::Result<&str> {
    let path = path.strip_prefix("$.").ok_or_else(|| {
        anyhow::anyhow!("Debezium JSONPath '{path}' must begin with a named top-level field")
    })?;
    let end = path.find(['.', '[']).unwrap_or(path.len());
    anyhow::ensure!(end > 0, "Debezium JSONPath '{path}' has no field name");
    Ok(&path[..end])
}

fn old_value_column_name(index: usize) -> String {
    format!("_system_old_value_{index}")
}

#[cfg(test)]
mod tests;
