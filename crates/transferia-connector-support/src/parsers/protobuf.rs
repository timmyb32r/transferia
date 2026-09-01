use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder,
    Int64Builder, StringBuilder, UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use base64::Engine as _;
use prost::Message as _;
use prost_reflect::{
    DescriptorPool, DynamicMessage, FieldDescriptor, Kind, MapKey, ReflectMessage, Value,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use transferia_core::data::message::{Message, MessageMeta};
use transferia_core::data::schema::{DatasetSchema, SchemaColumn, ARROW_JSON_EXTENSION_NAME};
use transferia_core::data::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use transferia_core::data::table_data::{dlq_name, TableData};

use crate::parsers::{ParserFactory, ParserSession, SystemColumnsConfig};

const PROTOSEQ_MAGIC: [u8; 32] = [
    31, 247, 247, 126, 190, 166, 94, 158, 55, 166, 246, 46, 254, 174, 71, 167, 183, 110, 191, 175,
    22, 158, 159, 55, 246, 87, 247, 102, 167, 6, 175, 247,
];
const PROTOSEQ_MAX_RECORD_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Deserialize, JsonSchema, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum DescriptorSource {
    #[schemars(title = "Inline base64")]
    InlineBase64 {
        #[schemars(title = "FileDescriptorSet (base64)", extend("x-ui" = { "widget": "textarea" }))]
        value: String,
    },

    #[schemars(title = "File")]
    File {
        #[schemars(title = "FileDescriptorSet path")]
        path: PathBuf,
    },
}

#[derive(Debug, Clone, Copy, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtobufPackageType {
    #[default]
    #[schemars(title = "Single message")]
    SingleMessage,

    #[schemars(title = "Repeated message wrapper")]
    RepeatedMessage,

    #[schemars(title = "Protoseq")]
    Protoseq,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtobufUnknownFieldPolicy {
    #[default]
    #[schemars(title = "Fail")]
    Fail,

    #[schemars(title = "Discard unknown fields")]
    Discard,
}

#[derive(Debug, Clone, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProtobufColumn {
    #[schemars(title = "Field")]
    pub name: String,

    #[serde(default)]
    #[schemars(title = "Required")]
    pub required: bool,
}

#[derive(Debug, Clone, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProtobufParserConfig {
    #[schemars(title = "Descriptor")]
    pub descriptor: DescriptorSource,

    #[schemars(title = "Message name")]
    pub message_name: String,

    #[serde(default)]
    #[schemars(title = "Package type")]
    pub package_type: ProtobufPackageType,

    #[serde(default)]
    #[schemars(title = "Columns", description = "Empty means every top-level field")]
    pub include_columns: Vec<ProtobufColumn>,

    #[serde(default)]
    #[schemars(title = "Primary key")]
    pub primary_key: Vec<String>,

    #[serde(default)]
    #[schemars(title = "Allow null primary keys")]
    pub null_keys_allowed: bool,

    #[serde(default)]
    #[schemars(title = "Leave absent fields null")]
    pub not_fill_empty_fields: bool,

    #[serde(default)]
    #[schemars(title = "On unknown protobuf field")]
    pub unknown_fields: ProtobufUnknownFieldPolicy,
}

pub struct ProtobufParser {
    root_descriptor: prost_reflect::MessageDescriptor,
    row_descriptor: prost_reflect::MessageDescriptor,
    columns: Vec<CompiledColumn>,
    dataset_schema: DatasetSchema,
    table: Arc<str>,
    arrow_schema: Arc<Schema>,
    dlq_schema: Arc<Schema>,
    system_config: SystemColumnsConfig,
    system_kinds: Arc<[SystemColumnKind]>,
    system_columns: SystemColumns,
    dlq_system_columns: SystemColumns,
    package_type: ProtobufPackageType,
    not_fill_empty_fields: bool,
    unknown_fields: ProtobufUnknownFieldPolicy,
}

struct CompiledColumn {
    field: FieldDescriptor,
    data_type: DataType,
    nullable: bool,
}

impl DescriptorSource {
    fn read(&self) -> anyhow::Result<Vec<u8>> {
        match self {
            Self::InlineBase64 { value } => {
                anyhow::ensure!(
                    !value.is_empty(),
                    "protobuf descriptor base64 must not be empty"
                );
                base64::engine::general_purpose::STANDARD
                    .decode(value)
                    .map_err(Into::into)
            }
            Self::File { path } => {
                anyhow::ensure!(
                    !path.as_os_str().is_empty(),
                    "protobuf descriptor path must not be empty"
                );
                std::fs::read(path).map_err(Into::into)
            }
        }
    }
}

impl ProtobufParser {
    pub fn new(
        config: &ProtobufParserConfig,
        system_config: &SystemColumnsConfig,
        table: Arc<str>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(!table.is_empty(), "protobuf table name must not be empty");
        anyhow::ensure!(
            !config.message_name.trim().is_empty(),
            "protobuf.message_name must not be empty"
        );
        system_config.validate()?;
        let bytes = config.descriptor.read()?;
        let pool = DescriptorPool::decode(bytes.as_slice())?;
        let root_descriptor = resolve_message(&pool, &config.message_name)?;
        let row_descriptor = match config.package_type {
            ProtobufPackageType::RepeatedMessage => repeated_item_descriptor(&root_descriptor)?,
            ProtobufPackageType::SingleMessage | ProtobufPackageType::Protoseq => {
                root_descriptor.clone()
            }
        };
        let (columns, dataset_schema) = compile_columns(config, &row_descriptor)?;
        let system_kinds = system_config.enabled().collect::<Vec<_>>();
        let arrow_schema = Arc::new(main_arrow_schema(
            &dataset_schema,
            system_config,
            &system_kinds,
        ));
        let dlq_schema = Arc::new(dlq_arrow_schema(system_config, &system_kinds));
        let main_system_columns =
            system_columns(system_config, &system_kinds, dataset_schema.columns.len());
        let dlq_system_columns = system_columns(system_config, &system_kinds, 3);
        Ok(Self {
            root_descriptor,
            row_descriptor,
            columns,
            dataset_schema,
            table,
            arrow_schema,
            dlq_schema,
            system_config: system_config.clone(),
            system_kinds: system_kinds.into(),
            system_columns: main_system_columns,
            dlq_system_columns,
            package_type: config.package_type,
            not_fill_empty_fields: config.not_fill_empty_fields,
            unknown_fields: config.unknown_fields,
        })
    }

    #[must_use]
    pub const fn dataset_schema(&self) -> &DatasetSchema {
        &self.dataset_schema
    }

    fn decode_source(&self, raw: &[u8]) -> Vec<DecodeResult> {
        match self.package_type {
            ProtobufPackageType::SingleMessage => {
                vec![decode_one(&self.row_descriptor, raw)]
            }
            ProtobufPackageType::RepeatedMessage => self.decode_repeated(raw),
            ProtobufPackageType::Protoseq => decode_protoseq(&self.row_descriptor, raw),
        }
    }

    fn decode_repeated(&self, raw: &[u8]) -> Vec<DecodeResult> {
        let wrapper = match DynamicMessage::decode(self.root_descriptor.clone(), raw) {
            Ok(message) => message,
            Err(error) => {
                return vec![Err((
                    raw.to_vec(),
                    format!("invalid protobuf wrapper: {error}"),
                ))]
            }
        };
        let Some(field) = self.root_descriptor.fields().next() else {
            return vec![Err((
                raw.to_vec(),
                "protobuf repeated wrapper descriptor has no fields".to_owned(),
            ))];
        };
        let Value::List(items) = wrapper.get_field(&field).into_owned() else {
            return vec![Err((
                raw.to_vec(),
                "protobuf wrapper field is not a repeated list".to_owned(),
            ))];
        };
        items
            .into_iter()
            .map(|item| match item {
                Value::Message(message) => {
                    let bytes = message.encode_to_vec();
                    Ok((message, bytes))
                }
                _ => Err((
                    Vec::new(),
                    "protobuf wrapper contains a non-message item".to_owned(),
                )),
            })
            .collect()
    }
}

fn resolve_message(
    pool: &DescriptorPool,
    configured: &str,
) -> anyhow::Result<prost_reflect::MessageDescriptor> {
    if let Some(message) = pool.get_message_by_name(configured) {
        return Ok(message);
    }
    let matches = pool
        .all_messages()
        .filter(|message| message.name() == configured)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [message] => Ok(message.clone()),
        [] => anyhow::bail!("protobuf message '{configured}' is absent from descriptor set"),
        _ => anyhow::bail!(
            "protobuf message name '{configured}' is ambiguous; use a fully-qualified name"
        ),
    }
}

fn repeated_item_descriptor(
    wrapper: &prost_reflect::MessageDescriptor,
) -> anyhow::Result<prost_reflect::MessageDescriptor> {
    let fields = wrapper.fields().collect::<Vec<_>>();
    anyhow::ensure!(
        fields.len() == 1 && fields[0].is_list(),
        "protobuf repeated-message wrapper must contain exactly one repeated field"
    );
    match fields[0].kind() {
        Kind::Message(message) => Ok(message),
        other => anyhow::bail!(
            "protobuf repeated-message wrapper field must contain messages, got {other:?}"
        ),
    }
}

fn compile_columns(
    config: &ProtobufParserConfig,
    descriptor: &prost_reflect::MessageDescriptor,
) -> anyhow::Result<(Vec<CompiledColumn>, DatasetSchema)> {
    let fields = descriptor
        .fields()
        .map(|field| (field.name().to_owned(), field))
        .collect::<HashMap<_, _>>();
    let mut keys = HashSet::with_capacity(config.primary_key.len());
    for key in &config.primary_key {
        anyhow::ensure!(
            !key.is_empty(),
            "protobuf primary-key names must not be empty"
        );
        anyhow::ensure!(
            keys.insert(key.as_str()),
            "protobuf.primary_key repeats field '{key}'"
        );
        anyhow::ensure!(
            fields.contains_key(key),
            "protobuf primary-key field '{key}' is absent from message '{}'",
            descriptor.full_name()
        );
    }

    let mut selected = Vec::<(FieldDescriptor, bool)>::new();
    let mut selected_names = HashSet::new();
    for key in &config.primary_key {
        selected.push((fields[key].clone(), !config.null_keys_allowed));
        selected_names.insert(key.as_str());
    }
    if config.include_columns.is_empty() {
        selected.extend(
            descriptor
                .fields()
                .filter(|field| !selected_names.contains(field.name()))
                .map(|field| (field, false)),
        );
    } else {
        let mut includes = HashSet::with_capacity(config.include_columns.len());
        for column in &config.include_columns {
            anyhow::ensure!(
                !column.name.is_empty(),
                "protobuf column names must not be empty"
            );
            anyhow::ensure!(
                includes.insert(column.name.as_str()),
                "protobuf.include_columns repeats field '{}'",
                column.name
            );
            let field = fields.get(&column.name).ok_or_else(|| {
                anyhow::anyhow!(
                    "protobuf included field '{}' is absent from message '{}'",
                    column.name,
                    descriptor.full_name()
                )
            })?;
            if keys.contains(column.name.as_str()) {
                anyhow::ensure!(
                    config.null_keys_allowed || column.required,
                    "protobuf primary-key field '{}' must be required unless null_keys_allowed=true",
                    column.name
                );
                continue;
            }
            selected.push((field.clone(), column.required));
        }
    }

    anyhow::ensure!(
        !selected.is_empty(),
        "protobuf output schema must not be empty"
    );
    let mut compiled = Vec::with_capacity(selected.len());
    let mut schema = Vec::with_capacity(selected.len());
    for (field, required) in selected {
        let data_type = arrow_type(&field);
        let nullable = !required;
        let mut column = SchemaColumn::new(field.name().to_owned(), data_type.clone(), nullable)
            .with_constraints(keys.contains(field.name()), false, None);
        if is_json_field(&field) {
            column = column.with_arrow_extension(ARROW_JSON_EXTENSION_NAME);
        }
        schema.push(column);
        compiled.push(CompiledColumn {
            field,
            data_type,
            nullable,
        });
    }
    Ok((compiled, DatasetSchema::new(schema)))
}

fn arrow_type(field: &FieldDescriptor) -> DataType {
    if is_json_field(field) {
        return DataType::Utf8;
    }
    match field.kind() {
        Kind::Bool => DataType::Boolean,
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 | Kind::Enum(_) => DataType::Int32,
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => DataType::Int64,
        Kind::Uint32 | Kind::Fixed32 => DataType::UInt32,
        Kind::Uint64 | Kind::Fixed64 => DataType::UInt64,
        Kind::Float => DataType::Float32,
        Kind::Double => DataType::Float64,
        Kind::String | Kind::Message(_) => DataType::Utf8,
        Kind::Bytes => DataType::Binary,
    }
}

fn is_json_field(field: &FieldDescriptor) -> bool {
    field.is_list() || field.is_map() || matches!(field.kind(), Kind::Message(_))
}

impl ParserFactory for ProtobufParser {
    fn create_session(self: Arc<Self>, memory_limit_bytes: usize) -> Box<dyn ParserSession> {
        Box::new(ProtobufParserSession {
            parser: self,
            memory_limit_bytes,
        })
    }
}

struct ProtobufParserSession {
    parser: Arc<ProtobufParser>,
    memory_limit_bytes: usize,
}

impl ParserSession for ProtobufParserSession {
    fn output_memory_bound(&self, messages: &[Message]) -> usize {
        messages.iter().fold(0_usize, |total, message| {
            total
                .saturating_add(message.value.len().saturating_mul(64))
                .saturating_add(self.parser.columns.len().saturating_mul(64))
                .saturating_add(1024)
        })
    }

    fn parse_into(
        &mut self,
        messages: Vec<Message>,
    ) -> anyhow::Result<(TableData, Option<TableData>)> {
        let memory_bound = self.output_memory_bound(&messages);
        anyhow::ensure!(
            memory_bound <= self.memory_limit_bytes,
            "protobuf output memory bound {memory_bound} exceeds pipeline memory limit {}",
            self.memory_limit_bytes
        );
        parse_messages(&self.parser, &messages)
    }
}

struct ParsedRow {
    values: Vec<PreparedValue>,
    source_index: usize,
    message_index: u64,
}

struct RejectedRow {
    raw: Vec<u8>,
    error: String,
    source_index: usize,
    message_index: u64,
}

fn parse_messages(
    parser: &ProtobufParser,
    messages: &[Message],
) -> anyhow::Result<(TableData, Option<TableData>)> {
    let mut parsed = Vec::new();
    let mut rejected = Vec::new();
    for (source_index, source) in messages.iter().enumerate() {
        for (message_index, row) in parser
            .decode_source(source.value.as_ref())
            .into_iter()
            .enumerate()
        {
            let message_index = u64::try_from(message_index)?;
            match row {
                Ok((message, raw)) => match parser.prepare_row(&message) {
                    Ok(values) => parsed.push(ParsedRow {
                        values,
                        source_index,
                        message_index,
                    }),
                    Err(error) => rejected.push(RejectedRow {
                        raw,
                        error: error.to_string(),
                        source_index,
                        message_index,
                    }),
                },
                Err((raw, error)) => rejected.push(RejectedRow {
                    raw,
                    error,
                    source_index,
                    message_index,
                }),
            }
        }
    }

    let mut builders = parser
        .columns
        .iter()
        .map(|column| ColumnBuilder::new(&column.data_type, parsed.len()))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let data_columns = parser.columns.len();
    let mut systems = SystemBuilders::new(&parser.system_kinds, parsed.len(), data_columns);
    for row in &parsed {
        for (builder, value) in builders.iter_mut().zip(&row.values) {
            builder.append(value)?;
        }
        systems.append(
            &parser.system_config,
            &parser.system_kinds,
            &messages[row.source_index].meta,
            row.message_index,
            data_columns,
        )?;
    }
    let mut arrays = builders
        .into_iter()
        .map(ColumnBuilder::finish)
        .collect::<Vec<_>>();
    arrays.extend(systems.finish());
    let main = TableData::new(
        Arc::clone(&parser.table),
        false,
        RecordBatch::try_new(Arc::clone(&parser.arrow_schema), arrays)?,
        parser.system_columns.clone(),
    );
    let dlq = (!rejected.is_empty())
        .then(|| build_dlq(parser, messages, &rejected))
        .transpose()?;
    Ok((main, dlq))
}

type DecodeResult = Result<(DynamicMessage, Vec<u8>), (Vec<u8>, String)>;

impl ProtobufParser {
    fn prepare_row(&self, message: &DynamicMessage) -> anyhow::Result<Vec<PreparedValue>> {
        if matches!(self.unknown_fields, ProtobufUnknownFieldPolicy::Fail) {
            ensure_no_unknown_fields(message)?;
        }
        self.columns
            .iter()
            .map(|column| {
                let present = message.has_field(&column.field);
                anyhow::ensure!(
                    !column.field.supports_presence() || present || column.nullable,
                    "required protobuf field '{}' is absent",
                    column.field.name()
                );
                if !present && self.not_fill_empty_fields {
                    return Ok(PreparedValue::Null);
                }
                PreparedValue::from_reflect(
                    message.get_field(&column.field).as_ref(),
                    &column.field,
                )
            })
            .collect()
    }
}

fn decode_one(descriptor: &prost_reflect::MessageDescriptor, raw: &[u8]) -> DecodeResult {
    DynamicMessage::decode(descriptor.clone(), raw)
        .map(|message| (message, raw.to_vec()))
        .map_err(|error| (raw.to_vec(), format!("invalid protobuf message: {error}")))
}

fn decode_protoseq(
    descriptor: &prost_reflect::MessageDescriptor,
    input: &[u8],
) -> Vec<DecodeResult> {
    let mut rows = Vec::new();
    let mut remaining = input;
    while !remaining.is_empty() {
        if remaining.len() < 4 {
            rows.push(Err((
                remaining.to_vec(),
                format!(
                    "incomplete protoseq frame: expected 4-byte size, got {} bytes",
                    remaining.len()
                ),
            )));
            break;
        }
        let size =
            u32::from_le_bytes([remaining[0], remaining[1], remaining[2], remaining[3]]) as usize;
        let framed = &remaining[4..];
        let frame_end = size.saturating_add(PROTOSEQ_MAGIC.len());
        if size <= PROTOSEQ_MAX_RECORD_BYTES
            && framed.len() >= frame_end
            && framed[size..frame_end] == PROTOSEQ_MAGIC
        {
            rows.push(decode_one(descriptor, &framed[..size]));
            remaining = &framed[frame_end..];
            continue;
        }

        if let Some(sync) = find_subslice(framed, &PROTOSEQ_MAGIC) {
            rows.push(Err((
                remaining[..4 + sync].to_vec(),
                "corrupted protoseq frame".to_owned(),
            )));
            remaining = &framed[sync + PROTOSEQ_MAGIC.len()..];
        } else {
            rows.push(Err((
                remaining.to_vec(),
                "corrupted protoseq frame has no following synchronization marker".to_owned(),
            )));
            break;
        }
    }
    rows
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn ensure_no_unknown_fields(message: &DynamicMessage) -> anyhow::Result<()> {
    anyhow::ensure!(
        message.unknown_fields().next().is_none(),
        "protobuf message contains fields absent from the configured descriptor"
    );
    for field in message.descriptor().fields() {
        if message.has_field(&field) {
            ensure_value_has_no_unknowns(message.get_field(&field).as_ref())?;
        }
    }
    Ok(())
}

fn ensure_value_has_no_unknowns(value: &Value) -> anyhow::Result<()> {
    match value {
        Value::Message(message) => ensure_no_unknown_fields(message),
        Value::List(values) => values.iter().try_for_each(ensure_value_has_no_unknowns),
        Value::Map(values) => values.values().try_for_each(ensure_value_has_no_unknowns),
        _ => Ok(()),
    }
}

enum PreparedValue {
    Null,
    Bool(bool),
    I32(i32),
    I64(i64),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    String(String),
    Bytes(bytes::Bytes),
}

impl PreparedValue {
    fn from_reflect(value: &Value, field: &FieldDescriptor) -> anyhow::Result<Self> {
        if is_json_field(field) {
            return Ok(Self::String(serde_json::to_string(&reflect_json(value)?)?));
        }
        Ok(match value {
            Value::Bool(value) => Self::Bool(*value),
            Value::I32(value) | Value::EnumNumber(value) => Self::I32(*value),
            Value::I64(value) => Self::I64(*value),
            Value::U32(value) => Self::U32(*value),
            Value::U64(value) => Self::U64(*value),
            Value::F32(value) => Self::F32(*value),
            Value::F64(value) => Self::F64(*value),
            Value::String(value) => Self::String(value.clone()),
            Value::Bytes(value) => Self::Bytes(value.clone()),
            Value::Message(_) | Value::List(_) | Value::Map(_) => {
                anyhow::bail!(
                    "protobuf field '{}' has an unexpected value kind",
                    field.name()
                )
            }
        })
    }
}

fn reflect_json(value: &Value) -> anyhow::Result<serde_json::Value> {
    Ok(match value {
        Value::Bool(value) => serde_json::Value::Bool(*value),
        Value::I32(value) | Value::EnumNumber(value) => (*value).into(),
        Value::I64(value) => (*value).into(),
        Value::U32(value) => (*value).into(),
        Value::U64(value) => (*value).into(),
        Value::F32(value) => float_json(f64::from(*value)),
        Value::F64(value) => float_json(*value),
        Value::String(value) => value.clone().into(),
        Value::Bytes(value) => base64::engine::general_purpose::STANDARD
            .encode(value)
            .into(),
        Value::Message(value) => serde_json::to_value(value)?,
        Value::List(values) => serde_json::Value::Array(
            values
                .iter()
                .map(reflect_json)
                .collect::<anyhow::Result<Vec<_>>>()?,
        ),
        Value::Map(values) => serde_json::Value::Object(
            values
                .iter()
                .map(|(key, value)| Ok((map_key_string(key), reflect_json(value)?)))
                .collect::<anyhow::Result<serde_json::Map<_, _>>>()?,
        ),
    })
}

fn float_json(value: f64) -> serde_json::Value {
    if value.is_finite() {
        serde_json::Number::from_f64(value)
            .map_or(serde_json::Value::Null, serde_json::Value::Number)
    } else if value.is_nan() {
        serde_json::Value::String("NaN".to_owned())
    } else if value.is_sign_positive() {
        serde_json::Value::String("Infinity".to_owned())
    } else {
        serde_json::Value::String("-Infinity".to_owned())
    }
}

fn map_key_string(key: &MapKey) -> String {
    match key {
        MapKey::Bool(value) => value.to_string(),
        MapKey::I32(value) => value.to_string(),
        MapKey::I64(value) => value.to_string(),
        MapKey::U32(value) => value.to_string(),
        MapKey::U64(value) => value.to_string(),
        MapKey::String(value) => value.clone(),
    }
}

enum ColumnBuilder {
    Boolean(BooleanBuilder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    UInt32(UInt32Builder),
    UInt64(UInt64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    String(StringBuilder),
    Binary(BinaryBuilder),
}

impl ColumnBuilder {
    fn new(data_type: &DataType, rows: usize) -> anyhow::Result<Self> {
        Ok(match data_type {
            DataType::Boolean => Self::Boolean(BooleanBuilder::with_capacity(rows)),
            DataType::Int32 => Self::Int32(Int32Builder::with_capacity(rows)),
            DataType::Int64 => Self::Int64(Int64Builder::with_capacity(rows)),
            DataType::UInt32 => Self::UInt32(UInt32Builder::with_capacity(rows)),
            DataType::UInt64 => Self::UInt64(UInt64Builder::with_capacity(rows)),
            DataType::Float32 => Self::Float32(Float32Builder::with_capacity(rows)),
            DataType::Float64 => Self::Float64(Float64Builder::with_capacity(rows)),
            DataType::Utf8 => Self::String(StringBuilder::with_capacity(rows, rows * 32)),
            DataType::Binary => Self::Binary(BinaryBuilder::with_capacity(rows, rows * 32)),
            other => anyhow::bail!("unsupported compiled protobuf Arrow type {other:?}"),
        })
    }

    fn append(&mut self, value: &PreparedValue) -> anyhow::Result<()> {
        match (self, value) {
            (Self::Boolean(builder), PreparedValue::Bool(value)) => builder.append_value(*value),
            (Self::Int32(builder), PreparedValue::I32(value)) => builder.append_value(*value),
            (Self::Int64(builder), PreparedValue::I64(value)) => builder.append_value(*value),
            (Self::UInt32(builder), PreparedValue::U32(value)) => builder.append_value(*value),
            (Self::UInt64(builder), PreparedValue::U64(value)) => builder.append_value(*value),
            (Self::Float32(builder), PreparedValue::F32(value)) => builder.append_value(*value),
            (Self::Float64(builder), PreparedValue::F64(value)) => builder.append_value(*value),
            (Self::String(builder), PreparedValue::String(value)) => builder.append_value(value),
            (Self::Binary(builder), PreparedValue::Bytes(value)) => builder.append_value(value),
            (Self::Boolean(builder), PreparedValue::Null) => builder.append_null(),
            (Self::Int32(builder), PreparedValue::Null) => builder.append_null(),
            (Self::Int64(builder), PreparedValue::Null) => builder.append_null(),
            (Self::UInt32(builder), PreparedValue::Null) => builder.append_null(),
            (Self::UInt64(builder), PreparedValue::Null) => builder.append_null(),
            (Self::Float32(builder), PreparedValue::Null) => builder.append_null(),
            (Self::Float64(builder), PreparedValue::Null) => builder.append_null(),
            (Self::String(builder), PreparedValue::Null) => builder.append_null(),
            (Self::Binary(builder), PreparedValue::Null) => builder.append_null(),
            _ => anyhow::bail!("protobuf value type does not match its compiled Arrow column"),
        }
        Ok(())
    }

    fn finish(mut self) -> ArrayRef {
        match &mut self {
            Self::Boolean(builder) => Arc::new(builder.finish()),
            Self::Int32(builder) => Arc::new(builder.finish()),
            Self::Int64(builder) => Arc::new(builder.finish()),
            Self::UInt32(builder) => Arc::new(builder.finish()),
            Self::UInt64(builder) => Arc::new(builder.finish()),
            Self::Float32(builder) => Arc::new(builder.finish()),
            Self::Float64(builder) => Arc::new(builder.finish()),
            Self::String(builder) => Arc::new(builder.finish()),
            Self::Binary(builder) => Arc::new(builder.finish()),
        }
    }
}

enum SystemBuilder {
    Binary(BinaryBuilder),
    String(StringBuilder),
    Int64(Int64Builder),
    UInt64(UInt64Builder),
}

struct SystemBuilders(Vec<SystemBuilder>);

impl SystemBuilders {
    fn new(kinds: &[SystemColumnKind], rows: usize, data_columns: usize) -> Self {
        Self(
            kinds
                .iter()
                .map(|kind| match kind {
                    SystemColumnKind::Topic | SystemColumnKind::ChangeOperation => {
                        SystemBuilder::String(StringBuilder::with_capacity(rows, rows * 32))
                    }
                    SystemColumnKind::Partition
                    | SystemColumnKind::Offset
                    | SystemColumnKind::WriteTimestampMs => {
                        SystemBuilder::Int64(Int64Builder::with_capacity(rows))
                    }
                    SystemColumnKind::MessageIndex => {
                        SystemBuilder::UInt64(UInt64Builder::with_capacity(rows))
                    }
                    SystemColumnKind::ChangedColumns => SystemBuilder::Binary(
                        BinaryBuilder::with_capacity(rows, rows.saturating_mul(data_columns.div_ceil(8))),
                    ),
                })
                .collect(),
        )
    }

    fn append(
        &mut self,
        config: &SystemColumnsConfig,
        kinds: &[SystemColumnKind],
        meta: &MessageMeta,
        message_index: u64,
        data_columns: usize,
    ) -> anyhow::Result<()> {
        for (kind, builder) in kinds.iter().zip(&mut self.0) {
            let missing = || {
                anyhow::anyhow!(
                    "source message is missing metadata required for system column '{}'",
                    config.name(*kind)
                )
            };
            match (kind, builder) {
                (SystemColumnKind::Topic, SystemBuilder::String(builder)) => {
                    builder.append_value(meta.topic.as_deref().ok_or_else(missing)?);
                }
                (SystemColumnKind::Partition, SystemBuilder::Int64(builder)) => {
                    builder.append_value(meta.partition.ok_or_else(missing)?);
                }
                (SystemColumnKind::Offset, SystemBuilder::Int64(builder)) => {
                    builder.append_value(meta.offset.ok_or_else(missing)?);
                }
                (SystemColumnKind::MessageIndex, SystemBuilder::UInt64(builder)) => {
                    builder.append_value(message_index);
                }
                (SystemColumnKind::WriteTimestampMs, SystemBuilder::Int64(builder)) => {
                    builder.append_value(meta.write_timestamp_ms.ok_or_else(missing)?);
                }
                (SystemColumnKind::ChangeOperation, SystemBuilder::String(builder)) => {
                    builder.append_value("c");
                }
                (SystemColumnKind::ChangedColumns, SystemBuilder::Binary(builder)) => {
                    let mut mask = vec![u8::MAX; data_columns.div_ceil(8)];
                    if let Some(last) = mask.last_mut() {
                        let used_bits = data_columns % 8;
                        if used_bits != 0 {
                            *last = (1_u8 << used_bits) - 1;
                        }
                    }
                    builder.append_value(&mask);
                }
                _ => anyhow::bail!("protobuf system-column builder type mismatch"),
            }
        }
        Ok(())
    }

    fn finish(self) -> Vec<ArrayRef> {
        self.0
            .into_iter()
            .map(|mut builder| match &mut builder {
                SystemBuilder::Binary(builder) => Arc::new(builder.finish()) as ArrayRef,
                SystemBuilder::String(builder) => Arc::new(builder.finish()) as ArrayRef,
                SystemBuilder::Int64(builder) => Arc::new(builder.finish()) as ArrayRef,
                SystemBuilder::UInt64(builder) => Arc::new(builder.finish()) as ArrayRef,
            })
            .collect()
    }
}

fn build_dlq(
    parser: &ProtobufParser,
    messages: &[Message],
    rows: &[RejectedRow],
) -> anyhow::Result<TableData> {
    let mut raw = StringBuilder::with_capacity(rows.len(), rows.len() * 128);
    let mut error = StringBuilder::with_capacity(rows.len(), rows.len() * 128);
    let mut source_time = Int64Builder::with_capacity(rows.len());
    let data_columns = parser.columns.len();
    let mut systems = SystemBuilders::new(&parser.system_kinds, rows.len(), data_columns);
    for row in rows {
        raw.append_value(base64::engine::general_purpose::STANDARD.encode(&row.raw));
        error.append_value(&row.error);
        source_time.append_option(messages[row.source_index].meta.write_timestamp_ms);
        systems.append(
            &parser.system_config,
            &parser.system_kinds,
            &messages[row.source_index].meta,
            row.message_index,
            data_columns,
        )?;
    }
    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(raw.finish()),
        Arc::new(error.finish()),
        Arc::new(source_time.finish()),
    ];
    arrays.extend(systems.finish());
    Ok(TableData::new(
        dlq_name(&parser.table).into(),
        false,
        RecordBatch::try_new(Arc::clone(&parser.dlq_schema), arrays)?,
        parser.dlq_system_columns.clone(),
    ))
}

fn main_arrow_schema(
    dataset: &DatasetSchema,
    system_config: &SystemColumnsConfig,
    system_kinds: &[SystemColumnKind],
) -> Schema {
    let mut fields = dataset
        .columns
        .iter()
        .map(|column| {
            Field::new(&column.name, column.data_type.clone(), column.nullable)
                .with_metadata(column.arrow_metadata())
        })
        .collect::<Vec<_>>();
    fields.extend(
        system_kinds
            .iter()
            .map(|kind| Field::new(system_config.name(*kind), kind.data_type(), false)),
    );
    Schema::new(fields)
}

fn dlq_arrow_schema(
    system_config: &SystemColumnsConfig,
    system_kinds: &[SystemColumnKind],
) -> Schema {
    let mut fields = vec![
        Field::new("raw_base64", DataType::Utf8, false),
        Field::new("error_message", DataType::Utf8, false),
        Field::new("source_write_timestamp_ms", DataType::Int64, true),
    ];
    fields.extend(
        system_kinds
            .iter()
            .map(|kind| Field::new(system_config.name(*kind), kind.data_type(), false)),
    );
    Schema::new(fields)
}

fn system_columns(
    config: &SystemColumnsConfig,
    kinds: &[SystemColumnKind],
    offset: usize,
) -> SystemColumns {
    SystemColumns::new(
        kinds
            .iter()
            .enumerate()
            .map(|(index, kind)| SystemColumn {
                kind: *kind,
                index: offset + index,
                name: Arc::from(config.name(*kind)),
            })
            .collect::<Vec<_>>(),
    )
}
