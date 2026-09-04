use apache_avro::Schema;
use prost::Message as _;
use prost_reflect::{DynamicMessage, MessageDescriptor};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::schema_registry::{
    encode_message_indexes, ConfluentEnvelope, RegistryClient, RegistrySchema, SchemaFormat,
    SchemaRegistryConnection,
};
use transferia_core::data::schema::{
    SYSTEM_ROLE_EVENT_TIMESTAMP_MS, SYSTEM_ROLE_EVENT_TIMESTAMP_NS, SYSTEM_ROLE_EVENT_TIMESTAMP_US,
    SYSTEM_ROLE_SOURCE_BINLOG_FILE, SYSTEM_ROLE_SOURCE_BINLOG_POSITION,
    SYSTEM_ROLE_SOURCE_BINLOG_ROW, SYSTEM_ROLE_SOURCE_DATABASE, SYSTEM_ROLE_SOURCE_GTID,
    SYSTEM_ROLE_SOURCE_SCHEMA, SYSTEM_ROLE_SOURCE_SERVER_ID, SYSTEM_ROLE_SOURCE_TABLE,
    SYSTEM_ROLE_SOURCE_TIMESTAMP_MS, SYSTEM_ROLE_SOURCE_TIMESTAMP_NS,
    SYSTEM_ROLE_SOURCE_TIMESTAMP_US, SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
};
use transferia_core::data::system_columns::SystemColumnKind;
use transferia_core::delivery::{DeliveryDiscovery, SinkLimits};
use transferia_core::sink::Delivery;
use transferia_delivery_contracts::semantics::RecordSemantics;

use super::debezium::{DebeziumJsonEncoder, DebeziumSourceDialect};
use super::json_serializer::{validate_mysql_debezium_column, validate_ydb_debezium_column};
use super::{
    JsonBatchEncoder, QueueMessageMode, SerializedBatch, SerializedDelivery, SerializedMessage,
};

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SerializerConfig {
    #[schemars(title = "JSON", extend("x-ui" = { "capabilities": { "component": "serializer", "key": "json", "record_semantics": ["append_only"] } }))]
    Json,

    #[schemars(title = "Debezium", extend("x-ui" = { "capabilities": { "component": "serializer", "key": "debezium", "record_semantics": ["append_only", "changelog"] } }))]
    Debezium {
        #[schemars(title = "Format")]
        format: DebeziumFormat,
    },

    #[schemars(title = "Schema Registry", extend("x-ui" = { "capabilities": { "component": "serializer", "key": "schema_registry", "record_semantics": ["append_only"] } }))]
    SchemaRegistry {
        #[schemars(title = "Connection")]
        connection: SchemaRegistryConnection,

        #[schemars(title = "Subject")]
        subject: String,

        #[schemars(title = "Format")]
        format: SchemaFormat,

        #[serde(default = "default_message_indexes")]
        #[schemars(
            title = "Protobuf message indexes",
            extend("x-ui" = { "section": "advanced" })
        )]
        protobuf_message_indexes: Vec<i32>,
    },
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum DebeziumFormat {
    #[schemars(title = "JSON (without Schema Registry)")]
    Json,

    #[schemars(title = "JSON (with Schema Registry)")]
    JsonSchema {
        #[schemars(title = "Connection")]
        connection: SchemaRegistryConnection,

        #[schemars(title = "Key subject")]
        key_subject: Option<String>,

        #[schemars(title = "Value subject")]
        value_subject: String,
    },

    #[schemars(title = "Avro")]
    Avro {
        #[schemars(title = "Connection")]
        connection: SchemaRegistryConnection,

        #[schemars(title = "Key subject")]
        key_subject: Option<String>,

        #[schemars(title = "Value subject")]
        value_subject: String,
    },

    #[schemars(title = "Protobuf")]
    Protobuf {
        #[schemars(title = "Connection")]
        connection: SchemaRegistryConnection,

        #[schemars(title = "Key subject")]
        key_subject: Option<String>,

        #[schemars(title = "Value subject")]
        value_subject: String,

        #[serde(default = "default_message_indexes")]
        #[schemars(
            title = "Key message indexes",
            extend("x-ui" = { "section": "advanced" })
        )]
        key_message_indexes: Vec<i32>,

        #[serde(default = "default_message_indexes")]
        #[schemars(
            title = "Value message indexes",
            extend("x-ui" = { "section": "advanced" })
        )]
        value_message_indexes: Vec<i32>,
    },
}

struct DebeziumRegistry<'a> {
    connection: &'a SchemaRegistryConnection,
    key_subject: Option<&'a str>,
    value_subject: &'a str,
    format: SchemaFormat,
    key_message_indexes: &'a [i32],
    value_message_indexes: &'a [i32],
}

impl DebeziumFormat {
    fn validate(&self) -> anyhow::Result<()> {
        let Some(registry) = self.registry() else {
            return Ok(());
        };
        registry.connection.validate()?;
        if let Some(subject) = registry.key_subject {
            validate_subject("debezium.format.key_subject", subject)?;
        }
        validate_subject("debezium.format.value_subject", registry.value_subject)?;
        if registry.format == SchemaFormat::Protobuf {
            validate_message_indexes(
                "debezium.format.key_message_indexes",
                registry.key_message_indexes,
            )?;
            validate_message_indexes(
                "debezium.format.value_message_indexes",
                registry.value_message_indexes,
            )?;
        }
        Ok(())
    }

    fn registry(&self) -> Option<DebeziumRegistry<'_>> {
        match self {
            Self::Json => None,
            Self::JsonSchema {
                connection,
                key_subject,
                value_subject,
            } => Some(DebeziumRegistry {
                connection,
                key_subject: key_subject.as_deref(),
                value_subject,
                format: SchemaFormat::JsonSchema,
                key_message_indexes: &[],
                value_message_indexes: &[],
            }),
            Self::Avro {
                connection,
                key_subject,
                value_subject,
            } => Some(DebeziumRegistry {
                connection,
                key_subject: key_subject.as_deref(),
                value_subject,
                format: SchemaFormat::Avro,
                key_message_indexes: &[],
                value_message_indexes: &[],
            }),
            Self::Protobuf {
                connection,
                key_subject,
                value_subject,
                key_message_indexes,
                value_message_indexes,
            } => Some(DebeziumRegistry {
                connection,
                key_subject: key_subject.as_deref(),
                value_subject,
                format: SchemaFormat::Protobuf,
                key_message_indexes,
                value_message_indexes,
            }),
        }
    }
}

fn default_message_indexes() -> Vec<i32> {
    vec![0]
}

impl SerializerConfig {
    pub const SUPPORTED_RECORD_SEMANTICS: [RecordSemantics; 2] =
        [RecordSemantics::AppendOnly, RecordSemantics::Changelog];

    pub fn validate(&self) -> anyhow::Result<()> {
        match self {
            Self::Json => Ok(()),
            Self::Debezium { format } => format.validate(),
            Self::SchemaRegistry {
                connection,
                subject,
                format,
                protobuf_message_indexes,
            } => {
                connection.validate()?;
                anyhow::ensure!(
                    subject.trim() == subject && !subject.is_empty(),
                    "schema_registry.subject must be nonempty and must not contain leading or trailing whitespace"
                );
                anyhow::ensure!(
                    *format != SchemaFormat::Protobuf || !protobuf_message_indexes.is_empty(),
                    "schema_registry protobuf_message_indexes must not be empty"
                );
                anyhow::ensure!(
                    protobuf_message_indexes.iter().all(|index| *index >= 0),
                    "schema_registry protobuf_message_indexes must be nonnegative"
                );
                Ok(())
            }
        }
    }

    pub fn destination_type(
        &self,
        data_type: &arrow::datatypes::DataType,
    ) -> anyhow::Result<String> {
        match self {
            Self::Json => Ok(format!("JSON {}", json_type_name(data_type)?)),
            Self::Debezium {
                format: DebeziumFormat::Json,
                ..
            } => Ok(format!("Debezium JSON {}", json_type_name(data_type)?)),
            Self::Debezium { format, .. } => {
                let Some(registry) = format.registry() else {
                    anyhow::bail!("Debezium Schema Registry format is required")
                };
                Ok(format!(
                    "Debezium {} ({})",
                    schema_format_title(registry.format),
                    registry.value_subject
                ))
            }
            Self::SchemaRegistry {
                subject, format, ..
            } => Ok(format!(
                "{} ({})",
                match format {
                    SchemaFormat::Avro => "Avro",
                    SchemaFormat::JsonSchema => "JSON Schema",
                    SchemaFormat::Protobuf => "Protobuf",
                },
                subject
            )),
        }
    }

    #[must_use]
    pub const fn record_semantics(&self) -> &'static [RecordSemantics] {
        match self {
            Self::Json | Self::SchemaRegistry { .. } => &[RecordSemantics::AppendOnly],
            Self::Debezium { .. } => &[RecordSemantics::AppendOnly, RecordSemantics::Changelog],
        }
    }

    #[must_use]
    pub const fn supports_changelog(&self) -> bool {
        matches!(self, Self::Debezium { .. })
    }

    pub fn validate_discovery(&self, discovery: &DeliveryDiscovery) -> anyhow::Result<()> {
        if !matches!(self, Self::Debezium { .. }) {
            return Ok(());
        }
        let dialect = DebeziumSourceDialect::from_source_name(discovery.source_name.as_ref())?;
        let datasets = discovery
            .datasets
            .iter()
            .filter(|dataset| dataset.role == transferia_core::delivery::DatasetRole::Main)
            .collect::<Vec<_>>();
        anyhow::ensure!(
            !datasets.is_empty(),
            "Debezium serializer requires at least one main dataset"
        );
        for dataset in datasets {
            let primary_keys = dataset
                .incoming_schema
                .columns
                .iter()
                .filter(|column| column.primary_key)
                .collect::<Vec<_>>();
            anyhow::ensure!(
                !primary_keys.is_empty(),
                "Debezium dataset '{}' requires at least one primary-key column",
                dataset.name
            );
            let changelog = dataset
                .system_columns
                .iter()
                .any(|column| column.kind == SystemColumnKind::ChangeOperation);
            if matches!(
                dialect,
                DebeziumSourceDialect::MySql | DebeziumSourceDialect::Ydb
            ) {
                let dialect_name = if dialect == DebeziumSourceDialect::MySql {
                    "MySQL"
                } else {
                    "YDB"
                };
                anyhow::ensure!(
                    changelog,
                    "Debezium {dialect_name} dataset '{}' must use a changelog replication schema",
                    dataset.name
                );
                let current_columns = dataset
                    .incoming_schema
                    .columns
                    .iter()
                    .filter(|column| {
                        column.system_role.is_none()
                            && column.old_value_of.is_none()
                            && column.old_key_of.is_none()
                            && !dataset
                                .system_columns
                                .iter()
                                .any(|system| system.name.as_ref() == column.name.as_str())
                            && ![
                                SystemColumnKind::Topic,
                                SystemColumnKind::Partition,
                                SystemColumnKind::Offset,
                                SystemColumnKind::MessageIndex,
                                SystemColumnKind::WriteTimestampMs,
                                SystemColumnKind::ChangeOperation,
                                SystemColumnKind::ChangedColumns,
                            ]
                            .iter()
                            .any(|kind| kind.default_name() == column.name)
                    })
                    .collect::<Vec<_>>();
                for current in current_columns {
                    let validate = if dialect == DebeziumSourceDialect::MySql {
                        validate_mysql_debezium_column
                    } else {
                        validate_ydb_debezium_column
                    };
                    validate(current).map_err(|error| {
                        anyhow::anyhow!(
                            "Debezium {dialect_name} dataset '{}' column '{}': {error}",
                            dataset.name,
                            current.name
                        )
                    })?;
                    let old_values = dataset
                        .incoming_schema
                        .columns
                        .iter()
                        .filter(|column| {
                            column.old_value_of.as_deref() == Some(current.name.as_str())
                        })
                        .collect::<Vec<_>>();
                    anyhow::ensure!(
                        old_values.len() == 1,
                        "Debezium {dialect_name} dataset '{}' column '{}' must have exactly one full old-value mapping, found {}",
                        dataset.name,
                        current.name,
                        old_values.len()
                    );
                    let old = old_values[0];
                    validate(old).map_err(|error| {
                        anyhow::anyhow!(
                            "Debezium {dialect_name} dataset '{}' old value for '{}': {error}",
                            dataset.name,
                            current.name
                        )
                    })?;
                    anyhow::ensure!(
                        old.data_type == current.data_type
                            && old.arrow_extension_name == current.arrow_extension_name
                            && old.arrow_extension_metadata == current.arrow_extension_metadata,
                        "Debezium {dialect_name} dataset '{}' old value for '{}' does not preserve its exact physical Arrow type and extension metadata",
                        dataset.name,
                        current.name
                    );
                }
            }
            if changelog {
                for key in primary_keys {
                    let mappings = dataset
                        .incoming_schema
                        .columns
                        .iter()
                        .filter(|column| {
                            column.old_key_of.as_deref() == Some(key.name.as_str())
                                || column.old_value_of.as_deref() == Some(key.name.as_str())
                        })
                        .count();
                    anyhow::ensure!(
                        mappings == 1,
                        "Debezium dataset '{}' primary key '{}' must have exactly one old-key or old-value mapping, found {mappings}",
                        dataset.name,
                        key.name
                    );
                }
            }
            let required_system_columns = if dialect == DebeziumSourceDialect::Ydb {
                &[
                    SystemColumnKind::Topic,
                    SystemColumnKind::Partition,
                    SystemColumnKind::Offset,
                    SystemColumnKind::MessageIndex,
                    SystemColumnKind::WriteTimestampMs,
                    SystemColumnKind::ChangeOperation,
                    SystemColumnKind::ChangedColumns,
                ][..]
            } else if changelog {
                &[
                    SystemColumnKind::Offset,
                    SystemColumnKind::ChangeOperation,
                    SystemColumnKind::ChangedColumns,
                ][..]
            } else {
                &[SystemColumnKind::Offset][..]
            };
            for kind in required_system_columns {
                anyhow::ensure!(
                    dataset
                        .system_columns
                        .iter()
                        .any(|column| column.kind == *kind),
                    "Debezium dataset '{}' is missing required {kind:?} metadata",
                    dataset.name
                );
            }
            let mut required_roles = vec![
                (
                    SYSTEM_ROLE_SOURCE_DATABASE,
                    arrow::datatypes::DataType::Utf8,
                    false,
                ),
                (
                    SYSTEM_ROLE_SOURCE_TABLE,
                    arrow::datatypes::DataType::Utf8,
                    false,
                ),
                (
                    SYSTEM_ROLE_SOURCE_TIMESTAMP_MS,
                    arrow::datatypes::DataType::Int64,
                    false,
                ),
            ];
            match dialect {
                DebeziumSourceDialect::Postgres => required_roles.extend([
                    (
                        SYSTEM_ROLE_SOURCE_SCHEMA,
                        arrow::datatypes::DataType::Utf8,
                        false,
                    ),
                    (
                        SYSTEM_ROLE_SOURCE_TIMESTAMP_US,
                        arrow::datatypes::DataType::Int64,
                        false,
                    ),
                    (
                        SYSTEM_ROLE_SOURCE_TIMESTAMP_NS,
                        arrow::datatypes::DataType::Int64,
                        false,
                    ),
                    (
                        SYSTEM_ROLE_EVENT_TIMESTAMP_MS,
                        arrow::datatypes::DataType::Int64,
                        false,
                    ),
                    (
                        SYSTEM_ROLE_EVENT_TIMESTAMP_US,
                        arrow::datatypes::DataType::Int64,
                        false,
                    ),
                    (
                        SYSTEM_ROLE_EVENT_TIMESTAMP_NS,
                        arrow::datatypes::DataType::Int64,
                        false,
                    ),
                    (
                        SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
                        arrow::datatypes::DataType::UInt64,
                        false,
                    ),
                ]),
                DebeziumSourceDialect::MySql => required_roles.extend([
                    (
                        SYSTEM_ROLE_SOURCE_SCHEMA,
                        arrow::datatypes::DataType::Utf8,
                        false,
                    ),
                    (
                        SYSTEM_ROLE_SOURCE_TIMESTAMP_US,
                        arrow::datatypes::DataType::Int64,
                        false,
                    ),
                    (
                        SYSTEM_ROLE_SOURCE_TIMESTAMP_NS,
                        arrow::datatypes::DataType::Int64,
                        false,
                    ),
                    (
                        SYSTEM_ROLE_EVENT_TIMESTAMP_MS,
                        arrow::datatypes::DataType::Int64,
                        false,
                    ),
                    (
                        SYSTEM_ROLE_EVENT_TIMESTAMP_US,
                        arrow::datatypes::DataType::Int64,
                        false,
                    ),
                    (
                        SYSTEM_ROLE_EVENT_TIMESTAMP_NS,
                        arrow::datatypes::DataType::Int64,
                        false,
                    ),
                    (
                        SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
                        arrow::datatypes::DataType::Binary,
                        false,
                    ),
                    (
                        SYSTEM_ROLE_SOURCE_SERVER_ID,
                        arrow::datatypes::DataType::Int64,
                        false,
                    ),
                    (
                        SYSTEM_ROLE_SOURCE_GTID,
                        arrow::datatypes::DataType::Utf8,
                        true,
                    ),
                    (
                        SYSTEM_ROLE_SOURCE_BINLOG_FILE,
                        arrow::datatypes::DataType::Utf8,
                        false,
                    ),
                    (
                        SYSTEM_ROLE_SOURCE_BINLOG_POSITION,
                        arrow::datatypes::DataType::Int64,
                        false,
                    ),
                    (
                        SYSTEM_ROLE_SOURCE_BINLOG_ROW,
                        arrow::datatypes::DataType::Int32,
                        false,
                    ),
                ]),
                DebeziumSourceDialect::Ydb => required_roles.push((
                    SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
                    arrow::datatypes::DataType::FixedSizeBinary(16),
                    false,
                )),
            }
            for (role, data_type, nullable) in required_roles {
                let matches = dataset
                    .incoming_schema
                    .columns
                    .iter()
                    .filter(|column| column.system_role.as_deref() == Some(role))
                    .collect::<Vec<_>>();
                anyhow::ensure!(
                    matches.len() == 1,
                    "Debezium dataset '{}' must have exactly one '{role}' control column, found {}",
                    dataset.name,
                    matches.len()
                );
                if nullable {
                    anyhow::ensure!(
                        matches[0].data_type == data_type && matches[0].nullable,
                        "Debezium dataset '{}' control role '{role}' must be nullable {data_type:?}",
                        dataset.name,
                    );
                } else {
                    anyhow::ensure!(
                        matches[0].data_type == data_type && !matches[0].nullable,
                        "Debezium dataset '{}' control role '{role}' must be non-nullable {data_type:?}",
                        dataset.name,
                    );
                }
            }
        }
        Ok(())
    }
}

fn validate_debezium_delivery_name(delivery_name: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        delivery_name.trim() == delivery_name && !delivery_name.is_empty(),
        "Debezium delivery name must be nonempty and must not contain leading or trailing whitespace"
    );
    Ok(())
}

fn validate_subject(path: &str, subject: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        subject.trim() == subject && !subject.is_empty(),
        "{path} must be nonempty and must not contain leading or trailing whitespace"
    );
    Ok(())
}

fn validate_message_indexes(path: &str, indexes: &[i32]) -> anyhow::Result<()> {
    anyhow::ensure!(!indexes.is_empty(), "{path} must not be empty");
    anyhow::ensure!(
        indexes.iter().all(|index| *index >= 0),
        "{path} must contain only nonnegative indexes"
    );
    Ok(())
}

const fn schema_format_title(format: SchemaFormat) -> &'static str {
    match format {
        SchemaFormat::Avro => "Avro",
        SchemaFormat::JsonSchema => "JSON Schema",
        SchemaFormat::Protobuf => "Protobuf",
    }
}

fn json_type_name(data_type: &arrow::datatypes::DataType) -> anyhow::Result<&'static str> {
    use arrow::datatypes::DataType;
    Ok(match data_type {
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_)
        | DataType::Date32
        | DataType::Date64
        | DataType::Timestamp(_, _) => "string",
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Duration(_) => "number",
        DataType::Boolean => "boolean",
        other => anyhow::bail!("Arrow type {other:?} has no JSON serializer representation"),
    })
}

pub struct DeliverySerializer {
    kind: SerializerKind,
}

enum SerializerKind {
    Json,
    DebeziumJson(DebeziumJsonEncoder),
    DebeziumRegistry {
        encoder: DebeziumJsonEncoder,
        registry: RegistryClient,
        key_subject: Option<String>,
        value_subject: String,
        format: SchemaFormat,
        key_message_indexes: Vec<i32>,
        value_message_indexes: Vec<i32>,
        key_schema: Box<Option<CompiledWriterSchema>>,
        value_schema: Box<Option<CompiledWriterSchema>>,
    },
    SchemaRegistry {
        registry: RegistryClient,
        subject: String,
        format: SchemaFormat,
        message_indexes: Vec<i32>,
        schema: Box<Option<CompiledWriterSchema>>,
    },
}

enum CompiledWriterSchema {
    Avro {
        id: i32,
        schema: Schema,
    },
    Json {
        id: i32,
        validator: jsonschema::Validator,
    },
    Protobuf {
        id: i32,
        descriptor: MessageDescriptor,
    },
}

impl DeliverySerializer {
    pub fn new(
        config: &SerializerConfig,
        mode: QueueMessageMode,
        delivery_name: &str,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        let kind = match config {
            SerializerConfig::Json => SerializerKind::Json,
            SerializerConfig::Debezium {
                format: DebeziumFormat::Json,
            } => {
                validate_debezium_delivery_name(delivery_name)?;
                SerializerKind::DebeziumJson(DebeziumJsonEncoder::new(
                    delivery_name.to_owned(),
                    mode,
                ))
            }
            SerializerConfig::Debezium { format } => {
                validate_debezium_delivery_name(delivery_name)?;
                let Some(registry_config) = format.registry() else {
                    anyhow::bail!("Debezium Schema Registry format is required")
                };
                anyhow::ensure!(
                    mode == QueueMessageMode::ValuesOnly || registry_config.key_subject.is_some(),
                    "Debezium Schema Registry serializer requires key_subject for a keyed queue sink"
                );
                SerializerKind::DebeziumRegistry {
                    encoder: DebeziumJsonEncoder::new(delivery_name.to_owned(), mode),
                    registry: RegistryClient::new(registry_config.connection)?,
                    key_subject: registry_config.key_subject.map(str::to_owned),
                    value_subject: registry_config.value_subject.to_owned(),
                    format: registry_config.format,
                    key_message_indexes: registry_config.key_message_indexes.to_vec(),
                    value_message_indexes: registry_config.value_message_indexes.to_vec(),
                    key_schema: Box::new(None),
                    value_schema: Box::new(None),
                }
            }
            SerializerConfig::SchemaRegistry {
                connection,
                subject,
                format,
                protobuf_message_indexes,
            } => SerializerKind::SchemaRegistry {
                registry: RegistryClient::new(connection)?,
                subject: subject.clone(),
                format: *format,
                message_indexes: protobuf_message_indexes.clone(),
                schema: Box::new(None),
            },
        };
        Ok(Self { kind })
    }

    pub async fn serialize(
        &mut self,
        delivery: &Delivery,
        discovery: &DeliveryDiscovery,
        limits: &dyn SinkLimits,
        message_size_limit: usize,
    ) -> anyhow::Result<SerializedDelivery> {
        let debezium_dialect = match &self.kind {
            SerializerKind::DebeziumJson(_) | SerializerKind::DebeziumRegistry { .. } => Some(
                DebeziumSourceDialect::from_source_name(discovery.source_name.as_ref())?,
            ),
            SerializerKind::Json | SerializerKind::SchemaRegistry { .. } => None,
        };
        if let SerializerKind::SchemaRegistry {
            registry,
            subject,
            format,
            message_indexes,
            schema,
        } = &mut self.kind
        {
            if schema.is_none() {
                **schema = Some(compile_writer_schema(
                    &registry.latest_schema(subject, *format).await?,
                    message_indexes,
                )?);
            }
        }
        if let SerializerKind::DebeziumRegistry {
            registry,
            key_subject,
            value_subject,
            format,
            key_message_indexes,
            value_message_indexes,
            key_schema,
            value_schema,
            ..
        } = &mut self.kind
        {
            if value_schema.is_none() {
                **value_schema = Some(compile_writer_schema(
                    &registry.latest_schema(value_subject, *format).await?,
                    value_message_indexes,
                )?);
            }
            if key_schema.is_none() {
                if let Some(subject) = key_subject {
                    **key_schema = Some(compile_writer_schema(
                        &registry.latest_schema(subject, *format).await?,
                        key_message_indexes,
                    )?);
                }
            }
        }

        let mut batches = Vec::with_capacity(delivery.outputs.len());
        let mut rows = 0_u64;
        for batch in &delivery.outputs {
            limits.validate_batch(discovery, batch)?;
            if let SerializerKind::DebeziumJson(encoder) = &self.kind {
                batches.push(encoder.encode_batch(
                    batch,
                    debezium_dialect.ok_or_else(|| {
                        anyhow::anyhow!("internal error: Debezium source dialect was not resolved")
                    })?,
                    message_size_limit,
                )?);
                rows = rows
                    .checked_add(u64::try_from(batch.rows())?)
                    .ok_or_else(|| anyhow::anyhow!("queue sink row counter overflow"))?;
                continue;
            }
            if let SerializerKind::DebeziumRegistry {
                encoder,
                key_message_indexes,
                value_message_indexes,
                key_schema,
                value_schema,
                ..
            } = &self.kind
            {
                let encoded = encoder.encode_batch(
                    batch,
                    debezium_dialect.ok_or_else(|| {
                        anyhow::anyhow!("internal error: Debezium source dialect was not resolved")
                    })?,
                    usize::MAX,
                )?;
                batches.push(encode_debezium_registered_batch(
                    encoded,
                    key_schema.as_ref().as_ref(),
                    key_message_indexes,
                    value_schema.as_ref().as_ref().ok_or_else(|| {
                        anyhow::anyhow!("Debezium Schema Registry value schema was not initialized")
                    })?,
                    value_message_indexes,
                    message_size_limit,
                )?);
                rows = rows
                    .checked_add(u64::try_from(batch.rows())?)
                    .ok_or_else(|| anyhow::anyhow!("queue sink row counter overflow"))?;
                continue;
            }
            let encoder = JsonBatchEncoder::new(&batch.batch, |index| {
                !batch
                    .system_columns
                    .iter()
                    .any(|column| column.index == index)
            })?;
            let mut payloads = Vec::with_capacity(batch.rows());
            for row in 0..batch.rows() {
                let mut json = Vec::new();
                encoder.write_row(row, &mut json)?;
                let newline = json.pop();
                anyhow::ensure!(
                    newline == Some(b'\n'),
                    "JSON serializer omitted row delimiter"
                );
                let output = match &self.kind {
                    SerializerKind::Json => {
                        json.push(b'\n');
                        json
                    }
                    SerializerKind::SchemaRegistry {
                        message_indexes,
                        schema,
                        ..
                    } => encode_registered(
                        schema.as_ref().as_ref().ok_or_else(|| {
                            anyhow::anyhow!("Schema Registry writer schema was not initialized")
                        })?,
                        message_indexes,
                        &json,
                    )?,
                    SerializerKind::DebeziumJson(_) | SerializerKind::DebeziumRegistry { .. } => {
                        anyhow::bail!(
                            "internal error: Debezium batch reached ordinary row serialization"
                        )
                    }
                };
                validate_payload_size(&output, message_size_limit)?;
                payloads.push(SerializedMessage {
                    key: None,
                    value: Some(output),
                });
            }
            batches.push(SerializedBatch {
                table: std::sync::Arc::clone(&batch.table),
                messages: payloads,
            });
            rows = rows
                .checked_add(batch.rows() as u64)
                .ok_or_else(|| anyhow::anyhow!("queue sink row counter overflow"))?;
        }
        Ok(SerializedDelivery {
            batches,
            source_rows: rows,
        })
    }
}

fn encode_debezium_registered_batch(
    mut batch: SerializedBatch,
    key_schema: Option<&CompiledWriterSchema>,
    key_message_indexes: &[i32],
    value_schema: &CompiledWriterSchema,
    value_message_indexes: &[i32],
    message_size_limit: usize,
) -> anyhow::Result<SerializedBatch> {
    for message in &mut batch.messages {
        if let Some(key) = &mut message.key {
            let schema = key_schema.ok_or_else(|| {
                anyhow::anyhow!("Debezium Schema Registry key schema was not initialized")
            })?;
            *key = encode_registered(schema, key_message_indexes, key)?;
        }
        if let Some(value) = &mut message.value {
            *value = encode_registered(value_schema, value_message_indexes, value)?;
        }
        let message_bytes =
            message.key.as_ref().map_or(0, Vec::len) + message.value.as_ref().map_or(0, Vec::len);
        anyhow::ensure!(
            message_bytes <= message_size_limit,
            "serialized queue message exceeds configured transport limit: message_bytes={message_bytes}, transport_limit_bytes={message_size_limit}"
        );
    }
    Ok(batch)
}

fn validate_payload_size(payload: &[u8], message_size_limit: usize) -> anyhow::Result<()> {
    anyhow::ensure!(
        payload.len() <= message_size_limit,
        "serialized queue message exceeds configured transport limit: message_bytes={}, transport_limit_bytes={message_size_limit}",
        payload.len()
    );
    Ok(())
}

fn compile_writer_schema(
    schema: &RegistrySchema,
    message_indexes: &[i32],
) -> anyhow::Result<CompiledWriterSchema> {
    match schema.format {
        SchemaFormat::Avro => Ok(CompiledWriterSchema::Avro {
            id: schema.id,
            schema: {
                let definitions = schema
                    .references
                    .iter()
                    .map(|reference| reference.definition.as_str())
                    .chain(std::iter::once(schema.definition.as_str()))
                    .collect::<Vec<_>>();
                Schema::parse_list(definitions)?
                    .pop()
                    .ok_or_else(|| anyhow::anyhow!("Schema Registry returned no Avro schema"))?
            },
        }),
        SchemaFormat::JsonSchema => {
            let definition = serde_json::from_str::<serde_json::Value>(&schema.definition)?;
            let resources = schema
                .references
                .iter()
                .map(|reference| {
                    let definition = serde_json::from_str(&reference.definition)?;
                    let resource = jsonschema::Resource::from_contents(definition)?;
                    Ok((reference.name.clone(), resource))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            Ok(CompiledWriterSchema::Json {
                id: schema.id,
                validator: jsonschema::options()
                    .with_resources(resources.into_iter())
                    .build(&definition)?,
            })
        }
        SchemaFormat::Protobuf => {
            let (pool, file_name) = crate::schema_registry::protobuf_descriptor_pool(
                &schema.definition,
                &schema.references,
            )?;
            let file = pool.get_file_by_name(&file_name).ok_or_else(|| {
                anyhow::anyhow!("Protobuf schema file is absent from descriptor pool")
            })?;
            let descriptor = message_at_indexes(file.messages().collect(), message_indexes)?;
            Ok(CompiledWriterSchema::Protobuf {
                id: schema.id,
                descriptor,
            })
        }
    }
}

fn encode_registered(
    schema: &CompiledWriterSchema,
    message_indexes: &[i32],
    json: &[u8],
) -> anyhow::Result<Vec<u8>> {
    match schema {
        CompiledWriterSchema::Avro { id, schema } => {
            let value: serde_json::Value = serde_json::from_slice(json)?;
            let value = crate::schema_registry::json_to_avro(schema, &value)?;
            ConfluentEnvelope::encode(*id, &apache_avro::to_avro_datum(schema, value)?)
        }
        CompiledWriterSchema::Json { id, validator } => {
            let value = serde_json::from_slice(json)?;
            validator
                .validate(&value)
                .map_err(|error| anyhow::anyhow!("JSON Schema validation failed: {error}"))?;
            ConfluentEnvelope::encode(*id, json)
        }
        CompiledWriterSchema::Protobuf { id, descriptor } => {
            let mut deserializer = serde_json::Deserializer::from_slice(json);
            let message = DynamicMessage::deserialize(descriptor.clone(), &mut deserializer)?;
            let mut payload = Vec::new();
            encode_message_indexes(message_indexes, &mut payload)?;
            message.encode(&mut payload)?;
            ConfluentEnvelope::encode(*id, &payload)
        }
    }
}

fn message_at_indexes(
    mut messages: Vec<MessageDescriptor>,
    indexes: &[i32],
) -> anyhow::Result<MessageDescriptor> {
    let mut selected = None;
    for index in indexes {
        let index = usize::try_from(*index)?;
        let message = messages
            .get(index)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Protobuf message index {index} is out of range"))?;
        messages = message.child_messages().collect();
        selected = Some(message);
    }
    selected.ok_or_else(|| anyhow::anyhow!("Protobuf message-index array is empty"))
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
