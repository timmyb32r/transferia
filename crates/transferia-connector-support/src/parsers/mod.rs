pub mod benchmark_discard;
pub mod config;
pub mod detection;
pub mod json_parser;
mod native_source;
mod plugin;
pub mod protobuf;
pub mod raw_to_table;
pub mod schema_registry;

use std::collections::HashMap;

use alloc::sync::Arc;
use serde::Deserialize;
use serde_yaml::Value;

use transferia_core::data::message::Message;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::data::table_data::{dlq_name, TableData};
use transferia_core::delivery::{
    DatasetRole, DeliveryDiscovery, DeliveryDiscoveryRequest, DiscoveredDataset,
    DiscoveredSystemColumn, SchemaOrigin, SourceTopology,
};

pub use config::{CommonParserConfig, ParserConfig, SystemColumnsConfig, TableNaming};
pub use plugin::{ParserPluginRegistry, ParserPluginSpec};
pub use schema_registry::SchemaDecoder;

/// Common parser interface. Every parser converts raw [`Message`]s into
/// Arrow [`TableData`] (valid + optional DLQ).
pub use transferia_delivery_contracts::parser::{ParserFactory, ParserSession};

/// A parser compiled once from source configuration and shared by all partition pipelines.
/// It is the single source of truth for the parser instance and its sink-facing schemas.
pub struct ParserPlan {
    parser: Arc<dyn ParserFactory>,
    table: Arc<str>,
    dataset_schema: DatasetSchema,
    parses_rows: bool,
    discovered_system_columns: Vec<DiscoveredSystemColumn>,
    primary_key: Arc<[String]>,
    dlq_dataset_schema: DatasetSchema,
}

impl ParserPlan {
    #[must_use]
    pub fn native_source() -> Self {
        Self {
            parser: Arc::new(native_source::NativeSourceParser),
            table: Arc::from(""),
            dataset_schema: DatasetSchema::default(),
            parses_rows: true,
            discovered_system_columns: Vec::new(),
            primary_key: Arc::from([]),
            dlq_dataset_schema: default_dlq_schema(),
        }
    }

    /// Build sink-facing discovery for a raw source whose rows are defined by this parser.
    pub fn delivery_discovery(
        &self,
        source_name: Arc<str>,
        source_topology: SourceTopology,
        request: DeliveryDiscoveryRequest,
    ) -> anyhow::Result<DeliveryDiscovery> {
        anyhow::ensure!(
            !source_name.is_empty(),
            "discovered source name must not be empty"
        );
        source_topology.validate()?;

        let datasets = if self.parses_rows() {
            let system_columns = self.system_columns();
            let table = self.table();
            let dlq_table: Arc<str> = dlq_name(&table).into();
            vec![
                DiscoveredDataset {
                    role: DatasetRole::Main,
                    name: table,
                    incoming_schema: self.sink_schema(true),
                    stored_schema: self.sink_schema(request.keep_system_columns),
                    system_columns: system_columns.clone(),
                },
                DiscoveredDataset {
                    role: DatasetRole::DeadLetterQueue,
                    name: dlq_table,
                    incoming_schema: self.dlq_schema(true),
                    stored_schema: self.dlq_schema(request.keep_system_columns),
                    system_columns,
                },
            ]
        } else {
            Vec::new()
        };

        Ok(DeliveryDiscovery {
            source_name,
            source_topology,
            schema_origin: SchemaOrigin::ParserProjection,
            keep_system_columns: request.keep_system_columns,
            datasets,
            performance_advice: Vec::new(),
        })
    }

    pub fn from_config(config: &ParserConfig, topic_path: &str) -> anyhow::Result<Self> {
        Self::from_config_with_plugins(config, topic_path, &ParserPluginRegistry::default())
    }

    pub fn from_config_with_plugins(
        config: &ParserConfig,
        topic_path: &str,
        plugins: &ParserPluginRegistry,
    ) -> anyhow::Result<Self> {
        config.common.system_columns.validate()?;
        let kind = config.parser.kind()?;
        let (parser, dataset_schema, parses_rows, discovered_system_columns, primary_key) =
            match kind {
                "json_parser" => {
                    let parser_config: json_parser::JsonParserConfig =
                        serde_yaml::from_value(config.parser.raw()?.clone())?;
                    return Self::from_json_config(&config.common, &parser_config, topic_path);
                }
                "schema_registry" => {
                    let table: Arc<str> = config.resolve_table_name(topic_path)?.into();
                    anyhow::ensure!(
                        config.common.system_columns.message_index.is_none(),
                        "schema_registry parser does not support the message_index system column because one source message always produces one row"
                    );
                    let parser_config: schema_registry::SchemaRegistryParserConfig =
                        serde_yaml::from_value(config.parser.raw()?.clone())?;
                    let schema = parser_config.json_parser.to_dataset_schema()?;
                    let discovered_system_columns = config
                        .common
                        .system_columns
                        .enabled()
                        .map(|kind| DiscoveredSystemColumn {
                            kind,
                            name: Arc::from(config.common.system_columns.name(kind)),
                        })
                        .collect::<Vec<_>>();
                    validate_primary_key(
                        &parser_config.json_parser,
                        &config.common.system_columns,
                        &schema,
                        &discovered_system_columns,
                    )?;
                    let primary_key = Arc::from(parser_config.json_parser.keys.clone());
                    let parser = Arc::new(schema_registry::SchemaRegistryParser::new(
                        &parser_config,
                        &config.common.system_columns,
                        Arc::clone(&table),
                    )?) as Arc<dyn ParserFactory>;
                    (parser, schema, true, discovered_system_columns, primary_key)
                }
                "raw_to_table" => {
                    anyhow::ensure!(
                        config.common.system_columns.enabled().next().is_none(),
                        "raw_to_table owns topic, partition, offset, write timestamp, key, and headers columns; common.system_columns must be empty"
                    );
                    let parser_config: raw_to_table::RawToTableParserConfig =
                        serde_yaml::from_value(config.parser.raw()?.clone())?;
                    return Self::from_raw_to_table_config(
                        &config.common,
                        &parser_config,
                        topic_path,
                    );
                }
                "benchmark_discard" => {
                    let _: benchmark_discard::BenchmarkDiscardConfig =
                        serde_yaml::from_value(config.parser.raw()?.clone())?;
                    return Ok(Self::from_benchmark_discard(topic_path));
                }
                other => {
                    if let Some(plan) =
                        plugins.build(other, &config.common, config.parser.raw()?, topic_path)?
                    {
                        return Ok(plan);
                    }
                    let mut supported = vec![
                        "json_parser",
                        "schema_registry",
                        "raw_to_table",
                        "benchmark_discard",
                    ];
                    supported.extend(plugins.kinds());
                    anyhow::bail!(
                        "unknown parser '{other}'; supported parsers: {}",
                        supported.join(", ")
                    );
                }
            };
        Ok(Self {
            parser,
            table: config.resolve_table_name(topic_path)?.into(),
            dataset_schema,
            parses_rows,
            discovered_system_columns,
            primary_key,
            dlq_dataset_schema: default_dlq_schema(),
        })
    }

    pub fn from_plugin(
        common: &CommonParserConfig,
        source_name: &str,
        parser: Arc<dyn ParserFactory>,
        dataset_schema: DatasetSchema,
        dlq_dataset_schema: Option<DatasetSchema>,
    ) -> anyhow::Result<Self> {
        common.system_columns.validate()?;
        let table: Arc<str> = common.resolve_table_name(source_name)?.into();
        let discovered_system_columns = common
            .system_columns
            .enabled()
            .map(|kind| DiscoveredSystemColumn {
                kind,
                name: Arc::from(common.system_columns.name(kind)),
            })
            .collect::<Vec<_>>();
        validate_plugin_schema(&dataset_schema, &discovered_system_columns)?;
        let primary_key = dataset_schema
            .columns
            .iter()
            .filter(|column| column.primary_key)
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        Ok(Self {
            parser,
            table,
            dataset_schema,
            parses_rows: true,
            discovered_system_columns,
            primary_key: primary_key.into(),
            dlq_dataset_schema: dlq_dataset_schema.unwrap_or_else(default_dlq_schema),
        })
    }

    pub fn from_json_config(
        common: &CommonParserConfig,
        parser_config: &json_parser::JsonParserConfig,
        source_name: &str,
    ) -> anyhow::Result<Self> {
        common.system_columns.validate()?;
        let table: Arc<str> = common.resolve_table_name(source_name)?.into();
        let schema = parser_config.to_dataset_schema()?;
        let discovered_system_columns = common
            .system_columns
            .enabled()
            .map(|kind| DiscoveredSystemColumn {
                kind,
                name: Arc::from(common.system_columns.name(kind)),
            })
            .collect::<Vec<_>>();
        validate_primary_key(
            parser_config,
            &common.system_columns,
            &schema,
            &discovered_system_columns,
        )?;
        let parser = Arc::new(json_parser::JsonParser::new(
            parser_config,
            &common.system_columns,
            Arc::clone(&table),
        )?) as Arc<dyn ParserFactory>;
        Ok(Self {
            parser,
            table,
            dataset_schema: schema,
            parses_rows: true,
            discovered_system_columns,
            primary_key: Arc::from(parser_config.keys.clone()),
            dlq_dataset_schema: default_dlq_schema(),
        })
    }

    pub fn from_raw_to_table_config(
        common: &CommonParserConfig,
        parser_config: &raw_to_table::RawToTableParserConfig,
        source_name: &str,
    ) -> anyhow::Result<Self> {
        common.system_columns.validate()?;
        anyhow::ensure!(
            common.system_columns.enabled().next().is_none(),
            "raw_to_table owns its source metadata columns; common.system_columns must be empty"
        );
        let table: Arc<str> = common.resolve_table_name(source_name)?.into();
        let parser = Arc::new(raw_to_table::RawToTableParser::new(
            parser_config,
            Arc::clone(&table),
        )?) as Arc<dyn ParserFactory>;
        Ok(Self {
            parser,
            table,
            dataset_schema: parser_config.dataset_schema(),
            parses_rows: true,
            discovered_system_columns: Vec::new(),
            primary_key: Arc::from(raw_to_table::PRIMARY_KEY.map(str::to_owned)),
            dlq_dataset_schema: raw_to_table::dlq_dataset_schema(),
        })
    }

    #[must_use]
    pub fn from_benchmark_discard(source_name: &str) -> Self {
        let table: Arc<str> = source_name.into();
        Self {
            parser: Arc::new(benchmark_discard::BenchmarkDiscardParser::new(Arc::clone(
                &table,
            ))),
            table,
            dataset_schema: DatasetSchema::default(),
            parses_rows: false,
            discovered_system_columns: Vec::new(),
            primary_key: Arc::from([]),
            dlq_dataset_schema: default_dlq_schema(),
        }
    }

    /// Route each parsed batch to the source topic carried by its messages.
    /// The source must keep one parser delivery topic-homogeneous.
    #[must_use]
    pub fn route_by_message_topic(mut self) -> Self {
        self.parser = Arc::new(TopicTableParserFactory {
            inner: Arc::clone(&self.parser),
        });
        self
    }

    #[must_use]
    pub fn parser(&self) -> Arc<dyn ParserFactory> {
        Arc::clone(&self.parser)
    }

    #[must_use]
    pub fn table(&self) -> Arc<str> {
        Arc::clone(&self.table)
    }

    #[must_use]
    pub const fn dataset_schema(&self) -> &DatasetSchema {
        &self.dataset_schema
    }

    #[must_use]
    pub fn system_columns(&self) -> Vec<DiscoveredSystemColumn> {
        self.discovered_system_columns.clone()
    }

    #[must_use]
    pub const fn parses_rows(&self) -> bool {
        self.parses_rows
    }

    #[must_use]
    pub fn sink_schema(&self, keep_system_columns: bool) -> DatasetSchema {
        let mut schema = self.dataset_schema.clone();
        if keep_system_columns {
            schema
                .columns
                .extend(self.discovered_system_columns.iter().map(|column| {
                    SchemaColumn::new(column.name.to_string(), column.kind.data_type(), false)
                        .with_constraints(
                            self.primary_key
                                .iter()
                                .any(|name| name == column.name.as_ref()),
                            false,
                            None,
                        )
                }));
        }
        schema
    }

    #[must_use]
    pub fn dlq_schema(&self, keep_system_columns: bool) -> DatasetSchema {
        let mut columns = self.dlq_dataset_schema.columns.clone();
        if keep_system_columns {
            columns.extend(self.discovered_system_columns.iter().map(|column| {
                SchemaColumn::new(column.name.to_string(), column.kind.data_type(), false)
            }));
        }
        DatasetSchema::new(columns)
    }
}

fn validate_plugin_schema(
    schema: &DatasetSchema,
    system_columns: &[DiscoveredSystemColumn],
) -> anyhow::Result<()> {
    anyhow::ensure!(
        !schema.columns.is_empty(),
        "parser plugin output schema must contain at least one column"
    );
    let mut names = std::collections::HashSet::new();
    for column in &schema.columns {
        anyhow::ensure!(
            !column.name.is_empty(),
            "parser plugin output column names must not be empty"
        );
        anyhow::ensure!(
            names.insert(column.name.as_str()),
            "parser plugin output repeats column '{}'",
            column.name
        );
        anyhow::ensure!(
            !column.primary_key || !column.nullable,
            "parser plugin primary-key column '{}' must not be nullable",
            column.name
        );
    }
    for column in system_columns {
        anyhow::ensure!(
            names.insert(column.name.as_ref()),
            "system column '{}' conflicts with a parser plugin output column",
            column.name
        );
    }
    Ok(())
}

fn default_dlq_schema() -> DatasetSchema {
    DatasetSchema::new(vec![
        SchemaColumn::new(
            "raw_base64".to_string(),
            arrow::datatypes::DataType::Utf8,
            false,
        ),
        SchemaColumn::new(
            "error_message".to_string(),
            arrow::datatypes::DataType::Utf8,
            false,
        ),
        SchemaColumn::new(
            "source_write_timestamp_ms".to_string(),
            arrow::datatypes::DataType::Int64,
            true,
        ),
    ])
}

struct TopicTableParserFactory {
    inner: Arc<dyn ParserFactory>,
}

impl ParserFactory for TopicTableParserFactory {
    fn create_session(self: Arc<Self>, memory_limit_bytes: usize) -> Box<dyn ParserSession> {
        Box::new(TopicTableParserSession {
            inner: Arc::clone(&self.inner).create_session(memory_limit_bytes),
        })
    }
}

struct TopicTableParserSession {
    inner: Box<dyn ParserSession>,
}

impl ParserSession for TopicTableParserSession {
    fn output_memory_bound(&self, messages: &[Message]) -> usize {
        self.inner.output_memory_bound(messages)
    }

    fn parse_into(
        &mut self,
        messages: Vec<Message>,
    ) -> anyhow::Result<(TableData, Option<TableData>)> {
        let table = messages
            .first()
            .and_then(|message| message.meta.topic.as_deref())
            .ok_or_else(|| anyhow::anyhow!("from_topic_name requires source topic metadata"))?;
        anyhow::ensure!(
            messages
                .iter()
                .all(|message| message.meta.topic.as_deref() == Some(table)),
            "from_topic_name parser delivery mixes messages from multiple topics"
        );
        let table: Arc<str> = Arc::from(table);
        let (mut main, mut dlq) = self.inner.parse_into(messages)?;
        main.table = Arc::clone(&table);
        if let Some(dlq) = &mut dlq {
            dlq.table = dlq_name(&table).into();
        }
        Ok((main, dlq))
    }
}

fn validate_primary_key(
    parser: &json_parser::JsonParserConfig,
    enabled: &SystemColumnsConfig,
    schema: &DatasetSchema,
    system_columns: &[DiscoveredSystemColumn],
) -> anyhow::Result<()> {
    let mut available = schema
        .columns
        .iter()
        .map(|column| (column.name.as_str(), column.nullable))
        .collect::<HashMap<_, _>>();
    for system in system_columns {
        anyhow::ensure!(
            enabled.enabled().any(|kind| kind == system.kind),
            "system primary-key column '{}' is not enabled",
            system.name,
        );
        anyhow::ensure!(
            available.insert(system.name.as_ref(), false).is_none(),
            "system column '{}' conflicts with a JSON output column",
            system.name,
        );
    }
    let mut unique = std::collections::HashSet::with_capacity(parser.keys.len());
    for key in &parser.keys {
        anyhow::ensure!(
            unique.insert(key),
            "json_parser.keys repeats column '{key}'"
        );
        let nullable = available.get(key.as_str()).ok_or_else(|| {
            anyhow::anyhow!("json_parser.keys column '{key}' is not produced by the parser")
        })?;
        anyhow::ensure!(
            !nullable,
            "json_parser.keys column '{key}' must be non-nullable"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// ParserEntry — dynamic { kind: { config } } dispatch, like SourceEntry/SinkEntry
// ---------------------------------------------------------------------------

/// Parser config entry: `parser: { <kind>: { ... } }` — exactly one key.
#[derive(Debug, Clone, Deserialize, serde::Serialize)]
pub struct ParserEntry {
    #[serde(flatten)]
    pub inner: HashMap<String, Value>,
}

impl ParserEntry {
    pub fn kind(&self) -> anyhow::Result<&str> {
        let keys: Vec<&str> = self.inner.keys().map(String::as_str).collect();
        match *keys.as_slice() {
            [single] => Ok(single),
            [] => anyhow::bail!(
                "parser: no parser key found (expected 'json_parser', 'schema_registry', 'raw_to_table', or 'benchmark_discard')"
            ),
            _ => anyhow::bail!("parser: expected exactly one parser key, got {keys:?}"),
        }
    }

    pub fn raw(&self) -> anyhow::Result<&Value> {
        let kind = self.kind()?;
        self.inner
            .get(kind)
            .ok_or_else(|| anyhow::anyhow!("parser: parser key '{kind}' missing from config"))
    }
}

#[cfg(test)]
mod tests;
