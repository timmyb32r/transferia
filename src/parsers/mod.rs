pub mod benchmark_discard;
pub mod config;
pub mod json_parser;
mod native_source;

use std::collections::HashMap;

use alloc::sync::Arc;
use serde::Deserialize;
use serde_yaml::Value;

use crate::core::data::message::Message;
use crate::core::data::schema::{DatasetSchema, SchemaColumn};
use crate::core::data::table_data::{dlq_name, TableData};
use crate::core::delivery::{
    DatasetRole, DeliveryDiscovery, DeliveryDiscoveryRequest, DiscoveredDataset,
    DiscoveredSystemColumn, SchemaOrigin, SourceTopology,
};

pub use config::{CommonParserConfig, ParserConfig, SystemColumnsConfig, TableNaming};

/// Common parser interface. Every parser converts raw [`Message`]s into
/// Arrow [`TableData`] (valid + optional DLQ).
pub trait ParserFactory: Send + Sync {
    fn create_session(self: Arc<Self>, memory_limit_bytes: usize) -> Box<dyn ParserSession>;
}

/// Mutable parser state owned by exactly one partition parser thread.
pub trait ParserSession: Send {
    /// Conservative parser/Arrow/DLQ allocation estimate used for transform
    /// admission before builders allocate. The pipeline accounts the exact
    /// materialized output afterwards; an estimate is never a correctness gate.
    fn output_memory_bound(&self, messages: &[Message]) -> usize;

    fn parse_into(
        &mut self,
        messages: Vec<Message>,
    ) -> anyhow::Result<(TableData, Option<TableData>)>;
}

/// A parser compiled once from source configuration and shared by all partition pipelines.
/// It is the single source of truth for the parser instance and its sink-facing schemas.
pub struct ParserPlan {
    parser: Arc<dyn ParserFactory>,
    table: Arc<str>,
    dataset_schema: DatasetSchema,
    parses_rows: bool,
    discovered_system_columns: Vec<DiscoveredSystemColumn>,
    primary_key: Arc<[String]>,
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
        })
    }

    pub fn from_config(config: &ParserConfig, topic_path: &str) -> anyhow::Result<Self> {
        config.common.system_columns.validate()?;
        let table: Arc<str> = config.resolve_table_name(topic_path)?.into();
        let kind = config.parser.kind()?;
        let (parser, dataset_schema, parses_rows, discovered_system_columns, primary_key) =
            match kind {
                "json_parser" => {
                    let parser_config: json_parser::JsonParserConfig =
                        serde_yaml::from_value(config.parser.raw()?.clone())?;
                    let schema = parser_config.to_dataset_schema()?;
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
                        &parser_config,
                        &config.common.system_columns,
                        &schema,
                        &discovered_system_columns,
                    )?;
                    let parser = Arc::new(json_parser::JsonParser::new(
                        &parser_config,
                        &config.common.system_columns,
                        Arc::clone(&table),
                    )?) as Arc<dyn ParserFactory>;
                    (
                        parser,
                        schema,
                        true,
                        discovered_system_columns,
                        Arc::from(parser_config.keys),
                    )
                }
                "benchmark_discard" => {
                    let _: benchmark_discard::BenchmarkDiscardConfig =
                        serde_yaml::from_value(config.parser.raw()?.clone())?;
                    let parser = Arc::new(benchmark_discard::BenchmarkDiscardParser::new(
                        Arc::clone(&table),
                    )) as Arc<dyn ParserFactory>;
                    (
                        parser,
                        DatasetSchema::default(),
                        false,
                        Vec::new(),
                        Arc::from([]),
                    )
                }
                other => anyhow::bail!(
                    "unknown parser '{other}'; supported parsers: json_parser, benchmark_discard"
                ),
            };
        Ok(Self {
            parser,
            table,
            dataset_schema,
            parses_rows,
            discovered_system_columns,
            primary_key,
        })
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
        let mut columns = vec![
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
        ];
        if keep_system_columns {
            columns.extend(self.discovered_system_columns.iter().map(|column| {
                SchemaColumn::new(column.name.to_string(), column.kind.data_type(), false)
            }));
        }
        DatasetSchema::new(columns)
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
                "parser: no parser key found (expected 'json_parser' or 'benchmark_discard')"
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
