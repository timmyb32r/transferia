use std::collections::BTreeMap;
use std::sync::Arc;

use schemars::{schema_for, JsonSchema};
use serde::Serialize;
use serde_json::Value as JsonValue;
use serde_yaml::Value;

use crate::metrics::MetricsRegistry;
use crate::providers::traits::{SinkProvider, SourceProvider};

type SourceFactory = Box<dyn Fn(Value) -> anyhow::Result<Box<dyn SourceProvider>> + Send + Sync>;
type SinkFactory = Box<dyn Fn(Value) -> anyhow::Result<Box<dyn SinkProvider>> + Send + Sync>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryMode {
    Batch,
    Stream,
}

#[derive(Clone, Debug, Serialize)]
pub struct EndpointDefinition {
    pub schema: JsonValue,
    pub initial: JsonValue,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub delivery_modes: Vec<DeliveryMode>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub partitioned: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProviderDefinition {
    pub key: &'static str,
    pub title: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<EndpointDefinition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sink: Option<EndpointDefinition>,
}

struct SourceRegistration {
    definition: EndpointDefinition,
    factory: SourceFactory,
}

struct SinkRegistration {
    definition: EndpointDefinition,
    factory: SinkFactory,
}

struct ProviderRegistration {
    key: &'static str,
    title: &'static str,
    source: Option<SourceRegistration>,
    sink: Option<SinkRegistration>,
}

impl ProviderRegistration {
    const fn new(key: &'static str, title: &'static str) -> Self {
        Self {
            key,
            title,
            source: None,
            sink: None,
        }
    }

    fn source<C, F>(
        mut self,
        delivery_modes: Vec<DeliveryMode>,
        partitioned: bool,
        initial: JsonValue,
        factory: F,
    ) -> anyhow::Result<Self>
    where
        C: JsonSchema,
        F: Fn(Value) -> anyhow::Result<Box<dyn SourceProvider>> + Send + Sync + 'static,
    {
        self.source = Some(SourceRegistration {
            definition: EndpointDefinition {
                schema: serde_json::to_value(schema_for!(C))?,
                initial,
                delivery_modes,
                partitioned,
            },
            factory: Box::new(factory),
        });
        Ok(self)
    }

    fn sink<C, F>(mut self, initial: JsonValue, factory: F) -> anyhow::Result<Self>
    where
        C: JsonSchema,
        F: Fn(Value) -> anyhow::Result<Box<dyn SinkProvider>> + Send + Sync + 'static,
    {
        self.sink = Some(SinkRegistration {
            definition: EndpointDefinition {
                schema: serde_json::to_value(schema_for!(C))?,
                initial,
                delivery_modes: Vec::new(),
                partitioned: false,
            },
            factory: Box::new(factory),
        });
        Ok(self)
    }
}

pub struct ProviderCatalog {
    definitions: Vec<ProviderDefinition>,
    sources: BTreeMap<&'static str, SourceFactory>,
    sinks: BTreeMap<&'static str, SinkFactory>,
}

impl ProviderCatalog {
    fn new() -> Self {
        Self {
            definitions: Vec::new(),
            sources: BTreeMap::new(),
            sinks: BTreeMap::new(),
        }
    }

    fn register(&mut self, registration: ProviderRegistration) -> anyhow::Result<()> {
        anyhow::ensure!(
            registration.source.is_some() || registration.sink.is_some(),
            "provider '{}' has neither a source nor a sink registration",
            registration.key
        );
        anyhow::ensure!(
            !self
                .definitions
                .iter()
                .any(|definition| definition.key == registration.key),
            "provider '{}' is registered more than once",
            registration.key
        );

        let source = registration.source.map(|source| {
            self.sources.insert(registration.key, source.factory);
            source.definition
        });
        let sink = registration.sink.map(|sink| {
            self.sinks.insert(registration.key, sink.factory);
            sink.definition
        });
        self.definitions.push(ProviderDefinition {
            key: registration.key,
            title: registration.title,
            source,
            sink,
        });
        Ok(())
    }

    #[must_use]
    pub fn definitions(&self) -> &[ProviderDefinition] {
        &self.definitions
    }

    pub fn build_source(&self, kind: &str, raw: Value) -> anyhow::Result<Box<dyn SourceProvider>> {
        self.sources.get(kind).map_or_else(
            || {
                anyhow::bail!(
                    "unknown source provider '{kind}'; registered: {:?}",
                    self.sources.keys().collect::<Vec<_>>()
                )
            },
            |factory| factory(raw),
        )
    }

    pub fn build_sink(&self, kind: &str, raw: Value) -> anyhow::Result<Box<dyn SinkProvider>> {
        self.sinks.get(kind).map_or_else(
            || {
                anyhow::bail!(
                    "unknown sink provider '{kind}'; registered: {:?}",
                    self.sinks.keys().collect::<Vec<_>>()
                )
            },
            |factory| factory(raw),
        )
    }
}

#[derive(JsonSchema)]
struct EmptyConfig {}

pub fn build_provider_catalog(
    metrics_registry: &Arc<MetricsRegistry>,
) -> anyhow::Result<ProviderCatalog> {
    let mut catalog = ProviderCatalog::new();

    catalog.register(
        ProviderRegistration::new("pqv1", "PQv1")
            .sink::<crate::providers::pqv1::config::PqV1SinkConfig, _>(
                serde_json::json!({
                    "host": "",
                    "port": 2135,
                    "topic_path": "",
                    "message_group_id": "",
                    "partition_group_id": 0,
                    "auth": { "type": "access_token", "token": "" },
                    "trusted_plaintext": true,
                    "network_timeout_ms": 30000
                }),
                |value| {
                    Ok(Box::new(
                        crate::providers::pqv1::PqV1SinkProvider::from_config(value)?,
                    ))
                },
            )?,
    )?;

    catalog.register(
        ProviderRegistration::new("ydb_topic", "Logbroker")
            .source::<crate::providers::ydb_topic::src_stream::YdbTopicSourceConfig, _>(
            vec![DeliveryMode::Stream],
            true,
            serde_json::json!({
                "host": "",
                "port": 2135,
                "topics": [{ "path": "", "partitions": [] }],
                "consumer_name": "",
                "auth": { "type": "token", "token": "" },
                "driver": "ydb",
                "trusted_plaintext": true,
                "allow_ttl_rewind": false,
                "parser": {},
                "read_buffer_bytes": 1_048_576
            }),
            {
                let metrics_registry = Arc::clone(metrics_registry);
                move |value| {
                    crate::providers::ydb_topic::build_source_provider(
                        value,
                        Arc::clone(&metrics_registry),
                    )
                }
            },
        )?,
    )?;

    catalog.register(
        ProviderRegistration::new("postgres", "PostgreSQL")
            .source::<crate::providers::postgres::src_batch::PostgresSourceConfig, _>(
                vec![DeliveryMode::Batch],
                false,
                serde_json::json!({
                    "host": "",
                    "port": 5432,
                    "database": "",
                    "username": "",
                    "password": "",
                    "trusted_plaintext": true,
                    "tables": [{ "schema": "", "name": "" }],
                    "batch_rows": 65536
                }),
                {
                    let metrics_registry = Arc::clone(metrics_registry);
                    move |value| {
                        Ok(Box::new(
                            crate::providers::postgres::PostgresSourceProvider::from_config(
                                value,
                                Arc::clone(&metrics_registry),
                            )?,
                        ))
                    }
                },
            )?
            .sink::<crate::providers::postgres::sink::PostgresSinkConfig, _>(
                serde_json::json!({
                    "host": "",
                    "port": 5432,
                    "database": "",
                    "username": "",
                    "password": "",
                    "trusted_plaintext": true,
                    "create_tables": true
                }),
                |value| {
                    Ok(Box::new(
                        crate::providers::postgres::PostgresSinkProvider::from_config(value)?,
                    ))
                },
            )?,
    )?;

    catalog.register(
        ProviderRegistration::new("clickhouse", "ClickHouse")
            .source::<crate::providers::clickhouse::src_batch::ClickHouseSourceConfig, _>(
                vec![DeliveryMode::Batch],
                false,
                serde_json::json!({
                    "hosts": [""],
                    "port": crate::providers::clickhouse::DEFAULT_NATIVE_PORT,
                    "trusted_plaintext": true,
                    "username": "",
                    "password": "",
                    "tables": [{
                        "database": "",
                        "name": "",
                        "output_name": "",
                        "order_by": [""]
                    }],
                    "batch_rows": 65536,
                    "connect_timeout_ms": 30000,
                    "request_timeout_ms": 30000
                }),
                {
                    let metrics_registry = Arc::clone(metrics_registry);
                    move |value| {
                        Ok(Box::new(
                            crate::providers::clickhouse::ClickHouseSourceProvider::from_config(
                                value,
                                Arc::clone(&metrics_registry),
                            )?,
                        ))
                    }
                },
            )?
            .sink::<crate::providers::clickhouse::ClickHouseSinkConfig, _>(
                serde_json::json!({
                    "hosts": [""],
                    "port": crate::providers::clickhouse::DEFAULT_NATIVE_PORT,
                    "trusted_plaintext": true,
                    "database": "",
                    "username": "",
                    "password": "",
                    "insert_target_rows": 100_000,
                    "insert_target_bytes": 67_108_864,
                    "flush_interval_ms": 100,
                    "retry_initial_ms": 50,
                    "retry_max_ms": 30000,
                    "connect_timeout_ms": 30000,
                    "request_timeout_ms": 30000
                }),
                |value| {
                    Ok(Box::new(
                        crate::providers::clickhouse::ClickHouseSinkProvider::from_config(value)?,
                    ))
                },
            )?,
    )?;

    catalog.register(
        ProviderRegistration::new("s3", "S3")
            .source::<crate::providers::s3::src_batch::S3SourceConfig, _>(
                vec![DeliveryMode::Batch],
                false,
                serde_json::json!({
                    "bucket": "",
                    "prefix": "",
                    "region": "",
                    "host": "",
                    "port": 4566,
                    "allow_http": true,
                    "credentials": { "access_key": "", "secret_key": "" },
                    "parser": {},
                    "timeout_ms": 30000
                }),
                {
                    let metrics_registry = Arc::clone(metrics_registry);
                    move |value| {
                        Ok(Box::new(
                            crate::providers::s3::S3SourceProvider::from_config(
                                value,
                                Arc::clone(&metrics_registry),
                            )?,
                        ))
                    }
                },
            )?
            .sink::<crate::providers::s3::sink::S3SinkConfig, _>(
                serde_json::json!({
                    "bucket": "",
                    "object_layout_version": 5,
                    "region": "",
                    "host": "",
                    "port": 4566,
                    "allow_http": true,
                    "credentials": { "access_key": "", "secret_key": "" },
                    "partitioning": { "type": "source" },
                    "rotation": { "max_rows": 10000, "max_bytes": "", "on_partition_path_change": "keep_epoch" },
                    "buffering": { "max_epoch_buffers": 32, "max_pending_upload_objects": 64, "max_buffered_bytes": "", "max_epoch_bytes": "" },
                    "upload": { "multipart_threshold": "", "part_size": "", "parallel_parts": 2, "max_in_flight_objects": 2, "operation_timeout": "" },
                    "retry": { "initial_backoff": "", "max_backoff": "", "max_attempts": 10 }
                }),
                |value| {
                    Ok(Box::new(
                        crate::providers::s3::sink::S3SinkProvider::from_config(value)?,
                    ))
                },
            )?,
    )?;

    catalog.register(
        ProviderRegistration::new("ytsaurus", "YTsaurus")
            .source::<crate::providers::ytsaurus::YTsaurusSourceConfig, _>(
                vec![DeliveryMode::Batch],
                false,
                serde_json::json!({
                    "host": "",
                    "port": 8000,
                    "trusted_plaintext": true,
                    "timeout_ms": 30000,
                    "tables": [{ "path": "", "output_name": "" }],
                    "batch_rows": 65536
                }),
                {
                    let metrics_registry = Arc::clone(metrics_registry);
                    move |value| {
                        Ok(Box::new(
                            crate::providers::ytsaurus::YTsaurusSourceProvider::from_config(
                                value,
                                Arc::clone(&metrics_registry),
                            )?,
                        ))
                    }
                },
            )?
            .sink::<crate::providers::ytsaurus::YTsaurusSinkConfig, _>(
                serde_json::json!({
                    "host": "",
                    "port": 8000,
                    "trusted_plaintext": true,
                    "timeout_ms": 30000,
                    "tables": [{ "dataset": "", "path": "" }],
                    "replace_tables": true,
                    "format": "arrow"
                }),
                |value| {
                    Ok(Box::new(
                        crate::providers::ytsaurus::YTsaurusSinkProvider::from_config(value)?,
                    ))
                },
            )?,
    )?;

    catalog.register(
        ProviderRegistration::new("discard", "Discard (benchmark)").sink::<EmptyConfig, _>(
            serde_json::json!({}),
            |value| {
                Ok(Box::new(
                    crate::providers::discard::provider::DiscardSinkProvider::from_config(value)?,
                ))
            },
        )?,
    )?;

    Ok(catalog)
}

#[cfg(test)]
#[path = "tests/catalog.rs"]
mod tests;
