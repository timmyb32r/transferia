use std::collections::BTreeMap;
use std::sync::Arc;

use schemars::{schema_for, JsonSchema};
use serde::Serialize;
use serde_json::Value as JsonValue;
use serde_yaml::Value;

use crate::extension::{
    EndpointRole, ExtensionRegistry, InstallationRegistration, OnPremiseResolver, Transferia,
};
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

    fn apply_installations(&mut self, registry: &ExtensionRegistry) -> anyhow::Result<()> {
        for definition in &mut self.definitions {
            if let Some(endpoint) = &mut definition.source {
                apply_endpoint_installations(
                    definition.key,
                    EndpointRole::Source,
                    endpoint,
                    registry,
                )?;
            }
            if let Some(endpoint) = &mut definition.sink {
                apply_endpoint_installations(
                    definition.key,
                    EndpointRole::Sink,
                    endpoint,
                    registry,
                )?;
            }
        }
        Ok(())
    }
}

#[derive(JsonSchema)]
struct EmptyConfig {}

pub fn build_provider_catalog(
    metrics_registry: &Arc<MetricsRegistry>,
) -> anyhow::Result<ProviderCatalog> {
    build_provider_catalog_with(&Transferia::public()?, metrics_registry)
}

pub fn build_provider_catalog_with(
    transferia: &Transferia,
    metrics_registry: &Arc<MetricsRegistry>,
) -> anyhow::Result<ProviderCatalog> {
    let mut catalog = ProviderCatalog::new();

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
            )?
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

    catalog.apply_installations(transferia.registry())?;
    Ok(catalog)
}

pub fn register_builtin_installations(registry: &mut ExtensionRegistry) -> anyhow::Result<()> {
    register_on_premise(
        registry,
        "postgres",
        EndpointRole::Source,
        &["host", "port", "trusted_plaintext", "tls_ca_file"],
        serde_json::json!({
            "host": { "type": "string", "title": "Host" },
            "port": { "type": "integer", "title": "Port", "minimum": 1, "maximum": 65535 },
            "trusted_plaintext": { "type": "boolean", "title": "Trusted plaintext" },
            "tls_ca_file": { "anyOf": [{ "type": "string" }, { "type": "null" }], "title": "TLS CA file" }
        }),
        &["host", "port", "trusted_plaintext"],
        serde_json::json!({ "host": "", "port": 5432, "trusted_plaintext": true, "tls_ca_file": null }),
    )?;
    register_on_premise(
        registry,
        "postgres",
        EndpointRole::Sink,
        &["host", "port", "trusted_plaintext", "tls_ca_file"],
        serde_json::json!({
            "host": { "type": "string", "title": "Host" },
            "port": { "type": "integer", "title": "Port", "minimum": 1, "maximum": 65535 },
            "trusted_plaintext": { "type": "boolean", "title": "Trusted plaintext" },
            "tls_ca_file": { "anyOf": [{ "type": "string" }, { "type": "null" }], "title": "TLS CA file" }
        }),
        &["host", "port", "trusted_plaintext"],
        serde_json::json!({ "host": "", "port": 5432, "trusted_plaintext": true, "tls_ca_file": null }),
    )?;
    register_on_premise(
        registry,
        "clickhouse",
        EndpointRole::Source,
        &["hosts", "port", "trusted_plaintext", "tls_ca_file"],
        serde_json::json!({
            "hosts": { "type": "array", "title": "Hosts", "items": { "type": "string" }, "x-ui": { "initial_items": 1 } },
            "port": { "type": "integer", "title": "Port", "description": "Native port", "minimum": 1, "maximum": 65535 },
            "trusted_plaintext": { "type": "boolean", "title": "Trusted plaintext" },
            "tls_ca_file": { "anyOf": [{ "type": "string" }, { "type": "null" }], "title": "TLS CA file" }
        }),
        &["hosts", "port", "trusted_plaintext"],
        serde_json::json!({ "hosts": [""], "port": crate::providers::clickhouse::DEFAULT_NATIVE_PORT, "trusted_plaintext": true, "tls_ca_file": null }),
    )?;
    register_on_premise(
        registry,
        "clickhouse",
        EndpointRole::Sink,
        &["hosts", "port", "trusted_plaintext", "tls_ca_file"],
        serde_json::json!({
            "hosts": { "type": "array", "title": "Hosts", "items": { "type": "string" }, "x-ui": { "initial_items": 1 } },
            "port": { "type": "integer", "title": "Port", "description": "Native port", "minimum": 1, "maximum": 65535 },
            "trusted_plaintext": { "type": "boolean", "title": "Trusted plaintext" },
            "tls_ca_file": { "anyOf": [{ "type": "string" }, { "type": "null" }], "title": "TLS CA file" }
        }),
        &["hosts", "port", "trusted_plaintext"],
        serde_json::json!({ "hosts": [""], "port": crate::providers::clickhouse::DEFAULT_NATIVE_PORT, "trusted_plaintext": true, "tls_ca_file": null }),
    )?;
    register_on_premise(
        registry,
        "ydb_topic",
        EndpointRole::Source,
        &["host", "port", "auth", "trusted_plaintext"],
        serde_json::json!({
            "host": { "type": "string", "title": "Host" },
            "port": { "type": "integer", "title": "Port", "minimum": 1, "maximum": 65535 },
            "auth": {
                "title": "Authentication",
                "oneOf": [
                    { "type": "object", "title": "Token", "properties": { "type": { "const": "token" }, "token": { "type": "string", "x-ui": { "widget": "password" } } }, "required": ["type", "token"] },
                    { "type": "object", "title": "Token file", "properties": { "type": { "const": "token_file" }, "token_file": { "type": "string" } }, "required": ["type", "token_file"] }
                ]
            },
            "trusted_plaintext": { "type": "boolean", "x-ui": { "widget": "hidden" } }
        }),
        &["host", "port", "auth", "trusted_plaintext"],
        serde_json::json!({ "host": "", "port": 2135, "auth": { "type": "token", "token": "" }, "trusted_plaintext": true }),
    )?;
    register_on_premise(
        registry,
        "ydb_topic",
        EndpointRole::Sink,
        &["host", "port", "auth", "trusted_plaintext"],
        serde_json::json!({
            "host": { "type": "string", "title": "Host" },
            "port": { "type": "integer", "title": "Port", "minimum": 1, "maximum": 65535 },
            "auth": {
                "type": "object",
                "title": "Authentication",
                "properties": {
                    "type": { "const": "access_token" },
                    "token": { "anyOf": [{ "type": "string", "x-ui": { "widget": "password" } }, { "type": "null" }] },
                    "token_file": { "anyOf": [{ "type": "string" }, { "type": "null" }] }
                },
                "required": ["type"]
            },
            "trusted_plaintext": { "type": "boolean", "title": "Trusted plaintext" }
        }),
        &["host", "port", "auth", "trusted_plaintext"],
        serde_json::json!({ "host": "", "port": 2135, "auth": { "type": "access_token", "token": "", "token_file": null }, "trusted_plaintext": true }),
    )?;
    register_on_premise(
        registry,
        "ytsaurus",
        EndpointRole::Source,
        &["host", "port", "token", "trusted_plaintext"],
        ytsaurus_on_premise_schema(),
        &["host", "port", "trusted_plaintext"],
        serde_json::json!({ "host": "", "port": 8000, "token": null, "trusted_plaintext": true }),
    )?;
    register_on_premise(
        registry,
        "ytsaurus",
        EndpointRole::Sink,
        &["host", "port", "token", "trusted_plaintext"],
        ytsaurus_on_premise_schema(),
        &["host", "port", "trusted_plaintext"],
        serde_json::json!({ "host": "", "port": 8000, "token": null, "trusted_plaintext": true }),
    )?;
    Ok(())
}

fn ytsaurus_on_premise_schema() -> JsonValue {
    serde_json::json!({
        "host": { "type": "string", "title": "Host" },
        "port": { "type": "integer", "title": "Port", "minimum": 1, "maximum": 65535 },
        "token": { "anyOf": [{ "type": "string", "x-ui": { "widget": "password" } }, { "type": "null" }], "title": "Token" },
        "trusted_plaintext": { "type": "boolean", "title": "Trusted plaintext" }
    })
}

fn register_on_premise(
    registry: &mut ExtensionRegistry,
    provider: &'static str,
    role: EndpointRole,
    replaces: &'static [&'static str],
    properties: JsonValue,
    required: &'static [&'static str],
    initial_fields: JsonValue,
) -> anyhow::Result<()> {
    let mut initial = initial_fields;
    initial
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("on-premise initial fields must be an object"))?
        .insert(
            "type".to_owned(),
            JsonValue::String("on_premise".to_owned()),
        );
    let JsonValue::Object(mut variant_properties) = properties else {
        anyhow::bail!("on-premise properties must be an object");
    };
    variant_properties.insert(
        "type".to_owned(),
        serde_json::json!({ "const": "on_premise" }),
    );
    let mut variant_required = required
        .iter()
        .map(|field| JsonValue::String((*field).to_owned()))
        .collect::<Vec<_>>();
    variant_required.push(JsonValue::String("type".to_owned()));
    registry.register_installation(InstallationRegistration {
        provider,
        role,
        kind: "on_premise",
        title: "On-premise",
        schema: serde_json::json!({
            "type": "object",
            "title": "On-premise",
            "properties": variant_properties,
            "required": variant_required
        }),
        initial,
        replaces,
        preferred: false,
        resolver: Arc::new(OnPremiseResolver),
    })?;
    Ok(())
}

fn apply_endpoint_installations(
    provider: &'static str,
    role: EndpointRole,
    endpoint: &mut EndpointDefinition,
    registry: &ExtensionRegistry,
) -> anyhow::Result<()> {
    let registrations = registry.installations_for(provider, role);
    if registrations.is_empty() {
        return Ok(());
    }
    let replaced = registrations
        .iter()
        .flat_map(|registration| registration.replaces.iter().copied())
        .collect::<std::collections::BTreeSet<_>>();
    let schema = endpoint
        .schema
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("{provider} endpoint schema must be an object"))?;
    let properties = schema
        .get_mut("properties")
        .and_then(JsonValue::as_object_mut)
        .ok_or_else(|| anyhow::anyhow!("{provider} endpoint schema has no object properties"))?;
    let installation_schema = serde_json::json!({
        "title": "Installation type",
        "oneOf": registrations.iter().map(|registration| {
            let mut schema = registration.schema.clone();
            if let Some(object) = schema.as_object_mut() {
                object.insert("title".to_owned(), JsonValue::String(registration.title.to_owned()));
                let variant_properties = object
                    .entry("properties")
                    .or_insert_with(|| serde_json::json!({}));
                if let Some(variant_properties) = variant_properties.as_object_mut() {
                    variant_properties.insert("type".to_owned(), serde_json::json!({ "const": registration.kind }));
                }
                let required = object
                    .entry("required")
                    .or_insert_with(|| serde_json::json!([]));
                if let Some(required) = required.as_array_mut() {
                    if !required.iter().any(|field| field == "type") {
                        required.push(JsonValue::String("type".to_owned()));
                    }
                }
            }
            schema
        }).collect::<Vec<_>>()
    });
    let endpoint_properties = std::mem::take(properties);
    properties.insert("installation".to_owned(), installation_schema);
    properties.extend(
        endpoint_properties
            .into_iter()
            .filter(|(field, _)| !replaced.contains(field.as_str())),
    );
    let required = schema
        .entry("required")
        .or_insert_with(|| serde_json::json!([]))
        .as_array_mut()
        .ok_or_else(|| anyhow::anyhow!("{provider} schema required must be an array"))?;
    required.retain(|field| field.as_str().is_none_or(|field| !replaced.contains(field)));
    if !required.iter().any(|field| field == "installation") {
        required.push(JsonValue::String("installation".to_owned()));
    }
    let initial = endpoint
        .initial
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("{provider} endpoint initial value must be an object"))?;
    for field in &replaced {
        initial.remove(*field);
    }
    let selected = registrations
        .iter()
        .rev()
        .find(|registration| registration.preferred)
        .copied()
        .unwrap_or(registrations[0]);
    initial.insert("installation".to_owned(), selected.initial.clone());
    Ok(())
}

#[cfg(test)]
#[path = "tests/catalog.rs"]
mod tests;
