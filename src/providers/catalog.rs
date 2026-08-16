use std::collections::BTreeMap;
use std::sync::Arc;

use schemars::{schema_for, JsonSchema};
use serde::de::DeserializeOwned;
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
    definition: Option<EndpointDefinition>,
    factory: SourceFactory,
}

struct SinkRegistration {
    definition: Option<EndpointDefinition>,
    factory: SinkFactory,
}

struct ProviderRegistration {
    key: &'static str,
    title: &'static str,
    source: Option<SourceRegistration>,
    sink: Option<SinkRegistration>,

    compile_definition: bool,
}

#[derive(Clone, Copy)]
struct ProviderRoleDescriptor {
    installation: Option<InstallationContract>,
}

struct ProviderDescriptor {
    key: &'static str,
    title: &'static str,
    source: Option<ProviderRoleDescriptor>,
    sink: Option<ProviderRoleDescriptor>,
}

const LOGBROKER_ROLE: Option<ProviderRoleDescriptor> = Some(ProviderRoleDescriptor {
    installation: Some(InstallationContract {
        output_fields: &["host", "port", "auth", "trusted_plaintext"],
        required_output_fields: &["host", "port", "auth", "trusted_plaintext"],
    }),
});
const POSTGRES_ROLE: Option<ProviderRoleDescriptor> = Some(ProviderRoleDescriptor {
    installation: Some(InstallationContract {
        output_fields: &["host", "port", "trusted_plaintext", "tls_ca_file"],
        required_output_fields: &["host", "port", "trusted_plaintext"],
    }),
});
const CLICKHOUSE_ROLE: Option<ProviderRoleDescriptor> = Some(ProviderRoleDescriptor {
    installation: Some(InstallationContract {
        output_fields: &["hosts", "port", "trusted_plaintext", "tls_ca_file"],
        required_output_fields: &["hosts", "port", "trusted_plaintext"],
    }),
});
const YTSAURUS_ROLE: Option<ProviderRoleDescriptor> = Some(ProviderRoleDescriptor {
    installation: Some(InstallationContract {
        output_fields: &["host", "port", "token", "trusted_plaintext"],
        required_output_fields: &["host", "port", "trusted_plaintext"],
    }),
});
const PLAIN: Option<ProviderRoleDescriptor> = Some(ProviderRoleDescriptor { installation: None });

static PROVIDERS: &[ProviderDescriptor] = &[
    ProviderDescriptor {
        key: "logbroker",
        title: "Logbroker",
        source: LOGBROKER_ROLE,
        sink: LOGBROKER_ROLE,
    },
    ProviderDescriptor {
        key: "postgres",
        title: "PostgreSQL",
        source: POSTGRES_ROLE,
        sink: POSTGRES_ROLE,
    },
    ProviderDescriptor {
        key: "clickhouse",
        title: "ClickHouse",
        source: CLICKHOUSE_ROLE,
        sink: CLICKHOUSE_ROLE,
    },
    ProviderDescriptor {
        key: "s3",
        title: "S3",
        source: PLAIN,
        sink: PLAIN,
    },
    ProviderDescriptor {
        key: "ytsaurus",
        title: "YTsaurus",
        source: YTSAURUS_ROLE,
        sink: YTSAURUS_ROLE,
    },
    ProviderDescriptor {
        key: "discard",
        title: "Discard (benchmark)",
        source: None,
        sink: PLAIN,
    },
];

struct EndpointSpec {
    definition: EndpointDefinition,
}

impl EndpointSpec {
    fn new<C: JsonSchema>(
        initial: JsonValue,
        delivery_modes: Vec<DeliveryMode>,
        partitioned: bool,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            definition: EndpointDefinition {
                schema: serde_json::to_value(schema_for!(C))?,
                initial,
                delivery_modes,
                partitioned,
            },
        })
    }
}

impl ProviderRegistration {
    fn new(key: &'static str, compile_definition: bool) -> anyhow::Result<Self> {
        let descriptor = provider_descriptor(key)
            .ok_or_else(|| anyhow::anyhow!("unknown provider descriptor '{key}'"))?;
        Ok(Self {
            key: descriptor.key,
            title: descriptor.title,
            source: None,
            sink: None,
            compile_definition,
        })
    }

    fn source<C, F, I>(
        mut self,
        delivery_modes: Vec<DeliveryMode>,
        partitioned: bool,
        initial: I,
        factory: F,
    ) -> anyhow::Result<Self>
    where
        C: DeserializeOwned + JsonSchema + 'static,
        F: Fn(C) -> anyhow::Result<Box<dyn SourceProvider>> + Send + Sync + 'static,
        I: FnOnce() -> JsonValue,
    {
        let definition = self
            .compile_definition
            .then(|| EndpointSpec::new::<C>(initial(), delivery_modes, partitioned))
            .transpose()?
            .map(|spec| spec.definition);
        self.source = Some(SourceRegistration {
            definition,
            factory: Box::new(move |raw| {
                let config = serde_yaml::from_value(raw)
                    .map_err(|error| anyhow::anyhow!("invalid source configuration: {error}"))?;
                factory(config)
            }),
        });
        Ok(self)
    }

    fn sink<C, F, I>(mut self, initial: I, factory: F) -> anyhow::Result<Self>
    where
        C: DeserializeOwned + JsonSchema + 'static,
        F: Fn(C) -> anyhow::Result<Box<dyn SinkProvider>> + Send + Sync + 'static,
        I: FnOnce() -> JsonValue,
    {
        let definition = self
            .compile_definition
            .then(|| EndpointSpec::new::<C>(initial(), Vec::new(), false))
            .transpose()?
            .map(|spec| spec.definition);
        self.sink = Some(SinkRegistration {
            definition,
            factory: Box::new(move |raw| {
                let config = serde_yaml::from_value(raw)
                    .map_err(|error| anyhow::anyhow!("invalid sink configuration: {error}"))?;
                factory(config)
            }),
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
        let descriptor = provider_descriptor(registration.key).ok_or_else(|| {
            anyhow::anyhow!("unknown provider '{}'; no descriptor", registration.key)
        })?;
        anyhow::ensure!(
            registration.source.is_some() || registration.sink.is_some(),
            "provider '{}' has neither a source nor a sink registration",
            registration.key
        );
        anyhow::ensure!(
            registration.source.is_some() == descriptor.source.is_some()
                && registration.sink.is_some() == descriptor.sink.is_some(),
            "provider '{}' runtime roles do not match its descriptor",
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

        let source = registration.source.and_then(|source| {
            self.sources.insert(registration.key, source.factory);
            source.definition
        });
        let sink = registration.sink.and_then(|sink| {
            self.sinks.insert(registration.key, sink.factory);
            sink.definition
        });
        if registration.compile_definition {
            self.definitions.push(ProviderDefinition {
                key: registration.key,
                title: registration.title,
                source,
                sink,
            });
        }
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

#[derive(Clone, Copy)]
pub(crate) struct InstallationContract {
    pub output_fields: &'static [&'static str],

    pub required_output_fields: &'static [&'static str],
}

pub(crate) fn installation_contract(
    provider: &str,
    role: EndpointRole,
) -> Option<InstallationContract> {
    provider_role(provider, role)?.installation
}

fn provider_descriptor(key: &str) -> Option<&'static ProviderDescriptor> {
    PROVIDERS.iter().find(|provider| provider.key == key)
}

fn provider_role(provider: &str, role: EndpointRole) -> Option<ProviderRoleDescriptor> {
    let descriptor = provider_descriptor(provider)?;
    match role {
        EndpointRole::Source => descriptor.source,
        EndpointRole::Sink => descriptor.sink,
    }
}

pub(crate) fn provider_roles() -> impl Iterator<Item = (&'static str, EndpointRole)> {
    PROVIDERS.iter().flat_map(|provider| {
        [
            provider
                .source
                .map(|_| (provider.key, EndpointRole::Source)),
            provider.sink.map(|_| (provider.key, EndpointRole::Sink)),
        ]
        .into_iter()
        .flatten()
    })
}

pub(crate) fn provider_contracts() -> JsonValue {
    let role = |descriptor: Option<ProviderRoleDescriptor>| {
        descriptor.map(|descriptor| {
            descriptor.installation.map_or_else(
                || serde_json::json!({ "installation": null }),
                |contract| {
                    serde_json::json!({
                        "installation": {
                            "output_fields": contract.output_fields,
                            "required_output_fields": contract.required_output_fields,
                        }
                    })
                },
            )
        })
    };
    JsonValue::Array(
        PROVIDERS
            .iter()
            .map(|provider| {
                serde_json::json!({
                    "key": provider.key,
                    "title": provider.title,
                    "source": role(provider.source),
                    "sink": role(provider.sink),
                })
            })
            .collect(),
    )
}

pub(crate) fn provider_supports_role(provider: &str, role: EndpointRole) -> bool {
    provider_role(provider, role).is_some()
}

pub(crate) fn validate_extension_registry(registry: &ExtensionRegistry) -> anyhow::Result<()> {
    let mut preferred = BTreeMap::<(&str, EndpointRole), &str>::new();
    for (provider, role, kind) in registry.installation_keys() {
        anyhow::ensure!(
            installation_contract(provider, role).is_some(),
            "installation '{provider}.{kind}.{role:?}' targets an unknown provider role"
        );
        let registration = registry
            .installations_for(provider, role)
            .into_iter()
            .find(|registration| registration.kind == kind)
            .ok_or_else(|| {
                anyhow::anyhow!("compiled installation key has no matching registration")
            })?;
        validate_dynamic_option_references(&registration.schema, registry)?;
        if registration.preferred {
            anyhow::ensure!(
                preferred.insert((provider, role), kind).is_none(),
                "provider '{provider}' has more than one preferred {role:?} installation"
            );
        }
    }
    for (provider, role) in provider_roles() {
        if installation_contract(provider, role).is_none() {
            continue;
        }
        let registrations = registry.installations_for(provider, role);
        anyhow::ensure!(
            !registrations.is_empty(),
            "provider '{provider}' has no {role:?} installation"
        );
        anyhow::ensure!(
            registrations.len() == 1 || preferred.contains_key(&(provider, role)),
            "provider '{provider}' must declare exactly one preferred {role:?} installation when multiple variants exist"
        );
    }
    Ok(())
}

fn validate_dynamic_option_references(
    value: &JsonValue,
    registry: &ExtensionRegistry,
) -> anyhow::Result<()> {
    match value {
        JsonValue::Array(values) => {
            for value in values {
                validate_dynamic_option_references(value, registry)?;
            }
        }
        JsonValue::Object(object) => {
            if let Some(key) = object.get("dynamic_options").and_then(JsonValue::as_str) {
                anyhow::ensure!(
                    registry.option_keys().any(|registered| registered == key),
                    "installation schema references unknown dynamic option source '{key}'"
                );
            }
            for value in object.values() {
                validate_dynamic_option_references(value, registry)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[derive(serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
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
    let mut catalog = build_base_provider_catalog(metrics_registry, false)?;
    catalog.definitions = transferia.composition().provider_definitions().to_vec();
    Ok(catalog)
}

pub(crate) fn compile_provider_definitions(
    registry: &ExtensionRegistry,
) -> anyhow::Result<Vec<ProviderDefinition>> {
    let metrics = Arc::new(MetricsRegistry::new());
    let mut catalog = build_base_provider_catalog(&metrics, true)?;
    catalog.apply_installations(registry)?;
    Ok(catalog.definitions)
}

fn build_base_provider_catalog(
    metrics_registry: &Arc<MetricsRegistry>,
    compile_definitions: bool,
) -> anyhow::Result<ProviderCatalog> {
    let mut catalog = ProviderCatalog::new();

    catalog.register(
        ProviderRegistration::new("logbroker", compile_definitions)?
            .source::<crate::providers::logbroker::src_stream::LogbrokerSourceConfig, _, _>(
                vec![DeliveryMode::Stream],
                true,
                || {
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
                    })
                },
                {
                    let metrics_registry = Arc::clone(metrics_registry);
                    move |value| {
                        crate::providers::logbroker::build_source_provider(
                            value,
                            Arc::clone(&metrics_registry),
                        )
                    }
                },
            )?
            .sink::<crate::providers::logbroker::sink::LogbrokerSinkConfig, _, _>(
                || {
                    serde_json::json!({
                        "host": "",
                        "port": 2135,
                        "topic_path": "",
                        "producer_id": "",
                        "partition_id": null,
                        "auth": { "type": "token", "token": "" },
                        "driver": "ydb",
                        "trusted_plaintext": true
                    })
                },
                crate::providers::logbroker::build_sink_provider,
            )?,
    )?;

    catalog.register(
        ProviderRegistration::new("postgres", compile_definitions)?
            .source::<crate::providers::postgres::src_batch::PostgresSourceConfig, _, _>(
                vec![DeliveryMode::Batch],
                false,
                || {
                    serde_json::json!({
                        "host": "",
                        "port": 5432,
                        "database": "",
                        "username": "",
                        "password": "",
                        "trusted_plaintext": true,
                        "tables": [{ "schema": "", "name": "" }],
                        "batch_rows": 65536
                    })
                },
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
            .sink::<crate::providers::postgres::sink::PostgresSinkConfig, _, _>(
                || {
                    serde_json::json!({
                        "host": "",
                        "port": 5432,
                        "database": "",
                        "username": "",
                        "password": "",
                        "trusted_plaintext": true,
                        "create_tables": true
                    })
                },
                |value| {
                    Ok(Box::new(
                        crate::providers::postgres::PostgresSinkProvider::from_config(value)?,
                    ))
                },
            )?,
    )?;

    catalog.register(
        ProviderRegistration::new("clickhouse", compile_definitions)?
            .source::<crate::providers::clickhouse::src_batch::ClickHouseSourceConfig, _, _>(
                vec![DeliveryMode::Batch],
                false,
                || {
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
                    })
                },
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
            .sink::<crate::providers::clickhouse::ClickHouseSinkConfig, _, _>(
                || {
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
                    })
                },
                |value| {
                    Ok(Box::new(
                        crate::providers::clickhouse::ClickHouseSinkProvider::from_config(value)?,
                    ))
                },
            )?,
    )?;

    catalog.register(
        ProviderRegistration::new("s3", compile_definitions)?
            .source::<crate::providers::s3::src_batch::S3SourceConfig, _, _>(
                vec![DeliveryMode::Batch],
                false,
                || serde_json::json!({
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
            .sink::<crate::providers::s3::sink::S3SinkConfig, _, _>(
                || serde_json::json!({
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
        ProviderRegistration::new("ytsaurus", compile_definitions)?
            .source::<crate::providers::ytsaurus::YTsaurusSourceConfig, _, _>(
                vec![DeliveryMode::Batch],
                false,
                || {
                    serde_json::json!({
                        "host": "",
                        "port": 8000,
                        "trusted_plaintext": true,
                        "timeout_ms": 30000,
                        "tables": [{ "path": "", "output_name": "" }],
                        "batch_rows": 65536
                    })
                },
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
            .sink::<crate::providers::ytsaurus::YTsaurusSinkConfig, _, _>(
                || {
                    serde_json::json!({
                        "host": "",
                        "port": 8000,
                        "trusted_plaintext": true,
                        "timeout_ms": 30000,
                        "tables": [{ "dataset": "", "path": "" }],
                        "replace_tables": true,
                        "format": "arrow"
                    })
                },
                |value| {
                    Ok(Box::new(
                        crate::providers::ytsaurus::YTsaurusSinkProvider::from_config(value)?,
                    ))
                },
            )?,
    )?;

    catalog.register(
        ProviderRegistration::new("discard", compile_definitions)?.sink::<EmptyConfig, _, _>(
            || serde_json::json!({}),
            |_config| {
                Ok(Box::new(
                    crate::providers::discard::provider::DiscardSinkProvider,
                ))
            },
        )?,
    )?;

    Ok(catalog)
}

pub fn register_builtin_installations(registry: &mut ExtensionRegistry) -> anyhow::Result<()> {
    register_on_premise(
        registry,
        "postgres",
        EndpointRole::Source,
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
        "logbroker",
        EndpointRole::Source,
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
        "logbroker",
        EndpointRole::Sink,
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
            "trusted_plaintext": { "type": "boolean", "title": "Trusted plaintext" }
        }),
        &["host", "port", "auth", "trusted_plaintext"],
        serde_json::json!({ "host": "", "port": 2135, "auth": { "type": "token", "token": "" }, "trusted_plaintext": true }),
    )?;
    register_on_premise(
        registry,
        "ytsaurus",
        EndpointRole::Source,
        ytsaurus_on_premise_schema(),
        &["host", "port", "trusted_plaintext"],
        serde_json::json!({ "host": "", "port": 8000, "token": null, "trusted_plaintext": true }),
    )?;
    register_on_premise(
        registry,
        "ytsaurus",
        EndpointRole::Sink,
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
            "required": variant_required,
            "additionalProperties": false
        }),
        initial,
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
    let replaced = installation_contract(provider, role)
        .ok_or_else(|| anyhow::anyhow!("provider '{provider}' does not support {role:?}"))?
        .output_fields
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    let schema = endpoint
        .schema
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("{provider} endpoint schema must be an object"))?;
    let properties = schema
        .get_mut("properties")
        .and_then(JsonValue::as_object_mut)
        .ok_or_else(|| anyhow::anyhow!("{provider} endpoint schema has no object properties"))?;
    for field in &replaced {
        anyhow::ensure!(
            properties.contains_key(*field),
            "{provider}.{role:?} installation contract declares unknown output field '{field}'"
        );
    }
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
    let selected = if registrations.len() == 1 {
        registrations[0]
    } else {
        registrations
            .iter()
            .find(|registration| registration.preferred)
            .copied()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "provider '{provider}' has no preferred {role:?} installation variant"
                )
            })?
    };
    initial.insert("installation".to_owned(), selected.initial.clone());
    Ok(())
}

#[cfg(test)]
#[path = "tests/catalog.rs"]
mod tests;
