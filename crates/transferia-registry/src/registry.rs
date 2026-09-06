use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use schemars::{schema_for, JsonSchema};
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;
use serde_yaml::Value;
use tokio_util::sync::CancellationToken;
use transferia_core::delivery::{DeliveryDiscovery, DeliveryDiscoveryRequest};
use transferia_core::TableData;
use transferia_delivery_contracts::metrics::MetricsRegistry;
use transferia_delivery_contracts::middleware::Middleware;
use transferia_delivery_contracts::semantics::RecordSemantics;

use crate::tuning::{validate_tuning_parameters_against_schema, TuningParameter};
use crate::ui_contract::{validate_endpoint_capabilities, validate_ui_dialect};
use crate::{
    ConnectionCheckResult, ConnectorDefinition, DeliveryMode, EndpointDefinition, EndpointRole,
    MiddlewareDefinition, SinkConnector, SourceConnector, TableIdentity,
};

type ConnectionChecker = Box<
    dyn Fn(Value) -> Pin<Box<dyn Future<Output = anyhow::Result<ConnectionCheckResult>> + Send>>
        + Send
        + Sync,
>;
type SourcePreviewer = Box<
    dyn Fn(
            Value,
            usize,
            CancellationToken,
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<SourcePreview>> + Send>>
        + Send
        + Sync,
>;
type SourceSchemaPreviewer = Box<
    dyn Fn(
            Value,
            DeliveryDiscoveryRequest,
            CancellationToken,
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<DeliveryDiscovery>> + Send>>
        + Send
        + Sync,
>;
/// Explicit preview limits. Connectors enforce byte admission while reading;
/// exceeding a limit fails the sample rather than silently truncating values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TableSampleLimits {
    /// Maximum rows requested by the sample query, never a post-read truncation.
    pub row_limit: usize,
    /// Source read and Arrow admission budget; exceeding it fails the request.
    pub max_bytes: usize,
    /// Server execution deadline. The request owner also applies its overall deadline.
    pub timeout_ms: usize,
}

impl TableSampleLimits {
    pub fn validate(self) -> anyhow::Result<()> {
        anyhow::ensure!(self.row_limit > 0, "row_limit must be positive");
        anyhow::ensure!(self.max_bytes > 0, "max_sample_bytes must be positive");
        anyhow::ensure!(self.timeout_ms > 0, "timeout_ms must be positive");
        Ok(())
    }

    pub fn check_bytes(self, bytes: usize) -> anyhow::Result<()> {
        anyhow::ensure!(bytes <= self.max_bytes,
            "source sample needs {bytes} bytes and exceeds max_sample_bytes ({} bytes); increase the sample byte budget or select fewer rows", self.max_bytes);
        Ok(())
    }
}

type SourceTableSampler = Box<
    dyn Fn(Value, TableIdentity, TableSampleLimits, CancellationToken)
        -> Pin<Box<dyn Future<Output = anyhow::Result<TableData>> + Send>>
        + Send + Sync,
>;
type SourceFactory = Box<dyn Fn(Value) -> anyhow::Result<Box<dyn SourceConnector>> + Send + Sync>;
type SinkFactory = Box<dyn Fn(Value) -> anyhow::Result<Box<dyn SinkConnector>> + Send + Sync>;
type MiddlewareFactory = Box<dyn Fn(Value) -> anyhow::Result<Box<dyn Middleware>> + Send + Sync>;
pub struct SourcePreview {
    pub payload: Vec<u8>,

    pub detection_payloads: Vec<Vec<u8>>,

    pub metadata: SourcePreviewMetadata,
}

pub struct SourcePreviewMetadata {
    pub topic: String,
    pub partition: i64,
    pub partition_session_id: i64,
    pub offset: i64,
    pub sequence_number: i64,
    pub created_at_ms: Option<i64>,
    pub written_at_ms: Option<i64>,
    pub producer_id: String,
    pub message_group_id: Option<String>,
    pub codec: String,
    pub compressed_size: usize,
    pub declared_uncompressed_size: Option<usize>,
    pub message_metadata: Vec<SourcePreviewMetadataItem>,
    pub write_session_metadata: BTreeMap<String, String>,
}

pub struct SourcePreviewMetadataItem {
    pub key: String,
    pub value: Vec<u8>,
}

/// Complete executable composition consumed by delivery preparation.
///
/// Implementations own installation resolution and component registration;
/// delivery orchestration depends only on this connector-neutral port.
pub trait Composition: Send + Sync {
    fn fingerprint(&self) -> &str;

    fn definitions(&self) -> &[ConnectorDefinition];

    fn build_registry(&self, metrics: &Arc<MetricsRegistry>) -> anyhow::Result<Registry>;

    fn resolve_many(
        &self,
        connector: &str,
        role: EndpointRole,
        raw: Value,
        cancellation: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<Value>>> + Send + '_>>;
}

pub struct ComponentRegistration {
    key: &'static str,
    title: &'static str,
    source: Option<(EndpointDefinition, SourceFactory)>,
    sink: Option<(EndpointDefinition, SinkFactory)>,
    source_checker: Option<ConnectionChecker>,
    sink_checker: Option<ConnectionChecker>,
    source_previewer: Option<SourcePreviewer>,
    source_schema_previewer: Option<SourceSchemaPreviewer>,
    source_table_sampler: Option<SourceTableSampler>,
    source_tuning_parameters: Vec<TuningParameter>,
    sink_tuning_parameters: Vec<TuningParameter>,
}

pub struct MiddlewareRegistration {
    key: &'static str,
    definition: MiddlewareDefinition,
    factory: MiddlewareFactory,
}

impl MiddlewareRegistration {
    pub fn new<C, F, I>(
        key: &'static str,
        title: &'static str,
        initial: I,
        factory: F,
    ) -> anyhow::Result<Self>
    where
        C: DeserializeOwned + JsonSchema + 'static,
        F: Fn(C) -> anyhow::Result<Box<dyn Middleware>> + Send + Sync + 'static,
        I: FnOnce() -> JsonValue,
    {
        anyhow::ensure!(!key.is_empty(), "middleware key must not be empty");
        anyhow::ensure!(
            !title.trim().is_empty(),
            "middleware '{key}' title must not be empty"
        );
        let initial = initial();
        let schema = serde_json::to_value(schema_for!(C))?;
        validate_ui_dialect(&schema)?;
        serde_json::from_value::<C>(initial.clone()).map_err(|error| {
            anyhow::anyhow!("invalid initial middleware configuration for '{key}': {error}")
        })?;
        Ok(Self {
            key,
            definition: MiddlewareDefinition {
                key,
                title,
                schema,
                initial,
            },
            factory: Box::new(move |raw| {
                let config = serde_yaml::from_value(raw).map_err(|error| {
                    anyhow::anyhow!("invalid middleware configuration: {error}")
                })?;
                factory(config)
            }),
        })
    }

}

impl ComponentRegistration {
    #[must_use]
    pub fn new(key: &'static str, title: &'static str) -> Self {
        Self {
            key,
            title,
            source: None,
            sink: None,
            source_checker: None,
            sink_checker: None,
            source_previewer: None,
            source_schema_previewer: None,
            source_table_sampler: None,
            source_tuning_parameters: Vec::new(),
            sink_tuning_parameters: Vec::new(),
        }
    }

    #[must_use]
    pub fn source_previewer<C, F, Fut>(mut self, previewer: F) -> Self
    where
        C: DeserializeOwned + Send + 'static,
        F: Fn(C, usize, CancellationToken) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<SourcePreview>> + Send + 'static,
    {
        self.source_previewer =
            Some(Box::new(
                move |raw, max_bytes, cancellation| match serde_yaml::from_value(raw) {
                    Ok(config) => Box::pin(previewer(config, max_bytes, cancellation)),
                    Err(error) => Box::pin(async move {
                        Err(anyhow::anyhow!("invalid source configuration: {error}"))
                    }),
                },
            ));
        self
    }

    /// Registers schema discovery that only depends on parser-related source
    /// fields. This keeps data-schema preview usable before connection and
    /// authentication settings are complete.
    #[must_use]
    pub fn source_schema_previewer<F, Fut>(mut self, previewer: F) -> Self
    where
        F: Fn(Value, DeliveryDiscoveryRequest, CancellationToken) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<DeliveryDiscovery>> + Send + 'static,
    {
        self.source_schema_previewer = Some(Box::new(move |raw, request, cancellation| {
            Box::pin(previewer(raw, request, cancellation))
        }));
        self
    }

    /// Bounded, read-only native rows for interactive previews. This capability
    /// must not start delivery workers or allocate replication resources.
    #[must_use]
    pub fn source_table_sampler<C, F, Fut>(mut self, sampler: F) -> Self
    where
        C: DeserializeOwned + Send + 'static,
        F: Fn(C, TableIdentity, TableSampleLimits, CancellationToken) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<TableData>> + Send + 'static,
    {
        self.source_table_sampler = Some(Box::new(move |raw, table, limits, cancellation| {
            match serde_yaml::from_value(raw) {
                Ok(config) => Box::pin(sampler(config, table, limits, cancellation)),
                Err(_) => Box::pin(async { anyhow::bail!("invalid source sample configuration") }),
            }
        }));
        self
    }

    pub fn source<C, F, I>(
        mut self,
        delivery_modes: Vec<DeliveryMode>,
        partitioned: bool,
        initial: I,
        factory: F,
    ) -> anyhow::Result<Self>
    where
        C: DeserializeOwned + JsonSchema + 'static,
        F: Fn(C) -> anyhow::Result<Box<dyn SourceConnector>> + Send + Sync + 'static,
        I: FnOnce() -> JsonValue,
    {
        let initial = initial();
        serde_json::from_value::<C>(initial.clone()).map_err(|error| {
            anyhow::anyhow!(
                "invalid initial source configuration for '{}': {error}",
                self.key
            )
        })?;
        let definition = endpoint_definition::<C>(
            initial,
            delivery_modes,
            vec![RecordSemantics::AppendOnly],
            partitioned,
        )?;
        self.source = Some((
            definition,
            Box::new(move |raw| {
                let config = serde_yaml::from_value(raw)
                    .map_err(|error| anyhow::anyhow!("invalid source configuration: {error}"))?;
                factory(config)
            }),
        ));
        Ok(self)
    }

    /// Registers a source whose UI draft is deliberately incomplete.
    ///
    /// Use this only when the product requires an explicit user selection
    /// before the runtime configuration can be decoded (for example, an
    /// unselected parser). Runtime construction still uses the strict `C`
    /// decoder and fails closed.
    pub fn source_draft<C, F, I>(
        mut self,
        delivery_modes: Vec<DeliveryMode>,
        partitioned: bool,
        initial: I,
        factory: F,
    ) -> anyhow::Result<Self>
    where
        C: DeserializeOwned + JsonSchema + 'static,
        F: Fn(C) -> anyhow::Result<Box<dyn SourceConnector>> + Send + Sync + 'static,
        I: FnOnce() -> JsonValue,
    {
        let definition = endpoint_definition::<C>(
            initial(),
            delivery_modes,
            vec![RecordSemantics::AppendOnly],
            partitioned,
        )?;
        self.source = Some((
            definition,
            Box::new(move |raw| {
                let config = serde_yaml::from_value(raw)
                    .map_err(|error| anyhow::anyhow!("invalid source configuration: {error}"))?;
                factory(config)
            }),
        ));
        Ok(self)
    }

    pub fn sink<C, F, I>(mut self, initial: I, factory: F) -> anyhow::Result<Self>
    where
        C: DeserializeOwned + JsonSchema + 'static,
        F: Fn(C) -> anyhow::Result<Box<dyn SinkConnector>> + Send + Sync + 'static,
        I: FnOnce() -> JsonValue,
    {
        let initial = initial();
        serde_json::from_value::<C>(initial.clone()).map_err(|error| {
            anyhow::anyhow!(
                "invalid initial sink configuration for '{}': {error}",
                self.key
            )
        })?;
        let definition = endpoint_definition::<C>(
            initial,
            Vec::new(),
            vec![RecordSemantics::AppendOnly],
            false,
        )?;
        self.sink = Some((
            definition,
            Box::new(move |raw| {
                let config = serde_yaml::from_value(raw)
                    .map_err(|error| anyhow::anyhow!("invalid sink configuration: {error}"))?;
                factory(config)
            }),
        ));
        Ok(self)
    }

    /// Registers a sink whose UI draft is deliberately incomplete.
    /// Runtime construction remains strict and uses the typed `C` decoder.
    pub fn sink_draft<C, F, I>(mut self, initial: I, factory: F) -> anyhow::Result<Self>
    where
        C: DeserializeOwned + JsonSchema + 'static,
        F: Fn(C) -> anyhow::Result<Box<dyn SinkConnector>> + Send + Sync + 'static,
        I: FnOnce() -> JsonValue,
    {
        let definition = endpoint_definition::<C>(
            initial(),
            Vec::new(),
            vec![RecordSemantics::AppendOnly],
            false,
        )?;
        self.sink = Some((
            definition,
            Box::new(move |raw| {
                let config = serde_yaml::from_value(raw)
                    .map_err(|error| anyhow::anyhow!("invalid sink configuration: {error}"))?;
                factory(config)
            }),
        ));
        Ok(self)
    }

    /// Declares all record semantics that the registered source can produce.
    pub fn source_record_semantics(
        mut self,
        record_semantics: Vec<RecordSemantics>,
    ) -> anyhow::Result<Self> {
        validate_record_semantics(self.key, "source", &record_semantics)?;
        self.source
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("component '{}' has no source", self.key))?
            .0
            .record_semantics = record_semantics;
        Ok(self)
    }

    /// Declares all record semantics that the registered sink can accept.
    pub fn sink_record_semantics(
        mut self,
        record_semantics: Vec<RecordSemantics>,
    ) -> anyhow::Result<Self> {
        validate_record_semantics(self.key, "sink", &record_semantics)?;
        self.sink
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("component '{}' has no sink", self.key))?
            .0
            .record_semantics = record_semantics;
        Ok(self)
    }

    /// Declares the only source configuration fields an automatic speed test
    /// may mutate. Every pointer and domain is validated against the authored
    /// JSON Schema; active-branch values are validated again before tuning.
    pub fn source_tuning_parameters(
        mut self,
        parameters: Vec<TuningParameter>,
    ) -> anyhow::Result<Self> {
        let definition = &self
            .source
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("component '{}' has no source", self.key))?
            .0;
        validate_tuning_parameters_against_schema(
            &definition.schema,
            &definition.initial,
            &parameters,
        )?;
        self.source_tuning_parameters = parameters;
        Ok(self)
    }

    /// Declares the only sink configuration fields an automatic speed test may
    /// mutate. Undeclared configuration is immutable during tuning.
    pub fn sink_tuning_parameters(
        mut self,
        parameters: Vec<TuningParameter>,
    ) -> anyhow::Result<Self> {
        let definition = &self
            .sink
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("component '{}' has no sink", self.key))?
            .0;
        validate_tuning_parameters_against_schema(
            &definition.schema,
            &definition.initial,
            &parameters,
        )?;
        self.sink_tuning_parameters = parameters;
        Ok(self)
    }

    #[must_use]
    pub fn source_checker<C, F, Fut>(mut self, checker: F) -> Self
    where
        C: DeserializeOwned + Send + 'static,
        F: Fn(C) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<ConnectionCheckResult>> + Send + 'static,
    {
        self.source_checker =
            Some(Box::new(move |raw| match serde_yaml::from_value(raw) {
                Ok(config) => Box::pin(checker(config)),
                Err(error) => Box::pin(async move {
                    Err(anyhow::anyhow!("invalid source configuration: {error}"))
                }),
            }));
        self
    }

    #[must_use]
    pub fn sink_checker<C, F, Fut>(mut self, checker: F) -> Self
    where
        C: DeserializeOwned + Send + 'static,
        F: Fn(C) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<ConnectionCheckResult>> + Send + 'static,
    {
        self.sink_checker = Some(Box::new(move |raw| match serde_yaml::from_value(raw) {
            Ok(config) => Box::pin(checker(config)),
            Err(error) => {
                Box::pin(async move { Err(anyhow::anyhow!("invalid sink configuration: {error}")) })
            }
        }));
        self
    }
}

fn validate_record_semantics(
    key: &str,
    role: &str,
    record_semantics: &[RecordSemantics],
) -> anyhow::Result<()> {
    anyhow::ensure!(
        !record_semantics.is_empty(),
        "component '{key}' {role} must declare at least one record semantics"
    );
    anyhow::ensure!(
        !record_semantics
            .iter()
            .enumerate()
            .any(|(index, semantics)| record_semantics[index + 1..].contains(semantics)),
        "component '{key}' {role} declares duplicate record semantics"
    );
    Ok(())
}

pub struct RegistryBuilder {
    registrations: Vec<ComponentRegistration>,
    keys: BTreeSet<&'static str>,
    middleware_registrations: Vec<MiddlewareRegistration>,
    middleware_keys: BTreeSet<&'static str>,
}

impl RegistryBuilder {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            registrations: Vec::new(),
            keys: BTreeSet::new(),
            middleware_registrations: Vec::new(),
            middleware_keys: BTreeSet::new(),
        }
    }

    pub fn register(&mut self, registration: ComponentRegistration) -> anyhow::Result<&mut Self> {
        anyhow::ensure!(
            !registration.key.is_empty(),
            "component key must not be empty"
        );
        anyhow::ensure!(
            !registration.title.trim().is_empty(),
            "component '{}' title must not be empty",
            registration.key
        );
        anyhow::ensure!(
            registration.source.is_some() || registration.sink.is_some(),
            "component '{}' has no runtime role",
            registration.key
        );
        anyhow::ensure!(
            registration.source_checker.is_none() || registration.source.is_some(),
            "component '{}' registers a source connection check without a source",
            registration.key
        );
        anyhow::ensure!(
            registration.source_previewer.is_none() || registration.source.is_some(),
            "component '{}' registers message preview without a source",
            registration.key
        );
        anyhow::ensure!(
            registration.source_schema_previewer.is_none() || registration.source.is_some(),
            "component '{}' registers source schema preview without a source",
            registration.key
        );
        anyhow::ensure!(
            registration.source_table_sampler.is_none() || registration.source.is_some(),
            "component '{}' registers native table sampling without a source",
            registration.key
        );
        anyhow::ensure!(
            registration.sink_checker.is_none() || registration.sink.is_some(),
            "component '{}' registers a sink connection check without a sink",
            registration.key
        );
        if let Some((source, _)) = &registration.source {
            validate_endpoint_capabilities(
                &source.schema,
                EndpointRole::Source,
                &source.delivery_modes,
                &source.record_semantics,
            )?;
        }
        if let Some((sink, _)) = &registration.sink {
            validate_endpoint_capabilities(
                &sink.schema,
                EndpointRole::Sink,
                &sink.delivery_modes,
                &sink.record_semantics,
            )?;
        }
        anyhow::ensure!(
            self.keys.insert(registration.key),
            "component '{}' is registered more than once",
            registration.key
        );
        self.registrations.push(registration);
        Ok(self)
    }

    pub fn register_middleware(
        &mut self,
        registration: MiddlewareRegistration,
    ) -> anyhow::Result<&mut Self> {
        anyhow::ensure!(
            self.middleware_keys.insert(registration.key),
            "middleware '{}' is registered more than once",
            registration.key
        );
        self.middleware_registrations.push(registration);
        Ok(self)
    }

    #[must_use]
    pub fn build(self) -> Registry {
        let mut definitions = Vec::with_capacity(self.registrations.len());
        let mut sources = BTreeMap::new();
        let mut sinks = BTreeMap::new();
        let mut source_checkers = BTreeMap::new();
        let mut sink_checkers = BTreeMap::new();
        let mut source_previewers = BTreeMap::new();
        let mut source_schema_previewers = BTreeMap::new();
        let mut source_table_samplers = BTreeMap::new();
        let mut tuning_parameters = BTreeMap::new();
        for registration in self.registrations {
            let source = registration.source.map(|(mut definition, factory)| {
                definition.connection_check = registration.source_checker.is_some();
                definition.message_preview = registration.source_previewer.is_some();
                definition.table_preview = registration.source_table_sampler.is_some();
                sources.insert(registration.key, factory);
                definition
            });
            let sink = registration.sink.map(|(mut definition, factory)| {
                definition.connection_check = registration.sink_checker.is_some();
                sinks.insert(registration.key, factory);
                definition
            });
            if let Some(checker) = registration.source_checker {
                source_checkers.insert(registration.key, checker);
            }
            if let Some(checker) = registration.sink_checker {
                sink_checkers.insert(registration.key, checker);
            }
            if let Some(previewer) = registration.source_previewer {
                source_previewers.insert(registration.key, previewer);
            }
            if let Some(previewer) = registration.source_schema_previewer {
                source_schema_previewers.insert(registration.key, previewer);
            }
            if let Some(sampler) = registration.source_table_sampler {
                source_table_samplers.insert(registration.key, sampler);
            }
            if !registration.source_tuning_parameters.is_empty() {
                tuning_parameters.insert(
                    (registration.key, EndpointRole::Source),
                    registration.source_tuning_parameters,
                );
            }
            if !registration.sink_tuning_parameters.is_empty() {
                tuning_parameters.insert(
                    (registration.key, EndpointRole::Sink),
                    registration.sink_tuning_parameters,
                );
            }
            definitions.push(ConnectorDefinition {
                key: registration.key,
                title: registration.title,
                source,
                sink,
            });
        }
        let mut middleware_definitions = Vec::with_capacity(self.middleware_registrations.len());
        let mut middlewares = BTreeMap::new();
        for registration in self.middleware_registrations {
            middleware_definitions.push(registration.definition);
            middlewares.insert(registration.key, registration.factory);
        }
        Registry {
            definitions,
            sources,
            sinks,
            source_checkers,
            sink_checkers,
            source_previewers,
            source_schema_previewers,
            source_table_samplers,
            tuning_parameters,
            middleware_definitions,
            middlewares,
        }
    }
}

impl Default for RegistryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Registry {
    definitions: Vec<ConnectorDefinition>,
    sources: BTreeMap<&'static str, SourceFactory>,
    sinks: BTreeMap<&'static str, SinkFactory>,
    source_checkers: BTreeMap<&'static str, ConnectionChecker>,
    sink_checkers: BTreeMap<&'static str, ConnectionChecker>,
    source_previewers: BTreeMap<&'static str, SourcePreviewer>,
    source_schema_previewers: BTreeMap<&'static str, SourceSchemaPreviewer>,
    source_table_samplers: BTreeMap<&'static str, SourceTableSampler>,
    tuning_parameters: BTreeMap<(&'static str, EndpointRole), Vec<TuningParameter>>,
    middleware_definitions: Vec<MiddlewareDefinition>,
    middlewares: BTreeMap<&'static str, MiddlewareFactory>,
}

impl Registry {
    #[must_use]
    pub fn definitions(&self) -> &[ConnectorDefinition] {
        &self.definitions
    }

    #[must_use]
    pub fn middleware_definitions(&self) -> &[MiddlewareDefinition] {
        &self.middleware_definitions
    }

    #[must_use]
    pub fn supports_source_schema_preview(&self, kind: &str) -> bool {
        self.source_schema_previewers.contains_key(kind)
    }

    /// Returns connector-authored tuning metadata. An empty slice means the
    /// endpoint deliberately exposes no safe automatic tuning parameters.
    pub fn tuning_parameters(
        &self,
        kind: &str,
        role: EndpointRole,
    ) -> anyhow::Result<&[TuningParameter]> {
        let definition = self
            .definitions
            .iter()
            .find(|definition| definition.key == kind)
            .ok_or_else(|| anyhow::anyhow!("unknown connector component '{kind}'"))?;
        let has_role = match role {
            EndpointRole::Source => definition.source.is_some(),
            EndpointRole::Sink => definition.sink.is_some(),
        };
        anyhow::ensure!(has_role, "component '{kind}' has no {role:?} endpoint");
        Ok(self
            .tuning_parameters
            .get(&(definition.key, role))
            .map(Vec::as_slice)
            .unwrap_or_default())
    }

    pub fn edit_definitions(
        &mut self,
        edit: impl FnOnce(&mut [ConnectorDefinition]) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let runtime_shape = definition_shape(&self.definitions);
        let mut candidate = self.definitions.clone();
        edit(&mut candidate)?;
        anyhow::ensure!(
            definition_shape(&candidate) == runtime_shape,
            "connector definition editing changed executable component identity or roles"
        );
        validate_definition_ui(&candidate)?;
        self.definitions = candidate;
        Ok(())
    }

    pub fn replace_definitions(
        &mut self,
        definitions: Vec<ConnectorDefinition>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            definition_shape(&definitions) == definition_shape(&self.definitions),
            "connector definitions do not match the executable registry"
        );
        validate_definition_ui(&definitions)?;
        self.definitions = definitions;
        Ok(())
    }

    pub fn build_source(&self, kind: &str, raw: Value) -> anyhow::Result<Box<dyn SourceConnector>> {
        self.sources
            .get(kind)
            .ok_or_else(|| anyhow::anyhow!("unknown source component '{kind}'"))?(raw)
    }

    pub fn build_sink(&self, kind: &str, raw: Value) -> anyhow::Result<Box<dyn SinkConnector>> {
        self.sinks
            .get(kind)
            .ok_or_else(|| anyhow::anyhow!("unknown sink component '{kind}'"))?(raw)
    }

    pub fn build_middleware(&self, kind: &str, raw: Value) -> anyhow::Result<Box<dyn Middleware>> {
        self.middlewares
            .get(kind)
            .ok_or_else(|| anyhow::anyhow!("unknown middleware '{kind}'"))?(raw)
    }

    pub async fn check_connection(
        &self,
        kind: &str,
        role: EndpointRole,
        raw: Value,
    ) -> anyhow::Result<ConnectionCheckResult> {
        let checker = match role {
            EndpointRole::Source => self.source_checkers.get(kind),
            EndpointRole::Sink => self.sink_checkers.get(kind),
        }
        .ok_or_else(|| anyhow::anyhow!("{kind} {role:?} does not support connection checks"))?;
        checker(raw).await
    }

    pub async fn preview_source(
        &self,
        kind: &str,
        raw: Value,
        max_bytes: usize,
        cancellation: CancellationToken,
    ) -> anyhow::Result<SourcePreview> {
        let previewer = self
            .source_previewers
            .get(kind)
            .ok_or_else(|| anyhow::anyhow!("{kind} source does not support message preview"))?;
        previewer(raw, max_bytes, cancellation).await
    }

    pub async fn preview_source_schema(
        &self,
        kind: &str,
        raw: Value,
        request: DeliveryDiscoveryRequest,
        cancellation: CancellationToken,
    ) -> anyhow::Result<DeliveryDiscovery> {
        let previewer = self.source_schema_previewers.get(kind).ok_or_else(|| {
            anyhow::anyhow!("{kind} source does not support partial schema preview")
        })?;
        previewer(raw, request, cancellation).await
    }

    pub async fn sample_source_table(
        &self,
        kind: &str,
        raw: Value,
        table: TableIdentity,
        limits: TableSampleLimits,
        cancellation: CancellationToken,
    ) -> anyhow::Result<TableData> {
        limits.validate()?;
        anyhow::ensure!(!table.namespace.is_empty() && !table.name.is_empty(), "source sample requires a qualified table identity");
        let sampler = self.source_table_samplers.get(kind)
            .ok_or_else(|| anyhow::anyhow!("{kind} source does not support native table sampling"))?;
        let result = tokio::select! {
            biased;
            () = cancellation.cancelled() => anyhow::bail!("source table sample cancelled"),
            result = sampler(raw, table.clone(), limits, cancellation.clone()) => result?,
        };
        anyhow::ensure!(result.namespace.as_deref() == Some(table.namespace.as_str()) && result.table.as_ref() == table.name,
            "source sample returned a different table identity");
        anyhow::ensure!(result.batch.num_rows() <= limits.row_limit,
            "source sample exceeded the requested row_limit");
        limits.check_bytes(result.batch.get_array_memory_size())?;
        Ok(result)
    }
}

fn validate_definition_ui(definitions: &[ConnectorDefinition]) -> anyhow::Result<()> {
    for definition in definitions {
        if let Some(source) = &definition.source {
            validate_ui_dialect(&source.schema)?;
            validate_endpoint_capabilities(
                &source.schema,
                EndpointRole::Source,
                &source.delivery_modes,
                &source.record_semantics,
            )?;
        }
        if let Some(sink) = &definition.sink {
            validate_ui_dialect(&sink.schema)?;
            validate_endpoint_capabilities(
                &sink.schema,
                EndpointRole::Sink,
                &sink.delivery_modes,
                &sink.record_semantics,
            )?;
        }
    }
    Ok(())
}

fn definition_shape(definitions: &[ConnectorDefinition]) -> Vec<(&'static str, bool, bool)> {
    definitions
        .iter()
        .map(|definition| {
            (
                definition.key,
                definition.source.is_some(),
                definition.sink.is_some(),
            )
        })
        .collect()
}

fn endpoint_definition<C: JsonSchema>(
    initial: JsonValue,
    delivery_modes: Vec<DeliveryMode>,
    record_semantics: Vec<RecordSemantics>,
    partitioned: bool,
) -> anyhow::Result<EndpointDefinition> {
    let schema = serde_json::to_value(schema_for!(C))?;
    validate_ui_dialect(&schema)?;
    Ok(EndpointDefinition {
        schema,
        initial,
        delivery_modes,
        record_semantics,
        partitioned,
        connection_check: false,
        message_preview: false,
        table_preview: false,
    })
}
