use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use schemars::{schema_for, JsonSchema};
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;
use serde_yaml::Value;
use tokio_util::sync::CancellationToken;
use transferia_delivery_contracts::metrics::MetricsRegistry;
use transferia_delivery_contracts::middleware::Middleware;

use crate::ui_contract::validate_ui_dialect;
use crate::{
    ConnectionCheckResult, ConnectorDefinition, DeliveryMode, EndpointDefinition, EndpointRole,
    MiddlewareDefinition, SinkConnector, SourceConnector,
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
type SourceFactory = Box<dyn Fn(Value) -> anyhow::Result<Box<dyn SourceConnector>> + Send + Sync>;
type SinkFactory = Box<dyn Fn(Value) -> anyhow::Result<Box<dyn SinkConnector>> + Send + Sync>;
type MiddlewareFactory = Box<dyn Fn(Value) -> anyhow::Result<Box<dyn Middleware>> + Send + Sync>;
type MiddlewarePreviewer = Box<
    dyn Fn(
            Value,
            Vec<JsonValue>,
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<MiddlewarePreview>> + Send>>
        + Send
        + Sync,
>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MiddlewarePreviewColumn {
    pub name: String,

    pub arrow_type: String,

    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MiddlewarePreview {
    pub columns: Vec<MiddlewarePreviewColumn>,

    pub rows: Vec<JsonValue>,
}

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
}

pub struct MiddlewareRegistration {
    key: &'static str,
    definition: MiddlewareDefinition,
    factory: MiddlewareFactory,
    previewer: Option<MiddlewarePreviewer>,
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
                playground: false,
            },
            factory: Box::new(move |raw| {
                let config = serde_yaml::from_value(raw).map_err(|error| {
                    anyhow::anyhow!("invalid middleware configuration: {error}")
                })?;
                factory(config)
            }),
            previewer: None,
        })
    }

    pub fn new_with_preview<C, F, I, P, Fut>(
        key: &'static str,
        title: &'static str,
        initial: I,
        factory: F,
        previewer: P,
    ) -> anyhow::Result<Self>
    where
        C: DeserializeOwned + JsonSchema + Send + 'static,
        F: Fn(C) -> anyhow::Result<Box<dyn Middleware>> + Send + Sync + 'static,
        I: FnOnce() -> JsonValue,
        P: Fn(C, Vec<JsonValue>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<MiddlewarePreview>> + Send + 'static,
    {
        let mut registration = Self::new::<C, _, _>(key, title, initial, factory)?;
        registration.definition.playground = true;
        registration.previewer = Some(Box::new(move |raw, rows| {
            let decoded = serde_yaml::from_value(raw);
            match decoded {
                Ok(config) => Box::pin(previewer(config, rows)),
                Err(error) => Box::pin(async move {
                    Err(anyhow::anyhow!("invalid middleware configuration: {error}"))
                }),
            }
        }));
        Ok(registration)
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
        let definition = endpoint_definition::<C>(initial, delivery_modes, partitioned)?;
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
        let definition = endpoint_definition::<C>(initial(), delivery_modes, partitioned)?;
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
        let definition = endpoint_definition::<C>(initial, Vec::new(), false)?;
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
        let definition = endpoint_definition::<C>(initial(), Vec::new(), false)?;
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
            registration.sink_checker.is_none() || registration.sink.is_some(),
            "component '{}' registers a sink connection check without a sink",
            registration.key
        );
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
        for registration in self.registrations {
            let source = registration.source.map(|(mut definition, factory)| {
                definition.connection_check = registration.source_checker.is_some();
                definition.message_preview = registration.source_previewer.is_some();
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
            definitions.push(ConnectorDefinition {
                key: registration.key,
                title: registration.title,
                source,
                sink,
            });
        }
        let mut middleware_definitions = Vec::with_capacity(self.middleware_registrations.len());
        let mut middlewares = BTreeMap::new();
        let mut middleware_previewers = BTreeMap::new();
        for registration in self.middleware_registrations {
            middleware_definitions.push(registration.definition);
            middlewares.insert(registration.key, registration.factory);
            if let Some(previewer) = registration.previewer {
                middleware_previewers.insert(registration.key, previewer);
            }
        }
        Registry {
            definitions,
            sources,
            sinks,
            source_checkers,
            sink_checkers,
            source_previewers,
            middleware_definitions,
            middlewares,
            middleware_previewers,
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
    middleware_definitions: Vec<MiddlewareDefinition>,
    middlewares: BTreeMap<&'static str, MiddlewareFactory>,
    middleware_previewers: BTreeMap<&'static str, MiddlewarePreviewer>,
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
        for definition in &definitions {
            if let Some(source) = &definition.source {
                validate_ui_dialect(&source.schema)?;
            }
            if let Some(sink) = &definition.sink {
                validate_ui_dialect(&sink.schema)?;
            }
        }
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

    pub async fn preview_middleware(
        &self,
        kind: &str,
        raw: Value,
        rows: Vec<JsonValue>,
    ) -> anyhow::Result<MiddlewarePreview> {
        let previewer = self.middleware_previewers.get(kind).ok_or_else(|| {
            anyhow::anyhow!("middleware '{kind}' does not support interactive preview")
        })?;
        previewer(raw, rows).await
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
    partitioned: bool,
) -> anyhow::Result<EndpointDefinition> {
    let schema = serde_json::to_value(schema_for!(C))?;
    validate_ui_dialect(&schema)?;
    Ok(EndpointDefinition {
        schema,
        initial,
        delivery_modes,
        partitioned,
        connection_check: false,
        message_preview: false,
    })
}
