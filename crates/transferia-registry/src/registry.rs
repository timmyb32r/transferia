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

use crate::{
    ConnectionCheckResult, DeliveryMode, EndpointDefinition, EndpointRole, ProviderDefinition,
    SinkProvider, SourceProvider,
};

type ConnectionChecker = Box<
    dyn Fn(Value) -> Pin<Box<dyn Future<Output = anyhow::Result<ConnectionCheckResult>> + Send>>
        + Send
        + Sync,
>;
type SourceFactory = Box<dyn Fn(Value) -> anyhow::Result<Box<dyn SourceProvider>> + Send + Sync>;
type SinkFactory = Box<dyn Fn(Value) -> anyhow::Result<Box<dyn SinkProvider>> + Send + Sync>;

/// Complete executable composition consumed by delivery preparation.
///
/// Implementations own installation resolution and component registration;
/// delivery orchestration depends only on this provider-neutral port.
pub trait Composition: Send + Sync {
    fn fingerprint(&self) -> &str;

    fn definitions(&self) -> &[ProviderDefinition];

    fn build_registry(&self, metrics: &Arc<MetricsRegistry>) -> anyhow::Result<Registry>;

    fn resolve_many(
        &self,
        provider: &str,
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
        }
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
        F: Fn(C) -> anyhow::Result<Box<dyn SourceProvider>> + Send + Sync + 'static,
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
        F: Fn(C) -> anyhow::Result<Box<dyn SourceProvider>> + Send + Sync + 'static,
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
        F: Fn(C) -> anyhow::Result<Box<dyn SinkProvider>> + Send + Sync + 'static,
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
        F: Fn(C) -> anyhow::Result<Box<dyn SinkProvider>> + Send + Sync + 'static,
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
}

impl RegistryBuilder {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            registrations: Vec::new(),
            keys: BTreeSet::new(),
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

    #[must_use]
    pub fn build(self) -> Registry {
        let mut definitions = Vec::with_capacity(self.registrations.len());
        let mut sources = BTreeMap::new();
        let mut sinks = BTreeMap::new();
        let mut source_checkers = BTreeMap::new();
        let mut sink_checkers = BTreeMap::new();
        for registration in self.registrations {
            let source = registration.source.map(|(mut definition, factory)| {
                definition.connection_check = registration.source_checker.is_some();
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
            definitions.push(ProviderDefinition {
                key: registration.key,
                title: registration.title,
                source,
                sink,
            });
        }
        Registry {
            definitions,
            sources,
            sinks,
            source_checkers,
            sink_checkers,
        }
    }
}

impl Default for RegistryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Registry {
    definitions: Vec<ProviderDefinition>,
    sources: BTreeMap<&'static str, SourceFactory>,
    sinks: BTreeMap<&'static str, SinkFactory>,
    source_checkers: BTreeMap<&'static str, ConnectionChecker>,
    sink_checkers: BTreeMap<&'static str, ConnectionChecker>,
}

impl Registry {
    #[must_use]
    pub fn definitions(&self) -> &[ProviderDefinition] {
        &self.definitions
    }

    pub fn edit_definitions(
        &mut self,
        edit: impl FnOnce(&mut [ProviderDefinition]) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let runtime_shape = definition_shape(&self.definitions);
        let mut candidate = self.definitions.clone();
        edit(&mut candidate)?;
        anyhow::ensure!(
            definition_shape(&candidate) == runtime_shape,
            "provider definition editing changed executable component identity or roles"
        );
        self.definitions = candidate;
        Ok(())
    }

    pub fn replace_definitions(
        &mut self,
        definitions: Vec<ProviderDefinition>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            definition_shape(&definitions) == definition_shape(&self.definitions),
            "provider definitions do not match the executable registry"
        );
        self.definitions = definitions;
        Ok(())
    }

    pub fn build_source(&self, kind: &str, raw: Value) -> anyhow::Result<Box<dyn SourceProvider>> {
        self.sources
            .get(kind)
            .ok_or_else(|| anyhow::anyhow!("unknown source component '{kind}'"))?(raw)
    }

    pub fn build_sink(&self, kind: &str, raw: Value) -> anyhow::Result<Box<dyn SinkProvider>> {
        self.sinks
            .get(kind)
            .ok_or_else(|| anyhow::anyhow!("unknown sink component '{kind}'"))?(raw)
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
}

fn definition_shape(definitions: &[ProviderDefinition]) -> Vec<(&'static str, bool, bool)> {
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
    Ok(EndpointDefinition {
        schema: serde_json::to_value(schema_for!(C))?,
        initial,
        delivery_modes,
        partitioned,
        connection_check: false,
    })
}
