use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;
use serde_yaml::Value;

use super::apply_endpoint_installations;
use super::definition::{DeliveryMode, EndpointDefinition, EndpointSpec, ProviderDefinition};
use super::descriptor::provider_descriptor;
use crate::extension::{EndpointRole, ExtensionRegistry};
use crate::providers::traits::ConnectionCheckResult;
use crate::providers::traits::{SinkProvider, SourceProvider};

type ConnectionChecker = Box<
    dyn Fn(Value) -> Pin<Box<dyn Future<Output = anyhow::Result<ConnectionCheckResult>> + Send>>
        + Send
        + Sync,
>;

type SourceFactory = Box<dyn Fn(Value) -> anyhow::Result<Box<dyn SourceProvider>> + Send + Sync>;
type SinkFactory = Box<dyn Fn(Value) -> anyhow::Result<Box<dyn SinkProvider>> + Send + Sync>;

struct SourceRegistration {
    definition: Option<EndpointDefinition>,
    factory: SourceFactory,
}

struct SinkRegistration {
    definition: Option<EndpointDefinition>,
    factory: SinkFactory,
}

pub(super) struct ProviderRegistration {
    key: &'static str,
    title: &'static str,
    source: Option<SourceRegistration>,
    sink: Option<SinkRegistration>,
    compile_definition: bool,
    source_checker: Option<ConnectionChecker>,
    sink_checker: Option<ConnectionChecker>,
}

impl ProviderRegistration {
    pub fn new(key: &'static str, compile_definition: bool) -> anyhow::Result<Self> {
        let descriptor = provider_descriptor(key)
            .ok_or_else(|| anyhow::anyhow!("unknown provider descriptor '{key}'"))?;
        Ok(Self {
            key: descriptor.key,
            title: descriptor.title,
            source: None,
            sink: None,
            compile_definition,
            source_checker: None,
            sink_checker: None,
        })
    }

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

    pub fn sink<C, F, I>(mut self, initial: I, factory: F) -> anyhow::Result<Self>
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
    pub(super) definitions: Vec<ProviderDefinition>,
    sources: BTreeMap<&'static str, SourceFactory>,
    sinks: BTreeMap<&'static str, SinkFactory>,
    source_checkers: BTreeMap<&'static str, ConnectionChecker>,
    sink_checkers: BTreeMap<&'static str, ConnectionChecker>,
}

impl ProviderCatalog {
    pub(super) fn new() -> Self {
        Self {
            definitions: Vec::new(),
            sources: BTreeMap::new(),
            sinks: BTreeMap::new(),
            source_checkers: BTreeMap::new(),
            sink_checkers: BTreeMap::new(),
        }
    }

    pub(super) fn register(&mut self, registration: ProviderRegistration) -> anyhow::Result<()> {
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

        let source_has_checker = registration.source_checker.is_some();
        let sink_has_checker = registration.sink_checker.is_some();
        let source = registration.source.and_then(|source| {
            self.sources.insert(registration.key, source.factory);
            source.definition.map(|mut definition| {
                definition.connection_check = source_has_checker;
                definition
            })
        });
        if let Some(checker) = registration.source_checker {
            self.source_checkers.insert(registration.key, checker);
        }
        if let Some(checker) = registration.sink_checker {
            self.sink_checkers.insert(registration.key, checker);
        }
        let sink = registration.sink.and_then(|sink| {
            self.sinks.insert(registration.key, sink.factory);
            sink.definition.map(|mut definition| {
                definition.connection_check = sink_has_checker;
                definition
            })
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

    pub(super) fn apply_installations(
        &mut self,
        registry: &ExtensionRegistry,
    ) -> anyhow::Result<()> {
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
