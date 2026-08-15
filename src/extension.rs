use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use serde_yaml::{Mapping, Value};

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointRole {
    Source,
    Sink,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct DynamicOption {
    pub value: String,

    pub label: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
pub struct DynamicOptions {
    pub options: Vec<DynamicOption>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
}

#[derive(Clone, Debug)]
pub struct OptionsRequest {
    pub query: Option<String>,

    pub refresh: bool,
}

#[derive(Clone, Debug)]
pub struct ResolveContext {
    pub provider: String,

    pub role: EndpointRole,
}

#[async_trait]
pub trait DynamicOptionsProvider: Send + Sync {
    async fn list(&self, request: OptionsRequest) -> anyhow::Result<DynamicOptions>;
}

#[async_trait]
pub trait InstallationResolver: Send + Sync {
    async fn resolve(
        &self,
        installation: Value,
        context: ResolveContext,
    ) -> anyhow::Result<Mapping>;
}

pub struct InstallationRegistration {
    pub provider: &'static str,

    pub role: EndpointRole,

    pub kind: &'static str,

    pub title: &'static str,

    pub schema: JsonValue,

    pub initial: JsonValue,

    pub replaces: &'static [&'static str],

    pub preferred: bool,

    pub resolver: Arc<dyn InstallationResolver>,
}

impl InstallationRegistration {
    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.provider.is_empty(),
            "installation provider must not be empty"
        );
        anyhow::ensure!(!self.kind.is_empty(), "installation kind must not be empty");
        anyhow::ensure!(
            self.schema.is_object(),
            "installation schema must be an object"
        );
        anyhow::ensure!(
            self.initial.is_object(),
            "installation initial value must be an object"
        );
        anyhow::ensure!(
            self.initial.get("type").and_then(JsonValue::as_str) == Some(self.kind),
            "installation initial value must contain type '{}'",
            self.kind
        );
        Ok(())
    }
}

#[derive(Default)]
pub struct ExtensionRegistry {
    installations: BTreeMap<(&'static str, EndpointRole, &'static str), InstallationRegistration>,

    option_sources: BTreeMap<&'static str, Arc<dyn DynamicOptionsProvider>>,
}

impl ExtensionRegistry {
    pub fn register_installation(
        &mut self,
        registration: InstallationRegistration,
    ) -> anyhow::Result<()> {
        registration.validate()?;
        let key = (registration.provider, registration.role, registration.kind);
        anyhow::ensure!(
            !self.installations.contains_key(&key),
            "installation '{}.{}.{:?}' is registered more than once",
            registration.provider,
            registration.kind,
            registration.role
        );
        self.installations.insert(key, registration);
        Ok(())
    }

    pub fn register_options(
        &mut self,
        key: &'static str,
        provider: Arc<dyn DynamicOptionsProvider>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !key.is_empty(),
            "dynamic option source key must not be empty"
        );
        anyhow::ensure!(
            self.option_sources.insert(key, provider).is_none(),
            "dynamic option source '{key}' is registered more than once"
        );
        Ok(())
    }

    pub(crate) fn installations_for(
        &self,
        provider: &str,
        role: EndpointRole,
    ) -> Vec<&InstallationRegistration> {
        self.installations
            .values()
            .filter(move |registration| {
                registration.provider == provider && registration.role == role
            })
            .collect()
    }

    pub async fn options(
        &self,
        key: &str,
        request: OptionsRequest,
    ) -> anyhow::Result<DynamicOptions> {
        let provider = self
            .option_sources
            .get(key)
            .ok_or_else(|| anyhow::anyhow!("unknown dynamic option source '{key}'"))?;
        provider.list(request).await
    }

    pub async fn resolve(
        &self,
        provider: &str,
        role: EndpointRole,
        raw: Value,
    ) -> anyhow::Result<Value> {
        if !self
            .installations
            .values()
            .any(|registration| registration.provider == provider && registration.role == role)
        {
            return Ok(raw);
        }
        let mut config = raw
            .as_mapping()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("{provider} configuration must be an object"))?;
        let installation_key = Value::String("installation".to_owned());
        let installation = config
            .remove(&installation_key)
            .ok_or_else(|| anyhow::anyhow!("{provider}.installation is required"))?;
        let kind = installation
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("{provider}.installation.type is required"))?
            .to_owned();
        let registration = self
            .installations
            .values()
            .find(|registration| {
                registration.provider == provider
                    && registration.role == role
                    && registration.kind == kind
            })
            .ok_or_else(|| {
                anyhow::anyhow!("unknown {provider} installation type '{kind}' for {role:?}")
            })?;
        let resolved = registration
            .resolver
            .resolve(
                installation,
                ResolveContext {
                    provider: provider.to_owned(),
                    role,
                },
            )
            .await?;
        for (key, value) in resolved {
            anyhow::ensure!(
                !config.contains_key(&key),
                "resolved installation attempted to overwrite {provider} field {}",
                key.as_str().unwrap_or("<non-string>")
            );
            config.insert(key, value);
        }
        Ok(Value::Mapping(config))
    }
}

pub trait TransferiaExtension: Send + Sync {
    fn register(&self, registry: &mut ExtensionRegistry) -> anyhow::Result<()>;
}

pub struct TransferiaBuilder {
    extensions: Vec<Arc<dyn TransferiaExtension>>,
}

impl TransferiaBuilder {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            extensions: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_extension(mut self, extension: Arc<dyn TransferiaExtension>) -> Self {
        self.extensions.push(extension);
        self
    }

    pub fn build(self) -> anyhow::Result<Transferia> {
        let mut registry = ExtensionRegistry::default();
        crate::providers::catalog::register_builtin_installations(&mut registry)?;
        for extension in self.extensions {
            extension.register(&mut registry)?;
        }
        Ok(Transferia {
            registry: Arc::new(registry),
        })
    }
}

impl Default for TransferiaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct Transferia {
    registry: Arc<ExtensionRegistry>,
}

impl Transferia {
    pub fn public() -> anyhow::Result<Self> {
        TransferiaBuilder::new().build()
    }

    #[must_use]
    pub const fn registry(&self) -> &Arc<ExtensionRegistry> {
        &self.registry
    }
}

pub struct OnPremiseResolver;

#[async_trait]
impl InstallationResolver for OnPremiseResolver {
    async fn resolve(
        &self,
        installation: Value,
        _context: ResolveContext,
    ) -> anyhow::Result<Mapping> {
        let mut fields = installation
            .as_mapping()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("on-premise installation must be an object"))?;
        fields.remove(Value::String("type".to_owned()));
        Ok(fields)
    }
}

#[cfg(test)]
#[path = "tests/extension.rs"]
mod tests;
