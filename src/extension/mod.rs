use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use serde_yaml::{Mapping, Value};
use sha2::{Digest, Sha256};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const RESOLVE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
/// Bump whenever core changes executable extension behavior without a public
/// schema change that would otherwise alter the composition fingerprint.
const CORE_EXTENSION_ABI_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointRole {
    Source,
    Sink,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DynamicOption {
    pub value: String,

    pub label: String,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DynamicOptions {
    pub options: Vec<DynamicOption>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-omit-none" = true))]
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

    pub cancellation: CancellationToken,

    pub deadline: Instant,
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
        let initial = self
            .initial
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("installation initial value must be an object"))?;
        anyhow::ensure!(
            self.initial.get("type").and_then(JsonValue::as_str) == Some(self.kind),
            "installation initial value must contain type '{}'",
            self.kind
        );
        let properties = self
            .schema
            .get("properties")
            .and_then(JsonValue::as_object)
            .ok_or_else(|| anyhow::anyhow!("installation schema must declare object properties"))?;
        anyhow::ensure!(
            self.schema.get("additionalProperties") == Some(&JsonValue::Bool(false)),
            "installation schema must set additionalProperties=false"
        );
        anyhow::ensure!(
            properties
                .get("type")
                .and_then(|schema| schema.get("const"))
                .and_then(JsonValue::as_str)
                == Some(self.kind),
            "installation schema type discriminator must be const '{}'",
            self.kind
        );
        for key in initial.keys() {
            anyhow::ensure!(
                properties.contains_key(key),
                "installation initial value contains undeclared field '{key}'"
            );
        }
        if let Some(required) = self.schema.get("required").and_then(JsonValue::as_array) {
            anyhow::ensure!(
                required.iter().any(|field| field == "type"),
                "installation schema must require its type discriminator"
            );
            for field in required {
                let field = field.as_str().ok_or_else(|| {
                    anyhow::anyhow!("installation schema required entries must be strings")
                })?;
                anyhow::ensure!(
                    self.initial.get(field).is_some(),
                    "installation initial value omits required field '{field}'"
                );
            }
        }
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

    pub(crate) fn installation_keys(
        &self,
    ) -> impl Iterator<Item = (&'static str, EndpointRole, &'static str)> + '_ {
        self.installations.keys().copied()
    }

    pub(crate) fn installations(&self) -> impl Iterator<Item = &InstallationRegistration> {
        self.installations.values()
    }

    pub(crate) fn option_keys(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.option_sources.keys().copied()
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
        cancellation: CancellationToken,
    ) -> anyhow::Result<Value> {
        anyhow::ensure!(
            crate::providers::catalog::provider_supports_role(provider, role),
            "unknown {role:?} provider '{provider}'"
        );
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
        let deadline = Instant::now() + RESOLVE_TIMEOUT;
        let context = ResolveContext {
            provider: provider.to_owned(),
            role,
            cancellation: cancellation.clone(),
            deadline,
        };
        let resolved = tokio::select! {
            () = cancellation.cancelled() => anyhow::bail!("{provider} installation resolution was cancelled"),
            result = tokio::time::timeout_at(
                deadline,
                registration.resolver.resolve(installation, context),
            ) => result.map_err(|_| anyhow::anyhow!(
                "{provider} installation resolution exceeded {} seconds",
                RESOLVE_TIMEOUT.as_secs()
            ))??,
        };
        let contract = crate::providers::catalog::installation_contract(provider, role)
            .ok_or_else(|| {
                anyhow::anyhow!("provider '{provider}' does not support the {role:?} role")
            })?;
        for key in resolved.keys() {
            let field = key
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("resolved installation field must be a string"))?;
            anyhow::ensure!(
                contract.output_fields.contains(&field),
                "installation resolver returned undeclared {provider}.{role:?} field '{field}'"
            );
        }
        for field in contract.required_output_fields {
            anyhow::ensure!(
                resolved.contains_key(Value::String((*field).to_owned())),
                "installation resolver omitted required {provider}.{role:?} field '{field}'"
            );
        }
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct ExtensionIdentity {
    pub package: &'static str,

    pub abi_version: u32,
}

pub trait TransferiaExtension: Send + Sync {
    fn identity(&self) -> ExtensionIdentity;

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
        let mut identities = Vec::with_capacity(self.extensions.len());
        crate::providers::catalog::register_builtin_installations(&mut registry)?;
        for extension in self.extensions {
            let identity = extension.identity();
            anyhow::ensure!(
                !identity.package.trim().is_empty(),
                "extension package identity must not be empty"
            );
            anyhow::ensure!(
                !identities.contains(&identity),
                "extension '{}@{}' is registered more than once",
                identity.package,
                identity.abi_version
            );
            identities.push(identity);
            extension.register(&mut registry)?;
        }
        crate::providers::catalog::validate_extension_registry(&registry)?;
        identities.sort_unstable();
        let definitions = crate::providers::catalog::compile_provider_definitions(&registry)?;
        let composition_fingerprint =
            composition_fingerprint(&registry, &identities, &definitions)?;
        Ok(Transferia {
            composition: Arc::new(CompiledComposition {
                registry,
                definitions: definitions.into(),
                extension_identities: identities.into(),
                fingerprint: Arc::from(composition_fingerprint),
            }),
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
    composition: Arc<CompiledComposition>,
}

pub struct CompiledComposition {
    registry: ExtensionRegistry,

    definitions: Arc<[crate::providers::catalog::ProviderDefinition]>,

    extension_identities: Arc<[ExtensionIdentity]>,

    fingerprint: Arc<str>,
}

impl Transferia {
    pub fn public() -> anyhow::Result<Self> {
        TransferiaBuilder::new().build()
    }

    #[must_use]
    pub fn registry(&self) -> &ExtensionRegistry {
        &self.composition.registry
    }

    #[must_use]
    pub fn composition(&self) -> &CompiledComposition {
        &self.composition
    }

    #[must_use]
    pub fn composition_fingerprint(&self) -> &str {
        self.composition.fingerprint()
    }
}

impl CompiledComposition {
    #[must_use]
    pub fn provider_definitions(&self) -> &[crate::providers::catalog::ProviderDefinition] {
        &self.definitions
    }

    #[must_use]
    pub fn extension_identities(&self) -> &[ExtensionIdentity] {
        &self.extension_identities
    }

    #[must_use]
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }
}

fn composition_fingerprint(
    registry: &ExtensionRegistry,
    identities: &[ExtensionIdentity],
    definitions: &[crate::providers::catalog::ProviderDefinition],
) -> anyhow::Result<String> {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let installations = registry
        .installations()
        .map(|registration| {
            serde_json::json!({
                "provider": registration.provider,
                "role": registration.role,
                "kind": registration.kind,
                "title": registration.title,
                "schema": registration.schema,
                "initial": registration.initial,
                "preferred": registration.preferred,
            })
        })
        .collect::<Vec<_>>();
    let contract = serde_json::json!({
        "core_version": env!("CARGO_PKG_VERSION"),
        "core_extension_abi": CORE_EXTENSION_ABI_VERSION,
        "extensions": identities,
        "provider_contracts": crate::providers::catalog::provider_contracts(),
        "providers": definitions,
        "installations": installations,
        "dynamic_option_keys": registry.option_keys().collect::<Vec<_>>(),
    });
    let mut bytes = Vec::new();
    write_canonical_json(&contract, &mut bytes)?;
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(format!("v2-sha256-{encoded}"))
}

fn write_canonical_json(value: &JsonValue, output: &mut Vec<u8>) -> anyhow::Result<()> {
    match value {
        JsonValue::Object(object) => {
            output.push(b'{');
            let mut entries = object.iter().collect::<Vec<_>>();
            entries.sort_unstable_by_key(|(key, _)| *key);
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                serde_json::to_writer(&mut *output, key)?;
                output.push(b':');
                write_canonical_json(value, output)?;
            }
            output.push(b'}');
        }
        JsonValue::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write_canonical_json(value, output)?;
            }
            output.push(b']');
        }
        _ => serde_json::to_writer(output, value)?,
    }
    Ok(())
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
#[path = "tests/mod.rs"]
mod tests;
