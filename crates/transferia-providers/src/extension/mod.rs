use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use serde_yaml::{Mapping, Value};
use sha2::{Digest, Sha256};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const RESOLVE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
/// Bump whenever core changes executable extension behavior without a public
/// schema change that would otherwise alter the composition fingerprint.
const CORE_EXTENSION_ABI_VERSION: u32 = 3;

pub use transferia_registry::EndpointRole;

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

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OptionsRequest {
    #[serde(default)]
    pub query: Option<String>,

    #[serde(default)]
    pub refresh: bool,

    #[serde(default)]
    pub dependencies: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct OptionsContext {
    pub cancellation: CancellationToken,

    pub deadline: Instant,
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
    async fn list(
        &self,
        request: OptionsRequest,
        context: OptionsContext,
    ) -> anyhow::Result<DynamicOptions>;
}

#[async_trait]
pub trait TypedInstallationResolver<I, O>: Send + Sync {
    async fn resolve(&self, installation: I, context: ResolveContext) -> anyhow::Result<O>;
}

#[async_trait]
pub trait TypedMultiInstallationResolver<I, O>: Send + Sync {
    async fn resolve_many(
        &self,
        installation: I,
        context: ResolveContext,
    ) -> anyhow::Result<Vec<O>>;
}

pub struct InstallationSpec<I> {
    pub provider: &'static str,

    pub role: EndpointRole,

    pub kind: &'static str,

    pub title: &'static str,

    pub initial: I,

    pub preferred: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(crate) struct DynamicOptionsBinding {
    pub provider: &'static str,

    pub role: EndpointRole,

    pub schema_pointer: &'static str,

    pub source: &'static str,

    pub dependencies: BTreeMap<&'static str, &'static str>,

    pub control: DynamicOptionsControl,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DynamicOptionsControl {
    #[default]
    Select,

    Path,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(crate) struct ExternalLinkBinding {
    pub provider: &'static str,

    pub role: EndpointRole,

    pub schema_pointer: &'static str,

    pub url_template: &'static str,
}

#[async_trait]
pub(crate) trait InstallationResolver: Send + Sync {
    async fn resolve_many(
        &self,
        installation: Value,
        context: ResolveContext,
    ) -> anyhow::Result<Vec<Mapping>>;
}

pub(crate) struct InstallationRegistration {
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

    option_bindings: BTreeMap<(&'static str, EndpointRole, &'static str), DynamicOptionsBinding>,

    external_link_bindings:
        BTreeMap<(&'static str, EndpointRole, &'static str), ExternalLinkBinding>,

    pre_installation_fields: BTreeMap<(&'static str, EndpointRole), Vec<&'static str>>,
}

impl ExtensionRegistry {
    pub fn register_field_before_installation(
        &mut self,
        provider: &'static str,
        role: EndpointRole,
        field: &'static str,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !provider.is_empty(),
            "field placement provider must not be empty"
        );
        anyhow::ensure!(!field.is_empty(), "placed field must not be empty");
        let fields = self
            .pre_installation_fields
            .entry((provider, role))
            .or_default();
        anyhow::ensure!(
            !fields.contains(&field),
            "field placement '{provider}.{field}.{role:?}' is registered more than once"
        );
        fields.push(field);
        Ok(())
    }

    pub fn register_installation<I, O, R>(
        &mut self,
        spec: InstallationSpec<I>,
        resolver: R,
    ) -> anyhow::Result<()>
    where
        I: DeserializeOwned + JsonSchema + Serialize + Send + Sync + 'static,
        O: Serialize + Send + Sync + 'static,
        R: TypedInstallationResolver<I, O> + 'static,
    {
        let InstallationSpec {
            provider,
            role,
            kind,
            title,
            initial,
            preferred,
        } = spec;
        let generator = schemars::generate::SchemaSettings::draft2020_12()
            .with(|settings| settings.inline_subschemas = true)
            .into_generator();
        let mut schema = serde_json::to_value(generator.into_root_schema_for::<I>())?;
        let properties = schema
            .get_mut("properties")
            .and_then(JsonValue::as_object_mut)
            .ok_or_else(|| anyhow::anyhow!("installation input schema must describe an object"))?;
        anyhow::ensure!(
            properties.contains_key("type"),
            "installation input type must contain a 'type' discriminator"
        );
        properties.insert("type".to_owned(), serde_json::json!({ "const": kind }));
        schema
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("installation input schema must be an object"))?
            .insert("additionalProperties".to_owned(), JsonValue::Bool(false));
        let initial = serde_json::to_value(initial)?;
        self.register_erased_installation(InstallationRegistration {
            provider,
            role,
            kind,
            title,
            schema,
            initial,
            preferred,
            resolver: Arc::new(TypedResolverAdapter::<I, O, R> {
                resolver,
                marker: std::marker::PhantomData,
            }),
        })
    }

    pub fn register_multi_installation<I, O, R>(
        &mut self,
        spec: InstallationSpec<I>,
        resolver: R,
    ) -> anyhow::Result<()>
    where
        I: DeserializeOwned + JsonSchema + Serialize + Send + Sync + 'static,
        O: Serialize + Send + Sync + 'static,
        R: TypedMultiInstallationResolver<I, O> + 'static,
    {
        let InstallationSpec {
            provider,
            role,
            kind,
            title,
            initial,
            preferred,
        } = spec;
        let generator = schemars::generate::SchemaSettings::draft2020_12()
            .with(|settings| settings.inline_subschemas = true)
            .into_generator();
        let mut schema = serde_json::to_value(generator.into_root_schema_for::<I>())?;
        let properties = schema
            .get_mut("properties")
            .and_then(JsonValue::as_object_mut)
            .ok_or_else(|| anyhow::anyhow!("installation input schema must describe an object"))?;
        anyhow::ensure!(
            properties.contains_key("type"),
            "installation input type must contain a 'type' discriminator"
        );
        properties.insert("type".to_owned(), serde_json::json!({ "const": kind }));
        schema
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("installation input schema must be an object"))?
            .insert("additionalProperties".to_owned(), JsonValue::Bool(false));
        self.register_erased_installation(InstallationRegistration {
            provider,
            role,
            kind,
            title,
            schema,
            initial: serde_json::to_value(initial)?,
            preferred,
            resolver: Arc::new(TypedMultiResolverAdapter::<I, O, R> {
                resolver,
                marker: std::marker::PhantomData,
            }),
        })
    }

    pub(crate) fn register_erased_installation(
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

    pub fn register_options_binding(
        &mut self,
        provider: &'static str,
        role: EndpointRole,
        schema_pointer: &'static str,
        source: &'static str,
        dependencies: impl IntoIterator<Item = (&'static str, &'static str)>,
    ) -> anyhow::Result<()> {
        self.register_options_binding_with_control(
            provider,
            role,
            schema_pointer,
            source,
            dependencies,
            DynamicOptionsControl::Select,
        )
    }

    pub fn register_path_options_binding(
        &mut self,
        provider: &'static str,
        role: EndpointRole,
        schema_pointer: &'static str,
        source: &'static str,
        dependencies: impl IntoIterator<Item = (&'static str, &'static str)>,
    ) -> anyhow::Result<()> {
        self.register_options_binding_with_control(
            provider,
            role,
            schema_pointer,
            source,
            dependencies,
            DynamicOptionsControl::Path,
        )
    }

    fn register_options_binding_with_control(
        &mut self,
        provider: &'static str,
        role: EndpointRole,
        schema_pointer: &'static str,
        source: &'static str,
        dependencies: impl IntoIterator<Item = (&'static str, &'static str)>,
        control: DynamicOptionsControl,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !provider.is_empty(),
            "options binding provider must not be empty"
        );
        anyhow::ensure!(
            schema_pointer.starts_with("/properties/") || schema_pointer.starts_with("/$defs/"),
            "options binding schema pointer must target an endpoint schema property"
        );
        anyhow::ensure!(
            !source.is_empty(),
            "options binding source must not be empty"
        );
        let dependencies = dependencies.into_iter().collect::<BTreeMap<_, _>>();
        anyhow::ensure!(
            dependencies
                .iter()
                .all(|(name, pointer)| !name.is_empty() && pointer.starts_with('/')),
            "options binding dependencies must have non-empty names and absolute JSON pointers"
        );
        let binding = DynamicOptionsBinding {
            provider,
            role,
            schema_pointer,
            source,
            dependencies,
            control,
        };
        let key = (provider, role, schema_pointer);
        anyhow::ensure!(
            self.option_bindings.insert(key, binding).is_none(),
            "options binding '{provider}.{role:?}{schema_pointer}' is registered more than once"
        );
        Ok(())
    }

    pub fn register_external_link(
        &mut self,
        provider: &'static str,
        role: EndpointRole,
        schema_pointer: &'static str,
        url_template: &'static str,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !provider.is_empty(),
            "external link provider must not be empty"
        );
        anyhow::ensure!(
            schema_pointer.starts_with("/properties/") || schema_pointer.starts_with("/$defs/"),
            "external link schema pointer must target an endpoint schema property"
        );
        anyhow::ensure!(
            url_template.starts_with("https://")
                && url_template.matches("{value}").count() == 1,
            "external link URL template must be HTTPS and contain exactly one '{{value}}' placeholder"
        );
        let binding = ExternalLinkBinding {
            provider,
            role,
            schema_pointer,
            url_template,
        };
        let key = (provider, role, schema_pointer);
        anyhow::ensure!(
            self.external_link_bindings.insert(key, binding).is_none(),
            "external link '{provider}.{role:?}{schema_pointer}' is registered more than once"
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

    pub(crate) fn option_bindings(&self) -> impl Iterator<Item = &DynamicOptionsBinding> {
        self.option_bindings.values()
    }

    pub(crate) fn external_link_bindings(&self) -> impl Iterator<Item = &ExternalLinkBinding> {
        self.external_link_bindings.values()
    }

    pub(crate) fn fields_before_installation(
        &self,
        provider: &str,
        role: EndpointRole,
    ) -> Vec<&'static str> {
        self.pre_installation_fields
            .iter()
            .find(|((registered_provider, registered_role), _)| {
                *registered_provider == provider && *registered_role == role
            })
            .map(|(_, fields)| fields.clone())
            .unwrap_or_default()
    }

    fn field_placements(
        &self,
    ) -> impl Iterator<Item = (&'static str, EndpointRole, &'static str)> + '_ {
        self.pre_installation_fields
            .iter()
            .flat_map(|(&(provider, role), fields)| {
                fields
                    .iter()
                    .copied()
                    .map(move |field| (provider, role, field))
            })
    }

    pub async fn options(
        &self,
        key: &str,
        request: OptionsRequest,
        cancellation: CancellationToken,
    ) -> anyhow::Result<DynamicOptions> {
        let provider = self
            .option_sources
            .get(key)
            .ok_or_else(|| anyhow::anyhow!("unknown dynamic option source '{key}'"))?;
        let deadline = Instant::now() + RESOLVE_TIMEOUT;
        let context = OptionsContext {
            cancellation: cancellation.clone(),
            deadline,
        };
        tokio::select! {
            () = cancellation.cancelled() => anyhow::bail!("dynamic option request was cancelled"),
            result = tokio::time::timeout_at(deadline, provider.list(request, context)) => {
                result.map_err(|_| anyhow::anyhow!(
                    "dynamic option request exceeded {} seconds",
                    RESOLVE_TIMEOUT.as_secs()
                ))?
            }
        }
    }

    pub async fn resolve(
        &self,
        provider: &str,
        role: EndpointRole,
        raw: Value,
        cancellation: CancellationToken,
    ) -> anyhow::Result<Value> {
        let mut resolved = self.resolve_many(provider, role, raw, cancellation).await?;
        anyhow::ensure!(
            resolved.len() == 1,
            "{provider} installation expands to {} pipelines where exactly one endpoint is required",
            resolved.len()
        );
        resolved
            .pop()
            .ok_or_else(|| anyhow::anyhow!("{provider} installation resolved no endpoints"))
    }

    pub async fn resolve_many(
        &self,
        provider: &str,
        role: EndpointRole,
        raw: Value,
        cancellation: CancellationToken,
    ) -> anyhow::Result<Vec<Value>> {
        anyhow::ensure!(
            crate::providers::catalog::provider_supports_role(provider, role),
            "unknown {role:?} provider '{provider}'"
        );
        if !self
            .installations
            .values()
            .any(|registration| registration.provider == provider && registration.role == role)
        {
            return Ok(vec![raw]);
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
        let resolved_variants = tokio::select! {
            () = cancellation.cancelled() => anyhow::bail!("{provider} installation resolution was cancelled"),
            result = tokio::time::timeout_at(
                deadline,
                registration.resolver.resolve_many(installation, context),
            ) => result.map_err(|_| anyhow::anyhow!(
                "{provider} installation resolution exceeded {} seconds",
                RESOLVE_TIMEOUT.as_secs()
            ))??,
        };
        anyhow::ensure!(
            !resolved_variants.is_empty(),
            "{provider} installation resolver returned no endpoints"
        );
        let contract = crate::providers::catalog::installation_contract(provider, role)
            .ok_or_else(|| {
                anyhow::anyhow!("provider '{provider}' does not support the {role:?} role")
            })?;
        resolved_variants
            .into_iter()
            .map(|resolved| {
                validate_resolved_fields(provider, role, &resolved, &contract)?;
                let mut variant = config.clone();
                for (key, value) in resolved {
                    anyhow::ensure!(
                        !variant.contains_key(&key),
                        "resolved installation attempted to overwrite {provider} field {}",
                        key.as_str().unwrap_or("<non-string>")
                    );
                    variant.insert(key, value);
                }
                Ok(Value::Mapping(variant))
            })
            .collect()
    }
}

fn validate_resolved_fields(
    provider: &str,
    role: EndpointRole,
    resolved: &Mapping,
    contract: &crate::providers::catalog::descriptor::InstallationContract,
) -> anyhow::Result<()> {
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
    Ok(())
}

struct TypedResolverAdapter<I, O, R> {
    resolver: R,
    marker: std::marker::PhantomData<fn(I) -> O>,
}

#[async_trait]
impl<I, O, R> InstallationResolver for TypedResolverAdapter<I, O, R>
where
    I: DeserializeOwned + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    R: TypedInstallationResolver<I, O> + 'static,
{
    async fn resolve_many(
        &self,
        installation: Value,
        context: ResolveContext,
    ) -> anyhow::Result<Vec<Mapping>> {
        let input = serde_yaml::from_value(installation)?;
        let output = self.resolver.resolve(input, context).await?;
        Ok(vec![serde_yaml::to_value(output)?
            .as_mapping()
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!("installation resolver output must be an object")
            })?])
    }
}

struct TypedMultiResolverAdapter<I, O, R> {
    resolver: R,
    marker: std::marker::PhantomData<fn(I) -> O>,
}

#[async_trait]
impl<I, O, R> InstallationResolver for TypedMultiResolverAdapter<I, O, R>
where
    I: DeserializeOwned + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    R: TypedMultiInstallationResolver<I, O> + 'static,
{
    async fn resolve_many(
        &self,
        installation: Value,
        context: ResolveContext,
    ) -> anyhow::Result<Vec<Mapping>> {
        let input = serde_yaml::from_value(installation)?;
        self.resolver
            .resolve_many(input, context)
            .await?
            .into_iter()
            .map(|output| {
                serde_yaml::to_value(output)?
                    .as_mapping()
                    .cloned()
                    .ok_or_else(|| {
                        anyhow::anyhow!("installation resolver output must be an object")
                    })
            })
            .collect()
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
        let middleware_definitions = crate::providers::catalog::compile_middleware_definitions()?;
        let composition_fingerprint = composition_fingerprint(
            &registry,
            &identities,
            &definitions,
            &middleware_definitions,
        )?;
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

impl transferia_registry::Composition for Transferia {
    fn fingerprint(&self) -> &str {
        self.composition_fingerprint()
    }

    fn definitions(&self) -> &[transferia_registry::ProviderDefinition] {
        self.composition.provider_definitions()
    }

    fn build_registry(
        &self,
        metrics: &Arc<crate::metrics::MetricsRegistry>,
    ) -> anyhow::Result<transferia_registry::Registry> {
        crate::providers::catalog::build_provider_catalog_with(self, metrics)
    }

    fn resolve_many(
        &self,
        provider: &str,
        role: EndpointRole,
        raw: Value,
        cancellation: CancellationToken,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<Vec<Value>>> + Send + '_>>
    {
        let provider = provider.to_owned();
        Box::pin(async move {
            self.registry()
                .resolve_many(&provider, role, raw, cancellation)
                .await
        })
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
    middleware_definitions: &[transferia_registry::MiddlewareDefinition],
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
        "middlewares": middleware_definitions,
        "installations": installations,
        "dynamic_option_keys": registry.option_keys().collect::<Vec<_>>(),
        "external_links": registry.external_link_bindings().collect::<Vec<_>>(),
        "field_placements": registry.field_placements().collect::<Vec<_>>(),
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
    async fn resolve_many(
        &self,
        installation: Value,
        _context: ResolveContext,
    ) -> anyhow::Result<Vec<Mapping>> {
        let mut fields = installation
            .as_mapping()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("on-premise installation must be an object"))?;
        fields.remove(Value::String("type".to_owned()));
        Ok(vec![fields])
    }
}

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;
