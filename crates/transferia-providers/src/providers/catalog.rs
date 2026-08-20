use std::collections::BTreeMap;
use std::sync::Arc;

use schemars::JsonSchema;
use serde_json::Value as JsonValue;

use crate::extension::{EndpointRole, ExtensionRegistry, Transferia};
use crate::extension::{InstallationRegistration, OnPremiseResolver};
use crate::metrics::MetricsRegistry;

pub(crate) mod descriptor;
pub use transferia_registry::{DeliveryMode, EndpointDefinition, ProviderDefinition};
pub type ProviderCatalog = transferia_registry::Registry;

pub(crate) use descriptor::{
    installation_contract, provider_contracts, provider_roles, provider_supports_role,
};
use transferia_registry::{ComponentRegistration, RegistryBuilder};

#[cfg(test)]
use descriptor::PROVIDERS;

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
    for binding in registry.option_bindings() {
        anyhow::ensure!(
            provider_supports_role(binding.provider, binding.role),
            "options binding targets unknown provider role '{}.{:?}'",
            binding.provider,
            binding.role
        );
        anyhow::ensure!(
            registry.option_keys().any(|key| key == binding.source),
            "options binding references unknown dynamic option source '{}'",
            binding.source
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
    let mut catalog = build_base_provider_catalog(metrics_registry)?;
    catalog.replace_definitions(transferia.composition().provider_definitions().to_vec())?;
    Ok(catalog)
}

pub(crate) fn compile_provider_definitions(
    registry: &ExtensionRegistry,
) -> anyhow::Result<Vec<ProviderDefinition>> {
    let metrics = Arc::new(MetricsRegistry::new());
    let mut catalog = build_base_provider_catalog(&metrics)?;
    catalog.edit_definitions(|definitions| {
        for definition in definitions.iter_mut() {
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
        apply_dynamic_options_bindings(definitions, registry)?;
        apply_external_link_bindings(definitions, registry)?;
        Ok(())
    })?;
    Ok(catalog.definitions().to_vec())
}

pub(crate) fn compile_middleware_definitions(
) -> anyhow::Result<Vec<transferia_registry::MiddlewareDefinition>> {
    Ok(
        build_base_provider_catalog(&Arc::new(MetricsRegistry::new()))?
            .middleware_definitions()
            .to_vec(),
    )
}

fn apply_external_link_bindings(
    definitions: &mut [ProviderDefinition],
    registry: &ExtensionRegistry,
) -> anyhow::Result<()> {
    for binding in registry.external_link_bindings() {
        let definition = definitions
            .iter_mut()
            .find(|definition| definition.key == binding.provider)
            .ok_or_else(|| anyhow::anyhow!("unknown provider '{}'", binding.provider))?;
        let endpoint = match binding.role {
            EndpointRole::Source => definition.source.as_mut(),
            EndpointRole::Sink => definition.sink.as_mut(),
        }
        .ok_or_else(|| {
            anyhow::anyhow!(
                "provider '{}' does not support {:?}",
                binding.provider,
                binding.role
            )
        })?;
        let field_schema = endpoint
            .schema
            .pointer_mut(binding.schema_pointer)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "external link '{}.{:?}{}' points to a missing schema node",
                    binding.provider,
                    binding.role,
                    binding.schema_pointer
                )
            })?;
        let ui = field_schema
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("external link schema node must be an object"))?
            .entry("x-ui")
            .or_insert_with(|| serde_json::json!({}))
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("external link x-ui must be an object"))?;
        ui.insert(
            "external_link_template".to_owned(),
            JsonValue::String(binding.url_template.to_owned()),
        );
    }
    Ok(())
}

fn apply_dynamic_options_bindings(
    definitions: &mut [ProviderDefinition],
    registry: &ExtensionRegistry,
) -> anyhow::Result<()> {
    for binding in registry.option_bindings() {
        let definition = definitions
            .iter_mut()
            .find(|definition| definition.key == binding.provider)
            .ok_or_else(|| anyhow::anyhow!("unknown provider '{}'", binding.provider))?;
        let endpoint = match binding.role {
            EndpointRole::Source => definition.source.as_mut(),
            EndpointRole::Sink => definition.sink.as_mut(),
        }
        .ok_or_else(|| {
            anyhow::anyhow!(
                "provider '{}' does not support {:?}",
                binding.provider,
                binding.role
            )
        })?;
        let field_schema = endpoint
            .schema
            .pointer_mut(binding.schema_pointer)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "options binding '{}.{:?}{}' points to a missing schema node",
                    binding.provider,
                    binding.role,
                    binding.schema_pointer
                )
            })?;
        let object = field_schema
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("options binding schema node must be an object"))?;
        let ui = object
            .entry("x-ui")
            .or_insert_with(|| serde_json::json!({}))
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("options binding x-ui must be an object"))?;
        ui.insert(
            "dynamic_options".to_owned(),
            JsonValue::String(binding.source.to_owned()),
        );
        ui.insert(
            "dynamic_options_dependencies".to_owned(),
            serde_json::to_value(&binding.dependencies)?,
        );
        if binding.control == crate::extension::DynamicOptionsControl::Path {
            ui.insert(
                "dynamic_options_control".to_owned(),
                JsonValue::String("path".to_owned()),
            );
        }
    }
    Ok(())
}

fn build_base_provider_catalog(
    _metrics_registry: &Arc<MetricsRegistry>,
) -> anyhow::Result<ProviderCatalog> {
    let mut catalog = RegistryBuilder::new();
    transferia_middleware_filter::register(&mut catalog)?;
    transferia_middleware_datafusion::register(&mut catalog)?;

    transferia_provider_logbroker::register(&mut catalog, _metrics_registry)?;

    transferia_provider_kafka::register(&mut catalog, _metrics_registry)?;

    transferia_provider_postgres::register(&mut catalog, _metrics_registry)?;

    transferia_provider_clickhouse::register(&mut catalog, _metrics_registry)?;

    transferia_provider_s3::register(&mut catalog, _metrics_registry)?;

    transferia_provider_iceberg::register(&mut catalog, _metrics_registry)?;

    transferia_provider_ytsaurus::register(&mut catalog, _metrics_registry)?;

    catalog.register(
        component_registration("data_generator")?
            .source::<crate::providers::generator::DataGeneratorConfig, _, _>(
            vec![DeliveryMode::Batch],
            false,
            || {
                serde_json::json!({
                    "table_name": "",
                    "column_count": 10,
                    "data_size_bytes": 107_374_182_400_u64,
                    "batch_rows": 65_536
                })
            },
            {
                let metrics = Arc::clone(_metrics_registry);
                move |config| {
                    Ok(Box::new(
                        crate::providers::generator::DataGeneratorSourceProvider::from_config(
                            config,
                            Arc::clone(&metrics),
                        )?,
                    ))
                }
            },
        )?,
    )?;

    catalog.register(
        component_registration("discard")?.sink::<EmptyConfig, _, _>(
            || serde_json::json!({}),
            |_config| {
                Ok(Box::new(
                    crate::providers::discard::provider::DiscardSinkProvider,
                ))
            },
        )?,
    )?;

    Ok(catalog.build())
}

fn component_registration(key: &'static str) -> anyhow::Result<ComponentRegistration> {
    let descriptor = descriptor::provider_descriptor(key)
        .ok_or_else(|| anyhow::anyhow!("unknown provider descriptor '{key}'"))?;
    Ok(ComponentRegistration::new(descriptor.key, descriptor.title))
}

pub fn register_builtin_installations(_registry: &mut ExtensionRegistry) -> anyhow::Result<()> {
    register_on_premise(
        _registry,
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
        _registry,
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
        _registry,
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
        _registry,
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
        _registry,
        "logbroker",
        EndpointRole::Source,
        serde_json::json!({
            "host": { "type": "string", "title": "Host" },
            "port": { "type": "integer", "title": "Port", "minimum": 1, "maximum": 65535 },
            "trusted_plaintext": { "type": "boolean", "x-ui": { "widget": "hidden" } }
        }),
        &["host", "port", "trusted_plaintext"],
        serde_json::json!({ "host": "", "port": 2135, "trusted_plaintext": true }),
    )?;
    for role in [EndpointRole::Source, EndpointRole::Sink] {
        register_on_premise(
            _registry,
            "kafka",
            role,
            serde_json::json!({
                "brokers": {
                    "type": "array",
                    "title": "Brokers",
                    "items": { "type": "string" },
                    "x-ui": { "initial_items": 1 }
                },
                "security": {
                    "oneOf": [
                        {
                            "type": "object",
                            "title": "Plaintext",
                            "properties": { "type": { "const": "plaintext" } },
                            "required": ["type"],
                            "additionalProperties": false
                        },
                        {
                            "type": "object",
                            "title": "TLS",
                            "properties": {
                                "type": { "const": "tls" },
                                "ca_file": {
                                    "anyOf": [{ "type": "string" }, { "type": "null" }],
                                    "title": "CA file"
                                }
                            },
                            "required": ["type"],
                            "additionalProperties": false
                        },
                        {
                            "type": "object",
                            "title": "SASL over TLS",
                            "properties": {
                                "type": { "const": "sasl_tls" },
                                "username": { "type": "string", "title": "Username" },
                                "password": {
                                    "type": "string",
                                    "title": "Password",
                                    "x-ui": { "widget": "password" }
                                },
                                "mechanism": {
                                    "type": "string",
                                    "title": "Mechanism",
                                    "enum": ["scram_sha256", "scram_sha512"]
                                },
                                "ca_file": {
                                    "anyOf": [{ "type": "string" }, { "type": "null" }],
                                    "title": "CA file"
                                }
                            },
                            "required": ["type", "username", "password", "mechanism"],
                            "additionalProperties": false
                        }
                    ]
                }
            }),
            &["brokers", "security"],
            serde_json::json!({ "brokers": [""], "security": { "type": "plaintext" } }),
        )?;
    }
    register_on_premise(
        _registry,
        "logbroker",
        EndpointRole::Sink,
        serde_json::json!({
            "host": { "type": "string", "title": "Host" },
            "port": { "type": "integer", "title": "Port", "minimum": 1, "maximum": 65535 },
            "trusted_plaintext": { "type": "boolean", "x-ui": { "widget": "hidden" } }
        }),
        &["host", "port", "trusted_plaintext"],
        serde_json::json!({ "host": "", "port": 2135, "trusted_plaintext": true }),
    )?;
    register_on_premise(
        _registry,
        "ytsaurus",
        EndpointRole::Source,
        ytsaurus_on_premise_schema(),
        &["host", "port", "trusted_plaintext"],
        serde_json::json!({ "host": "", "port": 8000, "trusted_plaintext": true }),
    )?;
    register_on_premise(
        _registry,
        "ytsaurus",
        EndpointRole::Sink,
        ytsaurus_on_premise_schema(),
        &["host", "port", "trusted_plaintext"],
        serde_json::json!({ "host": "", "port": 8000, "trusted_plaintext": true }),
    )?;
    Ok(())
}

fn ytsaurus_on_premise_schema() -> JsonValue {
    serde_json::json!({
        "host": { "type": "string", "title": "Host" },
        "port": { "type": "integer", "title": "Port", "minimum": 1, "maximum": 65535 },
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
    registry.register_erased_installation(InstallationRegistration {
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
        "x-ui": { "control_width": "installation" },
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
    let mut endpoint_properties = std::mem::take(properties);
    for field in registry.fields_before_installation(provider, role) {
        anyhow::ensure!(
            !replaced.contains(field),
            "field placement cannot target installation-owned field '{provider}.{field}'"
        );
        let field_schema = endpoint_properties.remove(field).ok_or_else(|| {
            anyhow::anyhow!("field placement targets unknown field '{provider}.{field}'")
        })?;
        properties.insert(field.to_owned(), field_schema);
    }
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
