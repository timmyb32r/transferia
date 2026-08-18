use std::sync::Arc;

use serde_yaml::{Mapping, Value};

use super::*;

#[test]
fn external_links_require_a_safe_unambiguous_template() -> anyhow::Result<()> {
    let mut registry = ExtensionRegistry::default();
    registry.register_external_link(
        "logbroker",
        EndpointRole::Source,
        "/properties/consumer_name",
        "https://console.example/consumers/{value}",
    )?;
    assert_eq!(registry.external_link_bindings().count(), 1);
    assert!(registry
        .register_external_link(
            "logbroker",
            EndpointRole::Source,
            "/properties/consumer_name",
            "https://console.example/duplicate/{value}",
        )
        .is_err());
    assert!(registry
        .register_external_link(
            "logbroker",
            EndpointRole::Sink,
            "/properties/topic",
            "javascript:{value}",
        )
        .is_err());
    Ok(())
}

#[tokio::test]
async fn on_premise_resolution_flattens_only_installation_fields() -> anyhow::Result<()> {
    let transferia = Transferia::public()?;
    let resolved = transferia
        .registry()
        .resolve(
            "postgres",
            EndpointRole::Source,
            serde_yaml::from_str(
                "installation: { type: on_premise, host: localhost, port: 5432, trusted_plaintext: true }\ndatabase: postgres\nusername: user\npassword: secret\ntables: []\n",
            )?,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(
        resolved.get("host"),
        Some(&Value::String("localhost".to_owned()))
    );
    assert!(resolved.get("installation").is_none());
    assert_eq!(
        resolved.get("database"),
        Some(&Value::String("postgres".to_owned()))
    );
    Ok(())
}

#[tokio::test]
async fn public_composition_rejects_plugin_only_installations() -> anyhow::Result<()> {
    let transferia = Transferia::public()?;
    let error = transferia
        .registry()
        .resolve(
            "postgres",
            EndpointRole::Source,
            serde_yaml::from_str(
                "installation: { type: plugin_only }\ndatabase: postgres\nusername: user\npassword: secret\ntables: []\n",
            )?,
            CancellationToken::new(),
        )
        .await
        .expect_err("public composition must reject plugin-only installation types");
    assert!(error
        .to_string()
        .contains("unknown postgres installation type 'plugin_only'"));
    Ok(())
}

#[tokio::test]
async fn resolution_rejects_unknown_provider_roles_before_using_raw_config() -> anyhow::Result<()> {
    let transferia = Transferia::public()?;
    let error = transferia
        .registry()
        .resolve(
            "typo",
            EndpointRole::Source,
            Value::Mapping(Mapping::default()),
            CancellationToken::new(),
        )
        .await
        .expect_err("unknown providers must not bypass installation resolution");
    assert!(error.to_string().contains("unknown Source provider 'typo'"));
    Ok(())
}

struct DuplicateExtension;

impl TransferiaExtension for DuplicateExtension {
    fn identity(&self) -> ExtensionIdentity {
        ExtensionIdentity {
            package: "test-duplicate",
            abi_version: 1,
        }
    }

    fn register(&self, registry: &mut ExtensionRegistry) -> anyhow::Result<()> {
        registry.register_options("duplicates", Arc::new(EmptyOptions))?;
        registry.register_options("duplicates", Arc::new(EmptyOptions))
    }
}

struct EmptyOptions;

#[async_trait]
impl DynamicOptionsProvider for EmptyOptions {
    async fn list(
        &self,
        _request: OptionsRequest,
        _context: OptionsContext,
    ) -> anyhow::Result<DynamicOptions> {
        Ok(DynamicOptions::default())
    }
}

struct UnknownProviderExtension;

impl TransferiaExtension for UnknownProviderExtension {
    fn identity(&self) -> ExtensionIdentity {
        ExtensionIdentity {
            package: "test-unknown-provider",
            abi_version: 1,
        }
    }

    fn register(&self, registry: &mut ExtensionRegistry) -> anyhow::Result<()> {
        registry.register_erased_installation(InstallationRegistration {
            provider: "typo",
            role: EndpointRole::Source,
            kind: "instance",
            title: "Instance",
            schema: serde_json::json!({
                "type": "object",
                "properties": { "type": { "const": "instance" } },
                "required": ["type"],
                "additionalProperties": false
            }),
            initial: serde_json::json!({ "type": "instance" }),
            preferred: false,
            resolver: Arc::new(OnPremiseResolver),
        })
    }
}

#[test]
fn composition_rejects_unknown_provider_roles() {
    let Err(error) = TransferiaBuilder::new()
        .with_extension(Arc::new(UnknownProviderExtension))
        .build()
    else {
        panic!("unknown provider role unexpectedly compiled");
    };
    assert!(error.to_string().contains("unknown provider role"));
}

#[test]
fn composition_fingerprint_is_stable_and_identifies_extensions() -> anyhow::Result<()> {
    let public_a = Transferia::public()?;
    let public_b = Transferia::public()?;
    assert_eq!(
        public_a.composition_fingerprint(),
        public_b.composition_fingerprint()
    );

    let extended = TransferiaBuilder::new()
        .with_extension(Arc::new(DuplicateFreeExtension))
        .build()?;
    assert_ne!(
        public_a.composition_fingerprint(),
        extended.composition_fingerprint()
    );
    Ok(())
}

struct AbiExtension(u32);

impl TransferiaExtension for AbiExtension {
    fn identity(&self) -> ExtensionIdentity {
        ExtensionIdentity {
            package: "fingerprint-abi",
            abi_version: self.0,
        }
    }

    fn register(&self, registry: &mut ExtensionRegistry) -> anyhow::Result<()> {
        registry.register_options("fingerprint-abi-options", Arc::new(EmptyOptions))
    }
}

struct OrderedExtension {
    package: &'static str,
    option: &'static str,
}

impl TransferiaExtension for OrderedExtension {
    fn identity(&self) -> ExtensionIdentity {
        ExtensionIdentity {
            package: self.package,
            abi_version: 1,
        }
    }

    fn register(&self, registry: &mut ExtensionRegistry) -> anyhow::Result<()> {
        registry.register_options(self.option, Arc::new(EmptyOptions))
    }
}

struct FingerprintInstallations {
    alpha_preferred: bool,
    schema_revision: u32,
}

impl TransferiaExtension for FingerprintInstallations {
    fn identity(&self) -> ExtensionIdentity {
        ExtensionIdentity {
            package: "fingerprint-installations",
            abi_version: 1,
        }
    }

    fn register(&self, registry: &mut ExtensionRegistry) -> anyhow::Result<()> {
        for (kind, preferred) in [
            ("fingerprint_alpha", self.alpha_preferred),
            ("fingerprint_beta", !self.alpha_preferred),
        ] {
            registry.register_erased_installation(InstallationRegistration {
                provider: "postgres",
                role: EndpointRole::Source,
                kind,
                title: kind,
                schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "type": { "const": kind },
                        "revision": { "const": self.schema_revision }
                    },
                    "required": ["type", "revision"],
                    "additionalProperties": false
                }),
                initial: serde_json::json!({
                    "type": kind,
                    "revision": self.schema_revision
                }),
                preferred,
                resolver: Arc::new(OnPremiseResolver),
            })?;
        }
        Ok(())
    }
}

#[test]
fn composition_fingerprint_covers_abi_schema_preference_and_is_order_independent(
) -> anyhow::Result<()> {
    let abi_one = TransferiaBuilder::new()
        .with_extension(Arc::new(AbiExtension(1)))
        .build()?;
    let abi_two = TransferiaBuilder::new()
        .with_extension(Arc::new(AbiExtension(2)))
        .build()?;
    assert_ne!(
        abi_one.composition_fingerprint(),
        abi_two.composition_fingerprint()
    );

    let schema_one = TransferiaBuilder::new()
        .with_extension(Arc::new(FingerprintInstallations {
            alpha_preferred: true,
            schema_revision: 1,
        }))
        .build()?;
    let schema_two = TransferiaBuilder::new()
        .with_extension(Arc::new(FingerprintInstallations {
            alpha_preferred: true,
            schema_revision: 2,
        }))
        .build()?;
    let preferred_beta = TransferiaBuilder::new()
        .with_extension(Arc::new(FingerprintInstallations {
            alpha_preferred: false,
            schema_revision: 1,
        }))
        .build()?;
    assert_ne!(
        schema_one.composition_fingerprint(),
        schema_two.composition_fingerprint()
    );
    assert_ne!(
        schema_one.composition_fingerprint(),
        preferred_beta.composition_fingerprint()
    );

    let left = Arc::new(OrderedExtension {
        package: "ordered-left",
        option: "ordered-left-option",
    });
    let right = Arc::new(OrderedExtension {
        package: "ordered-right",
        option: "ordered-right-option",
    });
    let left_first = TransferiaBuilder::new()
        .with_extension(left.clone())
        .with_extension(right.clone())
        .build()?;
    let right_first = TransferiaBuilder::new()
        .with_extension(right)
        .with_extension(left)
        .build()?;
    assert_eq!(
        left_first.composition_fingerprint(),
        right_first.composition_fingerprint()
    );
    Ok(())
}

struct IncompleteResolverExtension;

impl TransferiaExtension for IncompleteResolverExtension {
    fn identity(&self) -> ExtensionIdentity {
        ExtensionIdentity {
            package: "test-incomplete-resolver",
            abi_version: 1,
        }
    }

    fn register(&self, registry: &mut ExtensionRegistry) -> anyhow::Result<()> {
        registry.register_erased_installation(InstallationRegistration {
            provider: "postgres",
            role: EndpointRole::Source,
            kind: "incomplete",
            title: "Incomplete",
            schema: serde_json::json!({
                "type": "object",
                "properties": { "type": { "const": "incomplete" } },
                "required": ["type"],
                "additionalProperties": false
            }),
            initial: serde_json::json!({ "type": "incomplete" }),
            preferred: true,
            resolver: Arc::new(IncompleteResolver),
        })
    }
}

struct IncompleteResolver;

#[async_trait]
impl InstallationResolver for IncompleteResolver {
    async fn resolve(
        &self,
        _installation: Value,
        _context: ResolveContext,
    ) -> anyhow::Result<serde_yaml::Mapping> {
        let mut output = serde_yaml::Mapping::new();
        output.insert(Value::from("host"), Value::from("localhost"));
        Ok(output)
    }
}

#[tokio::test]
async fn resolver_must_satisfy_the_declared_output_contract() -> anyhow::Result<()> {
    let transferia = TransferiaBuilder::new()
        .with_extension(Arc::new(IncompleteResolverExtension))
        .build()?;
    let error = transferia
        .registry()
        .resolve(
            "postgres",
            EndpointRole::Source,
            serde_yaml::from_str("installation: { type: incomplete }\n")?,
            CancellationToken::new(),
        )
        .await
        .expect_err("incomplete resolver output unexpectedly succeeded");
    assert!(error.to_string().contains("omitted required"));
    Ok(())
}

struct BlockingResolverExtension;

impl TransferiaExtension for BlockingResolverExtension {
    fn identity(&self) -> ExtensionIdentity {
        ExtensionIdentity {
            package: "test-blocking-resolver",
            abi_version: 1,
        }
    }

    fn register(&self, registry: &mut ExtensionRegistry) -> anyhow::Result<()> {
        registry.register_erased_installation(InstallationRegistration {
            provider: "postgres",
            role: EndpointRole::Source,
            kind: "blocking",
            title: "Blocking",
            schema: serde_json::json!({
                "type": "object",
                "properties": { "type": { "const": "blocking" } },
                "required": ["type"],
                "additionalProperties": false
            }),
            initial: serde_json::json!({ "type": "blocking" }),
            preferred: true,
            resolver: Arc::new(BlockingResolver),
        })
    }
}

struct BlockingResolver;

#[async_trait]
impl InstallationResolver for BlockingResolver {
    async fn resolve(
        &self,
        _installation: Value,
        context: ResolveContext,
    ) -> anyhow::Result<Mapping> {
        context.cancellation.cancelled().await;
        anyhow::bail!("resolver observed cancellation")
    }
}

#[tokio::test]
async fn installation_resolution_is_cancellable() -> anyhow::Result<()> {
    let transferia = TransferiaBuilder::new()
        .with_extension(Arc::new(BlockingResolverExtension))
        .build()?;
    let cancellation = CancellationToken::new();
    let trigger = cancellation.clone();
    tokio::spawn(async move {
        tokio::task::yield_now().await;
        trigger.cancel();
    });

    let error = transferia
        .registry()
        .resolve(
            "postgres",
            EndpointRole::Source,
            serde_yaml::from_str("installation: { type: blocking }\n")?,
            cancellation,
        )
        .await
        .expect_err("cancelled resolver unexpectedly succeeded");
    assert!(error.to_string().contains("cancel"));
    Ok(())
}

struct BlockingOptions;

#[async_trait]
impl DynamicOptionsProvider for BlockingOptions {
    async fn list(
        &self,
        _request: OptionsRequest,
        context: OptionsContext,
    ) -> anyhow::Result<DynamicOptions> {
        context.cancellation.cancelled().await;
        anyhow::bail!("options provider observed cancellation")
    }
}

#[tokio::test]
async fn dynamic_options_are_cancellable() -> anyhow::Result<()> {
    let mut registry = ExtensionRegistry::default();
    registry.register_options("blocking", Arc::new(BlockingOptions))?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let error = registry
        .options(
            "blocking",
            OptionsRequest {
                query: None,
                refresh: false,
                dependencies: BTreeMap::default(),
            },
            cancellation,
        )
        .await
        .expect_err("cancelled option request unexpectedly succeeded");
    assert!(error.to_string().contains("cancel"));
    Ok(())
}

#[derive(Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
struct TypedTestInstallation {
    #[serde(rename = "type")]
    installation_type: String,

    host: String,

    port: u16,

    trusted_plaintext: bool,
}

#[derive(Serialize)]
struct TypedTestOutput {
    host: String,

    port: u16,

    trusted_plaintext: bool,
}

struct TypedTestResolver;

#[async_trait]
impl TypedInstallationResolver<TypedTestInstallation, TypedTestOutput> for TypedTestResolver {
    async fn resolve(
        &self,
        installation: TypedTestInstallation,
        _context: ResolveContext,
    ) -> anyhow::Result<TypedTestOutput> {
        anyhow::ensure!(installation.installation_type == "typed", "invalid type");
        Ok(TypedTestOutput {
            host: installation.host,
            port: installation.port,
            trusted_plaintext: installation.trusted_plaintext,
        })
    }
}

struct TypedTestExtension;

impl TransferiaExtension for TypedTestExtension {
    fn identity(&self) -> ExtensionIdentity {
        ExtensionIdentity {
            package: "typed-test",
            abi_version: 1,
        }
    }

    fn register(&self, registry: &mut ExtensionRegistry) -> anyhow::Result<()> {
        registry.register_installation(
            InstallationSpec {
                provider: "postgres",
                role: EndpointRole::Source,
                kind: "typed",
                title: "Typed",
                initial: TypedTestInstallation {
                    installation_type: "typed".to_owned(),
                    host: String::new(),
                    port: 5432,
                    trusted_plaintext: false,
                },
                preferred: true,
            },
            TypedTestResolver,
        )
    }
}

#[tokio::test]
async fn typed_installation_derives_schema_initial_and_runtime_codec() -> anyhow::Result<()> {
    let transferia = TransferiaBuilder::new()
        .with_extension(Arc::new(TypedTestExtension))
        .build()?;
    let registration = transferia
        .registry()
        .installations_for("postgres", EndpointRole::Source)
        .into_iter()
        .find(|registration| registration.kind == "typed")
        .ok_or_else(|| anyhow::anyhow!("typed installation was not compiled"))?;
    assert_eq!(registration.schema["properties"]["type"]["const"], "typed");
    assert_eq!(registration.initial["port"], 5432);
    let resolved = transferia
        .registry()
        .resolve(
            "postgres",
            EndpointRole::Source,
            serde_yaml::from_str(
                "installation: { type: typed, host: localhost, port: 5432, trusted_plaintext: true }\n",
            )?,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(resolved["host"], "localhost");
    Ok(())
}

struct DuplicateFreeExtension;

impl TransferiaExtension for DuplicateFreeExtension {
    fn identity(&self) -> ExtensionIdentity {
        ExtensionIdentity {
            package: "test-fingerprint",
            abi_version: 1,
        }
    }

    fn register(&self, registry: &mut ExtensionRegistry) -> anyhow::Result<()> {
        registry.register_options("fingerprint-test", Arc::new(EmptyOptions))
    }
}

struct NoPreferredInstallationExtension;

impl TransferiaExtension for NoPreferredInstallationExtension {
    fn identity(&self) -> ExtensionIdentity {
        ExtensionIdentity {
            package: "test-no-preferred",
            abi_version: 1,
        }
    }

    fn register(&self, registry: &mut ExtensionRegistry) -> anyhow::Result<()> {
        registry.register_erased_installation(InstallationRegistration {
            provider: "postgres",
            role: EndpointRole::Source,
            kind: "alternative",
            title: "Alternative",
            schema: serde_json::json!({
                "type": "object",
                "properties": { "type": { "const": "alternative" } },
                "required": ["type"],
                "additionalProperties": false
            }),
            initial: serde_json::json!({ "type": "alternative" }),
            preferred: false,
            resolver: Arc::new(OnPremiseResolver),
        })
    }
}

#[test]
fn multiple_installations_require_exactly_one_preferred_variant() {
    let Err(error) = TransferiaBuilder::new()
        .with_extension(Arc::new(NoPreferredInstallationExtension))
        .build()
    else {
        panic!("ambiguous installation default unexpectedly compiled");
    };
    assert!(error.to_string().contains("exactly one preferred"));
}

#[test]
fn duplicate_extension_keys_are_rejected() {
    let Err(error) = TransferiaBuilder::new()
        .with_extension(Arc::new(DuplicateExtension))
        .build()
    else {
        panic!("duplicate option source unexpectedly succeeded");
    };
    assert!(error.to_string().contains("registered more than once"));
}
