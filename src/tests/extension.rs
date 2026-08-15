use std::sync::Arc;

use serde_yaml::Value;

use super::*;

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
                "installation: { type: managed_service, cluster_id: cluster-id }\ndatabase: postgres\nusername: user\npassword: secret\ntables: []\n",
            )?,
        )
        .await
        .expect_err("public composition must reject plugin-only installation types");
    assert!(error
        .to_string()
        .contains("unknown postgres installation type 'managed_service'"));
    Ok(())
}

struct DuplicateExtension;

impl TransferiaExtension for DuplicateExtension {
    fn register(&self, registry: &mut ExtensionRegistry) -> anyhow::Result<()> {
        registry.register_options("duplicates", Arc::new(EmptyOptions))?;
        registry.register_options("duplicates", Arc::new(EmptyOptions))
    }
}

struct EmptyOptions;

#[async_trait]
impl DynamicOptionsProvider for EmptyOptions {
    async fn list(&self, _request: OptionsRequest) -> anyhow::Result<DynamicOptions> {
        Ok(DynamicOptions::default())
    }
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
