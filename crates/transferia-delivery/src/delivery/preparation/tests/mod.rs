use super::*;

mod integration;

#[tokio::test]
async fn plan_rejects_zero_pipeline_memory_before_discovery() -> anyhow::Result<()> {
    let yaml = r"
delivery_id: plan-test
durable_storage: { type: local_file, path: /tmp/transferia-plan-test }
delivery_type: batch
source:
  postgres:
    host: localhost
    port: 5432
    database: postgres
    username: postgres
    password: postgres
    trusted_plaintext: true
    tables: [{ schema: public, name: events }]
    batch_rows: 1
sink: { discard: {} }
pipeline_memory_limit_bytes: 0
";
    let composition = transferia_providers::extension::Transferia::public()?;
    let error = build_delivery_plan_with(
        Config::from_yaml(yaml)?,
        CancellationToken::new(),
        &composition,
    )
    .await
    .err()
    .context("zero memory limit must fail")?;
    assert!(error
        .to_string()
        .contains("pipeline_memory_limit_bytes must be positive"));
    Ok(())
}
