#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test assertions intentionally fail fast"
)]

mod support;

use std::sync::Arc;

use arrow::array::Int64Array;
use object_store::path::Path;
use object_store::ObjectStore as _;
use testcontainers::core::wait::HttpWaitStrategy;
use testcontainers::core::{ExecCommand, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner as _;
use testcontainers::{GenericImage, ImageExt as _};
use tokio_util::sync::CancellationToken;

use transferia::delivery::DeliveryDiscoveryRequest;
use transferia::metrics::MetricsRegistry;
use transferia::pipeline::memory::PipelineMemory;
use transferia::pipeline::source::Source as _;
use transferia::providers::s3::S3SourceProvider;
use transferia::providers::traits::SourceProvider as _;
use transferia::types::message::SourceBatch;

const IMAGE: &str = "localstack/localstack";
const TAG: &str = "4.14.0";

fn reachable_host(host: &impl ToString) -> String {
    let host = host.to_string();
    if host == "localhost" {
        "127.0.0.1".into()
    } else {
        host
    }
}

#[tokio::test]
async fn s3_source_snapshots_sorted_objects_and_parses_json() -> anyhow::Result<()> {
    let container = GenericImage::new(IMAGE, TAG)
        .with_exposed_port(4566.tcp())
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new("/_localstack/health")
                .with_port(4566.tcp())
                .with_expected_status_code(200_u16),
        ))
        .with_env_var("SERVICES", "s3")
        .start()
        .await?;
    let host = reachable_host(&container.get_host().await?);
    let port = container.get_host_port_ipv4(4566.tcp()).await?;
    let mut create_bucket = container
        .exec(ExecCommand::new([
            "awslocal",
            "s3api",
            "create-bucket",
            "--bucket",
            "transferia-source-e2e",
            "--region",
            "us-east-1",
        ]))
        .await?;
    let stderr = String::from_utf8(create_bucket.stderr_to_vec().await?)?;
    anyhow::ensure!(
        create_bucket.exit_code().await? == Some(0),
        "LocalStack bucket creation failed: {stderr}"
    );

    let endpoint = format!("http://{host}:{port}");
    let store = object_store::aws::AmazonS3Builder::new()
        .with_bucket_name("transferia-source-e2e")
        .with_region("us-east-1")
        .with_endpoint(&endpoint)
        .with_allow_http(true)
        .with_access_key_id("test")
        .with_secret_access_key("test")
        .build()?;
    store
        .put(
            &Path::from("snapshot/02.json"),
            bytes::Bytes::from_static(br#"{"id":2}"#).into(),
        )
        .await?;
    store
        .put(
            &Path::from("snapshot/01.json"),
            bytes::Bytes::from_static(br#"{"id":1}"#).into(),
        )
        .await?;

    let provider = S3SourceProvider::from_config(
        serde_yaml::from_str(&format!(
            "bucket: transferia-source-e2e\nprefix: snapshot\nregion: us-east-1\nhost: '{host}'\nport: {port}\nallow_http: true\ncredentials: {{ access_key: test, secret_key: test }}\nparser:\n  common:\n    table_naming: {{ type: from_config, name: events }}\n  json_parser:\n    conversion_error: dlq\n    unknown_fields: {{ action: fail }}\n    chunk_splitter: one-message-one-row\n    columns:\n      - {{ jsonpath: '$.id', column_name: id, json_data_type: integer, arrow_type: Int64, nullable: false }}\n"
        ))?,
        Arc::new(MetricsRegistry::new()),
    )?;
    provider
        .delivery_discovery(
            DeliveryDiscoveryRequest {
                keep_system_columns: false,
            },
            CancellationToken::new(),
        )
        .await?;

    let mut source = provider
        .build_source(
            0,
            CancellationToken::new(),
            PipelineMemory::new(16 * 1024 * 1024),
            support::durable_context(),
        )
        .await?;
    let mut parser = provider.parser_plan().parser().create_session();
    let mut ids = Vec::new();
    for expected_key in ["snapshot/01.json", "snapshot/02.json"] {
        match source.read_batch().await? {
            SourceBatch::Raw {
                messages,
                memory: batch_memory,
                ..
            } => {
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].meta.topic.as_deref(), Some(expected_key));
                let (main, dlq) = parser.parse_into(messages)?;
                assert!(dlq.is_none());
                let column = main
                    .batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("id Int64 column");
                ids.extend_from_slice(column.values());
                drop(batch_memory);
            }
            SourceBatch::Typed { .. } | SourceBatch::Finished => {
                anyhow::bail!("expected one raw S3 object")
            }
        }
    }
    assert!(matches!(source.read_batch().await?, SourceBatch::Finished));
    assert_eq!(ids, [1, 2]);
    Ok(())
}
