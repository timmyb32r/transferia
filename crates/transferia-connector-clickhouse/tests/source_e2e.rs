#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test assertions intentionally fail fast"
)]

use arrow::array::{Array as _, BinaryArray, Int64Array};
use std::sync::Arc;
use testcontainers::core::wait::HttpWaitStrategy;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner as _;
use testcontainers::{GenericImage, ImageExt as _};
use tokio_util::sync::CancellationToken;

use transferia_connector_clickhouse::clickhouse::ClickHouseSourceConnector;
use transferia_core::data::message::SourceBatch;
use transferia_core::delivery::{DeliveryDiscoveryRequest, SchemaOrigin};
use transferia_core::memory::PipelineMemory;
use transferia_core::source::Source as _;
use transferia_delivery_contracts::metrics::MetricsRegistry;
use transferia_registry::{SourceBuildContext, SourceConnector as _, SourceDiscoveryContext};

const IMAGE: &str = "clickhouse/clickhouse-server";
const TAG: &str = "25.8.28.1";

fn reachable_host(host: &impl ToString) -> String {
    let host = host.to_string();
    if host == "localhost" {
        "127.0.0.1".into()
    } else {
        host
    }
}

#[tokio::test]
async fn clickhouse_source_discovers_and_streams_a_deterministic_native_snapshot(
) -> anyhow::Result<()> {
    let container = GenericImage::new(IMAGE, TAG)
        .with_exposed_port(9000.tcp())
        .with_exposed_port(8123.tcp())
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new("/ping")
                .with_port(8123.tcp())
                .with_expected_status_code(200_u16),
        ))
        .with_env_var("CLICKHOUSE_SKIP_USER_SETUP", "1")
        .start()
        .await?;
    let host = reachable_host(&container.get_host().await?);
    let native_port = container.get_host_port_ipv4(9000.tcp()).await?;
    let http_port = container.get_host_port_ipv4(8123.tcp()).await?;
    let http = reqwest::Client::new();
    for query in [
        "CREATE TABLE events (id Int64, payload Nullable(String)) ENGINE=MergeTree ORDER BY id",
        "INSERT INTO events VALUES (2, NULL), (1, 'one'), (3, 'three')",
    ] {
        http.post(format!("http://{host}:{http_port}"))
            .body(query)
            .send()
            .await?
            .error_for_status()?;
    }

    let connector = ClickHouseSourceConnector::from_config(
        serde_yaml::from_str(&format!("hosts: ['{host}']\nport: {native_port}\ntrusted_plaintext: true\nusername: default\nbatch_rows: 2\nsnapshot_reader: {{ type: native, max_threads: 1, compression: lz4 }}\ntables:\n  - database: default\n    name: events\n"))?,
        Arc::new(MetricsRegistry::new()),
    )?;
    let discovery = connector
        .delivery_discovery(SourceDiscoveryContext {
            request: DeliveryDiscoveryRequest {
                keep_system_columns: false,
            },
            cancellation: CancellationToken::new(),
        })
        .await?;
    assert_eq!(discovery.schema_origin, SchemaOrigin::SourceNative);
    assert_eq!(discovery.datasets[0].stored_schema.columns.len(), 2);

    let mut source = connector
        .build_source(SourceBuildContext {
            partition_id: 0,
            cancellation: CancellationToken::new(),
            memory: PipelineMemory::new(256 * 1024 * 1024),
            durable: transferia_test_support::durable_context(),
        })
        .await?;
    let first = source.read_batch().await?;
    let second = source.read_batch().await?;
    assert!(matches!(source.read_batch().await?, SourceBatch::Finished));

    let batches = [first, second]
        .into_iter()
        .map(|source| match source {
            SourceBatch::Typed { tables, .. } => {
                Ok(tables.into_iter().next().expect("one table").batch)
            }
            SourceBatch::Raw { .. } | SourceBatch::Finished => {
                anyhow::bail!("expected typed ClickHouse source batch")
            }
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let ids = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, [1, 2, 3]);
    let values = batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(1)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            (0..array.len())
                .map(|row| {
                    if array.is_null(row) {
                        None
                    } else {
                        Some(array.value(row).to_vec())
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        values,
        [Some(b"one".to_vec()), None, Some(b"three".to_vec())]
    );
    Ok(())
}
