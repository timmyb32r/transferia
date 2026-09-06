#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    reason = "test assertions intentionally fail fast"
)]

use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::sync::Arc;

use arrow::array::{Array as _, Int64Array, StringArray};
use reqwest::StatusCode;
use testcontainers::core::wait::HttpWaitStrategy;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner as _;
use testcontainers::{GenericImage, ImageExt as _};
use tokio_util::sync::CancellationToken;
use transferia_connector_opensearch::opensearch::src_batch::OpenSearchSourceConnector;
use transferia_core::data::message::SourceBatch;
use transferia_core::data::system_columns::SystemColumnKind;
use transferia_core::delivery::{DeliveryDiscoveryRequest, SchemaOrigin};
use transferia_core::memory::PipelineMemory;
use transferia_core::source::Source as _;
use transferia_delivery_contracts::metrics::MetricsRegistry;
use transferia_registry::{SourceBuildContext, SourceConnector as _, SourceDiscoveryContext};

const IMAGE: &str = "opensearchproject/opensearch";
const TAG: &str = "2.19.1";

fn reachable_host(host: &impl ToString) -> String {
    let host = host.to_string();
    if host == "localhost" {
        "127.0.0.1".into()
    } else {
        host
    }
}

#[tokio::test]
async fn opensearch_source_reads_one_coherent_index_pit_without_loss() -> anyhow::Result<()> {
    let container = GenericImage::new(IMAGE, TAG)
        .with_exposed_port(9200.tcp())
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new("/_cluster/health")
                .with_port(9200.tcp())
                .with_expected_status_code(200_u16),
        ))
        .with_env_var("discovery.type", "single-node")
        .with_env_var("DISABLE_SECURITY_PLUGIN", "true")
        .with_env_var("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m")
        .start()
        .await?;
    let host = reachable_host(&container.get_host().await?);
    let port = container.get_host_port_ipv4(9200.tcp()).await?;
    let http = reqwest::Client::new();
    let base = format!("http://{host}:{port}");

    http.put(format!("{base}/events"))
        .json(&serde_json::json!({
            "settings": {"number_of_shards": 3, "number_of_replicas": 0},
            "mappings": {"_source": {"enabled": true}}
        }))
        .send()
        .await?
        .error_for_status()?;
    let (first_route, second_route) = routes_on_distinct_shards(&http, &base).await?;
    let mut bulk = String::new();
    for id in 0..19 {
        if id == 0 {
            writeln!(bulk, "{{\"index\":{{\"_index\":\"events\",\"_id\":\"doc-{id}\",\"routing\":\"tenant-a\"}}}}")?;
        } else {
            writeln!(
                bulk,
                "{{\"index\":{{\"_index\":\"events\",\"_id\":\"doc-{id}\"}}}}"
            )?;
        }
        writeln!(bulk, "{{\"number\":{id},\"text\":\"event-{id}\"}}")?;
    }
    for (route, value) in [(&first_route, "first"), (&second_route, "second")] {
        writeln!(
            bulk,
            "{{\"index\":{{\"_index\":\"events\",\"_id\":\"shared-id\",\"routing\":{}}}}}",
            serde_json::to_string(route)?
        )?;
        writeln!(bulk, "{{\"route\":\"{value}\"}}")?;
    }
    http.post(format!("{base}/_bulk?refresh=wait_for"))
        .header("Content-Type", "application/x-ndjson")
        .body(bulk)
        .send()
        .await?
        .error_for_status()?;

    for read_concurrency in [1, 2] {
        let expected_count = document_count(&http, &base).await?;
        let connector = OpenSearchSourceConnector::from_config(
        serde_yaml::from_str(&format!(
            "hosts: ['{host}']\nport: {port}\ntrusted_plaintext: true\nauth: {{ type: anonymous }}\nindices: [{{ name: events }}]\npage_rows: 2\nread_concurrency: {read_concurrency}\npit_keep_alive_ms: 60000\nrequest_timeout_ms: 30000\nmax_response_bytes: 1048576\n"
        ))?,
        Arc::new(MetricsRegistry::new()),
    )?;
        let discovery = connector
            .delivery_discovery(SourceDiscoveryContext {
                request: DeliveryDiscoveryRequest {
                    keep_system_columns: false,
                },
                cancellation: CancellationToken::new(),
                delivery_type: transferia_delivery_contracts::DeliveryType::Batch,
            })
            .await?;
        assert_eq!(discovery.schema_origin, SchemaOrigin::SourceNative);
        assert_eq!(discovery.datasets.len(), 1);
        assert_eq!(discovery.datasets[0].stored_schema.columns.len(), 4);
        assert!(discovery.datasets[0].stored_schema.columns[0].primary_key);
        assert!(discovery.datasets[0].stored_schema.columns[3].primary_key);

        let mut source = connector
            .build_source(SourceBuildContext {
                partition_id: 0,
                delivery_type: transferia_delivery_contracts::DeliveryType::Batch,
                phase: transferia_registry::SourcePhase::Snapshot,
                replay_identity: None,
                cancellation: CancellationToken::new(),
                memory: PipelineMemory::new(64 * 1024 * 1024),
                durable: transferia_test_support::durable_context(),
            })
            .await?;
        let late_id = format!("late-after-pit-{read_concurrency}");
        http.put(format!("{base}/events/_doc/{late_id}?refresh=wait_for"))
            .json(&serde_json::json!({"late": true}))
            .send()
            .await?
            .error_for_status()?;
        let mut identities = HashSet::new();
        let mut offsets = HashSet::new();
        let mut routed = false;
        let mut shared_id_routes = HashSet::new();
        loop {
            match source.read_batch().await? {
                SourceBatch::Typed {
                    tables,
                    source_rows,
                    ..
                } => {
                    let table = tables.into_iter().next().expect("one table");
                    assert_eq!(table.table.as_ref(), "events");
                    assert_eq!(source_rows as usize, table.batch.num_rows());
                    let id = table
                        .batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    let routing = table
                        .batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    let source_json = table
                        .batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    let routing_key = table
                        .batch
                        .column(3)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    let offset_column = table
                        .system_columns
                        .get(SystemColumnKind::Offset)
                        .expect("offset column");
                    let offset = table
                        .batch
                        .column(offset_column.index)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    for row in 0..table.batch.num_rows() {
                        assert_ne!(id.value(row), late_id);
                        let routing_value =
                            (!routing.is_null(row)).then(|| routing.value(row).to_owned());
                        assert_eq!(
                            routing_key.value(row),
                            routing_value.as_deref().unwrap_or_else(|| id.value(row))
                        );
                        identities.insert((id.value(row).to_owned(), routing_value.clone()));
                        if id.value(row) == "doc-0" {
                            assert_eq!(routing.value(row), "tenant-a");
                            routed = true;
                        } else if id.value(row) != "shared-id" {
                            assert!(routing.is_null(row));
                        }
                        if id.value(row) == "shared-id" {
                            shared_id_routes.insert(routing_value.expect("shared id is routed"));
                        }
                        offsets.insert(offset.value(row));
                        assert!(
                            serde_json::from_str::<serde_json::Value>(source_json.value(row))?
                                .is_object()
                        );
                    }
                }
                SourceBatch::Finished => break,
                SourceBatch::Dataset { .. } | SourceBatch::Raw { .. } => {
                    anyhow::bail!("expected typed OpenSearch rows")
                }
            }
        }
        assert_eq!(identities.len(), expected_count);
        assert_eq!(offsets, (0_i64..i64::try_from(expected_count)?).collect());
        assert!(routed);
        assert_eq!(
            shared_id_routes,
            [first_route.clone(), second_route.clone()]
                .into_iter()
                .collect()
        );
        source.shutdown().await?;
        source.shutdown().await?;
    }

    let alias = http
        .post(format!("{base}/_aliases"))
        .json(&serde_json::json!({"actions": [{"add": {"index": "events", "alias": "events-alias"}}]}))
        .send()
        .await?;
    assert_eq!(alias.status(), StatusCode::OK);
    let alias_connector = OpenSearchSourceConnector::from_config(
        serde_yaml::from_str(&format!(
            "hosts: ['{host}']\nport: {port}\ntrusted_plaintext: true\nauth: {{ type: anonymous }}\nindices: [{{ name: events-alias }}]\npage_rows: 2\nread_concurrency: 1\npit_keep_alive_ms: 60000\nrequest_timeout_ms: 30000\nmax_response_bytes: 1048576\n"
        ))?,
        Arc::new(MetricsRegistry::new()),
    )?;
    assert!(alias_connector
        .delivery_discovery(SourceDiscoveryContext {
            request: DeliveryDiscoveryRequest {
                keep_system_columns: false,
            },
            cancellation: CancellationToken::new(),
            delivery_type: transferia_delivery_contracts::DeliveryType::Batch,
        })
        .await
        .is_err());
    Ok(())
}

async fn document_count(http: &reqwest::Client, base: &str) -> anyhow::Result<usize> {
    let response = http
        .get(format!("{base}/events/_count"))
        .send()
        .await?
        .error_for_status()?
        .json::<serde_json::Value>()
        .await?;
    Ok(usize::try_from(response["count"].as_u64().ok_or_else(
        || anyhow::anyhow!("OpenSearch count response omitted count"),
    )?)?)
}

async fn routes_on_distinct_shards(
    http: &reqwest::Client,
    base: &str,
) -> anyhow::Result<(String, String)> {
    let mut routes = HashMap::new();
    for candidate in 0..100 {
        let route = format!("route-{candidate}");
        let response = http
            .get(format!("{base}/events/_search_shards"))
            .query(&[("routing", route.as_str())])
            .send()
            .await?
            .error_for_status()?
            .json::<serde_json::Value>()
            .await?;
        let shard = response["shards"][0][0]["shard"]
            .as_u64()
            .ok_or_else(|| anyhow::anyhow!("OpenSearch search_shards omitted shard id"))?;
        routes.entry(shard).or_insert(route);
        if routes.len() == 2 {
            let mut routes = routes.into_values();
            return Ok((routes.next().unwrap(), routes.next().unwrap()));
        }
    }
    anyhow::bail!("could not find routing values on two distinct OpenSearch shards")
}
