use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow::datatypes::DataType;
use futures_util::future::BoxFuture;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Response};
use hyper_util::rt::TokioIo;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::delivery::{
    DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin, SourceTopology,
};
use transferia_registry::{SinkConnector as _, SinkSpeedtestIsolation, SpeedtestPhysicalTarget};

use super::*;
use crate::opensearch::sink::RoutedIdentity;
use crate::opensearch::{OpenSearchAuth, OpenSearchConnectionConfig};

struct FailingVerifier;

impl SpeedtestVerifier for FailingVerifier {
    fn verify(&self) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async { anyhow::bail!("foreign replacement") })
    }
}

struct CountingTransport(AtomicUsize);

impl BulkTransport for CountingTransport {
    fn send(
        &self,
        _payload: Vec<u8>,
    ) -> BoxFuture<'_, Result<Vec<u16>, crate::opensearch::sink::bulk::BulkFailure>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(vec![201]) })
    }
}

#[tokio::test]
async fn speedtest_replacement_is_detected_immediately_before_bulk_write() {
    let inner = Arc::new(CountingTransport(AtomicUsize::new(0)));
    let transport = VerifiedSpeedtestBulkTransport {
        verifier: Arc::new(FailingVerifier),
        inner: inner.clone(),
    };
    let error = transport.send(b"{}\n{}\n".to_vec()).await.unwrap_err();
    assert!(matches!(
        error,
        crate::opensearch::sink::bulk::BulkFailure::Fatal(_)
    ));
    assert_eq!(inner.0.load(Ordering::SeqCst), 0);
}

fn schema() -> DatasetSchema {
    DatasetSchema::new(vec![
        SchemaColumn::new("id".to_owned(), DataType::Int64, false)
            .with_constraints(true, false, None),
        SchemaColumn::new("payload".to_owned(), DataType::Utf8, false),
    ])
}

fn discovery(name: Arc<str>) -> DeliveryDiscovery {
    DeliveryDiscovery {
        source_name: Arc::from("test"),
        source_topology: SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets: vec![DiscoveredDataset {
            update_policy: transferia_core::delivery::UpdatePolicy::Strict,
            role: DatasetRole::Main,
            name,
            incoming_schema: schema(),
            stored_schema: schema(),
            system_columns: Vec::new(),
        }],
        performance_advice: Vec::new(),
    }
}

#[tokio::test]
async fn acknowledged_delete_that_leaves_index_present_retains_cleanup_obligation(
) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let scratch: Arc<str> = Arc::from("transferia-st-0123456789abcdef0123456789abcdef-0");
    let owner: Arc<str> = Arc::from("owner");
    let mapping = strict_mapping(&schema(), Some(&owner))?;
    let description = serde_json::json!({
        (scratch.as_ref()): {
            "settings": { "index": { "translog": { "durability": "request" } } },
            "mappings": mapping
        }
    });
    let requests = Arc::new(AtomicUsize::new(0));
    let server = {
        let requests = Arc::clone(&requests);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let requests = Arc::clone(&requests);
                let description = description.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |request| {
                        let requests = Arc::clone(&requests);
                        let description = description.clone();
                        async move {
                            requests.fetch_add(1, Ordering::SeqCst);
                            let body = if request.method() == Method::DELETE {
                                serde_json::json!({ "acknowledged": true })
                            } else {
                                description
                            };
                            Ok::<_, Infallible>(Response::new(Full::new(Bytes::from(
                                serde_json::to_vec(&body).unwrap(),
                            ))))
                        }
                    });
                    drop(
                        http1::Builder::new()
                            .serve_connection(TokioIo::new(stream), service)
                            .await,
                    );
                });
            }
        })
    };

    let config = Arc::new(OpenSearchSinkConfig {
        connection: OpenSearchConnectionConfig {
            hosts: vec!["127.0.0.1".to_owned()],
            port,
            trusted_plaintext: true,
            tls_ca_file: None,
            auth: OpenSearchAuth::Anonymous,
            request_timeout_ms: 1_000,
            max_response_bytes: 1024 * 1024,
        },
        create_indices: true,
        routed_identity: RoutedIdentity::Fail,
        bulk_target_rows: 100,
        bulk_target_bytes: 1024 * 1024,
        bulk_concurrency: 1,
        flush_interval_ms: 10,
        retry_initial_ms: 1,
        retry_max_ms: 1,
        retry_max_attempts: 1,
    });
    let production: Arc<str> = Arc::from("logs");
    let target = SpeedtestPhysicalTarget {
        production: Arc::from(physical_target(&config, &production)),
        scratch: Arc::from(physical_target(&config, &scratch)),
    };
    let scope = Arc::new(SpeedtestScope {
        owner,
        schemas: BTreeMap::from([(Arc::clone(&scratch), schema())]),
        physical_targets: BTreeSet::from([(
            Arc::clone(&target.production),
            Arc::clone(&target.scratch),
        )]),
        attempted: Mutex::new(BTreeSet::from([Arc::clone(&scratch)])),
        claimed: Mutex::new(BTreeSet::from([Arc::clone(&scratch)])),
    });
    let production_connector = Arc::new(OpenSearchSinkConnector {
        config: Arc::clone(&config),
        speedtest_scope: None,
    });
    let connector: Arc<dyn SinkConnector> = production_connector;
    let original = discovery(production);
    let isolated = discovery(Arc::clone(&scratch));
    let isolation = SinkSpeedtestIsolation::scratch(
        connector,
        &original,
        isolated,
        BTreeMap::from([(Arc::from("logs"), Arc::clone(&scratch))]),
        vec![target],
    )?;
    let owned = OpenSearchSinkConnector {
        config,
        speedtest_scope: Some(Arc::clone(&scope)),
    };
    let error = owned.cleanup_speedtest(&isolation).await.unwrap_err();
    assert!(error.to_string().contains("still exists"));
    assert!(scope.attempted()?.contains(&scratch));
    assert!(scope.claimed()?.contains(&scratch));
    assert!(requests.load(Ordering::SeqCst) >= 3);
    server.abort();
    Ok(())
}

#[test]
fn id_is_exclusive_metadata_not_a_mixed_user_primary_key() {
    let mixed = DatasetSchema::new(vec![
        SchemaColumn::new("_id".to_owned(), DataType::Utf8, false).with_constraints(
            true,
            false,
            Some(512),
        ),
        SchemaColumn::new("tenant".to_owned(), DataType::Utf8, false)
            .with_constraints(true, false, None),
    ]);
    assert!(validate_schema(&mixed).is_err());

    let non_key = DatasetSchema::new(vec![
        SchemaColumn::new("_id".to_owned(), DataType::Utf8, false),
        SchemaColumn::new("payload".to_owned(), DataType::Utf8, false),
    ]);
    assert!(validate_schema(&non_key).is_err());

    let oversized = DatasetSchema::new(vec![SchemaColumn::new(
        "_id".to_owned(),
        DataType::Utf8,
        false,
    )
    .with_constraints(true, false, Some(513))]);
    assert!(validate_schema(&oversized).is_err());
}
