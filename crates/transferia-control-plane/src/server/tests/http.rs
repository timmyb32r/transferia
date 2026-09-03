use anyhow::Context as _;
use axum::body::to_bytes;
use axum::http::Request;
use tower::ServiceExt as _;

use super::*;
use crate::server::store::JsonDeliveryStore;
use crate::server::tests::TestSupervisor;
use crate::server::ui_catalog::build_ui_catalog;

static TEST_SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[test]
fn every_contract_route_has_exactly_one_registered_handler() {
    let mounted = MOUNTED_API_ROUTE_NAMES
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    let contracted = routes::API_ROUTES
        .iter()
        .map(|route| route.name)
        .collect::<std::collections::BTreeSet<_>>();

    assert_eq!(mounted.len(), MOUNTED_API_ROUTE_NAMES.len());
    assert_eq!(mounted, contracted);
}

#[tokio::test]
async fn operation_failure_preserves_a_safe_manual_recovery_message() -> anyhow::Result<()> {
    let response = ApiError(ServiceError::OperationFailed(
        "manual cleanup required for: db.__transferia_speedtest_123".to_owned(),
    ))
    .into_response();

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4096).await?)?;
    assert_eq!(body["error"]["code"], "operation_failed");
    assert_eq!(
        body["error"]["message"],
        "manual cleanup required for: db.__transferia_speedtest_123"
    );
    Ok(())
}

async fn test_router() -> anyhow::Result<(Router, std::path::PathBuf)> {
    test_router_with(transferia_connectors::extension::Transferia::public()?).await
}

async fn test_router_with(
    transferia: transferia_connectors::extension::Transferia,
) -> anyhow::Result<(Router, std::path::PathBuf)> {
    let root = std::env::temp_dir().join(format!(
        "transferia-http-test-{}-{}",
        std::process::id(),
        TEST_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let store = Arc::new(JsonDeliveryStore::open(root.clone()).await?);
    let control_plane = Arc::new(
        ControlPlane::new(store, Arc::new(TestSupervisor::new()), transferia)
            .with_worker_logs(crate::server::logs::WorkerLogReader::new(&root)),
    );
    Ok((router(control_plane, build_ui_catalog()?), root))
}

struct TestOptions;

#[async_trait::async_trait]
impl transferia_connectors::extension::DynamicOptionsConnector for TestOptions {
    async fn list(
        &self,
        request: transferia_connectors::extension::OptionsRequest,
        _context: transferia_connectors::extension::OptionsContext,
    ) -> anyhow::Result<transferia_connectors::extension::DynamicOptions> {
        Ok(transferia_connectors::extension::DynamicOptions {
            options: vec![transferia_connectors::extension::DynamicOption {
                value: request.query.unwrap_or_default(),
                label: format!(
                    "{}:{}",
                    if request.refresh { "fresh" } else { "cached" },
                    request
                        .dependencies
                        .get("cluster_id")
                        .map_or("", String::as_str)
                ),
            }],
            warning: None,
        })
    }
}

struct TestExtension;

impl transferia_connectors::extension::TransferiaExtension for TestExtension {
    fn identity(&self) -> transferia_connectors::extension::ExtensionIdentity {
        transferia_connectors::extension::ExtensionIdentity {
            package: "server-http-test",
            abi_version: 1,
        }
    }

    fn register(
        &self,
        registry: &mut transferia_connectors::extension::ExtensionRegistry,
    ) -> anyhow::Result<()> {
        registry.register_options("test.options", Arc::new(TestOptions))
    }
}

#[tokio::test]
async fn health_has_a_stable_json_contract() -> anyhow::Result<()> {
    let (app, root) = test_router().await?;
    let response = app
        .oneshot(Request::get("/api/v1/health").body(Body::empty())?)
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), 1024).await?,
        r#"{"status":"ok"}"#
    );
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn speedtest_estimate_rejects_zero_duration_before_touching_endpoints() -> anyhow::Result<()> {
    let (app, root) = test_router().await?;
    let response = app
        .oneshot(
            Request::post("/api/v1/speedtest/estimate")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"config":{},"duration_seconds":0,"cleanup_timeout_seconds":60}"#,
                ))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4096).await?)?;
    assert_eq!(body["error"]["code"], "invalid_request");
    assert!(body["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("duration_seconds")));
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn speedtest_estimate_rejects_zero_cleanup_timeout_before_endpoint_io(
) -> anyhow::Result<()> {
    let (app, root) = test_router().await?;
    let response = app
        .oneshot(
            Request::post("/api/v1/speedtest/estimate")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"config":{},"duration_seconds":1,"cleanup_timeout_seconds":0}"#,
                ))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4096).await?)?;
    assert_eq!(body["error"]["code"], "invalid_request");
    assert!(body["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("cleanup_timeout_seconds")));
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn speedtest_estimate_rejects_unrepresentable_duration_before_endpoint_io(
) -> anyhow::Result<()> {
    let (app, root) = test_router().await?;
    let response = app
        .oneshot(
            Request::post("/api/v1/speedtest/estimate")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(format!(
                    r#"{{"config":{{}},"duration_seconds":{},"cleanup_timeout_seconds":60}}"#,
                    u64::MAX
                )))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4096).await?)?;
    assert_eq!(body["error"]["code"], "invalid_request");
    assert!(body["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("too large")));
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn speedtest_tune_rejects_zero_trial_duration_before_endpoint_io() -> anyhow::Result<()> {
    let (app, root) = test_router().await?;
    let response = app
        .oneshot(
            Request::post("/api/v1/speedtest/tune")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"config":{"source":{"test":{}},"sink":{"test":{}}},"budget":{"type":"automatic","max_trials":1},"trial_duration_seconds":0,"cleanup_timeout_seconds":60}"#,
                ))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4096).await?)?;
    assert_eq!(body["error"]["code"], "invalid_request");
    assert!(body["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("trial_duration_seconds")));
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn speedtest_estimate_runs_actual_generator_through_discard() -> anyhow::Result<()> {
    let (app, root) = test_router().await?;
    let config = serde_json::json!({
        "delivery_type": null,
        "source": {
            "data_generator": {
                "table_name": "numbers",
                "preset": { "type": "numeric", "column_count": 2 },
                "amount": { "type": "rows", "row_count": 100_000 },
            },
        },
        "sink": { "discard": {} },
        "middlewares": [{ "incomplete_editor_middleware": {} }],
        "pipeline_memory_limit_bytes": 16 * 1024 * 1024,
        "metrics": null,
    });
    let request = serde_json::json!({
        "config": config,
        "duration_seconds": 1,
        "cleanup_timeout_seconds": 60,
    });
    let response = app
        .oneshot(
            Request::post("/api/v1/speedtest/estimate")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&request)?))?,
        )
        .await?;

    let status = response.status();
    assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 64 * 1024).await?)?;
    assert_eq!(status, StatusCode::OK, "unexpected response: {body}");
    assert_eq!(body["logical_streams"], 1);
    assert!(body["source"]["rows_per_second"].as_f64().is_some_and(|value| value > 0.0));
    assert!(body["destination"]["rows_per_second"]
        .as_f64()
        .is_some_and(|value| value > 0.0));
    assert_eq!(body["profile"]["datasets"][0]["dataset"], "numbers");
    assert_eq!(body["profile"]["datasets"][0]["columns"][0]["arrow_type"], "UInt64");
    assert!(
        !root.join("configured-state").exists(),
        "speedtest must not materialize production delivery state"
    );
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn speedtest_estimate_profiles_clickbench_generator_without_reader_failure(
) -> anyhow::Result<()> {
    let root = std::env::temp_dir().join(format!(
        "transferia-http-test-{}-{}",
        std::process::id(),
        TEST_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let store = Arc::new(JsonDeliveryStore::open(root.clone()).await?);
    let control_plane = Arc::new(ControlPlane::new(
        store,
        Arc::new(TestSupervisor::new()),
        transferia_connectors::extension::Transferia::public()?,
    ));
    let config = serde_json::json!({
        "delivery_type": null,
        "source": {
            "data_generator": {
                "table_name": "clickbench_hits",
                "preset": { "type": "clickbench" },
                "amount": { "type": "rows", "row_count": 100_000 },
            },
        },
        "sink": { "discard": {} },
        "middlewares": [],
        "pipeline_memory_limit_bytes": 512 * 1024 * 1024,
        "metrics": null,
    });
    let result = control_plane
        .speedtest_estimate(&config, 1, 60, tokio_util::sync::CancellationToken::new())
        .await;
    let result = result.map_err(|error| anyhow::anyhow!("clickbench speedtest failed: {error:?}"))?;

    assert_eq!(result.profile.datasets[0].dataset, "clickbench_hits");
    assert_eq!(result.profile.datasets[0].columns.len(), 105);
    assert!(result.source.rows_per_second > 0.0);
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn speedtest_estimate_rejects_a_sink_without_scratch_isolation() -> anyhow::Result<()> {
    let (app, root) = test_router().await?;
    let config = serde_json::json!({
        "delivery_id": "http-speedtest-unsupported-sink",
        "durable_storage": {
            "type": "local_file",
            "path": root.join("configured-state"),
        },
        "delivery_type": "batch",
        "source": {
            "data_generator": {
                "table_name": "numbers",
                "preset": { "type": "numeric", "column_count": 1 },
                "amount": { "type": "rows", "row_count": 1 },
            },
        },
        "sink": {
            "kafka": {
                "installation": {
                    "type": "on_premise",
                    "brokers": ["127.0.0.1:9092"],
                    "security": { "type": "plaintext" },
                },
                "topic": { "type": "topic", "topic": "production-topic" },
                "serializer": { "type": "json" },
                "partition": null,
                "request_timeout_ms": 30_000,
                "max_in_flight": 16,
            },
        },
        "middlewares": [],
        "pipeline_memory_limit_bytes": 16 * 1024 * 1024,
        "metrics": null,
    });
    let request = serde_json::json!({
        "config": config,
        "duration_seconds": 1,
        "cleanup_timeout_seconds": 60,
    });
    let response = app
        .oneshot(
            Request::post("/api/v1/speedtest/estimate")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&request)?))?,
        )
        .await?;

    let status = response.status();
    assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 16 * 1024).await?)?;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "unexpected response: {body}"
    );
    assert_eq!(body["error"]["code"], "validation_failed");
    assert!(body["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("isolated speedtest target")));
    assert!(!root.join("configured-state").exists());
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn speedtest_tune_never_echoes_full_endpoint_configuration() -> anyhow::Result<()> {
    let (app, root) = test_router().await?;
    let secret_marker = "must-never-appear-in-speedtest-response";
    let request = serde_json::json!({
        "config": {
            "delivery_type": null,
            "client_private_note": secret_marker,
            "source": {
                "data_generator": {
                    "table_name": "numbers",
                    "preset": { "type": "numeric", "column_count": 1 },
                    "amount": { "type": "rows", "row_count": 10_000 },
                },
            },
            "sink": { "discard": {} },
            "pipeline_memory_limit_bytes": 16 * 1024 * 1024,
        },
        "budget": { "type": "automatic", "max_trials": 1 },
        "trial_duration_seconds": 1,
        "cleanup_timeout_seconds": 60,
    });
    let response = app
        .oneshot(
            Request::post("/api/v1/speedtest/tune")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&request)?))?,
        )
        .await?;

    let status = response.status();
    assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
    let bytes = to_bytes(response.into_body(), 64 * 1024).await?;
    let body: serde_json::Value = serde_json::from_slice(&bytes)?;
    assert_eq!(status, StatusCode::OK, "unexpected response: {body}");
    assert!(!String::from_utf8(bytes.to_vec())?.contains(secret_marker));
    assert!(body["source"].get("configuration").is_none());
    assert!(body["destination"].get("configuration").is_none());
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn sql_playground_executes_the_runtime_datafusion_transform() -> anyhow::Result<()> {
    let (app, root) = test_router().await?;
    let response = app
        .oneshot(
            Request::post("/api/v1/playground/sql")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"sql":"SELECT id * 2 AS id FROM input WHERE id > 1","rows":[{"id":1},{"id":3}]}"#,
                ))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 16 * 1024).await?)?;
    assert_eq!(body["columns"][0]["name"], "id");
    assert_eq!(body["columns"][0]["arrow_type"], "Int64");
    assert_eq!(body["rows"], serde_json::json!([{ "id": 6 }]));
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn dynamic_options_forward_search_and_refresh() -> anyhow::Result<()> {
    let transferia = transferia_connectors::extension::TransferiaBuilder::new()
        .with_extension(Arc::new(TestExtension))
        .build()?;
    let (app, root) = test_router_with(transferia).await?;
    let response = app
        .oneshot(
            Request::post("/api/v1/options/test.options")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"query":"cluster","refresh":true,"dependencies":{"cluster_id":"mdb1"}}"#,
                ))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4096).await?)?;
    assert_eq!(body["options"][0]["value"], "cluster");
    assert_eq!(body["options"][0]["label"], "fresh:mdb1");
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn dynamic_options_reject_unknown_request_fields_as_json() -> anyhow::Result<()> {
    let transferia = transferia_connectors::extension::TransferiaBuilder::new()
        .with_extension(Arc::new(TestExtension))
        .build()?;
    let (app, root) = test_router_with(transferia).await?;
    let response = app
        .oneshot(
            Request::post("/api/v1/options/test.options")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"unexpected":true}"#))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response.headers()[CONTENT_TYPE], "application/json");
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4096).await?)?;
    assert_eq!(body["error"]["code"], "invalid_request");
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn connection_check_uses_the_typed_endpoint_capability() -> anyhow::Result<()> {
    let (app, root) = test_router().await?;
    let response = app
        .oneshot(
            Request::post("/api/v1/check-connection")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"connector":"discard","role":"sink","config":{}}"#,
                ))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4096).await?)?;
    assert_eq!(body["error"]["code"], "validation_failed");
    assert!(body["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("does not support connection checks")));
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn message_preview_rejects_sources_without_preview_capability() -> anyhow::Result<()> {
    let (app, root) = test_router().await?;
    let response = app
        .oneshot(
            Request::post("/api/v1/preview-message")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"connector":"postgres","config":{},"max_bytes":4194304}"#,
                ))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4096).await?)?;
    assert_eq!(body["error"]["code"], "validation_failed");
    assert!(body["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("does not support message preview")));
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn delivery_list_never_returns_config_or_secrets() -> anyhow::Result<()> {
    let (app, root) = test_router().await?;
    let request = serde_json::json!({
        "name": "secret draft",
        "config": { "password": "must-not-be-listed" }
    });
    let create = app
        .clone()
        .oneshot(
            Request::post("/api/v1/deliveries")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&request)?))?,
        )
        .await?;
    assert_eq!(create.status(), StatusCode::CREATED);

    let list = app
        .oneshot(Request::get("/api/v1/deliveries").body(Body::empty())?)
        .await?;
    let body = String::from_utf8(to_bytes(list.into_body(), 64 * 1024).await?.to_vec())?;
    assert!(!body.contains("config"));
    assert!(!body.contains("must-not-be-listed"));
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn create_rejects_surrounding_name_whitespace_without_persisting() -> anyhow::Result<()> {
    let (app, root) = test_router().await?;
    let response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/deliveries")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"name":" draft ","config":{}}"#))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4096).await?)?;
    assert!(body["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("leading or trailing whitespace")));

    let list = app
        .oneshot(Request::get("/api/v1/deliveries").body(Body::empty())?)
        .await?;
    let deliveries: serde_json::Value =
        serde_json::from_slice(&to_bytes(list.into_body(), 4096).await?)?;
    assert_eq!(deliveries, serde_json::json!([]));
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn delete_removes_only_the_expected_delivery_version() -> anyhow::Result<()> {
    let (app, root) = test_router().await?;
    let created = app
        .clone()
        .oneshot(
            Request::post("/api/v1/deliveries")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"name":"draft","config":{}}"#))?,
        )
        .await?;
    let created: serde_json::Value =
        serde_json::from_slice(&to_bytes(created.into_body(), 4096).await?)?;
    let id = created["id"].as_str().context("created id is a string")?;

    let stale = app
        .clone()
        .oneshot(
            Request::delete(format!("/api/v1/deliveries/{id}"))
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"expected_revision":1,"expected_record_version":"2"}"#,
                ))?,
        )
        .await?;
    assert_eq!(stale.status(), StatusCode::CONFLICT);

    let deleted = app
        .clone()
        .oneshot(
            Request::delete(format!("/api/v1/deliveries/{id}"))
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"expected_revision":1,"expected_record_version":"1"}"#,
                ))?,
        )
        .await?;
    assert_eq!(deleted.status(), StatusCode::OK);

    let missing = app
        .oneshot(Request::get(format!("/api/v1/deliveries/{id}")).body(Body::empty())?)
        .await?;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn validate_returns_the_authoritative_committed_record() -> anyhow::Result<()> {
    let (app, root) = test_router().await?;
    let created = app
        .clone()
        .oneshot(
            Request::post("/api/v1/deliveries")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"name":"draft","config":{}}"#))?,
        )
        .await?;
    let created: serde_json::Value =
        serde_json::from_slice(&to_bytes(created.into_body(), 4096).await?)?;
    let id = created["id"].as_str().context("created id is a string")?;
    let response = app
        .oneshot(
            Request::post(format!("/api/v1/deliveries/{id}/validate"))
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"expected_revision":1,"expected_record_version":"1"}"#,
                ))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 16 * 1024).await?)?;
    assert_eq!(body["delivery"]["record_version"], "2");
    assert_eq!(body["delivery"]["validation"]["state"], "invalid");
    assert!(body.get("discovery").is_none());
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn assets_and_missing_routes_have_correct_http_contracts() -> anyhow::Result<()> {
    let (app, root) = test_router().await?;
    let response = app
        .clone()
        .oneshot(Request::get("/").body(Body::empty())?)
        .await?;
    let index = String::from_utf8(to_bytes(response.into_body(), 64 * 1024).await?.to_vec())?;
    assert!(index.contains("/app.js?v="));
    assert!(index.contains("/style.css?v="));
    for (path, content_type) in [
        ("/", "text/html; charset=utf-8"),
        ("/app.js", "text/javascript; charset=utf-8"),
        ("/style.css", "text/css; charset=utf-8"),
    ] {
        let response = app
            .clone()
            .oneshot(Request::get(path).body(Body::empty())?)
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[CONTENT_TYPE], content_type);
    }
    let response = app
        .clone()
        .oneshot(Request::get("/app.js").body(Body::empty())?)
        .await?;
    let javascript = String::from_utf8(
        to_bytes(response.into_body(), 4 * 1024 * 1024)
            .await?
            .to_vec(),
    )?;
    assert!(
        javascript.contains("transferia-schema-dialect-path-options-v2"),
        "embedded UI bundle must declare support for dependency-aware catalog fields"
    );
    assert!(
        javascript.contains("transferia-options-post-v1"),
        "embedded UI bundle must declare the POST options transport"
    );
    let response = app
        .oneshot(Request::get("/missing").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn browser_boundary_rejects_hostile_host_and_origin() -> anyhow::Result<()> {
    let (app, root) = test_router().await?;
    for request in [
        Request::get("/api/v1/health")
            .header(HOST, "attacker.example")
            .body(Body::empty())?,
        Request::get("/api/v1/health")
            .header(HOST, "127.0.0.1:3000")
            .header(ORIGIN, "https://attacker.example")
            .body(Body::empty())?,
    ] {
        let response = app.clone().oneshot(request).await?;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
    }

    let response = app
        .clone()
        .oneshot(
            Request::get("/api/v1/health")
                .header(HOST, "[::1]:3000")
                .header(ORIGIN, "http://localhost:3000")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);

    let html = app
        .oneshot(
            Request::get("/")
                .header(HOST, "localhost:3000")
                .body(Body::empty())?,
        )
        .await?;
    let csp = html.headers()[CONTENT_SECURITY_POLICY]
        .to_str()
        .expect("CSP is ASCII");
    assert!(csp.contains("frame-ancestors 'none'"));
    assert!(csp.contains("base-uri 'none'"));
    assert!(csp.contains("form-action 'none'"));

    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn invalid_and_oversized_json_are_rejected_before_the_service() -> anyhow::Result<()> {
    let (app, root) = test_router().await?;
    let invalid = app
        .clone()
        .oneshot(
            Request::post("/api/v1/deliveries")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from("{"))?,
        )
        .await?;
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    assert_eq!(invalid.headers()[CONTENT_TYPE], "application/json");
    assert_eq!(invalid.headers()[CACHE_CONTROL], "no-store");
    let invalid_body: serde_json::Value =
        serde_json::from_slice(&to_bytes(invalid.into_body(), 4096).await?)?;
    assert_eq!(invalid_body["error"]["code"], "invalid_request");
    assert!(invalid_body["error"]["message"].is_string());

    let unknown_field = app
        .clone()
        .oneshot(
            Request::post("/api/v1/config/yaml")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"config":{},"unexpected":true}"#))?,
        )
        .await?;
    assert_eq!(unknown_field.status(), StatusCode::BAD_REQUEST);
    let unknown_body: serde_json::Value =
        serde_json::from_slice(&to_bytes(unknown_field.into_body(), 4096).await?)?;
    assert_eq!(unknown_body["error"]["code"], "invalid_request");

    let oversized = app
        .oneshot(
            Request::post("/api/v1/deliveries")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(vec![b'x'; MAX_BODY_BYTES + 1]))?,
        )
        .await?;
    assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let oversized_body: serde_json::Value =
        serde_json::from_slice(&to_bytes(oversized.into_body(), 4096).await?)?;
    assert_eq!(oversized_body["error"]["code"], "payload_too_large");
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn api_errors_use_the_shared_json_envelope_and_are_never_cached() -> anyhow::Result<()> {
    let fixture = crate::server::api_contract::fixture()?;
    let (app, root) = test_router().await?;
    let response = app
        .oneshot(Request::get("/api/v1/deliveries/missing").body(Body::empty())?)
        .await?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(response.headers()[CONTENT_TYPE], "application/json");
    assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4096).await?)?;
    assert_eq!(body, fixture["error_envelope"]);
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn yaml_can_round_trip_to_an_editable_json_config() -> anyhow::Result<()> {
    let (app, root) = test_router().await?;
    let request = serde_json::json!({
        "yaml": "delivery_type: stream\nsource:\n  logbroker: {}\nsink: {}\n"
    });
    let parsed = app
        .clone()
        .oneshot(
            Request::post("/api/v1/config/from-yaml")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&request)?))?,
        )
        .await?;
    assert_eq!(parsed.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(parsed.into_body(), 64 * 1024).await?)?;
    assert_eq!(body["config"]["delivery_type"], "stream");
    assert!(body["config"]["source"]["logbroker"].is_object());

    let invalid = app
        .oneshot(
            Request::post("/api/v1/config/from-yaml")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"yaml":"- not\\n- a mapping\\n"}"#))?,
        )
        .await?;
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);

    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn delivery_logs_are_bounded_scoped_and_never_cached() -> anyhow::Result<()> {
    let (app, root) = test_router().await?;
    let created = app
        .clone()
        .oneshot(
            Request::post("/api/v1/deliveries")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"name":"logs","description":"","config":{}}"#,
                ))?,
        )
        .await?;
    let created: serde_json::Value =
        serde_json::from_slice(&to_bytes(created.into_body(), 16 * 1024).await?)?;
    let delivery_id = created["id"].as_str().context("delivery id")?;
    let runs = root.join("runs");
    tokio::fs::create_dir_all(runs.join(delivery_id)).await?;
    tokio::fs::write(
        runs.join(delivery_id).join("worker-1.log"),
        "first\npassword=do-not-return\nlast\n",
    )
    .await?;

    let list = app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/deliveries/{delivery_id}/logs")).body(Body::empty())?,
        )
        .await?;
    assert_eq!(list.status(), StatusCode::OK);
    assert_eq!(list.headers()[CACHE_CONTROL], "no-store");
    let list: serde_json::Value =
        serde_json::from_slice(&to_bytes(list.into_body(), 16 * 1024).await?)?;
    assert_eq!(list["workers"][0]["worker_id"], "worker-1");

    let log = app
        .clone()
        .oneshot(
            Request::get(format!(
                "/api/v1/deliveries/{delivery_id}/logs/worker-1?cursor=0&limit_bytes=12"
            ))
            .body(Body::empty())?,
        )
        .await?;
    assert_eq!(log.status(), StatusCode::OK);
    assert_eq!(log.headers()[CACHE_CONTROL], "no-store");
    let log: serde_json::Value =
        serde_json::from_slice(&to_bytes(log.into_body(), 16 * 1024).await?)?;
    assert_eq!(log["start_offset"], 0);
    assert!(log["text"].as_str().is_some_and(|text| text.len() <= 12));

    let traversal = app
        .oneshot(
            Request::get(format!(
                "/api/v1/deliveries/{delivery_id}/logs/%2E%2E%2Fstate"
            ))
            .body(Body::empty())?,
        )
        .await?;
    assert!(matches!(
        traversal.status(),
        StatusCode::BAD_REQUEST | StatusCode::NOT_FOUND
    ));
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}
