use anyhow::Context as _;
use axum::body::to_bytes;
use axum::http::Request;
use tower::ServiceExt as _;

use super::*;
use crate::server::store::JsonDeliveryStore;
use crate::server::tests::TestSupervisor;
use crate::server::ui_catalog::build_ui_catalog;

static TEST_SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

async fn test_router() -> anyhow::Result<(Router, std::path::PathBuf)> {
    test_router_with(transferia::extension::Transferia::public()?).await
}

async fn test_router_with(
    transferia: transferia::extension::Transferia,
) -> anyhow::Result<(Router, std::path::PathBuf)> {
    let root = std::env::temp_dir().join(format!(
        "transferia-http-test-{}-{}",
        std::process::id(),
        TEST_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let store = Arc::new(JsonDeliveryStore::open(root.clone()).await?);
    let control_plane = Arc::new(ControlPlane::new(
        store,
        Arc::new(TestSupervisor::new()),
        transferia,
    ));
    Ok((router(control_plane, build_ui_catalog()?), root))
}

struct TestOptions;

#[async_trait::async_trait]
impl transferia::extension::DynamicOptionsProvider for TestOptions {
    async fn list(
        &self,
        request: transferia::extension::OptionsRequest,
        _context: transferia::extension::OptionsContext,
    ) -> anyhow::Result<transferia::extension::DynamicOptions> {
        Ok(transferia::extension::DynamicOptions {
            options: vec![transferia::extension::DynamicOption {
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

impl transferia::extension::TransferiaExtension for TestExtension {
    fn identity(&self) -> transferia::extension::ExtensionIdentity {
        transferia::extension::ExtensionIdentity {
            package: "server-http-test",
            abi_version: 1,
        }
    }

    fn register(
        &self,
        registry: &mut transferia::extension::ExtensionRegistry,
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
    let transferia = transferia::extension::TransferiaBuilder::new()
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
    let transferia = transferia::extension::TransferiaBuilder::new()
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
                    r#"{"provider":"discard","role":"sink","config":{}}"#,
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
                    r#"{"provider":"postgres","config":{},"max_bytes":4194304}"#,
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
        javascript.contains("transferia-schema-dialect-dynamic-options-dependencies-v1"),
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
