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
    ) -> anyhow::Result<transferia::extension::DynamicOptions> {
        Ok(transferia::extension::DynamicOptions {
            options: vec![transferia::extension::DynamicOption {
                value: request.query.unwrap_or_default(),
                label: if request.refresh { "fresh" } else { "cached" }.to_owned(),
            }],
            warning: None,
        })
    }
}

struct TestExtension;

impl transferia::extension::TransferiaExtension for TestExtension {
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
async fn dynamic_options_forward_search_and_refresh() -> anyhow::Result<()> {
    let transferia = transferia::extension::TransferiaBuilder::new()
        .with_extension(Arc::new(TestExtension))
        .build()?;
    let (app, root) = test_router_with(transferia).await?;
    let response = app
        .oneshot(
            Request::get("/api/v1/options/test.options?q=cluster&refresh=true")
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4096).await?)?;
    assert_eq!(body["options"][0]["value"], "cluster");
    assert_eq!(body["options"][0]["label"], "fresh");
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
async fn assets_and_missing_routes_have_correct_http_contracts() -> anyhow::Result<()> {
    let (app, root) = test_router().await?;
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
        .oneshot(Request::get("/missing").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
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

    let oversized = app
        .oneshot(
            Request::post("/api/v1/deliveries")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(vec![b'x'; MAX_BODY_BYTES + 1]))?,
        )
        .await?;
    assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn yaml_can_round_trip_to_an_editable_json_config() -> anyhow::Result<()> {
    let (app, root) = test_router().await?;
    let request = serde_json::json!({
        "yaml": "delivery_type: stream\nsource:\n  ydb_topic: {}\nsink: {}\n"
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
    assert!(body["config"]["source"]["ydb_topic"].is_object());

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
