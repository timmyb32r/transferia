use axum::body::to_bytes;
use axum::http::Request;
use tower::ServiceExt as _;

use super::*;
use crate::server::store::JsonDeliveryStore;
use crate::server::tests::TestSupervisor;
use crate::server::ui_catalog::build_ui_catalog;

static TEST_SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

async fn test_router() -> anyhow::Result<(Router, std::path::PathBuf)> {
    let root = std::env::temp_dir().join(format!(
        "transferia-http-test-{}-{}",
        std::process::id(),
        TEST_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let store = Arc::new(JsonDeliveryStore::open(root.clone()).await?);
    let control_plane = Arc::new(ControlPlane::new(store, Arc::new(TestSupervisor::new())));
    Ok((router(control_plane, build_ui_catalog()?), root))
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
