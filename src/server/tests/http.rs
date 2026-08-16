use axum::body::to_bytes;
use axum::http::Request;
use tower::ServiceExt as _;

use super::*;
use crate::server::service::{ColumnView, DatasetRoleView, DatasetView, DiscoveryResult};
use crate::server::store::JsonDeliveryStore;
use crate::server::tests::TestSupervisor;
use crate::server::ui_catalog::build_ui_catalog;
use transferia::delivery::{ArrowTypeFamily, NameSyntax, SinkLimitsDescription, TextLimit};

const API_CONTRACT: &str = include_str!("../../../contracts/server-api.json");

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
async fn dynamic_options_reject_unknown_query_fields_as_json() -> anyhow::Result<()> {
    let transferia = transferia::extension::TransferiaBuilder::new()
        .with_extension(Arc::new(TestExtension))
        .build()?;
    let (app, root) = test_router_with(transferia).await?;
    let response = app
        .oneshot(Request::get("/api/v1/options/test.options?unexpected=true").body(Body::empty())?)
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

#[test]
fn rust_dtos_serialize_exactly_as_the_shared_api_contract() -> anyhow::Result<()> {
    let fixture: serde_json::Value = serde_json::from_str(API_CONTRACT)?;
    let delivery = DeliveryRecord {
        id: "delivery-1".to_owned(),
        name: "Example".to_owned(),
        description: "Contract fixture".to_owned(),
        config: serde_json::json!({ "delivery_type": "stream" }),
        revision: 7,
        record_version: 11,
        validation: ValidationState::Invalid {
            revision: 7,
            message: "invalid fixture".to_owned(),
        },
        runtime: RuntimeState::Running {
            run_id: crate::server::model::RunId("run-7".to_owned()),
            pid: 42,
        },
        created_at_ms: 1000,
        updated_at_ms: 2000,
    };
    assert_eq!(serde_json::to_value(delivery)?, fixture["delivery_record"]);

    let runtime_states = [
        RuntimeState::Stopped,
        RuntimeState::Starting {
            run_id: crate::server::model::RunId("run-1".to_owned()),
        },
        RuntimeState::Running {
            run_id: crate::server::model::RunId("run-2".to_owned()),
            pid: 42,
        },
        RuntimeState::Stopping {
            run_id: crate::server::model::RunId("run-3".to_owned()),
        },
        RuntimeState::Failed {
            run_id: crate::server::model::RunId("run-4".to_owned()),
            message: "worker failed".to_owned(),
        },
    ];
    assert_eq!(
        serde_json::to_value(runtime_states)?,
        fixture["runtime_states"]
    );

    let discovery = DiscoveryResult {
        source: "logbroker".to_owned(),
        sink: "clickhouse".to_owned(),
        datasets: vec![DatasetView {
            role: DatasetRoleView::Main,
            name: "events".to_owned(),
            columns: vec![
                ColumnView {
                    name: "id".to_owned(),
                    arrow_type: "Utf8".to_owned(),
                    nullable: false,
                    primary_key: true,
                    low_cardinality: true,
                    max_length: Some(64),
                },
                ColumnView {
                    name: "created_at".to_owned(),
                    arrow_type: "Timestamp(Millisecond, None)".to_owned(),
                    nullable: true,
                    primary_key: false,
                    low_cardinality: false,
                    max_length: None,
                },
            ],
        }],
        sink_limits: SinkLimitsDescription {
            sink: "clickhouse",
            dataset_name: Some(TextLimit {
                syntax: NameSyntax::AsciiIdentifier,
                max_utf8_bytes: Some(255),
            }),
            column_name: None,
            supported_arrow_types: vec![
                ArrowTypeFamily::Utf8,
                ArrowTypeFamily::SignedInteger,
                ArrowTypeFamily::Timestamp,
            ],
            object_key: None,
        },
    };
    assert_eq!(
        serde_json::to_value(discovery)?,
        fixture["discovery_result"]
    );
    Ok(())
}

#[tokio::test]
async fn api_errors_use_the_shared_json_envelope_and_are_never_cached() -> anyhow::Result<()> {
    let fixture: serde_json::Value = serde_json::from_str(API_CONTRACT)?;
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
