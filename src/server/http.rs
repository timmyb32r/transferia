use std::convert::Infallible;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::header::{CACHE_CONTROL, CONTENT_SECURITY_POLICY, CONTENT_TYPE};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use super::assets::{APP_JS, INDEX_HTML, STYLE_CSS};
use super::model::{DeliveryRecord, RuntimeState, ValidationState};
use super::service::{ControlPlane, ServiceError};
use super::ui_catalog::UiCatalog;

const MAX_BODY_BYTES: usize = 2 * 1024 * 1024;

#[derive(Clone)]
struct AppState {
    control_plane: Arc<ControlPlane>,
    catalog: UiCatalog,
}

#[derive(Deserialize)]
struct ConfigRequest {
    config: Value,
}

#[derive(Deserialize)]
struct CreateDraftRequest {
    name: String,
    config: Value,
}

#[derive(Deserialize)]
struct UpdateDraftRequest {
    expected_revision: u64,
    name: String,
    config: Value,
}

#[derive(Deserialize)]
struct RevisionRequest {
    expected_revision: u64,
}

#[derive(Serialize)]
struct YamlResponse {
    yaml: String,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Clone, Debug, Serialize)]
struct DeliverySummary {
    id: String,
    name: String,
    revision: u64,
    validation: ValidationState,
    runtime: RuntimeState,
    updated_at_ms: u64,
}

impl From<DeliveryRecord> for DeliverySummary {
    fn from(record: DeliveryRecord) -> Self {
        Self {
            id: record.id,
            name: record.name,
            revision: record.revision,
            validation: record.validation,
            runtime: record.runtime,
            updated_at_ms: record.updated_at_ms,
        }
    }
}

#[derive(Serialize)]
struct ApiErrorBody {
    error: ApiErrorView,
}

#[derive(Serialize)]
struct ApiErrorView {
    code: &'static str,
    message: String,
}

struct ApiError(ServiceError);

impl From<ServiceError> for ApiError {
    fn from(error: ServiceError) -> Self {
        Self(error)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self.0 {
            ServiceError::InvalidInput(message) => {
                (StatusCode::BAD_REQUEST, "invalid_request", message)
            }
            ServiceError::NotFound(message) => (StatusCode::NOT_FOUND, "not_found", message),
            ServiceError::Conflict(message) => (StatusCode::CONFLICT, "conflict", message),
            ServiceError::Validation(message) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "validation_failed",
                message,
            ),
            ServiceError::Internal(error) => {
                tracing::error!(error = ?error, "control-plane request failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "the control plane could not complete the request".to_owned(),
                )
            }
        };
        (
            status,
            Json(ApiErrorBody {
                error: ApiErrorView { code, message },
            }),
        )
            .into_response()
    }
}

pub fn router(control_plane: Arc<ControlPlane>, ui_catalog: UiCatalog) -> Router {
    let state = AppState {
        control_plane,
        catalog: ui_catalog,
    };
    Router::new()
        .route("/", get(index))
        .route("/app.js", get(app_js))
        .route("/style.css", get(style_css))
        .route("/api/v1/health", get(health))
        .route("/api/v1/catalog", get(get_catalog))
        .route("/api/v1/config/yaml", post(render_yaml))
        .route("/api/v1/discover", post(discover))
        .route(
            "/api/v1/deliveries",
            get(list_deliveries).post(create_draft),
        )
        .route(
            "/api/v1/deliveries/{id}",
            get(get_delivery).put(update_draft),
        )
        .route("/api/v1/deliveries/{id}/validate", post(validate_saved))
        .route("/api/v1/deliveries/{id}/activate", post(activate))
        .route("/api/v1/deliveries/{id}/stop", post(stop))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(state)
}

pub async fn serve(
    listener: TcpListener,
    control_plane: Arc<ControlPlane>,
    catalog: UiCatalog,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    axum::serve(listener, router(control_plane, catalog))
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .await?;
    Ok(())
}

async fn index() -> Response {
    asset(INDEX_HTML, "text/html; charset=utf-8", true)
}

async fn app_js() -> Response {
    asset(APP_JS, "text/javascript; charset=utf-8", false)
}

async fn style_css() -> Response {
    asset(STYLE_CSS, "text/css; charset=utf-8", false)
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn get_catalog(State(state): State<AppState>) -> Json<UiCatalog> {
    Json(state.catalog)
}

async fn render_yaml(Json(request): Json<ConfigRequest>) -> Result<Json<YamlResponse>, ApiError> {
    Ok(Json(YamlResponse {
        yaml: ControlPlane::render_yaml(&request.config)?,
    }))
}

async fn discover(
    State(state): State<AppState>,
    Json(request): Json<ConfigRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let result = state
        .control_plane
        .validate_preview(&request.config, CancellationToken::new())
        .await?;
    Ok(Json(result))
}

async fn list_deliveries(
    State(state): State<AppState>,
) -> Result<Json<Vec<DeliverySummary>>, ApiError> {
    Ok(Json(
        state
            .control_plane
            .list()
            .await?
            .into_iter()
            .map(Into::into)
            .collect(),
    ))
}

async fn get_delivery(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let delivery = state.control_plane.get(&id).await?;
    Ok(([(CACHE_CONTROL, "no-store")], Json(delivery)))
}

async fn create_draft(
    State(state): State<AppState>,
    Json(request): Json<CreateDraftRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let delivery = state
        .control_plane
        .create_draft(request.name, request.config)
        .await?;
    Ok((StatusCode::CREATED, Json(delivery)))
}

async fn update_draft(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<UpdateDraftRequest>,
) -> Result<Json<DeliveryRecord>, ApiError> {
    Ok(Json(
        state
            .control_plane
            .update_draft(&id, request.expected_revision, request.name, request.config)
            .await?,
    ))
}

async fn validate_saved(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<RevisionRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let result = state
        .control_plane
        .validate_saved(&id, request.expected_revision, CancellationToken::new())
        .await?;
    Ok(Json(result))
}

async fn activate(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<RevisionRequest>,
) -> Result<Json<DeliveryRecord>, ApiError> {
    Ok(Json(
        state
            .control_plane
            .activate(&id, request.expected_revision)
            .await?,
    ))
}

async fn stop(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<RevisionRequest>,
) -> Result<Json<DeliveryRecord>, ApiError> {
    Ok(Json(
        state
            .control_plane
            .stop(&id, request.expected_revision)
            .await?,
    ))
}

fn asset(contents: &'static str, content_type: &'static str, html: bool) -> Response {
    let mut response = Response::new(Body::from(contents));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if html {
        response.headers_mut().insert(
            CONTENT_SECURITY_POLICY,
            HeaderValue::from_static(
                "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'",
            ),
        );
    }
    response
}

impl From<Infallible> for ApiError {
    fn from(value: Infallible) -> Self {
        match value {}
    }
}

#[cfg(test)]
#[path = "tests/http.rs"]
mod tests;
