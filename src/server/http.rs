use std::convert::Infallible;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::rejection::JsonRejection;
use axum::extract::rejection::QueryRejection;
use axum::extract::{DefaultBodyLimit, FromRequest, Path, Query, State};
use axum::http::header::{CACHE_CONTROL, CONTENT_SECURITY_POLICY, CONTENT_TYPE};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use transferia::extension::OptionsRequest;

use super::assets::{APP_JS, INDEX_HTML, STYLE_CSS};
use super::model::{DeliveryRecord, RunId, RuntimeState, ValidationState};
use super::service::{ControlPlane, ServiceError};
use super::ui_catalog::UiCatalog;

const MAX_BODY_BYTES: usize = 2 * 1024 * 1024;

#[derive(Clone)]
struct AppState {
    control_plane: Arc<ControlPlane>,
    catalog: UiCatalog,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigRequest {
    config: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct YamlRequest {
    yaml: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateDraftRequest {
    name: String,
    #[serde(default)]
    description: String,
    config: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateDraftRequest {
    expected_revision: u64,
    expected_record_version: u64,
    name: String,
    #[serde(default)]
    description: String,
    config: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RevisionRequest {
    expected_revision: u64,
    expected_record_version: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(
    clippy::struct_field_names,
    reason = "expected_* names are the optimistic-concurrency wire contract"
)]
struct StopRequest {
    expected_revision: u64,
    expected_record_version: u64,
    expected_run_id: String,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct OptionsQuery {
    q: Option<String>,

    #[serde(default)]
    refresh: bool,
}

#[derive(Serialize)]
struct YamlResponse {
    yaml: String,
}

#[derive(Serialize)]
struct ConfigResponse {
    config: Value,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Clone, Debug, Serialize)]
struct DeliverySummary {
    id: String,
    name: String,
    description: String,
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
            description: record.description,
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

struct ApiJson<T>(T);

struct ApiJsonRejection(JsonRejection);

impl<S, T> FromRequest<S> for ApiJson<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = ApiJsonRejection;

    async fn from_request(
        request: axum::extract::Request,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        Json::<T>::from_request(request, state)
            .await
            .map(|Json(value)| Self(value))
            .map_err(ApiJsonRejection)
    }
}

impl IntoResponse for ApiJsonRejection {
    fn into_response(self) -> Response {
        let (status, code) = if self.0.status() == StatusCode::PAYLOAD_TOO_LARGE {
            (StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large")
        } else {
            (StatusCode::BAD_REQUEST, "invalid_request")
        };
        error_response(status, code, self.0.body_text())
    }
}

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
        error_response(status, code, message)
    }
}

fn error_response(status: StatusCode, code: &'static str, message: String) -> Response {
    let mut response = (
        status,
        Json(ApiErrorBody {
            error: ApiErrorView { code, message },
        }),
    )
        .into_response();
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
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
        .route("/api/v1/options/{key}", get(dynamic_options))
        .route("/api/v1/config/yaml", post(render_yaml))
        .route("/api/v1/config/from-yaml", post(parse_yaml))
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
        .layer(axum::middleware::from_fn(no_store))
        .with_state(state)
}

async fn no_store(request: axum::extract::Request, next: axum::middleware::Next) -> Response {
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
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

async fn dynamic_options(
    State(state): State<AppState>,
    Path(key): Path<String>,
    query: Result<Query<OptionsQuery>, QueryRejection>,
) -> Result<impl IntoResponse, ApiError> {
    let Query(query) =
        query.map_err(|error| ApiError(ServiceError::InvalidInput(error.body_text())))?;
    let result = state
        .control_plane
        .dynamic_options(
            &key,
            OptionsRequest {
                query: query.q,
                refresh: query.refresh,
            },
        )
        .await?;
    Ok(([(CACHE_CONTROL, "no-store")], Json(result)))
}

async fn render_yaml(
    ApiJson(request): ApiJson<ConfigRequest>,
) -> Result<Json<YamlResponse>, ApiError> {
    Ok(Json(YamlResponse {
        yaml: ControlPlane::render_yaml(&request.config)?,
    }))
}

async fn parse_yaml(
    ApiJson(request): ApiJson<YamlRequest>,
) -> Result<Json<ConfigResponse>, ApiError> {
    Ok(Json(ConfigResponse {
        config: ControlPlane::parse_yaml(&request.yaml)?,
    }))
}

async fn discover(
    State(state): State<AppState>,
    ApiJson(request): ApiJson<ConfigRequest>,
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
    ApiJson(request): ApiJson<CreateDraftRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let delivery = state
        .control_plane
        .create_draft(request.name, request.description, request.config)
        .await?;
    Ok((StatusCode::CREATED, Json(delivery)))
}

async fn update_draft(
    State(state): State<AppState>,
    Path(id): Path<String>,
    ApiJson(request): ApiJson<UpdateDraftRequest>,
) -> Result<Json<DeliveryRecord>, ApiError> {
    Ok(Json(
        state
            .control_plane
            .update_draft(
                &id,
                request.expected_revision,
                request.expected_record_version,
                request.name,
                request.description,
                request.config,
            )
            .await?,
    ))
}

async fn validate_saved(
    State(state): State<AppState>,
    Path(id): Path<String>,
    ApiJson(request): ApiJson<RevisionRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let result = state
        .control_plane
        .validate_saved(
            &id,
            request.expected_revision,
            request.expected_record_version,
            CancellationToken::new(),
        )
        .await?;
    Ok(Json(result))
}

async fn activate(
    State(state): State<AppState>,
    Path(id): Path<String>,
    ApiJson(request): ApiJson<RevisionRequest>,
) -> Result<Json<DeliveryRecord>, ApiError> {
    Ok(Json(
        state
            .control_plane
            .activate(
                &id,
                request.expected_revision,
                request.expected_record_version,
            )
            .await?,
    ))
}

async fn stop(
    State(state): State<AppState>,
    Path(id): Path<String>,
    ApiJson(request): ApiJson<StopRequest>,
) -> Result<Json<DeliveryRecord>, ApiError> {
    Ok(Json(
        state
            .control_plane
            .stop(
                &id,
                request.expected_revision,
                request.expected_record_version,
                &RunId(request.expected_run_id),
            )
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
