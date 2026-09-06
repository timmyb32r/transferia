use std::convert::Infallible;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, FromRequest, Path, Query, State};
use axum::http::header::{CACHE_CONTROL, CONTENT_SECURITY_POLICY, CONTENT_TYPE, HOST, ORIGIN};
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::de::DeserializeOwned;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use transferia_connectors::extension::OptionsRequest;

use super::api_contract::{
    ApiErrorBody, ApiErrorCode, ApiErrorView, ConfigRequest, ConfigResponse,
    ConnectionCheckRequest, CreateDraftRequest, DeliverySummary, HealthResponse,
    MessagePreviewRequest, RevisionRequest, SpeedtestEstimateRequest, SpeedtestTuneRequest,
    SqlPlaygroundRequest, StopRequest, UpdateDraftRequest, WorkerLogReadQuery, YamlRequest,
    YamlResponse,
};
use super::assets::{
    APP_JS, APP_JS_GZIP, APP_JS_VERSION, INDEX_HTML, STYLE_CSS, STYLE_CSS_GZIP, STYLE_CSS_VERSION,
};
use super::service::{ControlPlane, ServiceError};
use super::ui_catalog::UiCatalog;
use transferia_runtime::RunId;
use transferia_server_contracts::routes;
use transferia_server_contracts::DeliveryRecord;

const MAX_BODY_BYTES: usize = 2 * 1024 * 1024;

#[derive(Clone)]
struct AppState {
    control_plane: Arc<ControlPlane>,
    catalog: UiCatalog,
}

struct ApiError(ServiceError);

struct ApiJson<T>(T);

struct ApiJsonRejection(JsonRejection);

struct CancelOnDrop(CancellationToken);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

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
            (StatusCode::PAYLOAD_TOO_LARGE, ApiErrorCode::PayloadTooLarge)
        } else {
            (StatusCode::BAD_REQUEST, ApiErrorCode::InvalidRequest)
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
            ServiceError::InvalidInput(message) => (
                StatusCode::BAD_REQUEST,
                ApiErrorCode::InvalidRequest,
                message,
            ),
            ServiceError::NotFound(message) => {
                (StatusCode::NOT_FOUND, ApiErrorCode::NotFound, message)
            }
            ServiceError::Conflict(message) => {
                (StatusCode::CONFLICT, ApiErrorCode::Conflict, message)
            }
            ServiceError::Validation(message) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                ApiErrorCode::ValidationFailed,
                message,
            ),
            ServiceError::OperationFailed(message) => (
                StatusCode::BAD_GATEWAY,
                ApiErrorCode::OperationFailed,
                message,
            ),
            ServiceError::Internal(error) => {
                tracing::error!(error = ?error, "control-plane request failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    ApiErrorCode::InternalError,
                    "the control plane could not complete the request".to_owned(),
                )
            }
        };
        error_response(status, code, message)
    }
}

fn error_response(status: StatusCode, code: ApiErrorCode, message: String) -> Response {
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

macro_rules! declare_api_handlers {
    ($($route:ident => $handler:expr),+ $(,)?) => {
        #[cfg(test)]
        const MOUNTED_API_ROUTE_NAMES: &[&str] = &[$(routes::$route.name),+];

        fn mount_api_routes(router: Router<AppState>) -> Router<AppState> {
            router$(.route(routes::$route.path, $handler))+
        }
    };
}

declare_api_handlers! {
    HEALTH => get(health),
    CATALOG => get(get_catalog),
    OPTIONS => post(dynamic_options),
    CHECK_CONNECTION => post(check_connection),
    TABLE_SELECTION_PREVIEW => post(table_selection_preview),
    PREVIEW_MESSAGE => post(preview_message),
    SQL_PLAYGROUND => post(sql_playground),
    SPEEDTEST_ESTIMATE => post(speedtest_estimate),
    SPEEDTEST_TUNE => post(speedtest_tune),
    RENDER_YAML => post(render_yaml),
    PARSE_YAML => post(parse_yaml),
    DISCOVER => post(discover),
    LIST_DELIVERIES => get(list_deliveries),
    CREATE_DELIVERY => post(create_draft),
    GET_DELIVERY => get(get_delivery),
    UPDATE_DELIVERY => axum::routing::put(update_draft),
    DELETE_DELIVERY => axum::routing::delete(delete_delivery),
    VALIDATE => post(validate_saved),
    ACTIVATE => post(activate),
    STOP => post(stop),
    WORKER_LOGS => get(worker_logs),
    WORKER_LOG => get(worker_log),
}

pub fn router(control_plane: Arc<ControlPlane>, ui_catalog: UiCatalog) -> Router {
    let state = AppState {
        control_plane,
        catalog: ui_catalog,
    };
    mount_api_routes(
        Router::new()
            .route("/", get(index))
            .route("/app.js", get(app_js))
            .route("/style.css", get(style_css)),
    )
    .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
    .layer(axum::middleware::from_fn(no_store))
    .layer(axum::middleware::from_fn(enforce_loopback_origin))
    .with_state(state)
}

async fn sql_playground(
    State(state): State<AppState>,
    ApiJson(request): ApiJson<SqlPlaygroundRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let future = state
        .control_plane
        .sql_playground(request.sql, request.rows);
    let result = future.await?;
    Ok(([(CACHE_CONTROL, "no-store")], Json(result)))
}

async fn enforce_loopback_origin(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    for (header, kind) in [(HOST, "Host"), (ORIGIN, "Origin")] {
        let Some(value) = request.headers().get(header) else {
            continue;
        };
        let Ok(value) = value.to_str() else {
            return forbidden_boundary(kind);
        };
        if !is_loopback_authority(value, kind == "Origin") {
            return forbidden_boundary(kind);
        }
    }
    next.run(request).await
}

fn forbidden_boundary(kind: &str) -> Response {
    error_response(
        StatusCode::FORBIDDEN,
        ApiErrorCode::InvalidRequest,
        format!("{kind} must identify this loopback control plane"),
    )
}

fn is_loopback_authority(value: &str, is_origin: bool) -> bool {
    let authority = if is_origin {
        let Ok(uri) = value.parse::<Uri>() else {
            return false;
        };
        if uri.scheme_str() != Some("http") {
            return false;
        }
        let Some(authority) = uri.authority() else {
            return false;
        };
        authority.clone()
    } else {
        let Ok(authority) = value.parse() else {
            return false;
        };
        authority
    };
    let host = authority.host().trim_matches(['[', ']']);
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

async fn no_store(request: axum::extract::Request, next: axum::middleware::Next) -> Response {
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .entry(CACHE_CONTROL)
        .or_insert(HeaderValue::from_static("no-store"));
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

async fn app_js(uri: Uri, headers: HeaderMap) -> Response {
    versioned_asset(
        APP_JS,
        APP_JS_GZIP,
        APP_JS_VERSION,
        "text/javascript; charset=utf-8",
        &uri,
        &headers,
    )
}

async fn style_css(uri: Uri, headers: HeaderMap) -> Response {
    versioned_asset(
        STYLE_CSS,
        STYLE_CSS_GZIP,
        STYLE_CSS_VERSION,
        "text/css; charset=utf-8",
        &uri,
        &headers,
    )
}

fn versioned_asset(
    contents: &'static str,
    gzip: &'static [u8],
    version: &str,
    content_type: &'static str,
    uri: &Uri,
    headers: &HeaderMap,
) -> Response {
    let accepts_gzip = headers
        .get_all("accept-encoding")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|entry| {
            let mut parts = entry.split(';');
            parts
                .next()
                .is_some_and(|name| name.trim().eq_ignore_ascii_case("gzip"))
                && parts.all(|parameter| {
                    parameter
                        .trim()
                        .strip_prefix("q=")
                        .is_none_or(|q| q.parse::<f32>().is_ok_and(|q| q > 0.0 && q <= 1.0))
                })
        });
    let mut response = asset(contents, content_type, false);
    response
        .headers_mut()
        .insert("vary", HeaderValue::from_static("Accept-Encoding"));
    if accepts_gzip {
        *response.body_mut() = Body::from(gzip);
        response
            .headers_mut()
            .insert("content-encoding", HeaderValue::from_static("gzip"));
    }
    // Only the exact current content version is immutable. Never cache a new
    // bundle under a stale version URL after the server has been upgraded.
    if !version.is_empty() && uri.query() == Some(format!("v={version}").as_str()) {
        response.headers_mut().insert(
            CACHE_CONTROL,
            HeaderValue::from_static("public, max-age=31536000, immutable"),
        );
    }
    response
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
    ApiJson(request): ApiJson<OptionsRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let cancellation = state.control_plane.request_cancellation();
    let _cancel_on_drop = CancelOnDrop(cancellation.clone());
    let result = state
        .control_plane
        .dynamic_options(&key, request, cancellation)
        .await?;
    Ok(([(CACHE_CONTROL, "no-store")], Json(result)))
}

async fn check_connection(
    State(state): State<AppState>,
    ApiJson(request): ApiJson<ConnectionCheckRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let cancellation = state.control_plane.request_cancellation();
    let _cancel_on_drop = CancelOnDrop(cancellation.clone());
    let result = state
        .control_plane
        .check_connection(
            &request.connector,
            request.role,
            request.config,
            cancellation,
        )
        .await?;
    Ok(([(CACHE_CONTROL, "no-store")], Json(result)))
}

async fn table_selection_preview(
    ApiJson(request): ApiJson<transferia_server_contracts::api::TableSelectionPreviewRequest>,
) -> Result<impl IntoResponse, ApiError> {
    // This is only a preview over the last authenticated catalog. Startup
    // independently re-queries the source; this request cannot authorize tables.
    let result = request
        .selection
        .compile()
        .map_err(anyhow::Error::from)
        .and_then(|selection| selection.resolve(&request.catalog))
        .map_err(|error| ApiError(ServiceError::Validation(error.to_string())))?;
    Ok(([(CACHE_CONTROL, "no-store")], Json(result)))
}

async fn preview_message(
    State(state): State<AppState>,
    ApiJson(request): ApiJson<MessagePreviewRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let cancellation = state.control_plane.request_cancellation();
    let _cancel_on_drop = CancelOnDrop(cancellation.clone());
    let result = state
        .control_plane
        .preview_message(
            &request.connector,
            request.config,
            request.max_bytes,
            cancellation,
        )
        .await?;
    Ok(([(CACHE_CONTROL, "no-store")], Json(result)))
}

async fn speedtest_estimate(
    State(state): State<AppState>,
    ApiJson(request): ApiJson<SpeedtestEstimateRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let cancellation = state.control_plane.request_cancellation();
    let _cancel_on_drop = CancelOnDrop(cancellation.clone());
    let result = state
        .control_plane
        .spawn_speedtest_estimate(
            request.config,
            request.duration_seconds,
            request.cleanup_timeout_seconds,
            cancellation,
        )
        .await?;
    Ok(([(CACHE_CONTROL, "no-store")], Json(result)))
}

async fn speedtest_tune(
    State(state): State<AppState>,
    ApiJson(request): ApiJson<SpeedtestTuneRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let cancellation = state.control_plane.request_cancellation();
    let _cancel_on_drop = CancelOnDrop(cancellation.clone());
    let result = state
        .control_plane
        .spawn_speedtest_tune(
            request.config,
            request.budget,
            request.trial_duration_seconds,
            request.cleanup_timeout_seconds,
            cancellation,
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
        .source_schema_preview(&request.config, CancellationToken::new())
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

async fn delete_delivery(
    State(state): State<AppState>,
    Path(id): Path<String>,
    ApiJson(request): ApiJson<RevisionRequest>,
) -> Result<Json<DeliveryRecord>, ApiError> {
    Ok(Json(
        state
            .control_plane
            .delete(
                &id,
                request.expected_revision,
                request.expected_record_version,
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

async fn worker_logs(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(state.control_plane.worker_logs(&id).await?))
}

async fn worker_log(
    State(state): State<AppState>,
    Path((id, worker_id)): Path<(String, String)>,
    Query(query): Query<WorkerLogReadQuery>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(
        state
            .control_plane
            .worker_log(&id, &worker_id, query.cursor, query.limit_bytes)
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
                "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'",
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
