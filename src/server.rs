use std::collections::BTreeMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Context as _;
use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use transferia::config::yaml::Config;
use transferia::delivery::{DeliveryDiscovery, DeliveryDiscoveryRequest};
use transferia::metrics::MetricsRegistry;
use transferia::providers::traits::{SinkProvider, SourceProvider};

type HttpBody = Full<Bytes>;

const INDEX_HTML: &str = include_str!("server/index.html");
const APP_JS: &str = include_str!("server/app.js");
const STYLE_CSS: &str = include_str!("server/style.css");

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum DeliveryStatus {
    Created,
    Active { pid: u32 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredDelivery {
    id: String,
    name: String,
    config_yaml: String,
    status: DeliveryStatus,
    config_path: Option<PathBuf>,
    log_path: Option<PathBuf>,
}

#[derive(Default, Serialize, Deserialize)]
struct StoredState {
    deliveries: BTreeMap<String, StoredDelivery>,
}

struct ServerState {
    state_dir: PathBuf,
    stored: Mutex<StoredState>,
    activation: Mutex<()>,
    next_id: AtomicU64,
}

#[derive(Deserialize)]
struct DiscoveryRequest {
    config_yaml: String,
}

#[derive(Deserialize)]
struct CreateRequest {
    name: String,
    config_yaml: String,
}

#[derive(Debug, Serialize)]
struct DiscoveryResponse {
    source: String,
    sink: String,
    datasets: Vec<DatasetView>,
    sink_limits: transferia::delivery::SinkLimitsDescription,
}

#[derive(Debug, Serialize)]
struct DatasetView {
    role: String,
    name: String,
    columns: Vec<ColumnView>,
}

#[derive(Debug, Serialize)]
struct ColumnView {
    name: String,
    arrow_type: String,
    nullable: bool,
    primary_key: bool,
    low_cardinality: bool,
    max_length: Option<usize>,
}

pub async fn run(bind: SocketAddr, state_dir: PathBuf) -> anyhow::Result<()> {
    tokio::fs::create_dir_all(&state_dir).await?;
    let stored = load_state(&state_dir).await?;
    let state = Arc::new(ServerState {
        state_dir,
        stored: Mutex::new(stored),
        activation: Mutex::new(()),
        next_id: AtomicU64::new(1),
    });
    let listener = TcpListener::bind(bind).await?;
    let address = listener.local_addr()?;
    tracing::info!(%address, "demo control plane is ready");
    println!("Transferia demo UI: http://{address}");
    loop {
        let (stream, _) = listener.accept().await?;
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let service = service_fn(move |request| route(request, Arc::clone(&state)));
            if let Err(error) = http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await
            {
                tracing::warn!(%error, "demo UI connection failed");
            }
        });
    }
}

async fn route(
    request: Request<Incoming>,
    state: Arc<ServerState>,
) -> Result<Response<HttpBody>, Infallible> {
    let response = route_fallible(request, state)
        .await
        .unwrap_or_else(|error| error_response(&error));
    Ok(response)
}

async fn route_fallible(
    request: Request<Incoming>,
    state: Arc<ServerState>,
) -> anyhow::Result<Response<HttpBody>> {
    let path = request.uri().path().to_owned();
    match (request.method(), path.as_str()) {
        (&Method::GET, "/") => Ok(asset(INDEX_HTML, "text/html; charset=utf-8")),
        (&Method::GET, "/app.js") => Ok(asset(APP_JS, "text/javascript; charset=utf-8")),
        (&Method::GET, "/style.css") => Ok(asset(STYLE_CSS, "text/css; charset=utf-8")),
        (&Method::GET, "/api/providers") => json_response(&serde_json::json!({
            "sources": ["pqv1", "postgres", "clickhouse", "s3", "ytsaurus"],
            "sinks": ["clickhouse", "postgres", "pqv1", "s3", "ytsaurus", "discard"]
        })),
        (&Method::GET, "/api/deliveries") => {
            let stored = state.stored.lock().await;
            json_response(&stored.deliveries.values().collect::<Vec<_>>())
        }
        (&Method::POST, "/api/discover") => {
            let body: DiscoveryRequest = read_json(request).await?;
            json_response(&discover(&body.config_yaml).await?)
        }
        (&Method::POST, "/api/deliveries") => {
            let body: CreateRequest = read_json(request).await?;
            anyhow::ensure!(
                !body.name.trim().is_empty(),
                "delivery name must not be empty"
            );
            let configured_delivery_id = Config::from_yaml(&body.config_yaml)?.delivery_id;
            discover(&body.config_yaml).await?;
            let id = new_id(&state);
            let delivery = StoredDelivery {
                id: id.clone(),
                name: body.name,
                config_yaml: body.config_yaml,
                status: DeliveryStatus::Created,
                config_path: None,
                log_path: None,
            };
            {
                let mut stored = state.stored.lock().await;
                for existing in stored.deliveries.values() {
                    let existing_delivery_id = Config::from_yaml(&existing.config_yaml)
                        .context("saved delivery contains an invalid configuration")?
                        .delivery_id;
                    anyhow::ensure!(
                        existing_delivery_id != configured_delivery_id,
                        "delivery_id '{configured_delivery_id}' is already used by saved delivery '{}'",
                        existing.name
                    );
                }
                stored.deliveries.insert(id, delivery.clone());
                save_state(&state.state_dir, &stored).await?;
                drop(stored);
            }
            json_response(&delivery)
        }
        (&Method::POST, _) if path.ends_with("/activate") => {
            let id = path
                .strip_prefix("/api/deliveries/")
                .and_then(|path| path.strip_suffix("/activate"))
                .context("unknown endpoint")?;
            let delivery = activate(&state, id).await?;
            json_response(&delivery)
        }
        _ => Ok(text_response(StatusCode::NOT_FOUND, "not found")),
    }
}

async fn discover(config_yaml: &str) -> anyhow::Result<DiscoveryResponse> {
    let config = Config::from_yaml(config_yaml)?;
    let _durable = config.durable_storage.build(&config.delivery_id)?;
    let metrics = Arc::new(MetricsRegistry::new());
    let registry = super::build_provider_registry(&metrics);
    let source_kind = config.source.kind()?.to_owned();
    let sink_kind = config.sink.kind()?.to_owned();
    let source: Arc<dyn SourceProvider> =
        Arc::from(registry.build_source(&source_kind, config.source.raw()?.clone())?);
    let sink: Arc<dyn SinkProvider> =
        Arc::from(registry.build_sink(&sink_kind, config.sink.raw()?.clone())?);
    let discovery = source
        .delivery_discovery(
            DeliveryDiscoveryRequest {
                keep_system_columns: config.keep_system_columns_in_sink,
            },
            CancellationToken::new(),
        )
        .await?;
    super::validate_discovered_pipeline(
        &source.compatibility(),
        &sink.compatibility(),
        sink.limits(),
        &discovery,
        config.keep_system_columns_in_sink,
    )?;
    Ok(discovery_response(
        source_kind,
        sink_kind,
        &discovery,
        sink.as_ref(),
    ))
}

fn discovery_response(
    source: String,
    sink: String,
    discovery: &DeliveryDiscovery,
    sink_provider: &dyn SinkProvider,
) -> DiscoveryResponse {
    DiscoveryResponse {
        source,
        sink,
        datasets: discovery
            .datasets
            .iter()
            .map(|dataset| DatasetView {
                role: format!("{:?}", dataset.role),
                name: dataset.name.to_string(),
                columns: dataset
                    .stored_schema
                    .columns
                    .iter()
                    .map(|column| ColumnView {
                        name: column.name.clone(),
                        arrow_type: format!("{:?}", column.data_type),
                        nullable: column.nullable,
                        primary_key: column.primary_key,
                        low_cardinality: column.low_cardinality,
                        max_length: column.max_length,
                    })
                    .collect(),
            })
            .collect(),
        sink_limits: sink_provider.limits().description(),
    }
}

async fn activate(state: &ServerState, id: &str) -> anyhow::Result<StoredDelivery> {
    // Activation is rare and a demo-only operation. Serialize it separately so source discovery
    // and process creation never hold the state mutex used by the delivery-list endpoint.
    let _activation = state.activation.lock().await;
    let delivery = {
        let stored = state.stored.lock().await;
        let delivery = stored
            .deliveries
            .get(id)
            .with_context(|| format!("delivery '{id}' does not exist"))?
            .clone();
        drop(stored);
        anyhow::ensure!(
            delivery.status == DeliveryStatus::Created,
            "delivery '{id}' is already active"
        );
        delivery
    };
    discover(&delivery.config_yaml).await?;
    let config_path = state.state_dir.join(format!("delivery-{id}.yaml"));
    let log_path = state.state_dir.join(format!("delivery-{id}.log"));
    tokio::fs::write(&config_path, &delivery.config_yaml).await?;
    let log = std::fs::File::create(&log_path)?;
    let error_log = log.try_clone()?;
    let child = std::process::Command::new(std::env::current_exe()?)
        .arg("--config")
        .arg(&config_path)
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(error_log))
        .spawn()?;
    let mut stored = state.stored.lock().await;
    let stored_delivery = stored
        .deliveries
        .get_mut(id)
        .with_context(|| format!("delivery '{id}' disappeared during activation"))?;
    stored_delivery.status = DeliveryStatus::Active { pid: child.id() };
    stored_delivery.config_path = Some(config_path);
    stored_delivery.log_path = Some(log_path);
    let result = stored_delivery.clone();
    save_state(&state.state_dir, &stored).await?;
    drop(stored);
    Ok(result)
}

fn new_id(state: &ServerState) -> String {
    let sequence = state.next_id.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!("{nanos:x}-{sequence:x}")
}

async fn load_state(state_dir: &Path) -> anyhow::Result<StoredState> {
    let path = state_dir.join("deliveries.json");
    match tokio::fs::read(&path).await {
        Ok(bytes) => serde_json::from_slice(&bytes).context("invalid demo server state"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(StoredState::default()),
        Err(error) => Err(error.into()),
    }
}

async fn save_state(state_dir: &Path, state: &StoredState) -> anyhow::Result<()> {
    let path = state_dir.join("deliveries.json");
    let temporary = state_dir.join("deliveries.json.tmp");
    tokio::fs::write(&temporary, serde_json::to_vec_pretty(state)?).await?;
    tokio::fs::rename(temporary, path).await?;
    Ok(())
}

async fn read_json<T: for<'de> Deserialize<'de>>(request: Request<Incoming>) -> anyhow::Result<T> {
    const MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;
    let body = http_body_util::Limited::new(request.into_body(), MAX_REQUEST_BYTES);
    let bytes = body
        .collect()
        .await
        .map_err(|error| anyhow::anyhow!("failed to read request body: {error}"))?
        .to_bytes();
    serde_json::from_slice(&bytes).context("invalid JSON request")
}

fn json_response(value: &impl Serialize) -> anyhow::Result<Response<HttpBody>> {
    let bytes = serde_json::to_vec(value)?;
    Ok(Response::builder()
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(bytes)))?)
}

fn asset(value: &'static str, content_type: &'static str) -> Response<HttpBody> {
    Response::builder()
        .header(hyper::header::CONTENT_TYPE, content_type)
        .body(Full::new(Bytes::from_static(value.as_bytes())))
        .expect("static asset response is valid")
}

fn text_response(status: StatusCode, value: impl Into<Bytes>) -> Response<HttpBody> {
    Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Full::new(value.into()))
        .expect("text response is valid")
}

fn error_response(error: &anyhow::Error) -> Response<HttpBody> {
    text_response(StatusCode::BAD_REQUEST, format!("{error:#}"))
}

#[cfg(test)]
mod tests;
