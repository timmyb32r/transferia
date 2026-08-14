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
use schemars::{schema_for, JsonSchema};
use serde::{Deserialize, Serialize};
use serde_json::Value;
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
    config: Value,
}

#[derive(Deserialize)]
struct CreateRequest {
    name: String,
    config: Value,
}

#[derive(JsonSchema)]
#[expect(dead_code, reason = "fields are consumed by the JsonSchema derive")]
struct ConfigFormSchema {
    #[schemars(
        title = "Delivery ID",
        description = "Stable identifier used for durable progress"
    )]
    delivery_id: String,
    durable_storage: transferia::durable::DurableStorageConfig,
    source: SourceFormSchema,
    sink: SinkFormSchema,
    middlewares: Vec<MiddlewareFormSchema>,
    #[schemars(title = "Pipeline memory limit", extend("x-ui" = { "widget": "byte_size" }))]
    pipeline_memory_limit_bytes: usize,
    #[schemars(title = "Keep system columns in sink")]
    keep_system_columns_in_sink: bool,
    metrics: Option<transferia::metrics::MetricsConfig>,
}

#[derive(JsonSchema)]
#[serde(rename_all = "lowercase")]
#[expect(dead_code, reason = "variants are consumed by the JsonSchema derive")]
enum SourceFormSchema {
    #[schemars(title = "PQv1 stream")]
    Pqv1(transferia::providers::pqv1::src_stream::PqV1SourceConfig),
    #[schemars(title = "PostgreSQL batch")]
    Postgres(transferia::providers::postgres::src_batch::PostgresSourceConfig),
    #[schemars(title = "ClickHouse batch")]
    Clickhouse(transferia::providers::clickhouse::src_batch::ClickHouseSourceConfig),
    #[schemars(title = "S3 batch")]
    S3(transferia::providers::s3::src_batch::S3SourceConfig),
    #[schemars(title = "YTsaurus batch")]
    Ytsaurus(transferia::providers::ytsaurus::YTsaurusSourceConfig),
}

#[derive(JsonSchema)]
#[serde(rename_all = "lowercase")]
#[expect(dead_code, reason = "variants are consumed by the JsonSchema derive")]
enum SinkFormSchema {
    #[schemars(title = "ClickHouse")]
    Clickhouse(transferia::providers::clickhouse::ClickHouseSinkConfig),
    #[schemars(title = "PostgreSQL")]
    Postgres(transferia::providers::postgres::sink::PostgresSinkConfig),
    #[schemars(title = "PQv1")]
    Pqv1(transferia::providers::pqv1::config::PqV1SinkConfig),
    #[schemars(title = "S3")]
    S3(Box<transferia::providers::s3::sink::S3SinkConfig>),
    #[schemars(title = "YTsaurus")]
    Ytsaurus(transferia::providers::ytsaurus::YTsaurusSinkConfig),
    #[schemars(title = "Discard (benchmark)")]
    Discard(EmptyFormSchema),
}

#[derive(JsonSchema)]
#[serde(rename_all = "lowercase")]
#[expect(dead_code, reason = "variants are consumed by the JsonSchema derive")]
enum MiddlewareFormSchema {
    Filter(transferia::middleware::filter::FilterConfig),
}

#[derive(JsonSchema)]
struct EmptyFormSchema {}

#[derive(Serialize)]
struct ConfigFormDefinition {
    schema: Value,
    initial: Value,
    source_presets: BTreeMap<&'static str, Value>,
    sink_presets: BTreeMap<&'static str, Value>,
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

#[expect(
    clippy::print_stdout,
    reason = "the interactive demo intentionally prints its discoverable UI URL"
)]
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
        (&Method::GET, "/api/config/schema") => json_response(&config_form_definition()?),
        (&Method::GET, "/api/deliveries") => {
            let stored = state.stored.lock().await;
            json_response(&stored.deliveries.values().collect::<Vec<_>>())
        }
        (&Method::POST, "/api/discover") => {
            let body: DiscoveryRequest = read_json(request).await?;
            let config_yaml = config_yaml_from_json(&body.config)?;
            json_response(&discover(&config_yaml).await?)
        }
        (&Method::POST, "/api/deliveries") => {
            let body: CreateRequest = read_json(request).await?;
            anyhow::ensure!(
                !body.name.trim().is_empty(),
                "delivery name must not be empty"
            );
            let config_yaml = config_yaml_from_json(&body.config)?;
            let configured_delivery_id = Config::from_yaml(&config_yaml)?.delivery_id;
            discover(&config_yaml).await?;
            let id = new_id(&state);
            let delivery = StoredDelivery {
                id: id.clone(),
                name: body.name,
                config_yaml,
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

fn config_form_definition() -> anyhow::Result<ConfigFormDefinition> {
    let source_presets = BTreeMap::from([
        (
            "pqv1",
            serde_json::json!({
                "host": "localhost",
                "port": 2135,
                "topic_path": "/demo/events",
                "consumer_name": "transferia-demo",
                "partition_group_ids": [0],
                "auth": { "type": "access_token", "token": "demo" },
                "parser": json_parser_preset(),
                "network_timeout_ms": 30000,
                "decompression_concurrency": 4,
                "benchmark_discard_before_decompression": false
            }),
        ),
        (
            "postgres",
            serde_json::json!({
                "host": "localhost",
                "port": 5432,
                "database": "postgres",
                "username": "postgres",
                "password": "postgres",
                "trusted_plaintext": true,
                "tables": [{ "schema": "public", "name": "events" }],
                "batch_rows": 65536
            }),
        ),
        (
            "clickhouse",
            serde_json::json!({
                "hosts": ["localhost"],
                "port": transferia::providers::clickhouse::DEFAULT_NATIVE_PORT,
                "trusted_plaintext": true,
                "username": "",
                "password": "",
                "tables": [{
                    "database": "",
                    "name": "events",
                    "output_name": "events",
                    "order_by": ["id"]
                }],
                "batch_rows": 65536,
                "connect_timeout_ms": 30000,
                "request_timeout_ms": 30000
            }),
        ),
        (
            "s3",
            serde_json::json!({
                "bucket": "demo",
                "prefix": "input",
                "region": "us-east-1",
                "host": "localhost",
                "port": 4566,
                "allow_http": true,
                "credentials": { "access_key": "test", "secret_key": "test" },
                "parser": json_parser_preset(),
                "timeout_ms": 30000
            }),
        ),
        (
            "ytsaurus",
            serde_json::json!({
                "host": "localhost",
                "port": 8000,
                "trusted_plaintext": true,
                "timeout_ms": 30000,
                "tables": [{ "path": "//home/demo/events", "output_name": "events" }],
                "batch_rows": 65536
            }),
        ),
    ]);
    let sink_presets = BTreeMap::from([
        (
            "clickhouse",
            serde_json::json!({
                "hosts": ["localhost"],
                "port": transferia::providers::clickhouse::DEFAULT_NATIVE_PORT,
                "trusted_plaintext": true,
                "database": "",
                "username": "",
                "password": "",
                "insert_target_rows": 100_000,
                "insert_target_bytes": 67_108_864,
                "flush_interval_ms": 100,
                "retry_initial_ms": 50,
                "retry_max_ms": 30000,
                "connect_timeout_ms": 30000,
                "request_timeout_ms": 30000
            }),
        ),
        (
            "postgres",
            serde_json::json!({
                "host": "localhost",
                "port": 5432,
                "database": "postgres",
                "username": "postgres",
                "password": "postgres",
                "trusted_plaintext": true,
                "create_tables": true
            }),
        ),
        (
            "pqv1",
            serde_json::json!({
                "host": "localhost",
                "port": 2135,
                "topic_path": "/demo/output",
                "message_group_id": "transferia-demo",
                "partition_group_id": 0,
                "auth": { "type": "access_token", "token": "demo" },
                "trusted_plaintext": true,
                "network_timeout_ms": 30000
            }),
        ),
        (
            "s3",
            serde_json::json!({
                "bucket": "demo",
                "object_layout_version": 5,
                "region": "us-east-1",
                "host": "localhost",
                "port": 4566,
                "allow_http": true,
                "credentials": { "access_key": "test", "secret_key": "test" },
                "partitioning": { "type": "source" },
                "rotation": { "max_rows": 10000, "max_bytes": "32MiB", "on_partition_path_change": "keep_epoch" },
                "buffering": { "max_epoch_buffers": 32, "max_pending_upload_objects": 64, "max_buffered_bytes": "128MiB", "max_epoch_bytes": "64MiB" },
                "upload": { "multipart_threshold": "25MiB", "part_size": "5MiB", "parallel_parts": 2, "max_in_flight_objects": 2, "operation_timeout": "30s" },
                "retry": { "initial_backoff": "100ms", "max_backoff": "5s", "max_attempts": 10 }
            }),
        ),
        (
            "ytsaurus",
            serde_json::json!({
                "host": "localhost",
                "port": 8000,
                "trusted_plaintext": true,
                "timeout_ms": 30000,
                "tables": [{ "dataset": "events", "path": "//home/demo/events_out" }],
                "replace_tables": true,
                "format": "arrow"
            }),
        ),
        ("discard", serde_json::json!({})),
    ]);
    let initial = serde_json::json!({
        "delivery_id": "demo-delivery",
        "durable_storage": { "type": "local_file", "path": ".transferia-state" },
        "source": { "pqv1": source_presets["pqv1"].clone() },
        "sink": { "clickhouse": sink_presets["clickhouse"].clone() },
        "middlewares": [],
        "pipeline_memory_limit_bytes": 268_435_456,
        "keep_system_columns_in_sink": false,
        "metrics": null
    });
    Ok(ConfigFormDefinition {
        schema: serde_json::to_value(schema_for!(ConfigFormSchema))?,
        initial,
        source_presets,
        sink_presets,
    })
}

fn json_parser_preset() -> Value {
    serde_json::json!({
        "common": {
            "table_naming": { "type": "from_config", "name": "events" },
            "system_columns": {}
        },
        "json_parser": {
            "conversion_error": "dlq",
            "unknown_fields": { "action": "fail" },
            "chunk_splitter": "one-message-one-row",
            "primary_key": [],
            "system_column_names": {},
            "columns": [{
                "jsonpath": "$.id",
                "column_name": "id",
                "json_data_type": "integer",
                "arrow_type": "Int64",
                "nullable": false,
                "low_cardinality": false
            }]
        }
    })
}

fn config_yaml_from_json(config: &Value) -> anyhow::Result<String> {
    let yaml = serde_yaml::to_string(config).context("failed to render configuration as YAML")?;
    Config::from_yaml(&yaml)?;
    Ok(yaml)
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
    let mut response = Response::new(Full::new(Bytes::from_static(value.as_bytes())));
    response.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static(content_type),
    );
    response
}

fn text_response(status: StatusCode, value: impl Into<Bytes>) -> Response<HttpBody> {
    let mut response = Response::new(Full::new(value.into()));
    *response.status_mut() = status;
    response.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

fn error_response(error: &anyhow::Error) -> Response<HttpBody> {
    text_response(StatusCode::BAD_REQUEST, format!("{error:#}"))
}

#[cfg(test)]
#[path = "tests/server.rs"]
mod tests;
