use std::sync::Arc;

use clap::Parser;
use mimalloc::MiMalloc;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use ydb_ch_replicator::config::yaml::{build_credentials, build_credentials_with_token, Config, SourceConfig};
use ydb_ch_replicator::middleware::filter::FilterMiddleware;
use ydb_ch_replicator::parser::JsonParser;
use ydb_ch_replicator::types::table_data::dlq_name;
use ydb_ch_replicator::pipeline::middleware::Middleware;
use ydb_ch_replicator::pipeline::run_partition_pipeline;
use ydb_ch_replicator::sink::clickhouse::ClickHouseSink;
use ydb_ch_replicator::source::pq_v1::{parse_endpoint, partition_to_group, PqV1Client, PqV1Source};
use ydb_ch_replicator::source::s3::S3Source;
use ydb_ch_replicator::source::s3_config::build_object_store;
use ydb_ch_replicator::source::ydb_topic::YdbTopicSource;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Parser, Debug)]
#[command(name = "ydb-ch-replicator", about = "YDB Topic to ClickHouse replicator")]
struct Cli {
    #[arg(long, env = "CONFIG_PATH")]
    config: String,

    #[arg(long, default_value_t = 1)]
    total_workers: u32,

    #[arg(long, default_value_t = 0)]
    worker_index: u32,
}

/// Shared dependencies handed to every partition task. Cheap to clone (Arcs + token).
#[derive(Clone)]
struct PipelineDeps {
    parser: Arc<JsonParser>,
    mw: Arc<Vec<Box<dyn Middleware>>>,
    snk: Arc<ClickHouseSink>,
    batch_size: usize,
    max_linger_ms: u64,
    token: CancellationToken,
}

/// Generic partition task: creates a source via the async `make_source` factory,
/// then runs the pipeline with retry/backoff. Shared by both PQv1 and YDB Topic sources.
fn spawn_partition_task<S, F, Fut>(
    partition_id: i64,
    deps: PipelineDeps,
    source_label: &'static str,
    mut make_source: F,
) -> tokio::task::JoinHandle<()>
where
    S: ydb_ch_replicator::pipeline::source::Source + 'static,
    F: FnMut() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = anyhow::Result<S>> + Send,
{
    let max_retries = 5u32;
    tokio::spawn(async move {
        let mut retry_count = 0u32;
        loop {
            if deps.token.is_cancelled() {
                return;
            }

            let source = match make_source().await {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("{} source creation for partition {}: {}", source_label, partition_id, e);
                    break;
                }
            };

            match run_partition_pipeline(
                source,
                deps.parser.clone(),
                deps.mw.clone(),
                deps.snk.clone(),
                deps.batch_size,
                deps.max_linger_ms,
                deps.token.clone(),
                partition_id,
            )
            .await
            {
                Ok(()) => break,
                Err(e) => {
                    retry_count += 1;
                    tracing::error!(
                        "Partition {} ({}) fatal error (retry {}/{}): {}",
                        partition_id, source_label, retry_count, max_retries, e,
                    );
                    if retry_count >= max_retries {
                        tracing::error!(
                            "Partition {} ({}) exhausted retries.",
                            partition_id, source_label
                        );
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }
        }
    })
}

fn spawn_pqv1_task(
    conn: String,
    tpath: String,
    consumer: String,
    auth: ydb_ch_replicator::config::yaml::AuthConfig,
    partition_id: i64,
    deps: PipelineDeps,
) -> tokio::task::JoinHandle<()> {
    spawn_partition_task::<PqV1Source, _, _>(partition_id, deps, "PQv1", move || {
        let conn = conn.clone();
        let tpath = tpath.clone();
        let consumer = consumer.clone();
        let auth = auth.clone();
        async move {
            let (_, raw_token) = build_credentials_with_token(&auth)?;
            let token_str = raw_token
                .ok_or_else(|| anyhow::anyhow!("PQv1 requires access_token auth"))?;
            let (scheme, host, _) = parse_endpoint(&conn)?;
            let endpoint = format!("{}://{}", scheme, host);
            let pg_id = partition_to_group(partition_id);

            let (client, mut queues) = PqV1Client::connect(
                &endpoint, &tpath, &consumer, &token_str, &[pg_id],
            )
            .await?;

            let rx = queues
                .remove(&partition_id)
                .ok_or_else(|| anyhow::anyhow!("No queue for partition {}", partition_id))?;

            Ok(PqV1Source::new(client, rx, partition_id))
        }
    })
}

fn spawn_ydb_task(
    conn: String,
    tpath: String,
    consumer: String,
    auth: ydb_ch_replicator::config::yaml::AuthConfig,
    disc_ep: Option<String>,
    partition_id: i64,
    deps: PipelineDeps,
) -> tokio::task::JoinHandle<()> {
    spawn_partition_task::<YdbTopicSource, _, _>(partition_id, deps, "YDB", move || {
        let conn = conn.clone();
        let tpath = tpath.clone();
        let consumer = consumer.clone();
        let auth = auth.clone();
        let disc_ep = disc_ep.clone();
        async move {
            let creds = build_credentials(&auth)?;
            YdbTopicSource::new(
                &conn, &tpath, &consumer, partition_id, creds, disc_ep.as_deref(),
            )
            .await
        }
    })
}

fn spawn_s3_task(
    store: Arc<dyn object_store::ObjectStore>,
    prefix: String,
    framer: ydb_ch_replicator::config::yaml::ChunkSplitter,
    chunk_size: usize,
    max_retries: u32,
    partition_id: i64,
    deps: PipelineDeps,
) -> tokio::task::JoinHandle<()> {
    spawn_partition_task::<S3Source, _, _>(partition_id, deps, "S3", move || {
        let store = store.clone();
        let prefix = prefix.clone();
        async move {
            S3Source::new(store, &prefix, framer, chunk_size, max_retries, partition_id).await
        }
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    tracing::info!(
        "ydb-ch-replicator starting (worker {}/{})",
        cli.worker_index,
        cli.total_workers,
    );

    // 1. Load config
    let config = Config::from_file(&cli.config)?;
    tracing::info!(
        "Source type: {}",
        match &config.source {
            SourceConfig::Topic(_) => "topic",
            SourceConfig::Pqv1(_) => "pqv1",
            SourceConfig::S3(_) => "s3",
        }
    );

    // 2. Discover partitions + resolve table name
    let (table, parser_cfg, my_partitions) = match &config.source {
        SourceConfig::Pqv1(p) => {
            let (_creds, token) = build_credentials_with_token(&p.auth)?;
            let token = token.ok_or_else(|| anyhow::anyhow!("PQv1 requires access_token auth"))?;
            let parts = if let Some(ref static_ids) = p.partition_ids {
                static_ids.iter()
                    .filter(|id| id.unsigned_abs() as u32 % cli.total_workers == cli.worker_index)
                    .copied()
                    .collect()
            } else {
                let (scheme, host, _) = parse_endpoint(&p.connection_string)?;
                let endpoint = format!("{}://{}", scheme, host);
                PqV1Client::discover_partitions(&endpoint, &p.topic_path, &p.consumer_name, &token)
                    .await?
                    .into_iter()
                    .filter(|id| id.unsigned_abs() as u32 % cli.total_workers == cli.worker_index)
                    .collect()
            };
            let table: Arc<str> = p.parser.resolve_table_name(&p.topic_path)?.into();
            (table, &p.parser, parts)
        }
        SourceConfig::Topic(t) => {
            let creds = build_credentials(&t.auth)?;
            let mut builder = ydb::ClientBuilder::new_from_connection_string(&t.connection_string)?
                .with_credentials(creds);
            if let Some(ref ep) = t.discovery_endpoint {
                let discovery = ydb::StaticDiscovery::new_from_str(ep.as_str())
                    .map_err(|e| anyhow::anyhow!("StaticDiscovery: {}", e))?;
                builder = builder.with_discovery(discovery);
            }
            let client = builder.client()?;
            let mut topic_client = client.topic_client();
            let parts = ydb_ch_replicator::partition::discover_my_partitions(
                &mut topic_client, &t.topic_path, cli.total_workers, cli.worker_index,
            ).await?;
            let table: Arc<str> = t.parser.resolve_table_name(&t.topic_path)?.into();
            (table, &t.parser, parts)
        }
        SourceConfig::S3(s) => {
            if cli.worker_index != 0 {
                tracing::info!("S3 snapshot runs on worker 0 only — worker {} exiting", cli.worker_index);
                return Ok(());
            }
            let table: Arc<str> = s.parser.resolve_table_name(&s.prefix)?.into();
            // S3: synthetic partition [0]
            (table, &s.parser, vec![0])
        }
    };

    if my_partitions.is_empty() {
        tracing::warn!("No partitions assigned. Exiting.");
        return Ok(());
    }
    tracing::info!("My partitions: {:?}", my_partitions);
    tracing::info!("Destination table: {}", table);

    // 3. Shared parser
    let parser = Arc::new(JsonParser::new(&parser_cfg.settings, table.clone())?);

    // 4. Middlewares
    let middlewares: Vec<Box<dyn Middleware>> = config
        .middlewares.iter()
        .map(|mc| match mc.mw_type.as_str() {
            "filter" => Ok(Box::new(FilterMiddleware::new(
                mc.field.clone().unwrap_or_default(),
                mc.value.clone().unwrap_or_default(),
            )?) as Box<dyn Middleware>),
            other => anyhow::bail!("Unknown middleware type: {}", other),
        })
        .collect::<anyhow::Result<_>>()?;
    let middlewares = Arc::new(middlewares);

    // 5. Shared sink
    let sink = ClickHouseSink::new(&config.sink).await?;
    let main_cols = ClickHouseSink::schema_columns(&parser_cfg.settings)?;
    sink.create_table(&table, &main_cols, &parser_cfg.settings.order_by, config.sink.recreate_tables).await?;
    let dlq_table = dlq_name(&table);
    let dlq_cols: Vec<(String, String)> = ydb_ch_replicator::parser::json_parser::DLQ_CH_COLUMNS
        .iter().map(|(n, t)| ((*n).to_string(), (*t).to_string())).collect();
    sink.create_table(&dlq_table, &dlq_cols, &[], config.sink.recreate_tables).await?;
    sink.verify_table(&table).await?;
    sink.verify_table(&dlq_table).await?;
    let sink = Arc::new(sink);

    // 6. Graceful shutdown
    let cancel_token = CancellationToken::new();
    let ct_clone = cancel_token.clone();
    tokio::spawn(async move {
        signal::ctrl_c().await.ok();
        tracing::info!("SIGTERM/SIGINT received, initiating graceful shutdown...");
        ct_clone.cancel();
    });

    // 7. Spawn tasks
    let deps = PipelineDeps {
        parser, mw: middlewares, snk: sink,
        batch_size: config.sink.batch_size,
        max_linger_ms: config.sink.max_linger_ms,
        token: cancel_token.clone(),
    };
    let mut handles = Vec::new();

    match &config.source {
        SourceConfig::Pqv1(p) => {
            for pid in my_partitions {
                handles.push(spawn_pqv1_task(
                    p.connection_string.clone(), p.topic_path.clone(),
                    p.consumer_name.clone(), p.auth.clone(), pid, deps.clone(),
                ));
            }
        }
        SourceConfig::Topic(t) => {
            for pid in my_partitions {
                handles.push(spawn_ydb_task(
                    t.connection_string.clone(), t.topic_path.clone(),
                    t.consumer_name.clone(), t.auth.clone(),
                    t.discovery_endpoint.clone(), pid, deps.clone(),
                ));
            }
        }
        SourceConfig::S3(s) => {
            let store = build_object_store(s)?;
            handles.push(spawn_s3_task(
                store, s.prefix.clone(), s.parser.settings.chunk_splitter,
                s.chunk_size_bytes, s.max_retries, 0, deps.clone(),
            ));
        }
    }

    for handle in handles {
        let _ = handle.await;
    }
    tracing::info!("All partition tasks completed. Exiting.");
    Ok(())
}
