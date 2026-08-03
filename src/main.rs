use std::sync::Arc;

use clap::Parser;
use mimalloc::MiMalloc;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use ydb_ch_replicator::config::yaml::{build_credentials, build_credentials_with_token, Config};
use ydb_ch_replicator::middleware::filter::FilterMiddleware;
use ydb_ch_replicator::parser::JsonParser;
use ydb_ch_replicator::pipeline::middleware::Middleware;
use ydb_ch_replicator::pipeline::run_partition_pipeline;
use ydb_ch_replicator::sink::clickhouse::ClickHouseSink;
use ydb_ch_replicator::source::pq_v1::{parse_endpoint, partition_to_group, PqV1Client, PqV1Source};
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    tracing::info!(
        "ydb-ch-replicator starting (worker {}/{})",
        cli.worker_index,
        cli.total_workers,
    );

    // 1. Load config
    let config = Config::from_file(&cli.config)?;
    let is_pqv1 = config.source.source_type == "pqv1";
    tracing::info!("Source type: {}", config.source.source_type);

    // 2. Discover partitions
    let my_partitions: Vec<i64> = if is_pqv1 {
        let (_creds, token) = build_credentials_with_token(&config.source.auth)?;
        let token = token.ok_or_else(|| anyhow::anyhow!("PQv1 requires access_token auth"))?;

        if let Some(ref static_ids) = config.source.partition_ids {
            static_ids
                .iter()
                .filter(|id| id.unsigned_abs() as u32 % cli.total_workers == cli.worker_index)
                .copied()
                .collect()
        } else {
            let (scheme, host, _) = parse_endpoint(&config.source.connection_string)?;
            let endpoint = format!("{}://{}", scheme, host);
            match PqV1Client::describe_topic(&endpoint, &config.source.topic_path, &token)
                .await
            {
                Ok(count) => {
                    let all: Vec<i64> = (0..count as i64).collect();
                    all.into_iter()
                        .filter(|id| id.unsigned_abs() as u32 % cli.total_workers == cli.worker_index)
                        .collect()
                }
                Err(e) => {
                    tracing::warn!("DescribeTopic failed ({}), trying static partition_ids", e);
                    if let Some(ref static_ids) = config.source.partition_ids {
                        static_ids
                            .iter()
                            .filter(|id| id.unsigned_abs() as u32 % cli.total_workers == cli.worker_index)
                            .copied()
                            .collect()
                    } else {
                        anyhow::bail!(
                            "PQv1: DescribeTopic failed and no partition_ids in config"
                        );
                    }
                }
            }
        }
    } else {
        let discovery_creds = build_credentials(&config.source.auth)?;
        let mut builder =
            ydb::ClientBuilder::new_from_connection_string(&config.source.connection_string)?
                .with_credentials(discovery_creds);
        if let Some(ref endpoint) = config.source.discovery_endpoint {
            let discovery = ydb::StaticDiscovery::new_from_str(endpoint.as_str())
                .map_err(|e| anyhow::anyhow!("StaticDiscovery: {}", e))?;
            builder = builder.with_discovery(discovery);
        }
        let client = builder.client()?;
        let mut topic_client = client.topic_client();
        ydb_ch_replicator::partition::discover_my_partitions(
            &mut topic_client,
            &config.source.topic_path,
            cli.total_workers,
            cli.worker_index,
        )
        .await?
    };

    if my_partitions.is_empty() {
        tracing::warn!("No partitions assigned. Exiting.");
        return Ok(());
    }
    tracing::info!("My partitions: {:?}", my_partitions);

    // 3. Shared parser — resolves the destination table name and stamps it into batches.
    let table: Arc<str> = config
        .source
        .parser
        .resolve_table_name(&config.source.topic_path)?
        .into();
    tracing::info!("Destination table: {}", table);
    let parser = Arc::new(JsonParser::new(&config.source.parser.settings, table.clone())?);

    // 4. Middlewares
    let middlewares: Vec<Box<dyn Middleware>> = config
        .middlewares
        .iter()
        .map(|mc| match mc.mw_type.as_str() {
            "filter" => {
                let mw = FilterMiddleware::new(
                    mc.field.clone().unwrap_or_default(),
                    mc.value.clone().unwrap_or_default(),
                )?;
                Ok(Box::new(mw) as Box<dyn Middleware>)
            }
            other => anyhow::bail!("Unknown middleware type: {}", other),
        })
        .collect::<anyhow::Result<_>>()?;
    let middlewares = Arc::new(middlewares);

    // 5. Shared sink
    let sink = ClickHouseSink::new(&config.sink).await?;
    sink.create_tables(&config.source.parser.settings, &table, config.sink.recreate_tables).await?;
    sink.verify_tables(&table).await?;
    let sink = Arc::new(sink);

    // 6. Graceful shutdown
    let cancel_token = CancellationToken::new();
    let ct_clone = cancel_token.clone();
    tokio::spawn(async move {
        signal::ctrl_c().await.ok();
        tracing::info!("SIGTERM/SIGINT received, initiating graceful shutdown...");
        ct_clone.cancel();
    });

    // 7. Spawn tasks — PQv1 or YDB
    let deps = PipelineDeps {
        parser,
        mw: middlewares,
        snk: sink,
        batch_size: config.sink.batch_size,
        max_linger_ms: config.sink.max_linger_ms,
        token: cancel_token.clone(),
    };
    let mut handles = Vec::new();

    if is_pqv1 {
        for partition_id in my_partitions {
            handles.push(spawn_pqv1_task(
                config.source.connection_string.clone(),
                config.source.topic_path.clone(),
                config.source.consumer_name.clone(),
                config.source.auth.clone(),
                partition_id,
                deps.clone(),
            ));
        }
    } else {
        for partition_id in my_partitions {
            handles.push(spawn_ydb_task(
                config.source.connection_string.clone(),
                config.source.topic_path.clone(),
                config.source.consumer_name.clone(),
                config.source.auth.clone(),
                config.source.discovery_endpoint.clone(),
                partition_id,
                deps.clone(),
            ));
        }
    }

    for handle in handles {
        let _ = handle.await;
    }
    tracing::info!("All partition tasks completed. Exiting.");
    Ok(())
}
