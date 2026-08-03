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

fn spawn_pqv1_task(
    conn: String,
    tpath: String,
    consumer: String,
    database_str: String,
    auth: ydb_ch_replicator::config::yaml::AuthConfig,
    partition_id: i64,
    parser: Arc<JsonParser>,
    mw: Arc<Vec<Box<dyn Middleware>>>,
    snk: Arc<ClickHouseSink>,
    batch_size: usize,
    max_linger_ms: u64,
    token: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut retry_count = 0u32;
        let max_retries = 5u32;
        loop {
            if token.is_cancelled() {
                return;
            }

            let (_, raw_token) = match build_credentials_with_token(&auth) {
                Ok(t) => t,
                Err(e) => {
                    tracing::error!("PQv1 credentials: {}", e);
                    break;
                }
            };
            let token_str = match raw_token {
                Some(t) => t,
                None => {
                    tracing::error!("PQv1 requires access_token auth");
                    break;
                }
            };

            let (scheme, host, _db_from_conn) = match parse_endpoint(&conn) {
                Ok(p) => p,
                Err(e) => {
                    tracing::error!("PQv1 parse_endpoint: {}", e);
                    break;
                }
            };
            let endpoint = format!("{}://{}", scheme, host);
            let database = &database_str;
            let pg_id = partition_to_group(partition_id);

            tracing::info!(
                "[PQV1-DIAG] spawn_pqv1_task: endpoint={} database={} topic={} consumer={} partition={}",
                endpoint, database, tpath, consumer, partition_id
            );

            let (client, mut queues) = match PqV1Client::connect(
                &endpoint, &database, &tpath, &consumer, &token_str, &[pg_id],
            )
            .await
            {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!("PQv1 connect for partition {}: {}", partition_id, e);
                    break;
                }
            };

            let rx = match queues.remove(&partition_id) {
                Some(rx) => rx,
                None => {
                    tracing::error!("No queue for partition {}", partition_id);
                    break;
                }
            };

            let source = PqV1Source::new(client, rx, partition_id);

            match run_partition_pipeline(
                source, parser.clone(), mw.clone(), snk.clone(),
                batch_size, max_linger_ms, token.clone(),
            )
            .await
            {
                Ok(()) => break,
                Err(e) => {
                    retry_count += 1;
                    tracing::error!(
                        "Partition {} fatal error (retry {}/{}): {}",
                        partition_id, retry_count, max_retries, e,
                    );
                    if retry_count >= max_retries {
                        tracing::error!("Partition {} exhausted retries.", partition_id);
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }
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
    parser: Arc<JsonParser>,
    mw: Arc<Vec<Box<dyn Middleware>>>,
    snk: Arc<ClickHouseSink>,
    batch_size: usize,
    max_linger_ms: u64,
    token: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut retry_count = 0u32;
        let max_retries = 5u32;
        loop {
            if token.is_cancelled() {
                return;
            }

            let source = match build_credentials(&auth) {
                Ok(creds) => {
                    match YdbTopicSource::new(
                        &conn, &tpath, &consumer, partition_id, creds, disc_ep.as_deref(),
                    )
                    .await
                    {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::error!(
                                "Failed to create YDB source for partition {}: {}",
                                partition_id, e
                            );
                            break;
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to build credentials: {}", e);
                    break;
                }
            };

            match run_partition_pipeline(
                source, parser.clone(), mw.clone(), snk.clone(),
                batch_size, max_linger_ms, token.clone(),
            )
            .await
            {
                Ok(()) => break,
                Err(e) => {
                    retry_count += 1;
                    tracing::error!(
                        "Partition {} fatal error (retry {}/{}): {}",
                        partition_id, retry_count, max_retries, e,
                    );
                    if retry_count >= max_retries {
                        tracing::error!("Partition {} exhausted retries.", partition_id);
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }
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
            let (scheme, host, database) = parse_endpoint(&config.source.connection_string)?;
            let endpoint = format!("{}://{}", scheme, host);
            match PqV1Client::describe_topic(&endpoint, &database, &config.source.topic_path, &token)
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

    // 3. Shared parser
    let parser = Arc::new(JsonParser::new(&config.schema)?);

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
    sink.verify_tables().await?;
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
    let batch_size = config.sink.batch_size;
    let max_linger_ms = config.sink.max_linger_ms;
    let mut handles = Vec::new();

    if is_pqv1 {
        for partition_id in my_partitions {
            handles.push(spawn_pqv1_task(
                config.source.connection_string.clone(),
                config.source.topic_path.clone(),
                config.source.consumer_name.clone(),
                config.source.database.clone(),
                config.source.auth.clone(),
                partition_id,
                parser.clone(),
                middlewares.clone(),
                sink.clone(),
                batch_size,
                max_linger_ms,
                cancel_token.clone(),
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
                parser.clone(),
                middlewares.clone(),
                sink.clone(),
                batch_size,
                max_linger_ms,
                cancel_token.clone(),
            ));
        }
    }

    for handle in handles {
        let _ = handle.await;
    }
    tracing::info!("All partition tasks completed. Exiting.");
    Ok(())
}
