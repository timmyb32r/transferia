mod config;
mod middleware;
mod parser;
mod partition;
mod pipeline;
mod sink;
mod source;
mod types;

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use clap::Parser;
use mimalloc::MiMalloc;
use tokio::signal;
use tokio_util::sync::CancellationToken;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use crate::config::yaml::{build_credentials, Config};
use crate::middleware::filter::FilterMiddleware;
use crate::parser::JsonParser;
use crate::pipeline::middleware::Middleware;
use crate::pipeline::run_partition_pipeline;
use crate::sink::clickhouse::ClickHouseSink;
use crate::source::ydb_topic::YdbTopicSource;

/// Monotonic batch-id counter — `u64` avoids heap allocation per batch.
static BATCH_ID: AtomicU64 = AtomicU64::new(1);

pub(crate) fn batch_id() -> u64 {
    BATCH_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

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

    // 2. Discover partitions — scope-drop YDB client immediately after
    let my_partitions = {
        let discovery_creds = build_credentials(&config.source.auth)?;
        let client = ydb::ClientBuilder::new_from_connection_string(&config.source.connection_string)?
            .with_credentials(discovery_creds)
            .client()?;
        let mut topic_client = client.topic_client();
        crate::partition::discover_my_partitions(
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

    // 3. Shared parser
    let parser = Arc::new(JsonParser::new(&config.schema)?);

    // 4. Middlewares
    let middlewares: Vec<Box<dyn Middleware>> = config.middlewares.iter().map(|mc| {
        match mc.mw_type.as_str() {
            "filter" => {
                let mw = FilterMiddleware::new(
                    mc.field.clone().unwrap_or_default(),
                    mc.value.clone().unwrap_or_default(),
                )?;
                Ok(Box::new(mw) as Box<dyn Middleware>)
            }
            other => anyhow::bail!("Unknown middleware type: {}", other),
        }
    }).collect::<anyhow::Result<_>>()?;
    let middlewares = Arc::new(middlewares);

    // 5. Shared sink (connection pool)
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

    // 7. Spawn one task per partition
    let conn_string = config.source.connection_string.clone();
    let topic_path = config.source.topic_path.clone();
    let consumer_name = config.source.consumer_name.clone();
    let auth_config = config.source.auth.clone();
    let partition_ids = my_partitions.clone();
    let batch_size = config.sink.batch_size;
    let max_linger_ms = config.sink.max_linger_ms;

    let mut handles = Vec::new();
    for partition_id in partition_ids {
        let conn = conn_string.clone();
        let tpath = topic_path.clone();
        let consumer = consumer_name.clone();
        let auth = auth_config.clone();
        let parser = parser.clone();
        let mw = middlewares.clone();
        let snk = sink.clone();
        let token = cancel_token.clone();

        handles.push(tokio::spawn(async move {
            let mut retry_count = 0u32;
            let max_retries = 5u32;
            loop {
                if token.is_cancelled() {
                    return;
                }

                let source = match build_credentials(&auth) {
                    Ok(creds) => match YdbTopicSource::new(&conn, &tpath, &consumer, partition_id, creds).await {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::error!("Failed to create YDB source for partition {}: {}", partition_id, e);
                            break;
                        }
                    },
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
                            tracing::error!("Partition {} exhausted retries. Aborting.", partition_id);
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    }
                }
            }
        }));
    }

    // Wait for all partition tasks
    for handle in handles {
        let _ = handle.await;
    }
    tracing::info!("All partition tasks completed. Exiting.");
    Ok(())
}
