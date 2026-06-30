mod config;
mod middleware;
mod partition;
mod pipeline;
mod sink;
mod source;
mod types;

use std::sync::Arc;

use clap::Parser;
use tokio::signal;
use tokio_util::sync::CancellationToken;

use crate::config::yaml::{build_credentials, Config};
use crate::middleware::json_arrow::JsonArrowMiddleware;
use crate::partition::discover_my_partitions;
use crate::pipeline::run_partition_pipeline;
use crate::sink::clickhouse::ClickHouseSink;
use crate::source::ydb_topic::YdbTopicSource;

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

    // 1. Load YAML config
    let config = Config::from_file(&cli.config)?;

    // 2. Discover partitions
    let discovery_creds = build_credentials(&config.source.auth)?;
    let client = ydb::ClientBuilder::new_from_connection_string(&config.source.connection_string)?
        .with_credentials(discovery_creds)
        .client()?;
    let mut topic_client = client.topic_client();
    let my_partitions = discover_my_partitions(
        &mut topic_client,
        &config.source.topic_path,
        cli.total_workers,
        cli.worker_index,
    )
    .await?;

    if my_partitions.is_empty() {
        tracing::warn!("No partitions assigned. Exiting.");
        return Ok(());
    }

    // 3. Build shared middleware (stateless, can be Arc'd)
    let middleware = Arc::new(JsonArrowMiddleware::new(
        &config.schema,
        &config.sink.table_name,
        &config.sink.dlq_table_name,
    )?);

    // 4. Build shared sink (single ClickHouse connection)
    let sink = ClickHouseSink::new(&config.sink).await?;
    sink.verify_tables().await?;
    let sink = Arc::new(sink);

    // 5. Graceful shutdown: wire ctrl_c to CancellationToken
    let cancel_token = CancellationToken::new();
    let ct_clone = cancel_token.clone();
    tokio::spawn(async move {
        signal::ctrl_c().await.ok();
        tracing::info!("SIGTERM/SIGINT received, initiating graceful shutdown...");
        ct_clone.cancel();
    });

    // 6. Clone config parts needed inside spawned tasks
    let conn_string = config.source.connection_string.clone();
    let topic_path = config.source.topic_path.clone();
    let consumer_name = config.source.consumer_name.clone();
    let auth_config = config.source.auth.clone();
    let partition_ids = my_partitions.clone();

    // 7. Spawn one task per partition with retry wrapper
    let mut handles = Vec::new();
    for partition_id in partition_ids {
        let partition_creds = build_credentials(&auth_config)?;
        let mut source = YdbTopicSource::new(
            &conn_string,
            &topic_path,
            &consumer_name,
            partition_id,
            partition_creds,
        )
        .await?;

        let mid = middleware.clone();
        let snk = sink.clone();
        let token = cancel_token.clone();
        let conn = conn_string.clone();
        let tpath = topic_path.clone();
        let consumer = consumer_name.clone();
        let auth = auth_config.clone();

        handles.push(tokio::spawn(async move {
            let mut retry_count = 0u32;
            let max_retries = 5u32;
            loop {
                if token.is_cancelled() {
                    return;
                }
                match run_partition_pipeline(&mut source, mid.as_ref(), snk.as_ref(), token.clone())
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
                        match build_credentials(&auth) {
                            Ok(creds) => {
                                match YdbTopicSource::new(&conn, &tpath, &consumer, partition_id, creds).await {
                                    Ok(new_source) => source = new_source,
                                    Err(e2) => {
                                        tracing::error!("Failed to re-create source: {}", e2);
                                        break;
                                    }
                                }
                            }
                            Err(e2) => {
                                tracing::error!("Failed to build credentials: {}", e2);
                                break;
                            }
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    }
                }
            }
        }));
    }

    // 8. Wait for all partition tasks
    futures::future::join_all(handles).await;

    tracing::info!("All partition tasks completed. Exiting.");
    Ok(())
}
