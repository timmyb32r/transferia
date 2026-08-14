use std::sync::Arc;

use anyhow::Context as _;
use clap::Parser;
use futures_util::stream::{FuturesUnordered, StreamExt as _};
use tokio_util::sync::CancellationToken;

use transferia::config::yaml::Config;
use transferia::delivery::DeliveryDiscoveryRequest;
use transferia::metrics::MetricsRegistry;
use transferia::pipeline::memory::PipelineMemory;
use transferia::providers::traits::SourceProvider as _;
use transferia::providers::ydb_topic::YdbTopicSourceProvider;
use transferia::types::message::SourceBatch;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    config: String,

    #[arg(long, default_value_t = 60)]
    timeout_seconds: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    let config = Config::from_file(&args.config)?;
    anyhow::ensure!(
        config.source.kind()? == "ydb_topic",
        "the smoke config source must be ydb_topic"
    );
    let durable = config.durable_storage.build(&config.delivery_id)?;
    let metrics = Arc::new(MetricsRegistry::new());
    let provider = Arc::new(YdbTopicSourceProvider::from_config(
        config.source.raw()?.clone(),
        metrics,
    )?);
    let cancellation = CancellationToken::new();
    provider
        .delivery_discovery(
            DeliveryDiscoveryRequest {
                keep_system_columns: config.keep_system_columns_in_sink,
            },
            cancellation.clone(),
        )
        .await?;
    let partitions = provider.partitions_for_worker(1, 0).await?;
    let mut reads = FuturesUnordered::new();
    for partition_id in partitions {
        let source = provider
            .build_source(
                partition_id,
                cancellation.child_token(),
                PipelineMemory::new(config.pipeline_memory_limit_bytes),
                durable.clone(),
            )
            .await
            .with_context(|| format!("failed to open YDB Topic partition {partition_id}"))?;
        reads.push(async move {
            let mut source = source;
            (partition_id, source.read_batch().await)
        });
    }
    let result = tokio::time::timeout(
        core::time::Duration::from_secs(args.timeout_seconds),
        reads.next(),
    )
    .await
    .context("timed out waiting for a YDB Topic message")?
    .context("YDB Topic discovery returned no partitions")?;
    cancellation.cancel();
    let (partition_id, batch) = result;
    let batch = batch.with_context(|| format!("read failed for partition {partition_id}"))?;
    let messages = match batch {
        SourceBatch::Raw { messages, .. } => messages,
        SourceBatch::Typed { .. } => anyhow::bail!("YDB Topic returned a typed batch"),
        SourceBatch::Finished => anyhow::bail!("YDB Topic finished before returning a message"),
    };
    anyhow::ensure!(!messages.is_empty(), "YDB Topic returned an empty batch");
    let first = &messages[0];
    tracing::info!(
        partition_id,
        message_count = messages.len(),
        offset = first.meta.offset,
        payload_bytes = first.value.len(),
        "YDB Topic smoke read succeeded without committing offsets"
    );
    Ok(())
}
