use std::sync::Arc;

use anyhow::Context as _;
use clap::Parser;
use tokio_util::sync::CancellationToken;

use transferia::config::yaml::Config;
use transferia::delivery::DeliveryDiscoveryRequest;
use transferia::metrics::MetricsRegistry;
use transferia::pipeline::memory::PipelineMemory;
use transferia::providers::logbroker::YdbDriverSourceProvider;
use transferia::providers::traits::SourceProvider as _;
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
        config.source.kind()? == "logbroker",
        "the smoke config source must be logbroker"
    );
    let durable = config.durable_storage.build(&config.delivery_id)?;
    let metrics = Arc::new(MetricsRegistry::new());
    let source_config = serde_yaml::from_value(config.source.raw()?.clone())?;
    let provider = Arc::new(YdbDriverSourceProvider::from_config(
        source_config,
        metrics,
    )?);
    let cancellation = CancellationToken::new();
    let discovery = provider
        .delivery_discovery(
            DeliveryDiscoveryRequest {
                keep_system_columns: true,
            },
            cancellation.clone(),
        )
        .await?;
    let reader_lane = discovery
        .source_topology
        .partitions_for_worker(1, 0)?
        .into_iter()
        .next()
        .context("YDB Topic provider returned no reader lane")?;
    let mut source = provider
        .build_source(
            reader_lane,
            cancellation.child_token(),
            PipelineMemory::new(config.pipeline_memory_limit_bytes),
            durable,
        )
        .await
        .context("failed to open YDB Topic reader")?;
    let batch = tokio::time::timeout(
        core::time::Duration::from_secs(args.timeout_seconds),
        source.read_batch(),
    )
    .await
    .context("timed out waiting for a YDB Topic message")??;
    cancellation.cancel();
    let messages = match batch {
        SourceBatch::Raw { messages, .. } => messages,
        SourceBatch::Typed { .. } => anyhow::bail!("YDB Topic returned a typed batch"),
        SourceBatch::Finished => anyhow::bail!("YDB Topic finished before returning a message"),
    };
    anyhow::ensure!(!messages.is_empty(), "YDB Topic returned an empty batch");
    let first = &messages[0];
    tracing::info!(
        topic = first.meta.topic.as_deref(),
        partition_id = first.meta.partition,
        message_count = messages.len(),
        offset = first.meta.offset,
        payload_bytes = first.value.len(),
        "YDB Topic smoke read succeeded without committing offsets"
    );
    Ok(())
}
