use std::sync::Arc;

use anyhow::Context as _;
use clap::Parser;
use tokio_util::sync::CancellationToken;

use transferia::connectors::logbroker::YdbDriverSourceConnector;
use transferia::core::data::message::SourceBatch;
use transferia::core::delivery::DeliveryDiscoveryRequest;
use transferia::core::memory::PipelineMemory;
use transferia::delivery::config::yaml::Config;
use transferia::metrics::MetricsRegistry;
use transferia::registry::{SourceBuildContext, SourceConnector as _, SourceDiscoveryContext};

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
    let connector = Arc::new(YdbDriverSourceConnector::from_config(
        source_config,
        metrics,
    )?);
    let cancellation = CancellationToken::new();
    let discovery = connector
        .delivery_discovery(SourceDiscoveryContext {
            request: DeliveryDiscoveryRequest {
                keep_system_columns: true,
            },
            cancellation: cancellation.clone(),
            delivery_type: transferia::delivery::config::yaml::DeliveryType::Stream,
        })
        .await?;
    let reader_lane = discovery
        .source_topology
        .partitions_for_worker(1, 0)?
        .into_iter()
        .next()
        .context("YDB Topic connector returned no reader lane")?;
    let mut source = connector
        .build_source(SourceBuildContext {
            partition_id: reader_lane,
            delivery_type: transferia::delivery::config::yaml::DeliveryType::Stream,
            phase: transferia::registry::SourcePhase::Stream,
            replay_identity: None,
            cancellation: cancellation.child_token(),
            memory: PipelineMemory::new(config.pipeline_memory_limit_bytes),
            durable,
        })
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
        SourceBatch::Dataset { .. } => anyhow::bail!("YDB Topic unexpectedly requested dataset admission"),
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
