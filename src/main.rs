use std::sync::Arc;

use clap::Parser;
use mimalloc::MiMalloc;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use transferia::config::yaml::{validate_parser, Config};
use transferia::middleware::filter::FilterMiddleware;
use transferia::parser::JsonParser;
use transferia::types::table_data::dlq_name;
use transferia::pipeline::middleware::Middleware;
use transferia::pipeline::run_partition_pipeline;
use transferia::pipeline::source::Source;
use transferia::providers::clickhouse::sink::ClickHouseSink;
use transferia::providers::traits::ProviderRegistry;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Parser, Debug)]
#[command(name = "transferia", about = "Multi-source, multi-sink data transfer pipeline")]
struct Cli {
    #[arg(long, env = "CONFIG_PATH")]
    config: String,

    #[arg(long, default_value_t = 1)]
    total_workers: u32,

    #[arg(long, default_value_t = 0)]
    worker_index: u32,
}

#[derive(Clone)]
struct PipelineDeps {
    parser: Arc<JsonParser>,
    mw: Arc<Vec<Box<dyn Middleware>>>,
    snk: Arc<dyn transferia::pipeline::sink::Sink>,
    batch_size: usize,
    max_linger_ms: u64,
    token: CancellationToken,
}

fn spawn_partition_task<F, Fut>(
    partition_id: i64,
    deps: PipelineDeps,
    source_label: String,
    make_source: F,
) -> tokio::task::JoinHandle<()>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = anyhow::Result<Box<dyn Source>>> + Send,
{
    let max_retries = 5u32;
    tokio::spawn(async move {
        let mut retry_count = 0u32;
        let mut make_source = make_source;
        loop {
            if deps.token.is_cancelled() { return; }
            let source = match make_source().await {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("{} source creation for partition {}: {}", &source_label, partition_id, e);
                    break;
                }
            };
            match run_partition_pipeline(
                source, deps.parser.clone(), deps.mw.clone(), deps.snk.clone(),
                deps.batch_size, deps.max_linger_ms, deps.token.clone(), partition_id,
            ).await {
                Ok(()) => break,
                Err(e) => {
                    retry_count += 1;
                    tracing::error!("Partition {} ({}) fatal error (retry {}/{}): {}",
                        partition_id, source_label, retry_count, max_retries, e);
                    if retry_count >= max_retries {
                        tracing::error!("Partition {} ({}) exhausted retries.", partition_id, source_label);
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
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    tracing::info!("transferia starting (worker {}/{})", cli.worker_index, cli.total_workers);

    // 1. Load config
    let config = Config::from_file(&cli.config)?;

    // 2. Build registry + providers
    let mut registry = ProviderRegistry::new();

    // Register source providers
    registry.register_source("topic", |v| {
        Ok(Box::new(transferia::providers::yds::provider::YdsSourceProvider::from_config(v, "topic")?))
    });
    registry.register_source("pqv1", |v| {
        Ok(Box::new(transferia::providers::yds::provider::YdsSourceProvider::from_config(v, "pqv1")?))
    });
    registry.register_source("s3", |v| {
        Ok(Box::new(transferia::providers::s3::provider::S3SourceProvider::from_config(v)?))
    });
    registry.register_source("clickhouse", |v| {
        Ok(Box::new(transferia::providers::clickhouse::source_provider::ClickHouseSourceProvider::from_config(v)?))
    });

    // Register sink providers
    registry.register_sink("clickhouse", |v| {
        Ok(Box::new(transferia::providers::clickhouse::provider::ClickHouseSinkProvider::from_config(v)?))
    });
    registry.register_sink("empty", |v| {
        Ok(Box::new(transferia::providers::empty::provider::EmptySinkProvider::from_config(v)?))
    });
    registry.register_sink("s3", |v| {
        Ok(Box::new(transferia::providers::s3::sink::provider::S3SinkProvider::from_config(v)?))
    });
    registry.register_sink("yds", |v| {
        Ok(Box::new(transferia::providers::yds::sink::provider::YdsSinkProvider::from_config(v)?))
    });

    let source_kind = config.source.kind()?.to_string();
    let source_provider = registry.build_source(&source_kind, config.source.raw()?.clone())?;
    let sink_kind = config.sink.kind()?.to_string();
    let sink_provider = registry.build_sink(&sink_kind, config.sink.raw()?.clone())?;

    tracing::info!("Source: {}, Sink: {}", source_kind, sink_kind);

    // 2a. Determine and log guarantee mode (spec §11)
    let has_exactly_once_key = false; // TODO: wire add_exactly_once_key config flag
    let guarantee_mode = if has_exactly_once_key && sink_kind == "clickhouse" {
        "EXACTLY_ONCE"
    } else {
        "AT_LEAST_ONCE"
    };
    tracing::info!(
        "Guarantee mode: {} (key: __system_partition+__system_offset, sink: {}, source: {})",
        guarantee_mode, sink_kind, source_kind,
    );

    // 3. Common parser validation
    let allowed_parsers: std::collections::HashSet<&str> = ["json_parser"].into();
    validate_parser(source_provider.parser_config(), &config.middlewares, &allowed_parsers)?;

    // 4. Resolve table + partitions
    let table: Arc<str> = source_provider.resolve_table_name()?.into();
    let my_partitions = source_provider.discover_partitions(cli.total_workers, cli.worker_index).await?;
    if my_partitions.is_empty() {
        tracing::warn!("No partitions assigned. Exiting.");
        return Ok(());
    }
    tracing::info!("Destination table: {}, partitions: {:?}", table, my_partitions);

    // 5. Shared parser + middlewares
    // Exactly-once key: wired once the source config flag (add_exactly_once_key)
    // lands; until then the parser runs in at-least-once mode.
    let parser = Arc::new(JsonParser::new(&source_provider.parser_config().settings, table.clone(), None)?);
    let middlewares: Vec<Box<dyn Middleware>> = config.middlewares.iter()
        .map(|mc| match mc.mw_type.as_str() {
            "filter" => Ok(Box::new(FilterMiddleware::new(
                mc.field.clone().unwrap_or_default(),
                mc.value.clone().unwrap_or_default(),
            )?) as Box<dyn Middleware>),
            other => anyhow::bail!("Unknown middleware type: {}", other),
        })
        .collect::<anyhow::Result<_>>()?;
    let middlewares = Arc::new(middlewares);

    // 6. Sink + startup checks + DDL
    let sink = sink_provider.build_sink().await?;
    let dlq_table = dlq_name(&table);

    // 6a. Exactly-once startup checks (spec §8)
    if let Some(ch_sink) = sink.as_any().downcast_ref::<ClickHouseSink>() {
        // CH version ≥ 22.8
        ch_sink.check_ch_version().await?;
        tracing::info!("ClickHouse version check passed (≥ 22.8)");
    }

    // 6b. DDL: create tables
    sink_provider.create_tables(&table, &dlq_table,
        &source_provider.parser_config().settings, config.recreate_tables).await?;

    // 6c. Engine/replica validation (after CREATE so table exists)
    if let Some(ch_sink) = sink.as_any().downcast_ref::<ClickHouseSink>() {
        let (engine, quorum, replicas) = ch_sink.check_table_engine(&table).await?;
        tracing::info!("Table '{}' engine: {} (quorum={}, replicas={})", table, engine, quorum, replicas);

        if engine.contains("Replicated") {
            let majority = (replicas / 2) + 1;
            if quorum < majority {
                anyhow::bail!(
                    "ReplicatedMergeTree table '{}' has insert_quorum={} but needs ≥ {} \
                     (majority of {} replicas). ALTER TABLE ... MODIFY SETTING insert_quorum = {} \
                     OR use a non-Replicated table.",
                    table, quorum, majority, replicas, majority,
                );
            }
            if replicas < quorum {
                anyhow::bail!(
                    "ReplicatedMergeTree table '{}' has {} replicas < insert_quorum={}. \
                     Every INSERT would timeout. Reduce insert_quorum or add replicas.",
                    table, replicas, quorum,
                );
            }
        }
        if engine.contains("Distributed") {
            tracing::warn!("Table '{}' uses Distributed engine — async forwarding breaks waterline. \
                           Write directly to the underlying MergeTree table. Degrading to AT_LEAST_ONCE.", table);
        }
        if engine.contains("Replacing") || engine.contains("Collapsing") || engine.contains("Summing")
            || engine.contains("Aggregating") || engine.contains("Versioned") {
            anyhow::bail!(
                "Table '{}' uses unsupported engine '{}'. Only MergeTree and ReplicatedMergeTree \
                 are supported for exactly-once.", table, engine,
            );
        }
    }

    // 7. Graceful shutdown
    let cancel_token = CancellationToken::new();
    let ct = cancel_token.clone();
    tokio::spawn(async move { signal::ctrl_c().await.ok(); ct.cancel(); });

    // 8. Spawn tasks
    let deps = PipelineDeps {
        parser, mw: middlewares, snk: sink,
        batch_size: config.sink_batch_size,
        max_linger_ms: config.sink_max_linger_ms,
        token: cancel_token.clone(),
    };
    let source_provider = Arc::new(source_provider);
    let mut handles = Vec::new();

    for pid in my_partitions {
        let sp = source_provider.clone();
        let d = deps.clone();
        let label = source_kind.clone();
        handles.push(spawn_partition_task(pid, d, label, move || {
            let sp = sp.clone();
            async move { sp.build_source(pid, CancellationToken::new()).await }
        }));
    }

    for h in handles { let _ = h.await; }
    tracing::info!("All partition tasks completed. Exiting.");
    Ok(())
}
