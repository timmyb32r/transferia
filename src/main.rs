extern crate alloc;

use alloc::sync::Arc;

use clap::Parser;
use mimalloc::MiMalloc;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use transferia::config::yaml::Config;
use transferia::middleware::filter::FilterMiddleware;
use transferia::metrics::{spawn_stats_reporter, MetricsRegistry, ParseCounters};
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
    parser: Option<Arc<dyn transferia::parsers::Parser>>,
    table: Arc<str>,
    mw: Arc<Vec<Box<dyn Middleware>>>,
    snk: Arc<dyn transferia::pipeline::sink::Sink>,
    batch_size: usize,
    token: CancellationToken,
    has_parser: bool,
}

fn spawn_partition_task<F, Fut>(
    partition_id: i64,
    deps: PipelineDeps,
    source_label: String,
    make_source: F,
    parse_counters: Arc<ParseCounters>,
) -> tokio::task::JoinHandle<()>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: core::future::Future<Output = anyhow::Result<Box<dyn Source>>> + Send,
{
    let max_retries: u32 = 5;
    tokio::spawn(async move {
        let mut retry_count: u32 = 0;
        let mut make_source_fn = make_source;
        loop {
            if deps.token.is_cancelled() { return; }
            let source = match make_source_fn().await {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("{} source creation for partition {}: {}", &source_label, partition_id, e);
                    break;
                }
            };
            match run_partition_pipeline(
                source, deps.parser.clone(), Arc::clone(&deps.table),
                Arc::clone(&deps.mw), Arc::clone(&deps.snk),
                deps.batch_size, deps.token.clone(), partition_id,
                Arc::clone(&parse_counters), deps.has_parser,
            ).await {
                Ok(()) => break,
                Err(e) => {
                    retry_count = retry_count.saturating_add(1);
                    tracing::error!("Partition {} ({}) fatal error (retry {}/{}): {}",
                        partition_id, source_label, retry_count, max_retries, e);
                    if retry_count >= max_retries {
                        tracing::error!("Partition {} ({}) exhausted retries.", partition_id, source_label);
                        break;
                    }
                    tokio::time::sleep(core::time::Duration::from_secs(5)).await;
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
    // Per-partition metrics registry (always created; the reporter is spawned
    // only when `metrics.enabled`). YDS sources register source counters here.
    let metrics_registry = Arc::new(MetricsRegistry::new());

    // Register source providers
    registry.register_source("topic", {
        let mr = Arc::clone(&metrics_registry);
        move |v| Ok(Box::new(transferia::providers::yds::provider::YdsSourceProvider::from_config(
            v, "topic", Arc::clone(&mr),
        )?))
    });
    registry.register_source("pqv1", {
        let mr = Arc::clone(&metrics_registry);
        move |v| Ok(Box::new(transferia::providers::yds::provider::YdsSourceProvider::from_config(
            v, "pqv1", Arc::clone(&mr),
        )?))
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

    // 3. Build parser (only for sources that use a parser)
    let table: Arc<str> = source_provider.resolve_table_name()?.into();
    let mut has_parser = false;
    let parser: Option<Arc<dyn transferia::parsers::Parser>> =
        if let Some(pc) = source_provider.parser_config() {
            let parser_kind = pc.parser.kind()?.to_string();
            let parser_raw = pc.parser.raw()?.clone();
            let p = transferia::parsers::build_parser(
                &parser_kind, parser_raw, Arc::clone(&table), None,
            )?;
            // "none" parser ⇒ no-parser mode (drop at parse stage, measure
            // download-only throughput). `has_parser` drives DDL skip and the
            // stats reporter's "(no parser)" label.
            has_parser = parser_kind != "none";
            Some(p)
        } else {
            None
        };

    // 4. Discover partitions
    let my_partitions = source_provider.discover_partitions(cli.total_workers, cli.worker_index).await?;
    if my_partitions.is_empty() {
        tracing::warn!("No partitions assigned. Exiting.");
        return Ok(());
    }
    tracing::info!("Destination table: {}, partitions: {:?}", table, my_partitions);

    // 5. Middlewares
    let middlewares: Arc<Vec<Box<dyn Middleware>>> = Arc::new(
        config.middlewares.iter()
            .map(|mc| match mc.mw_type.as_str() {
                "filter" => Ok(Box::new(FilterMiddleware::new(
                    mc.field.clone().unwrap_or_default(),
                    mc.value.clone().unwrap_or_default(),
                )?) as Box<dyn Middleware>),
                other => anyhow::bail!("Unknown middleware type: {other}"),
            })
            .collect::<anyhow::Result<_>>()?,
    );

    // 6. Sink + startup checks + DDL
    let sink = sink_provider.build_sink().await?;
    let dlq_table = dlq_name(&table);

    // 6a. Exactly-once startup checks (spec §8)
    if let Some(ch_sink) = sink.as_any().downcast_ref::<ClickHouseSink>() {
        // CH version ≥ 22.8
        ch_sink.check_ch_version().await?;
        tracing::info!("ClickHouse version check passed (\u{2265} 22.8)");
    }

    // 6b. DDL: create tables (ClickHouse only — other sinks don't need DDL).
    // Skipped in no-parser mode: the "none" parser yields no columns and nothing
    // is written, so the destination table is irrelevant.
    if has_parser {
        if let Some(ch_sink) = sink.as_any().downcast_ref::<ClickHouseSink>() {
            let schema_for_ddl = source_provider.schema_config()
                .cloned()
                .unwrap_or_default();
            let cols = ClickHouseSink::schema_columns(&schema_for_ddl.column_defs())?;
            ch_sink.create_table(&table, &cols, &schema_for_ddl.order_by, config.recreate_tables).await?;

            let dlq_cols: Vec<(String, String)> =
                transferia::parsers::json_parser::dlq_ch_columns(None)
                    .iter().map(|(n, t)| ((*n).to_string(), (*t).to_string())).collect();
            ch_sink.create_table(&dlq_table, &dlq_cols, &[], config.recreate_tables).await?;
        }
    }

    // 6c. Engine/replica validation (after CREATE so table exists)
    if has_parser {
        if let Some(ch_sink) = sink.as_any().downcast_ref::<ClickHouseSink>() {
        let (engine, quorum, replicas) = ch_sink.check_table_engine(&table).await?;
        tracing::info!("Table '{}' engine: {} (quorum={}, replicas={})", table, engine, quorum, replicas);

        if engine.contains("Replicated") {
            let majority = (replicas / 2) + 1;
            if quorum < majority {
                anyhow::bail!(
                    "ReplicatedMergeTree table '{table}' has insert_quorum={quorum} but needs ≥ {majority} \
                     (majority of {replicas} replicas). ALTER TABLE ... MODIFY SETTING insert_quorum = {majority} \
                     OR use a non-Replicated table.",
                );
            }
            if replicas < quorum {
                anyhow::bail!(
                    "ReplicatedMergeTree table '{table}' has {replicas} replicas < insert_quorum={quorum}. \
                     Every INSERT would timeout. Reduce insert_quorum or add replicas.",
                );
            }
        }
        if engine.contains("Distributed") {
            tracing::warn!("Table '{}' uses Distributed engine \u{2014} async forwarding breaks waterline. \
                           Write directly to the underlying MergeTree table. Degrading to AT_LEAST_ONCE.", table);
        }
        if engine.contains("Replacing") || engine.contains("Collapsing") || engine.contains("Summing")
            || engine.contains("Aggregating") || engine.contains("Versioned") {
            anyhow::bail!(
                "Table '{table}' uses unsupported engine '{engine}'. Only MergeTree and ReplicatedMergeTree \
                 are supported for exactly-once.",
            );
        }
    }
    }

    // 7. Graceful shutdown
    let cancel_token = CancellationToken::new();
    let ct = cancel_token.clone();
    tokio::spawn(async move {
        if let Err(e) = signal::ctrl_c().await {
            tracing::error!("Failed to await ctrl-c signal: {e}");
        }
        ct.cancel();
    });

    // 8. Spawn tasks
    let deps = PipelineDeps {
        parser, table: Arc::clone(&table), mw: middlewares, snk: sink,
        batch_size: config.sink_batch_size,
        token: cancel_token.clone(),
        has_parser,
    };
    let source_provider_arc = Arc::new(source_provider);
    let mut handles = Vec::new();

    // Stats reporter (per-second throughput + duty-cycle to the console).
    if let Some(mcfg) = &config.metrics {
        if mcfg.enabled {
            spawn_stats_reporter(Arc::clone(&metrics_registry), mcfg.interval_ms, mcfg.per_partition);
            tracing::info!(
                "Metrics reporter: interval={}ms per_partition={}",
                mcfg.interval_ms, mcfg.per_partition,
            );
        }
    }

    for pid in my_partitions {
        let sp = Arc::clone(&source_provider_arc);
        let d = deps.clone();
        let label = source_kind.clone();
        // Per-partition parse counters — registered so the reporter reads them.
        let parse_counters = Arc::new(ParseCounters::new());
        metrics_registry.register_parse(pid, has_parser, Arc::clone(&parse_counters));
        handles.push(spawn_partition_task(pid, d, label, move || {
            let sp_inner = Arc::clone(&sp);
            async move { return sp_inner.build_source(pid, CancellationToken::new()).await }
        }, parse_counters));
    }

    for h in handles {
        if let Err(e) = h.await {
            tracing::error!("Partition task failed: {e}");
        }
    }
    tracing::info!("All partition tasks completed. Exiting.");
    Ok(())
}
