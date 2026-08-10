extern crate alloc;

use std::sync::Arc;

use clap::Parser;
use mimalloc::MiMalloc;
use tokio::signal;
use tokio_util::sync::CancellationToken;

use transferia::config::yaml::Config;
use transferia::metrics::{spawn_stats_reporter, MetricsRegistry, ParseCounters, SinkCounters};
use transferia::middleware::filter::FilterMiddleware;
use transferia::pipeline::memory::PipelineMemory;
use transferia::pipeline::middleware::Middleware;
use transferia::pipeline::run_partition_pipeline;
use transferia::providers::clickhouse::sink::schema_columns;
use transferia::providers::traits::{
    ProviderRegistry, SinkContext, SinkPrepare, SinkProvider, SourceProvider,
};
use transferia::types::table_data::dlq_name;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Parser, Debug)]
#[command(name = "transferia", about = "PQv1 to ClickHouse data pipeline")]
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
    parser: Arc<dyn transferia::parsers::Parser>,
    middlewares: Arc<Vec<Box<dyn Middleware>>>,
    source_provider: Arc<dyn SourceProvider>,
    sink_provider: Arc<dyn SinkProvider>,
    memory_limit: usize,
    cancellation: CancellationToken,
}

fn spawn_partition_task(
    partition_id: i64,
    deps: PipelineDeps,
    parse_counters: Arc<ParseCounters>,
    sink_counters: Arc<SinkCounters>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        const MAX_PIPELINE_RESTARTS: u32 = 5;
        for attempt in 1..=MAX_PIPELINE_RESTARTS {
            if deps.cancellation.is_cancelled() {
                return;
            }
            let memory = PipelineMemory::new(deps.memory_limit);
            let source = match deps
                .source_provider
                .build_source(partition_id, deps.cancellation.clone(), memory.clone())
                .await
            {
                Ok(source) => source,
                Err(error) => {
                    tracing::error!(
                        partition = partition_id,
                        attempt,
                        "PQv1 source creation failed: {error}"
                    );
                    break;
                }
            };
            let sink = match deps
                .sink_provider
                .build_sink(SinkContext {
                    partition_id,
                    counters: Arc::clone(&sink_counters),
                })
                .await
            {
                Ok(sink) => sink,
                Err(error) => {
                    tracing::error!(
                        partition = partition_id,
                        attempt,
                        "ClickHouse sink creation failed: {error}"
                    );
                    break;
                }
            };
            let result = run_partition_pipeline(
                source,
                Arc::clone(&deps.parser),
                Arc::clone(&deps.middlewares),
                sink,
                memory,
                deps.cancellation.clone(),
                partition_id,
                Arc::clone(&parse_counters),
            )
            .await;
            match result {
                Ok(()) => return,
                Err(error) if attempt < MAX_PIPELINE_RESTARTS => {
                    tracing::error!(
                        partition = partition_id,
                        attempt,
                        "pipeline failed, restarting: {error}"
                    );
                    tokio::time::sleep(core::time::Duration::from_secs(5)).await;
                }
                Err(error) => {
                    tracing::error!(
                        partition = partition_id,
                        "pipeline exhausted restarts: {error}"
                    );
                    return;
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
    let config = Config::from_file(&cli.config)?;
    anyhow::ensure!(
        config.pipeline_memory_limit_bytes > 0,
        "pipeline_memory_limit_bytes must be positive"
    );

    let metrics_registry = Arc::new(MetricsRegistry::new());
    let mut registry = ProviderRegistry::new();
    registry.register_source("pqv1", {
        let registry = Arc::clone(&metrics_registry);
        move |value| {
            Ok(Box::new(
                transferia::providers::yds::provider::YdsSourceProvider::from_config(
                    value,
                    "pqv1",
                    Arc::clone(&registry),
                )?,
            ))
        }
    });
    registry.register_sink("clickhouse", |value| {
        Ok(Box::new(
            transferia::providers::clickhouse::provider::ClickHouseSinkProvider::from_config(
                value,
            )?,
        ))
    });

    let source_kind = config.source.kind()?;
    let sink_kind = config.sink.kind()?;
    anyhow::ensure!(
        source_kind == "pqv1",
        "only source.pqv1 is supported in this build"
    );
    anyhow::ensure!(
        sink_kind == "clickhouse",
        "only sink.clickhouse is supported in this build"
    );
    let source_provider: Arc<dyn SourceProvider> =
        Arc::from(registry.build_source(source_kind, config.source.raw()?.clone())?);
    let sink_provider: Arc<dyn SinkProvider> =
        Arc::from(registry.build_sink(sink_kind, config.sink.raw()?.clone())?);

    let table: Arc<str> = source_provider.resolve_table_name()?.into();
    let parser_config = source_provider
        .parser_config()
        .ok_or_else(|| anyhow::anyhow!("PQv1 source requires a JSON parser"))?;
    let parser_kind = parser_config.parser.kind()?;
    anyhow::ensure!(
        parser_kind == "json_parser",
        "only json_parser is supported in this build"
    );
    let parser_raw = parser_config.parser.raw()?.clone();
    anyhow::ensure!(
        !parser_raw
            .get("add_exactly_once_keys")
            .and_then(serde_yaml::Value::as_bool)
            .unwrap_or(false),
        "exactly-once is not implemented by the asynchronous ClickHouse sink",
    );
    let parser =
        transferia::parsers::build_parser(parser_kind, parser_raw, Arc::clone(&table), None)?;

    let middlewares: Arc<Vec<Box<dyn Middleware>>> = Arc::new(
        config
            .middlewares
            .iter()
            .map(|middleware| match middleware.mw_type.as_str() {
                "filter" => Ok(Box::new(FilterMiddleware::new(
                    middleware.field.clone().unwrap_or_default(),
                    middleware.value.clone().unwrap_or_default(),
                )?) as Box<dyn Middleware>),
                other => anyhow::bail!("Unknown middleware type: {other}"),
            })
            .collect::<anyhow::Result<_>>()?,
    );

    let partitions = source_provider
        .discover_partitions(cli.total_workers, cli.worker_index)
        .await?;
    if partitions.is_empty() {
        tracing::warn!("No PQv1 partitions assigned");
        return Ok(());
    }

    let schema = source_provider.schema_config().cloned().unwrap_or_default();
    let dlq_table: Arc<str> = dlq_name(&table).into();
    let dlq_columns = transferia::parsers::json_parser::dlq_ch_columns(None)
        .iter()
        .map(|(name, ty)| ((*name).to_string(), (*ty).to_string()))
        .collect();
    sink_provider
        .prepare(SinkPrepare {
            table: Arc::clone(&table),
            columns: schema_columns(&schema.column_defs())?,
            order_by: schema.order_by.clone(),
            dlq_table,
            dlq_columns,
            recreate: config.recreate_tables,
        })
        .await?;

    let cancellation = CancellationToken::new();
    let shutdown = cancellation.clone();
    tokio::spawn(async move {
        if signal::ctrl_c().await.is_ok() {
            shutdown.cancel();
        }
    });

    if let Some(metrics) = &config.metrics {
        if metrics.enabled {
            spawn_stats_reporter(
                Arc::clone(&metrics_registry),
                metrics.interval_ms,
                metrics.per_partition,
            );
        }
    }

    let deps = PipelineDeps {
        parser,
        middlewares,
        source_provider,
        sink_provider,
        memory_limit: config.pipeline_memory_limit_bytes,
        cancellation: cancellation.clone(),
    };
    let mut tasks = Vec::with_capacity(partitions.len());
    for partition_id in partitions {
        let parse_counters = Arc::new(ParseCounters::new());
        let sink_counters = Arc::new(SinkCounters::new());
        metrics_registry.register_parse(partition_id, true, Arc::clone(&parse_counters));
        metrics_registry.register_sink(partition_id, Arc::clone(&sink_counters));
        metrics_registry.set_eo_key(partition_id, false);
        tasks.push(spawn_partition_task(
            partition_id,
            deps.clone(),
            parse_counters,
            sink_counters,
        ));
    }
    for task in tasks {
        drop(task.await);
    }
    Ok(())
}
