extern crate alloc;

use std::sync::Arc;

use clap::Parser;
use mimalloc::MiMalloc;
use tokio::signal;
use tokio_util::sync::CancellationToken;

use transferia::compatibility::{validate_pipeline, DeliveryGuarantee};
use transferia::config::yaml::Config;
use transferia::metrics::{spawn_stats_reporter, MetricsRegistry, ParseCounters, SinkCounters};
use transferia::middleware::{build_middleware, MiddlewareEntry};
use transferia::parsers::ParserConfig;
use transferia::pipeline::memory::PipelineMemory;
use transferia::pipeline::run_partition_pipeline;
use transferia::providers::traits::{
    ProviderRegistry, SinkContext, SinkPrepare, SinkProvider, SourceProvider,
};
use transferia::types::table_data::dlq_name;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Parser, Debug)]
#[command(name = "transferia", about = "PQv1 to S3 data pipeline")]
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
    parser_config: Arc<ParserConfig>,
    table: Arc<str>,
    middlewares: Arc<Vec<MiddlewareEntry>>,
    source_provider: Arc<dyn SourceProvider>,
    sink_provider: Arc<dyn SinkProvider>,
    memory_limit: usize,
    cancellation: CancellationToken,
    keep_system_columns: bool,
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
            let parser_kind = match deps.parser_config.parser.kind() {
                Ok(kind) => kind,
                Err(error) => {
                    tracing::error!(partition = partition_id, "invalid parser config: {error}");
                    return;
                }
            };
            let parser_raw = match deps.parser_config.parser.raw() {
                Ok(raw) => raw.clone(),
                Err(error) => {
                    tracing::error!(partition = partition_id, "invalid parser config: {error}");
                    return;
                }
            };
            let parser = match transferia::parsers::build_parser(
                parser_kind,
                parser_raw,
                Arc::clone(&deps.table),
                deps.parser_config.common.clone(),
            ) {
                Ok(parser) => parser,
                Err(error) => {
                    tracing::error!(partition = partition_id, "parser creation failed: {error}");
                    return;
                }
            };
            let middlewares = match deps
                .middlewares
                .iter()
                .map(|middleware| build_middleware(middleware.kind()?, middleware.raw()?.clone()))
                .collect::<anyhow::Result<Vec<_>>>()
            {
                Ok(middlewares) => Arc::new(middlewares),
                Err(error) => {
                    tracing::error!(
                        partition = partition_id,
                        "middleware creation failed: {error}"
                    );
                    return;
                }
            };
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
                        "source creation failed: {error}"
                    );
                    break;
                }
            };
            let sink = match deps
                .sink_provider
                .build_sink(SinkContext {
                    partition_id,
                    counters: Arc::clone(&sink_counters),
                    keep_system_columns: deps.keep_system_columns,
                })
                .await
            {
                Ok(sink) => sink,
                Err(error) => {
                    tracing::error!(
                        partition = partition_id,
                        attempt,
                        "sink creation failed: {error}"
                    );
                    break;
                }
            };
            let result = run_partition_pipeline(
                source,
                parser,
                middlewares,
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
                    Arc::clone(&registry),
                )?,
            ))
        }
    });
    registry.register_sink("s3", |value| {
        Ok(Box::new(
            transferia::providers::s3::sink::S3SinkProvider::from_config(value)?,
        ))
    });

    let source_kind = config.source.kind()?;
    let sink_kind = config.sink.kind()?;
    let source_provider: Arc<dyn SourceProvider> =
        Arc::from(registry.build_source(source_kind, config.source.raw()?.clone())?);
    let sink_provider: Arc<dyn SinkProvider> =
        Arc::from(registry.build_sink(sink_kind, config.sink.raw()?.clone())?);

    let semantics = validate_pipeline(
        &source_provider.compatibility(),
        &sink_provider.compatibility(),
        config.keep_system_columns_in_sink,
    );
    semantics.ensure_valid()?;
    tracing::info!(report = %serde_json::to_string(&semantics)?, "delivery semantics inferred from configuration");

    let table: Arc<str> = source_provider.resolve_table_name()?.into();
    let parser_config = source_provider
        .parser_config()
        .ok_or_else(|| anyhow::anyhow!("source requires a parser"))?;
    parser_config.parser.kind()?;
    let parser_config = Arc::new(parser_config.clone());

    let partitions = source_provider
        .discover_partitions(cli.total_workers, cli.worker_index)
        .await?;
    if partitions.is_empty() {
        tracing::warn!("No source partitions assigned");
        return Ok(());
    }

    let schema = source_provider.schema().cloned().unwrap_or_default();
    let dlq_table: Arc<str> = dlq_name(&table).into();
    sink_provider
        .prepare(SinkPrepare {
            table: Arc::clone(&table),
            schema,
            dlq_table,
            dlq_schema: transferia::parsers::json_parser::dlq_dataset_schema(
                &parser_config.common.system_columns,
            ),
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
        parser_config,
        table,
        middlewares: Arc::new(config.middlewares.clone()),
        source_provider,
        sink_provider,
        memory_limit: config.pipeline_memory_limit_bytes,
        cancellation: cancellation.clone(),
        keep_system_columns: config.keep_system_columns_in_sink,
    };
    let mut tasks = Vec::with_capacity(partitions.len());
    for partition_id in partitions {
        let parse_counters = Arc::new(ParseCounters::new());
        let sink_counters = Arc::new(SinkCounters::new());
        metrics_registry.register_parse(partition_id, true, Arc::clone(&parse_counters));
        metrics_registry.register_sink(partition_id, Arc::clone(&sink_counters));
        metrics_registry.set_eo_key(
            partition_id,
            semantics.guarantee == DeliveryGuarantee::ExactlyOnce,
        );
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
