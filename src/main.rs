extern crate alloc;

use std::sync::Arc;

use anyhow::Context as _;
use clap::Parser;
use mimalloc::MiMalloc;
use tokio::signal;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use transferia::compatibility::{validate_pipeline, DeliveryGuarantee};
use transferia::config::yaml::Config;
use transferia::metrics::{spawn_stats_reporter, MetricsRegistry, ParseCounters, SinkCounters};
use transferia::middleware::{build_middleware, MiddlewareEntry};
use transferia::parsers::ParserConfig;
use transferia::pipeline::memory::PipelineMemory;
use transferia::pipeline::{run_partition_pipeline, PipelineFailure};
use transferia::providers::traits::{
    ProviderRegistry, SinkContext, SinkPrepare, SinkProvider, SourceProvider,
};
use transferia::types::table_data::dlq_name;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Parser, Debug)]
#[command(name = "transferia", about = "PQv1 data transfer pipeline")]
struct Cli {
    #[arg(long, env = "CONFIG_PATH")]
    config: String,
    #[arg(long, default_value_t = 1)]
    total_workers: u32,
    #[arg(long, default_value_t = 0)]
    worker_index: u32,
}

fn validate_worker_assignment(cli: &Cli) -> anyhow::Result<()> {
    anyhow::ensure!(cli.total_workers > 0, "total_workers must be positive");
    anyhow::ensure!(
        cli.worker_index < cli.total_workers,
        "worker_index must be less than total_workers"
    );
    Ok(())
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

async fn run_partition_attempt(
    partition_id: i64,
    deps: &PipelineDeps,
    parse_counters: Arc<ParseCounters>,
    sink_counters: Arc<SinkCounters>,
    attempt_token: CancellationToken,
) -> anyhow::Result<()> {
    let parser_kind = deps
        .parser_config
        .parser
        .kind()
        .map_err(|error| anyhow::Error::new(PipelineFailure::fatal(error)))?;
    let parser_raw = deps
        .parser_config
        .parser
        .raw()
        .cloned()
        .map_err(|error| anyhow::Error::new(PipelineFailure::fatal(error)))?;
    let parser = transferia::parsers::build_parser(
        parser_kind,
        parser_raw,
        Arc::clone(&deps.table),
        deps.parser_config.common.clone(),
    )
    .map_err(|error| anyhow::Error::new(PipelineFailure::fatal(error)))?;
    let middlewares = deps
        .middlewares
        .iter()
        .map(|middleware| build_middleware(middleware.kind()?, middleware.raw()?.clone()))
        .collect::<anyhow::Result<Vec<_>>>()
        .map(Arc::new)
        .map_err(|error| anyhow::Error::new(PipelineFailure::fatal(error)))?;
    let memory = PipelineMemory::new(deps.memory_limit);
    let source = deps
        .source_provider
        .build_source(partition_id, attempt_token.clone(), memory.clone())
        .await
        .context("source creation failed")?;
    let sink = deps
        .sink_provider
        .build_sink(SinkContext {
            partition_id,
            counters: sink_counters,
            keep_system_columns: deps.keep_system_columns,
        })
        .await
        .context("sink creation failed")?;
    run_partition_pipeline(
        source,
        parser,
        middlewares,
        sink,
        memory,
        attempt_token,
        partition_id,
        parse_counters,
    )
    .await
}

async fn run_partition_task(
    partition_id: i64,
    deps: PipelineDeps,
    parse_counters: Arc<ParseCounters>,
    sink_counters: Arc<SinkCounters>,
) -> anyhow::Result<()> {
    const MAX_PIPELINE_ATTEMPTS: u32 = 5;
    let mut restart_delay = core::time::Duration::from_secs(1);

    for attempt in 1..=MAX_PIPELINE_ATTEMPTS {
        if deps.cancellation.is_cancelled() {
            return Ok(());
        }
        let attempt_token = deps.cancellation.child_token();
        let result = run_partition_attempt(
            partition_id,
            &deps,
            Arc::clone(&parse_counters),
            Arc::clone(&sink_counters),
            attempt_token.clone(),
        )
        .await;
        // PQ background tasks are children of this token and must never
        // survive into the next attempt.
        attempt_token.cancel();

        let error = match result {
            Ok(()) if deps.cancellation.is_cancelled() => return Ok(()),
            Ok(()) => anyhow::anyhow!("partition pipeline stopped unexpectedly"),
            Err(error) => error,
        };
        let retryable = error
            .downcast_ref::<PipelineFailure>()
            .is_none_or(PipelineFailure::is_retryable);
        if !retryable {
            return Err(error).context("non-retryable partition failure");
        }
        if attempt == MAX_PIPELINE_ATTEMPTS {
            return Err(error).context(format!(
                "partition pipeline exhausted {MAX_PIPELINE_ATTEMPTS} attempts"
            ));
        }

        tracing::error!(
            partition = partition_id,
            attempt,
            delay_ms = restart_delay.as_millis(),
            "pipeline failed, restarting: {error}"
        );
        tokio::select! {
            () = deps.cancellation.cancelled() => return Ok(()),
            () = tokio::time::sleep(restart_delay) => {}
        }
        restart_delay = restart_delay
            .saturating_mul(2)
            .min(core::time::Duration::from_secs(30));
    }
    unreachable!("bounded partition retry loop always returns")
}

async fn stop_partition_tasks(
    tasks: &mut JoinSet<anyhow::Result<()>>,
    cancellation: &CancellationToken,
) {
    cancellation.cancel();
    let stopped = tokio::time::timeout(core::time::Duration::from_secs(10), async {
        while tasks.join_next().await.is_some() {}
    })
    .await;
    if stopped.is_err() {
        tracing::warn!("partition shutdown grace period expired; aborting remaining tasks");
        tasks.shutdown().await;
    }
}

fn build_provider_registry(metrics_registry: &Arc<MetricsRegistry>) -> ProviderRegistry {
    let mut registry = ProviderRegistry::new();
    registry.register_source("pqv1", {
        let registry = Arc::clone(metrics_registry);
        move |value| {
            Ok(Box::new(
                transferia::providers::yds::provider::YdsSourceProvider::from_config(
                    value,
                    Arc::clone(&registry),
                )?,
            ))
        }
    });
    registry.register_sink("clickhouse", |value| {
        Ok(Box::new(
            transferia::providers::clickhouse::ClickHouseSinkProvider::from_config(value)?,
        ))
    });
    registry.register_sink("empty", |value| {
        Ok(Box::new(
            transferia::providers::empty::provider::EmptySinkProvider::from_config(value)?,
        ))
    });
    registry.register_sink("s3", |value| {
        Ok(Box::new(
            transferia::providers::s3::sink::S3SinkProvider::from_config(value)?,
        ))
    });
    registry
}

fn spawn_shutdown_listener(cancellation: CancellationToken) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate = signal::unix::signal(signal::unix::SignalKind::terminate())?;
        tokio::spawn(async move {
            tokio::select! {
                _ = signal::ctrl_c() => {}
                _ = terminate.recv() => {}
            }
            cancellation.cancel();
        });
    }
    #[cfg(not(unix))]
    tokio::spawn(async move {
        if signal::ctrl_c().await.is_ok() {
            cancellation.cancel();
        }
    });
    Ok(())
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
    validate_worker_assignment(&cli)?;
    let config = Config::from_file(&cli.config)?;
    anyhow::ensure!(
        config.pipeline_memory_limit_bytes > 0,
        "pipeline_memory_limit_bytes must be positive"
    );

    let metrics_registry = Arc::new(MetricsRegistry::new());
    let registry = build_provider_registry(&metrics_registry);

    let source_kind = config.source.kind()?;
    let sink_kind = config.sink.kind()?;
    let source_provider: Arc<dyn SourceProvider> =
        Arc::from(registry.build_source(source_kind, config.source.raw()?.clone())?);
    let sink_provider: Arc<dyn SinkProvider> =
        Arc::from(registry.build_sink(sink_kind, config.sink.raw()?.clone())?);
    sink_provider.validate_pipeline_memory_limit(config.pipeline_memory_limit_bytes)?;

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
    spawn_shutdown_listener(cancellation.clone())?;

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
    let mut tasks = JoinSet::new();
    for partition_id in partitions {
        let parse_counters = Arc::new(ParseCounters::new());
        let sink_counters = Arc::new(SinkCounters::new());
        metrics_registry.register_parse(partition_id, true, Arc::clone(&parse_counters));
        metrics_registry.register_sink(partition_id, Arc::clone(&sink_counters));
        metrics_registry.set_eo_key(
            partition_id,
            semantics.guarantee == DeliveryGuarantee::ExactlyOnce,
        );
        tasks.spawn(run_partition_task(
            partition_id,
            deps.clone(),
            parse_counters,
            sink_counters,
        ));
    }
    while !tasks.is_empty() {
        let result = tokio::select! {
            () = cancellation.cancelled() => {
                stop_partition_tasks(&mut tasks, &cancellation).await;
                return Ok(());
            }
            result = tasks.join_next() => result,
        };
        let Some(result) = result else {
            break;
        };
        match result {
            Ok(Ok(())) if cancellation.is_cancelled() => {}
            Ok(Ok(())) => {
                stop_partition_tasks(&mut tasks, &cancellation).await;
                anyhow::bail!("partition task stopped while the service was still running");
            }
            Ok(Err(error)) => {
                stop_partition_tasks(&mut tasks, &cancellation).await;
                return Err(error).context("partition task failed");
            }
            Err(error) => {
                stop_partition_tasks(&mut tasks, &cancellation).await;
                return Err(anyhow::Error::new(error)).context("partition task panicked");
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_worker_assignment_before_partitioning() {
        let mut cli = Cli {
            config: "unused".into(),
            total_workers: 0,
            worker_index: 0,
        };
        assert!(validate_worker_assignment(&cli).is_err());
        cli.total_workers = 2;
        cli.worker_index = 2;
        assert!(validate_worker_assignment(&cli).is_err());
        cli.worker_index = 1;
        assert!(validate_worker_assignment(&cli).is_ok());
    }

    #[test]
    fn default_registry_builds_pqv1_to_clickhouse_pipeline() -> anyhow::Result<()> {
        let registry = build_provider_registry(&Arc::new(MetricsRegistry::new()));
        let config: Config = serde_yaml::from_str(
            r"
source:
  pqv1:
    connection_string: grpc://localhost
    topic_path: topic
    consumer_name: consumer
    partition_ids: [0]
    parser:
      common:
        table_naming: { type: from_config, name: events }
      json_parser:
        columns:
          - { jsonpath: $.id, column_name: id, arrow_type: Int64, nullable: false }
sink:
  clickhouse:
    connection_string: localhost:9000
    database: default
    use_tls: false
",
        )?;
        let source = registry.build_source(config.source.kind()?, config.source.raw()?.clone())?;
        let sink = registry.build_sink(config.sink.kind()?, config.sink.raw()?.clone())?;
        assert!(matches!(
            sink.compatibility(),
            transferia::compatibility::EndpointDescriptor::ClickHouse
        ));
        validate_pipeline(&source.compatibility(), &sink.compatibility(), false).ensure_valid()?;
        Ok(())
    }

    #[test]
    fn every_benchmark_config_matches_registered_provider_shapes() -> anyhow::Result<()> {
        let registry = build_provider_registry(&Arc::new(MetricsRegistry::new()));
        for relative_path in [
            "benchmarks/config_bench_yds_json_parser.yaml",
            "benchmarks/config_bench_yds_no_parser.yaml",
            "benchmarks/config_bench_yds_no_parser_and_decompress.yaml",
            "benchmarks/config_bench_yds_json_parser_to_ch.yaml",
            "benchmarks/config_bench_yds_json_parser_to_s3.yaml",
        ] {
            let path = format!("{}/{relative_path}", env!("CARGO_MANIFEST_DIR"));
            let config = Config::from_file(&path)
                .with_context(|| format!("failed to load {relative_path}"))?;
            let source = registry
                .build_source(config.source.kind()?, config.source.raw()?.clone())
                .with_context(|| format!("invalid source in {relative_path}"))?;
            let sink = registry
                .build_sink(config.sink.kind()?, config.sink.raw()?.clone())
                .with_context(|| format!("invalid sink in {relative_path}"))?;
            sink.validate_pipeline_memory_limit(config.pipeline_memory_limit_bytes)
                .with_context(|| format!("invalid memory limits in {relative_path}"))?;
            validate_pipeline(
                &source.compatibility(),
                &sink.compatibility(),
                config.keep_system_columns_in_sink,
            )
            .ensure_valid()
            .with_context(|| format!("incompatible providers in {relative_path}"))?;
        }
        Ok(())
    }

    #[test]
    fn root_example_config_matches_registered_provider_shapes() -> anyhow::Result<()> {
        let raw = std::fs::read_to_string(format!("{}/config.yaml", env!("CARGO_MANIFEST_DIR")))?
            .replace("${HOME}", "/tmp")
            .replace("${S3_ACCESS_KEY}", "test-access-key")
            .replace("${S3_SECRET_KEY}", "test-secret-key");
        let config: Config = serde_yaml::from_str(&raw)?;
        let registry = build_provider_registry(&Arc::new(MetricsRegistry::new()));
        let source = registry.build_source(config.source.kind()?, config.source.raw()?.clone())?;
        let sink = registry.build_sink(config.sink.kind()?, config.sink.raw()?.clone())?;
        sink.validate_pipeline_memory_limit(config.pipeline_memory_limit_bytes)?;
        validate_pipeline(
            &source.compatibility(),
            &sink.compatibility(),
            config.keep_system_columns_in_sink,
        )
        .ensure_valid()?;
        Ok(())
    }
}
