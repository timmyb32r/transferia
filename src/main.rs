use std::sync::Arc;

use clap::Parser;
use mimalloc::MiMalloc;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use ch_loader::config::yaml::{validate_parser, Config};
use ch_loader::middleware::filter::FilterMiddleware;
use ch_loader::parser::JsonParser;
use ch_loader::types::table_data::dlq_name;
use ch_loader::pipeline::middleware::Middleware;
use ch_loader::pipeline::run_partition_pipeline;
use ch_loader::pipeline::source::Source;
use ch_loader::providers::traits::ProviderRegistry;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Parser, Debug)]
#[command(name = "ch-loader", about = "Multi-source ClickHouse loader")]
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
    snk: Arc<dyn ch_loader::pipeline::sink::Sink>,
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
    tracing::info!("ch-loader starting (worker {}/{})", cli.worker_index, cli.total_workers);

    // 1. Load config
    let config = Config::from_file(&cli.config)?;

    // 2. Build registry + providers
    let mut registry = ProviderRegistry::new();

    // Register source providers
    registry.register_source("topic", |v| {
        Ok(Box::new(ch_loader::providers::yds::provider::YdsSourceProvider::from_config(v, "topic")?))
    });
    registry.register_source("pqv1", |v| {
        Ok(Box::new(ch_loader::providers::yds::provider::YdsSourceProvider::from_config(v, "pqv1")?))
    });
    registry.register_source("s3", |v| {
        Ok(Box::new(ch_loader::providers::s3::provider::S3SourceProvider::from_config(v)?))
    });

    // Register sink providers
    registry.register_sink("clickhouse", |v| {
        Ok(Box::new(ch_loader::providers::clickhouse::provider::ClickHouseSinkProvider::from_config(v)?))
    });
    registry.register_sink("empty", |v| {
        Ok(Box::new(ch_loader::providers::empty::provider::EmptySinkProvider::from_config(v)?))
    });

    let source_kind = config.source.kind()?.to_string();
    let source_provider = registry.build_source(&source_kind, config.source.raw()?.clone())?;
    let sink_kind = config.sink.kind()?.to_string();
    let sink_provider = registry.build_sink(&sink_kind, config.sink.raw()?.clone())?;

    tracing::info!("Source: {}, Sink: {}", source_kind, sink_kind);

    // 3. Common parser validation
    validate_parser(source_provider.parser_config(), &config.middlewares)?;

    // 4. Resolve table + partitions
    let table: Arc<str> = source_provider.resolve_table_name()?.into();
    let my_partitions = source_provider.discover_partitions(cli.total_workers, cli.worker_index).await?;
    if my_partitions.is_empty() {
        tracing::warn!("No partitions assigned. Exiting.");
        return Ok(());
    }
    tracing::info!("Destination table: {}, partitions: {:?}", table, my_partitions);

    // 5. Shared parser + middlewares
    let parser = Arc::new(JsonParser::new(&source_provider.parser_config().settings, table.clone())?);
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

    // 6. Sink + DDL
    let sink = sink_provider.build_sink().await?;
    let dlq_table = dlq_name(&table);
    sink_provider.create_tables(&table, &dlq_table,
        &source_provider.parser_config().settings, config.recreate_tables).await?;
    sink_provider.verify_tables(&table, &dlq_table).await?;

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
