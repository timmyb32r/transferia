use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use futures_util::future::BoxFuture;
use futures_util::TryStreamExt as _;
use object_store::path::Path;
use serde_yaml::Value;
use tokio_util::sync::CancellationToken;

use super::config::S3SourceConfig;
use super::reader::S3Source;
use crate::compatibility::{
    EndpointDescriptor, SourceBehavior, SourceDeliveryModes, SourceDescriptor,
};
use crate::delivery::{DeliveryDiscovery, DeliveryDiscoveryRequest};
use crate::metrics::{MetricsRegistry, SourceCounters};
use crate::parsers::ParserPlan;
use crate::pipeline::memory::PipelineMemory;
use crate::pipeline::source::Source;
use crate::providers::traits::SourceProvider;

pub struct S3SourceProvider {
    config: S3SourceConfig,
    store: Arc<dyn object_store::ObjectStore>,
    parser_plan: ParserPlan,
    metrics: Arc<MetricsRegistry>,
    snapshot: tokio::sync::OnceCell<Arc<Vec<Path>>>,
    counters: Mutex<HashMap<i64, Arc<SourceCounters>>>,
}

impl S3SourceProvider {
    pub fn from_config(value: Value, metrics: Arc<MetricsRegistry>) -> anyhow::Result<Self> {
        let config: S3SourceConfig = serde_yaml::from_value(value)
            .map_err(|error| anyhow::anyhow!("Failed to parse S3 source config: {error}"))?;
        config.validate()?;
        let parser_plan = ParserPlan::from_config(&config.parser, &config.prefix)?;
        let store = config.build_store()?;
        Ok(Self {
            config,
            store,
            parser_plan,
            metrics,
            snapshot: tokio::sync::OnceCell::new(),
            counters: Mutex::new(HashMap::new()),
        })
    }

    async fn snapshot(&self, cancellation: &CancellationToken) -> anyhow::Result<Arc<Vec<Path>>> {
        self.snapshot.get_or_try_init(|| async {
            let prefix = if self.config.prefix.is_empty() { None } else { Some(Path::parse(&self.config.prefix)?) };
            let listed = tokio::select! { biased; () = cancellation.cancelled() => anyhow::bail!("S3 listing cancelled"), result = tokio::time::timeout(self.config.timeout(), self.store.list(prefix.as_ref()).try_collect::<Vec<_>>()) => result.map_err(|_| anyhow::anyhow!("S3 listing timed out"))?? };
            let mut keys = listed.into_iter().filter(|object| object.size > 0).map(|object| object.location).collect::<Vec<_>>();
            keys.sort();
            let mut unique = HashSet::with_capacity(keys.len());
            for key in &keys { anyhow::ensure!(unique.insert(key.as_ref()), "S3 listing returned duplicate key '{key}'"); }
            anyhow::ensure!(!keys.is_empty(), "S3 prefix '{}' contains no non-empty objects", self.config.prefix);
            Ok(Arc::new(keys))
        }).await.map(Arc::clone)
    }

    fn counters(&self, partition: i64) -> Arc<SourceCounters> {
        Arc::clone(
            self.counters
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(partition)
                .or_insert_with(|| Arc::new(SourceCounters::new())),
        )
    }
}

impl SourceProvider for S3SourceProvider {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::S3Source(SourceDescriptor {
            behavior: SourceBehavior::FiniteSnapshotRows,
            delivery_modes: SourceDeliveryModes::BATCH,
        })
    }
    fn delivery_discovery(
        &self,
        request: DeliveryDiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, anyhow::Result<DeliveryDiscovery>> {
        Box::pin(async move {
            drop(self.snapshot(&cancellation).await?);
            DeliveryDiscovery::parser_projection(
                Arc::from(self.config.prefix.as_str()),
                vec![0],
                &self.parser_plan,
                request,
            )
        })
    }
    fn build_source(
        &self,
        partition_id: i64,
        cancellation: CancellationToken,
        memory: PipelineMemory,
        _durable: crate::durable::DurableContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>> {
        Box::pin(async move {
            anyhow::ensure!(partition_id == 0, "S3 source has only partition 0");
            let keys = self.snapshot(&cancellation).await?;
            let counters = self.counters(partition_id);
            self.metrics
                .register_source(partition_id, Arc::clone(&counters));
            Ok(Box::new(S3Source::new(
                Arc::clone(&self.store),
                keys,
                self.config.timeout(),
                cancellation,
                memory,
                counters,
            )) as Box<dyn Source>)
        })
    }
    fn partitions_for_worker(
        &self,
        total_workers: u32,
        worker_index: u32,
    ) -> BoxFuture<'_, anyhow::Result<Vec<i64>>> {
        Box::pin(async move {
            anyhow::ensure!(
                total_workers > 0 && worker_index < total_workers,
                "invalid worker assignment"
            );
            Ok(if worker_index == 0 {
                vec![0]
            } else {
                Vec::new()
            })
        })
    }
    fn parser_plan(&self) -> &ParserPlan {
        &self.parser_plan
    }
}
