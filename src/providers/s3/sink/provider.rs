use std::sync::Arc;

use futures_util::future::BoxFuture;
use serde_yaml::Value;

use crate::pipeline::sink::Sink;
use crate::providers::traits::{SinkContext, SinkPrepare, SinkProvider};

use super::actor::S3Sink;
use super::config::S3SinkConfig;
use super::upload::{ObjectUploader, S3Uploader};
use crate::compatibility::{EndpointDescriptor, S3Descriptor, S3Partitioning};

pub struct S3SinkProvider {
    cfg: S3SinkConfig,
    uploader: Arc<dyn ObjectUploader>,
}

impl S3SinkProvider {
    pub fn from_config(value: Value) -> anyhow::Result<Self> {
        let cfg: S3SinkConfig = serde_yaml::from_value(value)
            .map_err(|e| anyhow::anyhow!("Failed to parse S3 sink config: {e}"))?;
        cfg.validate()?;
        let uploader = Arc::new(S3Uploader::new(cfg.build_store()?, cfg.upload.clone()));
        Ok(Self { cfg, uploader })
    }
}

impl SinkProvider for S3SinkProvider {
    fn compatibility(&self) -> EndpointDescriptor {
        let partitioning = match &self.cfg.partitioning {
            super::config::PartitioningConfig::Source => S3Partitioning::Source,
            super::config::PartitioningConfig::Fields { columns } => {
                S3Partitioning::Fields(columns.clone())
            }
            super::config::PartitioningConfig::RecordTime { .. } => S3Partitioning::RecordTime,
        };
        EndpointDescriptor::S3(S3Descriptor {
            partitioning,
            record_time_rotation: self.cfg.rotation.record_time_interval.is_some(),
            wall_clock_rotation: self.cfg.rotation.wall_clock_interval.is_some(),
        })
    }

    fn prepare(&self, _request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn validate_pipeline_memory_limit(&self, limit_bytes: usize) -> anyhow::Result<()> {
        let epoch_bytes = self.cfg.epoch_byte_limit();
        anyhow::ensure!(
            epoch_bytes <= limit_bytes,
            "effective s3.buffering.max_epoch_bytes ({epoch_bytes}) must not exceed \
             pipeline_memory_limit_bytes ({limit_bytes}); lower the S3 epoch limit or raise \
             the pipeline memory limit"
        );
        Ok(())
    }

    fn build_sink(&self, context: SinkContext) -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>> {
        let sink = S3Sink::new(
            self.cfg.clone(),
            Arc::clone(&self.uploader),
            context.counters,
            context.keep_system_columns,
        );
        Box::pin(async move { Ok(Box::new(sink?) as Box<dyn Sink>) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_must_fit_pipeline_memory_to_guarantee_progress() -> anyhow::Result<()> {
        let provider = S3SinkProvider::from_config(serde_yaml::from_str(
            "bucket: test\nbuffering: { max_buffered_bytes: 64, max_epoch_bytes: 48 }\n",
        )?)?;

        assert!(provider.validate_pipeline_memory_limit(47).is_err());
        provider.validate_pipeline_memory_limit(48)?;
        Ok(())
    }

    #[tokio::test]
    async fn partition_sinks_share_one_uploader() -> anyhow::Result<()> {
        let provider = S3SinkProvider::from_config(serde_yaml::from_str("bucket: test\n")?)?;
        assert_eq!(Arc::strong_count(&provider.uploader), 1);

        let first = provider
            .build_sink(SinkContext {
                partition_id: 1,
                counters: Arc::new(crate::metrics::SinkCounters::new()),
                keep_system_columns: false,
            })
            .await?;
        let second = provider
            .build_sink(SinkContext {
                partition_id: 2,
                counters: Arc::new(crate::metrics::SinkCounters::new()),
                keep_system_columns: false,
            })
            .await?;

        assert_eq!(Arc::strong_count(&provider.uploader), 3);
        drop(first);
        drop(second);
        assert_eq!(Arc::strong_count(&provider.uploader), 1);
        Ok(())
    }
}
