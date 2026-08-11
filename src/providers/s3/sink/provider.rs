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
}

impl S3SinkProvider {
    pub fn from_config(value: Value) -> anyhow::Result<Self> {
        let cfg: S3SinkConfig = serde_yaml::from_value(value)
            .map_err(|e| anyhow::anyhow!("Failed to parse S3 sink config: {e}"))?;
        cfg.validate()?;
        Ok(Self { cfg })
    }

    #[must_use]
    pub const fn config(&self) -> &S3SinkConfig {
        &self.cfg
    }
}

impl SinkProvider for S3SinkProvider {
    fn compatibility(&self) -> EndpointDescriptor {
        let partitioning = match &self.cfg.partitioning {
            super::config::PartitioningConfig::Source => S3Partitioning::Source,
            super::config::PartitioningConfig::Fields { columns } => {
                S3Partitioning::Fields(columns.clone())
            }
            super::config::PartitioningConfig::Time { .. } => S3Partitioning::SourceTime,
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
        let store = match self.cfg.build_store() {
            Ok(store) => store,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let uploader: std::sync::Arc<dyn ObjectUploader> =
            std::sync::Arc::new(S3Uploader::new(store, self.cfg.upload.clone()));
        let sink = S3Sink::new(
            self.cfg.clone(),
            uploader,
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
}
