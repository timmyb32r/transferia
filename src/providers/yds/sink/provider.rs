use alloc::sync::Arc;

use futures_util::future::BoxFuture;
use serde_yaml::Value;

use crate::pipeline::sink::Sink;
use crate::providers::traits::SinkProvider;
use crate::providers::yds::sink::writer::{YdsSink, YdsSinkConfig};
use crate::serializer::Serializer;

pub struct YdsSinkProvider {
    cfg: YdsSinkConfig,
    serializer: Arc<dyn Serializer>,
}

impl YdsSinkProvider {
    pub fn from_config(value: Value) -> anyhow::Result<Self> {
        let cfg: YdsSinkConfig = serde_yaml::from_value(value)
            .map_err(|e| anyhow::anyhow!("Failed to parse YDS sink config: {e}"))?;
        if cfg.connection_string.is_empty() {
            anyhow::bail!("yds sink: connection_string must not be empty");
        }
        if cfg.topic_path.is_empty() {
            anyhow::bail!("yds sink: topic_path must not be empty");
        }

        let serializer = crate::serializer::build_json_serializer();

        tracing::info!(
            "YDS sink: topic={} serializer={}",
            cfg.topic_path, cfg.serializer_type,
        );

        Ok(Self { cfg, serializer })
    }
}

impl SinkProvider for YdsSinkProvider {
    fn build_sink(&self) -> BoxFuture<'_, anyhow::Result<Arc<dyn Sink>>> {
        let sink = Arc::new(YdsSink::new(
            self.cfg.clone(),
            Arc::clone(&self.serializer),
        ));
        Box::pin(async move { Ok(sink as Arc<dyn Sink>) })
    }


}
