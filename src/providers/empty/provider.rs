use futures_util::future::BoxFuture;
use serde::Deserialize;
use serde_yaml::Value;

use crate::compatibility::EndpointDescriptor;
use crate::pipeline::sink::Sink;
use crate::providers::empty::sink::EmptySink;
use crate::providers::traits::{SinkContext, SinkPrepare, SinkProvider};

pub struct EmptySinkProvider;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EmptySinkConfig {}

impl EmptySinkProvider {
    pub fn from_config(value: Value) -> anyhow::Result<Self> {
        let _: EmptySinkConfig = serde_yaml::from_value(value)
            .map_err(|error| anyhow::anyhow!("Failed to parse empty sink config: {error}"))?;
        Ok(Self)
    }
}

impl SinkProvider for EmptySinkProvider {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::Discard
    }

    fn prepare(&self, _request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn build_sink(&self, context: SinkContext) -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>> {
        Box::pin(async move { Ok(Box::new(EmptySink::new(context.counters)) as Box<dyn Sink>) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unknown_empty_sink_settings() -> anyhow::Result<()> {
        assert!(EmptySinkProvider::from_config(serde_yaml::from_str("unexpected: true")?).is_err());
        assert!(EmptySinkProvider::from_config(serde_yaml::from_str("{}")?).is_ok());
        Ok(())
    }
}
