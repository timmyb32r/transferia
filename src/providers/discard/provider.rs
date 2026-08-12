use futures_util::future::BoxFuture;
use serde::Deserialize;
use serde_yaml::Value;

use crate::compatibility::EndpointDescriptor;
use crate::delivery::{SinkLimits, NO_LIMITS};
use crate::pipeline::sink::Sink;
use crate::providers::discard::sink::DiscardSink;
use crate::providers::traits::{SinkContext, SinkPrepare, SinkProvider};

pub struct DiscardSinkProvider;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DiscardSinkConfig {}

impl DiscardSinkProvider {
    pub fn from_config(value: Value) -> anyhow::Result<Self> {
        let _: DiscardSinkConfig = serde_yaml::from_value(value)
            .map_err(|error| anyhow::anyhow!("Failed to parse discard sink config: {error}"))?;
        Ok(Self)
    }
}

impl SinkProvider for DiscardSinkProvider {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::Discard
    }

    fn limits(&self) -> &dyn SinkLimits {
        &NO_LIMITS
    }

    fn prepare(&self, _request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn build_sink(&self, context: SinkContext) -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>> {
        Box::pin(async move { Ok(Box::new(DiscardSink::new(context.counters)) as Box<dyn Sink>) })
    }
}

#[cfg(test)]
mod tests;
