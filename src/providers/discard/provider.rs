use futures_util::future::BoxFuture;

use crate::core::delivery::{SinkLimits, NO_LIMITS};
use crate::core::sink::Sink;
use crate::delivery::semantics::EndpointDescriptor;
use crate::providers::discard::sink::DiscardSink;
use crate::providers::traits::{SinkBuildContext, SinkPrepare, SinkProvider};

pub struct DiscardSinkProvider;

impl Default for DiscardSinkProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl DiscardSinkProvider {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl SinkProvider for DiscardSinkProvider {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::Discard
    }

    fn limits(&self) -> &dyn SinkLimits {
        &NO_LIMITS
    }

    fn destination_type(
        &self,
        _column: &crate::core::data::schema::SchemaColumn,
    ) -> anyhow::Result<String> {
        Ok("discarded".to_owned())
    }

    fn prepare(&self, _request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn build_sink(
        &self,
        context: SinkBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>> {
        Box::pin(async move { Ok(Box::new(DiscardSink::new(context.counters)) as Box<dyn Sink>) })
    }
}

#[cfg(test)]
#[path = "tests/provider.rs"]
mod tests;
