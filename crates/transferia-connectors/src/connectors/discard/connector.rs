use std::sync::Arc;

use futures_util::future::BoxFuture;

use crate::connectors::discard::sink::DiscardSink;
use transferia_core::delivery::{SinkLimits, NO_LIMITS};
use transferia_core::sink::Sink;
use transferia_delivery_contracts::semantics::EndpointDescriptor;
use transferia_registry::{
    SinkBuildContext, SinkConnector, SinkPrepare, SinkSpeedtestIsolation,
};

pub struct DiscardSinkConnector;

impl Default for DiscardSinkConnector {
    fn default() -> Self {
        Self::new()
    }
}

impl DiscardSinkConnector {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl SinkConnector for DiscardSinkConnector {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::Discard
    }

    fn limits(&self) -> &dyn SinkLimits {
        &NO_LIMITS
    }

    fn destination_type(
        &self,
        _column: &transferia_core::data::schema::SchemaColumn,
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

    fn isolate_speedtest(
        self: Arc<Self>,
        discovery: Arc<transferia_core::delivery::DeliveryDiscovery>,
        _isolation_id: String,
    ) -> BoxFuture<'static, anyhow::Result<SinkSpeedtestIsolation>> {
        let connector: Arc<dyn SinkConnector> = self;
        Box::pin(async move {
            Ok(SinkSpeedtestIsolation::no_external_writes(
                connector,
                discovery,
            ))
        })
    }
}

#[cfg(test)]
#[path = "tests/connector.rs"]
mod tests;
