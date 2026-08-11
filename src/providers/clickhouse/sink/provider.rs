use std::sync::Arc;

use futures_util::future::BoxFuture;
use serde_yaml::Value;
use tokio::sync::OnceCell;

use super::client::ReconnectingClient;
use super::table::prepare_tables;
use super::transport::NativeTransport;
use super::{ClickHouseSink, ClickHouseSinkConfig};
use crate::compatibility::EndpointDescriptor;
use crate::pipeline::sink::Sink;
use crate::providers::traits::{SinkContext, SinkPrepare, SinkProvider};

pub struct ClickHouseSinkProvider {
    config: ClickHouseSinkConfig,
    client: OnceCell<Arc<ReconnectingClient>>,
}

impl ClickHouseSinkProvider {
    pub fn from_config(value: Value) -> anyhow::Result<Self> {
        Ok(Self {
            config: ClickHouseSinkConfig::from_value(value)?,
            client: OnceCell::new(),
        })
    }

    async fn shared_client(&self) -> anyhow::Result<Arc<ReconnectingClient>> {
        self.client
            .get_or_try_init(|| async {
                let client = Arc::new(ReconnectingClient::connect(&self.config).await?);
                tracing::info!(
                    endpoint = self.config.connection_string,
                    "connected shared ClickHouse client"
                );
                Ok::<_, anyhow::Error>(client)
            })
            .await
            .map(Arc::clone)
    }
}

impl SinkProvider for ClickHouseSinkProvider {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::ClickHouse
    }

    fn prepare(&self, request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            let client = self.shared_client().await?;
            prepare_tables(client.as_ref(), &self.config, &request).await
        })
    }

    fn build_sink(&self, context: SinkContext) -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>> {
        Box::pin(async move {
            let client = self.shared_client().await?;
            tracing::info!(
                partition = context.partition_id,
                "building ClickHouse sink on shared client"
            );
            Ok(Box::new(ClickHouseSink::with_transport_for_partition(
                self.config.clone(),
                context.counters,
                Arc::new(NativeTransport::new(client)),
                context.partition_id,
            )) as Box<dyn Sink>)
        })
    }
}
