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

    async fn client_slot(&self) -> Arc<ReconnectingClient> {
        Arc::clone(
            self.client
                .get_or_init(|| async { Arc::new(ReconnectingClient::new(&self.config)) })
                .await,
        )
    }

    async fn shared_client(&self) -> anyhow::Result<Arc<ReconnectingClient>> {
        let client = self.client_slot().await;
        client
            .ensure_connected()
            .await
            .map_err(|error| anyhow::anyhow!("ClickHouse connection failed: {error}"))?;
        Ok(client)
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
            Ok(
                Box::new(ClickHouseSink::with_transport_for_partition_and_visibility(
                    self.config.clone(),
                    context.counters,
                    Arc::new(NativeTransport::new(client)),
                    context.partition_id,
                    context.keep_system_columns,
                )) as Box<dyn Sink>,
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn provider_publishes_client_before_any_connect_attempt() -> anyhow::Result<()> {
        let provider = ClickHouseSinkProvider::from_config(serde_yaml::from_str(
            "connection_string: 127.0.0.1:1\nuse_tls: false\nconnect_timeout_ms: 1\n",
        )?)?;

        let first = provider.client_slot().await;
        let second = provider.client_slot().await;

        assert!(Arc::ptr_eq(&first, &second));
        Ok(())
    }
}
