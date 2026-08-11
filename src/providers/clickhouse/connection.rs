use std::sync::Mutex;

use arrow::record_batch::RecordBatch;
use clickhouse_arrow::{
    ArrowFormat, Client, ClientBuilder, ConnectionStatus, Result as ClickHouseResult,
};
use futures_util::StreamExt as _;

use super::ClickHouseSinkConfig;

pub(super) struct ReconnectingClient {
    builder: ClientBuilder,
    client: Mutex<Option<Client<ArrowFormat>>>,
}

impl ReconnectingClient {
    pub(super) async fn connect(config: &ClickHouseSinkConfig) -> anyhow::Result<Self> {
        let builder = configured_builder(config)
            .verify()
            .await
            .map_err(|error| anyhow::anyhow!("Failed to configure ClickHouse client: {error}"))?;
        let client = builder
            .clone()
            .build_arrow()
            .await
            .map_err(|error| anyhow::anyhow!("ClickHouse connection failed: {error}"))?;
        client
            .execute("SELECT 1", None)
            .await
            .map_err(|error| anyhow::anyhow!("ClickHouse health check failed: {error}"))?;
        Ok(Self {
            builder,
            client: Mutex::new(Some(client)),
        })
    }

    pub(super) async fn insert_many(
        &self,
        table: &str,
        batches: Vec<RecordBatch>,
    ) -> ClickHouseResult<()> {
        let client = self.client().await?;
        let client_id = client.client_id;
        let result = async {
            let query = format!("INSERT INTO `{table}` VALUES");
            let mut stream = client.insert_many(&query, batches, None).await?;
            while let Some(item) = stream.next().await {
                item?;
            }
            Ok(())
        }
        .await;
        if result.is_err() {
            self.invalidate(client_id);
        }
        result
    }

    async fn client(&self) -> ClickHouseResult<Client<ArrowFormat>> {
        if let Some(client) = self.current_client() {
            return Ok(client);
        }
        tracing::info!("reconnecting ClickHouse client");
        let client = self.builder.clone().build_arrow().await?;
        self.client
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(client.clone());
        Ok(client)
    }

    fn current_client(&self) -> Option<Client<ArrowFormat>> {
        let mut current = self
            .client
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match current.as_ref() {
            Some(client) if client.status() == ConnectionStatus::Open => Some(client.clone()),
            Some(_) => {
                *current = None;
                None
            }
            None => None,
        }
    }

    fn invalidate(&self, client_id: u16) {
        let mut current = self
            .client
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if current
            .as_ref()
            .is_some_and(|client| client.client_id == client_id)
        {
            *current = None;
        }
    }
}

pub(super) async fn connect_client(
    config: &ClickHouseSinkConfig,
) -> ClickHouseResult<Client<ArrowFormat>> {
    configured_builder(config).build_arrow().await
}

fn configured_builder(config: &ClickHouseSinkConfig) -> ClientBuilder {
    let mut builder = ClientBuilder::new()
        .with_destination(config.connection_string.as_str())
        .with_database(config.database.as_str())
        .with_username(config.username.as_str())
        .with_password(config.password.as_str())
        .with_tls(config.use_tls);
    if let Some(domain) = &config.tls_domain {
        builder = builder.with_domain(domain.as_str());
    }
    builder
}
