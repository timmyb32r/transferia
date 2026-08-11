use std::sync::Mutex;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use clickhouse_arrow::{
    ArrowFormat, Client, ClientBuilder, ConnectionStatus, Error as ClickHouseError,
    Result as ClickHouseResult,
};
use futures_util::StreamExt as _;
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::timeout;

use super::ClickHouseSinkConfig;

pub(super) struct ReconnectingClient {
    builder: ClientBuilder,
    client: Mutex<Option<Client<ArrowFormat>>>,
    reconnect: AsyncMutex<()>,
    connect_timeout: Duration,
    request_timeout: Duration,
}

impl ReconnectingClient {
    pub(super) async fn connect(config: &ClickHouseSinkConfig) -> anyhow::Result<Self> {
        let connect_timeout = config.connect_timeout();
        let request_timeout = config.request_timeout();
        let builder = timeout(connect_timeout, configured_builder(config).verify())
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "ClickHouse destination resolution timed out after {} ms",
                    connect_timeout.as_millis()
                )
            })?
            .map_err(|error| anyhow::anyhow!("Failed to configure ClickHouse client: {error}"))?;
        let this = Self {
            builder,
            client: Mutex::new(None),
            reconnect: AsyncMutex::new(()),
            connect_timeout,
            request_timeout,
        };
        let client = this
            .build_client()
            .await
            .map_err(|error| anyhow::anyhow!("ClickHouse connection failed: {error}"))?;
        this.replace(client);
        Ok(this)
    }

    pub(super) async fn insert_many(
        &self,
        table: &str,
        batches: Vec<RecordBatch>,
    ) -> ClickHouseResult<()> {
        let schema = batches
            .first()
            .ok_or_else(|| clickhouse_arrow::Error::Client("empty INSERT batch list".into()))?
            .schema();
        if batches.iter().any(|batch| batch.schema() != schema) {
            return Err(clickhouse_arrow::Error::SchemaConfig(
                "all INSERT batches must have the same Arrow schema".into(),
            ));
        }
        let query = insert_query(table, schema.as_ref());
        let client = self.client().await?;
        let client_id = client.client_id;
        let mut invalidate = InvalidateOnDrop::new(self, client_id);
        let result = async {
            let mut stream = client.insert_many(&query, batches, None).await?;
            while let Some(item) = stream.next().await {
                item?;
            }
            Ok(())
        }
        .await;
        if result.is_ok() {
            invalidate.disarm();
        }
        result
    }

    pub(super) async fn execute(&self, query: &str) -> ClickHouseResult<()> {
        let client = self.client().await?;
        let client_id = client.client_id;
        let mut invalidate = InvalidateOnDrop::new(self, client_id);
        let result = timeout(self.request_timeout, client.execute(query, None))
            .await
            .map_err(|_| self.request_timeout_error("ClickHouse request"))?;
        if result.is_ok() {
            invalidate.disarm();
        }
        result
    }

    pub(super) async fn query_all(&self, query: &str) -> ClickHouseResult<Vec<RecordBatch>> {
        let client = self.client().await?;
        let client_id = client.client_id;
        let mut invalidate = InvalidateOnDrop::new(self, client_id);
        let result = timeout(self.request_timeout, async {
            let mut stream = client.query(query, None).await?;
            let mut batches = Vec::new();
            while let Some(batch) = stream.next().await {
                batches.push(batch?);
            }
            Ok::<_, ClickHouseError>(batches)
        })
        .await
        .map_err(|_| self.request_timeout_error("ClickHouse schema query"))?;
        if result.is_ok() {
            invalidate.disarm();
        }
        result
    }

    async fn client(&self) -> ClickHouseResult<Client<ArrowFormat>> {
        if let Some(client) = self.current_client() {
            return Ok(client);
        }
        let _reconnect = self.reconnect.lock().await;
        if let Some(client) = self.current_client() {
            return Ok(client);
        }
        tracing::info!("reconnecting ClickHouse client");
        let client = self.build_client().await?;
        self.replace(client.clone());
        Ok(client)
    }

    async fn build_client(&self) -> ClickHouseResult<Client<ArrowFormat>> {
        let client = timeout(self.connect_timeout, self.builder.clone().build_arrow())
            .await
            .map_err(|_| {
                ClickHouseError::ConnectionTimeout(format!(
                    "ClickHouse connect timed out after {} ms",
                    self.connect_timeout.as_millis()
                ))
            })??;
        timeout(self.request_timeout, client.execute("SELECT 1", None))
            .await
            .map_err(|_| self.request_timeout_error("ClickHouse health check"))??;
        Ok(client)
    }

    fn replace(&self, client: Client<ArrowFormat>) {
        self.client
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(client);
    }

    fn request_timeout_error(&self, operation: &str) -> ClickHouseError {
        ClickHouseError::OutgoingTimeout(format!(
            "{operation} timed out after {} ms",
            self.request_timeout.as_millis()
        ))
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

struct InvalidateOnDrop<'client> {
    owner: &'client ReconnectingClient,
    client_id: u16,
    armed: bool,
}

impl<'client> InvalidateOnDrop<'client> {
    const fn new(owner: &'client ReconnectingClient, client_id: u16) -> Self {
        Self {
            owner,
            client_id,
            armed: true,
        }
    }

    const fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for InvalidateOnDrop<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.owner.invalidate(self.client_id);
        }
    }
}

fn configured_builder(config: &ClickHouseSinkConfig) -> ClientBuilder {
    ClientBuilder::new()
        .with_destination(config.connection_string.as_str())
        .with_database(config.database.as_str())
        .with_username(config.username.as_str())
        .with_password(config.password.as_str())
        .with_tls(config.use_tls)
}

pub(super) fn quote_identifier(identifier: &str) -> String {
    let mut quoted = String::with_capacity(identifier.len() + 2);
    quoted.push('`');
    for character in identifier.chars() {
        if matches!(character, '`' | '\\') {
            quoted.push('\\');
        }
        quoted.push(character);
    }
    quoted.push('`');
    quoted
}

fn insert_query(table: &str, schema: &arrow::datatypes::Schema) -> String {
    let columns = schema
        .fields()
        .iter()
        .map(|field| quote_identifier(field.name()))
        .collect::<Vec<_>>()
        .join(", ");
    format!("INSERT INTO {} ({columns}) VALUES", quote_identifier(table))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn quotes_clickhouse_identifiers() {
        assert_eq!(quote_identifier("events"), "`events`");
        assert_eq!(quote_identifier("odd`name\\part"), "`odd\\`name\\\\part`");
    }

    #[test]
    fn insert_names_escaped_table_and_columns() {
        let schema = Schema::new(vec![
            Field::new("first", DataType::Int64, false),
            Field::new("odd`column", DataType::Utf8, true),
        ]);

        assert_eq!(
            insert_query("odd`table", &schema),
            "INSERT INTO `odd\\`table` (`first`, `odd\\`column`) VALUES"
        );
    }
}
