use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use clickhouse_arrow::{
    ArrowFormat, Client, ClientBuilder, ConnectionStatus, Error as ClickHouseError,
    Result as ClickHouseResult,
};
use futures_util::{Stream, StreamExt as _};
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::{JoinError, JoinHandle};
use tokio::time::timeout;

use super::ClickHouseSinkConfig;

type ConnectTask = JoinHandle<ClickHouseResult<Client<ArrowFormat>>>;

pub struct ReconnectingClient {
    builders: Vec<ClientBuilder>,
    client: Mutex<Option<Client<ArrowFormat>>>,
    reconnect: AsyncMutex<()>,
    connect_task: AsyncMutex<Option<ConnectTask>>,
    connect_timeout: Duration,
    request_timeout: Duration,
}

pub struct QueryStream {
    response: std::pin::Pin<Box<dyn Stream<Item = ClickHouseResult<RecordBatch>> + Send + 'static>>,
    owner: std::sync::Arc<ReconnectingClient>,
    client_id: u16,
    active: bool,
}

impl Stream for QueryStream {
    type Item = ClickHouseResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let result = self.response.as_mut().poll_next(context);
        match &result {
            std::task::Poll::Ready(None) => self.active = false,
            std::task::Poll::Ready(Some(Err(_))) => {
                self.active = false;
                self.owner.invalidate(self.client_id);
            }
            _ => {}
        }
        result
    }
}

impl Drop for QueryStream {
    fn drop(&mut self) {
        if self.active {
            self.owner.invalidate(self.client_id);
        }
    }
}

impl ReconnectingClient {
    pub(super) fn new(config: &ClickHouseSinkConfig) -> Self {
        Self::from_connections(
            configured_builders(config),
            config.connect_timeout(),
            config.request_timeout(),
        )
    }

    pub(crate) fn from_connections(
        builders: Vec<ClientBuilder>,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Self {
        debug_assert!(!builders.is_empty());
        Self {
            // Keep the hostname-bearing, unverified builder. `clickhouse-arrow::verify`
            // replaces it with resolved socket addresses, so caching a verified builder
            // would pin every reconnect to the DNS answer observed at process startup.
            builders,
            client: Mutex::new(None),
            reconnect: AsyncMutex::new(()),
            connect_task: AsyncMutex::new(None),
            connect_timeout,
            request_timeout,
        }
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

    pub(crate) async fn query_all(&self, query: &str) -> ClickHouseResult<Vec<RecordBatch>> {
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

    pub(crate) async fn query_stream(
        self: &std::sync::Arc<Self>,
        query: &str,
    ) -> ClickHouseResult<QueryStream> {
        let client = self.client().await?;
        let client_id = client.client_id;
        let response = timeout(self.request_timeout, client.query(query, None))
            .await
            .map_err(|_| self.request_timeout_error("ClickHouse snapshot query"))??;
        Ok(QueryStream {
            response: Box::pin(response),
            owner: std::sync::Arc::clone(self),
            client_id,
            active: true,
        })
    }

    pub(crate) async fn ensure_connected(&self) -> ClickHouseResult<()> {
        drop(self.client().await?);
        Ok(())
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
        // `build_arrow` verifies an unverified builder and therefore resolves DNS on
        // every reconnect instead of retrying a stale address forever.
        // It also performs a synchronous 30-second socket connect, so keep that poll
        // off Tokio workers until the dependency provides a cancellable connector.
        let builders = self.builders.clone();
        let runtime_handle = tokio::runtime::Handle::current();
        let connect_timeout = self.connect_timeout;
        self.build_client_with(move || {
            spawn_bounded_connect_task(
                runtime_handle,
                connect_timeout,
                connect_first_available(builders, connect_timeout),
            )
        })
        .await
    }

    async fn build_client_with(
        &self,
        spawn_connect: impl FnOnce() -> ConnectTask,
    ) -> ClickHouseResult<Client<ArrowFormat>> {
        let client = {
            let mut connect_task = self.connect_task.lock().await;
            let result = {
                // Await a borrow: timeout or caller cancellation must leave the one
                // underlying attempt available to the next reconnect waiter.
                let task = connect_task.get_or_insert_with(spawn_connect);
                timeout(self.connect_timeout, task).await
            };
            let result = result.map_err(|_| connect_timeout_error(self.connect_timeout))?;
            connect_task.take();
            drop(connect_task);
            result.map_err(|error| connect_task_error(&error))??
        };
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

async fn connect_first_available(
    builders: Vec<ClientBuilder>,
    connect_timeout: Duration,
) -> ClickHouseResult<Client<ArrowFormat>> {
    let mut errors = Vec::with_capacity(builders.len());
    for builder in builders {
        match timeout(connect_timeout, builder.build_arrow()).await {
            Ok(Ok(client)) => return Ok(client),
            Ok(Err(error)) => errors.push(error.to_string()),
            Err(_) => errors.push(format!(
                "connect timed out after {} ms",
                connect_timeout.as_millis()
            )),
        }
    }
    Err(ClickHouseError::Client(format!(
        "ClickHouse native protocol connection failed for every configured host: {}. Verify that clickhouse.port is the native port (default: {}), not the HTTP port",
        errors.join("; "),
        crate::providers::clickhouse::DEFAULT_NATIVE_PORT
    )))
}

fn spawn_bounded_connect_task(
    runtime_handle: tokio::runtime::Handle,
    connect_timeout: Duration,
    connect: impl Future<Output = ClickHouseResult<Client<ArrowFormat>>> + Send + 'static,
) -> ConnectTask {
    let deadline = tokio::time::Instant::now() + connect_timeout;
    tokio::task::spawn_blocking(move || {
        runtime_handle.block_on(async move {
            // Tokio timeout polls its child before checking the deadline. Check
            // explicitly so a task delayed in the blocking queue cannot start a
            // synchronous socket call after its deadline has already expired.
            if tokio::time::Instant::now() >= deadline {
                return Err(connect_timeout_error(connect_timeout));
            }
            tokio::time::timeout_at(deadline, connect)
                .await
                .map_err(|_| connect_timeout_error(connect_timeout))?
        })
    })
}

fn connect_timeout_error(connect_timeout: Duration) -> ClickHouseError {
    ClickHouseError::ConnectionTimeout(format!(
        "ClickHouse connect timed out after {} ms",
        connect_timeout.as_millis()
    ))
}

fn connect_task_error(error: &JoinError) -> ClickHouseError {
    if error.is_cancelled() {
        ClickHouseError::ConnectionGone("ClickHouse connect task was cancelled")
    } else {
        ClickHouseError::Client(format!("ClickHouse connect task failed: {error}"))
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

fn configured_builders(config: &ClickHouseSinkConfig) -> Vec<ClientBuilder> {
    config
        .hosts
        .iter()
        .map(|host| {
            ClientBuilder::new()
                .with_destination(crate::providers::address::host_port(host, config.port))
                .with_database(config.database.as_str())
                .with_username(config.username.as_str())
                .with_password(config.password.as_str())
                // The sink owns batching and acknowledges source offsets as soon as the
                // native INSERT completes. Never inherit a server/user profile that can
                // acknowledge an asynchronous insert before it is flushed.
                .with_setting("async_insert", 0_i64)
                .with_setting("wait_for_async_insert", 1_i64)
                // ReplicatedMergeTree deduplication is unsafe for this at-least-once
                // sink: two distinct source offsets may legitimately contain identical
                // rows. Preserve both and let ambiguous retries remain visible duplicates.
                .with_setting("insert_deduplicate", 0_i64)
                .with_tls(false)
        })
        .collect()
}

pub fn quote_identifier(identifier: &str) -> String {
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
#[path = "tests/client.rs"]
mod tests;
