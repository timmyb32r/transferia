use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::compute::cast;
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use clickhouse_arrow::{
    ArrowFormat, Client, ClientBuilder, ConnectionStatus, Error as ClickHouseError,
    Result as ClickHouseResult,
};
use futures_util::StreamExt as _;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::{JoinError, JoinHandle};
use tokio::time::timeout;

use super::ClickHouseSinkConfig;

type ConnectTask = JoinHandle<ClickHouseResult<Client<ArrowFormat>>>;

pub(super) struct ReconnectingClient {
    builder: ClientBuilder,
    client: Mutex<Option<Client<ArrowFormat>>>,
    reconnect: AsyncMutex<()>,
    connect_task: AsyncMutex<Option<ConnectTask>>,
    connect_timeout: Duration,
    request_timeout: Duration,
}

impl ReconnectingClient {
    pub(super) fn new(config: &ClickHouseSinkConfig) -> Self {
        let connect_timeout = config.connect_timeout();
        let request_timeout = config.request_timeout();
        Self {
            // Keep the hostname-bearing, unverified builder. `clickhouse-arrow::verify`
            // replaces it with resolved socket addresses, so caching a verified builder
            // would pin every reconnect to the DNS answer observed at process startup.
            builder: configured_builder(config),
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
        let batches = normalize_insert_batches(batches)?;
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

    pub(super) async fn ensure_connected(&self) -> ClickHouseResult<()> {
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
        let builder = self.builder.clone();
        let runtime = tokio::runtime::Handle::current();
        let connect_timeout = self.connect_timeout;
        self.build_client_with(move || {
            spawn_bounded_connect_task(runtime, connect_timeout, builder.build_arrow())
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

fn spawn_bounded_connect_task(
    runtime: tokio::runtime::Handle,
    connect_timeout: Duration,
    connect: impl Future<Output = ClickHouseResult<Client<ArrowFormat>>> + Send + 'static,
) -> ConnectTask {
    let deadline = tokio::time::Instant::now() + connect_timeout;
    tokio::task::spawn_blocking(move || {
        runtime.block_on(async move {
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

fn normalize_insert_batches(batches: Vec<RecordBatch>) -> ClickHouseResult<Vec<RecordBatch>> {
    batches.into_iter().map(normalize_insert_batch).collect()
}

fn normalize_insert_batch(batch: RecordBatch) -> ClickHouseResult<RecordBatch> {
    if !batch
        .schema()
        .fields()
        .iter()
        .any(|field| field.data_type() == &DataType::Date64)
    {
        return Ok(batch);
    }

    let target_type = DataType::Timestamp(TimeUnit::Millisecond, None);
    let schema = batch.schema();
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| {
            if field.data_type() == &DataType::Date64 {
                Arc::new(field.as_ref().clone().with_data_type(target_type.clone()))
            } else {
                Arc::clone(field)
            }
        })
        .collect();
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| {
            if field.data_type() == &DataType::Date64 {
                cast(column, &target_type)
            } else {
                Ok(Arc::clone(column))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    let schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    Ok(RecordBatch::try_new(schema, columns)?)
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
        .with_destination(config.endpoint.as_str())
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Condvar, PoisonError};

    use arrow::array::{Array as _, Date64Array, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use tokio::sync::Notify;

    use super::*;

    struct BlockingGate {
        open: Mutex<bool>,
        changed: Condvar,
    }

    impl BlockingGate {
        const fn new() -> Self {
            Self {
                open: Mutex::new(false),
                changed: Condvar::new(),
            }
        }

        fn wait(&self) {
            let open = self.open.lock().unwrap_or_else(PoisonError::into_inner);
            let (_open, _timeout) = self
                .changed
                .wait_timeout_while(open, Duration::from_secs(2), |open| !*open)
                .unwrap_or_else(PoisonError::into_inner);
        }

        fn open(&self) {
            *self.open.lock().unwrap_or_else(PoisonError::into_inner) = true;
            self.changed.notify_all();
        }
    }

    fn reconnecting_client(connect_timeout: Duration) -> ReconnectingClient {
        ReconnectingClient {
            builder: ClientBuilder::new().with_destination("127.0.0.1:1"),
            client: Mutex::new(None),
            reconnect: AsyncMutex::new(()),
            connect_task: AsyncMutex::new(None),
            connect_timeout,
            request_timeout: Duration::from_secs(1),
        }
    }

    fn gated_connect_task(
        starts: &AtomicUsize,
        gate: Arc<BlockingGate>,
        started: Option<Arc<Notify>>,
    ) -> tokio::task::JoinHandle<ClickHouseResult<Client<ArrowFormat>>> {
        starts.fetch_add(1, Ordering::Relaxed);
        tokio::task::spawn_blocking(move || {
            if let Some(started) = started {
                started.notify_one();
            }
            gate.wait();
            Err(ClickHouseError::StartupError)
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn blocking_connect_attempt_does_not_block_tokio_timeout() {
        let client = Arc::new(reconnecting_client(Duration::from_millis(50)));
        let starts = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(BlockingGate::new());
        let started = Arc::new(Notify::new());
        let waiter = tokio::spawn({
            let client = Arc::clone(&client);
            let starts = Arc::clone(&starts);
            let gate = Arc::clone(&gate);
            let started = Arc::clone(&started);
            async move {
                client
                    .build_client_with(move || gated_connect_task(&starts, gate, Some(started)))
                    .await
            }
        });

        let task_started = timeout(Duration::from_secs(1), started.notified()).await;
        let result = timeout(Duration::from_secs(1), waiter).await;
        gate.open();

        assert!(task_started.is_ok(), "blocking connect task did not start");
        assert!(matches!(
            result,
            Ok(Ok(Err(ClickHouseError::ConnectionTimeout(_))))
        ));
        assert_eq!(starts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn background_connect_task_obeys_its_internal_deadline() {
        let connect_timeout = Duration::from_millis(10);
        let task = spawn_bounded_connect_task(
            tokio::runtime::Handle::current(),
            connect_timeout,
            std::future::pending::<ClickHouseResult<Client<ArrowFormat>>>(),
        );

        let result = timeout(Duration::from_secs(1), task).await;

        assert!(matches!(
            result,
            Ok(Ok(Err(ClickHouseError::ConnectionTimeout(_))))
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn expired_background_connect_task_does_not_poll_the_connector() {
        let polls = Arc::new(AtomicUsize::new(0));
        let connect = std::future::poll_fn({
            let polls = Arc::clone(&polls);
            move |_| {
                polls.fetch_add(1, Ordering::Relaxed);
                std::task::Poll::Pending
            }
        });
        let task =
            spawn_bounded_connect_task(tokio::runtime::Handle::current(), Duration::ZERO, connect);

        let result = timeout(Duration::from_secs(1), task).await;

        assert!(matches!(
            result,
            Ok(Ok(Err(ClickHouseError::ConnectionTimeout(_))))
        ));
        assert_eq!(polls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn timed_out_connect_attempt_is_reused() {
        let client = reconnecting_client(Duration::from_millis(10));
        let starts = AtomicUsize::new(0);
        let gate = Arc::new(BlockingGate::new());

        let first = client
            .build_client_with(|| gated_connect_task(&starts, Arc::clone(&gate), None))
            .await;
        let second = client
            .build_client_with(|| gated_connect_task(&starts, Arc::clone(&gate), None))
            .await;
        gate.open();
        let completed = client
            .build_client_with(|| gated_connect_task(&starts, Arc::clone(&gate), None))
            .await;

        assert!(matches!(first, Err(ClickHouseError::ConnectionTimeout(_))));
        assert!(matches!(second, Err(ClickHouseError::ConnectionTimeout(_))));
        assert!(matches!(completed, Err(ClickHouseError::StartupError)));
        assert_eq!(starts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn cancelled_connect_waiter_preserves_the_attempt() {
        let client = reconnecting_client(Duration::from_secs(1));
        let starts = AtomicUsize::new(0);
        let gate = Arc::new(BlockingGate::new());
        let started = Arc::new(Notify::new());
        let mut waiter = Box::pin(client.build_client_with(|| {
            gated_connect_task(&starts, Arc::clone(&gate), Some(Arc::clone(&started)))
        }));

        tokio::select! {
            () = started.notified() => {}
            result = &mut waiter => panic!("connect waiter completed unexpectedly: {result:?}"),
        }
        drop(waiter);
        gate.open();
        let completed = client
            .build_client_with(|| gated_connect_task(&starts, Arc::clone(&gate), None))
            .await;

        assert!(matches!(completed, Err(ClickHouseError::StartupError)));
        assert_eq!(starts.load(Ordering::Relaxed), 1);
    }

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

    #[test]
    fn normalizes_date64_to_timestamp_milliseconds_for_native_serialization() -> ClickHouseResult<()>
    {
        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("date", DataType::Date64, true)],
            [("schema-key".into(), "schema-value".into())].into(),
        ));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Date64Array::from(vec![
                Some(-1),
                None,
                Some(86_400_000),
            ]))],
        )?;

        let normalized = normalize_insert_batch(batch)?;

        assert_eq!(
            normalized.schema().field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        );
        assert_eq!(
            normalized
                .schema()
                .metadata()
                .get("schema-key")
                .map(String::as_str),
            Some("schema-value")
        );
        let values = normalized
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("Date64 must be cast to TimestampMillisecond");
        assert_eq!(values.value(0), -1);
        assert!(values.is_null(1));
        assert_eq!(values.value(2), 86_400_000);
        Ok(())
    }
}
