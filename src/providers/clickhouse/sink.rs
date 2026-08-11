use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use clickhouse_arrow::{ArrowFormat, ConnectionPool, ConnectionPoolBuilder};
use futures_util::future::BoxFuture;
use futures_util::StreamExt as _;
use serde::Deserialize;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

use crate::metrics::SinkCounters;
use crate::pipeline::sink::{Delivery, DeliveryId, Sink, SinkBatch, SinkEvent, SinkIo};
use crate::types::schema::SchemaColumn;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseSinkConfig {
    pub connection_string: String,
    #[serde(default = "default_database")]
    pub database: String,
    #[serde(default = "default_username")]
    pub username: String,
    #[serde(default)]
    pub password: String,
    #[serde(default = "default_insert_rows")]
    pub max_insert_rows: usize,
    #[serde(default = "default_insert_bytes")]
    pub max_insert_bytes: usize,
    #[serde(default = "default_flush_interval")]
    pub flush_interval_ms: u64,
    #[serde(default = "default_retry_initial")]
    pub retry_initial_ms: u64,
    #[serde(default = "default_retry_max")]
    pub retry_max_ms: u64,
    #[serde(default)]
    pub retry_max_attempts: Option<u32>,
    #[serde(default = "default_tls")]
    pub use_tls: bool,
    #[serde(default)]
    pub tls_domain: Option<String>,
    #[serde(default)]
    pub sorting_key: Vec<String>,
    #[serde(default)]
    pub recreate_tables: bool,
}

fn default_database() -> String {
    "default".into()
}
fn default_username() -> String {
    "default".into()
}
const fn default_insert_rows() -> usize {
    100_000
}
const fn default_insert_bytes() -> usize {
    64 * 1024 * 1024
}
const fn default_flush_interval() -> u64 {
    100
}
const fn default_retry_initial() -> u64 {
    50
}
const fn default_retry_max() -> u64 {
    30_000
}
const fn default_tls() -> bool {
    true
}

#[derive(Debug)]
pub enum InsertError {
    Transient(anyhow::Error),
    Permanent(anyhow::Error),
}

impl core::fmt::Display for InsertError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Transient(error) | Self::Permanent(error) => error.fmt(f),
        }
    }
}

pub trait InsertTransport: Send + Sync {
    fn insert(
        &self,
        table: Arc<str>,
        batches: Vec<RecordBatch>,
    ) -> BoxFuture<'static, Result<(), InsertError>>;
}

struct NativeTransport {
    pool: Arc<ConnectionPool<ArrowFormat>>,
}

impl InsertTransport for NativeTransport {
    fn insert(
        &self,
        table: Arc<str>,
        batches: Vec<RecordBatch>,
    ) -> BoxFuture<'static, Result<(), InsertError>> {
        let pool = Arc::clone(&self.pool);
        Box::pin(async move {
            let client = pool.get().await.map_err(|error| {
                InsertError::Transient(anyhow::anyhow!("ClickHouse pool get: {error}"))
            })?;
            let query = format!("INSERT INTO `{table}` VALUES");
            let mut stream = client
                .insert_many(&query, batches, None)
                .await
                .map_err(classify_insert_error)?;
            while let Some(item) = stream.next().await {
                item.map_err(classify_insert_error)?;
            }
            Ok(())
        })
    }
}

fn classify_insert_error(error: impl core::fmt::Display) -> InsertError {
    const PERMANENT_MARKERS: [&str; 9] = [
        "AUTHENTICATION_FAILED",
        "UNKNOWN_TABLE",
        "UNKNOWN_IDENTIFIER",
        "NO_SUCH_COLUMN",
        "TYPE_MISMATCH",
        "SYNTAX_ERROR",
        "NUMBER_OF_COLUMNS_DOESNT_MATCH",
        "Unknown table",
        "password",
    ];
    let message = error.to_string();
    let error = anyhow::anyhow!(message.clone());
    if PERMANENT_MARKERS
        .iter()
        .any(|marker| message.contains(marker))
    {
        InsertError::Permanent(error)
    } else {
        InsertError::Transient(error)
    }
}

async fn build_pool(config: &ClickHouseSinkConfig) -> anyhow::Result<ConnectionPool<ArrowFormat>> {
    ConnectionPoolBuilder::<ArrowFormat>::new(config.connection_string.as_str())
        .configure_pool(|pool| pool.max_size(1))
        .configure_client(|builder| {
            let mut builder = builder
                .with_database(config.database.as_str())
                .with_username(config.username.as_str())
                .with_password(config.password.as_str())
                .with_tls(config.use_tls);
            if let Some(domain) = &config.tls_domain {
                builder = builder.with_domain(domain.as_str());
            }
            builder
        })
        .build()
        .await
        .map_err(|error| anyhow::anyhow!("Failed to build ClickHouse pool: {error}"))
}

struct BufferedBatch {
    delivery_id: DeliveryId,
    batch: SinkBatch,
}

struct TableBuffer {
    table: Arc<str>,
    first_seen: Instant,
    rows: usize,
    bytes: usize,
    batches: Vec<BufferedBatch>,
}

struct DeliveryProgress {
    remaining_outputs: usize,
    source_messages: u64,
}

struct ActiveInsert {
    table: Arc<str>,
    rows: usize,
    bytes: usize,
    batches: Vec<BufferedBatch>,
}

struct InsertFailure {
    error: anyhow::Error,
}

pub struct ClickHouseSink {
    transport: Arc<dyn InsertTransport>,
    config: ClickHouseSinkConfig,
    counters: Arc<SinkCounters>,
    buffers: HashMap<Arc<str>, TableBuffer>,
    progress: BTreeMap<DeliveryId, DeliveryProgress>,
    next_received: DeliveryId,
    next_ack: DeliveryId,
    last_insert_started: Option<Instant>,
}

impl ClickHouseSink {
    pub async fn new(
        config: ClickHouseSinkConfig,
        counters: Arc<SinkCounters>,
    ) -> anyhow::Result<Self> {
        let pool = Arc::new(build_pool(&config).await?);
        {
            let client = pool
                .get()
                .await
                .map_err(|error| anyhow::anyhow!("ClickHouse connection failed: {error}"))?;
            client
                .execute("SELECT 1", None)
                .await
                .map_err(|error| anyhow::anyhow!("ClickHouse health check failed: {error}"))?;
        }
        tracing::info!(
            "Connected to ClickHouse at {} (one connection per partition)",
            config.connection_string
        );
        Ok(Self::with_transport(
            config,
            counters,
            Arc::new(NativeTransport { pool }),
        ))
    }

    #[must_use]
    pub fn with_transport(
        config: ClickHouseSinkConfig,
        counters: Arc<SinkCounters>,
        transport: Arc<dyn InsertTransport>,
    ) -> Self {
        Self {
            transport,
            config,
            counters,
            buffers: HashMap::new(),
            progress: BTreeMap::new(),
            next_received: DeliveryId::new(1),
            next_ack: DeliveryId::new(1),
            last_insert_started: None,
        }
    }

    fn accept(&mut self, delivery: Delivery) -> anyhow::Result<()> {
        anyhow::ensure!(
            delivery.id == self.next_received,
            "sink delivery order violation: expected {}, got {}",
            self.next_received.get(),
            delivery.id.get(),
        );
        self.next_received = self.next_received.next();
        let remaining_outputs = delivery
            .outputs
            .iter()
            .filter(|output| output.batch.num_rows() > 0)
            .count();
        self.progress.insert(
            delivery.id,
            DeliveryProgress {
                remaining_outputs,
                source_messages: delivery.meta.source_messages,
            },
        );
        for batch in delivery
            .outputs
            .into_iter()
            .filter(|output| output.batch.num_rows() > 0)
        {
            let table = Arc::clone(&batch.table);
            let rows = batch.rows();
            let bytes = batch.bytes();
            let buffer = self
                .buffers
                .entry(Arc::clone(&table))
                .or_insert_with(|| TableBuffer {
                    table,
                    first_seen: Instant::now(),
                    rows: 0,
                    bytes: 0,
                    batches: Vec::new(),
                });
            buffer.rows = buffer.rows.saturating_add(rows);
            buffer.bytes = buffer.bytes.saturating_add(bytes);
            buffer.batches.push(BufferedBatch {
                delivery_id: delivery.id,
                batch,
            });
        }
        Ok(())
    }

    fn next_flush(&self, input_closed: bool, memory_pressure: bool) -> Option<(Arc<str>, Instant)> {
        let interval = Duration::from_millis(self.config.flush_interval_ms);
        let rate_limit = self
            .last_insert_started
            .map_or_else(Instant::now, |last| last + interval);
        self.buffers
            .values()
            .map(|buffer| {
                let full = memory_pressure
                    || buffer.rows >= self.config.max_insert_rows
                    || buffer.bytes >= self.config.max_insert_bytes;
                let wanted = if input_closed || full {
                    Instant::now()
                } else {
                    buffer.first_seen + interval
                };
                (Arc::clone(&buffer.table), wanted.max(rate_limit))
            })
            .min_by_key(|(_, deadline)| *deadline)
    }

    fn take_insert(&mut self, table: &Arc<str>) -> anyhow::Result<ActiveInsert> {
        let buffer = self
            .buffers
            .remove(table)
            .ok_or_else(|| anyhow::anyhow!("missing ClickHouse buffer for table '{table}'"))?;
        self.last_insert_started = Some(Instant::now());
        Ok(ActiveInsert {
            table: buffer.table,
            rows: buffer.rows,
            bytes: buffer.bytes,
            batches: buffer.batches,
        })
    }

    fn start_insert(
        &self,
        active: ActiveInsert,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> JoinHandle<Result<ActiveInsert, InsertFailure>> {
        let transport = Arc::clone(&self.transport);
        let counters = Arc::clone(&self.counters);
        let config = self.config.clone();
        tokio::spawn(async move {
            let mut attempts = 0_u32;
            let mut backoff = Duration::from_millis(config.retry_initial_ms.max(1));
            loop {
                attempts = attempts.saturating_add(1);
                let batches = active
                    .batches
                    .iter()
                    .map(|buffered| buffered.batch.batch.clone())
                    .collect();
                let started = std::time::Instant::now();
                let result = tokio::select! {
                    () = cancellation.cancelled() => {
                        return Err(InsertFailure { error: anyhow::anyhow!("ClickHouse insert cancelled") });
                    }
                    result = transport.insert(Arc::clone(&active.table), batches) => result,
                };
                counters.add_busy(started.elapsed());
                match result {
                    Ok(()) => {
                        counters.add_rows(active.rows as u64);
                        counters.add_bytes(active.bytes as u64);
                        counters.add_flush();
                        return Ok(active);
                    }
                    Err(InsertError::Permanent(error)) => return Err(InsertFailure { error }),
                    Err(InsertError::Transient(error)) => {
                        if config.retry_max_attempts.is_some_and(|max| attempts >= max) {
                            return Err(InsertFailure {
                                error: error.context("ClickHouse retry limit exhausted"),
                            });
                        }
                        tracing::warn!(
                            attempts,
                            backoff_ms = backoff.as_millis() as u64,
                            "ClickHouse INSERT failed, retrying: {error}"
                        );
                        tokio::select! {
                            () = cancellation.cancelled() => {
                                return Err(InsertFailure { error: anyhow::anyhow!("ClickHouse retry cancelled") });
                            }
                            () = tokio::time::sleep(backoff) => {}
                        }
                        backoff = backoff
                            .saturating_mul(2)
                            .min(Duration::from_millis(config.retry_max_ms.max(1)));
                    }
                }
            }
        })
    }

    fn complete_insert(&mut self, active: ActiveInsert) -> anyhow::Result<()> {
        for buffered in active.batches {
            let progress = self
                .progress
                .get_mut(&buffered.delivery_id)
                .ok_or_else(|| {
                    anyhow::anyhow!("missing delivery progress {}", buffered.delivery_id.get())
                })?;
            progress.remaining_outputs = progress
                .remaining_outputs
                .checked_sub(1)
                .ok_or_else(|| anyhow::anyhow!("delivery output underflow"))?;
            drop(buffered);
        }
        tracing::info!(rows = active.rows, bytes = active.bytes, table = %active.table, "ClickHouse INSERT completed");
        Ok(())
    }

    async fn emit_committed(
        &mut self,
        events: &tokio::sync::mpsc::Sender<SinkEvent>,
    ) -> anyhow::Result<()> {
        let mut committed = None;
        let mut source_messages = 0_u64;
        while self
            .progress
            .get(&self.next_ack)
            .is_some_and(|progress| progress.remaining_outputs == 0)
        {
            let progress = self
                .progress
                .remove(&self.next_ack)
                .ok_or_else(|| anyhow::anyhow!("missing completed delivery"))?;
            source_messages = source_messages.saturating_add(progress.source_messages);
            committed = Some(self.next_ack);
            self.next_ack = self.next_ack.next();
        }
        if let Some(id) = committed {
            self.counters.add_unique_offsets(source_messages);
            events
                .send(SinkEvent::CommittedThrough(id))
                .await
                .map_err(|_| anyhow::anyhow!("sink event receiver closed"))?;
        }
        Ok(())
    }

    async fn run_actor(mut self, mut io: SinkIo) -> anyhow::Result<()> {
        let mut active: Option<JoinHandle<Result<ActiveInsert, InsertFailure>>> = None;
        let mut input_closed = false;
        loop {
            self.emit_committed(&io.events).await?;

            if let Some(mut task) = active.take() {
                let mut completed = None;
                tokio::select! {
                    () = io.cancellation.cancelled() => {
                        task.abort();
                        return Ok(());
                    }
                    result = &mut task => completed = Some(result),
                    delivery = io.deliveries.recv(), if !input_closed => {
                        match delivery {
                            Some(delivery) => self.accept(delivery)?,
                            None => input_closed = true,
                        }
                    }
                }
                if let Some(result) = completed {
                    match result.map_err(|error| {
                        anyhow::anyhow!("ClickHouse insert task failed: {error}")
                    })? {
                        Ok(insert) => self.complete_insert(insert)?,
                        Err(failure) => return Err(failure.error),
                    }
                } else {
                    active = Some(task);
                }
                continue;
            }

            if input_closed && self.buffers.is_empty() {
                self.emit_committed(&io.events).await?;
                anyhow::ensure!(
                    self.progress.is_empty(),
                    "sink input closed with incomplete deliveries"
                );
                return Ok(());
            }

            let memory_pressure = io.memory.used() >= io.memory.limit();
            let Some((table, deadline)) = self.next_flush(input_closed, memory_pressure) else {
                tokio::select! {
                    () = io.cancellation.cancelled() => return Ok(()),
                    delivery = io.deliveries.recv(), if !input_closed => {
                        match delivery {
                            Some(delivery) => self.accept(delivery)?,
                            None => input_closed = true,
                        }
                    }
                }
                continue;
            };

            tokio::select! {
                () = io.cancellation.cancelled() => return Ok(()),
                delivery = io.deliveries.recv(), if !input_closed => {
                    match delivery {
                        Some(delivery) => self.accept(delivery)?,
                        None => input_closed = true,
                    }
                }
                () = tokio::time::sleep_until(deadline) => {
                    let insert = self.take_insert(&table)?;
                    active = Some(self.start_insert(insert, io.cancellation.clone()));
                }
            }
        }
    }
}

impl Sink for ClickHouseSink {
    fn run(self: Box<Self>, io: SinkIo) -> BoxFuture<'static, anyhow::Result<()>> {
        Box::pin(async move { self.run_actor(io).await })
    }
}

pub struct ClickHouseAdmin {
    pool: ConnectionPool<ArrowFormat>,
}

impl ClickHouseAdmin {
    pub async fn connect(config: &ClickHouseSinkConfig) -> anyhow::Result<Self> {
        let pool = build_pool(config).await?;
        let client = pool
            .get()
            .await
            .map_err(|error| anyhow::anyhow!("ClickHouse admin connection failed: {error}"))?;
        client
            .execute("SELECT 1", None)
            .await
            .map_err(|error| anyhow::anyhow!("ClickHouse admin health check failed: {error}"))?;
        drop(client);
        Ok(Self { pool })
    }

    pub async fn create_table(
        &self,
        name: &str,
        columns: &[(String, String)],
        sorting_key: &[String],
        recreate: bool,
    ) -> anyhow::Result<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|error| anyhow::anyhow!("ClickHouse admin pool get: {error}"))?;
        if recreate {
            tracing::warn!(table = name, "dropping table before recreation");
            client
                .execute(&format!("DROP TABLE IF EXISTS `{name}`"), None)
                .await
                .map_err(|error| anyhow::anyhow!("Failed to drop table '{name}': {error}"))?;
        }
        let columns = columns
            .iter()
            .map(|(column, ty)| format!("`{column}` {ty}"))
            .collect::<Vec<_>>()
            .join(", ");
        let order = if sorting_key.is_empty() {
            "tuple()".to_string()
        } else {
            sorting_key
                .iter()
                .map(|column| format!("`{column}`"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS `{name}` ({columns}) ENGINE = MergeTree ORDER BY ({order})",
        );
        client
            .execute(&ddl, None)
            .await
            .map_err(|error| anyhow::anyhow!("Failed to create table '{name}': {error}"))?;
        Ok(())
    }
}

pub(super) fn schema_columns(cols: &[SchemaColumn]) -> anyhow::Result<Vec<(String, String)>> {
    cols.iter()
        .map(|column| {
            let mut ty = arrow_to_clickhouse(&column.data_type)?;
            if column.nullable {
                ty = format!("Nullable({ty})");
            }
            Ok((column.name.clone(), ty))
        })
        .collect()
}

fn arrow_to_clickhouse(data_type: &DataType) -> anyhow::Result<String> {
    Ok(match data_type {
        DataType::Utf8 | DataType::LargeUtf8 => "String".into(),
        DataType::Int8 => "Int8".into(),
        DataType::Int16 => "Int16".into(),
        DataType::Int32 => "Int32".into(),
        DataType::Int64 => "Int64".into(),
        DataType::UInt8 => "UInt8".into(),
        DataType::UInt16 => "UInt16".into(),
        DataType::UInt32 => "UInt32".into(),
        DataType::UInt64 => "UInt64".into(),
        DataType::Float32 => "Float32".into(),
        DataType::Float64 => "Float64".into(),
        DataType::Boolean => "Bool".into(),
        DataType::Date32 => "Date32".into(),
        DataType::Date64 => "DateTime64(3)".into(),
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => "DateTime".into(),
            TimeUnit::Millisecond => "DateTime64(3)".into(),
            TimeUnit::Microsecond => "DateTime64(6)".into(),
            TimeUnit::Nanosecond => "DateTime64(9)".into(),
        },
        other => anyhow::bail!("No ClickHouse type mapping for Arrow type {other:?}"),
    })
}

#[cfg(test)]
mod tests {
    use core::sync::atomic::{AtomicUsize, Ordering};
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use arrow::array::Int64Array;
    use arrow::datatypes::{Field, Schema};
    use tokio::sync::{mpsc, Notify, Semaphore};
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::pipeline::memory::PipelineMemory;
    use crate::pipeline::sink::{DeliveryMeta, SinkIo};

    #[derive(Clone, Copy)]
    enum Plan {
        Success,
        Transient,
        Permanent,
    }

    struct FakeState {
        calls: AtomicUsize,
        active: AtomicUsize,
        max_active: AtomicUsize,
        plans: Mutex<VecDeque<Plan>>,
        gate: Semaphore,
        block: bool,
        started: Notify,
    }

    struct FakeTransport {
        state: Arc<FakeState>,
    }

    impl FakeTransport {
        fn new(block: bool, plans: impl IntoIterator<Item = Plan>) -> (Arc<Self>, Arc<FakeState>) {
            let state = Arc::new(FakeState {
                calls: AtomicUsize::new(0),
                active: AtomicUsize::new(0),
                max_active: AtomicUsize::new(0),
                plans: Mutex::new(plans.into_iter().collect()),
                gate: Semaphore::new(0),
                block,
                started: Notify::new(),
            });
            (
                Arc::new(Self {
                    state: Arc::clone(&state),
                }),
                state,
            )
        }
    }

    struct ActiveGuard(Arc<FakeState>);
    impl Drop for ActiveGuard {
        fn drop(&mut self) {
            self.0.active.fetch_sub(1, Ordering::AcqRel);
        }
    }

    impl InsertTransport for FakeTransport {
        fn insert(
            &self,
            _table: Arc<str>,
            _batches: Vec<RecordBatch>,
        ) -> BoxFuture<'static, Result<(), InsertError>> {
            let state = Arc::clone(&self.state);
            Box::pin(async move {
                state.calls.fetch_add(1, Ordering::AcqRel);
                let active = state.active.fetch_add(1, Ordering::AcqRel) + 1;
                state.max_active.fetch_max(active, Ordering::AcqRel);
                let _guard = ActiveGuard(Arc::clone(&state));
                state.started.notify_waiters();
                if state.block {
                    state
                        .gate
                        .acquire()
                        .await
                        .expect("test gate closed")
                        .forget();
                }
                let plan = state
                    .plans
                    .lock()
                    .expect("plans poisoned")
                    .pop_front()
                    .unwrap_or(Plan::Success);
                match plan {
                    Plan::Success => Ok(()),
                    Plan::Transient => Err(InsertError::Transient(anyhow::anyhow!("temporary"))),
                    Plan::Permanent => Err(InsertError::Permanent(anyhow::anyhow!("permanent"))),
                }
            })
        }
    }

    fn config() -> ClickHouseSinkConfig {
        ClickHouseSinkConfig {
            connection_string: "unused".into(),
            database: "default".into(),
            username: "default".into(),
            password: String::new(),
            max_insert_rows: 1,
            max_insert_bytes: usize::MAX,
            flush_interval_ms: 100,
            retry_initial_ms: 10,
            retry_max_ms: 100,
            retry_max_attempts: None,
            use_tls: false,
            tls_domain: None,
            sorting_key: Vec::new(),
            recreate_tables: false,
        }
    }

    #[test]
    fn clickhouse_owns_table_policy_config() -> anyhow::Result<()> {
        let config: ClickHouseSinkConfig = serde_yaml::from_str(
            "connection_string: localhost:9000\nsorting_key: [id]\nrecreate_tables: true\n",
        )?;
        anyhow::ensure!(config.sorting_key == ["id"]);
        anyhow::ensure!(config.recreate_tables);
        Ok(())
    }

    #[test]
    fn rejects_old_order_by_name() {
        let result = serde_yaml::from_str::<ClickHouseSinkConfig>(
            "connection_string: localhost:9000\norder_by: [id]\n",
        );
        assert!(result.is_err());
    }

    async fn delivery(memory: &PipelineMemory, id: u64, tables: &[&str]) -> Delivery {
        let mut outputs = Vec::new();
        for table in tables {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )]));
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(Int64Array::from(vec![id.cast_signed()]))],
            )
            .unwrap();
            let bytes = batch.get_array_memory_size();
            outputs.push(SinkBatch {
                table: Arc::from(*table),
                batch,
                byte_size: bytes,
                memory: memory.reserve(bytes).await,
            });
        }
        Delivery {
            id: DeliveryId::new(id),
            outputs,
            meta: DeliveryMeta {
                source_messages: 1,
                ..DeliveryMeta::default()
            },
        }
    }

    fn spawn_sink(
        transport: Arc<dyn InsertTransport>,
        memory: PipelineMemory,
        counters: Arc<SinkCounters>,
    ) -> (
        mpsc::Sender<Delivery>,
        mpsc::Receiver<SinkEvent>,
        CancellationToken,
        JoinHandle<anyhow::Result<()>>,
    ) {
        spawn_sink_with_config(config(), transport, memory, counters)
    }

    fn spawn_sink_with_config(
        config: ClickHouseSinkConfig,
        transport: Arc<dyn InsertTransport>,
        memory: PipelineMemory,
        counters: Arc<SinkCounters>,
    ) -> (
        mpsc::Sender<Delivery>,
        mpsc::Receiver<SinkEvent>,
        CancellationToken,
        JoinHandle<anyhow::Result<()>>,
    ) {
        let sink = ClickHouseSink::with_transport(config, counters, transport);
        let (delivery_tx, delivery_rx) = mpsc::channel(8);
        let (event_tx, event_rx) = mpsc::channel(8);
        let cancellation = CancellationToken::new();
        let io = SinkIo {
            deliveries: delivery_rx,
            events: event_tx,
            memory,
            cancellation: cancellation.clone(),
        };
        let task = tokio::spawn(Box::new(sink).run(io));
        (delivery_tx, event_rx, cancellation, task)
    }

    async fn wait_calls(state: &FakeState, calls: usize) {
        while state.calls.load(Ordering::Acquire) < calls {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn buffers_next_delivery_while_exactly_one_insert_runs() {
        let memory = PipelineMemory::new(1_000_000);
        let counters = Arc::new(SinkCounters::new());
        let (transport, state) = FakeTransport::new(true, []);
        let (tx, mut events, cancellation, task) =
            spawn_sink(transport, memory.clone(), Arc::clone(&counters));

        tx.send(delivery(&memory, 1, &["events"]).await)
            .await
            .unwrap();
        wait_calls(&state, 1).await;
        tx.send(delivery(&memory, 2, &["events"]).await)
            .await
            .unwrap();
        tokio::task::yield_now().await;
        assert_eq!(state.calls.load(Ordering::Acquire), 1);
        assert_eq!(state.max_active.load(Ordering::Acquire), 1);

        state.gate.add_permits(1);
        assert_eq!(
            events.recv().await,
            Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
        );
        tokio::time::advance(Duration::from_millis(99)).await;
        tokio::task::yield_now().await;
        assert_eq!(state.calls.load(Ordering::Acquire), 1);
        tokio::time::advance(Duration::from_millis(1)).await;
        wait_calls(&state, 2).await;
        state.gate.add_permits(1);
        assert_eq!(
            events.recv().await,
            Some(SinkEvent::CommittedThrough(DeliveryId::new(2)))
        );
        assert_eq!(state.max_active.load(Ordering::Acquire), 1);
        assert_eq!(counters.flushes_total(), 2);
        assert_eq!(counters.source_messages_total(), 2);
        cancellation.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn low_volume_delivery_flushes_at_interval() {
        let memory = PipelineMemory::new(1_000_000);
        let counters = Arc::new(SinkCounters::new());
        let (transport, state) = FakeTransport::new(false, []);
        let mut low_volume = config();
        low_volume.max_insert_rows = 1_000;
        let (tx, mut events, cancellation, task) =
            spawn_sink_with_config(low_volume, transport, memory.clone(), Arc::clone(&counters));

        tx.send(delivery(&memory, 1, &["events"]).await)
            .await
            .unwrap();
        tokio::task::yield_now().await;
        assert_eq!(state.calls.load(Ordering::Acquire), 0);
        tokio::time::advance(Duration::from_millis(99)).await;
        tokio::task::yield_now().await;
        assert_eq!(state.calls.load(Ordering::Acquire), 0);
        tokio::time::advance(Duration::from_millis(1)).await;
        wait_calls(&state, 1).await;
        assert_eq!(
            events.recv().await,
            Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
        );
        assert_eq!(counters.flushes_total(), 1);
        cancellation.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn full_pipeline_budget_requests_an_immediate_insert() {
        let memory = PipelineMemory::new(1);
        let counters = Arc::new(SinkCounters::new());
        let (transport, state) = FakeTransport::new(false, []);
        let mut high_targets = config();
        high_targets.max_insert_rows = 1_000;
        let (tx, mut events, cancellation, task) =
            spawn_sink_with_config(high_targets, transport, memory.clone(), counters);

        tx.send(delivery(&memory, 1, &["events"]).await)
            .await
            .unwrap();
        wait_calls(&state, 1).await;
        assert_eq!(
            events.recv().await,
            Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
        );
        cancellation.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn multi_table_delivery_commits_only_after_both_inserts() {
        let memory = PipelineMemory::new(1_000_000);
        let counters = Arc::new(SinkCounters::new());
        let (transport, state) = FakeTransport::new(false, []);
        let (tx, mut events, cancellation, task) = spawn_sink(transport, memory.clone(), counters);
        tx.send(delivery(&memory, 1, &["events", "events_dlq"]).await)
            .await
            .unwrap();
        wait_calls(&state, 1).await;
        tokio::task::yield_now().await;
        assert!(events.try_recv().is_err());
        tokio::time::advance(Duration::from_millis(100)).await;
        wait_calls(&state, 2).await;
        assert_eq!(
            events.recv().await,
            Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
        );
        cancellation.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn transient_error_retries_frozen_insert() {
        let memory = PipelineMemory::new(1_000_000);
        let counters = Arc::new(SinkCounters::new());
        let (transport, state) = FakeTransport::new(false, [Plan::Transient, Plan::Success]);
        let (tx, mut events, cancellation, task) =
            spawn_sink(transport, memory.clone(), Arc::clone(&counters));
        tx.send(delivery(&memory, 1, &["events"]).await)
            .await
            .unwrap();
        wait_calls(&state, 1).await;
        assert!(events.try_recv().is_err());
        tokio::time::advance(Duration::from_millis(10)).await;
        wait_calls(&state, 2).await;
        assert_eq!(
            events.recv().await,
            Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
        );
        assert_eq!(counters.flushes_total(), 1);
        cancellation.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn permanent_error_is_fatal_and_never_commits() {
        let memory = PipelineMemory::new(1_000_000);
        let (transport, _state) = FakeTransport::new(false, [Plan::Permanent]);
        let (tx, mut events, _cancellation, task) =
            spawn_sink(transport, memory.clone(), Arc::new(SinkCounters::new()));
        tx.send(delivery(&memory, 1, &["events"]).await)
            .await
            .unwrap();
        assert!(task.await.unwrap().is_err());
        assert!(events.try_recv().is_err());
    }

    #[tokio::test]
    async fn empty_delivery_commits_without_insert() {
        let memory = PipelineMemory::new(1_000_000);
        let (transport, state) = FakeTransport::new(false, []);
        let (tx, mut events, cancellation, task) =
            spawn_sink(transport, memory, Arc::new(SinkCounters::new()));
        tx.send(Delivery {
            id: DeliveryId::new(1),
            outputs: Vec::new(),
            meta: DeliveryMeta {
                source_messages: 1,
                ..DeliveryMeta::default()
            },
        })
        .await
        .unwrap();
        assert_eq!(
            events.recv().await,
            Some(SinkEvent::CommittedThrough(DeliveryId::new(1)))
        );
        assert_eq!(state.calls.load(Ordering::Acquire), 0);
        cancellation.cancel();
        task.await.unwrap().unwrap();
    }
}
