use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, PoisonError};

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
fn pins_lossless_insert_settings() {
    let builder = configured_builder(&ClickHouseSinkConfig {
        endpoint: "localhost:9000".into(),
        trusted_plaintext: true,
        database: "default".into(),
        username: "default".into(),
        password: String::new(),
        insert_target_rows: 1,
        insert_target_bytes: 1,
        flush_interval_ms: 1,
        retry_initial_ms: 1,
        retry_max_ms: 1,
        retry_max_attempts: Some(1),
        connect_timeout_ms: 1,
        request_timeout_ms: 1,
        sorting_key: Vec::new(),
    });
    let settings = builder.settings().expect("insert settings must be pinned");
    let values = settings.encode_to_key_value_strings();
    assert!(values.contains(&("async_insert".into(), "0".into())));
    assert!(values.contains(&("wait_for_async_insert".into(), "1".into())));
    assert!(values.contains(&("insert_deduplicate".into(), "0".into())));
}
