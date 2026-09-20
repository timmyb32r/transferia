use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn backend(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut message = vec![tag];
    message.extend_from_slice(&i32::try_from(body.len() + 4).unwrap().to_be_bytes());
    message.extend_from_slice(body);
    message
}

async fn startup(socket: &mut TcpStream, backend_id: i32) {
    let length = socket.read_u32().await.unwrap();
    let mut bytes = vec![0; usize::try_from(length - 4).unwrap()];
    socket.read_exact(&mut bytes).await.unwrap();
    socket.write_all(&[
        backend(b'R', &0_i32.to_be_bytes()),
        backend(b'K', &[backend_id.to_be_bytes(), 1357_i32.to_be_bytes()].concat()),
        backend(b'Z', b"I"),
    ].concat()).await.unwrap();
}

async fn cancel_request(listener: &TcpListener) -> i32 {
    let (mut socket, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
        .await.expect("cancelled query did not send a PostgreSQL CancelRequest").unwrap();
    assert_eq!(socket.read_u32().await.unwrap(), 16);
    assert_eq!(socket.read_u32().await.unwrap(), 80_877_102);
    let backend_id = socket.read_i32().await.unwrap();
    assert_eq!(socket.read_i32().await.unwrap(), 1357);
    backend_id
}

async fn simple_query(socket: &mut TcpStream) -> Vec<u8> {
    assert_eq!(socket.read_u8().await.unwrap(), b'Q');
    let length = socket.read_u32().await.unwrap();
    let mut bytes = vec![0; usize::try_from(length - 4).unwrap()];
    socket.read_exact(&mut bytes).await.unwrap();
    bytes
}

async fn begun(socket: &mut TcpStream) -> Vec<u8> {
    let query = simple_query(socket).await;
    assert!(String::from_utf8_lossy(&query).contains("BEGIN TRANSACTION"));
    socket.write_all(&[
        backend(b'C', b"BEGIN\0"),
        backend(b'Z', b"T"),
    ].concat()).await.unwrap();
    query
}

async fn closed(mut socket: TcpStream) {
    tokio::time::timeout(Duration::from_secs(1), async {
        let mut buffer = [0; 1024];
        loop {
            if socket.read(&mut buffer).await.unwrap() == 0 { return; }
        }
    }).await.expect("cancelled snapshot left a PostgreSQL protocol driver alive");
}

fn config(address: std::net::SocketAddr) -> PostgresConnectionConfig {
    PostgresConnectionConfig {
        host: address.ip().to_string(), port: address.port(), database: "snapshot".into(),
        username: "reader".into(), password: String::new(), trusted_plaintext: true,
        tls_ca_file: None,
    }
}

#[tokio::test]
async fn cancelling_a_blocked_guard_sends_cancel_before_closing_the_lock_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(listener.local_addr().unwrap());
    let (pending, observed) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        startup(&mut socket, 41).await;
        drop(begun(&mut socket).await);
        let query = simple_query(&mut socket).await;
        assert!(String::from_utf8_lossy(&query).contains("LOCK TABLE"));
        pending.send(()).unwrap();
        // No command completion: a detached driver would wait forever here.
        assert_eq!(cancel_request(&listener).await, 41);
        closed(socket).await;
    });
    let request = tokio::spawn(async move {
        SnapshotGuard::acquire(&config, &[TableConfig { schema: "public".into(), name: "blocked".into() }], Duration::from_secs(1)).await
    });
    observed.await.unwrap();
    request.abort();
    assert!(matches!(request.await, Err(error) if error.is_cancelled()));
    server.await.unwrap();
}

#[tokio::test]
async fn cancelling_snapshot_export_closes_owner_and_guard_connections() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(listener.local_addr().unwrap());
    let (pending, observed) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut guard, _) = listener.accept().await.unwrap();
        startup(&mut guard, 41).await;
        drop(begun(&mut guard).await);
        let guard_closed = tokio::spawn(closed(guard));
        let (mut owner, _) = listener.accept().await.unwrap();
        startup(&mut owner, 42).await;
        let query = begun(&mut owner).await;
        assert!(String::from_utf8_lossy(&query).contains("transferia_snapshot_owner"));
        // The export query uses the extended protocol. Do not answer its Parse.
        assert_eq!(owner.read_u8().await.unwrap(), b'P');
        pending.send(()).unwrap();
        let mut cancelled = [cancel_request(&listener).await, cancel_request(&listener).await];
        cancelled.sort();
        assert_eq!(cancelled, [41, 42]);
        closed(owner).await;
        guard_closed.await.unwrap();
    });
    let request = tokio::spawn(async move { ExportedSnapshot::create(&config, &[], Duration::from_secs(1)).await });
    observed.await.unwrap();
    request.abort();
    assert!(matches!(request.await, Err(error) if error.is_cancelled()));
    server.await.unwrap();
}

#[tokio::test]
async fn explicit_cancel_interrupts_an_abandoned_query_before_driver_shutdown() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(listener.local_addr().unwrap());
    let (pending, observed) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        startup(&mut socket, 43).await;
        drop(simple_query(&mut socket).await);
        pending.send(()).unwrap();
        assert_eq!(cancel_request(&listener).await, 43);
        closed(socket).await;
    });
    let mut client = connect_owned(&config).await.unwrap()
        .with_cancellation_timeout(Duration::from_secs(1)).unwrap();
    {
        let query = client.batch_execute("SELECT pg_sleep(60)");
        tokio::pin!(query);
        tokio::select! {
            result = &mut query => panic!("fixture must not complete the query: {result:?}"),
            started = observed => started.unwrap(),
        }
    }
    client.cancel().await.unwrap();
    client.cancel().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn zero_cancellation_deadline_is_rejected_before_connecting() {
    let config = config("127.0.0.1:1".parse().unwrap());
    let error = match SnapshotGuard::acquire(&config, &[], Duration::ZERO).await {
        Ok(_) => panic!("zero cancellation deadline unexpectedly accepted"),
        Err(error) => error,
    };
    assert_eq!(error.to_string(), "PostgreSQL snapshot cancellation timeout must be positive");
}
