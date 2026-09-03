#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    reason = "protocol tests intentionally fail fast"
)]

use std::future;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut as _, BytesMut};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _, DuplexStream};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::{
    bootstrap_session, parse_consistent_lsn, run_bounded, AmbiguousReplicationSlotCreation,
    ReplicationSlotBootstrap,
};
use crate::connectors::postgres::common::PostgresConnectionConfig;
use crate::connectors::postgres::src_stream::PostgresSystemIdentity;

const SLOT: &str = "transferia_slot";
const PLUGIN: &str = "pgoutput";
const SNAPSHOT: &str = "00000003-0000001B-1";
const LSN: &str = "16/B374D848";
const SYSTEM_IDENTIFIER: u64 = 7_412_345_678_901_234_567;
const SYSTEM_IDENTIFIER_TEXT: &str = "7412345678901234567";

#[tokio::test]
async fn trust_authentication_sends_replication_startup_and_preserves_owner_lifetime() {
    let (client, mut server) = tokio::io::duplex(16 * 1024);
    let (closed_sender, mut closed_receiver) = oneshot::channel();
    let server_task = tokio::spawn(async move {
        assert_replication_startup(&mut server, "alice", "inventory").await;
        server.write_all(&startup_success()).await.unwrap();
        assert_slot_query(&mut server).await;
        server.write_all(&valid_slot_response()).await.unwrap();
        let mut byte = [0_u8; 1];
        assert_eq!(server.read(&mut byte).await.unwrap(), 0);
        closed_sender.send(()).unwrap();
    });

    let bootstrap = bootstrap_session(
        Box::new(client),
        "alice",
        "secret",
        "inventory",
        SLOT,
        PLUGIN,
        &expected_system(),
        request_marker(),
    )
    .await
    .unwrap();
    assert_bootstrap(&bootstrap);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut closed_receiver)
            .await
            .is_err(),
        "the owner session closed while its bootstrap value was alive"
    );
    drop(bootstrap);
    tokio::time::timeout(Duration::from_secs(1), closed_receiver)
        .await
        .unwrap()
        .unwrap();
    server_task.await.unwrap();
}

#[tokio::test]
async fn cleartext_authentication_sends_only_the_configured_password() {
    let (client, mut server) = tokio::io::duplex(16 * 1024);
    let server_task = tokio::spawn(async move {
        read_startup(&mut server).await;
        server.write_all(&authentication(3, &[])).await.unwrap();
        let (tag, body) = read_tagged(&mut server).await;
        assert_eq!(tag, b'p');
        assert_eq!(body, b"secret\0");
        server.write_all(&startup_success()).await.unwrap();
        assert_slot_query(&mut server).await;
        server.write_all(&valid_slot_response()).await.unwrap();
    });

    let bootstrap = bootstrap_session(
        Box::new(client),
        "alice",
        "secret",
        "inventory",
        SLOT,
        PLUGIN,
        &expected_system(),
        request_marker(),
    )
    .await
    .unwrap();
    assert_bootstrap(&bootstrap);
    drop(bootstrap);
    server_task.await.unwrap();
}

#[tokio::test]
async fn md5_authentication_uses_user_password_and_server_salt() {
    let salt = [1_u8, 2, 3, 4];
    let (client, mut server) = tokio::io::duplex(16 * 1024);
    let server_task = tokio::spawn(async move {
        read_startup(&mut server).await;
        server.write_all(&authentication(5, &salt)).await.unwrap();
        let (tag, body) = read_tagged(&mut server).await;
        assert_eq!(tag, b'p');
        let expected = postgres_protocol::authentication::md5_hash(b"alice", b"secret", salt);
        assert_eq!(body, [expected.as_bytes(), b"\0"].concat());
        server.write_all(&startup_success()).await.unwrap();
        assert_slot_query(&mut server).await;
        server.write_all(&valid_slot_response()).await.unwrap();
    });

    let bootstrap = bootstrap_session(
        Box::new(client),
        "alice",
        "secret",
        "inventory",
        SLOT,
        PLUGIN,
        &expected_system(),
        request_marker(),
    )
    .await
    .unwrap();
    assert_bootstrap(&bootstrap);
    drop(bootstrap);
    server_task.await.unwrap();
}

#[tokio::test]
async fn scram_path_never_sends_the_plaintext_password_and_surfaces_server_error() {
    let (client, mut server) = tokio::io::duplex(16 * 1024);
    let server_task = tokio::spawn(async move {
        read_startup(&mut server).await;
        let mut mechanisms = BytesMut::new();
        mechanisms.put_slice(b"SCRAM-SHA-256\0\0");
        server
            .write_all(&authentication(10, &mechanisms))
            .await
            .unwrap();
        let (tag, body) = read_tagged(&mut server).await;
        assert_eq!(tag, b'p');
        assert!(body.starts_with(b"SCRAM-SHA-256\0"));
        assert!(!body
            .windows(b"secret".len())
            .any(|value| value == b"secret"));
        server
            .write_all(&error_response("28P01", "authentication rejected"))
            .await
            .unwrap();
    });

    let error = match bootstrap_session(
        Box::new(client),
        "alice",
        "secret",
        "inventory",
        SLOT,
        PLUGIN,
        &expected_system(),
        request_marker(),
    )
    .await
    {
        Ok(_) => panic!("rejected SCRAM authentication unexpectedly succeeded"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("28P01"));
    assert!(!error.to_string().contains("authentication rejected"));
    assert!(!error.to_string().contains("secret"));
    server_task.await.unwrap();
}

#[tokio::test]
async fn response_contract_rejects_wrong_columns_nulls_duplicates_and_identity_mismatch() {
    let cases = [
        slot_response(
            &["slot_name", "consistent_point", "snapshot_name"],
            &[Some(SLOT), Some(LSN), Some(SNAPSHOT)],
            1,
            "CREATE_REPLICATION_SLOT",
        ),
        slot_response(
            &["slot_name", "consistent_point", "wrong", "output_plugin"],
            &[Some(SLOT), Some(LSN), Some(SNAPSHOT), Some(PLUGIN)],
            1,
            "CREATE_REPLICATION_SLOT",
        ),
        slot_response(
            &[
                "slot_name",
                "consistent_point",
                "snapshot_name",
                "output_plugin",
            ],
            &[Some(SLOT), Some(LSN), None, Some(PLUGIN)],
            1,
            "CREATE_REPLICATION_SLOT",
        ),
        slot_response(
            &[
                "slot_name",
                "consistent_point",
                "snapshot_name",
                "output_plugin",
            ],
            &[Some(SLOT), Some(LSN), Some(SNAPSHOT), Some(PLUGIN)],
            2,
            "CREATE_REPLICATION_SLOT",
        ),
        slot_response(
            &[
                "slot_name",
                "consistent_point",
                "snapshot_name",
                "output_plugin",
            ],
            &[Some("other_slot"), Some(LSN), Some(SNAPSHOT), Some(PLUGIN)],
            1,
            "CREATE_REPLICATION_SLOT",
        ),
        slot_response(
            &[
                "slot_name",
                "consistent_point",
                "snapshot_name",
                "output_plugin",
            ],
            &[Some(SLOT), Some(LSN), Some(SNAPSHOT), Some(PLUGIN)],
            1,
            "SELECT 1",
        ),
    ];

    for response in cases {
        let error = run_scripted_response(response).await.unwrap_err();
        assert!(
            error
                .downcast_ref::<AmbiguousReplicationSlotCreation>()
                .is_some(),
            "invalid response was not marked as an ambiguous permanent-slot result: {error:#}"
        );
    }
}

#[tokio::test]
async fn response_contract_rejects_invalid_lsn_snapshot_and_plugin() {
    for values in [
        [Some(SLOT), Some("not-an-lsn"), Some(SNAPSHOT), Some(PLUGIN)],
        [
            Some(SLOT),
            Some("100000000/0"),
            Some(SNAPSHOT),
            Some(PLUGIN),
        ],
        [
            Some(SLOT),
            Some("0/100000000"),
            Some(SNAPSHOT),
            Some(PLUGIN),
        ],
        [Some(SLOT), Some(LSN), Some("unsafe'snapshot"), Some(PLUGIN)],
        [Some(SLOT), Some(LSN), Some(SNAPSHOT), Some("wal2json")],
    ] {
        let response = slot_response(
            &[
                "slot_name",
                "consistent_point",
                "snapshot_name",
                "output_plugin",
            ],
            &values,
            1,
            "CREATE_REPLICATION_SLOT",
        );
        let error = run_scripted_response(response).await.unwrap_err();
        assert!(
            error
                .downcast_ref::<AmbiguousReplicationSlotCreation>()
                .is_some(),
            "invalid response was not marked as an ambiguous permanent-slot result: {error:#}"
        );
    }
}

#[tokio::test]
async fn identify_system_drift_and_malformed_identity_fail_before_slot_creation() {
    let columns = [
        ("systemid", 25, -1),
        ("timeline", 20, 8),
        ("xlogpos", 25, -1),
        ("dbname", 25, -1),
    ];
    for response in [
        identify_response(
            &columns,
            &[
                Some("7412345678901234568"),
                Some("1"),
                Some(LSN),
                Some("inventory"),
            ],
            1,
            "IDENTIFY_SYSTEM",
        ),
        identify_response(
            &columns,
            &[
                Some(SYSTEM_IDENTIFIER_TEXT),
                Some("1"),
                Some(LSN),
                Some("replacement"),
            ],
            1,
            "IDENTIFY_SYSTEM",
        ),
        identify_response(
            &columns,
            &[
                Some("not-a-system-id"),
                Some("1"),
                Some(LSN),
                Some("inventory"),
            ],
            1,
            "IDENTIFY_SYSTEM",
        ),
    ] {
        let (error, request_was_sent) = run_scripted_identity_response(response).await;
        assert!(!request_was_sent, "CREATE_REPLICATION_SLOT was marked sent");
        assert!(
            error
                .downcast_ref::<AmbiguousReplicationSlotCreation>()
                .is_none(),
            "identity rejection was incorrectly classified as ambiguous: {error:#}"
        );
    }
}

#[test]
fn consistent_lsn_parser_preserves_the_full_unsigned_range_without_truncation() {
    assert_eq!(parse_consistent_lsn("0/0").unwrap(), 0);
    assert_eq!(parse_consistent_lsn("FFFFFFFF/FFFFFFFF").unwrap(), u64::MAX);
    for invalid in [
        "",
        "/",
        "0/",
        "/0",
        "0/0/0",
        "-1/0",
        "0/-1",
        "100000000/0",
        "0/100000000",
    ] {
        assert!(parse_consistent_lsn(invalid).is_err(), "accepted {invalid}");
    }
}

#[tokio::test]
async fn explicit_server_rejection_is_not_reported_as_an_ambiguous_creation() {
    let error = run_scripted_response(error_response(
        "42710",
        "replication slot already exists; secret must be redacted",
    ))
    .await
    .unwrap_err();
    assert!(error
        .downcast_ref::<AmbiguousReplicationSlotCreation>()
        .is_none());
    assert!(error.to_string().contains("42710"));
    assert!(!format!("{error:#}").contains("secret must be redacted"));
}

#[tokio::test]
async fn malformed_server_diagnostics_cannot_inject_credentials_into_the_error() {
    let error = run_scripted_response(error_response(
        "secret\n",
        "password=credential-that-must-not-escape",
    ))
    .await
    .unwrap_err();
    let diagnostic = format!("{error:#}");
    assert!(diagnostic.contains("SQLSTATE unknown"));
    assert!(!diagnostic.contains("secret"));
    assert!(!diagnostic.contains("credential-that-must-not-escape"));
}

#[tokio::test]
async fn bounded_operation_honors_cancellation_before_polling_the_request() {
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let error = run_bounded(
        &cancellation,
        Duration::from_secs(1),
        future::ready(Ok::<_, anyhow::Error>(())),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("cancelled"));
}

#[tokio::test]
async fn bounded_operation_honors_the_supplied_timeout_without_a_hidden_limit() {
    let error = run_bounded(
        &CancellationToken::new(),
        Duration::from_millis(10),
        future::pending::<anyhow::Result<()>>(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("timed out after 10 ms"));
}

#[tokio::test]
async fn create_cancellation_closes_a_blocked_bootstrap_connection() {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let (accepted_sender, accepted_receiver) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        accepted_sender.send(()).unwrap();
        let mut request = Vec::new();
        stream.read_to_end(&mut request).await.unwrap();
        assert!(!request.is_empty(), "client never sent its startup packet");
    });
    let cancellation = CancellationToken::new();
    let config = connection_config(port, true);
    let expected = expected_system();
    let create = ReplicationSlotBootstrap::create(
        &config,
        SLOT,
        PLUGIN,
        &expected,
        &cancellation,
        Duration::from_secs(1),
    );
    tokio::pin!(create);
    tokio::select! {
        result = &mut create => panic!("blocked bootstrap returned before cancellation: {result:?}"),
        result = accepted_receiver => result.unwrap(),
    }
    cancellation.cancel();
    let error = create.await.unwrap_err();
    assert!(error.to_string().contains("cancelled"));
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn create_timeout_closes_a_blocked_bootstrap_connection() {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        stream.read_to_end(&mut request).await.unwrap();
        assert!(!request.is_empty(), "client never sent its startup packet");
    });
    let error = ReplicationSlotBootstrap::create(
        &connection_config(port, true),
        SLOT,
        PLUGIN,
        &expected_system(),
        &CancellationToken::new(),
        Duration::from_millis(250),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("timed out after 250 ms"));
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn cancellation_after_query_is_an_explicit_ambiguous_permanent_slot_failure() {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let (query_sender, query_receiver) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_tcp_startup(&mut stream).await;
        stream.write_all(&startup_success()).await.unwrap();
        assert_tcp_slot_query(&mut stream).await;
        query_sender.send(()).unwrap();
        let mut byte = [0_u8; 1];
        assert_eq!(stream.read(&mut byte).await.unwrap(), 0);
    });
    let cancellation = CancellationToken::new();
    let config = connection_config(port, true);
    let expected = expected_system();
    let create = ReplicationSlotBootstrap::create(
        &config,
        SLOT,
        PLUGIN,
        &expected,
        &cancellation,
        Duration::from_secs(1),
    );
    tokio::pin!(create);
    tokio::select! {
        result = &mut create => panic!("bootstrap returned before cancellation: {result:?}"),
        result = query_receiver => result.unwrap(),
    }
    cancellation.cancel();
    let error = create.await.unwrap_err();
    assert!(error
        .downcast_ref::<AmbiguousReplicationSlotCreation>()
        .is_some());
    assert!(error.to_string().contains("requires a deliberate reset"));
    assert!(format!("{error:#}").contains("cancelled"));
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn timeout_after_query_is_an_explicit_ambiguous_permanent_slot_failure() {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_tcp_startup(&mut stream).await;
        stream.write_all(&startup_success()).await.unwrap();
        assert_tcp_slot_query(&mut stream).await;
        let mut byte = [0_u8; 1];
        assert_eq!(stream.read(&mut byte).await.unwrap(), 0);
    });
    let error = ReplicationSlotBootstrap::create(
        &connection_config(port, true),
        SLOT,
        PLUGIN,
        &expected_system(),
        &CancellationToken::new(),
        Duration::from_millis(250),
    )
    .await
    .unwrap_err();
    assert!(error
        .downcast_ref::<AmbiguousReplicationSlotCreation>()
        .is_some());
    assert!(error.to_string().contains("requires a deliberate reset"));
    assert!(format!("{error:#}").contains("timed out after 250 ms"));
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn tls_refusal_is_not_downgraded_to_plaintext() {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0_u8; 8];
        stream.read_exact(&mut request).await.unwrap();
        assert_eq!(i32::from_be_bytes(request[..4].try_into().unwrap()), 8);
        assert_eq!(
            i32::from_be_bytes(request[4..].try_into().unwrap()),
            80_877_103
        );
        stream.write_all(b"N").await.unwrap();
    });
    let config = connection_config(port, false);
    let error = match ReplicationSlotBootstrap::create(
        &config,
        SLOT,
        PLUGIN,
        &expected_system(),
        &CancellationToken::new(),
        Duration::from_secs(1),
    )
    .await
    {
        Ok(_) => panic!("TLS refusal unexpectedly downgraded to plaintext"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("refused required TLS"));
    server.await.unwrap();
}

#[tokio::test]
async fn invalid_identifiers_and_zero_timeout_fail_before_opening_a_socket() {
    let config = connection_config(1, true);
    for (slot, plugin, timeout) in [
        ("slot;DROP_TABLE", PLUGIN, Duration::from_secs(1)),
        (SLOT, "plugin-name", Duration::from_secs(1)),
        (SLOT, PLUGIN, Duration::ZERO),
    ] {
        let error = match ReplicationSlotBootstrap::create(
            &config,
            slot,
            plugin,
            &expected_system(),
            &CancellationToken::new(),
            timeout,
        )
        .await
        {
            Ok(_) => panic!("invalid bootstrap input unexpectedly opened a session"),
            Err(error) => error,
        };
        assert!(
            !error.to_string().contains("connect"),
            "input validation ran after the network side effect: {error}"
        );
    }

    for config in [
        PostgresConnectionConfig {
            username: "alice\0other".to_owned(),
            ..connection_config(1, true)
        },
        PostgresConnectionConfig {
            database: "inventory\0other".to_owned(),
            ..connection_config(1, true)
        },
        PostgresConnectionConfig {
            password: "secret\0other".to_owned(),
            ..connection_config(1, true)
        },
    ] {
        let error = ReplicationSlotBootstrap::create(
            &config,
            SLOT,
            PLUGIN,
            &expected_system(),
            &CancellationToken::new(),
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        assert!(!error.to_string().contains("secret"));
        assert!(!error.to_string().contains("connect"));
    }
}

async fn run_scripted_response(response: Vec<u8>) -> anyhow::Result<ReplicationSlotBootstrap> {
    let (client, mut server) = tokio::io::duplex(16 * 1024);
    let server_task = tokio::spawn(async move {
        read_startup(&mut server).await;
        server.write_all(&startup_success()).await.unwrap();
        assert_slot_query(&mut server).await;
        server.write_all(&response).await.unwrap();
    });
    let result = bootstrap_session(
        Box::new(client),
        "alice",
        "secret",
        "inventory",
        SLOT,
        PLUGIN,
        &expected_system(),
        request_marker(),
    )
    .await;
    server_task.await.unwrap();
    result
}

async fn run_scripted_identity_response(response: Vec<u8>) -> (anyhow::Error, bool) {
    let (client, mut server) = tokio::io::duplex(16 * 1024);
    let server_task = tokio::spawn(async move {
        read_startup(&mut server).await;
        server.write_all(&startup_success()).await.unwrap();
        let (tag, body) = read_tagged(&mut server).await;
        assert_eq!(tag, b'Q');
        assert_eq!(body, b"IDENTIFY_SYSTEM\0");
        server.write_all(&response).await.unwrap();
        let mut byte = [0_u8; 1];
        assert_eq!(server.read(&mut byte).await.unwrap(), 0);
    });
    let request_marker = request_marker();
    let error = match bootstrap_session(
        Box::new(client),
        "alice",
        "secret",
        "inventory",
        SLOT,
        PLUGIN,
        &expected_system(),
        Arc::clone(&request_marker),
    )
    .await
    {
        Ok(_) => panic!("invalid PostgreSQL identity unexpectedly created a slot"),
        Err(error) => error,
    };
    server_task.await.unwrap();
    (
        error,
        request_marker.load(std::sync::atomic::Ordering::Acquire),
    )
}

fn request_marker() -> Arc<AtomicBool> {
    Arc::new(AtomicBool::new(false))
}

fn assert_bootstrap(bootstrap: &ReplicationSlotBootstrap) {
    assert_eq!(bootstrap.slot, SLOT);
    assert_eq!(bootstrap.consistent_lsn, 0x16_B374_D848);
    assert_eq!(bootstrap.snapshot, SNAPSHOT);
    assert_eq!(bootstrap.plugin, PLUGIN);
}

async fn assert_replication_startup(
    stream: &mut DuplexStream,
    expected_user: &str,
    expected_database: &str,
) {
    let body = read_startup(stream).await;
    assert_eq!(i32::from_be_bytes(body[..4].try_into().unwrap()), 196_608);
    let values = body[4..]
        .split(|byte| *byte == 0)
        .take_while(|value| !value.is_empty())
        .map(|value| std::str::from_utf8(value).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        values,
        [
            "client_encoding",
            "UTF8",
            "user",
            expected_user,
            "database",
            expected_database,
            "replication",
            "database",
        ]
    );
}

async fn read_startup(stream: &mut DuplexStream) -> Vec<u8> {
    let mut length = [0_u8; 4];
    stream.read_exact(&mut length).await.unwrap();
    let length = usize::try_from(i32::from_be_bytes(length)).unwrap();
    assert!(length >= 8);
    let mut body = vec![0_u8; length - 4];
    stream.read_exact(&mut body).await.unwrap();
    body
}

async fn read_tcp_startup(stream: &mut tokio::net::TcpStream) {
    let mut length = [0_u8; 4];
    stream.read_exact(&mut length).await.unwrap();
    let length = usize::try_from(i32::from_be_bytes(length)).unwrap();
    assert!(length >= 8);
    let mut body = vec![0_u8; length - 4];
    stream.read_exact(&mut body).await.unwrap();
}

async fn assert_slot_query(stream: &mut DuplexStream) {
    let (tag, body) = read_tagged(stream).await;
    assert_eq!(tag, b'Q');
    assert_eq!(body, b"IDENTIFY_SYSTEM\0");
    stream.write_all(&valid_identify_response()).await.unwrap();
    let (tag, body) = read_tagged(stream).await;
    assert_eq!(tag, b'Q');
    assert_eq!(
        body,
        format!("CREATE_REPLICATION_SLOT \"{SLOT}\" LOGICAL \"{PLUGIN}\" EXPORT_SNAPSHOT\0")
            .as_bytes()
    );
}

async fn assert_tcp_slot_query(stream: &mut tokio::net::TcpStream) {
    let tag = stream.read_u8().await.unwrap();
    let length = usize::try_from(stream.read_i32().await.unwrap()).unwrap();
    assert!(length >= 4);
    let mut body = vec![0_u8; length - 4];
    stream.read_exact(&mut body).await.unwrap();
    assert_eq!(tag, b'Q');
    assert_eq!(body, b"IDENTIFY_SYSTEM\0");
    stream.write_all(&valid_identify_response()).await.unwrap();

    let tag = stream.read_u8().await.unwrap();
    let length = usize::try_from(stream.read_i32().await.unwrap()).unwrap();
    assert!(length >= 4);
    let mut body = vec![0_u8; length - 4];
    stream.read_exact(&mut body).await.unwrap();
    assert_eq!(tag, b'Q');
    assert_eq!(
        body,
        format!("CREATE_REPLICATION_SLOT \"{SLOT}\" LOGICAL \"{PLUGIN}\" EXPORT_SNAPSHOT\0")
            .as_bytes()
    );
}

async fn read_tagged(stream: &mut DuplexStream) -> (u8, Vec<u8>) {
    let tag = stream.read_u8().await.unwrap();
    let length = usize::try_from(stream.read_i32().await.unwrap()).unwrap();
    assert!(length >= 4);
    let mut body = vec![0_u8; length - 4];
    stream.read_exact(&mut body).await.unwrap();
    (tag, body)
}

fn startup_success() -> Vec<u8> {
    let mut response = authentication(0, &[]);
    response.extend(backend_message(b'S', b"client_encoding\0UTF8\0"));
    let mut key = BytesMut::new();
    key.put_i32(42);
    key.put_i32(84);
    response.extend(backend_message(b'K', &key));
    response.extend(backend_message(b'Z', b"I"));
    response
}

fn valid_slot_response() -> Vec<u8> {
    slot_response(
        &[
            "slot_name",
            "consistent_point",
            "snapshot_name",
            "output_plugin",
        ],
        &[Some(SLOT), Some(LSN), Some(SNAPSHOT), Some(PLUGIN)],
        1,
        "CREATE_REPLICATION_SLOT",
    )
}

fn valid_identify_response() -> Vec<u8> {
    identify_response(
        &[
            ("systemid", 25, -1),
            ("timeline", 20, 8),
            ("xlogpos", 25, -1),
            ("dbname", 25, -1),
        ],
        &[
            Some(SYSTEM_IDENTIFIER_TEXT),
            Some("1"),
            Some(LSN),
            Some("inventory"),
        ],
        1,
        "IDENTIFY_SYSTEM",
    )
}

fn identify_response(
    columns: &[(&str, u32, i16)],
    values: &[Option<&str>],
    row_count: usize,
    command: &str,
) -> Vec<u8> {
    let mut response = typed_row_description(columns);
    for _ in 0..row_count {
        response.extend(data_row(values));
    }
    response.extend(backend_message(b'C', &[command.as_bytes(), b"\0"].concat()));
    response.extend(backend_message(b'Z', b"I"));
    response
}

fn slot_response(
    columns: &[&str],
    values: &[Option<&str>],
    row_count: usize,
    command: &str,
) -> Vec<u8> {
    let mut response = row_description(columns);
    for _ in 0..row_count {
        response.extend(data_row(values));
    }
    response.extend(backend_message(b'C', &[command.as_bytes(), b"\0"].concat()));
    response.extend(backend_message(b'Z', b"I"));
    response
}

fn row_description(columns: &[&str]) -> Vec<u8> {
    let columns = columns
        .iter()
        .map(|column| (*column, 25, -1))
        .collect::<Vec<_>>();
    typed_row_description(&columns)
}

fn typed_row_description(columns: &[(&str, u32, i16)]) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_u16(u16::try_from(columns.len()).unwrap());
    for (column, oid, size) in columns {
        body.put_slice(column.as_bytes());
        body.put_u8(0);
        body.put_u32(0);
        body.put_i16(0);
        body.put_u32(*oid);
        body.put_i16(*size);
        body.put_i32(-1);
        body.put_i16(0);
    }
    backend_message(b'T', &body)
}

fn data_row(values: &[Option<&str>]) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_u16(u16::try_from(values.len()).unwrap());
    for value in values {
        if let Some(value) = value {
            body.put_i32(i32::try_from(value.len()).unwrap());
            body.put_slice(value.as_bytes());
        } else {
            body.put_i32(-1);
        }
    }
    backend_message(b'D', &body)
}

fn authentication(kind: i32, data: &[u8]) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i32(kind);
    body.put_slice(data);
    backend_message(b'R', &body)
}

fn error_response(code: &str, message: &str) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_u8(b'C');
    body.put_slice(code.as_bytes());
    body.put_u8(0);
    body.put_u8(b'M');
    body.put_slice(message.as_bytes());
    body.put_u8(0);
    body.put_u8(0);
    backend_message(b'E', &body)
}

fn backend_message(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut message = BytesMut::new();
    message.put_u8(tag);
    message.put_i32(i32::try_from(body.len() + 4).unwrap());
    message.put_slice(body);
    message.to_vec()
}

fn connection_config(port: u16, trusted_plaintext: bool) -> PostgresConnectionConfig {
    PostgresConnectionConfig {
        host: "127.0.0.1".to_owned(),
        port,
        database: "inventory".to_owned(),
        username: "alice".to_owned(),
        password: "secret".to_owned(),
        trusted_plaintext,
        tls_ca_file: None,
    }
}

fn expected_system() -> PostgresSystemIdentity {
    PostgresSystemIdentity {
        system_identifier: SYSTEM_IDENTIFIER,
        database: "inventory".to_owned(),
    }
}
