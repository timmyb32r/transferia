use super::authentication_check_message;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

#[test]
fn empty_password_authentication_failure_explains_how_to_retry() {
    let message = authentication_check_message(Some("28P01"), true).unwrap();
    assert!(message.contains("password field is empty"));
    assert!(message.contains("Enter the password"));
}

#[test]
fn authentication_diagnostics_do_not_misclassify_other_failures() {
    let message = authentication_check_message(Some("28P01"), false).unwrap();
    assert!(message.contains("Check the username and password"));
    assert!(!message.contains("empty"));
    assert!(authentication_check_message(Some("28000"), true)
        .unwrap()
        .contains("pg_hba.conf"));
    assert_eq!(authentication_check_message(Some("08001"), true), None);
    assert_eq!(authentication_check_message(None, true), None);
}

#[tokio::test]
async fn table_catalog_uses_one_unnamed_protocol_exchange_for_transaction_pooling() {
    let mut fields = 2_i16.to_be_bytes().to_vec();
    for name in ["nspname", "relname"] {
        fields.extend_from_slice(name.as_bytes());
        fields.push(0);
        fields.extend_from_slice(&0_u32.to_be_bytes()); // table OID
        fields.extend_from_slice(&0_i16.to_be_bytes()); // attribute number
        fields.extend_from_slice(&19_u32.to_be_bytes()); // PostgreSQL NAME
        fields.extend_from_slice(&64_i16.to_be_bytes());
        fields.extend_from_slice(&(-1_i32).to_be_bytes());
        fields.extend_from_slice(&1_i16.to_be_bytes()); // binary result format
    }
    let mut row = 2_i16.to_be_bytes().to_vec();
    for value in ["schema.with.dot", "table?name"] {
        row.extend_from_slice(&i32::try_from(value.len()).unwrap().to_be_bytes());
        row.extend_from_slice(value.as_bytes());
    }
    let response = [
        backend_message(b'1', &[]), // ParseComplete
        backend_message(b'2', &[]), // BindComplete
        backend_message(b'T', &fields),
        backend_message(b'D', &row),
        backend_message(b'C', b"SELECT 1\0"),
        backend_message(b'Z', b"I"),
    ]
    .concat();
    let (config, server) = catalog_protocol_fixture(response).await;
    let result = super::list_tables(&config).await.unwrap();
    server.await.unwrap();
    assert_eq!(
        result,
        vec![transferia_registry::TableIdentity {
            namespace: "schema.with.dot".into(),
            name: "table?name".into(),
        }]
    );
}

#[tokio::test]
async fn table_catalog_error_explains_the_stage_and_sqlstate_without_error_details() {
    let response = [
        backend_message(
            b'E',
            b"SERROR\0C42501\0Mpermission denied for catalog\0Dprivate diagnostic detail\0\0",
        ),
        backend_message(b'Z', b"I"),
    ]
    .concat();
    let (config, server) = catalog_protocol_fixture(response).await;
    let error = super::list_tables(&config).await.unwrap_err().to_string();
    server.await.unwrap();
    assert_eq!(
        error,
        "PostgreSQL table discovery failed: permission denied for catalog (SQLSTATE 42501)"
    );
    assert!(!error.contains("private diagnostic detail"));
    assert!(!error.contains(&config.password));
}

fn backend_message(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut message = vec![tag];
    message.extend_from_slice(&i32::try_from(body.len() + 4).unwrap().to_be_bytes());
    message.extend_from_slice(body);
    message
}

// A transaction pooler cannot preserve a named statement after ReadyForQuery.
// Assert the actual production client sends Parse/Bind/Describe/Execute before
// its first Sync, rather than mocking list_tables or merely inspecting SQL text.
async fn catalog_protocol_fixture(
    response: Vec<u8>,
) -> (super::PostgresConnectionConfig, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let length = socket.read_u32().await.unwrap();
        let mut startup = vec![0; usize::try_from(length - 4).unwrap()];
        socket.read_exact(&mut startup).await.unwrap();
        assert_eq!(&startup[..4], &196_608_u32.to_be_bytes());
        socket
            .write_all(
                &[
                    backend_message(b'R', &0_i32.to_be_bytes()),
                    backend_message(b'Z', b"I"),
                ]
                .concat(),
            )
            .await
            .unwrap();
        for &expected in b"PBDES" {
            let tag = socket.read_u8().await.unwrap();
            assert_eq!(
                tag, expected,
                "catalog request must finish execution before Sync"
            );
            let length = socket.read_u32().await.unwrap();
            let mut body = vec![0; usize::try_from(length - 4).unwrap()];
            socket.read_exact(&mut body).await.unwrap();
            if tag == b'P' {
                assert_eq!(body[0], 0, "catalog query must use an unnamed statement");
                let sql = std::str::from_utf8(body[1..].split(|byte| *byte == 0).next().unwrap())
                    .unwrap();
                assert!(sql.contains("has_schema_privilege"));
                assert!(sql.contains("has_table_privilege"));
            }
            if tag == b'B' {
                assert_eq!(
                    &body[..2],
                    &[0, 0],
                    "bind unnamed portal to unnamed statement"
                );
            }
        }
        socket.write_all(&response).await.unwrap();
    });
    (
        super::PostgresConnectionConfig {
            host: address.ip().to_string(),
            port: address.port(),
            database: "catalog_test".into(),
            username: "reader".into(),
            password: "test-only-password".into(),
            trusted_plaintext: true,
            tls_ca_file: None,
        },
        server,
    )
}
