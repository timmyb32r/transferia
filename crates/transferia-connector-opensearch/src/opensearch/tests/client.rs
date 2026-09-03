use std::sync::Arc;
use std::time::Duration;

use reqwest::StatusCode;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;
use tokio::sync::Notify;

use super::super::{
    OpenSearchAuth, OpenSearchClient, OpenSearchConnectionConfig, OpenSearchHttpError,
};

#[test]
fn only_transient_http_statuses_are_retryable() {
    for status in [408, 425, 429, 500, 502, 503, 504] {
        assert!(OpenSearchHttpError::Status {
            status: StatusCode::from_u16(status).unwrap()
        }
        .retryable());
    }
    for status in [400, 401, 403, 404, 409, 501, 505] {
        assert!(!OpenSearchHttpError::Status {
            status: StatusCode::from_u16(status).unwrap()
        }
        .retryable());
    }
}

#[tokio::test]
async fn redirect_never_forwards_bulk_body_or_authorization() {
    let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target_address = target.local_addr().unwrap();
    let target_contacted = Arc::new(Notify::new());
    let target_task = {
        let target_contacted = Arc::clone(&target_contacted);
        tokio::spawn(async move {
            if target.accept().await.is_ok() {
                target_contacted.notify_one();
            }
        })
    };

    let redirector = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let redirector_address = redirector.local_addr().unwrap();
    let redirect_task = tokio::spawn(async move {
        let (mut stream, _) = redirector.accept().await.unwrap();
        let request = read_http_request(&mut stream).await;
        let request = String::from_utf8_lossy(&request);
        assert!(request.lines().any(|line| {
            line.split_once(':').is_some_and(|(name, value)| {
                name.eq_ignore_ascii_case("authorization")
                    && value.trim_start().starts_with("Basic ")
            })
        }));
        assert!(request.contains("secret-document"));
        stream
            .write_all(
                format!(
                    "HTTP/1.1 307 Temporary Redirect\r\nLocation: http://{target_address}/stolen\r\nContent-Length: 0\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
    });

    let client = OpenSearchClient::new(&config(redirector_address.port(), 4096)).unwrap();
    let error = client
        .request(
            reqwest::Method::POST,
            &["_bulk"],
            &[],
            "application/x-ndjson",
            Some(b"{\"value\":\"secret-document\"}\n".to_vec()),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, OpenSearchHttpError::Boundary));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), target_contacted.notified())
            .await
            .is_err(),
        "redirect target must receive neither authorization nor body"
    );
    redirect_task.await.unwrap();
    target_task.abort();
}

async fn read_http_request(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut request = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        let read = stream.read(&mut chunk).await.unwrap();
        assert!(read > 0, "HTTP request ended before its declared body");
        request.extend_from_slice(&chunk[..read]);
        assert!(request.len() <= 64 * 1024, "test HTTP request is unexpectedly large");

        let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n") else {
            continue;
        };
        let header_end = header_end + 4;
        let headers = std::str::from_utf8(&request[..header_end]).unwrap();
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap_or(0);
        if request.len() >= header_end + content_length {
            return request;
        }
    }
}

#[tokio::test]
async fn streamed_response_cannot_cross_the_configured_byte_limit() {
    let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = server.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (mut stream, _) = server.accept().await.unwrap();
        let mut request = [0_u8; 2048];
        let _ = stream.read(&mut request).await.unwrap();
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n8\r\n12345678\r\n8\r\nabcdefgh\r\n0\r\n\r\n")
            .await
            .unwrap();
    });

    let client = OpenSearchClient::new(&config(address.port(), 12)).unwrap();
    let error = client
        .request(reqwest::Method::GET, &[], &[], "application/json", None)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        OpenSearchHttpError::ResponseTooLarge { limit: 12 }
    ));
    task.await.unwrap();
}

fn config(port: u16, max_response_bytes: usize) -> OpenSearchConnectionConfig {
    OpenSearchConnectionConfig {
        hosts: vec!["127.0.0.1".to_owned()],
        port,
        trusted_plaintext: true,
        tls_ca_file: None,
        auth: OpenSearchAuth::Basic {
            username: "test-user".to_owned(),
            password: "test-password".to_owned(),
        },
        request_timeout_ms: 1_000,
        max_response_bytes,
    }
}
