use std::{sync::Arc, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Notify,
};

use super::{OutboundHttpClient, OutboundHttpError};

#[tokio::test]
async fn rejects_redirect_without_contacting_target() {
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
        let mut request = [0_u8; 2048];
        let _ = stream.read(&mut request).await.unwrap();
        stream
            .write_all(
                format!(
                    "HTTP/1.1 302 Found\r\nLocation: http://{target_address}/stolen\r\nContent-Length: 0\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
    });

    let client = OutboundHttpClient::new(Duration::from_secs(1), []).unwrap();
    let error = client
        .get(format!("http://{redirector_address}/start").parse().unwrap())
        .send()
        .await
        .unwrap_err();

    assert!(matches!(error, OutboundHttpError::RedirectRejected { .. }));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), target_contacted.notified())
            .await
            .is_err(),
        "redirect target must not be contacted"
    );
    redirect_task.await.unwrap();
    target_task.abort();
}
