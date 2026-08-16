use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::TcpListener;

use super::*;

#[tokio::test]
async fn parent_stop_and_disconnect_cancel_the_worker() -> anyhow::Result<()> {
    for command in [Some("STOP\n"), None] {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let parent = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut reader = BufReader::new(stream);
            let mut authentication = String::new();
            reader.read_line(&mut authentication).await?;
            anyhow::ensure!(authentication == "AUTH test-token\n");
            let mut ready = String::new();
            reader.read_line(&mut ready).await?;
            anyhow::ensure!(ready == "READY\n");
            if let Some(command) = command {
                reader.get_mut().write_all(command.as_bytes()).await?;
            }
            Ok::<(), anyhow::Error>(())
        });
        let cancellation = CancellationToken::new();
        let control = WorkerControl::connect(address, "test-token", cancellation.clone()).await?;
        control.ready().await?;
        parent.await??;
        tokio::time::timeout(Duration::from_secs(1), cancellation.cancelled()).await?;
    }
    Ok(())
}

#[tokio::test]
async fn startup_failure_preserves_the_error_chain() -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let cancellation = CancellationToken::new();
    let worker = tokio::spawn(async move {
        let control = WorkerControl::connect(address, "token", cancellation).await?;
        let error =
            anyhow::anyhow!("native connect timed out").context("ClickHouse connection failed");
        control.startup_failed(&error).await
    });
    let (stream, _) = listener.accept().await?;
    let mut lines = BufReader::new(stream).lines();
    assert_eq!(lines.next_line().await?.as_deref(), Some("AUTH token"));
    let failure = lines
        .next_line()
        .await?
        .context("worker did not report its startup failure")?;
    let encoded = failure
        .strip_prefix("ERROR ")
        .context("startup failure used an unknown protocol message")?;
    assert_eq!(
        serde_json::from_str::<String>(encoded)?,
        "ClickHouse connection failed: native connect timed out"
    );
    worker.await??;
    Ok(())
}
