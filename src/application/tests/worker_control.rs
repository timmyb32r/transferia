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
