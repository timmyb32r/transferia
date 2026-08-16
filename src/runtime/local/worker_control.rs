use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub struct WorkerControl {
    writer: Arc<Mutex<OwnedWriteHalf>>,
}

impl WorkerControl {
    pub async fn connect(
        address: SocketAddr,
        token: &str,
        cancellation: CancellationToken,
    ) -> anyhow::Result<Self> {
        let stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(address))
            .await
            .context("timed out connecting to the parent control plane")??;
        stream.set_nodelay(true)?;
        let (reader, mut writer) = stream.into_split();
        writer
            .write_all(format!("AUTH {token}\n").as_bytes())
            .await?;
        let writer = Arc::new(Mutex::new(writer));
        tokio::spawn(async move {
            let mut lines = BufReader::new(reader).lines();
            loop {
                match lines.next_line().await {
                    Ok(Some(command)) if command == "STOP" => {
                        cancellation.cancel();
                        break;
                    }
                    Ok(Some(_)) => {}
                    Ok(None) | Err(_) => {
                        cancellation.cancel();
                        break;
                    }
                }
            }
        });
        Ok(Self { writer })
    }

    pub async fn ready(&self) -> anyhow::Result<()> {
        self.writer.lock().await.write_all(b"READY\n").await?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/worker_control.rs"]
mod tests;
