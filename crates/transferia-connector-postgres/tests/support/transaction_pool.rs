//! A deterministic wire-level session resetter backed by real PostgreSQL.
//! After every idle ReadyForQuery it executes DISCARD ALL before releasing the
//! next frontend exchange. Transactions keep their backend state; replication
//! startup connections pass through unchanged. This exercises the transaction
//! pooling contract without depending on probabilistic backend reassignment.

use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};

pub struct TransactionPool {
    port: u16,
    resets: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl TransactionPool {
    pub async fn start(host: String, port: u16) -> anyhow::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let frontend_port = listener.local_addr()?.port();
        let resets = Arc::new(AtomicUsize::new(0));
        let reset_count = Arc::clone(&resets);
        let task = tokio::spawn(async move {
            let mut clients = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    incoming = listener.accept() => {
                        let Ok((frontend, _)) = incoming else { break; };
                        let upstream = host.clone();
                        let resets = Arc::clone(&reset_count);
                        clients.spawn(async move { relay(frontend, &upstream, port, &resets).await });
                    }
                    _ = clients.join_next(), if !clients.is_empty() => {}
                }
            }
        });
        Ok(Self { port: frontend_port, resets, task })
    }

    pub fn port(&self) -> u16 { self.port }
    pub fn resets(&self) -> usize { self.resets.load(Ordering::Relaxed) }
}

impl Drop for TransactionPool {
    fn drop(&mut self) { self.task.abort(); }
}

async fn frame(socket: &mut TcpStream) -> anyhow::Result<(u8, Vec<u8>)> {
    let tag = socket.read_u8().await?;
    let length = usize::try_from(socket.read_u32().await?)?;
    anyhow::ensure!(length >= 4, "invalid test protocol message length");
    let mut bytes = vec![0; length + 1];
    bytes[0] = tag;
    bytes[1..5].copy_from_slice(&u32::try_from(length)?.to_be_bytes());
    socket.read_exact(&mut bytes[5..]).await?;
    Ok((tag, bytes))
}

async fn reset(backend: &mut TcpStream, resets: &AtomicUsize) -> anyhow::Result<()> {
    let query = b"DISCARD ALL\0";
    backend.write_u8(b'Q').await?;
    backend.write_u32(u32::try_from(query.len() + 4)?).await?;
    backend.write_all(query).await?;
    loop {
        let (tag, bytes) = frame(backend).await?;
        anyhow::ensure!(tag != b'E', "test pool could not discard idle backend state");
        if tag == b'Z' {
            anyhow::ensure!(bytes[5] == b'I', "test pool reset did not become idle");
            resets.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
    }
}

async fn relay(mut frontend: TcpStream, host: &str, port: u16, resets: &AtomicUsize) -> anyhow::Result<()> {
    let mut backend = TcpStream::connect((host, port)).await?;
    frontend.set_nodelay(true)?;
    backend.set_nodelay(true)?;
    let length = frontend.read_u32().await?;
    anyhow::ensure!(length >= 8, "invalid test startup message");
    let mut startup = vec![0; usize::try_from(length - 4)?];
    frontend.read_exact(&mut startup).await?;
    backend.write_u32(length).await?;
    backend.write_all(&startup).await?;
    if startup[..4] == 80_877_102_u32.to_be_bytes() { return Ok(()); } // CancelRequest
    let replication = startup[4..].split(|byte| *byte == 0).any(|value| value == b"replication");
    loop {
        let (tag, bytes) = frame(&mut backend).await?;
        frontend.write_all(&bytes).await?;
        if tag == b'Z' { break; }
        if tag == b'R' {
            let method = u32::from_be_bytes(bytes[5..9].try_into()?);
            if matches!(method, 3 | 5 | 10 | 11) {
                let (_, password) = frame(&mut frontend).await?;
                backend.write_all(&password).await?;
            }
        }
    }
    if replication {
        tokio::io::copy_bidirectional(&mut frontend, &mut backend).await?;
        return Ok(());
    }
    loop {
        loop {
            let (tag, bytes) = frame(&mut frontend).await?;
            backend.write_all(&bytes).await?;
            if tag == b'X' { return Ok(()); }
            if matches!(tag, b'S' | b'Q') { break; }
        }
        loop {
            let (tag, bytes) = frame(&mut backend).await?;
            if tag == b'Z' && bytes[5] == b'I' { reset(&mut backend, resets).await?; }
            frontend.write_all(&bytes).await?;
            if tag == b'Z' { break; }
        }
    }
}
