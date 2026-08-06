//! S3 snapshot source — reads JSON-line files from S3-compatible object stores.
//!
//! Streaming architecture:
//!   list objects → open file → read chunk (16 MiB) → `safe_split_at` →
//!   `split_into_records` → `Vec<Message>` → `ReadResult::Batch`
//!
//! Chunk boundaries never split records. Remainder after the last delimiter
//! is carried to the next chunk. EOF remainder (no trailing `\n`) is emitted
//! as a final record when the file ends.
//!
//! Zero-copy: `Bytes::slice` from the chunk buffer — no per-record memcpy.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
use futures_util::{StreamExt, TryStreamExt};
use object_store::{GetResult, ObjectStore};

use crate::config::yaml::ChunkSplitter;
use crate::pipeline::source::{CommitMarker, ReadResult, Source};
use crate::types::message::{Message, MessageBatch};

// ---------------------------------------------------------------------------
// Error classification
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum S3ReadError {
    /// Network, S3 API, stream interruption — retry with backoff.
    Transport { op: &'static str, file: Arc<str>, source: anyhow::Error },
    /// Bad / corrupt / too-large record — non-retryable, abort snapshot.
    Data { file: Arc<str>, reason: String },
    /// File vanished between list and read — abort (incomplete snapshot).
    NoSuchFile { file: Arc<str> },
}

impl std::fmt::Display for S3ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport { op, file, source } => write!(f, "S3 transport {op} {file}: {source}"),
            Self::Data { file, reason } => write!(f, "S3 data error {file}: {reason}"),
            Self::NoSuchFile { file } => write!(f, "S3 NoSuchFile: {file}"),
        }
    }
}

impl std::error::Error for S3ReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport { source, .. } => Some(source.as_ref()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3Retry {
    Retry,
    Return,
}

/// Single classification point — maps every `S3ReadError` to a retry decision.
pub fn classify(err: &S3ReadError) -> S3Retry {
    match err {
        S3ReadError::Transport { .. } => S3Retry::Retry,
        _ => S3Retry::Return,
    }
}

fn backoff_ms(attempt: u32) -> std::time::Duration {
    std::time::Duration::from_millis((100u64 * 2u64.pow(attempt.saturating_sub(1))).min(5_000))
}

// ---------------------------------------------------------------------------
// ChunkedReader — streaming file reader with resume-offset
// ---------------------------------------------------------------------------

struct ChunkedReader {
    stream: futures_util::stream::BoxStream<'static, Result<Bytes, object_store::Error>>,
    eof: bool,
}

impl ChunkedReader {
    fn new(result: GetResult) -> Self {
        Self { stream: result.into_stream(), eof: false }
    }

    /// Read enough bytes from the stream so that `buf` grows by approximately
    /// `target` bytes, or until EOF. The actual growth may exceed `target`
    /// because the underlying S3 chunks are appended whole.
    ///
    /// Returns the number of bytes added. 0 means EOF.
    async fn read_more(&mut self, buf: &mut BytesMut, target: usize) -> anyhow::Result<usize> {
        if self.eof { return Ok(0); }
        buf.reserve(target);
        let needed = target.saturating_sub(buf.len());
        let before = buf.len();
        while buf.len() - before < needed {
            match self.stream.next().await {
                Some(Ok(chunk)) => buf.extend_from_slice(&chunk),
                Some(Err(e)) => return Err(anyhow::anyhow!("S3 read: {e}")),
                None => { self.eof = true; break; }
            }
        }
        Ok(buf.len() - before)
    }
}

// ---------------------------------------------------------------------------
// S3Source
// ---------------------------------------------------------------------------

const READ_MORE_GRANULARITY: usize = 1024 * 1024; // 1 MiB — progress for giant records
const LOG_EVERY_ROWS: u64 = 10_000;

pub struct S3Source {
    store: Arc<dyn ObjectStore>,
    files: Vec<object_store::ObjectMeta>,
    current_idx: usize,
    current_reader: Option<ChunkedReader>,
    framer: ChunkSplitter,
    chunk_size: usize,
    max_retries: u32,
    partition_id: i64,
    /// Un-framed bytes: read-cursor via `BytesMut` (O(1) split_to).
    pending: BytesMut,
    /// Prefix of `pending` already known to contain no `\n`.
    /// `safe_split_at` only scans `pending[pending_scanned..]`.
    pending_scanned: usize,
    /// Bytes already framed into messages (for resume offset).
    framed: usize,
    /// Files fully ingested.
    files_done: usize,
    /// Total rows produced.
    rows_produced: u64,
}

impl S3Source {
    pub async fn new(
        store: Arc<dyn ObjectStore>,
        prefix: &str,
        framer: ChunkSplitter,
        chunk_size: usize,
        max_retries: u32,
        partition_id: i64,
    ) -> anyhow::Result<Self> {
        let mut files: Vec<object_store::ObjectMeta> = store
            .list(Some(&object_store::path::Path::from(prefix)))
            .try_collect()
            .await
            .map_err(|e| anyhow::anyhow!("S3 list '{}' failed: {e}", prefix))?;
        files.sort_by(|a, b| a.location.cmp(&b.location));
        if files.is_empty() {
            anyhow::bail!(
                "S3 source: no objects found under prefix '{}' — refusing empty snapshot",
                prefix
            );
        }
        tracing::info!(
            "S3 source: {} objects under '{}' (chunk={}MiB retries={})",
            files.len(), prefix, chunk_size / (1024 * 1024), max_retries,
        );
        Ok(Self {
            store, files, current_idx: 0, current_reader: None, framer,
            chunk_size, max_retries, partition_id,
            pending: BytesMut::new(), pending_scanned: 0, framed: 0, files_done: 0, rows_produced: 0,
        })
    }

    /// Open the current file. On retry (resume > 0), uses `GetOptions.range`
    /// to avoid re-reading already-framed bytes.
    async fn open_current(&mut self) -> Result<(), S3ReadError> {
        let meta = &self.files[self.current_idx];
        let path = meta.location.clone();
        let resume = self.framed + self.pending.len();

        let get = if resume > 0 {
            tracing::debug!(
                "S3: resuming {} at offset {}",
                path, resume,
            );
            let opts = object_store::GetOptions {
                range: Some(object_store::GetRange::from(resume as u64..)),
                ..Default::default()
            };
            self.store.get_opts(&path, opts).await
        } else {
            self.store.get(&path).await
        };

        match get {
            Ok(r) => {
                self.current_reader = Some(ChunkedReader::new(r));
                Ok(())
            }
            Err(object_store::Error::NotFound { .. }) => {
                Err(S3ReadError::NoSuchFile { file: path.to_string().into() })
            }
            Err(e) => Err(S3ReadError::Transport {
                op: "get",
                file: path.to_string().into(),
                source: anyhow::anyhow!(e),
            }),
        }
    }

    /// Helper: maps the outcome of `try_read_records` into a `ReadResult`.
    fn outcome_to_result(messages: Option<Vec<Message>>, partition_id: i64) -> ReadResult {
        match messages {
            Some(msgs) => ReadResult::Batch(MessageBatch {
                messages: msgs, partition_id, commit_marker: None,
            }),
            None => ReadResult::Exhausted,
        }
    }

    /// Core loop: read chunks until we have ≥1 framed record, or EOF.
    /// Returns `None` when all files are exhausted.
    async fn try_read_records(&mut self) -> Result<Option<Vec<Message>>, S3ReadError> {
        loop {
            if self.current_reader.is_none() {
                if self.current_idx >= self.files.len() {
                    return Ok(None);
                }
                self.framed = 0;
                self.pending.clear();
                self.pending_scanned = 0;
                self.open_current().await?;
            }

            let reader = self.current_reader.as_mut().unwrap();
            let boundary = if reader.eof {
                self.pending.len() // EOF: emit remainder as final record
            } else if self.pending_scanned < self.pending.len() {
                // Incremental scan: only check the new tail since last scan.
                let tail = &self.pending[self.pending_scanned..];
                if let Some(i) = tail.iter().rposition(|&b| b == b'\n') {
                    self.pending_scanned + i + 1  // boundary in full pending coords
                } else {
                    self.pending_scanned = self.pending.len();
                    0
                }
            } else {
                0 // already scanned, no boundary found
            };

            if boundary == 0 && !reader.eof {
                // No delimiter yet — need more bytes.
                let max = 2 * self.chunk_size + READ_MORE_GRANULARITY;
                if self.pending.len() > max {
                    return Err(S3ReadError::Data {
                        file: self.files[self.current_idx].location.to_string().into(),
                        reason: format!(
                            "pending buffer exceeded {} MiB without record boundary — \
                             corrupt file or single record > chunk_size",
                            max / (1024 * 1024),
                        ),
                    });
                }
                let target = if self.pending.len() < self.chunk_size {
                    self.chunk_size - self.pending.len()
                } else {
                    READ_MORE_GRANULARITY
                };
                reader.read_more(&mut self.pending, target).await
                    .map_err(|e| S3ReadError::Transport {
                        op: "read",
                        file: self.files[self.current_idx].location.to_string().into(),
                        source: e,
                    })?;
                continue;
            }

            if boundary == 0 {
                // EOF + nothing pending → file done.
                self.current_reader = None;
                self.current_idx += 1;
                self.files_done += 1;
                tracing::info!(
                    "S3: file {}/{} done ({} rows so far)",
                    self.files_done, self.files.len(), self.rows_produced,
                );
                continue;
            }

            // Split framed bytes into records — zero-copy via Bytes::slice.
            let chunk = Bytes::from(self.pending.split_to(boundary));
            self.pending_scanned = self.pending_scanned.saturating_sub(boundary);
            let base_ptr = chunk.as_ptr() as usize;
            self.framed += boundary;

            let messages: Vec<Message> = self.framer
                .split_into_records(&chunk)
                .into_iter()
                .map(|rec| {
                    let offset = rec.as_ptr() as usize - base_ptr;
                    Message { value: chunk.slice(offset..offset + rec.len()), offset: None, partition: None }
                })
                .collect();

            // Progress logging — milestone-based.
            let n = messages.len() as u64;
            let prev_stone = self.rows_produced / LOG_EVERY_ROWS;
            self.rows_produced += n;
            let new_stone = self.rows_produced / LOG_EVERY_ROWS;
            if new_stone > prev_stone || self.rows_produced == n {
                tracing::info!(
                    "S3: {} rows produced (file {}/{})",
                    self.rows_produced, self.files_done + 1, self.files.len(),
                );
            }

            return Ok(Some(messages));
        }
    }
}

impl Source for S3Source {
    fn read_batch<'a>(&'a mut self) -> BoxFuture<'a, anyhow::Result<ReadResult>> { Box::pin(async move {
        let outcome = self.try_read_records().await;
        match outcome {
            Ok(messages) => Ok(Self::outcome_to_result(messages, self.partition_id)),
            Err(ref e @ S3ReadError::NoSuchFile { .. }) => {
                tracing::error!("{e} — file vanished, aborting snapshot");
                Ok(ReadResult::Failed(anyhow::anyhow!("{e}")))
            }
            Err(ref first_err) => match classify(first_err) {
                S3Retry::Retry => {
                    let mut attempt = 0u32;
                    let mut last_msg = format!("{first_err}");
                    loop {
                        attempt += 1;
                        if attempt > self.max_retries {
                            tracing::error!(
                                "{} retries exhausted (last: {last_msg}); aborting snapshot",
                                self.max_retries,
                            );
                            return Ok(ReadResult::Failed(anyhow::anyhow!("{last_msg}")));
                        }
                        tracing::warn!("attempt {}/{} — {last_msg}", attempt, self.max_retries);
                        tokio::time::sleep(backoff_ms(attempt)).await;
                        self.current_reader = None; // force re-open with resume offset
                        match self.try_read_records().await {
                            Ok(messages) => {
                                return Ok(Self::outcome_to_result(messages, self.partition_id));
                            }
                            Err(e2) => {
                                last_msg = format!("{e2}");
                                match classify(&e2) {
                                    S3Retry::Retry => continue,
                                    S3Retry::Return => {
                                        tracing::error!("{e2} — non-retryable, aborting");
                                        return Ok(ReadResult::Failed(anyhow::anyhow!("{e2}")));
                                    }
                                }
                            }
                        }
                    }
                }
                S3Retry::Return => {
                    tracing::error!("{first_err} — non-retryable, aborting snapshot");
                    Ok(ReadResult::Failed(anyhow::anyhow!("{first_err}")))
                }
            },
        }
    })}

    fn commit_offsets<'a>(&'a mut self, _marker: &'a CommitMarker) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move { Ok(()) })
    }
}
