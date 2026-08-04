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

use bytes::Bytes;
use bytes::BytesMut;
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
    /// Invalid configuration — abort immediately.
    Config { reason: String },
    /// Invariant violation — abort immediately.
    Fatal { reason: String },
    /// File vanished between list and read — abort (incomplete snapshot).
    NoSuchFile { file: Arc<str> },
}

impl std::fmt::Display for S3ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport { op, file, source } => write!(f, "S3 transport {op} {file}: {source}"),
            Self::Data { file, reason } => write!(f, "S3 data error {file}: {reason}"),
            Self::Config { reason } => write!(f, "S3 config: {reason}"),
            Self::Fatal { reason } => write!(f, "S3 fatal: {reason}"),
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

    /// Read more bytes into `buf`, growing it by at most `target` bytes.
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

const READ_MORE_GRANULARITY: usize = 1024 * 1024;              // 1 MiB — progress for giant records
const MAX_PENDING_BYTES: usize = 33_554_432 + READ_MORE_GRANULARITY; // 2×chunk + 1 MiB
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
            pending: BytesMut::new(), framed: 0, files_done: 0, rows_produced: 0,
        })
    }

    /// Open the current file. On retry (resume > 0), uses `GetOptions.range`
    /// to avoid re-reading already-framed bytes.
    async fn open_current(&mut self) -> Result<(), S3ReadError> {
        let meta = &self.files[self.current_idx];
        let path = meta.location.clone();
        let resume = self.framed + self.pending.len();

        let get = if resume > 0 {
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
    fn outcome_to_result(
        messages: Option<Vec<Message>>,
        partition_id: i64,
    ) -> ReadResult {
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
            // Open next file if needed
            if self.current_reader.is_none() {
                if self.current_idx >= self.files.len() {
                    return Ok(None);
                }
                self.framed = 0;
                self.pending.clear();
                self.open_current().await?;
            }

            let fname: Arc<str> = self.files[self.current_idx].location.to_string().into();
            let reader = self.current_reader.as_mut().unwrap();
            let boundary = if reader.eof {
                self.pending.len() // EOF: emit whatever is left as a final record
            } else {
                self.framer.safe_split_at(&self.pending)
            };

            if boundary == 0 && !reader.eof {
                // No delimiter yet — read more.
                if self.pending.len() > MAX_PENDING_BYTES {
                    return Err(S3ReadError::Data {
                        file: fname,
                        reason: format!(
                            "pending buffer exceeded {} MiB without a record boundary — \
                             corrupt file or record > chunk_size",
                            MAX_PENDING_BYTES / (1024 * 1024),
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
                        op: "read", file: fname, source: e,
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
            let base_ptr = chunk.as_ptr() as usize;
            self.framed += boundary;

            let messages: Vec<Message> = self.framer
                .split_into_records(&chunk)
                .into_iter()
                .map(|rec| {
                    let offset = rec.as_ptr() as usize - base_ptr;
                    Message { value: chunk.slice(offset..offset + rec.len()) }
                })
                .collect();

            // Progress logging — milestone-based, no off-by-one.
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
    async fn read_batch(&mut self) -> anyhow::Result<ReadResult> {
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
                    loop {
                        attempt += 1;
                        if attempt > self.max_retries {
                            tracing::error!(
                                "{first_err} — {} retries exhausted; aborting snapshot",
                                self.max_retries,
                            );
                            return Ok(ReadResult::Failed(anyhow::anyhow!("{first_err}")));
                        }
                        tracing::warn!("{first_err} — retry {}/{}", attempt, self.max_retries);
                        tokio::time::sleep(backoff_ms(attempt)).await;
                        self.current_reader = None; // force re-open with resume offset
                        match self.try_read_records().await {
                            Ok(messages) => {
                                return Ok(Self::outcome_to_result(messages, self.partition_id));
                            }
                            Err(e2) => match classify(&e2) {
                                S3Retry::Retry => continue,
                                S3Retry::Return => {
                                    tracing::error!("{e2} — non-retryable, aborting");
                                    return Ok(ReadResult::Failed(anyhow::anyhow!("{e2}")));
                                }
                            },
                        }
                    }
                }
                S3Retry::Return => {
                    tracing::error!("{first_err} — non-retryable, aborting snapshot");
                    Ok(ReadResult::Failed(anyhow::anyhow!("{first_err}")))
                }
            },
        }
    }

    async fn commit_offsets(&mut self, _marker: &CommitMarker) -> anyhow::Result<()> {
        Ok(()) // snapshot — no offsets to commit
    }
}
