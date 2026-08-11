use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;

use super::client::ReconnectingClient;

#[derive(Debug)]
pub enum InsertError {
    Transient(anyhow::Error),
    Permanent(anyhow::Error),
}

impl core::fmt::Display for InsertError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Transient(error) | Self::Permanent(error) => error.fmt(formatter),
        }
    }
}

pub trait InsertTransport: Send + Sync {
    fn insert(
        &self,
        table: Arc<str>,
        batches: Vec<RecordBatch>,
    ) -> BoxFuture<'static, Result<(), InsertError>>;
}

pub(super) struct NativeTransport {
    client: Arc<ReconnectingClient>,
}

impl NativeTransport {
    pub(super) const fn new(client: Arc<ReconnectingClient>) -> Self {
        Self { client }
    }
}

impl InsertTransport for NativeTransport {
    fn insert(
        &self,
        table: Arc<str>,
        batches: Vec<RecordBatch>,
    ) -> BoxFuture<'static, Result<(), InsertError>> {
        let client = Arc::clone(&self.client);
        Box::pin(async move {
            client
                .insert_many(&table, batches)
                .await
                .map_err(classify_insert_error)
        })
    }
}

fn classify_insert_error(error: impl core::fmt::Display) -> InsertError {
    const PERMANENT_MARKERS: [&str; 9] = [
        "AUTHENTICATION_FAILED",
        "UNKNOWN_TABLE",
        "UNKNOWN_IDENTIFIER",
        "NO_SUCH_COLUMN",
        "TYPE_MISMATCH",
        "SYNTAX_ERROR",
        "NUMBER_OF_COLUMNS_DOESNT_MATCH",
        "Unknown table",
        "password",
    ];
    let message = error.to_string();
    let error = anyhow::anyhow!(message.clone());
    if PERMANENT_MARKERS
        .iter()
        .any(|marker| message.contains(marker))
    {
        InsertError::Permanent(error)
    } else {
        InsertError::Transient(error)
    }
}
