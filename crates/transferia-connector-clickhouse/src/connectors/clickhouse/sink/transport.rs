use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use clickhouse_arrow::native::error_codes::ClickHouseError as ServerErrorCode;
use clickhouse_arrow::{Error as ClickHouseError, Severity};
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

    fn query_all(
        &self,
        query: String,
    ) -> BoxFuture<'static, Result<Vec<RecordBatch>, InsertError>>;
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

    fn query_all(
        &self,
        query: String,
    ) -> BoxFuture<'static, Result<Vec<RecordBatch>, InsertError>> {
        let client = Arc::clone(&self.client);
        Box::pin(async move {
            client
                .query_all(&query)
                .await
                .map_err(classify_insert_error)
        })
    }
}

pub(super) fn classify_insert_error(error: ClickHouseError) -> InsertError {
    let transient = match &error {
        ClickHouseError::Io(_)
        | ClickHouseError::InternalChannelError
        | ClickHouseError::ConnectionTimeout(_)
        | ClickHouseError::ConnectionGone(_)
        | ClickHouseError::StartupError
        | ClickHouseError::ChannelClosed
        | ClickHouseError::OutgoingTimeout(_)
        | ClickHouseError::InsertArrowRetry(_) => true,
        ClickHouseError::Protocol(message) => is_connection_protocol_error(message),
        ClickHouseError::Client(message) => is_connection_client_error(message),
        ClickHouseError::ServerException(server) => is_transient_server_error(server),
        _ => false,
    };
    let error = anyhow::Error::new(error);
    if transient {
        InsertError::Transient(error)
    } else {
        InsertError::Permanent(error)
    }
}

fn is_connection_protocol_error(message: &str) -> bool {
    message.starts_with("Failed to receive response for query ")
        || message.starts_with("Failed to receive header for query ")
        || message.starts_with("Failed to receive response from insert ")
}

fn is_connection_client_error(message: &str) -> bool {
    message == "No active connection"
        || message == "channel closed"
        || message == "Internal channel closed"
        || message.starts_with("io error: ")
        || message.starts_with("connection gone: ")
}

const fn is_transient_server_error(error: &clickhouse_arrow::ServerError) -> bool {
    // Native-protocol exception names are class names such as
    // `DB::Exception`; the stable branch discriminator is the numeric server
    // code. Keep this list narrow so configuration and data errors still fail
    // fast instead of consuming both retry budgets.
    if matches!(
        error.code,
        202 // TOO_MANY_SIMULTANEOUS_QUERIES
            | 203 // NO_FREE_CONNECTION
            | 244 // UNEXPECTED_ZOOKEEPER_ERROR
            | 252 // TOO_MANY_PARTS
            | 254 // NO_ACTIVE_REPLICAS
            | 265 // NO_AVAILABLE_REPLICA
            | 285 // TOO_FEW_LIVE_REPLICAS
            | 286 // UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE
            | 289 // REPLICA_IS_NOT_IN_QUORUM
            | 319 // UNKNOWN_STATUS_OF_INSERT
            | 416 // REPLICA_STATUS_CHANGED
            | 733 // TABLE_IS_BEING_RESTARTED
            | 745 // SERVER_OVERLOADED
            | 999 // KEEPER_EXCEPTION
    ) {
        return true;
    }
    match &error.error {
        Severity::Server(_) => true,
        Severity::Protocol(error) => matches!(
            error,
            ServerErrorCode::CannotReadFromSocket
                | ServerErrorCode::CannotWriteToSocket
                | ServerErrorCode::SocketTimeout
                | ServerErrorCode::NetworkError
        ),
        Severity::Query(error) => matches!(
            error,
            ServerErrorCode::TimeoutExceeded
                | ServerErrorCode::QueryWasCancelled
                | ServerErrorCode::Aborted
                | ServerErrorCode::TableIsReadOnly
        ),
        Severity::Syntax(_) | Severity::Data(_) | Severity::Unknown(_) => false,
    }
}

#[cfg(test)]
#[path = "tests/transport.rs"]
mod tests;
