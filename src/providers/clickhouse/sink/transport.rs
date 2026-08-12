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

fn classify_insert_error(error: ClickHouseError) -> InsertError {
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
        ClickHouseError::ServerException(server) => is_transient_server_error(&server.error),
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

const fn is_transient_server_error(error: &Severity) -> bool {
    match error {
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
        ),
        Severity::Syntax(_) | Severity::Data(_) | Severity::Unknown(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use clickhouse_arrow::ServerError;

    use super::*;

    fn server_exception(error: Severity) -> ClickHouseError {
        ClickHouseError::ServerException(ServerError {
            error,
            code: 0,
            name: "test".into(),
            message: "test".into(),
            stack_trace: String::new(),
        })
    }

    #[test]
    fn classifies_typed_transport_errors_as_transient() {
        assert!(matches!(
            classify_insert_error(ClickHouseError::ConnectionTimeout("test".into())),
            InsertError::Transient(_)
        ));
        assert!(matches!(
            classify_insert_error(server_exception(Severity::Protocol(
                ServerErrorCode::NetworkError
            ))),
            InsertError::Transient(_)
        ));
        assert!(matches!(
            classify_insert_error(ClickHouseError::Client("No active connection".into())),
            InsertError::Transient(_)
        ));
        assert!(matches!(
            classify_insert_error(ClickHouseError::Protocol(
                "Failed to receive response for query abc".into()
            )),
            InsertError::Transient(_)
        ));
        assert!(matches!(
            classify_insert_error(ClickHouseError::Protocol(
                "Failed to receive response from insert abc".into()
            )),
            InsertError::Transient(_)
        ));
    }

    #[test]
    fn classifies_authentication_and_unknown_errors_as_permanent() {
        assert!(matches!(
            classify_insert_error(server_exception(Severity::Query(
                ServerErrorCode::MemoryLimitExceeded
            ))),
            InsertError::Permanent(_)
        ));
        assert!(matches!(
            classify_insert_error(server_exception(Severity::Protocol(
                ServerErrorCode::WrongPassword
            ))),
            InsertError::Permanent(_)
        ));
        assert!(matches!(
            classify_insert_error(ClickHouseError::Unknown("network timeout".into())),
            InsertError::Permanent(_)
        ));
        assert!(matches!(
            classify_insert_error(ClickHouseError::Protocol(
                "Unexpected packet Data, expected server hello".into()
            )),
            InsertError::Permanent(_)
        ));
        assert!(matches!(
            classify_insert_error(ClickHouseError::Client(
                "arrow serialize error: incompatible value".into()
            )),
            InsertError::Permanent(_)
        ));
    }
}
