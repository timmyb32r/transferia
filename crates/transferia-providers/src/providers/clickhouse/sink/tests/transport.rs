use clickhouse_arrow::ServerError;

use super::*;

fn server_exception_with_code(error: Severity, code: i32) -> ClickHouseError {
    ClickHouseError::ServerException(ServerError {
        error,
        code,
        name: "DB::Exception".into(),
        message: "test".into(),
        stack_trace: String::new(),
    })
}

fn server_exception(error: Severity) -> ClickHouseError {
    server_exception_with_code(error, 0)
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
    assert!(matches!(
        classify_insert_error(server_exception(Severity::Query(
            ServerErrorCode::TableIsReadOnly
        ))),
        InsertError::Transient(_)
    ));
    assert!(matches!(
        classify_insert_error(server_exception_with_code(
            Severity::Unknown(ServerErrorCode::UnknownUser),
            254,
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

#[test]
fn classifies_allowlisted_server_codes_as_transient() {
    for (code, symbolic_name) in [
        (202, "TOO_MANY_SIMULTANEOUS_QUERIES"),
        (203, "NO_FREE_CONNECTION"),
        (244, "UNEXPECTED_ZOOKEEPER_ERROR"),
        (252, "TOO_MANY_PARTS"),
        (254, "NO_ACTIVE_REPLICAS"),
        (265, "NO_AVAILABLE_REPLICA"),
        (285, "TOO_FEW_LIVE_REPLICAS"),
        (286, "UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE"),
        (289, "REPLICA_IS_NOT_IN_QUORUM"),
        (319, "UNKNOWN_STATUS_OF_INSERT"),
        (416, "REPLICA_STATUS_CHANGED"),
        (733, "TABLE_IS_BEING_RESTARTED"),
        (745, "SERVER_OVERLOADED"),
        (999, "KEEPER_EXCEPTION"),
    ] {
        assert!(
            matches!(
                classify_insert_error(server_exception_with_code(
                    Severity::Unknown(ServerErrorCode::UnknownUser),
                    code,
                )),
                InsertError::Transient(_)
            ),
            "{symbolic_name} ({code}) must be retried within the bounded sink retry budget",
        );
    }
}

#[test]
fn keeps_non_allowlisted_server_codes_permanent() {
    for (code, symbolic_name) in [
        (117, "INCORRECT_DATA"),
        (225, "NO_ZOOKEEPER"),
        (251, "NO_SUCH_REPLICA"),
        (415, "ALL_REPLICAS_LOST"),
        (497, "ACCESS_DENIED"),
    ] {
        assert!(
            matches!(
                classify_insert_error(server_exception_with_code(
                    Severity::Unknown(ServerErrorCode::UnknownUser),
                    code,
                )),
                InsertError::Permanent(_)
            ),
            "{symbolic_name} ({code}) requires configuration or data repair, not blind retry",
        );
    }
}
