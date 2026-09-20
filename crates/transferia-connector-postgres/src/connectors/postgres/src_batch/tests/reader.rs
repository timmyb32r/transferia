use super::sanitized_request_failure;

#[test]
fn explicit_text_projection_cannot_resolve_a_user_defined_text_type() {
    use crate::connectors::postgres::source::UnsupportedTypePolicy;
    let expression = super::source_column_expression(
        "amount\"quoted",
        &tokio_postgres::types::Type::NUMERIC,
        UnsupportedTypePolicy::Fail,
    ).unwrap();
    assert_eq!(expression, "\"amount\"\"quoted\"::pg_catalog.text AS \"amount\"\"quoted\"");
}

#[test]
fn database_diagnostics_do_not_reproduce_request_values_or_driver_details() {
    for retryable in [false, true] {
        let error = sanitized_request_failure(
            "read_snapshot_chunk",
            anyhow::anyhow!("driver message containing source-value-secret and credential-secret"),
            retryable,
        );
        assert_eq!(error.is_retryable(), retryable);
        let diagnostic = format!("{error:?}");
        assert!(diagnostic.contains("read_snapshot_chunk"));
        assert!(!diagnostic.contains("source-value-secret"));
        assert!(!diagnostic.contains("credential-secret"));
        assert!(!diagnostic.contains("driver message"));
    }
}
