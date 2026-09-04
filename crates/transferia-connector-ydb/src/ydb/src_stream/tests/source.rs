#![allow(
    clippy::expect_used,
    reason = "test assertions intentionally fail fast"
)]

use super::{blocking_decode_result, validate_generated_column_names, DiscoveredTable};
use crate::ydb::config::YdbTableConfig;
use arrow::datatypes::DataType;
use transferia_core::data::message::SourceBatch;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::failure::FailureDisposition;

#[tokio::test]
async fn blocking_decode_worker_failure_is_fatal() {
    let joined = tokio::task::spawn_blocking(|| -> anyhow::Result<SourceBatch> {
        panic!("simulated YDB CDC decode worker failure")
    })
    .await;
    let Err(error) = blocking_decode_result(joined) else {
        panic!("panicked blocking decoder must fail")
    };
    assert_eq!(error.disposition(), FailureDisposition::Fatal);
}

#[test]
fn user_columns_cannot_collide_with_generated_cdc_columns() {
    let table = DiscoveredTable {
        config: YdbTableConfig {
            path: "/production/events".to_owned(),
        },
        schema: DatasetSchema::new(vec![SchemaColumn::new(
            "_system_old_value_0".to_owned(),
            DataType::UInt64,
            false,
        )]),
        columns: Vec::new(),
    };
    assert!(validate_generated_column_names(&table).is_err());
}
