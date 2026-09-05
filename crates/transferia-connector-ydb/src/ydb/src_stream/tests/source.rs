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

#[test]
fn first_cdc_record_strictly_follows_snapshot_even_at_offset_zero() {
    assert_eq!(super::cdc_row_version(0, true).unwrap(), 1);
    assert_eq!(super::cdc_row_version(i64::MAX, true).unwrap(), i64::MAX as u64 + 1);
    assert_eq!(super::cdc_row_version(0, false).unwrap(), 0);
    assert_eq!(super::cdc_row_version(42, false).unwrap(), 42);
    assert!(super::cdc_row_version(-1, true).is_err());
}

#[test]
fn overlap_reconciles_complete_updates_without_changing_deletes_or_before_images() {
    use std::sync::Arc;
    use ydb_grpc::ydb_proto::{Type, r#type::{PrimitiveTypeId, Type as TypeKind}, table::ColumnMeta};
    use crate::ydb::types::column_plans;
    let columns = column_plans(vec![ColumnMeta { name: "id".into(),
        r#type: Some(Type { r#type: Some(TypeKind::TypeId(PrimitiveTypeId::Int64 as i32)) }),
        ..Default::default() }], &["id".into()]).unwrap();
    let decoder = super::YdbCdcDecoder::new(Arc::from(columns), 1024).unwrap();
    let mut update = decoder.decode(br#"{"key":[7],"update":{},"oldImage":{},"newImage":{},"ts":[1,2]}"#).unwrap();
    assert_eq!(update.operation, transferia_core::ChangeOperation::Update);
    let old = update.old.clone();
    super::reconcile_overlap(&mut update).unwrap();
    assert_eq!(update.operation, transferia_core::ChangeOperation::Create);
    assert_eq!(update.old, old);
    let mut delete = decoder.decode(br#"{"key":[7],"erase":{},"oldImage":{},"ts":[2,3]}"#).unwrap();
    super::reconcile_overlap(&mut delete).unwrap();
    assert_eq!(delete.operation, transferia_core::ChangeOperation::Delete);
    update.operation = transferia_core::ChangeOperation::Update;
    update.changed_columns.fill(0);
    assert!(super::reconcile_overlap(&mut update).is_err());
}

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
