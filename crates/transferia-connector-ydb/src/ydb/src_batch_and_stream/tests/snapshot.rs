use super::*;
use arrow::array::{Array, Int64Array};
use crate::ydb::config::YdbTableConfig;
use crate::ydb::types::{column_plans, dataset_schema};
use ydb_grpc::ydb_proto::{Type, r#type::{PrimitiveTypeId, Type as TypeKind}};
use ydb_grpc::ydb_proto::table::ColumnMeta;

fn table() -> DiscoveredTable {
    let columns = column_plans(vec![ColumnMeta { name: "id".into(),
        r#type: Some(Type { r#type: Some(TypeKind::TypeId(PrimitiveTypeId::Int64 as i32)) }),
        ..Default::default() }], &["id".into()]).unwrap();
    DiscoveredTable { config: YdbTableConfig { path: "/db/t".into() }, schema: dataset_schema(&columns), columns }
}

#[test]
fn snapshot_keys_reject_duplicates_within_and_across_batches() {
    let table = table();
    let batch = |ids| RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(ids)) as ArrayRef)]).unwrap();
    let mut previous = None;
    validate_snapshot_keys(&table, &batch(vec![1, 2]), &mut previous).unwrap();
    assert!(validate_snapshot_keys(&table, &batch(vec![2, 3]), &mut previous).is_err());
    assert!(validate_snapshot_keys(&table, &batch(vec![4, 4]), &mut None).is_err());
}

#[test]
fn snapshot_uses_cdc_schema_zero_version_and_no_fabricated_timestamp() {
    let table = table();
    let kinds = [SystemColumnKind::Topic, SystemColumnKind::Partition, SystemColumnKind::Offset, SystemColumnKind::MessageIndex];
    let mut fields = vec![arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false)];
    fields.extend(kinds.map(|kind| arrow::datatypes::Field::new(kind.default_name(), kind.data_type(), false)));
    let input = TableData::new(Arc::from("t"), false, RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), vec![
        Arc::new(Int64Array::from(vec![7])), Arc::new(StringArray::from(vec!["/db/t"])),
        Arc::new(Int64Array::from(vec![0])), Arc::new(Int64Array::from(vec![42])), Arc::new(UInt64Array::from(vec![0])),
    ]).unwrap(), SystemColumns::new(kinds.iter().enumerate().map(|(index, kind)| SystemColumn { kind: *kind, index: index + 1, name: Arc::from(kind.default_name()) }).collect::<Vec<_>>()));
    let result = materialize_snapshot(&table, "/db", input).unwrap();
    assert_eq!(result.batch.schema(), build_table_schema(&table).unwrap());
    let version = result.batch.column_by_name("_system_source_version").unwrap().as_any().downcast_ref::<UInt64Array>().unwrap();
    assert_eq!(version.value(0), 0);
    assert_eq!(result.batch.column_by_name("_system_source_timestamp_ms").unwrap().null_count(), 1);
    assert_eq!(result.batch.column_by_name("_system_source_transaction_id").unwrap().null_count(), 1);
    assert_eq!(result.batch.column_by_name("_system_write_timestamp_ms").unwrap().null_count(), 1);
    assert_eq!(result.batch.column_by_name("_system_offset").unwrap().as_any().downcast_ref::<Int64Array>().unwrap().value(0), 42);
    assert_eq!(result.batch.column_by_name("_system_change_operation").unwrap().as_any().downcast_ref::<StringArray>().unwrap().value(0), "r");
}
