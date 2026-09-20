use std::collections::BTreeMap;

use super::{captured_closure, SnapshotQueryScope};

#[test]
fn selected_tables_keep_separate_relation_closures() {
    let relations = BTreeMap::from([
        (1, vec![3]), (2, vec![4]), (3, vec![5]), (4, vec![]), (5, vec![]),
    ]);
    assert_eq!(captured_closure(1, &relations).unwrap(), vec![1, 3, 5]);
    assert_eq!(captured_closure(2, &relations).unwrap(), vec![2, 4]);
}

#[test]
fn multiple_inheritance_paths_do_not_duplicate_a_physical_relation() {
    let relations = BTreeMap::from([
        (1, vec![2, 3]), (2, vec![4]), (3, vec![4]), (4, vec![]),
    ]);
    assert_eq!(captured_closure(1, &relations).unwrap(), vec![1, 2, 3, 4]);
}

#[test]
fn missing_guards_and_invalid_oids_fail_before_snapshot_export() {
    assert!(captured_closure(1, &BTreeMap::from([(1, vec![2])])).is_err());
    assert!(captured_closure(0, &BTreeMap::from([(0, vec![])])).is_err());
}

#[test]
fn a_leaf_uses_only_and_parent_filters_exact_postgres_owned_oids() {
    let leaf = SnapshotQueryScope { only: true, relation_oids: vec![42] };
    assert_eq!(leaf.from_sql("a\"b", "c\"d"), "ONLY \"a\"\"b\".\"c\"\"d\"");
    assert_eq!(leaf.membership_predicate(), "TRUE");
    let parent = SnapshotQueryScope { only: false, relation_oids: vec![42, u32::MAX] };
    assert_eq!(parent.from_sql("public", "events"), "\"public\".\"events\"");
    assert_eq!(parent.membership_predicate(), "tableoid OPERATOR(pg_catalog.=) ANY (ARRAY[42,4294967295]::pg_catalog.oid[])");
}
