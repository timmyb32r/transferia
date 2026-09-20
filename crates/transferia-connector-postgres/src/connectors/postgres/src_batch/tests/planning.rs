use super::*;
use super::super::planner::{chunks, decide_for_evaluation};

fn plan(strategy: Strategy, parts: u32) -> PreparedTable {
    let facts = TableFacts::try_from(RawTableFacts {
        physical_access: PhysicalAccess::GuardedHeap,
        native_tid_ranges: true,
        heap_pages: 16,
        block_bytes: 8192,
        estimated_rows: Some(100.0),
        estimated_output_bytes: Some(12800.0),
        metadata_seconds: 0.01,
        request_seconds: 0.005,
        indexed_cuts: 3,
        indexed_boundary_seconds: 0.0,
        index_correlation: Some(1.0),
        output_distribution: if strategy == Strategy::WeightedCtid {
            OutputDistribution::Observed(PhysicalDistribution::new(vec![0.0, 0.0, 1.0, 9.0], 100, 4).unwrap())
        } else { OutputDistribution::UniformPrior },
        rates: CostRates::priors(),
    }).unwrap();
    let decision = decide_for_evaluation(facts, Constraints::default(), strategy, NonZeroU32::new(parts).unwrap()).unwrap();
    let chunks = chunks(&decision).unwrap();
    PreparedTable {
        decision, chunks,
        physical_identity: PhysicalIdentity { relation_oid: 1, relfilenode: 1, relation_kind: "r".to_owned(), access_method_oid: 2, schema: "public".to_owned(), table: "example".to_owned() },
        typed_cuts: Some(TypedCuts { column: "value\"quoted".to_owned(), sql_type: "text".to_owned(), values: vec!["a".to_owned(), "can't\\escape".to_owned(), "z".to_owned()] }),
    }
}

#[test]
fn ctid_predicates_keep_native_type_and_open_tails() {
    let plan = plan(Strategy::Ctid, 2);
    assert_eq!(plan.predicate(&plan.chunks[0]).unwrap(), "ctid OPERATOR(pg_catalog.<) '(8,0)'::pg_catalog.tid");
    assert_eq!(plan.predicate(&plan.chunks[1]).unwrap(), "ctid OPERATOR(pg_catalog.>=) '(8,0)'::pg_catalog.tid");
    let (left, right) = plan.chunks[1].split_pending().unwrap();
    assert_eq!(plan.predicate(&left).unwrap(), "ctid OPERATOR(pg_catalog.>=) '(8,0)'::pg_catalog.tid AND ctid OPERATOR(pg_catalog.<) '(12,0)'::pg_catalog.tid");
    assert_eq!(plan.predicate(&right).unwrap(), "ctid OPERATOR(pg_catalog.>=) '(12,0)'::pg_catalog.tid");
}

#[test]
fn typed_keys_are_exactly_escaped_without_logging_values() {
    let plan = plan(Strategy::Indexed, 2);
    let predicate = plan.predicate(&plan.chunks[0]).unwrap();
    assert_eq!(predicate, "\"value\"\"quoted\" OPERATOR(pg_catalog.<) E'can''t\\\\escape'::text");
    assert!(!format!("{plan:?}").contains("can't"));
    assert!(!format!("{plan:?}").contains("value\"quoted"));
    assert_eq!(quote_literal("\\x00'"), "E'\\\\x00''' ".trim_end());
}

#[test]
fn hash_cast_precedes_modulo_and_all_buckets_use_same_modulus() {
    let plan = plan(Strategy::HashCtid, 4);
    for (index, chunk) in plan.chunks.iter().enumerate() {
        assert_eq!(plan.predicate(chunk).unwrap(), format!("(((pg_catalog.hashtid(ctid)::pg_catalog.int8 OPERATOR(pg_catalog.%) 4) OPERATOR(pg_catalog.+) 4) OPERATOR(pg_catalog.%) 4) OPERATOR(pg_catalog.=) {index}"));
    }
}

#[test]
fn whole_table_predicate_is_unrestricted() {
    let plan = plan(Strategy::Single, 1);
    assert_eq!(plan.predicate(&plan.chunks[0]).unwrap(), "TRUE");
}

#[test]
fn weighted_predicates_and_refinement_use_native_tid_ranges() {
    let weighted = plan(Strategy::WeightedCtid, 2);
    for chunk in &weighted.chunks {
        let predicate = weighted.predicate(chunk).unwrap();
        assert!(predicate.contains("::pg_catalog.tid"));
        if let Some((left, right)) = chunk.split_pending() {
            assert!(weighted.predicate(&left).unwrap().contains("ctid"));
            assert!(weighted.predicate(&right).unwrap().contains("ctid"));
        }
    }
    let unrelated = plan(Strategy::Indexed, 2);
    assert!(weighted.predicate(&unrelated.chunks[0]).is_err());
}

#[test]
fn small_heap_large_toast_counterexample_gets_complete_page_widths() {
    // The real 200k-row fixture has 3052 heap pages and ~164 MB COPY output.
    // Sparse SYSTEM sampling observed only ~21 MB; changing bin count alone
    // cannot find a wide tail on pages that were never inspected.
    let mut types = vec![1700; 8];
    types.push(25);
    let observation = ObservationPlan::choose(3052, 8192, 145_000_000.0, Some(200_000.0), &types, 0.018).unwrap();
    assert_eq!(observation.percent, 100.0);
    assert_eq!(observation.bins, 3052);
    assert_eq!(observation.reason, "complete_native_width_observation_repays_external_output_uncertainty");
    assert_eq!(observation.numeric_storage_proxy_columns, 8);
    assert!(observation.full_width_seconds < observation.potential_saving_seconds);
}

#[test]
fn full_observation_can_be_admissible_but_priced_out_against_hash() {
    let types = [1700, 1700, 1700, 1700, 1700, 1700, 1700, 1700, 25];
    let observation = ObservationPlan::choose(3052, 8192, 145_000_000.0, Some(200_000.0), &types, 0.018).unwrap();
    assert_eq!(observation.percent, 100.0);
    let facts = TableFacts::try_from(RawTableFacts {
        physical_access: PhysicalAccess::GuardedHeap, native_tid_ranges: true,
        heap_pages: 3052, block_bytes: 8192, estimated_rows: Some(200_000.0),
        estimated_output_bytes: Some(145_000_000.0), metadata_seconds: 0.018,
        request_seconds: 0.018, indexed_cuts: 100, indexed_boundary_seconds: 0.0, index_correlation: Some(1.0),
        output_distribution: OutputDistribution::UnobservedConcentration,
        rates: CostRates::priors_for_columns(NonZeroU32::new(9).unwrap()),
    }).unwrap();
    let information = planner::evaluate_observation(&facts, Constraints::new(NonZeroU32::new(8)), observation.estimated_probe_seconds()).unwrap();
    assert!(!information.should_observe());
    assert!(information.priced_probe_seconds() > information.maximum_saving_seconds());
    assert_eq!(information.robust_strategy(), Strategy::HashCtid);
}

#[test]
fn complete_observation_excludes_expensive_or_unknown_projection() {
    // JSONB/array/custom types may need conversion or detoasting. Their
    // existing bounded sample stays available; they never trigger a full scan.
    for oid in [3802, 1009, 900_000] {
        let observation = ObservationPlan::choose(3052, 8192, 145_000_000.0, Some(200_000.0), &[20, oid], 0.018).unwrap();
        assert!(observation.percent < 100.0);
        assert_eq!(observation.reason, "sample_only_expensive_or_unknown_width_projection");
    }
}

#[test]
fn numeric_storage_proxy_alone_cannot_establish_raw_external_output_work() {
    let observation = ObservationPlan::choose(3052, 8192, 145_000_000.0, Some(200_000.0), &[20, 1700], 0.018).unwrap();
    assert!(observation.percent < 100.0);
    assert_eq!(observation.reason, "sample_only_no_raw_external_width_column");
}

#[test]
fn full_width_observation_requires_both_small_heap_and_potential_saving() {
    let no_toast = ObservationPlan::choose(3052, 8192, 0.0, Some(200_000.0), &[20, 25], 0.018).unwrap();
    assert!(no_toast.percent < 100.0);
    let large_heap = ObservationPlan::choose(1_000_000, 8192, 100_000_000_000.0, Some(200_000.0), &[20, 25], 0.018).unwrap();
    assert!(large_heap.percent < 100.0);
    assert_eq!(large_heap.reason, "sample_only_heap_inspection_exceeds_reader_setup");
    let costly_rows = ObservationPlan::choose(3052, 8192, 145_000_000.0, Some(100_000_000.0), &[20, 25], 0.018).unwrap();
    assert!(costly_rows.percent < 100.0);
    assert_eq!(costly_rows.reason, "sample_only_full_width_work_exceeds_potential_saving");
}

#[test]
fn histogram_resolution_ceiling_never_rejects_or_samples_away_a_large_selected_heap() {
    let observation = ObservationPlan::choose(100_000, 8192, 100_000_000_000.0, Some(1_000_000.0), &[20, 17], 1.0).unwrap();
    assert_eq!(observation.percent, 100.0);
    assert_eq!(observation.bins, 4096);
    let one_page = ObservationPlan::choose(1, 8192, 100_000_000.0, None, &[20, 25], 0.01).unwrap();
    assert_eq!(one_page.percent, 100.0);
    assert_eq!(one_page.bins, 1);
}

#[test]
fn observation_policy_rejects_invalid_facts_before_queries() {
    for rows in [Some(-1.0), Some(f64::INFINITY), Some(f64::NAN)] {
        assert!(ObservationPlan::choose(1, 8192, 0.0, rows, &[20], 0.01).is_err());
    }
    assert!(ObservationPlan::choose(0, 8192, 0.0, None, &[20], 0.01).is_err());
    assert!(ObservationPlan::choose(1, 0, 0.0, None, &[20], 0.01).is_err());
    assert!(ObservationPlan::choose(1, 8192, -1.0, None, &[20], 0.01).is_err());
    assert!(ObservationPlan::choose(1, 8192, 0.0, None, &[], 0.01).is_err());
    assert!(ObservationPlan::choose(1, 8192, 0.0, None, &[20], 0.0).is_err());
}
