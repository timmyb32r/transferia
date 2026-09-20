use super::*;

fn raw() -> RawTableFacts {
    RawTableFacts {
        physical_access: PhysicalAccess::GuardedHeap,
        native_tid_ranges: true,
        heap_pages: 20_000,
        block_bytes: 8192,
        estimated_rows: Some(2_000_000.0),
        estimated_output_bytes: Some(160_000_000.0),
        metadata_seconds: 0.001,
        request_seconds: 0.005,
        indexed_boundary_seconds: 0.005,
        indexed_cuts: 0,
        index_correlation: None,
        output_distribution: OutputDistribution::UniformPrior,
        rates: CostRates::priors(),
    }
}

fn count(value: u32) -> NonZeroU32 { NonZeroU32::new(value).unwrap() }

#[test]
fn projected_cost_prior_rejects_inconsistent_field_counts() {
    assert!(CostRates::priors_for_projected_columns(count(1), 2).is_err());
    assert!(CostRates::priors_for_projected_columns(count(u32::MAX - 1), u32::MAX).is_err());
    let maximum = CostRates::priors_for_projected_columns(count(u32::MAX), u32::MAX).unwrap();
    assert!(maximum.rows_per_second().is_finite() && maximum.rows_per_second() > 0.0);
}

#[test]
fn unprojected_columns_preserve_all_existing_rate_priors() {
    for columns in [1, 8, 105, u32::MAX] {
        let ordinary = CostRates::priors_for_columns(count(columns));
        let projected = CostRates::priors_for_projected_columns(count(columns), 0).unwrap();
        assert_eq!(projected.rows_per_second().to_bits(), ordinary.rows_per_second().to_bits());
        assert_eq!(projected.heap_bytes_per_second(), ordinary.heap_bytes_per_second());
        assert_eq!(projected.output_bytes_per_second(), ordinary.output_bytes_per_second());
        assert_eq!(projected.hash_rows_per_second, ordinary.hash_rows_per_second);
    }
}

#[test]
fn numeric_projection_prior_has_explicit_per_row_units() {
    for (numeric_fields, expected_nanoseconds) in [(0, 180.0), (4, 580.0), (8, 980.0)] {
        let rates = CostRates::priors_for_projected_columns(count(8), numeric_fields).unwrap();
        assert!((1e9 / rates.rows_per_second() - expected_nanoseconds).abs() < 1e-9);
        assert_eq!(rates.heap_bytes_per_second(), CostRates::priors().heap_bytes_per_second());
        assert_eq!(rates.output_bytes_per_second(), CostRates::priors().output_bytes_per_second());
        assert_eq!(rates.hash_rows_per_second, CostRates::priors().hash_rows_per_second);
    }
}

#[test]
fn projected_row_work_is_charged_once_to_the_reading_lane() {
    let mut input = raw();
    input.estimated_rows = Some(1000.0);
    input.rates = CostRates::priors_for_projected_columns(count(8), 0).unwrap();
    let ordinary = TableFacts::try_from(input.clone()).unwrap();
    input.rates = CostRates::priors_for_projected_columns(count(8), 4).unwrap();
    let projected = TableFacts::try_from(input).unwrap();
    for (strategy, lanes) in [(Strategy::Single, 1), (Strategy::Ctid, 4), (Strategy::HashCtid, 4)] {
        let before = estimate(&ordinary, strategy, lanes).unwrap();
        let after = estimate(&projected, strategy, lanes).unwrap();
        let expected_extra_seconds = 0.0004 / f64::from(lanes);
        assert!((after.row_seconds - before.row_seconds - expected_extra_seconds).abs() < 1e-12);
        assert_eq!(after.heap_seconds, before.heap_seconds);
        assert_eq!(after.output_seconds, before.output_seconds);
        assert_eq!(after.setup_seconds, before.setup_seconds);
        assert_eq!(after.planning_seconds, before.planning_seconds);
    }
}

#[test]
fn deferred_indexed_boundary_cost_is_validated_before_a_decision() {
    for seconds in [-1.0, f64::INFINITY, f64::NAN] {
        let mut input = raw();
        input.indexed_boundary_seconds = seconds;
        assert!(TableFacts::try_from(input).is_err());
    }
    let mut overflow = raw();
    overflow.metadata_seconds = f64::MAX;
    overflow.indexed_boundary_seconds = f64::MAX;
    assert!(TableFacts::try_from(overflow).is_err());
    let mut known_zero = raw();
    known_zero.indexed_boundary_seconds = 0.0;
    assert!(TableFacts::try_from(known_zero).is_ok());
}

#[test]
fn materialized_indexed_boundaries_are_not_charged_twice() {
    let mut input = raw();
    input.indexed_cuts = 16;
    input.index_correlation = Some(1.0);
    input.metadata_seconds = 0.002;
    input.indexed_boundary_seconds = 0.007;
    let deferred = TableFacts::try_from(input.clone()).unwrap();
    assert!((estimate(&deferred, Strategy::Indexed, 4).unwrap().planning_seconds - 0.009).abs() < 1e-12);
    assert_eq!(estimate(&deferred, Strategy::Indexed, 1).unwrap().planning_seconds, 0.002);
    assert_eq!(estimate(&deferred, Strategy::Ctid, 4).unwrap().planning_seconds, 0.002);

    // The adapter measured the boundary request: move its duration into the
    // shared metadata total and mark the remaining indexed work as zero.
    input.metadata_seconds = 0.009;
    input.indexed_boundary_seconds = 0.0;
    let materialized = TableFacts::try_from(input).unwrap();
    assert_eq!(estimate(&materialized, Strategy::Indexed, 4).unwrap().planning_seconds, 0.009);
}

fn uncertain_external_work() -> RawTableFacts {
    let mut input = raw();
    input.heap_pages = 3052;
    input.estimated_rows = Some(200_000.0);
    input.estimated_output_bytes = Some(145_000_000.0);
    input.request_seconds = 0.018;
    input.indexed_boundary_seconds = 0.018;
    input.indexed_cuts = 100;
    input.index_correlation = Some(1.0);
    input.output_distribution = OutputDistribution::UnobservedConcentration;
    input.rates = CostRates::priors_for_columns(count(9));
    input
}

#[test]
fn expensive_information_cannot_beat_an_available_robust_hash_plan() {
    let facts = TableFacts::try_from(uncertain_external_work()).unwrap();
    let constraints = Constraints::new(Some(count(8)));
    let information = evaluate_observation(&facts, constraints, 0.079).unwrap();
    assert!(!information.should_observe());
    assert_eq!(information.reason(), "observation_priced_out_against_robust_alternative");
    assert_eq!(information.robust_strategy(), Strategy::HashCtid);
    assert!(information.robust_lanes() > 1);
    assert!(information.maximum_saving_seconds() < 0.034);
    assert!(information.priced_probe_seconds() > information.maximum_saving_seconds());
    assert!(information.robust_seconds() > information.ideal_seconds());
    let decision = decide(facts, constraints).unwrap();
    assert_eq!(decision.strategy(), Strategy::HashCtid);
    assert_eq!(decision.reason(), "lowest_estimated_cost_under_unobserved_output_concentration");
    // A conservative Auto score does not remove correct forced alternatives.
    assert!(decision.candidates().iter().filter(|candidate| matches!(candidate.strategy, Strategy::Ctid | Strategy::Indexed)).all(|candidate| candidate.eligible));
}

#[test]
fn cheap_information_and_expensive_heap_work_change_probe_economics() {
    let facts = TableFacts::try_from(uncertain_external_work()).unwrap();
    let constraints = Constraints::new(Some(count(8)));
    assert!(evaluate_observation(&facts, constraints, 0.001).unwrap().should_observe());
    let mut input = uncertain_external_work();
    input.rates = CostRates::new(40.0 * 1024.0 * 1024.0, 268_435_456.0, 5_000_000.0, 20_000_000.0).unwrap();
    let expensive_heap = TableFacts::try_from(input).unwrap();
    assert!(evaluate_observation(&expensive_heap, constraints, 0.079).unwrap().should_observe());
    // These are explicit cost scenarios, never an inference that a heap is hot
    // or cold based on its size or the machine on which the test runs.
}

#[test]
fn one_part_and_unavailable_native_ranges_cannot_buy_useless_information() {
    let facts = TableFacts::try_from(uncertain_external_work()).unwrap();
    let one = evaluate_observation(&facts, Constraints::new(Some(count(1))), 0.0).unwrap();
    assert!(!one.should_observe());
    assert_eq!(one.robust_strategy(), Strategy::Single);
    assert_eq!(one.robust_lanes(), 1);
    assert_eq!(one.maximum_saving_seconds(), 0.0);
    let mut input = uncertain_external_work();
    input.native_tid_ranges = false;
    assert!(!evaluate_observation(&TableFacts::try_from(input).unwrap(), Constraints::default(), 0.0).unwrap().should_observe());
}

#[test]
fn distribution_variants_replace_uncertainty_without_changing_coverage() {
    let mut input = uncertain_external_work();
    let uncertain = TableFacts::try_from(input.clone()).unwrap();
    assert_eq!(estimate(&uncertain, Strategy::Ctid, 4).unwrap().largest_output_fraction, 1.0);
    assert_eq!(estimate(&uncertain, Strategy::Indexed, 4).unwrap().largest_output_fraction, 1.0);
    input.output_distribution = OutputDistribution::UniformPrior;
    let uniform = TableFacts::try_from(input.clone()).unwrap();
    assert_eq!(estimate(&uniform, Strategy::Ctid, 4).unwrap().largest_output_fraction, 0.25);
    input.output_distribution = OutputDistribution::Observed(PhysicalDistribution::new(vec![1.0; 4], 100, 4).unwrap());
    let observed = TableFacts::try_from(input).unwrap();
    assert!(observed.has_observation());
    assert_eq!(observed.output_distribution().label(), "physical_observation");
    assert_eq!(estimate(&observed, Strategy::Ctid, 4).unwrap().largest_output_fraction, 0.25);
    let forced = |facts| chunks(&decide_for_evaluation(facts, Constraints::default(), Strategy::Ctid, count(4)).unwrap()).unwrap();
    let coverage = forced(uniform);
    assert_eq!(forced(uncertain), coverage);
    assert_eq!(forced(observed.clone()), coverage);
    assert!(evaluate_observation(&observed, Constraints::default(), 0.0).is_err());
}

#[test]
fn observation_price_validation_rejects_nonfinite_or_negative_costs() {
    let facts = TableFacts::try_from(uncertain_external_work()).unwrap();
    for price in [-1.0, f64::NAN, f64::INFINITY] {
        assert!(evaluate_observation(&facts, Constraints::default(), price).is_err());
    }
}

#[test]
fn rejected_facts_never_reach_a_decision() {
    let mut value = raw();
    value.estimated_rows = Some(f64::NAN);
    assert!(TableFacts::try_from(value).is_err());
    let mut value = raw();
    value.rates = CostRates::new(f64::MIN_POSITIVE, f64::MIN_POSITIVE, f64::MIN_POSITIVE, f64::MIN_POSITIVE).unwrap();
    assert!(TableFacts::try_from(value).is_err());
    let mut value = raw();
    value.estimated_output_bytes = Some(-1.0);
    assert!(TableFacts::try_from(value).is_err());
    let mut value = raw();
    value.request_seconds = 0.0;
    assert!(TableFacts::try_from(value).is_err());
    let mut value = raw();
    value.heap_pages = u64::from(u32::MAX) + 1;
    assert!(TableFacts::try_from(value).is_err());
    let mut value = raw();
    value.index_correlation = Some(1.01);
    assert!(TableFacts::try_from(value).is_err());
    assert!(CostRates::new(0.0, 1.0, 1.0, 1.0).is_err());
    assert!(PhysicalDistribution::new(vec![0.0], 1, 1).is_err());
    assert!(PhysicalDistribution::new(vec![1.0], 0, 1).is_err());
    assert!(PhysicalDistribution::new(vec![f64::INFINITY], 1, 1).is_err());
}

#[test]
fn maximum_is_a_ceiling_and_a_one_part_override_keeps_complete_query() {
    let facts = TableFacts::try_from(raw()).unwrap();
    let decision = decide(facts.clone(), Constraints::new(Some(count(1)))).unwrap();
    assert_eq!(decision.strategy(), Strategy::Single);
    assert_eq!(decision.lanes(), 1);
    assert_eq!(decision.initial_parts(), 1);
    assert_eq!(chunks(&decision).unwrap()[0].kind(), &ChunkKind::Whole);
    assert!(decide_for_evaluation(facts, Constraints::new(Some(count(4))), Strategy::Ctid, count(5)).is_err());
}

#[test]
fn zero_and_unknown_estimates_do_not_create_an_empty_plan() {
    for rows in [None, Some(0.0)] {
        let mut input = raw();
        input.estimated_rows = rows;
        input.estimated_output_bytes = None;
        input.heap_pages = 0;
        let decision = decide(TableFacts::try_from(input).unwrap(), Constraints::default()).unwrap();
        assert_eq!(decision.strategy(), Strategy::Single);
        assert_eq!(chunks(&decision).unwrap(), vec![Chunk { kind: ChunkKind::Whole }]);
    }
}

#[test]
fn unsupported_relation_and_postgres_versions_exclude_tid_scanning() {
    let mut input = raw();
    input.physical_access = PhysicalAccess::CompleteQueryOnly;
    let decision = decide(TableFacts::try_from(input).unwrap(), Constraints::default()).unwrap();
    assert_eq!(decision.strategy(), Strategy::Single);
    assert!(decision.candidates().iter().filter(|candidate| candidate.strategy != Strategy::Single).all(|candidate| !candidate.eligible));
    let mut input = raw();
    input.native_tid_ranges = false;
    let decision = decide(TableFacts::try_from(input).unwrap(), Constraints::default()).unwrap();
    let ctid = decision.candidates().iter().find(|candidate| candidate.strategy == Strategy::Ctid).unwrap();
    assert!(!ctid.eligible);
    assert_eq!(ctid.reason, "postgresql_14_native_tid_range_scan_required");
}

#[test]
fn ctid_parts_have_exact_adjacent_bounds_and_open_tails() {
    for pages in [1, 2, 3, 31, u64::from(u32::MAX)] {
        let mut input = raw();
        input.heap_pages = pages;
        for parts in [1, 2, 3, 7].into_iter().filter(|parts| u64::from(*parts) <= pages) {
            let decision = decide_for_evaluation(TableFacts::try_from(input.clone()).unwrap(), Constraints::default(), Strategy::Ctid, count(parts)).unwrap();
            let descriptors = chunks(&decision).unwrap();
            assert_eq!(descriptors.len(), parts as usize);
            let mut previous = None;
            for (index, descriptor) in descriptors.iter().enumerate() {
                let ChunkKind::Ctid { lower_page, upper_page, estimated_end_page } = descriptor.kind() else { panic!("wrong strategy") };
                assert_eq!(*lower_page, previous);
                assert!(descriptor.page_span().unwrap() >= 1);
                assert!(*estimated_end_page <= pages);
                if index + 1 == parts as usize { assert!(upper_page.is_none()); }
                previous = *upper_page;
            }
        }
    }
}

#[test]
fn refinement_covers_exact_parent_without_mutating_it() {
    let decision = decide_for_evaluation(TableFacts::try_from(raw()).unwrap(), Constraints::default(), Strategy::Ctid, count(2)).unwrap();
    for parent in chunks(&decision).unwrap() {
        let original = parent.clone();
        let (left, right) = parent.split_pending().unwrap();
        assert_eq!(parent, original);
        assert_eq!(left.page_span().unwrap() + right.page_span().unwrap(), parent.page_span().unwrap());
        let ChunkKind::Ctid { lower_page: original_lower, upper_page: original_upper, .. } = parent.kind() else { unreachable!() };
        let ChunkKind::Ctid { lower_page: left_lower, upper_page: left_upper, .. } = left.kind() else { unreachable!() };
        let ChunkKind::Ctid { lower_page: right_lower, upper_page: right_upper, .. } = right.kind() else { unreachable!() };
        assert_eq!(left_lower, original_lower);
        assert_eq!(left_upper, right_lower);
        assert_eq!(right_upper, original_upper);
    }
}

#[test]
fn hash_descriptor_covers_every_bucket_and_cannot_be_refined() {
    let decision = decide_for_evaluation(TableFacts::try_from(raw()).unwrap(), Constraints::default(), Strategy::HashCtid, count(7)).unwrap();
    let descriptors = chunks(&decision).unwrap();
    for (bucket, descriptor) in descriptors.iter().enumerate() {
        assert_eq!(descriptor.kind(), &ChunkKind::HashCtid { bucket: bucket as u32, modulus: count(7) });
        assert!(descriptor.split_pending().is_none());
    }
    for hash in [i32::MIN, -100, -1, 0, 1, i32::MAX] {
        let normalized = ((i64::from(hash) % 7) + 7) % 7;
        assert!((0..7).contains(&normalized));
    }
}

#[test]
fn indexed_cut_references_are_distinct_and_retain_both_tails() {
    let mut input = raw();
    input.indexed_cuts = 9;
    input.index_correlation = Some(1.0);
    let decision = decide_for_evaluation(TableFacts::try_from(input).unwrap(), Constraints::default(), Strategy::Indexed, count(4)).unwrap();
    let descriptors = chunks(&decision).unwrap();
    let mut previous = None;
    for (index, chunk) in descriptors.iter().enumerate() {
        let ChunkKind::Indexed { lower_cut, upper_cut } = chunk.kind() else { panic!("wrong strategy") };
        assert_eq!(*lower_cut, previous);
        assert!(upper_cut.is_none_or(|cut| cut < 9));
        if let (Some(lower), Some(upper)) = (lower_cut, upper_cut) { assert!(lower < upper); }
        if index == 3 { assert!(upper_cut.is_none()); }
        previous = *upper_cut;
    }
}

#[test]
fn replay_is_deterministic_but_does_not_freeze_a_strategy_winner() {
    let first = decide(TableFacts::try_from(raw()).unwrap(), Constraints::new(Some(count(7)))).unwrap();
    let second = decide(TableFacts::try_from(raw()).unwrap(), Constraints::new(Some(count(7)))).unwrap();
    assert_eq!(format!("{first:?}"), format!("{second:?}"));
    assert!(first.initial_parts() <= 7);
    assert!(first.lanes() <= first.initial_parts());
    assert!(first.candidates().iter().filter_map(|candidate| candidate.cost.as_ref()).all(|cost| cost.total_seconds.is_finite() && cost.total_seconds > 0.0));
}

#[test]
fn queue_refinement_is_only_paid_for_when_observed_work_balances_better() {
    let mut input = raw();
    input.output_distribution = OutputDistribution::Observed(PhysicalDistribution::new(vec![1.0; 16], 1600, 16).unwrap());
    let facts = TableFacts::try_from(input.clone()).unwrap();
    assert_eq!(queued_parts(&facts, 4, 64).0, 4);
    input.output_distribution = OutputDistribution::Observed(PhysicalDistribution::new(vec![0.0, 0.0, 0.0, 100.0], 100, 1).unwrap());
    input.request_seconds = 0.0001;
    let facts = TableFacts::try_from(input).unwrap();
    let (parts, fraction, _) = queued_parts(&facts, 4, 64);
    assert!(parts > 4 && parts <= 64);
    assert!(fraction.unwrap() < 1.0);
    assert_eq!(queued_parts(&facts, 4, 4).0, 4);
}

#[test]
fn histogram_integration_is_bounded_for_large_candidate_counts() {
    let distribution = PhysicalDistribution::new(vec![1.0; 16], 1600, 16).unwrap();
    assert!((distribution.largest_fraction(4) - 0.25).abs() < 1e-10);
    let fraction = distribution.largest_fraction(u32::MAX);
    assert!(fraction > 0.0 && fraction < 0.000001);
}

fn observed(mut input: RawTableFacts, weights: Vec<f64>) -> TableFacts {
    input.output_distribution = OutputDistribution::Observed(PhysicalDistribution::new(weights, 1600, input.heap_pages.min(16)).unwrap());
    TableFacts::try_from(input).unwrap()
}

#[test]
fn weighted_ranges_require_observation_native_tid_access_and_valid_work() {
    let facts = TableFacts::try_from(raw()).unwrap();
    assert!(decide_for_evaluation(facts, Constraints::default(), Strategy::WeightedCtid, count(2)).is_err());
    assert!(CtidRangeWork::new(0, 0.5).is_err());
    assert!(CtidRangeWork::new(1, f64::NAN).is_err());
    assert!(CtidRangeWork::new(1, -0.1).is_err());
    assert!(CtidRangeWork::new(1, 1.1).is_err());
    let mut input = raw();
    input.native_tid_ranges = false;
    let facts = observed(input, vec![1.0; 16]);
    assert!(decide_for_evaluation(facts, Constraints::default(), Strategy::WeightedCtid, count(2)).is_err());
    let facts = observed(raw(), vec![1.0; 16]);
    assert!(WeightedCtidLayout::new(&facts, 0).is_err());
    assert!(WeightedCtidLayout::new(&facts, 20_001).is_err());
    assert!(decide_for_evaluation(facts, Constraints::new(Some(count(2))), Strategy::WeightedCtid, count(3)).is_err());
}

#[test]
fn weighted_ranges_cover_every_page_with_open_tails_even_with_empty_bins() {
    for pages in [1, 2, 3, 31, 1000, u64::from(u32::MAX)] {
        let mut input = raw();
        input.heap_pages = pages;
        for weights in [vec![1.0; 16], vec![0.0, 1.0, 0.0], vec![0.0, 0.0, 1.0]] {
            let facts = observed(input.clone(), weights);
            for parts in [1, 2, 3, 7, 17, 64, 65, 128].into_iter().filter(|parts| u64::from(*parts) <= pages) {
                let decision = decide_for_evaluation(facts.clone(), Constraints::default(), Strategy::WeightedCtid, count(parts)).unwrap();
                let descriptors = chunks(&decision).unwrap();
                let mut previous = None;
                let mut covered = 0;
                for descriptor in &descriptors {
                    let ChunkKind::Ctid { lower_page, upper_page, .. } = descriptor.kind() else { panic!("weighted strategy must use native TID descriptors") };
                    assert_eq!(*lower_page, previous);
                    assert!(descriptor.page_span().unwrap() >= 1);
                    covered += descriptor.page_span().unwrap();
                    previous = *upper_page;
                    if let Some((left, right)) = descriptor.split_pending() {
                        assert_eq!(left.page_span().unwrap() + right.page_span().unwrap(), descriptor.page_span().unwrap());
                    }
                }
                assert_eq!(covered, pages);
                assert_eq!(descriptors.len(), parts as usize);
                assert!(previous.is_none());
            }
        }
    }
}

#[test]
fn weighted_random_access_cuts_match_sequential_unique_rounding_in_dense_middle() {
    let mut input = raw();
    input.heap_pages = 129;
    input.estimated_output_bytes = Some(1e15);
    let mut weights = vec![0.0; 16];
    weights[7] = 100.0;
    let facts = observed(input, weights);
    for parts in [2, 7, 32, 64, 100, 129] {
        let layout = WeightedCtidLayout::new(&facts, parts).unwrap();
        let mut previous = 0;
        for part in 1..parts {
            let expected = layout.raw_boundary(part).max(previous + 1)
                .min(layout.pages - u64::from(parts - part));
            assert_eq!(layout.boundary(part), expected, "parts={parts}, part={part}");
            previous = expected;
        }
    }
}

#[test]
fn weighted_critical_cost_pays_for_long_empty_heap_span() {
    let mut input = raw();
    input.heap_pages = 1600;
    input.estimated_output_bytes = Some(1e12);
    let mut weights = vec![0.0; 16];
    weights[15] = 100.0;
    let facts = observed(input, weights);
    let cost = estimate(&facts, Strategy::WeightedCtid, 2).unwrap();
    let serial = estimate(&facts, Strategy::Single, 1).unwrap();
    let layout = WeightedCtidLayout::new(&facts, 2).unwrap();
    let long_range = layout.range_work(0, layout.boundary(1)).unwrap();
    assert!(long_range.heap_pages() > 1400);
    let long_heap_cost = long_range.heap_pages() as f64 * layout.heap_seconds_per_page;
    assert!(long_heap_cost > serial.heap_seconds * 0.9);
    let critical = cost.critical_range.unwrap();
    // A rounded page can make either range critical. Whichever wins must pay
    // its actual span, and total estimated work must cover the long range too.
    let expected_heap = critical.heap_pages() as f64 * layout.heap_seconds_per_page;
    assert!((cost.heap_seconds - expected_heap).abs() < 1e-12);
    assert!((cost.heap_seconds - serial.heap_seconds / 2.0).abs() > serial.heap_seconds * 0.4);
    assert!(cost.heap_seconds + cost.row_seconds + cost.output_seconds + 1e-9 >= layout.work_seconds(long_range));
    assert!(critical.output_fraction() > 0.49 && critical.output_fraction() < 0.51);
    assert_eq!(cost.total_seconds, cost.heap_seconds + cost.row_seconds + cost.output_seconds
        + cost.setup_seconds + cost.coordination_seconds + cost.planning_seconds);
}

#[test]
fn weighted_large_plan_cost_bounds_all_actual_ranges_with_page_rounding_uncertainty() {
    let mut input = raw();
    input.heap_pages = 10_000;
    input.estimated_output_bytes = Some(1e10);
    let facts = observed(input, vec![0.0, 1.0, 0.0, 19.0, 0.0, 2.0, 0.0, 3.0]);
    for parts in [33, 65, 127, 257, 4096] {
        let layout = WeightedCtidLayout::new(&facts, parts).unwrap();
        let (critical, _) = layout.critical_work().unwrap();
        let bound = layout.work_seconds(critical);
        let actual = (0..parts).map(|part| layout.work_seconds(
            layout.range_work(layout.boundary(part), layout.boundary(part + 1)).unwrap()
        )).fold(0.0_f64, f64::max);
        let one_page = layout.bin_work.iter().copied().fold(0.0_f64, f64::max)
            / (layout.pages as f64 / layout.bin_work.len() as f64);
        assert!(bound + 1e-9 >= actual, "parts={parts}, bound={bound}, actual={actual}");
        assert!(bound <= actual + one_page + 1e-9, "parts={parts}, bound={bound}, actual={actual}");
    }
}

#[test]
fn weighted_huge_candidate_count_does_not_materialize_trial_descriptors() {
    let mut input = raw();
    input.heap_pages = u64::from(u32::MAX);
    let facts = observed(input, vec![0.0, 1.0, 0.0, 19.0]);
    let layout = WeightedCtidLayout::new(&facts, u32::MAX).unwrap();
    let (critical, _) = layout.critical_work().unwrap();
    assert_eq!(critical.heap_pages(), 1);
    for part in [0, 1, 15, u32::MAX / 2, u32::MAX - 1, u32::MAX] {
        assert_eq!(layout.boundary(part), u64::from(part));
    }
}
