//! A dependency-free policy for a PostgreSQL table snapshot.
//!
//! # Contract
//!
//! [`TableFacts`] separates the current heap size from catalog estimates. A
//! missing row estimate is **unknown**, never an empty table. The SQL adapter
//! must collect facts under the retained snapshot and relation guards; this
//! module neither opens a connection nor establishes those external guarantees.
//! It accepts only finite, nonnegative estimates and rejects inconsistent input.
//!
//! [`decide`] compares one scan, equal-page TID ranges, weighted TID ranges,
//! indexed key ranges and a hash of the native TID. Eligibility is checked before cost. TID strategies
//! require a guarded physical heap; key ranges require an adapter-proven native
//! ordering and typed boundaries. Unsupported relations retain one complete
//! scan. Estimates decide performance, never which rows belong to the snapshot.
//!
//! TID ranges use half-open page boundaries and leave the final upper tail
//! open. A catalog row count, histogram endpoint or captured relation size can
//! therefore never silently remove rows. Hash buckets cover the complete TID
//! hash domain. Indexed descriptors refer to server-ordered cuts, with both
//! outer tails open; the adapter owns their types and SQL rendering.
//!
//! # Costs and uncertainty
//!
//! The model estimates seconds from pages, live rows, output bytes and measured
//! request latency. Built-in rates are explicit **priors**, not limits or a
//! claim about server capacity. They are intentionally visible in the decision
//! report and can be replaced by calibrated rates in an evaluator. A table
//! fitting in memory does not establish that it is cached. Without observations
//! the policy does not assume physically clustered skew merely from TOAST size.
//! [`CostRates::priors_for_projected_columns`] adds a type-projection cost
//! without importing PostgreSQL or Arrow types into this module. Its 100 ns per
//! NUMERIC-to-text field corrects the total row-work prior; it is not a measured
//! isolated conversion time. Digit count, NULLs, host capacity and cache state
//! still affect actual cost. Deferred indexed-boundary work is charged once;
//! work already included in metadata time must not be charged again.
//! Weighted TID ranges additionally require a physical output histogram. They
//! place page cuts at quantiles of combined heap-read and output/row work, then
//! preserve at least one page in every range. Empty sampled bins still cost
//! heap reads and remain covered. Costs use the actual unequal heap spans and
//! output fractions of the same critical range, rather than assuming heap/P.
//! A forced weighted evaluation still collects and pays for its observation.
//!
//! Large external storage may conceal a concentrated output tail without
//! establishing that such skew exists. [`OutputDistribution`] records that
//! uncertainty explicitly. Until observed, range costs allow all output in one
//! range; hash retains its probabilistic distribution assumption. Before buying
//! an observation, [`evaluate_observation`] compares its price with the best
//! single/hash plan and a hypothetical perfectly balanced physical plan. If
//! even perfect information cannot repay the probe, Auto keeps the robust plan.
//! This accounts for information value against an available alternative, rather
//! than comparing every probe only with one serial scan.
//!
//! Connections start concurrently: setup has a shared latency plus a small
//! per-lane scheduling term. Query coordination is a separate cost prior,
//! informed by release measurements; it must not be mislabeled as serialized
//! connection establishment. Repeated heap and tuple work
//! makes hash partitioning more expensive, while a measured physical histogram
//! can show that its improved balance repays that work. The useful concurrency
//! search is bounded mathematically by setup cost exceeding the complete
//! single-scan estimate, rather than by a hidden reader or server-CPU limit.
//! The optional maximum-parts setting is a ceiling on leaf tasks, not an exact
//! count or a concurrency budget. The runtime can later apply its own budget.
//!
//! [`Chunk::split_pending`] is the only refinement operation: it produces two
//! disjoint children covering exactly one TID parent. The queue must atomically
//! replace an **unassigned** parent, account for every completed/active/pending
//! leaf against the cap, and preserve snapshot/acknowledgement ownership.
//! Splitting running work or switching algorithms mid-snapshot is not supported.
//!
//! # Evaluation
//!
//! [`decide_for_evaluation`] uses the same eligibility, cost and descriptors;
//! it only fixes the candidate and part count. It is deliberately absent from
//! product configuration. Compare actual end-to-end snapshot times in Rust
//! release mode on the same snapshot and constraints. A robust slowdown above
//! 5% fails the performance evaluator; these priors cannot prove universal
//! optimality. Counterexamples should extend the fixture corpus, not freeze a
//! named strategy as the permanent winner.

use std::error::Error;
use std::fmt;
use std::num::NonZeroU32;
use std::collections::BTreeSet;

/// A physical strategy; labels are stable, credential-free diagnostic values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Strategy {
    Single,
    Ctid,
    WeightedCtid,
    Indexed,
    HashCtid,
}

impl Strategy {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Single => "single_scan",
            Self::Ctid => "ctid_ranges",
            Self::WeightedCtid => "weighted_ctid_ranges",
            Self::Indexed => "indexed_ranges",
            Self::HashCtid => "hash_ctid",
        }
    }
}

/// Eligibility comes from catalog capabilities and retained relation guards.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PhysicalAccess {
    GuardedHeap,
    CompleteQueryOnly,
}

/// Rates use seconds, bytes and rows; they are estimates, never safety limits.
#[derive(Clone, Copy, Debug)]
pub struct CostRates {
    heap_bytes_per_second: f64,
    output_bytes_per_second: f64,
    rows_per_second: f64,
    hash_rows_per_second: f64,
}

impl CostRates {
    pub fn heap_bytes_per_second(self) -> f64 { self.heap_bytes_per_second }
    pub fn output_bytes_per_second(self) -> f64 { self.output_bytes_per_second }
    pub fn rows_per_second(self) -> f64 { self.rows_per_second }

    pub fn new(heap: f64, output: f64, rows: f64, hash_rows: f64) -> Result<Self, PlannerError> {
        for value in [heap, output, rows, hash_rows] {
            positive(value, "cost rate")?;
        }
        Ok(Self {
            heap_bytes_per_second: heap,
            output_bytes_per_second: output,
            rows_per_second: rows,
            hash_rows_per_second: hash_rows,
        })
    }

    /// Deliberately uncalibrated starting rates, emitted in the plan report.
    pub fn priors() -> Self {
        Self {
            heap_bytes_per_second: 1_073_741_824.0,
            output_bytes_per_second: 268_435_456.0,
            rows_per_second: 10_000_000.0,
            hash_rows_per_second: 20_000_000.0,
        }
    }

    /// Account for per-field conversion/framing work without coupling policy
    /// to Arrow or SQL types. The rates remain workload-independent priors.
    pub fn priors_for_columns(columns: NonZeroU32) -> Self {
        let mut rates = Self::priors();
        rates.rows_per_second = 1.0 / (1.0 / rates.rows_per_second + f64::from(columns.get()) / 100_000_000.0);
        rates
    }

    /// Row-work priors for the projection actually read from PostgreSQL.
    /// `numeric_text_columns` counts NUMERIC fields converted to text per row
    /// and must not exceed the positive total column count. Zero preserves the
    /// ordinary column prior exactly. All returned rates are finite/positive;
    /// transport, heap and hash rates are unchanged.
    ///
    /// The extra 100 ns per projected NUMERIC field is an uncertain empirical
    /// correction to total row work, not an isolated conversion measurement.
    /// Matched dense/sparse release cases suggested roughly 78--103 ns beyond
    /// the generic row/field prior under plausible wire-cost assumptions; the
    /// incremental text-versus-native conversion estimate was smaller. This
    /// scalar prior does not model arbitrary digit counts or NULL frequency.
    /// The adapter identifies actual conversions; no dataset identity or
    /// winning strategy influences this constructor. Cheap storage-width
    /// inspection without text conversion uses `priors_for_columns` instead.
    pub fn priors_for_projected_columns(columns: NonZeroU32, numeric_text_columns: u32) -> Result<Self, PlannerError> {
        if numeric_text_columns > columns.get() {
            return Err(PlannerError("NUMERIC-to-text field count exceeds total columns"));
        }
        let mut rates = Self::priors_for_columns(columns);
        if numeric_text_columns > 0 {
            rates.rows_per_second = 1.0 / (1.0 / rates.rows_per_second + f64::from(numeric_text_columns) * 100e-9);
        }
        Ok(rates)
    }
}

/// A page-position histogram observed in this snapshot. Weights estimate output
/// work, not row identity. Equal bins cover the captured heap's page interval.
/// Empty sampled bins are estimates; they are never removed from a plan.
#[derive(Clone, Debug)]
pub struct PhysicalDistribution {
    weights: Vec<f64>,
    sampled_rows: u64,
    sampled_pages: u64,
}

/// What is known about output work along physical/key ranges. These variants
/// cannot contradict one another: an observation replaces an unobserved prior.
/// UniformPrior is an explicit model assumption, not measured uniformity.
/// UnobservedConcentration is a risk signal, not a claim that skew was found.
#[derive(Clone, Debug)]
pub enum OutputDistribution {
    UniformPrior,
    UnobservedConcentration,
    Observed(PhysicalDistribution),
}

impl OutputDistribution {
    pub fn label(&self) -> &'static str {
        match self {
            Self::UniformPrior => "uniform_prior",
            Self::UnobservedConcentration => "unobserved_concentration",
            Self::Observed(_) => "physical_observation",
        }
    }

    fn observation(&self) -> Option<&PhysicalDistribution> {
        match self { Self::Observed(value) => Some(value), _ => None }
    }
}

impl PhysicalDistribution {
    pub fn new(weights: Vec<f64>, sampled_rows: u64, sampled_pages: u64) -> Result<Self, PlannerError> {
        if weights.is_empty() || sampled_rows == 0 || sampled_pages == 0 || sampled_pages > sampled_rows {
            return Err(PlannerError("a physical observation needs bins, rows and pages"));
        }
        let mut total = 0.0;
        for weight in &weights {
            nonnegative(*weight, "physical histogram weight")?;
            total += weight;
        }
        positive(total, "physical histogram total")?;
        Ok(Self { weights, sampled_rows, sampled_pages })
    }

    pub fn sampled_rows(&self) -> u64 { self.sampled_rows }
    pub fn sampled_pages(&self) -> u64 { self.sampled_pages }
    /// Numeric workload estimates per equal-width physical bin; no row values.
    pub fn weights(&self) -> &[f64] { &self.weights }

    fn largest_fraction(&self, parts: u32) -> f64 {
        let total = self.weights.iter().sum::<f64>();
        let bins = self.weights.len() as f64;
        let mut largest: f64 = 0.0;
        // Integration uses fractions of observed bins. It cannot infer within-
        // bin skew; confidence remains an estimate even for many observations.
        // Only partitions adjacent to histogram boundaries can change density.
        // Avoid a loop over billions of candidate lanes for a very large heap.
        let mut adjacent = BTreeSet::from([0, parts - 1]);
        for boundary in 0..=self.weights.len() {
            let at = ((boundary as f64) * f64::from(parts) / bins).floor();
            if at < f64::from(parts) { adjacent.insert(at as u32); }
            if at >= 1.0 { adjacent.insert((at as u32) - 1); }
        }
        for part in adjacent {
            let begin = f64::from(part) * bins / f64::from(parts);
            let end = f64::from(part + 1) * bins / f64::from(parts);
            let mut weight = 0.0;
            for (index, value) in self.weights.iter().enumerate() {
                let index = index as f64;
                let overlap = (end.min(index + 1.0) - begin.max(index)).max(0.0);
                weight += value * overlap;
            }
            largest = largest.max(weight / total);
        }
        largest
    }
}

/// Unvalidated metadata input. `heap_pages` is a current physical fact, while
/// optional live rows/output bytes can be missing or stale catalog estimates.
#[derive(Clone, Debug)]
pub struct RawTableFacts {
    pub physical_access: PhysicalAccess,
    pub native_tid_ranges: bool,
    pub heap_pages: u64,
    pub block_bytes: u32,
    pub estimated_rows: Option<f64>,
    pub estimated_output_bytes: Option<f64>,
    pub metadata_seconds: f64,
    pub request_seconds: f64,
    /// Finite, nonnegative remaining cost of obtaining typed indexed cuts.
    /// Zero means no extra work: eager/materialized cuts are already charged
    /// in metadata_seconds. Applied only to indexed plans with multiple lanes.
    pub indexed_boundary_seconds: f64,
    pub indexed_cuts: u32,
    pub index_correlation: Option<f64>,
    pub output_distribution: OutputDistribution,
    pub rates: CostRates,
}

#[derive(Clone, Debug)]
pub struct TableFacts(RawTableFacts);

impl TryFrom<RawTableFacts> for TableFacts {
    type Error = PlannerError;

    fn try_from(raw: RawTableFacts) -> Result<Self, Self::Error> {
        // A BlockNumber is uint32; UINT32_MAX denotes InvalidBlockNumber.
        if raw.heap_pages > u64::from(u32::MAX) || raw.block_bytes == 0 {
            return Err(PlannerError("physical heap size is outside PostgreSQL block addressing"));
        }
        for estimate in [raw.estimated_rows, raw.estimated_output_bytes].into_iter().flatten() {
            nonnegative(estimate, "catalog estimate")?;
        }
        nonnegative(raw.metadata_seconds, "metadata duration")?;
        positive(raw.request_seconds, "request duration")?;
        nonnegative(raw.indexed_boundary_seconds, "indexed boundary duration")?;
        nonnegative(raw.metadata_seconds + raw.indexed_boundary_seconds, "indexed planning duration")?;
        if raw.index_correlation.is_some_and(|value| !value.is_finite() || value.abs() > 1.0) {
            return Err(PlannerError("index correlation must be finite and within [-1, 1]"));
        }
        if raw.output_distribution.observation().is_some_and(|value| value.sampled_pages > raw.heap_pages) {
            return Err(PlannerError("sampled physical pages exceed the current heap size"));
        }
        if raw.indexed_cuts == u32::MAX {
            return Err(PlannerError("indexed cut count leaves no address for its final open range"));
        }
        let facts = Self(raw);
        for strategy in [Strategy::Single, Strategy::Ctid, Strategy::Indexed, Strategy::HashCtid] {
            positive(estimate(&facts, strategy, 1)?.total_seconds, "snapshot cost overflow")?;
        }
        Ok(facts)
    }
}

impl TableFacts {
    pub fn heap_pages(&self) -> u64 { self.0.heap_pages }
    pub fn estimated_rows(&self) -> Option<f64> { self.0.estimated_rows }
    pub fn estimated_output_bytes(&self) -> Option<f64> { self.0.estimated_output_bytes }
    pub fn physical_access(&self) -> PhysicalAccess { self.0.physical_access }
    pub fn indexed_cuts(&self) -> u32 { self.0.indexed_cuts }
    /// Estimated seconds still needed to collect native indexed boundaries.
    pub fn indexed_boundary_seconds(&self) -> f64 { self.0.indexed_boundary_seconds }
    pub fn rates(&self) -> CostRates { self.0.rates }
    pub fn has_observation(&self) -> bool { self.physical_distribution().is_some() }
    pub fn physical_distribution(&self) -> Option<&PhysicalDistribution> { self.0.output_distribution.observation() }
    pub fn output_distribution(&self) -> &OutputDistribution { &self.0.output_distribution }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Constraints {
    max_parts: Option<NonZeroU32>,
}

impl Constraints {
    pub const fn new(max_parts: Option<NonZeroU32>) -> Self { Self { max_parts } }
    pub fn max_parts(self) -> Option<u32> { self.max_parts.map(NonZeroU32::get) }
}

#[derive(Clone, Debug)]
pub struct CostEstimate {
    pub heap_seconds: f64,
    pub row_seconds: f64,
    pub output_seconds: f64,
    pub setup_seconds: f64,
    pub coordination_seconds: f64,
    pub planning_seconds: f64,
    pub total_seconds: f64,
    pub heap_passes: f64,
    pub largest_output_fraction: f64,
    /// Weighted TID components describe the same critical range. Its heap span
    /// may be much larger than 1/P even when output is approximately balanced.
    pub critical_range: Option<CtidRangeWork>,
}

/// A cost observation, never a row-coverage boundary. The private constructor
/// validates a positive physical span and a finite fraction of observed output.
/// Histogram uncertainty changes estimated work only; it cannot exclude pages.
#[derive(Clone, Copy, Debug)]
pub struct CtidRangeWork {
    heap_pages: u64,
    output_fraction: f64,
}

impl CtidRangeWork {
    fn new(heap_pages: u64, output_fraction: f64) -> Result<Self, PlannerError> {
        if heap_pages == 0 || !output_fraction.is_finite() || !(0.0..=1.0).contains(&output_fraction) {
            return Err(PlannerError("invalid physical range work estimate"));
        }
        Ok(Self { heap_pages, output_fraction })
    }

    pub fn heap_pages(self) -> u64 { self.heap_pages }
    pub fn output_fraction(self) -> f64 { self.output_fraction }
}

/// Quantiles of combined heap and output work, with one or more pages in every
/// range. Integer cuts use the prefix maximum of `raw_cut(k) - k`: this is the
/// exact result of moving each cut past its predecessor, while reserving one
/// page for each remaining range. Independent rounding would duplicate cuts
/// inside a dense middle bin. Histogram breakpoints suffice to evaluate that
/// prefix maximum without walking or allocating P trial ranges.
struct WeightedCtidLayout<'a> {
    pages: u64,
    parts: u32,
    distribution: &'a PhysicalDistribution,
    bin_work: Vec<f64>,
    cumulative_work: Vec<f64>,
    transitions: BTreeSet<u32>,
    output_weight: f64,
    heap_seconds_per_page: f64,
    output_seconds: f64,
}

impl<'a> WeightedCtidLayout<'a> {
    fn new(facts: &'a TableFacts, parts: u32) -> Result<Self, PlannerError> {
        let pages = facts.heap_pages();
        if parts == 0 || pages == 0 || u64::from(parts) > pages {
            return Err(PlannerError("weighted physical ranges require at least one page per part"));
        }
        let distribution = facts.physical_distribution()
            .ok_or(PlannerError("weighted physical ranges require an observation"))?;
        let raw = &facts.0;
        let heap_bytes = pages as f64 * f64::from(raw.block_bytes);
        let heap_seconds_per_page = f64::from(raw.block_bytes) / raw.rates.heap_bytes_per_second;
        let output_seconds = raw.estimated_rows.unwrap_or(heap_bytes / 128.0) / raw.rates.rows_per_second
            + raw.estimated_output_bytes.unwrap_or(heap_bytes) / raw.rates.output_bytes_per_second;
        let output_weight = distribution.weights.iter().sum::<f64>();
        let mut bin_work = Vec::new();
        let mut cumulative_work = vec![0.0];
        let mut total = 0.0;
        for weight in &distribution.weights {
            let work = heap_bytes / raw.rates.heap_bytes_per_second / distribution.weights.len() as f64
                + output_seconds * (*weight / output_weight);
            positive(work, "weighted physical bin cost overflow")?;
            total += work;
            positive(total, "weighted physical cost overflow")?;
            bin_work.push(work);
            cumulative_work.push(total);
        }
        let mut transitions = BTreeSet::from([1]);
        for cumulative in &cumulative_work {
            let at = (cumulative / total * f64::from(parts)).floor() as u32;
            for part in [at.checked_sub(1), Some(at), at.checked_add(1)].into_iter().flatten() {
                if part > 0 && part < parts { transitions.insert(part); }
            }
        }
        Ok(Self { pages, parts, distribution, bin_work, cumulative_work, transitions, output_weight, heap_seconds_per_page, output_seconds })
    }

    fn total_work(&self) -> f64 { self.cumulative_work[self.cumulative_work.len() - 1] }

    fn raw_boundary(&self, part: u32) -> u64 {
        let target = self.total_work() * (f64::from(part) / f64::from(self.parts));
        let bin = self.cumulative_work.windows(2).position(|bounds| target < bounds[1])
            .unwrap_or(self.distribution.weights.len() - 1);
        let lower = self.cumulative_work[bin];
        let fraction = ((target - lower) / self.bin_work[bin]).clamp(0.0, 1.0);
        ((bin as f64 + fraction) * self.pages as f64 / self.distribution.weights.len() as f64).round() as u64
    }

    fn boundary(&self, part: u32) -> u64 {
        if part == 0 { return 0; }
        if part == self.parts { return self.pages; }
        let mut shift = 0;
        for at in self.transitions.range(..=part).copied().chain(std::iter::once(part)) {
            let raw = self.raw_boundary(at);
            if raw > u64::from(at) { shift = shift.max(raw - u64::from(at)); }
        }
        u64::from(part) + shift.min(self.pages - u64::from(self.parts))
    }

    fn range_work(&self, begin: u64, end: u64) -> Result<CtidRangeWork, PlannerError> {
        if begin >= end || end > self.pages { return Err(PlannerError("invalid weighted physical range boundaries")); }
        let bins = self.distribution.weights.len() as f64;
        let lower = begin as f64 * bins / self.pages as f64;
        let upper = end as f64 * bins / self.pages as f64;
        let weight = self.distribution.weights.iter().enumerate().map(|(index, weight)| {
            let at = index as f64;
            weight * (upper.min(at + 1.0) - lower.max(at)).clamp(0.0, 1.0)
        }).sum::<f64>();
        CtidRangeWork::new(end - begin, weight / self.output_weight)
    }

    fn work_seconds(&self, work: CtidRangeWork) -> f64 {
        work.heap_pages as f64 * self.heap_seconds_per_page + work.output_fraction * self.output_seconds
    }

    fn first_boundary_at(&self, page: u64) -> u32 {
        let (mut lower, mut upper) = (0, self.parts);
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            if self.boundary(middle) < page { lower = middle + 1; } else { upper = middle; }
        }
        lower
    }

    /// Small plans cost every actual range. For very large P, inspect actual
    /// ranges crossing histogram boundaries and bound ranges within each bin.
    /// A uniform-bin quantile spans at most ceil(ideal pages), so the bound is
    /// conservative by less than one page of modeled work. This algorithmic
    /// switch limits computation, never the eligible or executable part count.
    fn critical_work(&self) -> Result<(CtidRangeWork, f64), PlannerError> {
        let mut candidates = BTreeSet::from([0, self.parts - 1]);
        let bins = self.distribution.weights.len();
        let mut within_bins = Vec::new();
        if u64::from(self.parts) <= bins as u64 * 4 {
            candidates.extend(0..self.parts);
        } else {
            for bin in 0..bins {
                let begin = (bin as u64 * self.pages).div_ceil(bins as u64);
                let end = (bin as u64 + 1) * self.pages / bins as u64;
                let first = self.first_boundary_at(begin);
                for part in [first.checked_sub(1), Some(first)].into_iter().flatten() {
                    if part < self.parts { candidates.insert(part); }
                }
                let after = if end == self.pages { self.parts } else { self.first_boundary_at(end + 1) - 1 };
                if after <= first { continue; }
                let bin_pages = self.pages as f64 / bins as f64;
                let density = self.bin_work[bin] / bin_pages;
                let ideal_pages = (self.total_work() / f64::from(self.parts) / density).ceil().max(1.0) as u64;
                let covered = self.boundary(after) - self.boundary(first);
                let span = ideal_pages.min(covered - u64::from(after - first - 1));
                let fraction = (self.distribution.weights[bin] / self.output_weight) * (span as f64 / bin_pages);
                within_bins.push(CtidRangeWork::new(span, fraction)?);
            }
        }
        let mut critical = self.range_work(0, self.boundary(1))?;
        let mut largest_output_fraction = 0.0_f64;
        for work in candidates.into_iter().map(|part| self.range_work(self.boundary(part), self.boundary(part + 1)))
            .chain(within_bins.into_iter().map(Ok))
        {
            let work = work?;
            largest_output_fraction = largest_output_fraction.max(work.output_fraction);
            if self.work_seconds(work) > self.work_seconds(critical) { critical = work; }
        }
        Ok((critical, largest_output_fraction))
    }
}

#[derive(Clone, Debug)]
pub struct Candidate {
    pub strategy: Strategy,
    pub lanes: u32,
    pub parts: u32,
    pub eligible: bool,
    pub reason: &'static str,
    pub cost: Option<CostEstimate>,
}

#[derive(Clone, Debug)]
pub struct Decision {
    strategy: Strategy,
    lanes: u32,
    initial_parts: u32,
    max_parts: Option<u32>,
    reason: &'static str,
    candidates: Vec<Candidate>,
    facts: TableFacts,
}

impl Decision {
    pub fn strategy(&self) -> Strategy { self.strategy }
    pub fn lanes(&self) -> u32 { self.lanes }
    pub fn initial_parts(&self) -> u32 { self.initial_parts }
    pub fn max_parts(&self) -> Option<u32> { self.max_parts }
    pub fn reason(&self) -> &'static str { self.reason }
    pub fn candidates(&self) -> &[Candidate] { &self.candidates }
    pub fn facts(&self) -> &TableFacts { &self.facts }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Chunk {
    kind: ChunkKind,
}

/// Boundary values remain in the SQL adapter. Indexed numbers identify cuts,
/// rather than encoding or comparing user data in Rust.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChunkKind {
    Whole,
    Ctid { lower_page: Option<u32>, upper_page: Option<u32>, estimated_end_page: u64 },
    Indexed { lower_cut: Option<u32>, upper_cut: Option<u32> },
    HashCtid { bucket: u32, modulus: NonZeroU32 },
}

impl Chunk {
    pub fn kind(&self) -> &ChunkKind { &self.kind }

    pub fn page_span(&self) -> Option<u64> {
        match self.kind {
            ChunkKind::Ctid { lower_page, upper_page, estimated_end_page } =>
                Some(upper_page.map_or(estimated_end_page, u64::from) - u64::from(lower_page.unwrap_or(0))),
            _ => None,
        }
    }

    pub fn split_pending(&self) -> Option<(Self, Self)> {
        let ChunkKind::Ctid { lower_page, upper_page, estimated_end_page } = self.kind else { return None };
        let lower = u64::from(lower_page.unwrap_or(0));
        let upper = upper_page.map_or(estimated_end_page, u64::from);
        if upper - lower < 2 { return None; }
        let middle = u32::try_from(lower + (upper - lower) / 2).ok()?;
        Some((
            Self { kind: ChunkKind::Ctid { lower_page, upper_page: Some(middle), estimated_end_page: u64::from(middle) } },
            Self { kind: ChunkKind::Ctid { lower_page: Some(middle), upper_page, estimated_end_page } },
        ))
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct PlannerError(&'static str);
impl fmt::Display for PlannerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result { formatter.write_str(self.0) }
}
impl Error for PlannerError {}

pub fn decide(facts: TableFacts, constraints: Constraints) -> Result<Decision, PlannerError> {
    choose(facts, constraints, None)
}

/// Priced value of a proposed observation under the same cost priors and part
/// ceiling as execution. Private fields retain finite, nonnegative costs.
#[derive(Clone, Debug)]
pub struct ObservationValue {
    priced_probe_seconds: f64,
    robust_strategy: Strategy,
    robust_lanes: u32,
    robust_seconds: f64,
    ideal_seconds: f64,
    maximum_saving_seconds: f64,
    should_observe: bool,
    reason: &'static str,
}

impl ObservationValue {
    pub fn priced_probe_seconds(&self) -> f64 { self.priced_probe_seconds }
    pub fn robust_strategy(&self) -> Strategy { self.robust_strategy }
    pub fn robust_lanes(&self) -> u32 { self.robust_lanes }
    pub fn robust_seconds(&self) -> f64 { self.robust_seconds }
    pub fn ideal_seconds(&self) -> f64 { self.ideal_seconds }
    pub fn maximum_saving_seconds(&self) -> f64 { self.maximum_saving_seconds }
    pub fn should_observe(&self) -> bool { self.should_observe }
    pub fn reason(&self) -> &'static str { self.reason }
}

/// Buy information only if a perfectly balanced physical plan could recover
/// its price versus an already available single/hash plan. At equal P, weighted
/// ranges save at most H*(1-1/P) + hash-row work versus hash; output, row and
/// setup terms cancel. Comparing only with Single would miss this bound and
/// spend more on probing than the repeated heap scan it hoped to avoid.
///
/// `priced_probe_seconds` includes the proposed database request and inspection
/// work; it is a prior, not measured time. A true result means information might
/// pay, never that it will. A forced evaluator may still buy the observation.
pub fn evaluate_observation(facts: &TableFacts, constraints: Constraints, priced_probe_seconds: f64) -> Result<ObservationValue, PlannerError> {
    nonnegative(priced_probe_seconds, "observation price must be finite and nonnegative")?;
    if facts.has_observation() { return Err(PlannerError("observation value requires unobserved input facts")); }
    let current = decide(facts.clone(), constraints)?;
    let (robust, robust_cost) = current.candidates().iter()
        .filter(|candidate| candidate.eligible && matches!(candidate.strategy, Strategy::Single | Strategy::HashCtid))
        .filter_map(|candidate| candidate.cost.as_ref().map(|cost| (candidate, cost)))
        .min_by(|left, right| left.1.total_seconds.total_cmp(&right.1.total_seconds))
        .ok_or(PlannerError("observation pricing has no robust baseline"))?;
    let mut ideal_raw = facts.0.clone();
    ideal_raw.output_distribution = OutputDistribution::UniformPrior;
    let ideal = decide(TableFacts::try_from(ideal_raw)?, constraints)?;
    let ideal_seconds = ideal.candidates().iter()
        .filter(|candidate| candidate.eligible && matches!(candidate.strategy, Strategy::Single | Strategy::Ctid))
        .filter_map(|candidate| candidate.cost.as_ref().map(|cost| cost.total_seconds))
        .min_by(f64::total_cmp)
        .ok_or(PlannerError("observation pricing has no ideal physical baseline"))?;
    let physical_candidate = ideal.candidates().iter()
        .any(|candidate| candidate.eligible && candidate.strategy == Strategy::Ctid && candidate.lanes > 1);
    let maximum_saving_seconds = (robust_cost.total_seconds - ideal_seconds).max(0.0);
    let should_observe = physical_candidate && priced_probe_seconds < maximum_saving_seconds;
    let reason = if !physical_candidate { "no_useful_physical_information_candidate" }
        else if should_observe { "perfect_information_could_repay_probe" }
        else { "observation_priced_out_against_robust_alternative" };
    Ok(ObservationValue {
        priced_probe_seconds, robust_strategy: robust.strategy, robust_lanes: robust.lanes,
        robust_seconds: robust_cost.total_seconds, ideal_seconds, maximum_saving_seconds,
        should_observe, reason,
    })
}

/// Evaluator-only override. Ineligible strategies and contradictory constraints
/// fail explicitly; an override never relaxes coverage or snapshot requirements.
pub fn decide_for_evaluation(facts: TableFacts, constraints: Constraints, strategy: Strategy, parts: NonZeroU32) -> Result<Decision, PlannerError> {
    if constraints.max_parts().is_some_and(|cap| parts.get() > cap) {
        return Err(PlannerError("forced evaluation parts exceed the configured maximum"));
    }
    choose(facts, constraints, Some((strategy, parts.get())))
}

pub fn chunks(decision: &Decision) -> Result<Vec<Chunk>, PlannerError> {
    let count = decision.initial_parts;
    let weighted = if decision.strategy == Strategy::WeightedCtid {
        Some(WeightedCtidLayout::new(&decision.facts, count)?)
    } else { None };
    let mut result = Vec::new();
    result.try_reserve_exact(count as usize).map_err(|_| PlannerError("cannot allocate snapshot descriptors"))?;
    for part in 0..count {
        let kind = match decision.strategy {
            Strategy::Single => ChunkKind::Whole,
            Strategy::Ctid => {
                let pages = decision.facts.heap_pages();
                let begin = u64::from(part) * pages / u64::from(count);
                let end = u64::from(part + 1) * pages / u64::from(count);
                ChunkKind::Ctid {
                    lower_page: (part != 0).then(|| u32::try_from(begin).expect("validated block address")),
                    upper_page: (part + 1 != count).then(|| u32::try_from(end).expect("validated block address")),
                    estimated_end_page: end,
                }
            }
            Strategy::WeightedCtid => {
                let layout = weighted.as_ref().ok_or(PlannerError("weighted descriptor layout is missing"))?;
                let begin = layout.boundary(part);
                let end = layout.boundary(part + 1);
                ChunkKind::Ctid {
                    lower_page: (part != 0).then(|| begin as u32),
                    upper_page: (part + 1 != count).then(|| end as u32),
                    estimated_end_page: end,
                }
            }
            Strategy::Indexed => {
                let cuts = u64::from(decision.facts.0.indexed_cuts);
                let at = |boundary: u32| -> u32 {
                    // A cut list need not have exactly P-1 elements. Select
                    // ordered, distinct cuts; omitted cuts do not remove rows.
                    ((u64::from(boundary) * (cuts + 1) / u64::from(count)) - 1) as u32
                };
                ChunkKind::Indexed { lower_cut: (part != 0).then(|| at(part)), upper_cut: (part + 1 != count).then(|| at(part + 1)) }
            }
            Strategy::HashCtid => ChunkKind::HashCtid { bucket: part, modulus: NonZeroU32::new(count).ok_or(PlannerError("zero hash buckets"))? },
        };
        result.push(Chunk { kind });
    }
    Ok(result)
}

fn choose(facts: TableFacts, constraints: Constraints, forced: Option<(Strategy, u32)>) -> Result<Decision, PlannerError> {
    let mut candidates = Vec::new();
    let serial = estimate(&facts, Strategy::Single, 1)?;
    positive(serial.total_seconds, "snapshot cost overflow")?;
    // Every lane incurs marginal scheduling and query coordination. Connection
    // latency itself is shared because readers connect concurrently.
    let setup = facts.0.request_seconds * 1.08;
    let useful = (serial.total_seconds / setup).ceil().max(1.0).min(f64::from(u32::MAX)) as u32;
    let physical = u32::try_from(facts.heap_pages().max(1)).map_err(|_| PlannerError("physical part count overflow"))?;
    let upper = useful.min(constraints.max_parts().unwrap_or(u32::MAX));
    let mut best: Option<(Strategy, u32, f64)> = None;
    for strategy in [Strategy::Single, Strategy::Ctid, Strategy::WeightedCtid, Strategy::Indexed, Strategy::HashCtid] {
        let reason = exclusion(&facts, strategy);
        if let Some(reason) = reason {
            candidates.push(Candidate { strategy, lanes: 0, parts: 0, eligible: false, reason, cost: None });
            continue;
        }
        let maximum = match strategy {
            Strategy::Single => 1,
            Strategy::Indexed => upper.min(facts.0.indexed_cuts.saturating_add(1)),
            Strategy::Ctid | Strategy::WeightedCtid => upper.min(physical),
            Strategy::HashCtid => upper,
        };
        let mut chosen = (1, estimate(&facts, strategy, 1)?);
        if let Some((selected, parts)) = forced {
            if strategy == selected {
                if (matches!(strategy, Strategy::Ctid | Strategy::WeightedCtid) && parts > physical) || (strategy == Strategy::Single && parts != 1) || (strategy == Strategy::Indexed && parts > facts.0.indexed_cuts.saturating_add(1)) {
                    return Err(PlannerError("forced part count is not supported by this candidate"));
                }
                chosen = (parts, estimate(&facts, strategy, parts)?);
            }
        } else {
            // Candidate-grid complexity grows logarithmically for huge tables.
            // Include the smooth-model minimum and physical-bin transitions;
            // this is a search grid, never an operational concurrency limit.
            for lanes in lane_candidates(&facts, strategy, maximum, setup)? {
                let cost = estimate(&facts, strategy, lanes)?;
                if cost.total_seconds < chosen.1.total_seconds { chosen = (lanes, cost); }
            }
        }
        let eligible_for_selection = forced.is_none_or(|(selected, _)| selected == strategy);
        if eligible_for_selection && best.as_ref().is_none_or(|value| chosen.1.total_seconds < value.2) {
            best = Some((strategy, chosen.0, chosen.1.total_seconds));
        }
        let candidate_reason = if matches!(facts.output_distribution(), OutputDistribution::UnobservedConcentration)
            && matches!(strategy, Strategy::Ctid | Strategy::Indexed) {
            "eligible_conservative_unobserved_output_concentration"
        } else { "eligible_cost_estimate" };
        candidates.push(Candidate { strategy, lanes: chosen.0, parts: chosen.0, eligible: true, reason: candidate_reason, cost: Some(chosen.1) });
    }
    let (strategy, lanes, _) = best.ok_or(PlannerError("forced strategy is not eligible for this table"))?;
    let reason = if forced.is_some() { "evaluator_override" }
        else if strategy == Strategy::Single { "parallel_setup_not_repaid_or_no_safe_split" }
        else if facts.has_observation() { "lowest_estimated_cost_with_physical_observation" }
        else if matches!(facts.output_distribution(), OutputDistribution::UnobservedConcentration) { "lowest_estimated_cost_under_unobserved_output_concentration" }
        else { "lowest_estimated_cost_using_uncalibrated_priors" };
    let (initial_parts, queue_fraction, extra_queries) = if forced.is_none() && strategy == Strategy::Ctid {
        queued_parts(&facts, lanes, physical.min(constraints.max_parts().unwrap_or(u32::MAX)))
    } else { (lanes, None, 0.0) };
    if initial_parts != lanes {
        if let Some(candidate) = candidates.iter_mut().find(|candidate| candidate.strategy == strategy) {
            candidate.parts = initial_parts;
            candidate.reason = "observed_tail_saving_repays_queued_queries";
            if let (Some(cost), Some(fraction)) = (&mut candidate.cost, queue_fraction) {
                let ratio = fraction / cost.largest_output_fraction;
                cost.row_seconds *= ratio;
                cost.output_seconds *= ratio;
                cost.largest_output_fraction = fraction;
                cost.coordination_seconds += extra_queries;
                cost.total_seconds = cost.heap_seconds + cost.row_seconds + cost.output_seconds + cost.setup_seconds + cost.coordination_seconds + cost.planning_seconds;
            }
        }
    }
    Ok(Decision { strategy, lanes, initial_parts, max_parts: constraints.max_parts(), reason, candidates, facts })
}

fn lane_candidates(facts: &TableFacts, strategy: Strategy, maximum: u32, setup: f64) -> Result<BTreeSet<u32>, PlannerError> {
    let mut candidates = BTreeSet::from([1, maximum]);
    let at_one = estimate(facts, strategy, 1)?;
    let divisible = at_one.output_seconds + at_one.row_seconds
        + if strategy == Strategy::HashCtid { 0.0 } else { at_one.heap_seconds };
    let minimum = (divisible / setup).sqrt().max(1.0).min(f64::from(maximum));
    for value in [minimum.floor(), minimum.ceil()] { candidates.insert(value as u32); }
    let mut power = 2_u32;
    while power <= maximum {
        candidates.insert(power);
        let Some(next) = power.checked_mul(2) else { break; };
        power = next;
    }
    if let Some(observation) = facts.physical_distribution() {
        let bins = u32::try_from(observation.weights.len()).unwrap_or(u32::MAX);
        candidates.extend(1..=maximum.min(bins));
    }
    Ok(candidates)
}

fn queued_parts(facts: &TableFacts, lanes: u32, maximum: u32) -> (u32, Option<f64>, f64) {
    let Some(distribution) = facts.physical_distribution() else { return (lanes, None, 0.0); };
    let raw = &facts.0;
    let bytes = raw.estimated_output_bytes.unwrap_or(raw.heap_pages as f64 * f64::from(raw.block_bytes));
    let row_work = raw.estimated_rows.unwrap_or(0.0) / raw.rates.rows_per_second;
    let output_work = bytes / raw.rates.output_bytes_per_second + row_work;
    let original_tail = distribution.largest_fraction(lanes) * output_work;
    let mut selected = lanes;
    let mut selected_fraction = None;
    let mut selected_overhead = 0.0;
    let mut best = original_tail;
    let total_weight = distribution.weights.iter().sum::<f64>();
    let bins = distribution.weights.len() as f64;
    let mut parts = lanes;
    while let Some(next) = parts.checked_mul(2).filter(|next| *next <= maximum) {
        parts = next;
        // Once query overhead alone exceeds the current tail estimate, further
        // refinement cannot repay its cost under this model.
        let extra_queries = f64::from(parts - lanes) * raw.request_seconds;
        if extra_queries >= best { break; }
        let mut completion = vec![0.0_f64; lanes as usize];
        for part in 0..parts {
            let begin = f64::from(part) * bins / f64::from(parts);
            let end = f64::from(part + 1) * bins / f64::from(parts);
            let weight = distribution.weights.iter().enumerate().map(|(index, value)| {
                let index = index as f64;
                value * (end.min(index + 1.0) - begin.max(index)).max(0.0)
            }).sum::<f64>() / total_weight;
            let lane = completion.iter().enumerate().min_by(|a, b| a.1.total_cmp(b.1)).map_or(0, |(index, _)| index);
            completion[lane] += weight * output_work;
        }
        let tail = completion.into_iter().fold(0.0_f64, f64::max);
        let estimated = tail + extra_queries;
        if estimated < best {
            selected = parts;
            best = estimated;
            selected_fraction = Some(tail / output_work);
            selected_overhead = extra_queries;
        }
    }
    (selected, selected_fraction, selected_overhead)
}

fn exclusion(facts: &TableFacts, strategy: Strategy) -> Option<&'static str> {
    match strategy {
        Strategy::Ctid | Strategy::WeightedCtid | Strategy::HashCtid if facts.physical_access() != PhysicalAccess::GuardedHeap => Some("guarded_physical_heap_required"),
        Strategy::Ctid | Strategy::WeightedCtid if !facts.0.native_tid_ranges => Some("postgresql_14_native_tid_range_scan_required"),
        Strategy::WeightedCtid if !facts.has_observation() => Some("physical_output_observation_required"),
        Strategy::Indexed if facts.0.indexed_cuts == 0 => Some("no_proven_index_order_and_typed_cuts"),
        _ => None,
    }
}

fn estimate(facts: &TableFacts, strategy: Strategy, lanes: u32) -> Result<CostEstimate, PlannerError> {
    let raw = &facts.0;
    let rates = raw.rates;
    let p = f64::from(lanes);
    let heap_bytes = raw.heap_pages as f64 * f64::from(raw.block_bytes);
    let rows = raw.estimated_rows.unwrap_or(heap_bytes / 128.0);
    let output_bytes = raw.estimated_output_bytes.unwrap_or(heap_bytes);
    let balanced = 1.0 / p;
    let weighted = if strategy == Strategy::WeightedCtid {
        Some(WeightedCtidLayout::new(facts, lanes)?.critical_work()?)
    } else { None };
    let largest_output_fraction = if let Some((_, fraction)) = weighted { fraction }
    else if matches!(raw.output_distribution, OutputDistribution::UnobservedConcentration)
        && matches!(strategy, Strategy::Ctid | Strategy::Indexed) { 1.0 }
    else if matches!(strategy, Strategy::Ctid) {
        facts.physical_distribution().map_or(balanced, |distribution| distribution.largest_fraction(lanes))
    } else { balanced };
    let heap_passes = if strategy == Strategy::HashCtid { p } else { 1.0 };
    let index_penalty = if strategy == Strategy::Indexed { 1.0 + 3.0 * (1.0 - raw.index_correlation.unwrap_or(0.0).abs()) } else { 1.0 };
    // Hash scans execute concurrently, but their total scan work still costs
    // source bandwidth/CPU. Do not divide P complete heaps into one heap/P.
    let critical_range = weighted.map(|(work, _)| work);
    let heap_seconds = if let Some(work) = critical_range {
        work.heap_pages as f64 * f64::from(raw.block_bytes) / rates.heap_bytes_per_second
    } else { heap_bytes / rates.heap_bytes_per_second * heap_passes / p * index_penalty };
    let critical_output_fraction = critical_range.map_or(largest_output_fraction, |work| work.output_fraction);
    let row_seconds = rows / rates.rows_per_second * critical_output_fraction
        + if strategy == Strategy::HashCtid { rows / rates.hash_rows_per_second } else { 0.0 };
    let output_seconds = output_bytes / rates.output_bytes_per_second * critical_output_fraction;
    // First release measurements show ~47 ms connection/import latency and
    // ~1 ms additional scheduling per lane at a ~12 ms metadata request. The
    // source is not assumed to establish all those connections serially.
    let setup_seconds = raw.request_seconds * (4.0 + 0.08 * (p - 1.0));
    // A pooled fit across small numeric, nullable and wide cases measured
    // ~10.6 ms/lane in the completed read phase. Keep it separate from setup;
    // it is an estimated coordination/short-stream cost, not a resource cap.
    let coordination_seconds = raw.request_seconds * p;
    // Charge only deferred typed-boundary work. Eager/materialized cuts are
    // already included in metadata_seconds and must not pay a second request.
    let planning_seconds = raw.metadata_seconds
        + if strategy == Strategy::Indexed && lanes > 1 { raw.indexed_boundary_seconds } else { 0.0 };
    Ok(CostEstimate {
        heap_seconds, row_seconds, output_seconds, setup_seconds, coordination_seconds, planning_seconds,
        total_seconds: heap_seconds + row_seconds + output_seconds + setup_seconds + coordination_seconds + planning_seconds,
        heap_passes, largest_output_fraction, critical_range,
    })
}

fn positive(value: f64, name: &'static str) -> Result<(), PlannerError> {
    if value.is_finite() && value > 0.0 { Ok(()) } else { Err(PlannerError(name)) }
}

fn nonnegative(value: f64, name: &'static str) -> Result<(), PlannerError> {
    if value.is_finite() && value >= 0.0 { Ok(()) } else { Err(PlannerError(name)) }
}

#[cfg(test)]
#[path = "tests/planner.rs"]
mod tests;
