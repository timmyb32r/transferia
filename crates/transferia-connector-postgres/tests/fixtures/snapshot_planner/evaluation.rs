//! Shared, deterministic evaluation bookkeeping. Database and wall-clock work
//! belongs to the explicitly invoked Rust release example, never these helpers.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize)]
pub struct Profile {
    pub id: String,
    pub table: String,
    pub columns: usize,
    pub live_divisor: u64,
    pub purpose: String,
}

#[derive(Deserialize)]
pub struct Corpus {
    pub version: u32,
    pub provenance: String,
    pub minimum_rows: u64,
    pub row_multiple: u64,
    pub profiles: Vec<Profile>,
}

impl Corpus {
    pub fn load() -> anyhow::Result<Self> {
        Ok(serde_json::from_str(include_str!("corpus.json"))?)
    }

    pub fn validate_scale(&self, rows: u64) -> anyhow::Result<()> {
        anyhow::ensure!(
            rows >= self.minimum_rows && rows % self.row_multiple == 0,
            "fixture rows must be at least {} and divisible by {}",
            self.minimum_rows,
            self.row_multiple
        );
        anyhow::ensure!(rows <= i64::MAX as u64 / 104_729, "fixture arithmetic exceeds BIGINT");
        Ok(())
    }
}

pub fn quote_identifier(value: &str) -> anyhow::Result<String> {
    anyhow::ensure!(!value.is_empty() && !value.contains('\0'), "invalid PostgreSQL identifier");
    anyhow::ensure!(value.len() <= 63, "PostgreSQL identifier exceeds 63 bytes");
    Ok(format!("\"{}\"", value.replace('"', "\"\"")))
}

pub fn qualified(schema: &str, table: &str) -> anyhow::Result<String> {
    Ok(format!("{}.{}", quote_identifier(schema)?, quote_identifier(table)?))
}

pub fn render_fixture(template: &str, schema: &str, rows: u64) -> anyhow::Result<String> {
    Corpus::load()?.validate_scale(rows)?;
    // profiles.sql includes a quoted dynamic SQL body. Restrict this *new test
    // schema name* explicitly; existing benchmark relations allow full quoted
    // identifiers through qualified(). No user identifier is rewritten.
    anyhow::ensure!(
        schema.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
        "new fixture schema accepts lowercase ASCII letters, digits and underscores only"
    );
    Ok(template
        .replace("{{schema}}", &quote_identifier(schema)?)
        .replace("{{rows}}", &format!("{rows}::bigint")))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Run {
    pub round: usize,
    pub variant: String,
    pub selected_strategy: String,
    pub selection_reason: String,
    pub total_seconds: f64,
    pub planning_seconds: f64,
    pub startup_seconds: f64,
    pub scan_seconds: f64,
    pub rows: u64,
    pub bytes: u64,
    pub lanes: usize,
    pub parts: usize,
    pub completed_parts: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    Pass,
    Regression,
    Inconclusive,
}

#[derive(Debug, Serialize)]
pub struct Comparison {
    pub candidate: String,
    pub auto_median_seconds: f64,
    pub candidate_median_seconds: f64,
    pub median_paired_ratio: f64,
    pub min_paired_ratio: f64,
    pub max_paired_ratio: f64,
    pub slower_pairs: usize,
    pub below_threshold_pairs: usize,
    pub threshold_ties: usize,
    pub pass_p_value: f64,
    pub regression_p_value: f64,
    pub regression_holm_p_value: f64,
    pub verdict: Verdict,
}

#[derive(Debug, Serialize)]
pub struct Gate {
    pub method: &'static str,
    pub tolerance_fraction: f64,
    pub significance_level: f64,
    pub regression_family_count: usize,
    pub regression_family_alpha: f64,
    pub repetitions: usize,
    pub best_measured_candidate: String,
    pub median_time_regret: f64,
    pub verdict: Verdict,
    pub comparisons: Vec<Comparison>,
}

pub fn median(values: &[f64]) -> anyhow::Result<f64> {
    anyhow::ensure!(
        !values.is_empty() && values.iter().all(|v| v.is_finite() && *v > 0.0),
        "timing samples must be finite and strictly positive"
    );
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let middle = sorted.len() / 2;
    Ok(if sorted.len() % 2 == 0 {
        sorted[middle - 1] / 2.0 + sorted[middle] / 2.0
    } else {
        sorted[middle]
    })
}

/// Exact one-sided sign tests compare the population median paired ratio with
/// 1.05. Independent round signs are required; select the number of rounds
/// before measuring, rather than repeatedly checking until a desired verdict.
/// Threshold ties count in N but support neither direction, conservatively.
///
/// Pass requires every candidate's lower-tail test at alpha .05, an
/// intersection-union test, plus observed Auto/best median time <= 1.05.
/// Regression requires a Holm-adjusted upper-tail p-value <= .05 / families.
/// Families is the preselected number of tables in this invocation; splitting
/// alpha across tables then applying Holm within each table controls overall
/// false regression despite dependent candidates sharing the same Auto runs.
/// The min/max interval is descriptive, not a statistical confidence interval.
/// Every candidate is compared, avoiding dependence on a frozen winner name.
pub fn compare(runs: &[Run], repetitions: usize, regression_families: usize) -> anyhow::Result<Gate> {
    const TOLERANCE: f64 = 0.05;
    const ALPHA: f64 = 0.05;
    anyhow::ensure!(repetitions >= 5, "performance acceptance needs at least five repetitions");
    anyhow::ensure!(regression_families > 0, "regression family count must be positive");
    let mut groups: BTreeMap<&str, BTreeMap<usize, &Run>> = BTreeMap::new();
    for run in runs {
        anyhow::ensure!(run.round < repetitions, "unexpected repetition index");
        let _ = median(&[run.total_seconds])?;
        anyhow::ensure!(
            groups.entry(&run.variant).or_default().insert(run.round, run).is_none(),
            "duplicate variant/repetition"
        );
    }
    anyhow::ensure!(groups.len() >= 2, "Auto needs at least one actual candidate");
    let automatic = groups.get("auto").ok_or_else(|| anyhow::anyhow!("missing Auto runs"))?;
    anyhow::ensure!(groups.values().all(|g| g.len() == repetitions), "incomplete paired rounds");
    let expected_rows = automatic[&0].rows;
    anyhow::ensure!(runs.iter().all(|r| r.rows == expected_rows), "row count differs across candidates");
    let auto_median = median(&automatic.values().map(|r| r.total_seconds).collect::<Vec<_>>())?;
    let sign_p_values = binomial_sign_p_values(repetitions);
    let regression_alpha = ALPHA / regression_families as f64;
    let mut comparisons = Vec::new();
    let mut best: Option<(String, f64)> = None;
    for (variant, candidate) in &groups {
        if *variant == "auto" { continue; }
        let candidate_median = median(&candidate.values().map(|r| r.total_seconds).collect::<Vec<_>>())?;
        if best.as_ref().is_none_or(|(_, time)| candidate_median < *time) {
            best = Some(((*variant).to_owned(), candidate_median));
        }
        let ratios = (0..repetitions)
            .map(|round| automatic[&round].total_seconds / candidate[&round].total_seconds)
            .collect::<Vec<_>>();
        let ratio = median(&ratios)?;
        let slower = ratios.iter().filter(|v| **v > 1.0 + TOLERANCE).count();
        let below = ratios.iter().filter(|v| **v < 1.0 + TOLERANCE).count();
        comparisons.push(Comparison {
            candidate: (*variant).to_owned(), auto_median_seconds: auto_median,
            candidate_median_seconds: candidate_median, median_paired_ratio: ratio,
            min_paired_ratio: ratios.iter().copied().fold(f64::INFINITY, f64::min),
            max_paired_ratio: ratios.iter().copied().fold(0.0, f64::max),
            slower_pairs: slower, below_threshold_pairs: below,
            threshold_ties: ratios.len() - slower - below,
            pass_p_value: sign_p_values[below],
            regression_p_value: sign_p_values[slower],
            regression_holm_p_value: 1.0,
            verdict: Verdict::Inconclusive,
        });
    }
    // Holm step-down adjusted p-values. Candidate dependence is allowed;
    // sorting is only statistical bookkeeping, never winner selection.
    let mut order = (0..comparisons.len()).collect::<Vec<_>>();
    order.sort_by(|&a, &b| comparisons[a].regression_p_value.total_cmp(&comparisons[b].regression_p_value));
    let mut previous = 0.0_f64;
    for (rank, &index) in order.iter().enumerate() {
        previous = previous.max((comparisons.len() - rank) as f64 * comparisons[index].regression_p_value).min(1.0);
        let comparison = &mut comparisons[index];
        comparison.regression_holm_p_value = previous;
        comparison.verdict = if previous <= regression_alpha {
            Verdict::Regression
        } else if comparison.pass_p_value <= ALPHA {
            Verdict::Pass
        } else {
            Verdict::Inconclusive
        };
    }
    let (best_measured_candidate, best_time) = best.ok_or_else(|| anyhow::anyhow!("no candidate"))?;
    let median_time_regret = auto_median / best_time;
    let verdict = if comparisons.iter().any(|c| c.verdict == Verdict::Regression) {
        Verdict::Regression
    } else if comparisons.iter().any(|c| c.verdict == Verdict::Inconclusive)
        || median_time_regret > 1.0 + TOLERANCE {
        Verdict::Inconclusive
    } else {
        Verdict::Pass
    };
    Ok(Gate { method: "paired_sign_holm_v2", tolerance_fraction: TOLERANCE,
        significance_level: ALPHA, regression_family_count: regression_families,
        regression_family_alpha: regression_alpha, repetitions, best_measured_candidate,
        median_time_regret, verdict, comparisons })
}

/// P[Binomial(n, 1/2) >= k] for every k. Relative masses start at the mode
/// and recur outwards, avoiding factorial overflow and 2^-n underflow before
/// normalization. This evaluates the exact discrete test, not an asymptotic
/// approximation or resampling estimate; arithmetic uses f64 probabilities.
fn binomial_sign_p_values(n: usize) -> Vec<f64> {
    let mode = n / 2;
    let mut masses = vec![0.0; n + 1];
    masses[mode] = 1.0;
    for k in (1..=mode).rev() {
        masses[k - 1] = masses[k] * k as f64 / (n - k + 1) as f64;
    }
    for k in mode..n {
        masses[k + 1] = masses[k] * (n - k) as f64 / (k + 1) as f64;
    }
    let total = masses.iter().sum::<f64>();
    let mut tail = 0.0;
    for mass in masses.iter_mut().rev() {
        tail += *mass;
        *mass = tail / total;
    }
    masses[0] = 1.0;
    masses
}
