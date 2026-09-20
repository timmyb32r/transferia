//! Explicit Rust RELEASE evaluator of production PostgreSQL snapshot plans.
//! See tests/fixtures/snapshot_planner/README.md for the measurement contract.

#[path = "../tests/fixtures/snapshot_planner/evaluation.rs"]
mod evaluation;
#[path = "../tests/fixtures/snapshot_planner/copy_meter.rs"]
mod copy_meter;
// Compile the exact production queue implementation, rather than maintaining a
// benchmark scheduler with subtly different ownership/adaptation semantics.
#[path = "../src/connectors/postgres/src_batch/queue.rs"]
#[allow(dead_code, reason = "the release evaluator reuses queue source; its retry API is exercised by the production queue tests")]
mod queue;

use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use futures_util::StreamExt as _;
use serde_json::json;
use tokio::task::JoinSet;
use transferia_connector_postgres::postgres::{self, PostgresConnectionConfig};
use transferia_connector_postgres::postgres::src_batch::{planner, planner::Strategy, planning, source_select_projection};
use transferia_connector_postgres::postgres::source::UnsupportedTypePolicy;

use evaluation::{Run, Verdict};

struct Options {
    schema: String,
    table: Option<String>,
    case_id: Option<String>,
    prepare: bool,
    rows: u64,
    repetitions: usize,
    max_parts: Option<NonZeroU32>,
    output: PathBuf,
}

impl Options {
    fn parse() -> anyhow::Result<Self> {
        let mut result = Self {
            schema: "snapshot_planner_benchmark_v1".into(), table: None, case_id: None,
            prepare: false, rows: 100_000, repetitions: 31,
            max_parts: NonZeroU32::new(8), output: PathBuf::from("target/snapshot-planner-evaluation"),
        };
        let mut args = std::env::args().skip(1);
        while let Some(argument) = args.next() {
            if argument == "--prepare" { result.prepare = true; continue; }
            let value = args.next().ok_or_else(|| anyhow::anyhow!("missing value for {argument}"))?;
            match argument.as_str() {
                "--schema" => result.schema = value,
                "--table" => result.table = Some(value),
                "--case-id" => result.case_id = Some(value),
                "--rows" => result.rows = value.parse()?,
                "--repetitions" => result.repetitions = value.parse()?,
                "--max-parts" => result.max_parts = if value == "auto" { None } else {
                    Some(NonZeroU32::new(value.parse()?).ok_or_else(|| anyhow::anyhow!("max-parts must be positive"))?)
                },
                "--output" => result.output = value.into(),
                _ => anyhow::bail!("unknown argument {argument}"),
            }
        }
        anyhow::ensure!(result.repetitions >= 5, "at least five repetitions are required");
        anyhow::ensure!(!result.prepare || result.table.is_none(), "prepare creates the complete new corpus; omit --table");
        anyhow::ensure!(result.case_id.is_none() || result.table.is_some(), "case-id requires table");
        evaluation::quote_identifier(&result.schema)?;
        if let Some(table) = &result.table { evaluation::quote_identifier(table)?; }
        evaluation::Corpus::load()?.validate_scale(result.rows)?;
        Ok(result)
    }
}

fn connection_config() -> anyhow::Result<PostgresConnectionConfig> {
    let mode = std::env::var("PGSSLMODE").unwrap_or_else(|_| "verify-full".into());
    anyhow::ensure!(matches!(mode.as_str(), "verify-full" | "disable"), "PGSSLMODE must be verify-full or explicitly disable");
    let config = PostgresConnectionConfig {
        host: std::env::var("PGHOST").context("PGHOST is required")?,
        port: std::env::var("PGPORT").unwrap_or_else(|_| "5432".into()).parse()?,
        database: std::env::var("PGDATABASE").context("PGDATABASE is required")?,
        username: std::env::var("PGUSER").context("PGUSER is required")?,
        password: std::env::var("PGPASSWORD").unwrap_or_default(),
        trusted_plaintext: mode == "disable",
        tls_ca_file: std::env::var("PGSSLROOTCERT").ok(),
    };
    config.validate()?;
    Ok(config)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    anyhow::ensure!(!cfg!(debug_assertions), "performance evaluation requires cargo run --release");
    let options = Options::parse()?;
    let config = connection_config()?;
    let client = postgres::connect(&config).await?;
    let corpus = evaluation::Corpus::load()?;
    if options.prepare {
        client.batch_execute(&evaluation::render_fixture(
            include_str!("../tests/fixtures/snapshot_planner/profiles.sql"), &options.schema, options.rows,
        )?).await?;
        client.batch_execute(&format!("VACUUM {}", evaluation::qualified(&options.schema, "empty_prefix")?)).await?;
        for profile in &corpus.profiles {
            client.batch_execute(&format!("ANALYZE {}", evaluation::qualified(&options.schema, &profile.table)?)).await?;
        }
    }
    if options.table.is_none() {
        client.batch_execute("BEGIN READ ONLY").await.context("pinning fixture-validation connection")?;
        validate_profile_preconditions(&client, &options, &corpus).await.context("validating prepared fixture properties")?;
        client.batch_execute("COMMIT").await.context("ending fixture-validation transaction")?;
    }
    let cases = if let Some(table) = &options.table {
        vec![(options.case_id.clone().unwrap_or_else(|| format!("external_{}", table)), table.clone())]
    } else {
        corpus.profiles.iter().map(|p| (p.id.clone(), p.table.clone())).collect()
    };
    client.batch_execute("BEGIN READ ONLY").await.context("pinning environment-metadata connection")?;
    let environment = client.query_one(
        "SELECT current_setting('server_version_num'), current_setting('server_version'), \
         current_setting('block_size'), current_setting('shared_buffers'), \
         current_setting('default_statistics_target')", &[],
    ).await.context("reading environment metadata")?;
    client.batch_execute("COMMIT").await.context("ending environment-metadata transaction")?;
    let started = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let source_fingerprints = [
        ("planner.rs", include_bytes!("../src/connectors/postgres/src_batch/planner.rs").as_slice()),
        ("planning.rs", include_bytes!("../src/connectors/postgres/src_batch/planning.rs").as_slice()),
        ("queue.rs", include_bytes!("../src/connectors/postgres/src_batch/queue.rs").as_slice()),
        ("reader.rs", include_bytes!("../src/connectors/postgres/src_batch/reader.rs").as_slice()),
        ("common.rs", include_bytes!("../src/connectors/postgres/common.rs").as_slice()),
        ("evaluator.rs", include_bytes!("snapshot_planner_benchmark.rs").as_slice()),
        ("evaluation.rs", include_bytes!("../tests/fixtures/snapshot_planner/evaluation.rs").as_slice()),
        ("corpus.json", include_bytes!("../tests/fixtures/snapshot_planner/corpus.json").as_slice()),
        ("profiles.sql", include_bytes!("../tests/fixtures/snapshot_planner/profiles.sql").as_slice()),
        ("Cargo.lock", include_bytes!("../../../Cargo.lock").as_slice()),
        ("Cargo.toml", include_bytes!("../../../Cargo.toml").as_slice()),
        ("rust-toolchain.toml", include_bytes!("../../../rust-toolchain.toml").as_slice()),
        (".cargo/config.toml", include_bytes!("../../../.cargo/config.toml").as_slice()),
    ].into_iter().map(|(name, bytes)| Ok((name, format!("{:032x}", murmur3::murmur3_x64_128(&mut std::io::Cursor::new(bytes), 0)?))))
        .collect::<anyhow::Result<std::collections::BTreeMap<_, _>>>()?;
    let binary_fingerprint = format!("{:032x}", murmur3::murmur3_x64_128(
        &mut std::io::BufReader::new(std::fs::File::open(std::env::current_exe()?)?), 0)?);
    std::fs::create_dir_all(&options.output)?;
    let path = options.output.join(format!("evaluation-{started}-{}.json", std::process::id()));
    let mut reports = Vec::new();
    let mut overall = Verdict::Pass;
    let regression_families = cases.len();
    for (id, table) in cases {
        let result = evaluate_case(&client, &config, &options, &id, &table, regression_families).await
            .with_context(|| format!("evaluating snapshot case {id}"))?;
        let verdict = result["gate"]["verdict"].as_str();
        if verdict == Some("regression") { overall = Verdict::Regression; }
        else if verdict == Some("inconclusive") && overall == Verdict::Pass { overall = Verdict::Inconclusive; }
        println!("case={id} verdict={} regret={:.4}", verdict.unwrap_or("invalid"), result["gate"]["median_time_regret"].as_f64().unwrap_or(f64::NAN));
        reports.push(result);
        // Preserve every completed case even if a later source operation fails.
        let report = json!({
            "schema_version": 3, "consumer": "rust_release", "debug_assertions": false,
            "copy_projection": "production_snapshot_source_select_projection",
            "unsupported_type_policy": "batch_default_to_string",
            "candidate_discovery": "auto_preflight_plus_independent_required_weighted_observation",
            "started_unix": started, "corpus_version": corpus.version,
            "corpus_provenance": corpus.provenance, "repetitions": options.repetitions,
            "regression_family_count": regression_families,
            "regression_family_scope": "tables in this invocation; separate commands are separate inference families",
            "rust_toolchain": include_str!("../../../rust-toolchain.toml"),
            "cargo_configuration": include_str!("../../../.cargo/config.toml"),
            "maximum_parts": options.max_parts.map(NonZeroU32::get),
            "independent_grid_maximum_parts": options.max_parts.map_or(8, NonZeroU32::get),
            "source_murmur3_x64_128": source_fingerprints, "binary_murmur3_x64_128": binary_fingerprint,
            "client_os": std::env::consts::OS, "client_arch": std::env::consts::ARCH,
            "client_available_parallelism": std::thread::available_parallelism().ok().map(|value| value.get()),
            "seed": 20260920, "cache_scenario": "uncontrolled_warm_after_reference_count",
            "server_version_num": environment.get::<_,String>(0),
            "server_version": environment.get::<_,String>(1), "block_size": environment.get::<_,String>(2),
            "shared_buffers": environment.get::<_,String>(3), "statistics_target": environment.get::<_,String>(4),
            "verdict": overall, "cases": reports,
            "scope": "production planner, predicates, snapshot column projection and exact production queue source including adaptation; projected binary COPY plus checked SQL completion; immediate discard-consumer acknowledgement, no destination or Arrow decoding",
            "correctness_scope": "timed runs verify complete framing and exact row count; exact identities and payloads are checked separately by e2e_snapshot_planner",
            "excluded_shared_cost": "fixture preparation, shared snapshot coordinator connection/export/lock, reference COUNT, applicability and projection-metadata preflight, report writes"
        });
        std::fs::write(&path, serde_json::to_vec_pretty(&report)?)?;
    }
    println!("report={}", path.display());
    match overall {
        Verdict::Pass => Ok(()),
        Verdict::Regression => anyhow::bail!("production Auto exceeded the 5% gate; completed evidence was saved"),
        Verdict::Inconclusive => anyhow::bail!("measurement is inconclusive; completed evidence was saved; do not report a pass"),
    }
}

async fn validate_profile_preconditions(client: &tokio_postgres::Client, options: &Options, corpus: &evaluation::Corpus) -> anyhow::Result<()> {
    for profile in &corpus.profiles {
        anyhow::ensure!(!profile.purpose.is_empty(), "fixture purpose is missing");
        let table = evaluation::qualified(&options.schema, &profile.table)?;
        let count = u64::try_from(client.query_one(&format!("SELECT count(*) FROM {table}"), &[]).await?.get::<_, i64>(0))?;
        anyhow::ensure!(count == options.rows / profile.live_divisor, "fixture {} has the wrong row count", profile.id);
        anyhow::ensure!(client.prepare(&format!("SELECT * FROM {table} LIMIT 0")).await?.columns().len() == profile.columns,
            "fixture {} has the wrong column count", profile.id);
    }
    let empty_prefix = evaluation::qualified(&options.schema, "empty_prefix")?;
    let pages = client.query_one("SELECT pg_relation_size($1::text::regclass)/current_setting('block_size')::bigint", &[&empty_prefix]).await?.get::<_, i64>(0);
    anyhow::ensure!(pages >= 2, "empty-prefix fixture does not span multiple pages");
    let live_prefix = client.query_one(&format!("SELECT count(*) FROM {empty_prefix} WHERE ctid < '({},0)'::tid", pages/2), &[]).await?.get::<_, i64>(0);
    anyhow::ensure!(live_prefix == 0, "empty-prefix fixture lost the intended physical skew");
    let toast = evaluation::qualified(&options.schema, "toast_tail")?;
    let row = client.query_one(&format!("SELECT count(*) FILTER(WHERE octet_length(payload)=65536), count(*) FILTER(WHERE octet_length(payload)=32) FROM {toast}"), &[]).await?;
    anyhow::ensure!(u64::try_from(row.get::<_, i64>(0))? == options.rows/100
        && u64::try_from(row.get::<_, i64>(1))? == options.rows*99/100, "TOAST fixture lost its width distribution");
    let skew = evaluation::qualified(&options.schema, "key_skew")?;
    anyhow::ensure!(u64::try_from(client.query_one(&format!("SELECT count(*) FROM {skew} WHERE row_id < {}", options.rows), &[]).await?.get::<_, i64>(0))? == options.rows*9/10,
        "key-skew fixture lost its dense prefix");
    Ok(())
}

#[derive(Clone, Copy)]
struct Variant { strategy: Option<Strategy>, parts: NonZeroU32 }

impl Variant {
    fn label(self) -> String {
        self.strategy.map_or_else(|| "auto".into(), |s| format!("{}_p{}", s.label(), self.parts))
    }
}

async fn evaluate_case(client: &tokio_postgres::Client, config: &PostgresConnectionConfig,
    options: &Options, id: &str, table: &str, regression_families: usize) -> anyhow::Result<serde_json::Value> {
    let qualified = evaluation::qualified(&options.schema, table)?;
    client.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET LOCAL max_parallel_workers_per_gather=0; SET LOCAL jit=off").await?;
    client.batch_execute(&format!("LOCK TABLE {qualified} IN ACCESS SHARE MODE")).await?;
    let snapshot = client.query_one("SELECT pg_export_snapshot()::text", &[]).await?.get::<_, String>(0);
    anyhow::ensure!(snapshot.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-'), "unexpected snapshot identifier");
    let oid = client.query_one("SELECT $1::text::regclass::oid", &[&qualified]).await?.get::<_, u32>(0);
    let reference = client.query_one(&format!("SELECT count(*)::bigint FROM {qualified}"), &[]).await?.get::<_, i64>(0);
    let expected_rows = u64::try_from(reference)?;
    let metadata = client.prepare(&format!("SELECT * FROM {qualified} LIMIT 0")).await?;
    let fields = metadata.columns().len();
    let projection = Arc::<str>::from(source_select_projection(metadata.columns(), UnsupportedTypePolicy::ToString)?);
    let projected_metadata = client.prepare(&format!("SELECT {projection} FROM {qualified} LIMIT 0")).await?;
    anyhow::ensure!(projected_metadata.columns().len() == fields, "snapshot projection changed the field count");
    let column_types = metadata.columns().iter().zip(projected_metadata.columns()).map(|(source, projected)| json!({
        "name": source.name(), "source_type_oid": source.type_().oid(), "source_type_name": source.type_().name(),
        "projected_type_oid": projected.type_().oid(), "projected_type_name": projected.type_().name(),
    })).collect::<Vec<_>>();
    let preflight = planning::prepare_relation(client, &options.schema, table, oid, options.max_parts, None).await?;
    // Auto's lack of an observation is a policy decision, not proof that a
    // weighted baseline is unavailable. Discover it independently so a bad
    // probe-pay prediction cannot remove its own counterexample from the gate.
    let discover_weighted = options.max_parts.is_none_or(|parts| parts.get() > 1)
        && preflight.decision().facts().heap_pages() > 0
        && preflight.decision().candidates().iter().any(|candidate| candidate.strategy == Strategy::Ctid && candidate.eligible)
        && preflight.decision().candidates().iter().any(|candidate| candidate.strategy == Strategy::WeightedCtid && !candidate.eligible);
    let candidate_discovery_started = Instant::now();
    let observed_candidates = if discover_weighted {
        Some(planning::prepare_relation_for_evaluation_candidates(client, &options.schema, table, oid, options.max_parts).await?)
    } else { None };
    let candidate_discovery_seconds = candidate_discovery_started.elapsed().as_secs_f64();
    let mut variants = vec![Variant { strategy: None, parts: NonZeroU32::MIN }];
    let mut candidate_report = Vec::new();
    for automatic_candidate in preflight.decision().candidates() {
        // Enrich only the missing weighted candidate. All other baseline
        // counts and Auto's own facts remain those of its ordinary preflight.
        let candidate = if automatic_candidate.strategy == Strategy::WeightedCtid {
            match observed_candidates.as_ref() {
                Some(prepared) => prepared.decision().candidates().iter()
                    .find(|candidate| candidate.strategy == Strategy::WeightedCtid)
                    .ok_or_else(|| anyhow::anyhow!("evaluation discovery did not report weighted eligibility"))?,
                None => automatic_candidate,
            }
        } else { automatic_candidate };
        let eligibility_origin = if automatic_candidate.eligible { "automatic_preflight" }
            else if candidate.eligible { "evaluation_required_observation" }
            else if observed_candidates.is_some() && candidate.strategy == Strategy::WeightedCtid { "evaluation_observation_unavailable" }
            else { "ineligible" };
        candidate_report.push(json!({"strategy": candidate.strategy.label(), "lanes": candidate.lanes,
            "parts": candidate.parts, "eligible": candidate.eligible, "reason": candidate.reason,
            "eligibility_origin": eligibility_origin,
            "automatic_preflight_eligible": automatic_candidate.eligible,
            "automatic_preflight_reason": automatic_candidate.reason,
            "estimated_heap_passes": candidate.cost.as_ref().map(|cost| cost.heap_passes),
            "estimated_total_seconds": candidate.cost.as_ref().map(|cost| cost.total_seconds)}));
        if candidate.eligible {
            let bound = match candidate.strategy {
                Strategy::Single => 1,
                Strategy::Ctid | Strategy::WeightedCtid => u32::try_from(preflight.decision().facts().heap_pages().max(1))?,
                Strategy::Indexed => preflight.decision().facts().indexed_cuts().checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("indexed part count overflow"))?,
                Strategy::HashCtid => u32::MAX,
            }.min(options.max_parts.map_or(8, NonZeroU32::get));
            // This independent grid is essential: comparing only the model's
            // recommended concurrency would reproduce its own blind spots.
            let mut counts = vec![candidate.lanes, candidate.parts];
            let mut power = 1_u32;
            while power <= bound {
                counts.push(power);
                let Some(next) = power.checked_mul(2) else { break; };
                power = next;
            }
            for count in counts {
                let variant = Variant { strategy: Some(candidate.strategy), parts: NonZeroU32::new(count)
                    .ok_or_else(|| anyhow::anyhow!("eligible candidate has zero lanes"))? };
                if !variants.iter().any(|v| v.label() == variant.label()) { variants.push(variant); }
            }
        }
    }
    anyhow::ensure!(variants.len() > 1, "planner reported no feasible baseline");
    let mut runs = Vec::new();
    let mut seed = 20260920_u64;
    for round in 0..options.repetitions {
        let mut order = (0..variants.len()).collect::<Vec<_>>();
        shuffle(&mut order, &mut seed);
        for index in order {
            let variant = variants[index];
            let run = run_variant(client, config, &snapshot, options, table, oid, &qualified, fields, &projection, round, variant).await?;
            anyhow::ensure!(run.rows == expected_rows, "row count mismatch for case {id}, variant {}", run.variant);
            runs.push(run);
        }
    }
    client.batch_execute("ROLLBACK").await?;
    let gate = evaluation::compare(&runs, options.repetitions, regression_families)?;
    Ok(json!({"id": id, "schema": options.schema, "table": table, "expected_rows": expected_rows,
        "columns": fields, "column_types": column_types,
        "copy_projection": "production_snapshot_source_select_projection",
        "facts": {"heap_pages": preflight.decision().facts().heap_pages(),
            "estimated_rows": preflight.decision().facts().estimated_rows(),
            "estimated_output_bytes": preflight.decision().facts().estimated_output_bytes(),
            "physical_sample_available": preflight.decision().facts().has_observation()},
        "candidate_discovery": {"additional_observation_attempted": discover_weighted,
            "outside_measurement_seconds": candidate_discovery_seconds,
            "physical_observation_available": observed_candidates.as_ref().is_some_and(|prepared| prepared.decision().facts().has_observation()),
            "timed_auto_uses_ordinary_preparation": true,
            "timed_weighted_pays_for_own_observation": true},
        "candidates": candidate_report, "runs": runs, "gate": gate}))
}

#[allow(clippy::too_many_arguments, reason = "explicit evaluator inputs keep timed state visible")]
async fn run_variant(client: &tokio_postgres::Client, config: &PostgresConnectionConfig,
    snapshot: &str, options: &Options, table: &str, oid: u32, qualified: &str, fields: usize, projection: &Arc<str>,
    round: usize, variant: Variant) -> anyhow::Result<Run> {
    let start = Instant::now();
    let prepared = if let Some(strategy) = variant.strategy {
        planning::prepare_relation_for_evaluation(client, &options.schema, table, oid, options.max_parts, strategy, variant.parts).await?
    } else {
        planning::prepare_relation(client, &options.schema, table, oid, options.max_parts, None).await?
    };
    let lanes = usize::try_from(prepared.decision().lanes())?;
    let parts = prepared.chunks().len();
    let work = queue::SnapshotQueue::new(prepared.chunks().to_vec(), prepared.decision().lanes(), options.max_parts)?;
    let prepared = Arc::new(prepared);
    let planning_seconds = start.elapsed().as_secs_f64();
    let connect_start = Instant::now();
    let mut connectors = JoinSet::new();
    for _ in 0..lanes {
        let config = config.clone();
        let snapshot = snapshot.to_owned();
        connectors.spawn(async move {
            let reader = postgres::connect(&config).await?;
            reader.batch_execute(&format!("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT '{snapshot}'; SET LOCAL max_parallel_workers_per_gather=0; SET LOCAL jit=off")).await?;
            Ok::<_, anyhow::Error>(reader)
        });
    }
    let mut readers = Vec::new();
    while let Some(reader) = connectors.join_next().await { readers.push(reader??); }
    let startup_seconds = connect_start.elapsed().as_secs_f64();
    let scan_start = Instant::now();
    let mut workers = JoinSet::new();
    for (lane_index, reader) in readers.into_iter().enumerate() {
        let prepared = Arc::clone(&prepared);
        let mut lease = work.open_lane(lane_index)?;
        let qualified = qualified.to_owned();
        let projection = Arc::clone(projection);
        workers.spawn(async move {
            let mut rows = 0_u64;
            let mut bytes = 0_u64;
            let mut completed_parts = 0_usize;
            loop {
                let Some(task) = lease.claim()? else { break; };
                let query = format!("COPY (SELECT {projection} FROM {qualified} WHERE {}) TO STDOUT (FORMAT BINARY)", prepared.predicate(&task.chunk)?);
                let query_started = Instant::now();
                let stream = reader.copy_out(&query[..]).await?;
                let query_start = query_started.elapsed();
                futures_util::pin_mut!(stream);
                let mut meter = copy_meter::CopyMeter::new(fields)?;
                while let Some(chunk) = stream.next().await { meter.push(&chunk?)?; }
                meter.finish()?;
                // CopyOutStream ends at CopyDone. This same-transaction command
                // also observes delayed executor errors before claiming success.
                reader.simple_query("SELECT 1").await?;
                let mut markers = Vec::with_capacity(2);
                if meter.rows > 0 {
                    markers.push(lease.emit_rows(&task, lease.offset()?, meter.rows)?.1);
                }
                markers.push(lease.finish_read(&task, query_started.elapsed(), query_start, meter.bytes)?);
                lease.acknowledge(&markers)?;
                completed_parts = completed_parts.checked_add(1).ok_or_else(|| anyhow::anyhow!("completed part count overflow"))?;
                rows = rows.checked_add(meter.rows).ok_or_else(|| anyhow::anyhow!("row count overflow"))?;
                bytes = bytes.checked_add(meter.bytes).ok_or_else(|| anyhow::anyhow!("byte count overflow"))?;
            }
            lease.close()?;
            reader.batch_execute("ROLLBACK").await?;
            Ok::<_, anyhow::Error>((rows, bytes, completed_parts))
        });
    }
    let mut rows = 0_u64;
    let mut bytes = 0_u64;
    let mut completed_parts = 0_usize;
    while let Some(worker) = workers.join_next().await {
        let (worker_rows, worker_bytes, worker_parts) = worker??;
        rows = rows.checked_add(worker_rows).ok_or_else(|| anyhow::anyhow!("row count overflow"))?;
        bytes = bytes.checked_add(worker_bytes).ok_or_else(|| anyhow::anyhow!("byte count overflow"))?;
        completed_parts = completed_parts.checked_add(worker_parts).ok_or_else(|| anyhow::anyhow!("completed part count overflow"))?;
    }
    work.ensure_complete()?;
    let scan_seconds = scan_start.elapsed().as_secs_f64();
    Ok(Run { round, variant: variant.label(), total_seconds: start.elapsed().as_secs_f64(),
        selected_strategy: prepared.decision().strategy().label().to_owned(),
        selection_reason: prepared.decision().reason().to_owned(),
        planning_seconds, startup_seconds, scan_seconds, rows, bytes, lanes, parts, completed_parts })
}

fn shuffle(values: &mut [usize], seed: &mut u64) {
    for end in (1..values.len()).rev() {
        *seed ^= *seed << 13;
        *seed ^= *seed >> 7;
        *seed ^= *seed << 17;
        values.swap(end, (*seed % (end as u64 + 1)) as usize);
    }
}
