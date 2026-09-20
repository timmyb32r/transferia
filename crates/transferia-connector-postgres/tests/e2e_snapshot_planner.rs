#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used, reason = "test assertions fail fast")]

#[path = "fixtures/snapshot_planner/evaluation.rs"]
mod evaluation;
#[path = "fixtures/snapshot_planner/copy_meter.rs"]
mod copy_meter;

use std::num::NonZeroU32;

use testcontainers::core::{IntoContainerPort as _, WaitFor};
use testcontainers::runners::AsyncRunner as _;
use testcontainers::{GenericImage, ImageExt as _};

use evaluation::{Run, Verdict};

fn synthetic_run(round: usize, variant: &str, seconds: f64) -> Run {
    Run { round, variant: variant.into(), selected_strategy: "synthetic".into(), selection_reason: "test".into(), total_seconds: seconds,
        planning_seconds: 0.0, startup_seconds: 0.0, scan_seconds: seconds,
        rows: 42, bytes: 1000, lanes: 1, parts: 1, completed_parts: 1 }
}

fn paired_runs(auto: &[f64], candidate: &[f64]) -> Vec<Run> {
    auto.iter().zip(candidate).enumerate().flat_map(|(round, (&a, &c))|
        [synthetic_run(round, "auto", a), synthetic_run(round, "candidate", c)]).collect()
}

#[test]
fn five_percent_gate_distinguishes_regression_pass_and_noise() -> anyhow::Result<()> {
    let regression = evaluation::compare(&paired_runs(&[1.06; 5], &[1.0; 5]), 5, 1)?;
    assert_eq!(regression.verdict, Verdict::Regression);
    assert_eq!(regression.comparisons[0].regression_p_value, 1.0 / 32.0);
    assert_eq!(evaluation::compare(&paired_runs(&[1.04; 5], &[1.0; 5]), 5, 1)?.verdict, Verdict::Pass);
    assert_eq!(evaluation::compare(&paired_runs(&[1.06, 1.06, 1.06, 0.90, 0.90], &[1.0; 5]), 5, 1)?.verdict, Verdict::Inconclusive);
    let four = evaluation::compare(&paired_runs(&[1.06, 1.06, 1.06, 1.06, 0.90], &[1.0; 5]), 5, 1)?;
    assert_eq!(four.verdict, Verdict::Inconclusive);
    assert!((four.comparisons[0].regression_p_value - 6.0 / 32.0).abs() < 1e-14);
    Ok(())
}

#[test]
fn comparator_rejects_incomplete_invalid_or_different_row_sets() {
    let original = paired_runs(&[1.0; 5], &[1.0; 5]);
    assert!(evaluation::compare(&original, 4, 1).is_err());
    assert!(evaluation::compare(&original, 5, 0).is_err());
    assert!(evaluation::compare(&original[..9], 5, 1).is_err());
    let mut duplicate = original.clone();
    duplicate.push(original[0].clone());
    assert!(evaluation::compare(&duplicate, 5, 1).is_err());
    for value in [0.0, -1.0, f64::NAN, f64::INFINITY] {
        let mut invalid = original.clone();
        invalid[0].total_seconds = value;
        assert!(evaluation::compare(&invalid, 5, 1).is_err());
    }
    let mut wrong_rows = original;
    wrong_rows[0].rows += 1;
    assert!(evaluation::compare(&wrong_rows, 5, 1).is_err());
}

#[test]
fn winner_is_measured_instead_of_encoded_in_fixture() -> anyhow::Result<()> {
    let mut runs = paired_runs(&[1.0; 6], &[1.2; 6]);
    for round in 0..6 { runs.push(synthetic_run(round, "new_strategy", 0.8)); }
    let gate = evaluation::compare(&runs, 6, 1)?;
    assert_eq!(gate.best_measured_candidate, "new_strategy");
    assert_eq!(gate.verdict, Verdict::Regression);
    assert_eq!(gate.median_time_regret, 1.25);
    Ok(())
}

#[test]
fn sign_test_uses_sample_count_instead_of_a_permanent_agreement_fraction() -> anyhow::Result<()> {
    let mut automatic = [1.20; 31];
    automatic[..21].fill(1.04);
    let pass = evaluation::compare(&paired_runs(&automatic, &[1.0; 31]), 31, 1)?;
    assert_eq!(pass.verdict, Verdict::Pass);
    assert_eq!(pass.comparisons[0].below_threshold_pairs, 21);
    assert!((pass.comparisons[0].pass_p_value - 75_973_189.0 / 2_147_483_648.0).abs() < 1e-14);
    automatic[20] = 1.20;
    assert_eq!(evaluation::compare(&paired_runs(&automatic, &[1.0; 31]), 31, 1)?.verdict, Verdict::Inconclusive);
    automatic.fill(1.0);
    automatic[..21].fill(1.06);
    assert_eq!(evaluation::compare(&paired_runs(&automatic, &[1.0; 31]), 31, 1)?.verdict, Verdict::Regression);
    Ok(())
}

#[test]
fn threshold_ties_do_not_manufacture_evidence() -> anyhow::Result<()> {
    let gate = evaluation::compare(&paired_runs(&[1.05; 31], &[1.0; 31]), 31, 1)?;
    let comparison = &gate.comparisons[0];
    assert_eq!(comparison.threshold_ties, 31);
    assert_eq!(comparison.pass_p_value, 1.0);
    assert_eq!(comparison.regression_p_value, 1.0);
    assert_eq!(gate.verdict, Verdict::Inconclusive);
    Ok(())
}

#[test]
fn large_round_counts_do_not_underflow_into_false_significance() -> anyhow::Result<()> {
    let mut automatic = vec![1.04; 10_000];
    automatic[..5_000].fill(1.06);
    let gate = evaluation::compare(&paired_runs(&automatic, &vec![1.0; 10_000]), 10_000, 1)?;
    let comparison = &gate.comparisons[0];
    assert!((0.5..0.51).contains(&comparison.pass_p_value));
    assert!((0.5..0.51).contains(&comparison.regression_p_value));
    assert_eq!(gate.verdict, Verdict::Inconclusive);
    Ok(())
}

#[test]
fn regression_corrects_candidate_and_table_multiplicity_but_pass_is_an_intersection() -> anyhow::Result<()> {
    let mut runs = paired_runs(&[1.06; 5], &[1.0; 5]);
    for round in 0..5 { runs.push(synthetic_run(round, "second_candidate", 2.0)); }
    let gate = evaluation::compare(&runs, 5, 1)?;
    assert_eq!(gate.verdict, Verdict::Inconclusive);
    let significant_before_correction = gate.comparisons.iter().find(|c| c.candidate == "candidate").unwrap();
    assert_eq!(significant_before_correction.regression_p_value, 1.0 / 32.0);
    assert_eq!(significant_before_correction.regression_holm_p_value, 1.0 / 16.0);
    assert_eq!(evaluation::compare(&paired_runs(&[1.06; 5], &[1.0; 5]), 5, 2)?.verdict, Verdict::Inconclusive);
    assert_eq!(evaluation::compare(&paired_runs(&[1.06; 6], &[1.0; 6]), 6, 2)?.verdict, Verdict::Regression);

    let mut all_pass = paired_runs(&[1.0; 5], &[1.0; 5]);
    for round in 0..5 { all_pass.push(synthetic_run(round, "second_candidate", 1.1)); }
    assert_eq!(evaluation::compare(&all_pass, 5, 12)?.verdict, Verdict::Pass);
    Ok(())
}

#[test]
fn descriptive_best_candidate_median_must_also_stay_within_tolerance() -> anyhow::Result<()> {
    let mut automatic = [2.0; 31];
    automatic[..15].fill(1.0);
    let mut candidate = [2.1; 31];
    candidate[..15].fill(1.1);
    candidate[30] = 1.0;
    let gate = evaluation::compare(&paired_runs(&automatic, &candidate), 31, 1)?;
    assert_eq!(gate.comparisons[0].verdict, Verdict::Pass);
    assert!(gate.median_time_regret > 1.05);
    assert_eq!(gate.verdict, Verdict::Inconclusive);
    Ok(())
}

#[test]
fn fixture_parameters_are_validated_without_identifier_rewriting() -> anyhow::Result<()> {
    let corpus = evaluation::Corpus::load()?;
    assert_eq!(corpus.version, 1);
    assert!(corpus.provenance.contains("not byte-identical"));
    assert_eq!(corpus.profiles.len(), 6);
    for profile in &corpus.profiles {
        assert!(!profile.id.is_empty() && !profile.table.is_empty() && !profile.purpose.is_empty());
        assert!(profile.columns > 0 && profile.live_divisor > 0);
    }
    for rows in [0, 999, 1001, u64::MAX] { assert!(corpus.validate_scale(rows).is_err()); }
    assert!(evaluation::render_fixture("{{schema}} {{rows}}", "bad';select", 1000).is_err());
    assert_eq!(evaluation::render_fixture("{{schema}} {{rows}}", "fixture_v1", 1000)?, "\"fixture_v1\" 1000::bigint");
    assert_eq!(evaluation::qualified("a\"b", "c.d")?, "\"a\"\"b\".\"c.d\"");
    Ok(())
}

#[test]
fn release_copy_meter_accepts_every_fragmentation_and_rejects_corruption() -> anyhow::Result<()> {
    let mut wire = b"PGCOPY\n\xff\r\n\0".to_vec();
    wire.extend_from_slice(&0_i32.to_be_bytes());
    wire.extend_from_slice(&0_i32.to_be_bytes());
    wire.extend_from_slice(&2_i16.to_be_bytes());
    wire.extend_from_slice(&3_i32.to_be_bytes());
    wire.extend_from_slice(b"abc");
    wire.extend_from_slice(&(-1_i32).to_be_bytes());
    wire.extend_from_slice(&(-1_i16).to_be_bytes());
    for split in 0..=wire.len() {
        let mut meter = copy_meter::CopyMeter::new(2)?;
        meter.push(&wire[..split])?;
        meter.push(&wire[split..])?;
        meter.finish()?;
        assert_eq!(meter.rows, 1);
        assert_eq!(meter.bytes, wire.len() as u64);
    }
    let mut bytewise = copy_meter::CopyMeter::new(2)?;
    for byte in &wire { bytewise.push(&[*byte])?; }
    bytewise.finish()?;
    for truncate in 0..wire.len() {
        let mut meter = copy_meter::CopyMeter::new(2)?;
        meter.push(&wire[..truncate])?;
        assert!(meter.finish().is_err());
    }
    assert!(bytewise.push(b"x").is_err());
    let mut bad = wire.clone(); bad[0] = b'x';
    assert!(copy_meter::CopyMeter::new(2)?.push(&bad).is_err());
    assert!(copy_meter::CopyMeter::new(1)?.push(&wire).is_err());
    Ok(())
}

#[tokio::test]
async fn production_plans_preserve_exact_rows_and_payloads_in_adversarial_corpus() -> anyhow::Result<()> {
    use transferia_connector_postgres::postgres::src_batch::planning;

    let postgres = GenericImage::new("postgres", "17.6-bookworm")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr("database system is ready to accept connections"))
        .with_env_var("POSTGRES_PASSWORD", "test")
        .with_env_var("POSTGRES_DB", "transferia")
        .start().await?;
    let host = postgres.get_host().await?.to_string();
    let port = postgres.get_host_port_ipv4(5432.tcp()).await?;
    let config = format!("host={host} port={port} user=postgres password=test dbname=transferia");
    let (client, connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls).await?;
    let driver = tokio::spawn(connection);
    let schema = "snapshot_planner_exactness_v1";
    client.batch_execute(&evaluation::render_fixture(
        include_str!("fixtures/snapshot_planner/profiles.sql"), schema, 1000,
    )?).await?;
    client.batch_execute(&format!("VACUUM {}", evaluation::qualified(schema, "empty_prefix")?)).await?;
    let corpus = evaluation::Corpus::load()?;
    for profile in &corpus.profiles {
        client.batch_execute(&format!("ANALYZE {}", evaluation::qualified(schema, &profile.table)?)).await?;
    }
    client.batch_execute(&evaluation::render_fixture(
        include_str!("fixtures/snapshot_planner/corners.sql"), schema, 1000,
    )?).await?;
    client.batch_execute(&evaluation::render_fixture(
        include_str!("fixtures/snapshot_planner/research.sql"), schema, 1000,
    )?).await?;
    let mut tables = corpus.profiles.iter().map(|p| p.table.as_str()).collect::<Vec<_>>();
    tables.extend(["empty_rows", "one_row", "extreme_keys", "nullable_composite", "special_values", "ordered_keys", "mcv_keys", "missing_statistics", "stale_statistics", "partition_parent"]);
    client.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY").await?;
    for table in &tables {
        client.batch_execute(&format!("LOCK TABLE {} IN ACCESS SHARE MODE", evaluation::qualified(schema, table)?)).await?;
    }
    for table in tables {
        let qualified = evaluation::qualified(schema, table)?;
        let oid = client.query_one("SELECT $1::text::regclass::oid", &[&qualified]).await?.get::<_, u32>(0);
        let expected = exact_rows(&client, &qualified, "TRUE").await?;
        for maximum in [1, 4, 16] {
            let prepared = planning::prepare_relation(&client, schema, table, oid, NonZeroU32::new(maximum), None).await?;
            assert!(!prepared.chunks().is_empty(), "empty table must still have complete coverage");
            assert!(prepared.chunks().len() <= maximum as usize, "max_parts exceeded for {table}");
            let mut actual = Vec::new();
            for chunk in prepared.chunks() {
                actual.extend(exact_rows(&client, &qualified, &prepared.predicate(chunk)?).await?);
            }
            actual.sort();
            assert_eq!(actual, expected, "coverage/value mismatch for {table}, cap {maximum}");
        }
    }
    client.batch_execute("ROLLBACK").await?;
    evaluation_projection_preserves_original_generator_numeric_values(&client).await?;
    first_request_normalizes_restored_numeric_histograms(&client, schema).await?;
    snapshot_boundaries_ignore_shadowed_builtin_operations(&client, schema).await?;
    evaluation_discovers_weighted_when_auto_skips_observation(&client, schema).await?;
    run_research_scenarios(&client, &config, schema).await?;
    drop(client);
    driver.await??;
    Ok(())
}

async fn first_request_normalizes_restored_numeric_histograms(client: &tokio_postgres::Client, schema: &str) -> anyhow::Result<()> {
    use transferia_connector_postgres::postgres::src_batch::{planner::Strategy, planning};

    let table = "restored_numeric_histogram";
    let qualified = evaluation::qualified(schema, table)?;
    // Deliberately unordered, with numerically equal spellings, adjacent values
    // beyond f64 precision, tiny fractions, infinities and PostgreSQL's NaN.
    // The disposable superuser fixture simulates restored/imported statistics;
    // source rows themselves retain a valid unique primary key.
    let bounds = "ARRAY['NaN', '9007199254740993', '-Infinity', \
        '0.0000000000000000000001', '9007199254740992', 'Infinity', \
        '-9007199254740993', '0', '12345678901234567890.123456789012', \
        '9007199254740993.0', 'NaN', '-0.0000000000000000000001', '0.0']::numeric[]";
    client.batch_execute(&format!(
        "CREATE TABLE {qualified} (row_id bigint GENERATED ALWAYS AS IDENTITY, k numeric PRIMARY KEY, padding text); \
         ALTER TABLE {qualified} ALTER COLUMN padding SET STORAGE PLAIN; \
         INSERT INTO {qualified} (k, padding) SELECT DISTINCT cut, repeat('p',3000) FROM unnest({bounds}) AS cuts(cut); \
         ANALYZE {qualified}"
    )).await?;
    let oid = client.query_one("SELECT $1::text::regclass::oid", &[&qualified]).await?.get::<_, u32>(0);
    let slot: i32 = client.query_one(
        "SELECT array_position(ARRAY[stakind1,stakind2,stakind3,stakind4,stakind5], 2::smallint) \
         FROM pg_catalog.pg_statistic WHERE starelid=$1 AND NOT stainherit \
         AND staattnum=(SELECT attnum FROM pg_catalog.pg_attribute WHERE attrelid=$1 AND attname='k')",
        &[&oid],
    ).await?.try_get(0)?;
    assert!((1..=5).contains(&slot), "ANALYZE must provide a histogram slot");
    // Catalog stavalues has declared type anyarray. array_in preserves that
    // row descriptor while selecting exact NUMERIC element input by its OID.
    let changed = client.execute(&format!(
        "UPDATE pg_catalog.pg_statistic SET stavalues{slot}=pg_catalog.array_in(({bounds})::text::cstring, 1700::oid, -1) WHERE starelid=$1 AND NOT stainherit \
         AND staattnum=(SELECT attnum FROM pg_catalog.pg_attribute WHERE attrelid=$1 AND attname='k')"
    ), &[&oid]).await?;
    assert_eq!(changed, 1);
    client.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY").await?;
    client.batch_execute(&format!("LOCK TABLE {qualified} IN ACCESS SHARE MODE")).await?;
    let expected = exact_rows(client, &qualified, "TRUE").await?;
    assert_eq!(expected.len(), 10);
    // Even Single receives normalized cuts in its first metadata request. This
    // catches moving the work back into a later, selected-index-only query.
    let single = planning::prepare_relation_for_evaluation(client, schema, table, oid,
        NonZeroU32::new(16), Strategy::Single, NonZeroU32::MIN).await?;
    assert_eq!(single.decision().facts().indexed_cuts(), 10);
    assert_eq!(single.decision().facts().indexed_boundary_seconds(), 0.0);
    for parts in [2, 4, 11] {
        let plan = planning::prepare_relation_for_evaluation(client, schema, table, oid,
            NonZeroU32::new(16), Strategy::Indexed, NonZeroU32::new(parts).unwrap()).await?;
        let mut actual = Vec::new();
        for chunk in plan.chunks() {
            actual.extend(exact_rows(client, &qualified, &plan.predicate(chunk)?).await?);
        }
        actual.sort();
        assert_eq!(actual, expected, "native NUMERIC boundaries must preserve exact coverage at {parts} parts");
    }
    client.batch_execute("ROLLBACK").await?;
    Ok(())
}

async fn snapshot_boundaries_ignore_shadowed_builtin_operations(client: &tokio_postgres::Client, schema: &str) -> anyhow::Result<()> {
    use transferia_connector_postgres::postgres::src_batch::{planner::Strategy, planning};

    let shadow = "snapshot_planner_shadow_operations";
    client.batch_execute(&format!("CREATE SCHEMA {shadow}; CREATE DOMAIN {shadow}.text AS pg_catalog.int4")).await?;
    for (suffix, native_type) in [("numeric", "numeric"), ("tid", "tid")] {
        client.batch_execute(&format!(
            "CREATE FUNCTION {shadow}.false_{suffix}(pg_catalog.{native_type}, pg_catalog.{native_type}) \
             RETURNS pg_catalog.bool LANGUAGE SQL IMMUTABLE STRICT AS 'SELECT false'; \
             CREATE OPERATOR {shadow}.< (FUNCTION={shadow}.false_{suffix}, LEFTARG=pg_catalog.{native_type}, RIGHTARG=pg_catalog.{native_type}); \
             CREATE OPERATOR {shadow}.>= (FUNCTION={shadow}.false_{suffix}, LEFTARG=pg_catalog.{native_type}, RIGHTARG=pg_catalog.{native_type})"
        )).await?;
    }
    for (suffix, native_type) in [("oid", "oid"), ("name", "name")] {
        client.batch_execute(&format!(
            "CREATE FUNCTION {shadow}.false_{suffix}(pg_catalog.{native_type}, pg_catalog.{native_type}) \
             RETURNS pg_catalog.bool LANGUAGE SQL IMMUTABLE STRICT AS 'SELECT false'; \
             CREATE OPERATOR {shadow}.= (FUNCTION={shadow}.false_{suffix}, LEFTARG=pg_catalog.{native_type}, RIGHTARG=pg_catalog.{native_type})"
        )).await?;
    }
    client.batch_execute(&format!(
        "CREATE FUNCTION {shadow}.zero_hash(pg_catalog.int8,pg_catalog.int8) RETURNS pg_catalog.int8 LANGUAGE SQL IMMUTABLE STRICT AS 'SELECT 0::pg_catalog.int8'; \
         CREATE OPERATOR {shadow}.% (FUNCTION={shadow}.zero_hash, LEFTARG=pg_catalog.int8, RIGHTARG=pg_catalog.int8); \
         CREATE OPERATOR {shadow}.+ (FUNCTION={shadow}.zero_hash, LEFTARG=pg_catalog.int8, RIGHTARG=pg_catalog.int8); \
         CREATE FUNCTION {shadow}.false_hash(pg_catalog.int8,pg_catalog.int4) RETURNS pg_catalog.bool LANGUAGE SQL IMMUTABLE STRICT AS 'SELECT false'; \
         CREATE OPERATOR {shadow}.= (FUNCTION={shadow}.false_hash, LEFTARG=pg_catalog.int8, RIGHTARG=pg_catalog.int4); \
         CREATE FUNCTION {shadow}.to_jsonb(anyelement) RETURNS pg_catalog.jsonb LANGUAGE SQL IMMUTABLE AS 'SELECT NULL::pg_catalog.jsonb'; \
         CREATE FUNCTION {shadow}.jsonb_array_elements(pg_catalog.jsonb) RETURNS SETOF pg_catalog.jsonb LANGUAGE SQL IMMUTABLE AS 'SELECT NULL::pg_catalog.jsonb WHERE false'; \
         CREATE FUNCTION {shadow}.wrong_json_text(pg_catalog.jsonb,pg_catalog.text[]) RETURNS pg_catalog.text LANGUAGE SQL IMMUTABLE AS 'SELECT ''0''::pg_catalog.text'; \
         CREATE OPERATOR {shadow}.#>> (FUNCTION={shadow}.wrong_json_text, LEFTARG=pg_catalog.jsonb, RIGHTARG=pg_catalog.text[])"
    )).await?;
    let numeric = evaluation::qualified(schema, "shadow_numeric_keys")?;
    let textual = evaluation::qualified(schema, "shadow_text_keys")?;
    client.batch_execute(&format!(
        "CREATE TABLE {numeric} (row_id bigint, k numeric PRIMARY KEY, padding text); \
         ALTER TABLE {numeric} ALTER COLUMN padding SET STORAGE PLAIN; \
         INSERT INTO {numeric} SELECT g, 9007199254740992::numeric+g, repeat('p',3000) FROM generate_series(1,32) g; ANALYZE {numeric}; \
         CREATE TABLE {textual} (row_id bigint, k text PRIMARY KEY, padding text); \
         ALTER TABLE {textual} ALTER COLUMN padding SET STORAGE PLAIN; \
         INSERT INTO {textual} SELECT g, 'key-'||lpad(g::text,8,'0'), repeat('p',3000) FROM generate_series(1,32) g; ANALYZE {textual}"
    )).await?;
    for (table, qualified) in [("shadow_numeric_keys", numeric), ("shadow_text_keys", textual)] {
        let oid = client.query_one("SELECT $1::pg_catalog.text::pg_catalog.regclass::pg_catalog.oid", &[&qualified]).await?.get::<_, u32>(0);
        let expected = exact_rows(client, &qualified, "TRUE").await?;
        client.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY").await?;
        client.batch_execute(&format!("LOCK TABLE {qualified} IN ACCESS SHARE MODE")).await?;
        // With the ordinary path, qualification must preserve the native plan,
        // including PostgreSQL's recognition of TID/index range operators.
        for strategy in [Strategy::Ctid, Strategy::Indexed] {
            let plan = planning::prepare_relation_for_evaluation(client, schema, table, oid,
                NonZeroU32::new(4), strategy, NonZeroU32::new(4).unwrap()).await?;
            let qualified_predicate = plan.predicate(&plan.chunks()[1])?;
            let ordinary_predicate = qualified_predicate.replace("OPERATOR(pg_catalog.<)", "<").replace("OPERATOR(pg_catalog.>=)", ">=");
            let explain = |predicate: &str| format!("EXPLAIN (COSTS OFF) SELECT * FROM ONLY {qualified} WHERE {predicate}");
            let qualified_plan = client.query(&explain(&qualified_predicate), &[]).await?.into_iter().map(|r| r.get::<_, String>(0)).collect::<Vec<_>>();
            let ordinary_plan = client.query(&explain(&ordinary_predicate), &[]).await?.into_iter().map(|r| r.get::<_, String>(0)).collect::<Vec<_>>();
            assert_eq!(qualified_plan, ordinary_plan, "operator qualification changed the native query plan");
        }
        client.batch_execute(&format!("SET LOCAL search_path = {shadow}, pg_catalog")).await?;
        let search_path: String = client.query_one("SELECT pg_catalog.current_setting('search_path')", &[]).await?.get(0);
        assert!(!client.query_one("SELECT 1::pg_catalog.numeric < 2::pg_catalog.numeric", &[]).await?.get::<_, bool>(0));
        assert!(!client.query_one("SELECT '(1,0)'::pg_catalog.tid < '(2,0)'::pg_catalog.tid", &[]).await?.get::<_, bool>(0));
        for strategy in [Strategy::Ctid, Strategy::WeightedCtid, Strategy::Indexed, Strategy::HashCtid] {
            let plan = planning::prepare_relation_for_evaluation(client, schema, table, oid,
                NonZeroU32::new(4), strategy, NonZeroU32::new(4).unwrap()).await?;
            plan.validate_identity(client).await?;
            let mut actual = Vec::new();
            for chunk in plan.chunks() {
                actual.extend(exact_rows(client, &qualified, &plan.predicate(chunk)?).await?);
            }
            actual.sort();
            assert_eq!(actual, expected, "search_path changed {strategy:?} exact row coverage for {table}");
        }
        assert_eq!(client.query_one("SELECT pg_catalog.current_setting('search_path')", &[]).await?.get::<_, String>(0), search_path,
            "planning must preserve the session path used by user expressions/RLS");
        client.batch_execute("ROLLBACK").await?;
    }
    Ok(())
}

async fn evaluation_projection_preserves_original_generator_numeric_values(client: &tokio_postgres::Client) -> anyhow::Result<()> {
    use transferia_connector_postgres::postgres::source::UnsupportedTypePolicy;
    use transferia_connector_postgres::postgres::src_batch::source_select_projection;

    let fixture = "SELECT 18446744073709551615::numeric(20,0) AS exact_unsigned, \
        12345678901234567890.123456789012::numeric(40,12) AS exact_decimal, 'payload'::text AS payload";
    let metadata = client.prepare(fixture).await?;
    assert_eq!(metadata.columns()[0].type_(), &tokio_postgres::types::Type::NUMERIC);
    let projection = source_select_projection(metadata.columns(), UnsupportedTypePolicy::ToString)?;
    let row = client.query_one(&format!("SELECT {projection} FROM ({fixture}) AS source_values"), &[]).await?;
    assert_eq!(row.columns()[0].type_(), &tokio_postgres::types::Type::TEXT);
    assert_eq!(row.columns()[1].type_(), &tokio_postgres::types::Type::TEXT);
    assert_eq!(row.try_get::<_, String>(0)?, "18446744073709551615");
    assert_eq!(row.try_get::<_, String>(1)?, "12345678901234567890.123456789012");
    assert_eq!(row.try_get::<_, String>(2)?, "payload");
    Ok(())
}

async fn evaluation_discovers_weighted_when_auto_skips_observation(client: &tokio_postgres::Client, schema: &str) -> anyhow::Result<()> {
    use transferia_connector_postgres::postgres::src_batch::{planner::Strategy, planning};

    let table = "evaluation_probe_coverage";
    let qualified = evaluation::qualified(schema, table)?;
    let columns = (1..16).map(|index| format!("c{index} bigint")).collect::<Vec<_>>().join(", ");
    let values = (0..16).map(|_| "g").collect::<Vec<_>>().join(", ");
    // Dense fixed-width rows have no TOAST relation and estimated payload >
    // half the heap size. Auto therefore skips its suspicious-layout probe
    // independently of machine speed or measured request latency.
    client.batch_execute(&format!(
        "CREATE TABLE {qualified} (row_id bigint PRIMARY KEY, {columns}); \
         INSERT INTO {qualified} SELECT {values} FROM generate_series(1,40) AS g; \
         ANALYZE {qualified};"
    )).await?;
    let oid = client.query_one("SELECT $1::text::regclass::oid", &[&qualified]).await?.get::<_, u32>(0);
    let maximum = NonZeroU32::new(16);
    client.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY").await?;
    client.batch_execute(&format!("LOCK TABLE {qualified} IN ACCESS SHARE MODE")).await?;
    let automatic = planning::prepare_relation(client, schema, table, oid, maximum, None).await?;
    assert_eq!(automatic.decision().facts().heap_pages(), 1, "one page makes required sampling deterministic at 100 percent");
    assert!(!automatic.decision().facts().has_observation(), "fixture must exercise Auto's skipped-probe path");
    assert!(automatic.decision().candidates().iter().any(|candidate|
        candidate.strategy == Strategy::WeightedCtid && !candidate.eligible));
    let expanded = planning::prepare_relation_for_evaluation_candidates(client, schema, table, oid, maximum).await?;
    assert!(expanded.decision().facts().has_observation());
    assert!(expanded.decision().candidates().iter().any(|candidate|
        candidate.strategy == Strategy::WeightedCtid && candidate.eligible),
        "the evaluator must consider weighted cuts even when Auto did not collect their observation");
    let forced = planning::prepare_relation_for_evaluation(client, schema, table, oid, maximum,
        Strategy::WeightedCtid, NonZeroU32::MIN).await?;
    assert!(forced.decision().facts().has_observation(), "a timed weighted run must collect its own observation");
    let expected = exact_rows(client, &qualified, "TRUE").await?;
    let mut actual = Vec::new();
    for chunk in forced.chunks() {
        actual.extend(exact_rows(client, &qualified, &forced.predicate(chunk)?).await?);
    }
    actual.sort();
    assert_eq!(actual, expected);
    assert!(planning::prepare_relation_for_evaluation_candidates(client, schema, table, 0, maximum).await.is_err());
    client.batch_execute("ROLLBACK").await?;

    // A legitimate empty observation is an unavailable candidate, not a
    // swallowed query failure or permission to manufacture histogram weights.
    client.batch_execute(&format!("DELETE FROM {qualified}; ANALYZE {qualified}")).await?;
    client.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY").await?;
    client.batch_execute(&format!("LOCK TABLE {qualified} IN ACCESS SHARE MODE")).await?;
    let empty = planning::prepare_relation_for_evaluation_candidates(client, schema, table, oid, maximum).await?;
    assert!(!empty.decision().facts().has_observation());
    assert!(empty.decision().candidates().iter().any(|candidate|
        candidate.strategy == Strategy::WeightedCtid && !candidate.eligible));
    client.batch_execute("ROLLBACK").await?;
    Ok(())
}

async fn exact_rows(client: &tokio_postgres::Client, qualified: &str, predicate: &str) -> anyhow::Result<Vec<(i64, Vec<u8>)>> {
    let rows = client.query(&format!("SELECT row_id, pg_catalog.record_send(t) FROM {qualified} AS t WHERE {predicate}"), &[]).await?;
    let mut result = rows.into_iter().map(|r| Ok((r.try_get::<_, i64>(0)?, r.try_get::<_, Vec<u8>>(1)?)))
        .collect::<Result<Vec<_>, tokio_postgres::Error>>()?;
    result.sort();
    Ok(result)
}

async fn binary_rows(client: &tokio_postgres::Client, table: &str, predicate: &str) -> anyhow::Result<Vec<Vec<u8>>> {
    let mut rows = client.query(&format!("SELECT pg_catalog.record_send(t) FROM {table} AS t WHERE {predicate}"), &[])
        .await?.into_iter().map(|row| row.try_get::<_, Vec<u8>>(0)).collect::<Result<Vec<_>, _>>()?;
    rows.sort();
    Ok(rows)
}

/// Caller holds its relation lock and repeatable-read snapshot. Comparing the
/// sorted complete row representation preserves multiplicity, not only COUNT.
async fn research_plan_rows(client: &tokio_postgres::Client, schema: &str, table: &str) -> anyhow::Result<Vec<Vec<u8>>> {
    use transferia_connector_postgres::postgres::src_batch::{planner::Strategy, planning};
    let table = format!("research_{table}");
    let qualified = evaluation::qualified(schema, &table)?;
    let oid = client.query_one("SELECT $1::text::regclass::oid", &[&qualified]).await?.get::<_, u32>(0);
    let expected = binary_rows(client, &qualified, "TRUE").await?;
    let plan = planning::prepare_relation(client, schema, &table, oid, NonZeroU32::new(16), None).await?;
    let mut choices = vec![plan.clone()];
    let mut seen = Vec::new();
    for candidate in plan.decision().candidates().iter().filter(|candidate| candidate.eligible) {
        let bound = match candidate.strategy {
            Strategy::Single => 1,
            Strategy::Ctid | Strategy::WeightedCtid => u32::try_from(plan.decision().facts().heap_pages().max(1))?,
            Strategy::Indexed => plan.decision().facts().indexed_cuts().checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("indexed part count overflow"))?,
            Strategy::HashCtid => 16,
        }.min(16);
        let counts = [candidate.lanes, candidate.parts].into_iter().chain([1, 2, 4, 8, 16].into_iter().filter(|&parts| parts <= bound));
        for parts in counts {
            if seen.contains(&(candidate.strategy, parts)) { continue; }
            seen.push((candidate.strategy, parts));
            choices.push(planning::prepare_relation_for_evaluation(client, schema, &table, oid,
                NonZeroU32::new(16), candidate.strategy, NonZeroU32::new(parts).unwrap()).await?);
        }
    }
    for choice in choices {
        let mut actual = Vec::new();
        for chunk in choice.chunks() {
            actual.extend(binary_rows(client, &qualified, &choice.predicate(chunk)?).await?);
        }
        actual.sort();
        assert_eq!(actual, expected, "production {:?} failed exact rows for {table}", choice.decision().strategy());
    }
    Ok(expected)
}

async fn research_snapshot(client: &tokio_postgres::Client, schema: &str, table: &str) -> anyhow::Result<()> {
    client.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY").await?;
    client.batch_execute(&format!("LOCK TABLE {} IN ACCESS SHARE MODE", evaluation::qualified(schema, &format!("research_{table}"))?)).await?;
    Ok(())
}

async fn run_research_scenarios(client: &tokio_postgres::Client, config: &str, schema: &str) -> anyhow::Result<()> {
    use transferia_connector_postgres::postgres::src_batch::{planner::Strategy, planning};
    let q = |table: &str| evaluation::qualified(schema, &format!("research_{table}"));
    // Ensure eligible typed-index cases span actual heap pages. Their PK
    // values remain precisely those from the original research recipes.
    for table in ["uuid_keys", "binary_keys", "date_keys", "timestamp_keys", "decimal_keys", "float_keys", "locale_keys", "array_keys", "enum_keys", "wide_keys"] {
        client.batch_execute(&format!("ALTER TABLE {} ADD COLUMN padding text; ALTER TABLE {} ALTER COLUMN padding SET STORAGE PLAIN; UPDATE {} SET padding=repeat('p',3000); ANALYZE {}", q(table)?, q(table)?, q(table)?, q(table)?)).await?;
    }
    client.batch_execute(&format!("ANALYZE {}; UPDATE {} SET k=-k WHERE k<=100; INSERT INTO {} SELECT i,i FROM generate_series(1001,1500) i; ANALYZE {}; ALTER TABLE {} ALTER COLUMN k SET STATISTICS 0; ANALYZE {}",
        q("stats_stale")?, q("stats_stale")?, q("stats_stale")?, q("stats_mcv")?, q("stats_missing")?, q("stats_missing")?)).await?;
    let old_extent = client.query_one("SELECT pg_relation_size($1::text::regclass)/current_setting('block_size')::bigint", &[&q("growth")?]).await?.get::<_, i64>(0);
    client.batch_execute(&format!("INSERT INTO {} SELECT i,repeat('y',1800) FROM generate_series(11,100) i", q("growth")?)).await?;

    let cases = [
        ("empty_and_fewer_rows_than_workers", "empty_table"),
        ("stale_pg_class_relpages", "stale_pages"),
        ("empty_and_out_of_heap_ctid_bounds", "single_row"),
        ("bigint_extremes_sparse_ranges", "extreme_keys"),
        ("null_split_field_and_duplicate_keyset", "nullable_keys"),
        ("half_open_boundary_coverage", "extreme_keys"),
        ("composite_text_pk_server_collation", "composite_keys"),
        ("partition_ctid_scope", "parted"),
        ("uuid_server_order_ranges", "uuid_keys"),
        ("bytea_server_order_ranges", "binary_keys"),
        ("date_bc_infinity_server_ranges", "date_keys"),
        ("timestamptz_dst_microseconds_infinity", "timestamp_keys"),
        ("fractional_numeric_without_float_rounding", "decimal_keys"),
        ("float_nan_infinity_signed_zero", "float_keys"),
        ("default_database_text_collation", "locale_keys"),
        ("heterogeneous_composite_pk", "mixed_composite"),
        ("nullable_composite_patterns_and_ties", "null_patterns"),
        ("all_null_split_column", "all_nulls"),
        ("missing_and_disabled_column_statistics", "stats_missing"),
        ("stale_histogram_after_key_updates_and_inserts", "stats_stale"),
        ("mcv_skew_duplicate_quantile_boundaries", "stats_mcv"),
        ("empty_tablesample_preserves_coverage", "single_row"),
        ("wide_text_key_omitted_from_histogram", "wide_keys"),
        ("array_elements_nulls_dimensions_server_order", "array_keys"),
        ("enum_declared_order_not_lexical_order", "enum_keys"),
        ("extent_captured_before_snapshot", "growth"),
    ];
    let mut completed = Vec::new();
    for (case, table) in cases {
        research_snapshot(client, schema, table).await?;
        match case {
            "stale_pg_class_relpages" => {
                let row = client.query_one("SELECT relpages, pg_relation_size(oid)>0 FROM pg_class WHERE oid=$1::text::regclass", &[&q(table)?]).await?;
                assert_eq!(row.get::<_, i32>(0), 0);
                assert!(row.get::<_, bool>(1));
            }
            "empty_and_out_of_heap_ctid_bounds" => {
                assert!(binary_rows(client, &q(table)?, "ctid >= '(4000000000,0)'::tid").await?.is_empty());
                assert!(binary_rows(client, &q(table)?, "ctid >= '(2,0)'::tid AND ctid < '(1,0)'::tid").await?.is_empty());
            }
            "half_open_boundary_coverage" => {
                let mut inclusive = binary_rows(client, &q(table)?, "id<=0").await?;
                inclusive.extend(binary_rows(client, &q(table)?, "id>=0").await?);
                assert_eq!(inclusive.len(), 8, "counterexample must contain an overlapping boundary");
            }
            "partition_ctid_scope" => {
                let row = client.query_one(&format!("SELECT count(*), count(DISTINCT ctid), count(DISTINCT(tableoid,ctid)) FROM {}", q(table)?), &[]).await?;
                assert!(row.get::<_, i64>(1) < row.get::<_, i64>(0));
                assert_eq!(row.get::<_, i64>(2), row.get::<_, i64>(0));
            }
            "empty_tablesample_preserves_coverage" => {
                assert_eq!(client.query_one(&format!("SELECT count(*) FROM {} TABLESAMPLE SYSTEM(0)", q(table)?), &[]).await?.get::<_, i64>(0), 0);
            }
            "wide_text_key_omitted_from_histogram" => {
                let row = client.query_one("SELECT histogram_bounds IS NULL, most_common_vals IS NULL FROM pg_stats WHERE schemaname=$1 AND tablename=$2 AND attname='k'", &[&schema, &format!("research_{table}")]).await?;
                assert!(row.get::<_, bool>(0) && row.get::<_, bool>(1));
            }
            "enum_declared_order_not_lexical_order" => {
                let values = client.query(&format!("SELECT k::text AS value_text FROM {} AS t ORDER BY t.k", q(table)?), &[]).await?.into_iter().map(|r| r.get::<_, String>(0)).collect::<Vec<_>>();
                assert_eq!(values, ["z_low", "a_middle", "m_high"]);
            }
            "extent_captured_before_snapshot" => {
                let old_bounded = binary_rows(client, &q(table)?, &format!("ctid < '({old_extent},0)'::tid")).await?;
                assert!(old_bounded.len() < 100, "fixture must have grown beyond the old bound");
            }
            _ => {}
        }
        research_plan_rows(client, schema, table).await?;
        if case == "empty_and_fewer_rows_than_workers" {
            research_plan_rows(client, schema, "single_row").await?;
        }
        client.batch_execute("ROLLBACK").await?;
        completed.push(case);
    }

    let (writer, writer_connection) = tokio_postgres::connect(config, tokio_postgres::NoTls).await?;
    let writer_driver = tokio::spawn(writer_connection);
    for (table, case, mutation) in [
        ("key_moves", "moving_keys_same_snapshot_vs_mixed", "UPDATE {table} SET id=101 WHERE id=1; UPDATE {table} SET id=0 WHERE id=99"),
        ("tid_moves", "moving_ctid_same_snapshot_vs_mixed", "UPDATE {table} SET payload=repeat('z',1800) WHERE id=1"),
    ] {
        research_snapshot(client, schema, table).await?;
        let before = research_plan_rows(client, schema, table).await?;
        writer.batch_execute(&mutation.replace("{table}", &q(table)?)).await?;
        let after = research_plan_rows(client, schema, table).await?;
        assert_eq!(before, after, "retained snapshot changed during concurrent write");
        assert_ne!(before, binary_rows(&writer, &q(table)?, "TRUE").await?, "writer must actually change current rows");
        if table == "key_moves" {
            let mut mixed = client.query(&format!("SELECT logical_id FROM {} WHERE id<50", q(table)?), &[]).await?.into_iter().map(|r| r.get::<_, i64>(0)).collect::<Vec<_>>();
            mixed.extend(writer.query(&format!("SELECT logical_id FROM {} WHERE id>=50", q(table)?), &[]).await?.into_iter().map(|r| r.get::<_, i64>(0)));
            assert_eq!(mixed.len(), 100);
            assert_eq!(mixed.iter().filter(|&&id| id == 1).count(), 2);
            assert!(!mixed.contains(&99));
        }
        client.batch_execute("ROLLBACK").await?;
        completed.push(case);
    }

    research_snapshot(client, schema, "single_row").await?;
    let exported = client.query_one("SELECT pg_export_snapshot()::text", &[]).await?.get::<_, String>(0);
    writer.batch_execute(&format!("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT '{exported}'")).await?;
    client.batch_execute("ROLLBACK").await?;
    client.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY").await?;
    let expired = client.batch_execute(&format!("SET TRANSACTION SNAPSHOT '{exported}'")).await.unwrap_err();
    assert_eq!(expired.as_db_error().unwrap().code().code(), "42704");
    client.batch_execute("ROLLBACK").await?;
    completed.push("exported_snapshot_lifetime");
    let reexported = writer.query_one("SELECT pg_export_snapshot()::text", &[]).await?.get::<_, String>(0);
    assert_ne!(exported, reexported);
    client.batch_execute(&format!("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT '{reexported}'")).await?;
    assert_eq!(binary_rows(client, &q("single_row")?, "TRUE").await?, binary_rows(&writer, &q("single_row")?, "TRUE").await?);
    client.batch_execute("ROLLBACK").await?;
    writer.batch_execute("ROLLBACK").await?;
    completed.push("live_importer_reexports_after_original_exporter_ends");

    research_snapshot(client, schema, "single_row").await?;
    writer.batch_execute("SET lock_timeout='100ms'").await?;
    let blocked = writer.batch_execute(&format!("ALTER TABLE {} ADD COLUMN should_be_blocked integer", q("single_row")?)).await.unwrap_err();
    assert_eq!(blocked.as_db_error().unwrap().code().code(), "55P03");
    client.batch_execute("ROLLBACK").await?;
    completed.push("coordinator_lock_blocks_rewrite");

    research_snapshot(client, schema, "stale_pages").await?;
    let table = q("stale_pages")?;
    let oid = client.query_one("SELECT $1::text::regclass::oid", &[&table]).await?.get::<_, u32>(0);
    let native = planning::prepare_relation_for_evaluation(client, schema, "research_stale_pages", oid, NonZeroU32::new(16), Strategy::Ctid, NonZeroU32::new(16).unwrap()).await?;
    let plan = client.query(&format!("EXPLAIN SELECT * FROM {table} WHERE {}", native.predicate(&native.chunks()[0])?), &[]).await?.into_iter().map(|r| r.get::<_, String>(0)).collect::<Vec<_>>().join("\n");
    assert!(plan.contains("Tid Range Scan"), "native production predicate lost Tid Range Scan: {plan}");
    client.batch_execute("ROLLBACK").await?;
    completed.push("native_tid_predicate_vs_extracted_block");

    client.batch_execute(&format!("ANALYZE {}; CREATE ROLE snapshot_planner_reader; GRANT USAGE ON SCHEMA {} TO snapshot_planner_reader; GRANT SELECT ON {} TO snapshot_planner_reader; ALTER TABLE {} ENABLE ROW LEVEL SECURITY; ALTER TABLE {} FORCE ROW LEVEL SECURITY; CREATE POLICY only_even ON {} USING(id%2=0)", q("rls_keys")?, evaluation::quote_identifier(schema)?, q("rls_keys")?, q("rls_keys")?, q("rls_keys")?, q("rls_keys")?)).await?;
    client.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET LOCAL ROLE snapshot_planner_reader").await?;
    client.batch_execute(&format!("LOCK TABLE {} IN ACCESS SHARE MODE", q("rls_keys")?)).await?;
    assert_eq!(client.query_one("SELECT count(*) FROM pg_stats WHERE schemaname=$1 AND tablename='research_rls_keys'", &[&schema]).await?.get::<_, i64>(0), 0);
    assert_eq!(research_plan_rows(client, schema, "rls_keys").await?.len(), 50);
    client.batch_execute("ROLLBACK").await?;
    completed.push("forced_rls_hides_stats_preserves_authorized_ranges");

    client.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY").await?;
    client.batch_execute(&format!("LOCK TABLE ONLY {} IN SHARE UPDATE EXCLUSIVE MODE; LOCK TABLE ONLY {}, ONLY {} IN ACCESS SHARE MODE", q("parted")?, q("parted_a")?, q("parted_b")?)).await?;
    let before = research_plan_rows(client, schema, "parted").await?;
    writer.batch_execute(&format!("INSERT INTO {} VALUES (1,99)", q("parted")?)).await?;
    for statement in [
        format!("ALTER TABLE {} ATTACH PARTITION {} FOR VALUES IN (3)", q("parted")?, q("attach_candidate")?),
        format!("ANALYZE {}", q("parted")?),
    ] {
        let error = writer.batch_execute(&statement).await.unwrap_err();
        assert_eq!(error.as_db_error().unwrap().code().code(), "55P03");
    }
    assert_eq!(research_plan_rows(client, schema, "parted").await?, before);
    assert_eq!(binary_rows(&writer, &q("parted")?, "TRUE").await?.len(), before.len()+1);
    client.batch_execute("ROLLBACK").await?;
    completed.push("partition_topology_sue_allows_dml_blocks_attach");
    assert_eq!(completed.len(), 34);
    for case in completed { eprintln!("research_case={case} result=pass"); }
    drop(writer);
    writer_driver.await??;
    Ok(())
}

#[tokio::test]
async fn source_freezes_inheritance_membership_and_requires_task_acknowledgements() -> anyhow::Result<()> {
    use std::sync::Arc;
    use arrow::array::{Int64Array, StringArray};
    use tokio_util::sync::CancellationToken;
    use transferia_connector_postgres::postgres::PostgresSourceConnector;
    use transferia_core::data::message::SourceBatch;
    use transferia_core::delivery::DeliveryDiscoveryRequest;
    use transferia_core::memory::PipelineMemory;
    use transferia_delivery_contracts::DeliveryType;
    use transferia_registry::{SourceConnector as _, SourceBuildContext, SourceDiscoveryContext, SourceExecutionContext, SourcePhase};

    let postgres = GenericImage::new("postgres", "17.6-bookworm")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr("database system is ready to accept connections"))
        .with_env_var("POSTGRES_PASSWORD", "test")
        .with_env_var("POSTGRES_DB", "transferia")
        .start().await?;
    let host = postgres.get_host().await?.to_string();
    let port = postgres.get_host_port_ipv4(5432.tcp()).await?;
    let (client, connection) = tokio_postgres::connect(&format!("host={host} port={port} user=postgres password=test dbname=transferia"), tokio_postgres::NoTls).await?;
    let driver = tokio::spawn(connection);
    client.batch_execute("SET lock_timeout='1s'").await?;
    for (case_index, with_child) in [false, true].into_iter().enumerate() {
        for format in ["binary", "text"] {
            let parent = format!("membership_{case_index}_{format}");
            let child = format!("{parent}_child");
            let unrelated = format!("{parent}_new");
            client.batch_execute(&format!(
                "CREATE TABLE {parent}(row_id bigint PRIMARY KEY, payload text); \
                 INSERT INTO {parent} VALUES(1,'parent-before'); \
                 CREATE TABLE {unrelated}(LIKE {parent} INCLUDING ALL); \
                 INSERT INTO {unrelated} VALUES(3,'unrelated-before-snapshot')"
            )).await?;
            let mut expected = vec![(1_i64, "parent-before".to_owned())];
            if with_child {
                client.batch_execute(&format!("CREATE TABLE {child}() INHERITS({parent}); INSERT INTO {child} VALUES(2,'child-before')")).await?;
                expected.push((2, "child-before".to_owned()));
            }
            let connector = PostgresSourceConnector::from_config(serde_json::from_value(serde_json::json!({
                "host": host, "port": port, "database": "transferia", "username": "postgres", "password": "test",
                "trusted_plaintext": true, "tables": {"type":"selected", "rules":[{"include":format!("public.{parent}")}]},
                "copy_to_format":format, "batch_rows":1, "max_snapshot_parts":4
            }))?, Arc::new(transferia_connector_postgres::metrics::MetricsRegistry::new()))?;
            let cancellation = CancellationToken::new();
            let durable = transferia_test_support::durable_context();
            connector.delivery_discovery(SourceDiscoveryContext {
                request: DeliveryDiscoveryRequest { keep_system_columns: false }, cancellation: cancellation.child_token(), delivery_type: DeliveryType::Batch,
            }).await?;
            let prepared = connector.prepare_execution(SourceExecutionContext {
                request: DeliveryDiscoveryRequest { keep_system_columns: false }, cancellation: cancellation.child_token(), delivery_type: DeliveryType::Batch,
                replay_identity: None, durable: durable.clone(),
            }).await?.expect("batch source must prepare a fixed snapshot queue");
            // All child rows predate the snapshot. MVCC alone cannot exclude
            // them if a later recursive FROM silently expands membership.
            client.batch_execute(&format!("ALTER TABLE {unrelated} INHERIT {}", if with_child { &child } else { &parent })).await?;
            let current = client.query_one(&format!("SELECT count(*) FROM {parent}"), &[]).await?.get::<_, i64>(0);
            assert_eq!(current, expected.len() as i64 + 1, "INHERIT must succeed under the retained guards");
            let partitions = prepared.remaining_phases[0].topology.static_partitions().unwrap();
            let mut sources = Vec::new();
            for &partition_id in partitions {
                sources.push(connector.build_source(SourceBuildContext {
                    partition_id, delivery_type: DeliveryType::Batch, phase: SourcePhase::Snapshot,
                    replay_identity: None, cancellation: cancellation.child_token(), memory: PipelineMemory::new(16*1024*1024), durable: durable.clone(),
                }).await?);
            }
            let mut actual = Vec::new();
            let mut source_markers = Vec::new();
            let mut empty_fences = 0;
            for source in &mut sources {
                let mut markers = Vec::new();
                loop {
                    match source.read_batch().await? {
                        SourceBatch::Typed { tables, commit_marker, .. } => {
                            if tables.is_empty() && commit_marker.is_some() { empty_fences += 1; }
                            for table in tables {
                                let ids = table.batch.column_by_name("row_id").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
                                let payloads = table.batch.column_by_name("payload").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
                                for row in 0..table.batch.num_rows() { actual.push((ids.value(row), payloads.value(row).to_owned())); }
                            }
                            if let Some(marker) = commit_marker { markers.push(marker); }
                        }
                        SourceBatch::Finished => break,
                        _ => panic!("snapshot must produce typed rows"),
                    }
                }
                source_markers.push(markers);
            }
            actual.sort();
            assert_eq!(actual, expected, "late inherited rows leaked into the fixed {format} snapshot");
            assert!(empty_fences > 0, "every completed task requires an explicit end fence");
            assert!(connector.complete_execution_phase(SourcePhase::Snapshot, durable.clone(), cancellation.child_token()).await.is_err(),
                "COPY completion must not bypass destination acknowledgement");
            for (source, markers) in sources.iter_mut().zip(source_markers) {
                source.commit_offsets(&markers).await?;
                source.shutdown().await?;
            }
            connector.complete_execution_phase(SourcePhase::Snapshot, durable, cancellation.child_token()).await?;
        }
    }
    drop(client);
    driver.await??;
    Ok(())
}

#[tokio::test]
async fn actual_parallel_snapshot_lanes_preserve_rows_and_gate_both_delivery_modes() -> anyhow::Result<()> {
    use std::sync::Arc;
    use arrow::array::{Int64Array, StringArray};
    use tokio_util::sync::CancellationToken;
    use transferia_connector_postgres::postgres::PostgresSourceConnector;
    use transferia_core::data::message::SourceBatch;
    use transferia_core::delivery::DeliveryDiscoveryRequest;
    use transferia_core::memory::PipelineMemory;
    use transferia_delivery_contracts::DeliveryType;
    use transferia_registry::{SourceConnector as _, SourceBuildContext, SourceDiscoveryContext, SourceExecutionContext, SourcePhase};

    const ROWS: usize = 20_000;
    let postgres = GenericImage::new("postgres", "17.6-bookworm")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr("database system is ready to accept connections"))
        .with_env_var("POSTGRES_PASSWORD", "test")
        .with_env_var("POSTGRES_DB", "transferia")
        .with_cmd(["postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=10", "-c", "max_wal_senders=10"])
        .start().await?;
    let host = postgres.get_host().await?.to_string();
    let port = postgres.get_host_port_ipv4(5432.tcp()).await?;
    let (client, connection) = tokio_postgres::connect(&format!("host={host} port={port} user=postgres password=test dbname=transferia"), tokio_postgres::NoTls).await?;
    let driver = tokio::spawn(connection);
    // Real uncompressed heap data makes parallelism useful without forged
    // relpages, injected cost rates, or a forced production strategy.
    client.batch_execute(&format!(
        "CREATE TABLE parallel_snapshot(row_id bigint PRIMARY KEY, payload text NOT NULL); \
         ALTER TABLE parallel_snapshot ALTER COLUMN payload SET STORAGE PLAIN; \
         ALTER TABLE parallel_snapshot REPLICA IDENTITY FULL; \
         INSERT INTO parallel_snapshot SELECT g,repeat('x',4080)||lpad(g::text,16,'0') FROM generate_series(0,{}::bigint) g; \
         ANALYZE parallel_snapshot; \
         CREATE PUBLICATION parallel_snapshot_publication FOR TABLE parallel_snapshot WITH(publish='insert,update,delete')", ROWS-1
    )).await?;
    for (mode_index, delivery_type) in [DeliveryType::Batch, DeliveryType::BatchAndStream].into_iter().enumerate() {
        for format in ["binary", "text"] {
            client.batch_execute("UPDATE parallel_snapshot SET payload=repeat('x',4080)||lpad(row_id::text,16,'0') WHERE row_id=0").await?;
            let transfer_id = format!("parallel_{mode_index}_{format}");
            let connector = PostgresSourceConnector::from_config(serde_json::from_value(serde_json::json!({
                "host":host, "port":port, "database":"transferia", "username":"postgres", "password":"test", "trusted_plaintext":true,
                "tables":{"type":"selected","rules":[{"include":"public.parallel_snapshot"}]},
                "batch_rows":128, "copy_to_format":format, "max_snapshot_parts":4,
                "replication":{"plugin":{"type":"pgoutput","publication":"parallel_snapshot_publication"},"poll_interval_ms":1}
            }))?, Arc::new(transferia_connector_postgres::metrics::MetricsRegistry::new()))?;
            let cancellation = CancellationToken::new();
            let durable = transferia_test_support::durable_contexts(&[&transfer_id]).remove(0);
            let replay_identity = (delivery_type == DeliveryType::BatchAndStream).then(|| Arc::<str>::from("parallel-snapshot-e2e-v1"));
            connector.delivery_discovery(SourceDiscoveryContext {
                request:DeliveryDiscoveryRequest { keep_system_columns:false }, cancellation:cancellation.child_token(), delivery_type,
            }).await?;
            let prepared = connector.prepare_execution(SourceExecutionContext {
                request:DeliveryDiscoveryRequest { keep_system_columns:false }, cancellation:cancellation.child_token(), delivery_type,
                replay_identity:replay_identity.clone(), durable:durable.clone(),
            }).await?.expect("snapshot must prepare its actual lane topology");
            let partitions = prepared.remaining_phases[0].topology.static_partitions().unwrap().to_vec();
            assert!((2..=4).contains(&partitions.len()), "real 160-MiB heap must exercise multiple lanes: {partitions:?}");
            if delivery_type == DeliveryType::BatchAndStream {
                assert_eq!(prepared.remaining_phases.len(), 2);
                assert_eq!(prepared.remaining_phases[1].phase, SourcePhase::Stream);
            }
            client.batch_execute("UPDATE parallel_snapshot SET payload='after-snapshot' WHERE row_id=0").await?;
            let mut sources = Vec::new();
            for partition_id in partitions {
                sources.push(connector.build_source(SourceBuildContext {
                    partition_id, delivery_type, phase:SourcePhase::Snapshot,
                    replay_identity:replay_identity.clone(), cancellation:cancellation.child_token(),
                    memory:PipelineMemory::new(16*1024*1024), durable:durable.clone(),
                }).await?);
            }
            let results = futures_util::future::join_all(sources.into_iter().map(|mut source| async move {
                let mut ids_seen = Vec::new();
                let mut markers = Vec::new();
                let mut source_rows_total = 0_u64;
                let mut task_ends = 0_usize;
                loop {
                    match source.read_batch().await? {
                        SourceBatch::Typed { tables, source_rows, commit_marker, .. } => {
                            let mut batch_rows = 0_u64;
                            if tables.is_empty() {
                                assert!(commit_marker.is_some(), "empty completion must carry its acknowledgement marker");
                                task_ends += 1;
                            }
                            for table in tables {
                                let ids = table.batch.column_by_name("row_id").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
                                let payloads = table.batch.column_by_name("payload").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
                                for row in 0..table.batch.num_rows() {
                                    let id = usize::try_from(ids.value(row))?;
                                    anyhow::ensure!(id < ROWS, "snapshot emitted an unexpected identity");
                                    let expected = format!("{}{:016}", "x".repeat(4080), id);
                                    anyhow::ensure!(payloads.value(row) == expected, "snapshot payload differs at row {id}");
                                    ids_seen.push(id);
                                }
                                batch_rows += table.batch.num_rows() as u64;
                            }
                            assert_eq!(source_rows, batch_rows, "source row counters differ from actual Arrow rows");
                            source_rows_total += source_rows;
                            if let Some(marker) = commit_marker { markers.push(marker); }
                        }
                        SourceBatch::Finished => break,
                        _ => panic!("snapshot emitted a non-typed batch"),
                    }
                }
                Ok::<_, anyhow::Error>((source, ids_seen, markers, source_rows_total, task_ends))
            })).await.into_iter().collect::<anyhow::Result<Vec<_>>>()?;
            assert!(results.iter().filter(|(_, ids, _, _, _)| !ids.is_empty()).count() >= 2,
                "multiple advertised lanes must actually read data");
            let mut seen = vec![false; ROWS];
            for (_, ids, _, _, _) in &results {
                for &id in ids {
                    assert!(!seen[id], "duplicate primary key {id} across real source lanes");
                    seen[id] = true;
                }
            }
            assert!(seen.into_iter().all(|seen| seen), "parallel snapshot lost rows");
            assert_eq!(results.iter().map(|(_,_,_,rows,_)| rows).sum::<u64>(), ROWS as u64);
            assert!(results.iter().map(|(_,_,_,_,ends)| ends).sum::<usize>() >= 2);
            assert!(connector.complete_execution_phase(SourcePhase::Snapshot, durable.clone(), cancellation.child_token()).await.is_err(),
                "phase changed before every task was durably acknowledged");
            for (mut source, _, markers, _, _) in results {
                source.commit_offsets(&markers).await?;
                source.shutdown().await?;
            }
            connector.complete_execution_phase(SourcePhase::Snapshot, durable.clone(), cancellation.child_token()).await?;
            if delivery_type == DeliveryType::BatchAndStream {
                let mut stream = connector.build_source(SourceBuildContext {
                    partition_id:0, delivery_type, phase:SourcePhase::Stream, replay_identity:replay_identity.clone(),
                    cancellation:cancellation.child_token(), memory:PipelineMemory::new(16*1024*1024), durable:durable.clone(),
                }).await?;
                tokio::time::timeout(std::time::Duration::from_secs(15), async {
                    loop {
                        match stream.read_batch().await? {
                            SourceBatch::Typed { tables, commit_marker, .. } => {
                                let mut found = false;
                                for table in tables {
                                    if let (Some(ids), Some(payloads)) = (table.batch.column_by_name("row_id"), table.batch.column_by_name("payload")) {
                                        let ids = ids.as_any().downcast_ref::<Int64Array>().unwrap();
                                        let payloads = payloads.as_any().downcast_ref::<StringArray>().unwrap();
                                        found |= (0..table.batch.num_rows()).any(|row| ids.value(row)==0 && payloads.value(row)=="after-snapshot");
                                    }
                                }
                                if let Some(marker) = commit_marker { stream.commit_offsets(&[marker]).await?; }
                                if found { return Ok::<_, anyhow::Error>(()); }
                            }
                            SourceBatch::Finished => anyhow::bail!("CDC ended before the post-snapshot update"),
                            _ => anyhow::bail!("CDC emitted an unexpected batch kind"),
                        }
                    }
                }).await??;
                stream.shutdown().await?;
            }
        }
    }
    drop(client);
    driver.await??;
    Ok(())
}

#[tokio::test]
async fn cancelling_combined_preparation_releases_a_blocked_topology_guard() -> anyhow::Result<()> {
    use anyhow::Context as _;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;
    use transferia_connector_postgres::postgres::PostgresSourceConnector;
    use transferia_core::delivery::DeliveryDiscoveryRequest;
    use transferia_delivery_contracts::DeliveryType;
    use transferia_registry::{SourceConnector as _, SourceDiscoveryContext, SourceExecutionContext};

    let postgres = GenericImage::new("postgres", "17.6-bookworm")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr("database system is ready to accept connections"))
        .with_env_var("POSTGRES_PASSWORD", "test")
        .with_env_var("POSTGRES_DB", "transferia")
        .with_cmd(["postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=10", "-c", "max_wal_senders=10"])
        .start().await?;
    let host = postgres.get_host().await?.to_string();
    let port = postgres.get_host_port_ipv4(5432.tcp()).await?;
    let config = format!("host={host} port={port} user=postgres password=test dbname=transferia");
    let (client, connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls).await?;
    let driver = tokio::spawn(connection);
    client.batch_execute("CREATE TABLE cancelled_guard(row_id bigint PRIMARY KEY); CREATE TABLE cancelled_child() INHERITS(cancelled_guard); CREATE PUBLICATION cancelled_guard_publication FOR TABLE cancelled_guard WITH(publish='insert,update,delete')").await?;
    let connector = Arc::new(PostgresSourceConnector::from_config(serde_json::from_value(serde_json::json!({
        "host":host,"port":port,"database":"transferia","username":"postgres","password":"test","trusted_plaintext":true,
        "tables":{"type":"selected","rules":[{"include":"public.cancelled_guard"}]},
        "replication":{"plugin":{"type":"pgoutput","publication":"cancelled_guard_publication"}}
    }))?, Arc::new(transferia_connector_postgres::metrics::MetricsRegistry::new()))?);
    let cancellation = CancellationToken::new();
    connector.delivery_discovery(SourceDiscoveryContext {
        request:DeliveryDiscoveryRequest { keep_system_columns:false }, cancellation:cancellation.child_token(), delivery_type:DeliveryType::BatchAndStream,
    }).await?;
    let (blocker, blocker_connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls).await?;
    let blocker_driver = tokio::spawn(blocker_connection);
    blocker.batch_execute("BEGIN; LOCK TABLE ONLY cancelled_guard IN SHARE UPDATE EXCLUSIVE MODE").await?;
    let durable = transferia_test_support::durable_contexts(&["cancelled_guard_snapshot"]).remove(0);
    let task_connector = Arc::clone(&connector);
    let task_cancellation = cancellation.child_token();
    let mut preparation = tokio::spawn(async move {
        task_connector.prepare_execution(SourceExecutionContext {
            request:DeliveryDiscoveryRequest { keep_system_columns:false }, cancellation:task_cancellation,
            delivery_type:DeliveryType::BatchAndStream, replay_identity:Some(Arc::from("cancelled-guard-e2e-v1")), durable,
        }).await
    });
    let blocked_pid = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let row = tokio::select! {
                result = &mut preparation => anyhow::bail!("preparation ended before requesting the topology guard: {result:?}"),
                row = client.query_opt("SELECT pid FROM pg_locks WHERE relation='public.cancelled_guard'::regclass AND mode='ShareUpdateExclusiveLock' AND NOT granted", &[]) => row?,
            };
            if let Some(row) = row {
                return Ok::<_, anyhow::Error>(row.get::<_, i32>(0));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.context("preparation did not request the conflicting topology guard within 10 seconds")??;
    cancellation.cancel();
    let result = tokio::time::timeout(Duration::from_secs(2), preparation).await
        .context("preparation did not return within 2 seconds after cancellation")??;
    let error = result.expect_err("cancelled source preparation must fail promptly");
    assert!(error.to_string().contains("cancel"), "unexpected preparation failure: {error}");
    // Do not release the conflicting lock until the abandoned connection is
    // gone: otherwise a detached driver could hide behind successful cleanup.
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let exists = client.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1)", &[&blocked_pid]).await?.get::<_, bool>(0);
            if !exists { return Ok::<_, anyhow::Error>(()); }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.context("cancelled preparation backend remained alive behind the conflicting lock for more than 3 seconds")??;
    assert_eq!(client.query_one("SELECT count(*) FROM pg_replication_slots WHERE slot_name='cancelled_guard_snapshot'", &[]).await?.get::<_, i64>(0), 0,
        "slot bootstrap must not precede the cancelled topology guard");
    blocker.batch_execute("ROLLBACK").await?;
    drop(blocker);
    blocker_driver.await??;
    drop(client);
    driver.await??;
    Ok(())
}
