//! PostgreSQL catalog/probe adapter for the pure snapshot policy.
//!
//! The caller owns an imported or exported repeatable-read snapshot and guards
//! acquired **before** that snapshot. Ordinary heaps require ACCESS SHARE for
//! the epoch. Inheritance/partition topology needs its own retained guard and
//! currently keeps a complete query here. This adapter never acquires stronger
//! locks, changes snapshots, calls ANALYZE, or uses estimates as coverage bounds.
//! All predicates preserve the original table projection and PostgreSQL types.
//! Builtin operators/functions are explicitly qualified: an explicit role
//! search_path can put user operators before pg_catalog even for builtin types,
//! while ORDER BY still uses the type's default btree ordering. Qualification
//! preserves that ordering contract without changing user RLS/session semantics.

use std::fmt;
use std::num::NonZeroU32;
use std::time::Instant;

use anyhow::Context;
use tokio_postgres::Client;
use transferia_connector_support::external_request::observe_external_request;

use super::planner::{self, Chunk, ChunkKind, Constraints, CostRates, Decision, OutputDistribution, PhysicalAccess, PhysicalDistribution, RawTableFacts, Strategy, TableFacts};
use crate::connectors::postgres::common::{quote_identifier, validate_identifier};
use crate::connectors::postgres::source::DiscoveredTable;

/// Physical identity is checked separately from Arrow/schema identity. Retained
/// relation guards make this identity stable until the snapshot epoch ends.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalIdentity {
    relation_oid: u32,
    relfilenode: u32,
    relation_kind: String,
    access_method_oid: u32,
    schema: String,
    table: String,
}

impl PhysicalIdentity {
    pub fn relation_oid(&self) -> u32 { self.relation_oid }
}

#[derive(Clone)]
struct TypedCuts {
    column: String,
    sql_type: String,
    values: Vec<String>,
}

impl fmt::Debug for TypedCuts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("TypedCuts").field("count", &self.values.len()).finish_non_exhaustive()
    }
}

/// Immutable, completely prepared table plan. Descriptors contain no SQL or row
/// values; only this adapter can turn its server-ordered cuts into predicates.
/// Debug output never includes sampled keys or raw predicate values.
#[derive(Clone, Debug)]
pub struct PreparedTable {
    decision: Decision,
    chunks: Vec<Chunk>,
    physical_identity: PhysicalIdentity,
    typed_cuts: Option<TypedCuts>,
}

impl PreparedTable {
    pub fn decision(&self) -> &Decision { &self.decision }
    pub fn chunks(&self) -> &[Chunk] { &self.chunks }
    pub fn physical_identity(&self) -> &PhysicalIdentity { &self.physical_identity }

    /// Render a complete predicate, without a WHERE keyword. Inputs are opaque
    /// validated descriptors produced by this policy or its exact-cover split.
    pub fn predicate(&self, chunk: &Chunk) -> anyhow::Result<String> {
        let descriptor_strategy = match chunk.kind() {
            ChunkKind::Whole => Strategy::Single,
            ChunkKind::Ctid { .. } => Strategy::Ctid,
            ChunkKind::Indexed { .. } => Strategy::Indexed,
            ChunkKind::HashCtid { .. } => Strategy::HashCtid,
        };
        anyhow::ensure!(descriptor_strategy == self.decision.strategy()
            || (descriptor_strategy == Strategy::Ctid && self.decision.strategy() == Strategy::WeightedCtid),
            "snapshot descriptor does not match its prepared strategy");
        Ok(match chunk.kind() {
            ChunkKind::Whole => "TRUE".to_owned(),
            ChunkKind::Ctid { lower_page, upper_page, .. } => {
                let mut predicates = Vec::new();
                if let Some(page) = lower_page { predicates.push(format!("ctid OPERATOR(pg_catalog.>=) '({page},0)'::pg_catalog.tid")); }
                if let Some(page) = upper_page { predicates.push(format!("ctid OPERATOR(pg_catalog.<) '({page},0)'::pg_catalog.tid")); }
                if predicates.is_empty() { "TRUE".to_owned() } else { predicates.join(" AND ") }
            }
            ChunkKind::Indexed { lower_cut, upper_cut } => {
                if lower_cut.is_none() && upper_cut.is_none() { return Ok("TRUE".to_owned()); }
                let cuts = self.typed_cuts.as_ref().context("indexed snapshot descriptor has no typed boundary catalog")?;
                let expression = quote_identifier(&cuts.column);
                let mut predicates = Vec::new();
                for (cut, operator) in [(lower_cut, ">="), (upper_cut, "<")] {
                    if let Some(index) = cut {
                        let value = cuts.values.get(*index as usize).context("indexed snapshot boundary is outside its catalog")?;
                        predicates.push(format!("{expression} OPERATOR(pg_catalog.{operator}) {}::{}", quote_literal(value), cuts.sql_type));
                    }
                }
                if predicates.is_empty() { "TRUE".to_owned() } else { predicates.join(" AND ") }
            }
            ChunkKind::HashCtid { bucket, modulus } => {
                anyhow::ensure!(modulus.get() == self.decision.initial_parts() && *bucket < modulus.get(), "hash snapshot descriptor does not match its prepared bucket set");
                // Cast to bigint before normalization: abs(INT_MIN) overflows.
                format!("(((pg_catalog.hashtid(ctid)::pg_catalog.int8 OPERATOR(pg_catalog.%) {0}) OPERATOR(pg_catalog.+) {0}) OPERATOR(pg_catalog.%) {0}) OPERATOR(pg_catalog.=) {bucket}", modulus.get())
            }
        })
    }

    pub async fn validate_identity(&self, client: &Client) -> anyhow::Result<()> {
        let identity = &self.physical_identity;
        let row = observe_external_request("postgres", "snapshot_validate_physical_identity", client.query_opt(
            "SELECT c.oid, c.relfilenode, c.relkind::pg_catalog.text, c.relam FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) c.relnamespace WHERE c.oid OPERATOR(pg_catalog.=) $1 AND n.nspname OPERATOR(pg_catalog.=) $2 AND c.relname OPERATOR(pg_catalog.=) $3",
            &[&identity.relation_oid, &identity.schema, &identity.table],
        )).await.map_err(redact_database_error)?.context("snapshot relation no longer has its discovered identity")?;
        anyhow::ensure!(
            row.try_get::<_, u32>(0)? == identity.relation_oid
                && row.try_get::<_, u32>(1)? == identity.relfilenode
                && row.try_get::<_, String>(2)? == identity.relation_kind
                && row.try_get::<_, u32>(3)? == identity.access_method_oid,
            "snapshot physical relation identity changed; the epoch cannot continue"
        );
        Ok(())
    }
}

pub(crate) async fn prepare_table(client: &Client, discovered: &DiscoveredTable, max_parts: Option<NonZeroU32>) -> anyhow::Result<PreparedTable> {
    prepare_relation(client, &discovered.config.schema, &discovered.config.name, discovered.relation_oid, max_parts, None).await
}

/// Prepare a named relation using the same production path. The caller must
/// establish the snapshot/guard contract documented at module level. `forced`
/// exists for evaluation and must never be exposed as a product/UI setting.
pub async fn prepare_relation(client: &Client, schema: &str, table: &str, expected_oid: u32, max_parts: Option<NonZeroU32>, forced: Option<Strategy>) -> anyhow::Result<PreparedTable> {
    prepare(client, schema, table, expected_oid, max_parts, forced.map(|strategy| (strategy, None)), false).await
}

/// Discover evaluator candidates independently of Auto's probe-pay decision.
/// Native guarded heaps attempt the observation needed by weighted ranges even
/// when Auto would skip it. An empty observation leaves that candidate explicitly
/// ineligible; database/metadata failures still propagate. Use this only for
/// untimed candidate discovery, never as the measured Auto plan. Timed weighted
/// runs must call `prepare_relation_for_evaluation` and pay for their own probe.
pub async fn prepare_relation_for_evaluation_candidates(client: &Client, schema: &str, table: &str, expected_oid: u32, max_parts: Option<NonZeroU32>) -> anyhow::Result<PreparedTable> {
    prepare(client, schema, table, expected_oid, max_parts, None, true).await
}

/// Force an applicable candidate at an exact part count for release evaluation.
/// This changes neither metadata collection nor predicate construction.
pub async fn prepare_relation_for_evaluation(client: &Client, schema: &str, table: &str, expected_oid: u32, max_parts: Option<NonZeroU32>, strategy: Strategy, parts: NonZeroU32) -> anyhow::Result<PreparedTable> {
    prepare(client, schema, table, expected_oid, max_parts, Some((strategy, Some(parts))), false).await
}

async fn prepare(client: &Client, schema: &str, table: &str, expected_oid: u32, max_parts: Option<NonZeroU32>, forced: Option<(Strategy, Option<NonZeroU32>)>, evaluation_candidates: bool) -> anyhow::Result<PreparedTable> {
    validate_identifier("schema", schema)?;
    validate_identifier("table", table)?;
    anyhow::ensure!(expected_oid != 0, "snapshot relation OID must be nonzero");
    tracing::info!(target: "transferia.postgres.snapshot_planner", schema, table, relation_oid = expected_oid, max_parts = max_parts.map(NonZeroU32::get), "Checking how this table can be partitioned for its snapshot");
    let started = Instant::now();
    let request = Instant::now();
    let facts_query = format!("{TABLE_FACTS_SQL} LEFT JOIN LATERAL ({INDEX_CUTS_SQL}) AS index_facts ON TRUE WHERE c.oid OPERATOR(pg_catalog.=) $1 AND n.nspname OPERATOR(pg_catalog.=) $2 AND c.relname OPERATOR(pg_catalog.=) $3");
    let row = observe_external_request("postgres", "snapshot_planner_table_facts", client.query_opt(&facts_query, &[&expected_oid, &schema, &table])).await.map_err(redact_database_error)?
        .context("snapshot relation no longer matches discovery")?;
    let request_seconds = request.elapsed().as_secs_f64();
    anyhow::ensure!(request_seconds > 0.0, "snapshot metadata request timer returned zero duration");
    let relation_kind: String = row.try_get("relation_kind")?;
    let am: Option<String> = row.try_get("access_method")?;
    let inherited: bool = row.try_get("has_children")?;
    let heap = relation_kind == "r" && am.as_deref() == Some("heap") && !inherited;
    let physical_access = if heap { PhysicalAccess::GuardedHeap } else { PhysicalAccess::CompleteQueryOnly };
    let heap_bytes: i64 = row.try_get("heap_bytes")?;
    let block_bytes: i32 = row.try_get("block_bytes")?;
    anyhow::ensure!(heap_bytes >= 0 && block_bytes > 0, "PostgreSQL returned an invalid physical heap size");
    let heap_pages = u64::try_from(heap_bytes)?.div_ceil(u64::try_from(block_bytes)?);
    let estimate: f64 = row.try_get("estimated_rows")?;
    anyhow::ensure!(estimate.is_finite() && (estimate >= 0.0 || estimate == -1.0), "PostgreSQL returned an invalid catalog row estimate");
    let mut estimated_rows = (estimate >= 0.0).then_some(estimate);
    let average_width: Option<f64> = row.try_get("average_width")?;
    let mut estimated_output_bytes = estimated_rows.zip(average_width).map(|(rows, width)| rows * width);
    let toast_bytes: i64 = row.try_get("toast_bytes")?;
    anyhow::ensure!(toast_bytes >= 0, "PostgreSQL returned a negative TOAST relation size");
    let server_version: i32 = row.try_get("server_version_num")?;
    let stats_age: Option<f64> = row.try_get("statistics_age_seconds")?;
    let modified_rows: Option<i64> = row.try_get("modified_rows_since_analyze")?;
    let column_names: Vec<String> = row.try_get("column_names")?;
    let column_oids: Vec<u32> = row.try_get("column_oids")?;
    anyhow::ensure!(column_names.len() == column_oids.len(), "snapshot column metadata lengths disagree");
    let column_count = NonZeroU32::new(u32::try_from(column_names.len())?).context("snapshot relation has no columns")?;
    let numeric_text_columns = u32::try_from(column_oids.iter().filter(|oid| **oid == 1700).count())?;
    let correlation: Option<f64> = row.try_get("correlation")?;
    let eager_numeric_values: Option<Vec<String>> = row.try_get("prepared_numeric_cuts")?;
    let boundaries_already_collected = eager_numeric_values.is_some();
    let catalog_cuts = if let Some(values) = &eager_numeric_values {
        if values.len() >= 2 { u32::try_from(values.len())? } else { 0 }
    } else { u32::try_from(row.try_get::<_, Option<i32>>("cut_count")?.unwrap_or(0))? };
    let eager_numeric_cuts = match eager_numeric_values {
        Some(values) if values.len() >= 2 => Some(TypedCuts {
            column: row.try_get("column_name")?, sql_type: row.try_get("sql_type")?, values,
        }),
        _ => None,
    };
    let physical_identity = PhysicalIdentity {
        relation_oid: expected_oid,
        relfilenode: row.try_get("relfilenode")?,
        relation_kind,
        access_method_oid: row.try_get("access_method_oid")?,
        schema: schema.to_owned(),
        table: table.to_owned(),
    };
    // External storage can hide output work from pg_stats.avg_width. Physical
    // TOAST bytes are an uncertain work prior, not a lower bound on live output
    // (compression and dead tuples matter), nor evidence of actual skew/cache.
    let catalog_output_bytes = estimated_output_bytes;
    let external_storage_dominates = heap && toast_bytes > heap_bytes;
    let mut output_estimate_origin = "catalog";
    if external_storage_dominates {
        estimated_output_bytes = Some(estimated_output_bytes.unwrap_or(heap_bytes as f64).max(toast_bytes as f64));
        output_estimate_origin = "catalog_with_physical_toast_risk_prior";
    }
    let constraints = Constraints::new(max_parts);
    let mut raw_facts = RawTableFacts {
        physical_access,
        native_tid_ranges: server_version >= 140_000,
        heap_pages,
        block_bytes: u32::try_from(block_bytes)?,
        estimated_rows,
        estimated_output_bytes,
        metadata_seconds: started.elapsed().as_secs_f64(),
        request_seconds,
        indexed_cuts: if heap { catalog_cuts } else { 0 },
        indexed_boundary_seconds: if boundaries_already_collected { 0.0 } else { request_seconds },
        index_correlation: correlation,
        output_distribution: if external_storage_dominates {
            OutputDistribution::UnobservedConcentration
        } else { OutputDistribution::UniformPrior },
        rates: CostRates::priors_for_projected_columns(column_count, numeric_text_columns)?,
    };
    let unobserved_facts = TableFacts::try_from(raw_facts.clone())?;
    // Zero physical pages needs no probe or histogram. It still receives a
    // complete query, because concurrent extension and stale catalogs must not
    // turn an estimate into an empty source.
    let can_consider_splits = max_parts.is_none_or(|cap| cap.get() > 1)
        && forced.is_none_or(|(strategy, _)| strategy != Strategy::Single);
    let needs_policy_estimate = forced.is_none_or(|(_, parts)| parts.is_none());
    // Weighted cuts depend on this observation even when P is fixed by the
    // evaluator. Its real query and elapsed planning cost must remain inside
    // every measured run, just as they do for an automatic weighted plan.
    let needs_weighted_boundaries = forced.is_some_and(|(strategy, _)| strategy == Strategy::WeightedCtid);
    if heap && heap_pages > 0 && can_consider_splits
        && (needs_weighted_boundaries || (evaluation_candidates && server_version >= 140_000) || (needs_policy_estimate
            && probe_can_pay(heap_bytes as f64, toast_bytes as f64, estimated_output_bytes, request_seconds)))
    {
        let observation = ObservationPlan::choose(heap_pages, u32::try_from(block_bytes)?, toast_bytes as f64,
            estimated_rows, &column_oids, request_seconds)?;
        // Keep ordinary non-dominant-TOAST behavior unchanged. The external
        // output risk path prices information against a distribution-insensitive
        // single/hash alternative before paying for an observation.
        let information = if external_storage_dominates {
            Some(planner::evaluate_observation(&unobserved_facts, constraints, observation.estimated_probe_seconds())?)
        } else { None };
        let evaluator_requires_observation = needs_weighted_boundaries || evaluation_candidates;
        let should_observe = evaluator_requires_observation || information.as_ref().is_none_or(planner::ObservationValue::should_observe);
        if let Some(information) = &information {
            tracing::info!(target: "transferia.postgres.snapshot_planner", schema, table,
                information_value = ?information, should_observe, evaluator_requires_observation,
                output_estimate_origin,
                "Observation value compared with an available single/hash plan before querying distribution");
        }
        if should_observe {
            let sample = collect_distribution(client, schema, table, heap_pages, &column_names, &column_oids, &observation).await?;
            if let Some(sample) = sample {
                estimated_output_bytes = Some(sample.estimated_output_bytes);
                output_estimate_origin = "physical_observation_with_storage_width_proxies";
                if estimated_rows.is_none() || estimated_rows == Some(0.0) { estimated_rows = Some(sample.estimated_rows); }
                raw_facts.output_distribution = OutputDistribution::Observed(sample.distribution);
            }
        }
    }
    raw_facts.estimated_rows = estimated_rows;
    raw_facts.estimated_output_bytes = estimated_output_bytes;
    raw_facts.metadata_seconds = started.elapsed().as_secs_f64();
    let mut facts = TableFacts::try_from(raw_facts.clone())?;
    let preliminary = planner::decide(facts.clone(), constraints)?;
    let materialize_keys = match forced {
        Some((Strategy::Indexed, Some(parts))) => parts.get() > 1,
        Some((Strategy::Indexed, None)) => true,
        Some(_) => false,
        None => preliminary.strategy() == Strategy::Indexed && preliminary.lanes() > 1,
    };
    let typed_cuts = if materialize_keys {
        let typed = match eager_numeric_cuts {
            Some(cuts) => Some(cuts),
            None => collect_index_cuts(client, expected_oid, &row).await?,
        };
        let mut refined = raw_facts;
        refined.indexed_cuts = typed.as_ref().map_or(Ok(0), |cuts| u32::try_from(cuts.values.len()))?;
        refined.metadata_seconds = started.elapsed().as_secs_f64();
        refined.indexed_boundary_seconds = 0.0;
        facts = TableFacts::try_from(refined)?;
        typed
    } else { None };
    tracing::info!(target: "transferia.postgres.snapshot_planner", schema, table, heap_pages, heap_bytes, block_bytes, server_version, columns = column_count.get(), numeric_text_columns, estimated_rows, estimated_output_bytes, catalog_output_bytes, output_estimate_origin, output_distribution = facts.output_distribution().label(), toast_bytes, statistics_age_seconds = stats_age, modified_rows_since_analyze = modified_rows, has_children = inherited, physical_heap_eligible = heap, indexed_cuts = facts.indexed_cuts(), index_correlation = correlation, boundaries_in_first_request = boundaries_already_collected, first_request_boundary_count = if boundaries_already_collected { catalog_cuts } else { 0 }, indexed_boundary_seconds = facts.indexed_boundary_seconds(), typed_boundaries_materialized = typed_cuts.is_some(), has_physical_observation = facts.has_observation(), metadata_ms = started.elapsed().as_secs_f64() * 1000.0, request_ms = request_seconds * 1000.0, rates = ?facts.rates(), estimates_are_exact = false, "Snapshot planning facts collected; catalog estimates do not define row coverage");
    let decision = match forced {
        None => planner::decide(facts, constraints)?,
        Some((strategy, Some(parts))) => planner::decide_for_evaluation(facts, constraints, strategy, parts)?,
        Some((strategy, None)) => {
            let automatic = planner::decide(facts.clone(), constraints)?;
            let candidate = automatic.candidates().iter().find(|candidate| candidate.strategy == strategy && candidate.eligible)
                .context("forced snapshot strategy is not eligible")?;
            planner::decide_for_evaluation(facts, constraints, strategy, NonZeroU32::new(candidate.lanes).context("eligible candidate has no lanes")?)?
        }
    };
    if let Some(observation) = decision.facts().physical_distribution() {
        tracing::info!(target: "transferia.postgres.snapshot_planner", schema, table,
            sampled_rows = observation.sampled_rows(), sampled_pages = observation.sampled_pages(),
            bins = observation.weights().len(), observed_output_bytes = observation.weights().iter().sum::<f64>(),
            "Physical workload observation used by snapshot candidates; detailed bin weights are available at DEBUG");
    }
    for candidate in decision.candidates() {
        tracing::debug!(target: "transferia.postgres.snapshot_planner", schema, table, strategy = candidate.strategy.label(), lanes = candidate.lanes, parts = candidate.parts, eligible = candidate.eligible, reason = candidate.reason, estimated_cost = ?candidate.cost, "Snapshot partitioning candidate evaluated");
    }
    tracing::info!(target: "transferia.postgres.snapshot_planner", schema, table, strategy = decision.strategy().label(), lanes = decision.lanes(), initial_parts = decision.initial_parts(), max_parts = decision.max_parts(), reason = decision.reason(), confidence = "estimated", "Snapshot plan selected; lane count is a recommendation, not a source load guarantee");
    let chunks = planner::chunks(&decision)?;
    let prepared = PreparedTable { decision, chunks, physical_identity, typed_cuts };
    // Complete construction is the validation boundary. Consumers never need
    // to remember a separate intrinsic-validity call before publishing lanes.
    for descriptor in prepared.chunks() { drop(prepared.predicate(descriptor)?); }
    Ok(prepared)
}

const TABLE_FACTS_SQL: &str = "
SELECT c.relkind::pg_catalog.text AS relation_kind, c.relfilenode, c.relam AS access_method_oid,
       am.amname::pg_catalog.text AS access_method,
       EXISTS (SELECT 1 FROM pg_catalog.pg_inherits WHERE inhparent OPERATOR(pg_catalog.=) c.oid) AS has_children,
       CASE WHEN c.relkind OPERATOR(pg_catalog.=) 'r' OR c.relkind OPERATOR(pg_catalog.=) 'm' THEN pg_catalog.pg_relation_size(c.oid, 'main') ELSE 0 END AS heap_bytes,
       pg_catalog.current_setting('block_size')::pg_catalog.int4 AS block_bytes,
       pg_catalog.current_setting('server_version_num')::pg_catalog.int4 AS server_version_num,
       c.reltuples::pg_catalog.float8 AS estimated_rows,
       (SELECT CASE WHEN pg_catalog.count(*) OPERATOR(pg_catalog.=) (SELECT pg_catalog.count(*) FROM pg_catalog.pg_attribute a WHERE a.attrelid OPERATOR(pg_catalog.=) c.oid AND a.attnum OPERATOR(pg_catalog.>) 0 AND NOT a.attisdropped) THEN (pg_catalog.sum(s.avg_width OPERATOR(pg_catalog.*) (1.0 OPERATOR(pg_catalog.-) s.null_frac)) OPERATOR(pg_catalog.+) 2 OPERATOR(pg_catalog.+) (4 OPERATOR(pg_catalog.*) pg_catalog.count(*)))::pg_catalog.float8 ELSE NULL END FROM pg_catalog.pg_stats s WHERE s.schemaname OPERATOR(pg_catalog.=) n.nspname AND s.tablename OPERATOR(pg_catalog.=) c.relname AND NOT s.inherited) AS average_width,
       (SELECT pg_catalog.array_agg(a.attname::pg_catalog.text ORDER BY a.attnum) FROM pg_catalog.pg_attribute a WHERE a.attrelid OPERATOR(pg_catalog.=) c.oid AND a.attnum OPERATOR(pg_catalog.>) 0 AND NOT a.attisdropped) AS column_names,
       (SELECT pg_catalog.array_agg(a.atttypid ORDER BY a.attnum) FROM pg_catalog.pg_attribute a WHERE a.attrelid OPERATOR(pg_catalog.=) c.oid AND a.attnum OPERATOR(pg_catalog.>) 0 AND NOT a.attisdropped) AS column_oids,
       CASE WHEN c.reltoastrelid OPERATOR(pg_catalog.<>) 0 THEN pg_catalog.pg_relation_size(c.reltoastrelid, 'main') ELSE 0 END AS toast_bytes,
       EXTRACT(EPOCH FROM (pg_catalog.clock_timestamp() OPERATOR(pg_catalog.-) GREATEST(st.last_analyze, st.last_autoanalyze)))::pg_catalog.float8 AS statistics_age_seconds,
       st.n_mod_since_analyze AS modified_rows_since_analyze,
       index_facts.column_name, index_facts.sql_type, index_facts.cut_count, index_facts.prepared_numeric_cuts,
       index_facts.correlation, index_facts.collation_schema, index_facts.collation_name
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) c.relnamespace
LEFT JOIN pg_catalog.pg_am am ON am.oid OPERATOR(pg_catalog.=) c.relam
LEFT JOIN pg_catalog.pg_stat_all_tables st ON st.relid OPERATOR(pg_catalog.=) c.oid";

// The builtin default ASC btree contract is deliberately narrow. Composite,
// custom opclass, expression, nondeterministic collation and inherited cases
// keep the type-independent CTID alternative; column membership alone is not
// proof of a usable ordering. PostgreSQL owns comparison and serialization.
// Integer/NUMERIC cuts share exact native numeric ordering and are normalized
// inside the first request. Do not trust the stored histogram's ordinal order:
// restored statistics can contain unordered/duplicate bounds. This tiny sort
// never visits table rows and keeps key values out of logs and Rust numerics.
const INDEX_CUTS_SQL: &str = "
SELECT a.attname::pg_catalog.text AS column_name, pg_catalog.format_type(a.atttypid, a.atttypmod) AS sql_type,
       pg_catalog.array_length(s.histogram_bounds, 1) AS cut_count, s.correlation::pg_catalog.float8 AS correlation,
       CASE WHEN a.atttypid OPERATOR(pg_catalog.=) ANY (ARRAY[20, 21, 23, 1700]::pg_catalog.oid[]) AND a.attcollation OPERATOR(pg_catalog.=) 0 THEN
           ARRAY(SELECT pg_catalog.to_jsonb(ordered.cut) OPERATOR(pg_catalog.#>>) '{}' FROM
               (SELECT DISTINCT (h.value OPERATOR(pg_catalog.#>>) '{}')::pg_catalog.numeric AS cut
                FROM pg_catalog.jsonb_array_elements(pg_catalog.to_jsonb(s.histogram_bounds)) AS h(value)) AS ordered
               ORDER BY ordered.cut)
           ELSE NULL::pg_catalog.text[] END AS prepared_numeric_cuts,
       colln.nspname::pg_catalog.text AS collation_schema, coll.collname::pg_catalog.text AS collation_name
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class idx ON idx.oid OPERATOR(pg_catalog.=) i.indexrelid
JOIN pg_catalog.pg_am am ON am.oid OPERATOR(pg_catalog.=) idx.relam AND am.amname OPERATOR(pg_catalog.=) 'btree'
JOIN pg_catalog.pg_attribute a ON a.attrelid OPERATOR(pg_catalog.=) i.indrelid AND a.attnum OPERATOR(pg_catalog.=) i.indkey[0]
JOIN pg_catalog.pg_opclass op ON op.oid OPERATOR(pg_catalog.=) i.indclass[0] AND op.opcdefault AND op.opcintype OPERATOR(pg_catalog.=) a.atttypid
JOIN pg_catalog.pg_namespace opn ON opn.oid OPERATOR(pg_catalog.=) op.opcnamespace AND opn.nspname OPERATOR(pg_catalog.=) 'pg_catalog'
JOIN pg_catalog.pg_class c ON c.oid OPERATOR(pg_catalog.=) i.indrelid
JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) c.relnamespace
LEFT JOIN pg_catalog.pg_collation coll ON coll.oid OPERATOR(pg_catalog.=) a.attcollation
LEFT JOIN pg_catalog.pg_namespace colln ON colln.oid OPERATOR(pg_catalog.=) coll.collnamespace
LEFT JOIN pg_catalog.pg_stats s ON s.schemaname OPERATOR(pg_catalog.=) n.nspname AND s.tablename OPERATOR(pg_catalog.=) c.relname AND s.attname OPERATOR(pg_catalog.=) a.attname AND NOT s.inherited
WHERE i.indrelid OPERATOR(pg_catalog.=) $1 AND i.indisprimary AND i.indisvalid AND i.indisready
  AND i.indnkeyatts OPERATOR(pg_catalog.=) 1 AND i.indpred IS NULL AND i.indexprs IS NULL AND i.indoption[0] OPERATOR(pg_catalog.=) 0
  AND i.indcollation[0] OPERATOR(pg_catalog.=) a.attcollation
  AND a.attnotnull AND (a.attcollation OPERATOR(pg_catalog.=) 0 OR coll.collisdeterministic)
  AND a.atttypid OPERATOR(pg_catalog.=) ANY (ARRAY[20, 21, 23, 25, 700, 701, 1042, 1043, 1082, 1114, 1184, 1700, 2950, 17]::pg_catalog.oid[])";

async fn collect_index_cuts(client: &Client, oid: u32, row: &tokio_postgres::Row) -> anyhow::Result<Option<TypedCuts>> {
    let Some(count) = row.try_get::<_, Option<i32>>("cut_count")? else { return Ok(None); };
    // Never decode numeric boundaries through a JSON/f64 client intermediary.
    // PostgreSQL JSONB retains arbitrary-precision numeric values and returns
    // exact text only after performing the JSON conversion on the server.
    if count < 2 { return Ok(None); }
    let sql_type: String = row.try_get("sql_type")?;
    let collation = match (row.try_get::<_, Option<String>>("collation_schema")?, row.try_get::<_, Option<String>>("collation_name")?) {
        (Some(schema), Some(name)) => format!(" COLLATE {}.{}", quote_identifier(&schema), quote_identifier(&name)),
        (None, None) => String::new(),
        _ => anyhow::bail!("primary key collation identity is incomplete"),
    };
    // Re-sort and deduplicate using today's actual PostgreSQL type/collation,
    // rather than trusting the order of an old ANALYZE histogram. Stale bounds
    // remain useful hints, but nonmonotonic bounds must never create overlaps.
    // JSON encoding supplies ISO temporal values and exact numeric text without
    // depending on the keeper's DateStyle or converting keys through Rust f64.
    let query = format!(
        "WITH typed AS (SELECT ((h.value OPERATOR(pg_catalog.#>>) '{{}}')::{sql_type}){collation} AS cut FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON c.oid OPERATOR(pg_catalog.=) i.indrelid JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) c.relnamespace JOIN pg_catalog.pg_attribute a ON a.attrelid OPERATOR(pg_catalog.=) i.indrelid AND a.attnum OPERATOR(pg_catalog.=) i.indkey[0] JOIN pg_catalog.pg_stats s ON s.schemaname OPERATOR(pg_catalog.=) n.nspname AND s.tablename OPERATOR(pg_catalog.=) c.relname AND s.attname OPERATOR(pg_catalog.=) a.attname AND NOT s.inherited CROSS JOIN LATERAL pg_catalog.jsonb_array_elements(pg_catalog.to_jsonb(s.histogram_bounds)) AS h(value) WHERE i.indrelid OPERATOR(pg_catalog.=) $1 AND i.indisprimary) SELECT pg_catalog.to_jsonb(cut) OPERATOR(pg_catalog.#>>) '{{}}' AS boundary FROM (SELECT DISTINCT cut FROM typed) ordered ORDER BY cut"
    );
    let text_rows = observe_external_request("postgres", "snapshot_planner_typed_boundaries", client.query(&query, &[&oid])).await.map_err(redact_database_error)?;
    let mut exact = Vec::with_capacity(text_rows.len());
    for row in text_rows {
        let value: String = row.try_get("boundary")?;
        exact.push(value);
    }
    if exact.len() < 2 { return Ok(None); }
    Ok(Some(TypedCuts { column: row.try_get("column_name")?, sql_type, values: exact }))
}

fn probe_can_pay(heap_bytes: f64, toast_bytes: f64, output: Option<f64>, request_seconds: f64) -> bool {
    // A physical probe is useful only when catalog clues suggest imbalance and
    // the serial work prior exceeds the cost of two more requests. This is a
    // planning decision, not a table-size limit or a proof that pages are cached.
    let rates = CostRates::priors();
    let work_seconds = heap_bytes / rates.heap_bytes_per_second()
        + output.unwrap_or(heap_bytes).max(toast_bytes) / rates.output_bytes_per_second();
    let suspicious = output.is_none() || toast_bytes > 0.0 || output.is_some_and(|bytes| bytes < heap_bytes / 2.0);
    suspicious && work_seconds > request_seconds * 2.0 && heap_bytes > 0.0
}

struct PhysicalSample {
    distribution: PhysicalDistribution,
    estimated_output_bytes: f64,
    estimated_rows: f64,
}

/// A probe policy, not a delivery limit. Sparse page sampling can entirely miss
/// a physically clustered wide-value tail: 55 random page trials miss a 1% tail
/// about 58% of the time. More bins cannot repair an unobserved tail.
///
/// Inspect every page only when builtin width functions avoid fetching/converting
/// external values, the heap-read prior fits within one reader's setup prior,
/// and all row/field inspection work is cheaper than an ideal two-way saving on
/// external output. TOAST bytes are a risk/cost hint, not an output lower bound:
/// compression and dead TOAST tuples can make this estimate inaccurate.
/// Text/bytea raw lengths reveal the external tail; NUMERIC may accompany them
/// using pg_column_size as a cheap storage-width proxy, never numeric::text.
/// Its projected text size remains uncertain, especially for compressed or
/// unbounded NUMERIC. Custom, JSON and array text projections remain sampled.
///
/// Complete observations use page resolution up to 4096 equal physical bins.
/// The ceiling bounds estimator metadata/computation only; it never limits rows,
/// table size, chunks or source eligibility. Larger observed heaps still have
/// within-bin uncertainty. The usual sparse probe retains sixteen bins.
#[derive(Debug)]
struct ObservationPlan {
    percent: f64,
    bins: usize,
    reason: &'static str,
    cheap_native_widths: bool,
    numeric_storage_proxy_columns: usize,
    full_heap_seconds: f64,
    full_width_seconds: f64,
    estimated_probe_seconds: f64,
    reader_setup_seconds: f64,
    potential_saving_seconds: f64,
}

impl ObservationPlan {
    /// The same probe that would execute is priced before any query is sent.
    /// This excludes unknown executor/grouping overhead, so it is an optimistic
    /// prior; a probe rejected even at this price cannot justify its real cost.
    fn estimated_probe_seconds(&self) -> f64 { self.estimated_probe_seconds }

    fn choose(pages: u64, block_bytes: u32, toast_bytes: f64, rows: Option<f64>, type_oids: &[u32], request_seconds: f64) -> anyhow::Result<Self> {
        anyhow::ensure!(pages > 0 && pages <= u64::from(u32::MAX) && block_bytes > 0,
            "physical observation needs a valid nonempty PostgreSQL heap");
        anyhow::ensure!(toast_bytes.is_finite() && toast_bytes >= 0.0 && request_seconds.is_finite() && request_seconds > 0.0,
            "physical observation costs must be finite and nonnegative with positive request duration");
        anyhow::ensure!(rows.is_none_or(|rows| rows.is_finite() && rows >= 0.0),
            "physical observation row estimate must be finite and nonnegative");
        let columns = NonZeroU32::new(u32::try_from(type_oids.len())?).context("physical observation has no columns")?;
        let rates = CostRates::priors_for_columns(columns);
        let heap_bytes = pages as f64 * f64::from(block_bytes);
        let full_heap_seconds = heap_bytes / rates.heap_bytes_per_second();
        let estimated_rows = rows.filter(|rows| *rows > 0.0).unwrap_or(heap_bytes / 128.0);
        let full_width_seconds = full_heap_seconds + estimated_rows / rates.rows_per_second();
        let reader_setup_seconds = request_seconds * 4.0;
        let potential_saving_seconds = toast_bytes / rates.output_bytes_per_second() / 2.0;
        anyhow::ensure!([full_heap_seconds, full_width_seconds, reader_setup_seconds, potential_saving_seconds].iter().all(|value| value.is_finite()),
            "physical observation cost overflow");
        let cheap_native_widths = type_oids.iter().all(|oid| *oid == 1700 || tokio_postgres::types::Type::from_oid(*oid)
            .is_some_and(|kind| !crate::connectors::postgres::common::postgres_requires_text_projection(&kind)));
        let has_raw_external_width = type_oids.iter().any(|oid| matches!(*oid, 17 | 25 | 1042 | 1043));
        let numeric_storage_proxy_columns = type_oids.iter().filter(|oid| **oid == 1700).count();
        let reason = if !cheap_native_widths { "sample_only_expensive_or_unknown_width_projection" }
            else if !has_raw_external_width { "sample_only_no_raw_external_width_column" }
            else if toast_bytes <= heap_bytes { "sample_only_external_storage_not_dominant" }
            else if full_heap_seconds > reader_setup_seconds { "sample_only_heap_inspection_exceeds_reader_setup" }
            else if full_width_seconds >= potential_saving_seconds { "sample_only_full_width_work_exceeds_potential_saving" }
            else { "complete_native_width_observation_repays_external_output_uncertainty" };
        let complete = reason == "complete_native_width_observation_repays_external_output_uncertainty";
        let percent = if complete { 100.0 } else { 100.0 / (pages as f64).sqrt() };
        let estimated_probe_seconds = request_seconds + full_width_seconds * (percent / 100.0);
        anyhow::ensure!(estimated_probe_seconds.is_finite(), "physical observation price overflow");
        Ok(Self {
            percent,
            bins: if complete { usize::try_from(pages.min(4096))? } else { 16 },
            reason, cheap_native_widths, numeric_storage_proxy_columns, full_heap_seconds, full_width_seconds, estimated_probe_seconds,
            reader_setup_seconds, potential_saving_seconds,
        })
    }
}

async fn collect_distribution(client: &Client, schema: &str, table: &str, pages: u64, columns: &[String], type_oids: &[u32], observation: &ObservationPlan) -> anyhow::Result<Option<PhysicalSample>> {
    if pages == 0 { return Ok(None); }
    let percent = observation.percent;
    let bins = observation.bins;
    tracing::info!(target: "transferia.postgres.snapshot_planner", schema, table,
        reason = observation.reason, sample_percent = percent, bins,
        cheap_native_widths = observation.cheap_native_widths,
        numeric_storage_proxy_columns = observation.numeric_storage_proxy_columns,
        estimated_full_heap_ms = observation.full_heap_seconds * 1000.0,
        estimated_full_width_ms = observation.full_width_seconds * 1000.0,
        estimated_reader_setup_ms = observation.reader_setup_seconds * 1000.0,
        estimated_potential_saving_ms = observation.potential_saving_seconds * 1000.0,
        "Physical observation selected; costs are priors and TOAST size is an uncertainty hint");
    // PostgreSQL's structural bounds (at most 1600 table columns, each datum
    // below 1 GiB) put one row's width well within int8. Avoid per-field NUMERIC
    // arithmetic; SUM(bigint) still uses a NUMERIC accumulator across rows.
    let mut width = format!("{}::pg_catalog.int8", 2 + 4 * columns.len());
    for (column, oid) in columns.iter().zip(type_oids) {
        let column = format!("t.{}", quote_identifier(column));
        // Text/bytea octet_length reads the raw TOAST length from its header;
        // pg_column_size would measure compressed storage. Other text-projected
        // types use their actual text conversion only for this optional sample.
        let term = if matches!(*oid, 17 | 25 | 1042 | 1043) {
            format!("pg_catalog.octet_length({column})")
        } else if *oid != 1700 && tokio_postgres::types::Type::from_oid(*oid).is_none_or(|kind| crate::connectors::postgres::common::postgres_requires_text_projection(&kind)) {
            format!("pg_catalog.octet_length({column}::pg_catalog.text)")
        } else { format!("pg_catalog.pg_column_size({column})") };
        width.push_str(&format!(" OPERATOR(pg_catalog.+) COALESCE({term}, 0)"));
    }
    let query = format!(
        "SELECT LEAST({}, (((ctid::pg_catalog.text::pg_catalog.point)[0]::pg_catalog.int8 OPERATOR(pg_catalog.*) {bins}) OPERATOR(pg_catalog./) {pages})::pg_catalog.int4) AS bin, pg_catalog.count(*)::pg_catalog.int8 AS rows, pg_catalog.sum({width})::pg_catalog.text AS bytes, pg_catalog.count(DISTINCT (ctid::pg_catalog.text::pg_catalog.point)[0])::pg_catalog.int8 AS pages FROM ONLY {}.{} AS t TABLESAMPLE pg_catalog.system ({percent}) REPEATABLE (0) GROUP BY bin",
        bins - 1, quote_identifier(schema), quote_identifier(table),
    );
    let started = Instant::now();
    let result = observe_external_request("postgres", "snapshot_planner_physical_sample", client.query(&query, &[])).await.map_err(redact_database_error)?;
    let mut weights = vec![0.0; bins];
    let mut sampled_rows = 0_u64;
    let mut sampled_pages = 0_u64;
    for row in result {
        let bin = usize::try_from(row.try_get::<_, i32>("bin")?)?;
        let weight: f64 = row.try_get::<_, String>("bytes")?.parse()?;
        *weights.get_mut(bin).context("physical sample returned an invalid page bin")? = weight;
        sampled_rows = sampled_rows.checked_add(u64::try_from(row.try_get::<_, i64>("rows")?)?).context("physical sample row count overflow")?;
        sampled_pages = sampled_pages.checked_add(u64::try_from(row.try_get::<_, i64>("pages")?)?).context("physical sample page count overflow")?;
    }
    if sampled_rows == 0 {
        tracing::info!(target: "transferia.postgres.snapshot_planner", schema, table,
            sample_percent = percent, bins, observed_rows = 0, elapsed_ms = started.elapsed().as_secs_f64() * 1000.0,
            "Physical observation completed without visible rows; complete query coverage is retained");
        return Ok(None);
    }
    // Horvitz-Thompson expansion uses the page selection probability, not the
    // count of populated sampled pages: the latter would erase empty-prefix
    // bloat and overestimate rows by the live-page fraction's inverse.
    let estimated_output_bytes = weights.iter().sum::<f64>() * 100.0 / percent;
    let estimated_rows = sampled_rows as f64 * 100.0 / percent;
    let distribution = PhysicalDistribution::new(weights, sampled_rows, sampled_pages)?;
    tracing::info!(target: "transferia.postgres.snapshot_planner", schema, table,
        sampled_rows = distribution.sampled_rows(), sampled_pages = distribution.sampled_pages(), bins,
        sample_percent = percent, elapsed_ms = started.elapsed().as_secs_f64() * 1000.0,
        estimated_rows, estimated_output_bytes, "Physical observation completed; all pages remain in snapshot coverage");
    tracing::debug!(target: "transferia.postgres.snapshot_planner", bin_output_bytes = ?distribution.weights(), sample_percent = percent, sampling = "SYSTEM", repeatable_seed = 0, width_method = "raw_payload_with_native_storage_proxies_and_framing", "Physical output histogram; within-bin skew and projected text sizes remain estimates");
    Ok(Some(PhysicalSample { distribution, estimated_output_bytes, estimated_rows }))
}

fn quote_literal(value: &str) -> String {
    // E syntax makes escaping independent of standard_conforming_strings.
    format!("E'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

fn redact_database_error(error: tokio_postgres::Error) -> anyhow::Error {
    // A cast error may repeat a sampled key; do not retain an unredacted error
    // chain that a caller could later print. SQLSTATE is enough for diagnosis.
    error.as_db_error().map_or_else(
        || anyhow::anyhow!("PostgreSQL snapshot planning request failed (connection_closed={})", error.is_closed()),
        |database| anyhow::anyhow!("PostgreSQL snapshot planning request failed (SQLSTATE {})", database.code().code()),
    )
}

#[cfg(test)]
#[path = "tests/planning.rs"]
mod tests;
