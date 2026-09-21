#!/usr/bin/env python3
"""Apply the explicitly benchmark-only range override to an isolated source copy.

Never run against the working repository. This preserves the production planner's
metadata/discovery cost and runtime, but replaces its chosen policy with Indexed
at the requested count and the synthetic fixture's exact quarter boundaries.
The original production binary must be retained as transferia-auto first.
"""
import pathlib,sys
root=pathlib.Path(sys.argv[1]).resolve()
if root.name!='source' or root.parent.name!='benchmark-20260921':
    raise SystemExit('only the isolated benchmark source copy is permitted')
p=root/'crates/transferia-connector-postgres/src/connectors/postgres/src_batch/planning.rs'
s=p.read_text()
old='''    prepare_relation(client, &discovered.config.schema, &discovered.config.name, discovered.relation_oid, max_parts, None).await'''
new='''    if std::env::var_os("FAIR21_EXACT_RANGES").is_some() {
        anyhow::ensure!(discovered.config.schema == "fair21", "benchmark override only supports fair21 fixtures");
        let rows: u64 = match discovered.config.name.as_str() {
            "smoke" => 10_000, "narrow" | "wide" => 1_000_000, "narrow10m" => 10_000_000,
            _ => anyhow::bail!("unknown benchmark fixture"),
        };
        let parts = max_parts.context("benchmark exact part count is required")?;
        anyhow::ensure!([1, 4].contains(&parts.get()), "benchmark supports only one or four parts");
        let strategy = if parts.get() == 1 { Strategy::Single } else { Strategy::Indexed };
        let mut prepared = prepare_relation_for_evaluation(client, &discovered.config.schema,
            &discovered.config.name, discovered.relation_oid, max_parts, strategy, parts).await?;
        if parts.get() > 1 {
            let cuts = prepared.typed_cuts.as_mut().context("benchmark requires indexed cuts")?;
            anyhow::ensure!(cuts.column == "id", "benchmark requires id scan key");
            for (ordinal, chunk) in prepared.chunks.iter().enumerate() {
                if let ChunkKind::Indexed { upper_cut: Some(index), .. } = chunk.kind() {
                    cuts.values[*index as usize] = (1 + (ordinal as u64 + 1) * rows / u64::from(parts.get())).to_string();
                }
            }
        }
        tracing::warn!(rows, parts = parts.get(), "BENCHMARK ONLY: exact synthetic primary-key quarter boundaries override Auto");
        return Ok(prepared);
    }
    prepare_relation(client, &discovered.config.schema, &discovered.config.name, discovered.relation_oid, max_parts, None).await'''
if s.count(old)!=1: raise SystemExit('expected unique unpatched production entrypoint')
p.write_text(s.replace(old,new))
