#!/usr/bin/env python3
"""Derive numeric comparisons; this intentionally makes no causal claims."""
import json
import sys
from pathlib import Path
root=Path(sys.argv[1])
rows=json.loads((root/'summary.json').read_text())
idx={(r['route'],r['dataset'],r['product'],r['parts_requested']):r for r in rows}
facts={'rankings':[], 'parallel_speedups':[], 'rust_auto_vs_exact':[], 'debezium_bulk_vs_baseline':[], 'scale_changes':[]}
for route in ('pg-pg','pg-ch'):
 for dataset in ('narrow','wide','narrow10m'):
  rr=sorted((r for r in rows if r['route']==route and r['dataset']==dataset and r['parts_requested']==4 and r['product'] not in ('rust_ranges','debezium_bulk')),key=lambda r:r['rows_per_second_median'],reverse=True)
  if rr:
   facts['rankings'].append({'route':route,'dataset':dataset,'parts':4,'products':[r['product'] for r in rr], 'top_rows_per_second':rr[0]['rows_per_second_median'],'top_over_second':rr[0]['rows_per_second_median']/rr[1]['rows_per_second_median'] if len(rr)>1 else None})
  for tool in {r['product'] for r in rr}|{'rust_ranges','debezium_bulk'}:
   one=idx.get((route,dataset,tool,1));four=idx.get((route,dataset,tool,4))
   if one and four:
    facts['parallel_speedups'].append({'route':route,'dataset':dataset,'product':tool,'p4_over_p1':four['rows_per_second_median']/one['rows_per_second_median'],'repeats':[one['n'],four['n']]})
  auto=idx.get((route,dataset,'rust',4));exact=idx.get((route,dataset,'rust_ranges',4))
  if auto and exact:facts['rust_auto_vs_exact'].append({'route':route,'dataset':dataset,'auto_over_exact':auto['rows_per_second_median']/exact['rows_per_second_median']})
 for tool in {r['product'] for r in rows if r['route']==route}:
  small=idx.get((route,'narrow',tool,4));large=idx.get((route,'narrow10m',tool,4))
  if small and large:facts['scale_changes'].append({'route':route,'product':tool,'throughput_10m_over_1m':large['rows_per_second_median']/small['rows_per_second_median'],'repeats':[small['n'],large['n']]})
for dataset in ('narrow','wide','narrow10m'):
 for p in (1,4,16):
  base=idx.get(('pg-pg',dataset,'debezium',p));bulk=idx.get(('pg-pg',dataset,'debezium_bulk',p))
  if base and bulk:facts['debezium_bulk_vs_baseline'].append({'dataset':dataset,'parts':p,'bulk_over_baseline':bulk['rows_per_second_median']/base['rows_per_second_median'],'cpu_efficiency_ratio':bulk['rows_per_cpu_second_median']/base['rows_per_cpu_second_median'],'repeats':[base['n'],bulk['n']]})
(root/'findings.json').write_text(json.dumps(facts,indent=2)+'\n')
print('Wrote numeric findings; interpret against coverage and protocol.')
