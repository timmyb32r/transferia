#!/usr/bin/env python3
"""Create auditable Markdown tables, leaving interpretation to the report author."""
import json,statistics,sys
from pathlib import Path
root=Path(sys.argv[1]);rows=json.loads((root/'summary.json').read_text());idx={(r['route'],r['dataset'],r['product'],r['parts_requested']):r for r in rows}
labels={'sail_jdbc':'Sail JDBC + sink⁷','sail_adbc':'Sail ADBC buffered + sink⁷','rust':'Transferia Rust Auto','rust_ranges':'Transferia Rust exact PK¹','go':'Transferia Go · typed path','seatunnel':'SeaTunnel','sling':'Sling²','datax':'DataX · default polling','airbyte':'Airbyte connectors³','flink':'Flink CDC','debezium':'Debezium + Kafka + JDBC','debezium_bulk':'Debezium bulk-tuned⁴','inlong':'InLong Sort','meltano':'Meltano²','spark':'Spark','sqoop':'Sqoop import + export','estuary':'Estuary local preview² ³'}
def num(r,key,scale=1,precision=0):
 return '—' if not r else f'{r[key]/scale:,.{precision}f}'.replace(',',' ')
out=['# Detailed benchmark tables','', 'Medians of verified runs. `n` = accepted repeats P1/P4. CPU efficiency counts the whole client cgroup; managed database hosts are outside this scope.','']
for route in ('pg-pg','pg-ch'):
 for dataset in ('narrow','wide','narrow10m'):
  tools={r['product'] for r in rows if r['route']==route and r['dataset']==dataset}
  if not tools:continue
  tools=sorted(tools,key=lambda t:idx.get((route,dataset,t,4),idx.get((route,dataset,t,1)))['rows_per_second_median'],reverse=True)
  out += [f'## {route.upper()} · {dataset}','','| Engine | P1 rows/s | P4 rows/s | P4/P1 | P4 rows/CPU-s | P4 mean CPUs | P4 peak RSS GiB | n |','|---|---:|---:|---:|---:|---:|---:|---:|']
  for t in tools:
   a=idx.get((route,dataset,t,1));b=idx.get((route,dataset,t,4))
   speed=f"{b['rows_per_second_median']/a['rows_per_second_median']:.2f}×" if a and b else '—'
   out.append(f"| {labels[t]+(' · large heap18GiB⁵' if t=='flink' and dataset=='narrow10m' else '')} | {num(a,'rows_per_second_median')} | {num(b,'rows_per_second_median')} | {speed} | {num(b,'rows_per_cpu_second_median')} | {num(b,'cpu_percent_median',100,2)} | {num(b,'peak_rss_bytes_median',1024**3,2)} | {a['n'] if a else 0}/{b['n'] if b else 0} |")
  out+=['']
extra=[r for r in rows if r['parts_requested']==16]
if extra:
 out+=['## Debezium · 16 parts · narrow PG→PG','','Separate scaling check; not ranked against competitors using four parts.','','| Variant | Rows/s | Rows/CPU-s | Mean CPUs | Peak RSS GiB | n |','|---|---:|---:|---:|---:|---:|']
 for r in extra:
  out.append(f"| {labels[r['product']]} | {num(r,'rows_per_second_median')} | {num(r,'rows_per_cpu_second_median')} | {num(r,'cpu_percent_median',100,2)} | {num(r,'peak_rss_bytes_median',1024**3,2)} | {r['n']} |")
 out+=['']
out+=['¹ Benchmark-only exact-PK override. ² External parallel pipelines. ³ Local connector/preview path, not the full managed platform. ⁴ Separate Kafka buffering/compression configuration. ⁵ Flink 10M uses TaskManager 18 GiB / managed fraction 0.1 instead of 8 GiB; total cgroup remains 24 GiB.','','⁷ Sail uses the explicit benchmark Arrow sink and common TLS proxy. It is a later comparison block; fresh interleaved Rust/Spark controls are in SAIL.md. See `summary.csv` for all medians/min/max, including P1 memory, CPU time, cgroup peak memory and wall time. Min/max are descriptive, not confidence intervals.','']
runs=[json.loads(s) for s in (root/'runs.jsonl').read_text().splitlines() if s.strip()]
if (root/'sail-followup.json').exists():
 for entry in json.loads((root/'sail-followup.json').read_text()):
  if entry['product'] in ('sail_jdbc','sail_adbc'):
   runs.append(dict(entry['result'],repetition=entry['followup_repetition']))
failed=[r for r in runs if r['repetition']>0 and r['status']!='verified']
out+=['## Failed measured runs','','| Run | Outcome | Elapsed s | Comparison |','|---|---|---:|---|']
for r in failed:out.append(f"| {r['name']} | {r.get('error','unknown').replace('|','/')} | {r.get('elapsed_seconds',0):.2f} | {'Superseded; reason below' if r.get('exclude_from_comparison') else 'Current failed case'} |")
if not failed:out.append('| — | None among currently exported measured runs | — | — |')
out+=['','## Superseded measured runs','','| Run | Reason |','|---|---|']
for r in runs:
 if r['repetition']>0 and r.get('exclude_from_comparison'):out.append(f"| {r['name']} | {str(r['exclude_from_comparison']).replace('|','/')} |")
(root/'TABLES.md').write_text('\n'.join(out)+'\n')
print('Wrote TABLES.md')
