#!/usr/bin/env python3
"""Summarize optional configuration diagnostics, separately from main rankings."""
import json
import statistics
import sys
from pathlib import Path
root=Path(sys.argv[1])
main=json.loads((root/'summary.json').read_text())
med=lambda rr,key:statistics.median(r[key] for r in rr)
out=['# Configuration diagnostics','','These repetition=0 runs are deliberately excluded from the primary rankings. They use the same server, immutable fixtures, resource budget and all-cell validation. Most cases use P4; Go homogeneous mode explicitly includes P1 and P4.','']
path=root/'jdbc-prepare-probe.json'
if path.exists():
 rows=json.loads(path.read_text())
 out+=['## pgJDBC prepareThreshold: 0 versus 5','','Randomized diagnostic block, narrow 1M fixture. The threshold changes in both source and destination URLs. This does not isolate their individual contributions.','','| Engine | Threshold | Verified n | Rows/s | Rows/CPU-s | Peak RSS GiB |','|---|---:|---:|---:|---:|---:|']
 ratios=[]
 for tool in ('datax','spark'):
  groups={}
  for value in (0,5):
   rr=[r['result'] for r in rows if r['tool']==tool and r['prepare_threshold']==value and r['result']['status']=='verified']
   groups[value]=rr
   if rr:out.append(f"| {tool} | {value} | {len(rr)} | {med(rr,'rows_per_second'):,.0f} | {med(rr,'rows_per_cpu_second'):,.0f} | {med(rr,'peak_rss_bytes')/1024**3:.2f} |")
  if all(groups.values()):
   ratios.append(f"{tool}: threshold 5 / threshold 0 throughput = **{med(groups[5],'rows_per_second')/med(groups[0],'rows_per_second'):.3f}×**.")
 out+=['',*ratios,'','Three repeats give descriptive evidence only; the roughly 4% Spark difference is not a demonstrated statistical or universal improvement.','']
path=root/'estuary-delta-probe.json'
if path.exists():
 rows=json.loads(path.read_text())
 out+=['## Estuary local preview: delta_updates','','Exploratory comparison with the standard materialization series. The delta runs were later in a separate block, so temporal conditions are not paired. This is full-row insertion into an empty PK table; changing-source reductions/update semantics are outside scope. It is not managed Flow performance.','','| Dataset | Path | Verified n | Rows/s | Rows/CPU-s | Peak RSS GiB |','|---|---|---:|---:|---:|---:|']
 ratios=[]
 for dataset in ('narrow','wide'):
  baseline=next((r for r in main if r['product']=='estuary' and r['route']=='pg-pg' and r['dataset']==dataset and r['parts_requested']==4),None)
  rr=[r['result'] for r in rows if r['dataset']==dataset and r['result']['status']=='verified']
  if baseline:out.append(f"| {dataset} | Standard keyed | {baseline['n']} | {baseline['rows_per_second_median']:,.0f} | {baseline['rows_per_cpu_second_median']:,.0f} | {baseline['peak_rss_bytes_median']/1024**3:.2f} |")
  if rr:out.append(f"| {dataset} | delta_updates=true | {len(rr)} | {med(rr,'rows_per_second'):,.0f} | {med(rr,'rows_per_cpu_second'):,.0f} | {med(rr,'peak_rss_bytes')/1024**3:.2f} |")
  if baseline and rr:ratios.append(f"{dataset}: delta / standard throughput = **{med(rr,'rows_per_second')/baseline['rows_per_second_median']:.3f}×**.")
 out+=['',*ratios,'']
path=root/'datax-poll-probe.json'
if path.exists():
 rows=json.loads(path.read_text())
 out+=['## DataX scheduler completion polling','','The upstream default checks job completion every 10000 ms. This diagnostic sets only core.container.job.sleepInterval=100. The data path, batch settings, P4 and JVM remain unchanged. Main-series defaults are retained; this later diagnostic block is not temporally paired.','','| Route | Dataset | Poll ms | Verified n | Rows/s | CPU seconds | Wall seconds |','|---|---|---:|---:|---:|---:|---:|']
 for route in ('pg-pg','pg-ch'):
  for dataset in ('narrow','wide'):
   baseline=next((r for r in main if r['product']=='datax' and r['route']==route and r['dataset']==dataset and r['parts_requested']==4),None)
   rr=[r['result'] for r in rows if r['route']==route and r['dataset']==dataset and r['result']['status']=='verified']
   if baseline:out.append(f"| {route} | {dataset} | 10000 | {baseline['n']} | {baseline['rows_per_second_median']:,.0f} | {baseline['cpu_seconds_median']:.2f} | {baseline['elapsed_seconds_median']:.2f} |")
   if rr:out.append(f"| {route} | {dataset} | 100 | {len(rr)} | {med(rr,'rows_per_second'):,.0f} | {med(rr,'cpu_seconds'):.2f} | {med(rr,'elapsed_seconds'):.2f} |")
 out+=['','A shorter final polling delay improves finite-job latency, not the underlying steady-state row-transfer rate. No fixed ten seconds are subtracted from the original observations.','']
path=root/'go-homo-probe.json'
if path.exists():
 rows=json.loads(path.read_text())
 out+=['## Transferia Go: PostgreSQL homogeneous mode','','The primary series deliberately uses NoHomo=true for the generic typed path. This diagnostic sets NoHomo=false on PG→PG; the native provider selects its homogeneous representation. Binary source result format, sharding, rename, destination constraints and validation stay the same. Later separate block, not temporally paired.','','| Dataset | Parts | NoHomo | Verified n | Rows/s | Rows/CPU-s | Peak RSS GiB |','|---|---:|---|---:|---:|---:|---:|']
 for dataset in ('narrow','wide'):
  for parts in (1,4):
   baseline=next((r for r in main if r['product']=='go' and r['route']=='pg-pg' and r['dataset']==dataset and r['parts_requested']==parts),None)
   rr=[r['result'] for r in rows if r['dataset']==dataset and r['parts']==parts and r['result']['status']=='verified']
   if baseline:out.append(f"| {dataset} | {parts} | true: main typed path | {baseline['n']} | {baseline['rows_per_second_median']:,.0f} | {baseline['rows_per_cpu_second_median']:,.0f} | {baseline['peak_rss_bytes_median']/1024**3:.2f} |")
   if rr:out.append(f"| {dataset} | {parts} | false: homogeneous | {len(rr)} | {med(rr,'rows_per_second'):,.0f} | {med(rr,'rows_per_cpu_second'):,.0f} | {med(rr,'peak_rss_bytes')/1024**3:.2f} |")
 out+=['']
path=root/'jdbc-batch-probe.json'
if path.exists():
 rows=json.loads(path.read_text())
 poll_controls=json.loads((root/'datax-poll-probe.json').read_text())
 out+=['## JDBC writer batch: 1000 versus 8192 rows','','PG→PG, P4. DataX controls come from its 100-ms completion-poll diagnostic; Spark controls come from the main series. Same driver, JVM and source settings. Later blocks are not temporally paired.','','| Tool | Dataset | Writer rows | Verified n | Rows/s | Rows/CPU-s | Peak RSS GiB |','|---|---|---:|---:|---:|---:|---:|']
 for tool in ('datax','spark'):
  for dataset in ('narrow','wide'):
   if tool=='datax':
    rr=[r['result'] for r in poll_controls if r['route']=='pg-pg' and r['dataset']==dataset and r['result']['status']=='verified']
    if rr:out.append(f"| {tool} | {dataset} | 1000 | {len(rr)} | {med(rr,'rows_per_second'):,.0f} | {med(rr,'rows_per_cpu_second'):,.0f} | {med(rr,'peak_rss_bytes')/1024**3:.2f} |")
   else:
    b=next((r for r in main if r['product']==tool and r['route']=='pg-pg' and r['dataset']==dataset and r['parts_requested']==4),None)
    if b:out.append(f"| {tool} | {dataset} | 1000 | {b['n']} | {b['rows_per_second_median']:,.0f} | {b['rows_per_cpu_second_median']:,.0f} | {b['peak_rss_bytes_median']/1024**3:.2f} |")
   rr=[r['result'] for r in rows if r['tool']==tool and r['dataset']==dataset and r['result']['status']=='verified']
   if rr:out.append(f"| {tool} | {dataset} | 8192 | {len(rr)} | {med(rr,'rows_per_second'):,.0f} | {med(rr,'rows_per_cpu_second'):,.0f} | {med(rr,'peak_rss_bytes')/1024**3:.2f} |")
 out+=['','This diagnostic is not an exhaustive search for an optimal batch size. PostgreSQL commit cadence still differs between DataX and Spark.','']
path=root/'flink-backfill-probe.json'
if path.exists():
 rows=json.loads(path.read_text())
 baseline=next((r for r in main if r['product']=='flink' and r['route']=='pg-pg' and r['dataset']=='narrow10m' and r['parts_requested']==4),None)
 out+=['## Flink CDC: immutable-source backfill skip','','Single 10M/P4 diagnostic, not a repeated ranking. Both paths use TaskManager 18 GiB / managed fraction 0.1 within the same 24-GiB cgroup. Only scan.incremental.snapshot.backfill.skip changes. Skipping backfill changes guarantees on a mutating source; this is not a production default recommendation.','','| Path | Verified n | Rows/s | Rows/CPU-s | Peak RSS GiB |','|---|---:|---:|---:|---:|']
 if baseline:out.append(f"| Buffered backfill, primary scale | {baseline['n']} | {baseline['rows_per_second_median']:,.0f} | {baseline['rows_per_cpu_second_median']:,.0f} | {baseline['peak_rss_bytes_median']/1024**3:.2f} |")
 rr=[x['result'] for x in rows if x['result']['status']=='verified']
 if rr:out.append(f"| Skip backfill, diagnostic | {len(rr)} | {med(rr,'rows_per_second'):,.0f} | {med(rr,'rows_per_cpu_second'):,.0f} | {med(rr,'peak_rss_bytes')/1024**3:.2f} |")
 out+=['','The original 8-GiB TaskManager attempt failed with Java heap OOM and is retained separately; it is not assigned a throughput score.','']
for filename in ('jdbc-prepare-probe.json','estuary-delta-probe.json','datax-poll-probe.json','go-homo-probe.json','jdbc-batch-probe.json','flink-backfill-probe.json'):
 path=root/filename
 if path.exists():
  for item in json.loads(path.read_text()):
   r=item['result']
   if r['status']!='verified':out.append(f"Failed diagnostic: `{r['name']}` — {r.get('error','unknown')}")
(root/'DIAGNOSTICS.md').write_text('\n'.join(out)+'\n')
print('Wrote configuration diagnostic tables.')
