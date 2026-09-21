#!/usr/bin/env python3
"""Extract bounded, credential-screened partition evidence from engine logs."""
import json,re
from setup import ROOT,CONFIG
from runner import redact
PATTERNS={
'rust':['Snapshot plan selected','BENCHMARK ONLY'],
'rust_ranges':['Snapshot plan selected','BENCHMARK ONLY'],
'go':['extracted bounds:','sharded by sequence','After sharding, tables'],
'airbyte':['concurrent partition reader','Effective concurrency','Sampled','Benchmark platform accounting: total'],
'debezium':['chunk','snapshot thread','Exporting data','Snapshotting finished'],
'flink':['Split table','received all splits finished','scan.incremental.snapshot.chunk.size'],
'inlong':['scan.partition'],
'seatunnel':['Split table','fixed chunk splitter','partition_num'],
'spark':['partitions','Starting task'],
'datax':['start [','channels for','byte_speed_limit','record_speed_limit'],
'meltano':['custom_where_clauses'],
'sling':['sql','rows copied'],
'sqoop':['number of splits','map tasks','Retrieved'],
'estuary':['local preview'],
}
PATTERNS['debezium_bulk']=PATTERNS['debezium']
result=[]
for p in sorted((ROOT/'results/runs').glob('*.json')):
 r=json.loads(p.read_text())
 if r['repetition']<=0:continue
 log=ROOT/'results/logs'/(r['name']+'.log')
 if not log.exists():continue
 lines=[]
 for raw in log.read_text().splitlines():
  line=re.sub(r'\x1b\[[0-9;]*m','',raw)
  if any(x.lower() in line.lower() for x in PATTERNS[r['product']]):
   line=redact(line)
   if len(line)>2400:line=line[:2400]+' [truncated evidence line]'
   lines.append(line)
  if len(lines)>=16:break
 result.append({'name':r['name'],'product':r['product'],'parts_requested':r['parts_requested'],'status':r['status'],'excluded':r.get('exclude_from_comparison'),'evidence':lines})
text=json.dumps(result,indent=2)
if CONFIG['password'] in text:raise RuntimeError('credential screen failed')
(ROOT/'results/partition-evidence.json').write_text(text+'\n')
print('Partition evidence:',len(result),'runs')
