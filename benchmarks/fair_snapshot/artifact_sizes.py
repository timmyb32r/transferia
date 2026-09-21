#!/usr/bin/env python3
"""Post-run intermediate file sizes only; never read private file contents."""
import json
from setup import ROOT
path=ROOT/'results/intermediate-file-sizes.json'
out={r['name']:r for r in json.loads(path.read_text())} if path.exists() else {}
for p in sorted((ROOT/'results/runs').glob('*.json')):
 r=json.loads(p.read_text())
 if r['repetition']<=0 or r['status']!='verified' or r.get('exclude_from_comparison'):continue
 name=r['name'];files=[];kind=None
 if r['product'] in ('debezium','debezium_bulk'):
  files=list((ROOT/'private'/name/'kafka-data').rglob('*.log'));kind='Kafka record log files, including metadata log'
 elif r['product']=='estuary':
  files=list((ROOT/'private').glob(name+'_e*.fixture'));kind='Flow local-preview JSONL staging fixtures'
 elif r['product']=='sqoop':
  files=list((ROOT/'private'/(name+'_staging')).glob('part-m-*'));kind='Sqoop local import output'
 if files:
  out[name]={'name':name,'product':r['product'],'dataset':r['dataset'],'parts':r['parts_requested'],'kind':kind,'files':len(files),'logical_file_bytes':sum(f.stat().st_size for f in files),'allocated_file_bytes':sum(f.stat().st_blocks*512 for f in files)}
path.write_text(json.dumps(list(out.values()),indent=2)+'\n')
print('Intermediate size records:',len(out))
