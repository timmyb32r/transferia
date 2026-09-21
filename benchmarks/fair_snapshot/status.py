#!/usr/bin/env python3
"""Read-only, credential-free campaign progress for long sequential runs."""
import datetime
import json
import pathlib
import subprocess
root=pathlib.Path.home()/'benchmark-20260921'
runs=[]
for p in (root/'results/runs').glob('*.json'):
    try:r=json.loads(p.read_text())
    except json.JSONDecodeError:continue  # A result writer may be between write and close.
    if not r.get('exclude_from_comparison'):runs.append(r)
primary=[r for r in runs if r['repetition'] in (1,2,3) and r['parts_requested'] in (1,4) and r['dataset'] in ('narrow','wide')]
scale=[r for r in runs if r['repetition']>0 and r['dataset']=='narrow10m']
active=subprocess.check_output(['docker','ps','--filter','name=fair21_','--format','{{.Names}}'],text=True).splitlines()
print(json.dumps({'utc':datetime.datetime.now(datetime.timezone.utc).strftime('%H:%M:%S'),'primary_verified':sum(r['status']=='verified' for r in primary),'primary_expected':264,'primary_failed':[r['name'] for r in primary if r['status']!='verified'],'scale_verified':sum(r['status']=='verified' for r in scale),'scale_failed':[r['name'] for r in scale if r['status']!='verified'],'active':active}))
