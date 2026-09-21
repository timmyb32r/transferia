#!/usr/bin/env python3
"""Diagnostic local-preview delta_updates on an immutable full-row snapshot.

Uses fresh empty PK tables and retains all-cell validation. This is deliberately
separate from the standard keyed materialization series and from managed Flow.
No claim about update/reduction semantics on changing data follows from this test.
"""
import fcntl
import json
import random
import runner
from runner import ROOT, PRIVATE, _run, redact
original_configure=runner.configure

def enable_delta(spec):
    for materialization in spec['materializations'].values():
        for binding in materialization['bindings']:
            binding['resource']['delta_updates']=True

def configure(product,dataset,parts,name,route):
    commands,variant=original_configure(product,dataset,parts,name,route)
    for path in PRIVATE.glob(name+'_e*.flow.json'):
        spec=json.loads(path.read_text());enable_delta(spec)
        path.write_text(json.dumps(spec));path.chmod(0o600)
    path=PRIVATE/(name+'.json')
    specs=json.loads(path.read_text())
    for spec in specs:enable_delta(spec)
    path.write_text(json.dumps(specs));path.chmod(0o600)
    (ROOT/'results/configs'/path.name).write_text(redact(json.dumps(specs)))
    return commands,variant+'-diagnostic-delta-updates'

runner.configure=configure
jobs=[(dataset,rep) for dataset in ('narrow','wide') for rep in (1,2,3)]
random.Random(2026092106).shuffle(jobs)
results=[]
with (ROOT/'measurement.lock').open('w') as lock:
    fcntl.flock(lock,fcntl.LOCK_EX)
    for dataset,rep in jobs:
        result=_run('estuary',dataset,4,0,180,'pg-pg')
        results.append({'dataset':dataset,'diagnostic_repetition':rep,'delta_updates':True,'result':result})
        (ROOT/'results/estuary-delta-probe.json').write_text(json.dumps(results,indent=2)+'\n')
        if result['status']!='verified':raise RuntimeError('delta diagnostic failed; inspect before continuing')
print('Estuary delta diagnostic completed; excluded from main ranking.')
