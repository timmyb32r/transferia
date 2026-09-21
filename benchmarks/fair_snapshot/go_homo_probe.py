#!/usr/bin/env python3
"""Compare Go's homogeneous PG→PG path with the main typed-path configuration.

Only NoHomo changes to false; finite snapshot, binary result format, exact
fixture, native part count and common destination constraints remain unchanged.
Diagnostics use repetition zero and are not merged into the main rankings.
"""
import fcntl
import json
import random
import runner
from runner import ROOT, PRIVATE, _run, redact
original_configure=runner.configure

def configure(product,dataset,parts,name,route):
    commands,variant=original_configure(product,dataset,parts,name,route)
    path=PRIVATE/(name+'.json');cfg=json.loads(path.read_text())
    src=json.loads(cfg['src']['params']);src['NoHomo']=False
    cfg['src']['params']=json.dumps(src)
    path.write_text(json.dumps(cfg));path.chmod(0o600)
    (ROOT/'results/configs'/path.name).write_text(redact(json.dumps(cfg)))
    return commands,variant+'-diagnostic-homogeneous'

runner.configure=configure
jobs=[(dataset,parts,rep) for dataset in ('narrow','wide') for parts in (1,4) for rep in (1,2,3)]
random.Random(2026092108).shuffle(jobs)
results=[]
with (ROOT/'measurement.lock').open('w') as lock:
    fcntl.flock(lock,fcntl.LOCK_EX)
    for dataset,parts,rep in jobs:
        result=_run('go',dataset,parts,0,180,'pg-pg')
        results.append({'dataset':dataset,'parts':parts,'diagnostic_repetition':rep,'no_homo':False,'result':result})
        (ROOT/'results/go-homo-probe.json').write_text(json.dumps(results,indent=2)+'\n')
        if result['status']!='verified':raise RuntimeError('homogeneous diagnostic failed; inspect before continuing')
print('Go homogeneous diagnostic completed; excluded from main ranking.')
