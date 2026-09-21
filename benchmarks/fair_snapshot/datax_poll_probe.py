#!/usr/bin/env python3
"""DataX finite-job completion polling diagnostic; records remain in DataX/JVM.

Only core.container.job.sleepInterval changes to 100 ms. Standard main runs
retain the upstream 10000-ms default. Diagnostics never enter the main ranking.
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
    cfg['core']={'container':{'job':{'sleepInterval':100}}}
    path.write_text(json.dumps(cfg));path.chmod(0o600)
    (ROOT/'results/configs'/path.name).write_text(redact(json.dumps(cfg)))
    return commands,variant+'-diagnostic-completion-poll100ms'

runner.configure=configure
jobs=[(route,dataset,rep) for route in ('pg-pg','pg-ch') for dataset in ('narrow','wide') for rep in (1,2,3)]
random.Random(2026092107).shuffle(jobs)
results=[]
with (ROOT/'measurement.lock').open('w') as lock:
    fcntl.flock(lock,fcntl.LOCK_EX)
    for route,dataset,rep in jobs:
        result=_run('datax',dataset,4,0,180,route)
        results.append({'route':route,'dataset':dataset,'diagnostic_repetition':rep,'completion_poll_ms':100,'result':result})
        (ROOT/'results/datax-poll-probe.json').write_text(json.dumps(results,indent=2)+'\n')
        if result['status']!='verified':raise RuntimeError('poll diagnostic failed; inspect before continuing')
print('DataX completion polling diagnostic completed; excluded from main ranking.')
