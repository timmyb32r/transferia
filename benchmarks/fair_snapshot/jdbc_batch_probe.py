#!/usr/bin/env python3
"""Exploratory PG→PG JDBC writer batch 8192, P4, compared with batch 1000.

DataX uses 100-ms completion polling, matching datax_poll_probe controls. Spark
compares with its primary batch-1000 series. Other driver/JVM settings stay fixed.
These later diagnostic blocks are not temporally paired with the controls.
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
    if product=='datax':
        cfg['job']['content'][0]['writer']['parameter']['batchSize']=8192
        cfg['core']={'container':{'job':{'sleepInterval':100}}}
    else:
        cfg['batch_size']=8192
    path.write_text(json.dumps(cfg));path.chmod(0o600)
    (ROOT/'results/configs'/path.name).write_text(redact(json.dumps(cfg)))
    return commands,variant+'-diagnostic-jdbc-batch8192'

runner.configure=configure
jobs=[(tool,dataset,rep) for tool in ('datax','spark') for dataset in ('narrow','wide') for rep in (1,2,3)]
random.Random(2026092109).shuffle(jobs)
results=[]
with (ROOT/'measurement.lock').open('w') as lock:
    fcntl.flock(lock,fcntl.LOCK_EX)
    for tool,dataset,rep in jobs:
        result=_run(tool,dataset,4,0,180,'pg-pg')
        results.append({'tool':tool,'dataset':dataset,'diagnostic_repetition':rep,'writer_batch_rows':8192,'completion_poll_ms':100 if tool=='datax' else None,'result':result})
        (ROOT/'results/jdbc-batch-probe.json').write_text(json.dumps(results,indent=2)+'\n')
        if result['status']!='verified':raise RuntimeError('batch diagnostic failed; inspect before continuing')
print('JDBC batch diagnostic completed; excluded from main ranking.')
