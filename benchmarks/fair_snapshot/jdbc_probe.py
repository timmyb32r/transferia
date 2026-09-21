#!/usr/bin/env python3
"""Paired diagnostic: pgJDBC prepareThreshold=0 versus 5, all else fixed.

Runs use repetition=0 and never enter the main ranking. Both source/destination
URLs use the selected threshold. This is not an isolated source/sink attribution.
The outer measurement lock keeps this diagnostic block sequential with campaign.
"""
import fcntl
import json
import random
from runner import ROOT, _run
import runner

original_jdbc=runner.jdbc
original_configure=runner.configure
threshold=0

def jdbc(side):
    return original_jdbc(side).replace('prepareThreshold=0', 'prepareThreshold='+str(threshold))

def configure(*args, **kwargs):
    commands, variant=original_configure(*args, **kwargs)
    return commands, variant+'-diagnostic-prepareThreshold'+str(threshold)

runner.jdbc=jdbc
runner.configure=configure
jobs=[(tool,rep,value) for tool in ('datax','spark') for rep in (1,2,3) for value in (0,5)]
random.Random(2026092105).shuffle(jobs)
results=[]
with (ROOT/'measurement.lock').open('w') as lock:
    fcntl.flock(lock, fcntl.LOCK_EX)
    for tool,rep,value in jobs:
        threshold=value
        result=_run(tool,'narrow',4,0,180,'pg-pg')
        results.append({'tool':tool,'paired_repetition':rep,'prepare_threshold':value,'result':result})
        (ROOT/'results/jdbc-prepare-probe.json').write_text(json.dumps(results,indent=2)+'\n')
        if result['status']!='verified':
            raise RuntimeError('diagnostic failed; inspect before continuing')
print('JDBC threshold diagnostic completed; excluded from main ranking.')
