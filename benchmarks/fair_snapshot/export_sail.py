#!/usr/bin/env python3
"""Export only Sail follow-up evidence; preserve the historical campaign files."""
import fcntl
import json
import re
import urllib.parse
from setup import ROOT,CONFIG
from runner import redact

with (ROOT/'measurement.lock').open('w') as lock:
    fcntl.flock(lock,fcntl.LOCK_EX)
    results=ROOT/'results'
    entries=json.loads((results/'sail-followup.json').read_text())
    if len(entries)!=80 or any(e['result']['status']!='verified' for e in entries if e['dataset']!='narrow10m'):
        raise RuntimeError('Sail final export requires 80 recorded outcomes and all primary outcomes verified')
    names={e['result']['name'] for e in entries}
    for file in ('sail-smoke.json','sail-first-attempt.json','sail-batch-ingest-attempt.json'):
        names.update(e['result']['name'] for e in json.loads((results/file).read_text()))
    names.update(e['name'] for e in json.loads((results/'sail-native-probe.json').read_text()))
    configs={name:json.loads((results/'configs'/(name+'.json')).read_text()) for name in sorted(names)}
    (results/'sail-configurations.json').write_text(json.dumps(configs,indent=2)+'\n')
    evidence={}
    for name in sorted(names):
        log=(results/'logs'/(name+'.log')).read_text()
        lines=[line for line in log.splitlines() if re.search(r'SAIL_(?:ADBC|SINK|TLS)|writer is not implemented',line)]
        activity=results/'activity'/(name+'.json')
        samples=json.loads(activity.read_text()) if activity.exists() else []
        evidence[name]={'log_evidence':lines,'source_query_samples':len(samples),
                       'max_observed_active_queries':max((len(s['queries']) for s in samples),default=0),
                       'queries':sorted({q['query'] for s in samples for q in s['queries']})}
    (results/'sail-partition-evidence.json').write_text(redact(json.dumps(evidence,indent=2))+'\n')
    negative=(results/'sail-tls-negative.log').read_text()
    if 'certificate verify failed' not in negative:
        raise RuntimeError('Negative hostname verification evidence missing')
    (results/'sail-tls-evidence.json').write_text(json.dumps({'wrong_hostname_rejected':True,'expected_hostname':'invalid-hostname.benchmark.invalid','errors':[line for line in negative.splitlines() if 'certificate verify failed' in line]},indent=2)+'\n')
    audit=json.loads((results/'destination-schema-audit.json').read_text())
    if audit['issues']:raise RuntimeError('Destination schema audit failed')
    selected={'pg':{},'ch':{},'issues':[]}
    for entry in entries:
        if entry['result']['status']!='verified':continue
        side='pg' if entry['route']=='pg-pg' else 'ch'
        name=entry['result']['name']
        selected[side][name]=audit[side][name]
    (results/'sail-schema-audit.json').write_text(json.dumps(selected,indent=2)+'\n')
    files=['sail-followup.json','sail-smoke.json','sail-first-attempt.json','sail-batch-ingest-attempt.json','sail-native-probe.json','sail-environment.json','sail-affinity.json','sail-configurations.json','sail-partition-evidence.json','sail-tls-evidence.json','sail-schema-audit.json','sail-stall-evidence.json']
    for filename in files:
        text=(results/filename).read_text()
        if any(secret in text for secret in (CONFIG['password'],urllib.parse.quote(CONFIG['password'],safe=''))):
            raise RuntimeError('Credential screen failed: '+filename)
    print('Sail exports verified and credential-screened:',len(files),'files')
