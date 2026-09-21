#!/usr/bin/env python3
"""Audit exported Sail follow-up evidence without credentials or database access."""
import datetime
import json
import re
import sys
from collections import Counter
from pathlib import Path

root=Path(sys.argv[1])
def read(name):return json.loads((root/name).read_text())
entries=read('sail-followup.json')
assert len(entries)==80
identities=[(e['product'],e['route'],e['dataset'],e['parts'],e['followup_repetition']) for e in entries]
assert len(set(identities))==80
assert all(e['result']['status']=='verified' for e in entries if e['dataset']!='narrow10m')
evidence=read('sail-partition-evidence.json')
schemas=read('sail-schema-audit.json')
assert not schemas['issues']
verified=[e for e in entries if e['result']['status']=='verified']
for e in verified:
    r=e['result'];n=10000000 if e['dataset']=='narrow10m' else 1000000
    assert r['verified_rows']==n
    assert r['cpu_affinity']=='0-15' and r['memory_limit_bytes']==24*1024**3
    assert r['cpu_seconds']>0 and r['elapsed_seconds']>0 and r['peak_rss_bytes']>0
    assert abs(r['rows_per_second']-n/r['elapsed_seconds'])<1e-6
    assert abs(r['rows_per_cpu_second']-n/r['cpu_seconds'])<1e-6
    assert r['name'] in schemas['pg' if e['route']=='pg-pg' else 'ch']
    if e['product'].startswith('sail_'):
        log='\n'.join(evidence[r['name']]['log_evidence'])
        commits=re.findall(r'SAIL_SINK_PARTITION_COMMITTED rows=(\d+)',log)
        assert len(commits)==e['parts'] and sum(map(int,commits))==n
        assert f'SAIL_SINK_ALL_COMMITTED rows={n} partitions={e["parts"]}' in log
        assert 'SAIL_TLS_PROXY verifyChain=yes checkHost=' in log
        if e['product']=='sail_adbc':
            buffers=re.findall(r'SAIL_ADBC_BUFFERED rows=(\d+) bytes=\d+ batches=(\d+)',log)
            assert len(buffers)==e['parts'] and all(int(rows)==n//e['parts'] and batches=='1' for rows,batches in buffers)
for product in ('sail_jdbc','sail_adbc'):
    assert any(e['product']==product and e['parts']==4 and evidence[e['result']['name']]['max_observed_active_queries']==4 for e in verified)
ordered=sorted((e['result'] for e in entries),key=lambda r:r['started_utc'])
for a,b in zip(ordered,ordered[1:]):
    assert datetime.datetime.fromisoformat(a['started_utc'])+datetime.timedelta(seconds=a['elapsed_seconds'])<=datetime.datetime.fromisoformat(b['started_utc'])
affinities=read('sail-affinity.json')
assert len(affinities)==5 and all(a['before']==a['after'] for a in affinities)
assert read('sail-tls-evidence.json')['wrong_hostname_rejected']
result={'recorded':80,'verified':len(verified),'failed':[{'name':e['result']['name'],'error':e['result'].get('error')} for e in entries if e['result']['status']!='verified'],
        'primary_verified':72,'sail_verified':sum(e['product'].startswith('sail_') for e in verified),'fresh_controls_verified':sum(not e['product'].startswith('sail_') for e in verified),
        'verified_by_product':dict(Counter(e['product'] for e in verified)),
        'schema_tables':{'pg':len(schemas['pg']),'ch':len(schemas['ch'])},
        'unique_cases':True,'row_checks':True,'resource_math':True,'cpu_and_memory_budget':True,'no_measurement_overlap':True,
        'sail_committed_part_counts':True,'adbc_single_batch_per_part':True,'tls_wrong_hostname_rejected':True,'background_affinities_restored':True,
        'p4_observed_source_concurrency':{e['result']['name']:evidence[e['result']['name']]['max_observed_active_queries'] for e in verified if e['parts']==4 and e['product'].startswith('sail_')},
        'limitations':['CPU excludes database hosts.','100-ms RSS and 500-ms source-query sampling can miss short peaks.','10M results are single runs.','Verification covers immutable fixture values, not failure recovery.']}
(root/'sail-final-audit.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({k:v for k,v in result.items() if k!='p4_observed_source_concurrency'},indent=2))
