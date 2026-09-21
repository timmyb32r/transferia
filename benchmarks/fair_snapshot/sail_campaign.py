#!/usr/bin/env python3
"""Sail JDBC/ADBC comparison; one isolated cgroup and identical Arrow sink.

Smoke failures stop the campaign. Successful primary runs are three shuffled
repeats; exploratory 10M P4 runs are single measurements. Rust/Spark controls
are interleaved after the cluster role restoration. All are repetition zero
in the historical ledger and independently indexed by this manifest.
"""
import argparse
import fcntl
import json
import random
import subprocess
import urllib.parse
import runner
from runner import ROOT, PRIVATE, CONFIG, _run, redact

original_configure=runner.configure
for tool in ('sail_jdbc','sail_adbc'):
    runner.IMAGES[tool]='transferia/benchmark-sail:0.7.1'

def configure(product,dataset,parts,name,route):
    if product not in ('sail_jdbc','sail_adbc'):
        return original_configure(product,dataset,parts,name,route)
    rows=10000 if dataset=='smoke' else (10000000 if dataset=='narrow10m' else 1000000)
    def uri(which):
        return f"postgresql://user1:{urllib.parse.quote(CONFIG['password'],safe='')}@{CONFIG[which+'_host']}:6432/db1?sslmode=verify-full&sslrootcert=/cert/RootCA.crt"
    cfg={'source_host':CONFIG['source_host'],'source_transport':'stunnel-5.72-TLS-CA-and-hostname-verified','source_kind':product.removeprefix('sail_'),'source_uri':uri('source'),'source_url':f"jdbc:postgresql://{CONFIG['source_host']}:6432/db1?sslmode=verify-full&sslrootcert=/cert/RootCA.crt",'source_table':'fair21.'+dataset,'target_uri':uri('pg'),'target_name':name,'ch_host':CONFIG['ch_host'],'password':CONFIG['password'],'rows':rows,'parts':parts,'route':route}
    path=PRIVATE/(name+'.json');path.write_text(json.dumps(cfg));path.chmod(0o600)
    (ROOT/'results/configs'/path.name).write_text(redact(json.dumps(cfg)))
    return [['env','PYTHONPATH=/work','/opt/sail/bin/python','/work/sail_transfer.py','/work/private/'+path.name]],'sail-'+cfg['source_kind']+('-buffered-workaround' if product=='sail_adbc' else '')+'-benchmark-arrow-sink-shared-tls-proxy'

runner.configure=configure
parser=argparse.ArgumentParser();parser.add_argument('--smoke',action='store_true');parser.add_argument('--source',choices=['jdbc','adbc']);parser.add_argument('--resume',action='store_true',help='Retain completed outcomes, including failures, and run only missing cases');args=parser.parse_args()
if args.smoke:
    jobs=[(tool,route,'smoke',parts,0) for tool in ('sail_jdbc','sail_adbc') for route in ('pg-pg','pg-ch') for parts in (1,4)]
else:
    jobs=[(tool,route,dataset,parts,rep) for tool in ('sail_jdbc','sail_adbc') for route in ('pg-pg','pg-ch') for dataset in ('narrow','wide') for parts in (1,4) for rep in (1,2,3)]
    # Contemporaneous controls after the managed-cluster role change.
    jobs += [(tool,route,dataset,4,rep) for tool in ('rust','spark') for route in ('pg-pg','pg-ch') for dataset in ('narrow','wide') for rep in (1,2,3)]
    random.Random(2026092112).shuffle(jobs)
    jobs += [(tool,route,'narrow10m',4,1) for tool in ('sail_jdbc','sail_adbc','rust','spark') for route in ('pg-pg','pg-ch')]
if args.source:jobs=[job for job in jobs if job[0]=='sail_'+args.source]
results=[];affinities=[]
manifest=ROOT/'results'/('sail-smoke.json' if args.smoke else 'sail-followup.json')
with (ROOT/'measurement.lock').open('w') as lock:
    fcntl.flock(lock,fcntl.LOCK_EX)
    if args.resume:
        results=json.loads(manifest.read_text())
        completed={(e['product'],e['route'],e['dataset'],e['parts'],e['followup_repetition']) for e in results}
        jobs=[job for job in jobs if job not in completed]
    try:
        for row in json.loads((ROOT/'results/background-affinity-before.json').read_text()):
            name=row['name'];before=subprocess.check_output(['docker','inspect','--format','{{.HostConfig.CpusetCpus}}',name],text=True).strip()
            affinities.append({'name':name,'before':before})
            subprocess.run(['docker','update','--cpuset-cpus','16-31',name],check=True,stdout=subprocess.DEVNULL)
        for tool,route,dataset,parts,rep in jobs:
            result=_run(tool,dataset,parts,0,600 if dataset=='narrow10m' else 180,route)
            results.append({'series':tool if tool.startswith('sail_') else 'sail_control_'+tool,'product':tool,'route':route,'dataset':dataset,'parts':parts,'followup_repetition':rep,'result':result})
            manifest.write_text(json.dumps(results,indent=2)+'\n')
            print('RESULT',tool,route,dataset,parts,rep,result['status'],flush=True)
            if result['status']!='verified' and dataset!='narrow10m':
                raise RuntimeError('Sail transfer failed; inspect evidence before continuing')
    finally:
        for row in affinities:
            restored=row['before'] or '0-31'
            subprocess.run(['docker','update','--cpuset-cpus',restored,row['name']],check=True,stdout=subprocess.DEVNULL)
            row['after']=subprocess.check_output(['docker','inspect','--format','{{.HostConfig.CpusetCpus}}',row['name']],text=True).strip()
            if row['after']!=restored:raise RuntimeError('Affinity restoration failed')
        (ROOT/'results'/('sail-smoke-affinity.json' if args.smoke else 'sail-affinity.json')).write_text(json.dumps(affinities,indent=2)+'\n')
