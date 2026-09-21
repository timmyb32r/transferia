#!/usr/bin/env python3
"""Deterministic randomized, sequential campaign with durable result-based resume."""
import argparse,datetime,json,random,subprocess,sys,traceback
from setup import ROOT
from runner import run,redact
TOOLS=['rust','rust_ranges','go','seatunnel','sling','datax','airbyte','flink','debezium','debezium_bulk','inlong','meltano','spark','sqoop','estuary']
CH=['rust','rust_ranges','go','seatunnel','sling','spark','datax']
def main():
    p=argparse.ArgumentParser();p.add_argument('--phase',choices=['coverage','repeats','scale'],required=True);a=p.parse_args()
    jobs=[]
    if a.phase=='coverage':
        for dataset in ('narrow','wide'):
            batch=[(t,dataset,k,1,'pg-pg') for t in TOOLS for k in (1,4)]
            random.Random(20260921+(dataset=='wide')).shuffle(batch);jobs+=batch
        batch=[(t,d,k,1,'pg-ch') for t in CH for d in ('narrow','wide') for k in (1,4)]
        random.Random(20260923).shuffle(batch);jobs+=batch
    elif a.phase=='repeats':
        for rep in (2,3):
            batch=[(t,d,k,rep,r) for r,tools in [('pg-pg',TOOLS),('pg-ch',CH)] for t in tools for d in ('narrow','wide') for k in (1,4)]
            random.Random(20260921+rep).shuffle(batch);jobs+=batch
    else:
        # Prior finite jobs are complete; reclaim only their verified staging
        # under the common lock before allocating larger Kafka/import files.
        subprocess.run([sys.executable,str(ROOT/'prune_intermediates.py'),'--apply'],check=True)
        for k in (4,1):
            batch=[(t,'narrow10m',k,1,r) for r,tools in [('pg-pg',TOOLS),('pg-ch',CH)] for t in tools]
            random.Random(20260926+k).shuffle(batch);jobs+=batch
    (ROOT/'results'/('schedule-'+a.phase+'.json')).write_text(json.dumps(jobs,indent=2))
    for tool,dataset,parts,rep,route in jobs:
        if datetime.datetime.now(datetime.timezone.utc)>=datetime.datetime(2026,9,21,6,35,tzinfo=datetime.timezone.utc):
            print('Campaign measurement cutoff reached; preserve report hour.',flush=True);break
        if (ROOT/'pause-campaign').exists():print('Campaign paused at run boundary.',flush=True);break
        existing=[json.loads(p.read_text()) for p in (ROOT/'results/runs').glob('*.json')]
        if any(not x.get('exclude_from_comparison') and x['product']==tool and x['dataset']==dataset and x['parts_requested']==parts and x['repetition']==rep and x['route']==route for x in existing):continue
        # Do not repeat a failed adapter blindly. Coverage remains an explicit failure.
        if rep>1 and any(not x.get('exclude_from_comparison') and x['product']==tool and x['dataset']==dataset and x['parts_requested']==parts and x['route']==route and x['repetition']==1 and x['status']!='verified' for x in existing):continue
        print('START',tool,dataset,parts,rep,route,flush=True)
        timeout=900 if a.phase=='scale' and parts==4 else 600
        try:run(tool,dataset,parts,rep,timeout,route)
        except Exception as exc:print('HARNESS_FAILURE',redact(str(exc)),flush=True)
if __name__=='__main__':main()
