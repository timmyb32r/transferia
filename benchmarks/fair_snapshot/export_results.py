"""Export credential-screened compact results, keep bulky diagnostics on server."""
import json,pathlib,urllib.parse
from setup import ROOT,CONFIG
for directory in ('runs','configs','activity'):
    for p in (ROOT/'results'/directory).glob('*.json'):
        s=p.read_text()
        for secret in (CONFIG['password'],urllib.parse.quote(CONFIG['password'],safe='')):
            if secret in s:raise SystemExit('Credential screen failed for '+p.name)
runs=[json.loads(p.read_text()) for p in sorted((ROOT/'results/runs').glob('*.json'))]
(ROOT/'results/runs.jsonl').write_text(''.join(json.dumps(r)+'\n' for r in runs))
configs={p.stem:json.loads(p.read_text()) for p in sorted((ROOT/'results/configs').glob('*.json'))}
(ROOT/'results/configurations.json').write_text(json.dumps(configs,indent=2))
activity={}
for p in sorted((ROOT/'results/activity').glob('*.json')):
    samples=json.loads(p.read_text());queries=sorted({q['query'] for sample in samples for q in sample['queries']})
    activity[p.stem]={'samples':len(samples),'max_active':max((len(s['queries']) for s in samples),default=0),'distinct_queries':queries}
(ROOT/'results/source-queries.json').write_text(json.dumps(activity,indent=2))
print('Exported',len(runs),'runs; credentials screen passed.')
