#!/usr/bin/env python3
"""Separate Debezium P16 check; stop immediately if its adapter fails."""
import json
from setup import ROOT
from runner import run
for rep in (1,2,3):
 for tool in ('debezium','debezium_bulk'):
  previous=[json.loads(p.read_text()) for p in (ROOT/'results/runs').glob('*.json')]
  if any(r['product']==tool and r['dataset']=='narrow' and r['parts_requested']==16 and r['repetition']==rep and not r.get('exclude_from_comparison') for r in previous):continue
  result=run(tool,'narrow',16,rep,600,'pg-pg')
  if result['status']!='verified':raise SystemExit('P16 failed; inspect before repeating')
