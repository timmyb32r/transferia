#!/usr/bin/env python3
"""Planner-only evidence; not EXPLAIN ANALYZE and not a transfer measurement."""
import fcntl,json
from setup import ROOT,connect
with (ROOT/'measurement.lock').open('w') as lock:
 fcntl.flock(lock,fcntl.LOCK_EX)
 results=[]
 with connect() as c:
  for table,n in [('narrow',1000000),('wide',1000000),('narrow10m',10000000)]:
   pages,bytes_=c.execute('SELECT relpages,pg_relation_size(oid) FROM pg_class WHERE oid=%s::regclass',('fair21.'+table,)).fetchone()
   base='SELECT id,group_id,amount,label,payload FROM ONLY fair21.'+table
   for kind,sql in [('full_scan',base),('first_pk_quarter',base+f' WHERE id >= 1 AND id < {1+n//4}'),('first_ctid_quarter',base+f" WHERE ctid >= '(0,0)'::tid AND ctid < '({(pages+3)//4},0)'::tid")]:
    plan=c.execute('EXPLAIN (FORMAT JSON, COSTS TRUE) '+sql).fetchone()[0]
    results.append({'table':table,'rows':n,'heap_pages':pages,'heap_bytes':bytes_,'variant':kind,'sql':sql,'plan':plan})
 (ROOT/'results/query-plans.json').write_text(json.dumps(results,indent=2)+'\n')
 print('Saved',len(results),'planner-only plans; no execution timing claimed.')
