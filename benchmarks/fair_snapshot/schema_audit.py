#!/usr/bin/env python3
"""Audit actual destination constraints after connector DDL, outside timing."""
import fcntl
import json
from setup import ROOT,connect
import clickhouse
COLS=('id','group_id','amount','label','payload')
with (ROOT/'measurement.lock').open('w') as lock:
    fcntl.flock(lock,fcntl.LOCK_EX)
    runs=[json.loads(p.read_text()) for p in (ROOT/'results/runs').glob('*.json')]
    diagnostic_names=set()
    for filename in ('jdbc-prepare-probe.json','estuary-delta-probe.json','datax-poll-probe.json','go-homo-probe.json','jdbc-batch-probe.json','flink-backfill-probe.json'):
        path=ROOT/'results'/filename
        if path.exists():
            diagnostic_names.update(x['result']['name'] for x in json.loads(path.read_text()) if x['result']['status']=='verified')
    runs=[r for r in runs if r['status']=='verified' and (r['repetition']>0 or r['name'] in diagnostic_names) and not r.get('exclude_from_comparison')]
    names=sorted({r['dataset'] if r['product']=='airbyte' else r['name'] for r in runs if r['route']=='pg-pg'})
    result={'diagnostic_runs_included':len(diagnostic_names),'pg':{},'ch':{},'issues':[],'note':'Airbyte reuses one target per dataset; this audits the currently retained schema, not a historical DDL trace.'}
    with connect('pg') as c:
        rows=c.execute("""SELECT c.relname,a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,
          EXISTS(SELECT 1 FROM pg_constraint k WHERE k.conrelid=c.oid AND k.contype='p' AND k.conkey=ARRAY[a.attnum]::smallint[])
          FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_attribute a ON a.attrelid=c.oid
          WHERE n.nspname='fair21' AND c.relname=ANY(%s) AND a.attname=ANY(%s) AND NOT a.attisdropped""",(names,list(COLS))).fetchall()
    for table,col,typ,notnull,pk in rows:
        result['pg'].setdefault(table,{})[col]={'type':typ,'not_null':notnull,'single_column_pk':pk}
    for name in names:
        cols=result['pg'].get(name,{})
        types_ok=all(cols.get(col,{}).get('type') in ({'bigint'} if i<3 else {'text','character varying'}) for i,col in enumerate(COLS))
        if set(cols)!=set(COLS) or not types_ok or not all(v['not_null'] for v in cols.values()) or not cols.get('id',{}).get('single_column_pk'):
            result['issues'].append({'route':'pg-pg','table':name,'reason':'missing expected id primary key, NOT NULL, field, or lossless type'})
    names=sorted({r['name'] for r in runs if r['route']=='pg-ch'})
    if names:
        # Names originate from completed harness runs and contain no SQL punctuation.
        if not all(n.replace('_','').isalnum() for n in names):raise RuntimeError('unexpected generated table name')
        literal=','.join("'"+n+"'" for n in names)
        tables=[json.loads(s) for s in clickhouse.query(f"SELECT name,engine,sorting_key FROM system.tables WHERE database='db1' AND name IN ({literal}) FORMAT JSONEachRow").splitlines()]
        fields=[json.loads(s) for s in clickhouse.query(f"SELECT table,name,type FROM system.columns WHERE database='db1' AND table IN ({literal}) FORMAT JSONEachRow").splitlines()]
        for t in tables:result['ch'][t['name']]={'engine':t['engine'],'sorting_key':t['sorting_key'],'fields':{}}
        for f in fields:
            if f['name'] in COLS:result['ch'][f['table']]['fields'][f['name']]=f['type']
        expected={c:'Int64' if i<3 else 'String' for i,c in enumerate(COLS)}
        for name in names:
            t=result['ch'].get(name,{})
            if t.get('engine')!='MergeTree' or t.get('sorting_key')!='id' or t.get('fields')!=expected:
                result['issues'].append({'route':'pg-ch','table':name,'reason':'unexpected engine, ordering, or field type/nullability'})
    (ROOT/'results/destination-schema-audit.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({'pg_tables':len(result['pg']),'ch_tables':len(result['ch']),'issues':result['issues']}))
