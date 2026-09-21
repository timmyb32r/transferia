#!/usr/bin/env python3
"""Server-only fixture preparation; credentials stay in a private external file."""
import json, pathlib, sys, time
import psycopg
ROOT = pathlib.Path.home() / 'benchmark-20260921'
CONFIG = json.loads((ROOT / 'private/connections.json').read_text())
CA = '/usr/local/share/ca-certificates/YandexInternalRootCA.crt'
def connect(which='source'):
    return psycopg.connect(host=CONFIG[which+'_host'],port=6432,dbname=CONFIG['database'],user=CONFIG['username'],password=CONFIG['password'],sslmode='verify-full',sslrootcert=CA,autocommit=True,prepare_threshold=None,connect_timeout=15)

def main():
    info={}
    for which in ['source','pg']:
        with connect(which) as c:
            info[which]={'version':c.execute('select version()').fetchone()[0], 'tables':c.execute("select schemaname,relname,n_live_tup from pg_stat_user_tables order by n_live_tup desc limit 20").fetchall()}
            c.execute('CREATE SCHEMA IF NOT EXISTS fair21')
    (ROOT/'results/environment.json').write_text(json.dumps(info,indent=2,default=str))
    print(json.dumps(info,indent=2,default=str),flush=True)
    if '--prepare' not in sys.argv: return
    with connect() as c:
        fixtures=[('narrow10m',10000000,96)] if '--large' in sys.argv else [('narrow',1000000,96),('wide',1000000,1024),('smoke',10000,96)]
        for name,n,width in fixtures:
            table='fair21.'+name
            if c.execute('select to_regclass(%s)',(table,)).fetchone()[0]:
                count=c.execute(f'select count(*) from {table}').fetchone()[0]
                if count == n:
                    print('existing fixture',table,count,flush=True); continue
                if count != 0: raise RuntimeError('partial fixture: '+table)
            else:
                c.execute(f'CREATE TABLE {table} (id BIGSERIAL PRIMARY KEY, group_id BIGINT NOT NULL, amount BIGINT NOT NULL, label TEXT NOT NULL, payload TEXT NOT NULL)')
            started=time.monotonic()
            c.execute(f"INSERT INTO {table} SELECT i, (i*17)%1000, (i::bigint*7919)%100000000-50000000, 'row-'||i::text, left(repeat('value-'||i::text||'-abcdefghijklmnopqrstuvwxyz-',100),{width}) FROM generate_series(1,{n}) i")
            c.execute(f'ANALYZE {table}')
            for part in range(4):
                lo=1+part*n//4; hi=1+(part+1)*n//4
                c.execute(f'CREATE VIEW fair21.{name}_p{part} AS SELECT * FROM {table} WHERE id >= {lo} AND id < {hi}')
            stats=c.execute(f'select count(*),min(id),max(id),sum(amount),sum(octet_length(payload)) from {table}').fetchone()
            print('prepared',name,stats,'seconds',round(time.monotonic()-started,2),flush=True)
if __name__=='__main__': main()
