"""TLS-verified ClickHouse fixture DDL and post-timing correctness only."""
import base64,json,ssl,urllib.request
from setup import CONFIG,CA
class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs): return None

def query(sql):
    context=ssl.create_default_context(cafile=CA)
    opener=urllib.request.build_opener(urllib.request.HTTPSHandler(context=context),NoRedirect())
    request=urllib.request.Request('https://'+CONFIG['ch_host']+':8443/',data=sql.encode(),method='POST')
    request.add_header('Authorization','Basic '+base64.b64encode((CONFIG['username']+':'+CONFIG['password']).encode()).decode())
    with opener.open(request,timeout=120) as r:return r.read().decode()

def prepare(name,product):
    extra=', _system_topic Nullable(String), _system_partition Nullable(Int64), _system_offset Nullable(Int64), _system_message_index Nullable(UInt64)' if product in ('rust','rust_ranges') else ''
    query(f'CREATE TABLE db1.{name} (id Int64, group_id Int64, amount Int64, label String, payload String{extra}) ENGINE=MergeTree ORDER BY id')

def verify(name,dataset):
    n=10000 if dataset=='smoke' else (10000000 if dataset=='narrow10m' else 1000000);width=1024 if dataset=='wide' else 96
    sql=f"SELECT count() AS n,uniqExact(id) AS unique_n,min(id) AS min_id,max(id) AS max_id,countIf(group_id != (id*17)%1000 OR amount != (id*7919)%100000000-50000000 OR label != concat('row-',toString(id)) OR payload != substring(repeat(concat('value-',toString(id),'-abcdefghijklmnopqrstuvwxyz-'),100),1,{width})) AS bad FROM db1.{name} FORMAT JSONEachRow"
    r=json.loads(query(sql));values=tuple(int(r[k]) for k in ('n','unique_n','min_id','max_id','bad'))
    if values!=(n,n,1,n,0):raise RuntimeError('ClickHouse destination mismatch: '+str(values))
    return {'verified_rows':n,'verification':'all five cells of every row, deterministic expression; count/uniqExact/min/max identity check'}
