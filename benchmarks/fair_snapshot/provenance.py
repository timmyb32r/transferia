"""Record only non-secret environment/build metadata, never container environments."""
import json,pathlib,platform,subprocess
from setup import ROOT,connect
from runner import IMAGES
images={}
for image in sorted(set(IMAGES.values())|{'ghcr.io/estuary/source-postgres:dev','ghcr.io/estuary/source-postgres-batch:dev','ghcr.io/estuary/materialize-postgres:dev'}):
    data=json.loads(subprocess.check_output(['docker','image','inspect',image]))[0]
    images[image]={k:data.get(k) for k in ('Id','RepoDigests','Created','Architecture','Os','Size')}
    images[image]['labels']=data.get('Config',{}).get('Labels')
    safe_keys={'KAFKA_VERSION','DEBEZIUM_VERSION','JAVA_VERSION','SPARK_VERSION','SCALA_VERSION'}
    images[image]['published_versions']={item.split('=',1)[0]:item.split('=',1)[1] for item in data.get('Config',{}).get('Env',[]) if item.split('=',1)[0] in safe_keys}
result={'captured_at':subprocess.check_output(['date','-u','+%FT%TZ'],text=True).strip(),'images':images,'kernel':platform.platform(),'cpu':subprocess.check_output(['lscpu'],text=True),'memory':pathlib.Path('/proc/meminfo').read_text(),'binaries':{}}
result['source_rustc']=subprocess.check_output([str(pathlib.Path.home()/'.cargo/bin/rustc'),'--version'],cwd=ROOT/'source',text=True).strip()
result['go_binary_build_info']=subprocess.check_output(['go','version','-m',str(ROOT/'bin/transferctl')],text=True).strip()
for p in (ROOT/'bin').iterdir():
    if p.is_file():result['binaries'][p.name]={'bytes':p.stat().st_size,'mtime_ns':p.stat().st_mtime_ns}
for side in ('source','pg'):
    with connect(side) as c:
        user_parameters=c.execute("SELECT rolconnlimit,current_setting('search_path') FROM pg_roles WHERE rolname=current_user").fetchone()
        result[side]={'user_connection_limit':user_parameters[0],'search_path':user_parameters[1],'version':c.execute('SELECT version()').fetchone()[0],'settings':dict(c.execute("SELECT name,setting FROM pg_settings WHERE name IN ('max_parallel_workers_per_gather','max_connections','shared_buffers','work_mem','synchronous_commit','default_transaction_isolation','wal_level','max_wal_senders','max_replication_slots','jit','effective_cache_size','track_io_timing','pg_stat_statements.track','pg_stat_statements.track_utility')").fetchall())}
(ROOT/'results/provenance.json').write_text(json.dumps(result,indent=2))
