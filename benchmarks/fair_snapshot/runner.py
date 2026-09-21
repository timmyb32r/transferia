#!/usr/bin/env python3
"""Isolated snapshot runner. Python orchestrates; only the named engines move data.

All engine processes share one 16-CPU/24-GiB Docker cgroup per finite run.
A completion marker keeps that cgroup alive until its final CPU counter is read.
Correctness checks execute after timing and validate every deterministic cell.
"""
import argparse,datetime,json,os,pathlib,shlex,subprocess,time,urllib.parse
import psutil,yaml,fcntl
from observer import Observer
import clickhouse,db_stats
from setup import ROOT,CONFIG,CA,connect
PRIVATE=ROOT/'private'
COLS=['id','group_id','amount','label','payload']
DDL='id BIGINT PRIMARY KEY, group_id BIGINT NOT NULL, amount BIGINT NOT NULL, label TEXT NOT NULL, payload TEXT NOT NULL'
IMAGES={'debezium_bulk':'quay.io/debezium/connect:3.6','rust_ranges':'transferia/benchmark-runtime:20260921','estuary':'transferia/benchmark-runtime:20260921','sling':'slingdata/sling:latest','seatunnel':'apache/seatunnel:2.3.12','datax':'apache/seatunnel:2.3.12','rust':'transferia/benchmark-runtime:20260921','go':'transferia/benchmark-runtime:20260921','spark':'apache/spark:3.5.7','meltano':'transferia/benchmark-runtime:20260921','sqoop':'eclipse-temurin:8-jdk','flink':'flink:1.20.3-scala_2.12-java17','inlong':'flink:1.18.1-scala_2.12-java11','airbyte':'transferia/airbyte-pair:20260921','debezium':'quay.io/debezium/connect:3.6'}

def save(path,value):
    path.parent.mkdir(parents=True,exist_ok=True); path.write_text(json.dumps(value,indent=2,default=str)+'\n')
def redact(text):
    for value in [CONFIG['password'],urllib.parse.quote(CONFIG['password'],safe='')]: text=text.replace(value,'[REDACTED]')
    return text

def connection(which):
    return {'database':'db1','username':'user1','password':CONFIG['password'],'installation':{'type':'on_premise','host':CONFIG[which+'_host'],'port':6432,'trusted_plaintext':False,'tls_ca_file':'/cert/RootCA.crt'}}
def jdbc(which):
    return f"jdbc:postgresql://{CONFIG[which+'_host']}:6432/db1?sslmode=verify-full&sslrootcert=/cert/RootCA.crt&prepareThreshold=0&reWriteBatchedInserts=true"
def uri(which):
    return f"postgresql://user1:{urllib.parse.quote(CONFIG['password'],safe='')}@{CONFIG[which+'_host']}:6432/db1?sslmode=verify-full&sslrootcert=/cert/RootCA.crt"

def configure(product,dataset,parts,name,route):
    n=10000 if dataset=='smoke' else (10000000 if dataset=='narrow10m' else 1000000)
    src='fair21.'+dataset; dst='fair21.'+name
    commands=[]; variant='native'
    path=PRIVATE/(name+'.json')
    if product in ('rust','rust_ranges'):
        cfg={'delivery_id':name,'delivery_name':name,'delivery_type':'batch','source':{'postgres':connection('source')|{'tables':{'type':'selected','rules':[{'include':src}]},'batch_rows':65536,'copy_to_format':'binary','unsupported_types':'fail','max_snapshot_parts':parts}},'sink':{'postgres':connection('pg')|{'create_tables':False,'copy_from_format':'binary'}},'middlewares':[{'rename_table':{'mode':'exact','name':dst}}],'durable_storage':{'type':'local_file','path':'/work/private/state/'+name},'pipeline_memory_limit_bytes':1073741824,'metrics':{'interval_ms':1000}}
        commands=[['/work/bin/transferia-auto','--config','/work/private/'+path.name]]
        variant='native-cap'
        if product=='rust_ranges':
            commands=[['env','FAIR21_EXACT_RANGES=1','/work/bin/transferia-ranges','--config','/work/private/'+path.name]];variant='benchmark-exact-pk-ranges'
    elif product=='go':
        source={'Hosts':[CONFIG['source_host']],'Port':6432,'Database':'db1','User':'user1','Password':CONFIG['password'],'TLSFile':pathlib.Path(CA).read_text(),'EnableTLS':True,'DBTables':[src],'NoHomo':True,'SnapshotDegreeOfParallelism':parts,'DesiredTableSize':1,'SnapshotSerializationFormat':'binary','PreSteps':{},'PostSteps':{}}
        sink={'Hosts':[CONFIG['pg_host']],'Port':6432,'Name':'db1','User':'user1','Password':CONFIG['password'],'TLSFile':pathlib.Path(CA).read_text(),'EnableTLS':True,'Cleanup':'Disabled'}
        cfg={'id':name,'transfername':name,'type':'SNAPSHOT_ONLY','src':{'type':'pg','params':json.dumps(source)},'dst':{'type':'pg','params':json.dumps(sink)},'data_objects':{'include_objects':[src]},'transformation':json.dumps({'transformers':[{'renameTables':{'renameTables':[{'originalName':{'nameSpace':'fair21','name':dataset},'newName':{'nameSpace':'fair21','name':name}}]}}]})}
        commands=[['/go/transferctl','--log-level','info','run','activate','--params','/work/private/'+path.name]]
    elif product=='seatunnel':
        cfg={'env':{'parallelism':parts,'job.mode':'BATCH'},'source':{'Jdbc':{'url':jdbc('source'),'driver':'org.postgresql.Driver','user':'user1','password':CONFIG['password'],'query':f'SELECT {",".join(COLS)} FROM {src}','partition_column':'id','partition_lower_bound':1,'partition_upper_bound':n+1,'partition_num':parts,'fetch_size':8192}},'sink':{'Jdbc':{'url':jdbc('pg'),'driver':'org.postgresql.Driver','user':'user1','password':CONFIG['password'],'query':f'INSERT INTO {dst} ({",".join(COLS)}) VALUES (?,?,?,?,?)','batch_size':1000,'is_exactly_once':False}}}
        cfg['source']=[{'plugin_name':'Jdbc',**cfg['source']['Jdbc']}];cfg['sink']=[{'plugin_name':'Jdbc',**cfg['sink']['Jdbc']}]
        commands=[['/opt/seatunnel/bin/seatunnel.sh','--config','/work/private/'+path.name,'--master','local','-DJvmOption=-Xmx4g']]
        variant='native-heap4g'
    elif product=='datax':
        content=[]
        for part in range(parts):
            lo=1+part*n//parts; hi=1+(part+1)*n//parts
            query=f'SELECT {",".join(COLS)} FROM {src} WHERE id >= {lo} AND id < {hi}'
            reader={'name':'postgresqlreader','parameter':{'username':'user1','password':CONFIG['password'],'fetchSize':8192,'connection':[{'jdbcUrl':[jdbc('source')],'querySql':[query]}]}}
            writer={'name':'postgresqlwriter','parameter':{'username':'user1','password':CONFIG['password'],'column':COLS,'batchSize':1000,'connection':[{'jdbcUrl':jdbc('pg'),'table':[dst]}]}}
            content.append({'reader':reader,'writer':writer})
        queries=[item['reader']['parameter']['connection'][0]['querySql'][0] for item in content]
        content[0]['reader']['parameter']['connection'][0]['querySql']=queries
        cfg={'job':{'setting':{'speed':{'channel':parts},'errorLimit':{'record':0,'percentage':0}},'content':[content[0]]}}
        commands=[['python3','/datax/bin/datax.py','/work/private/'+path.name]]; variant='explicit-ranges'
    elif product in ('debezium','debezium_bulk'):
        with connect() as c: c.execute(f'CREATE PUBLICATION {name} FOR TABLE {src}')
        source={'name':name+'_source','connector.class':'io.debezium.connector.postgresql.PostgresConnector','tasks.max':1,'database.hostname':CONFIG['source_host'],'database.port':6432,'database.user':'user1','database.password':CONFIG['password'],'database.dbname':'db1','database.sslmode':'verify-full','database.sslrootcert':'/cert/RootCA.crt','plugin.name':'pgoutput','slot.name':name,'publication.name':name,'publication.autocreate.mode':'disabled','topic.prefix':name,'table.include.list':src.replace('.','\\.'),'snapshot.mode':'initial_only','snapshot.max.threads':parts,'snapshot.max.threads.multiplier':1,'legacy.snapshot.max.threads':False,'decimal.handling.mode':'precise','max.batch.size':8192,'max.queue.size':32768,'poll.interval.ms':100,'driver.prepareThreshold':0,'snapshot.fetch.size':8192}
        sink={'name':name+'_sink','connector.class':'io.debezium.connector.jdbc.JdbcSinkConnector','tasks.max':parts,'connection.url':jdbc('pg'),'connection.username':'user1','connection.password':CONFIG['password'],'topics':name+'.'+src,'table.name.format':dst,'insert.mode':'insert','delete.enabled':False,'primary.key.mode':'record_key','primary.key.fields':'id','schema.evolution':'none','batch.size':1000,'consumer.override.max.poll.records':1000}
        if product=='debezium_bulk':
            source.update({'producer.override.linger.ms':5,'producer.override.batch.size':262144,'producer.override.compression.type':'lz4','producer.override.buffer.memory':67108864})
            sink.update({'consumer.override.fetch.min.bytes':1048576,'consumer.override.fetch.max.wait.ms':50,'consumer.override.max.partition.fetch.bytes':8388608,'consumer.override.fetch.max.bytes':33554432})
        cfg={'parts':parts,'directory':'/work/private/'+name,'source':source,'sink':sink}
        commands=[['python3','/work/debezium_job.py','/work/private/'+path.name]];variant='native-chunks-kafka-jdbc'+('-bulk-tuned' if product=='debezium_bulk' else '')
    elif product=='estuary':
        cfg=[]
        for part in range(parts):
            task=name+'_e'+str(part); collection='fair21/'+task
            auth={'auth_type':'UserPassword','password':CONFIG['password']}
            capture={'address':CONFIG['source_host']+':6432','user':'user1','database':'db1','password':CONFIG['password'],'advanced':{'sslmode':'verify-full','discover_schemas':['fair21'],'poll':'1h'}}
            target={'address':CONFIG['pg_host']+':6432','user':'user1','database':'db1','schema':'fair21','credentials':auth,'advanced':{'sslmode':'verify-full','no_flow_document':True,'feature_flags':'allow_existing_tables_for_new_bindings'}}
            lo=1+part*n//parts;hi=1+(part+1)*n//parts
            schema={'type':'object','required':COLS,'properties':{col:{'type':'integer' if i<3 else 'string'} for i,col in enumerate(COLS)}}
            resource={'name':task,'schema':'fair21','table':dataset,'cursor':[],'template':f'SELECT {",".join(COLS)} FROM {src} WHERE id >= {lo} AND id < {hi}'}
            spec={'collections':{collection:{'schema':schema,'key':['/id']}},'captures':{'fair21/'+task+'_capture':{'endpoint':{'local':{'command':['/work/bin/estuary-source-postgres-batch'],'protobuf':True,'env':{'GOMEMLIMIT':'900MiB','PGSSLROOTCERT':'/cert/RootCA.crt','SHUTDOWN_AFTER_POLLING':'yes'},'config':capture}},'bindings':[{'resource':resource,'target':collection}]}},'materializations':{'fair21/'+task+'_materialize':{'endpoint':{'local':{'command':['/work/bin/estuary-materialize-postgres'],'protobuf':True,'env':{'GOMEMLIMIT':'900MiB','PGSSLROOTCERT':'/cert/RootCA.crt'},'config':target}},'bindings':[{'resource':{'table':name,'schema':'fair21'},'source':collection,'fields':{'recommended':False,'include':{col:{} for col in COLS}}}]}}}
            specpath=PRIVATE/(task+'.flow.json');specpath.write_text(json.dumps(spec));specpath.chmod(0o600)
            fixture='/work/private/'+task+'.fixture'
            command=f'export PGSSLROOTCERT=/cert/RootCA.crt SHUTDOWN_AFTER_POLLING=yes; /work/bin/estuary-capture {hi-lo} {fixture} /work/private/{specpath.name} fair21/{task}_capture && /work/flowctl preview --source /work/private/{specpath.name} --name fair21/{task}_materialize --fixture {fixture} -o json'
            commands.append(['bash','-c',command]);cfg.append(spec)
        variant='local-preview-external-ranges-staged'
    elif product=='airbyte':
        ssl={'mode':'verify-full','ca_certificate':pathlib.Path(CA).read_text()}
        common={'port':6432,'database':'db1','username':'user1','password':CONFIG['password'],'ssl_mode':ssl,'tunnel_method':{'tunnel_method':'NO_TUNNEL'},'jdbc_url_params':'prepareThreshold=0'}
        source=common|{'host':CONFIG['source_host'],'schemas':['fair21'],'replication_method':{'method':'Standard'},'max_db_connections':parts}
        target=common|{'host':CONFIG['pg_host'],'schema':'fair21','ssl':True,'raw_data_schema':'fair21_raw','disable_type_dedupe':False}
        schema={'type':'object','properties':{col:({'type':'number','airbyte_type':'integer'} if i<3 else {'type':'string'}) for i,col in enumerate(COLS)}}
        catalog={'streams':[{'stream':{'name':dataset,'namespace':'fair21','json_schema':schema,'supported_sync_modes':['full_refresh'],'source_defined_primary_key':[['id']]},'sync_mode':'full_refresh','destination_sync_mode':'append','primary_key':[['id']],'generation_id':1,'minimum_generation_id':0,'sync_id':1}]}
        for suffix,value in [('source',source),('target',target),('catalog',catalog)]:
            p=PRIVATE/f'{name}_{suffix}.json';p.write_text(json.dumps(value));p.chmod(0o600)
        prefix='/work/private/'+name
        command=f'env AIRBYTE_CONNECTOR_EXTRACT_JDBC_MAX_SAMPLE_SIZE={parts} AIRBYTE_CONNECTOR_EXTRACT_JDBC_EXPECTED_THROUGHPUT_BYTES_PER_SECOND={parts} /airbyte/bin/source-postgres --read --config {prefix}_source.json --catalog {prefix}_catalog.json | /work/bin/fair21-airbyte-bridge | /target/bin/destination-postgres --write --config {prefix}_target.json --catalog {prefix}_catalog.json'
        commands=[['bash','-o','pipefail','-c',command]];cfg={'source':source,'target':target,'catalog':catalog};variant='native-exact-ctid-parts-platform-counting'
        cfg['benchmark_runtime_overrides']={'table_sample_size':parts,'throughput_bytes_per_second':parts,'reason':'force exactly P sampled boundaries; production CTID splitter divides heap into P ranges'}
    elif product=='sqoop':
        passwd=PRIVATE/(name+'.password');passwd.write_text(CONFIG['password']);passwd.chmod(0o600)
        sq='/work/tools/sqoop-1.4.7.bin__hadoop-2.6.0'
        hadoop='/work/tools/hadoop-2.10.2'
        output='/work/private/'+name+'_staging'
        env=f'export HADOOP_HOME={hadoop} HADOOP_COMMON_HOME={hadoop} HADOOP_MAPRED_HOME={hadoop} SQOOP_HOME={sq}; '
        common=['-Dmapreduce.framework.name=local',f'-Dmapreduce.local.map.tasks.maximum={parts}','-Dfs.defaultFS=file:///']
        imp=[sq+'/bin/sqoop','import',*common,'--connect',jdbc('source'),'--username','user1','--password-file','/work/private/'+passwd.name,'--query',f'SELECT {",".join(COLS)} FROM {src} WHERE $CONDITIONS','--split-by','id','--boundary-query',f'SELECT 1,{n+1}','--num-mappers',str(parts),'--target-dir',output,'--fields-terminated-by','\001','--class-name','FairSnapshot','--outdir','/work/private/'+name+'_classes']
        exp=[sq+'/bin/sqoop','export',*common,'--connect',jdbc('pg'),'--username','user1','--password-file','/work/private/'+passwd.name,'--table',name,'--columns',','.join(COLS),'--num-mappers',str(parts),'--export-dir',output,'--input-fields-terminated-by','\001','--batch','--','--schema','fair21']
        cfg={'import':imp,'export':exp}
        commands=[['bash','-c',env+shlex.join(imp)+' && '+shlex.join(exp)]];variant='native-ranges-import-export'
    elif product in ('flink','inlong'):
        fields='id BIGINT, group_id BIGINT, amount BIGINT, label STRING, payload STRING'
        def options(values): return ',\n'.join("'%s'='%s'"%(k,str(v).replace("'","''")) for k,v in values.items())
        if product=='flink':
            with connect() as c: c.execute(f'CREATE PUBLICATION {name} FOR TABLE {src}')
            source={'connector':'postgres-cdc','hostname':CONFIG['source_host'],'port':6432,'username':'user1','password':CONFIG['password'],'database-name':'db1','schema-name':'fair21','table-name':dataset,'slot.name':name,'decoding.plugin.name':'pgoutput','debezium.publication.name':name,'debezium.publication.autocreate.mode':'disabled','debezium.database.sslmode':'verify-full','debezium.database.sslrootcert':'/cert/RootCA.crt','debezium.driver.prepareThreshold':'0','scan.incremental.snapshot.enabled':'true','scan.startup.mode':'snapshot','scan.incremental.snapshot.chunk.size':max(100,n//parts),'scan.snapshot.fetch.size':8192,'scan.incremental.snapshot.unbounded-chunk-first.enabled':'false'}
            connector='jdbc'
            jars=['/work/tools/flink-sql-connector-postgres-cdc-3.4.0.jar','/work/tools/flink-connector-jdbc-3.3.0-1.20.jar','/jars/postgresql.jar']
            sourcefields=fields+', PRIMARY KEY(id) NOT ENFORCED'
        else:
            source={'connector':'jdbc-inlong','url':jdbc('source'),'table-name':src,'username':'user1','password':CONFIG['password'],'scan.partition.column':'id','scan.partition.num':parts,'scan.partition.lower-bound':1,'scan.partition.upper-bound':n+1,'scan.fetch-size':8192,'scan.auto-commit':'false'}
            connector='jdbc-inlong';jars=['/work/tools/apache-inlong-2.4.0/inlong-sort/connectors/sort-connector-jdbc-v1.18-2.4.0.jar','/jars/postgresql.jar'];sourcefields=fields
        sink={'connector':connector,'url':jdbc('pg'),'table-name':dst,'username':'user1','password':CONFIG['password'],'sink.buffer-flush.max-rows':1000,'sink.buffer-flush.interval':'1s','sink.parallelism':parts}
        sql="SET 'table.dml-sync'='true';\nSET 'execution.checkpointing.interval'='5s';\n"+f"CREATE TABLE src ({sourcefields}) WITH ({options(source)});\nCREATE TABLE dst ({fields}{', PRIMARY KEY(id) NOT ENFORCED' if product=='flink' else ''}) WITH ({options(sink)});\nINSERT INTO dst SELECT * FROM src;\n"
        sqlpath=PRIVATE/(name+'.sql');sqlpath.write_text(sql);sqlpath.chmod(0o600)
        config={'jobmanager.rpc.address':'localhost','jobmanager.rpc.port':16123,'jobmanager.bind-host':'127.0.0.1','jobmanager.memory.process.size':'1600m','taskmanager.memory.process.size':'8g','taskmanager.numberOfTaskSlots':parts,'parallelism.default':parts,'rest.port':18084,'rest.address':'localhost','rest.bind-address':'127.0.0.1','taskmanager.bind-host':'127.0.0.1','taskmanager.host':'localhost'}
        if product=='flink' and dataset=='narrow10m':
            # Large native chunks are retained in Java heap. Keep the same
            # 24-GiB cgroup, allocating less unused managed memory to the TM.
            config['taskmanager.memory.process.size']='18g'
            config['taskmanager.memory.managed.fraction']=0.1
        fpath=PRIVATE/(name+'_flink.yaml');fpath.write_text(yaml.safe_dump(config))
        configfile='config.yaml' if product=='flink' else 'flink-conf.yaml'
        script='cp /work/private/'+fpath.name+' /opt/flink/conf/'+configfile+'; '
        if product=='inlong': script+='rm /opt/flink/lib/flink-table-planner-loader-*.jar; '
        script+=' '.join('cp '+shlex.quote(j)+' /opt/flink/lib/; ' for j in jars)
        script+=f'/opt/flink/bin/start-cluster.sh && /opt/flink/bin/sql-client.sh -f /work/private/{sqlpath.name}; rc=$?; /opt/flink/bin/stop-cluster.sh; cat /opt/flink/log/*log; exit "$rc"'
        commands=[['bash','-c',script]];cfg={'source':source,'sink':sink,'flink':config};variant=('native-chunks-large-heap18g' if dataset=='narrow10m' else 'native-chunks') if product=='flink' else 'native-ranges-cursor'
    elif product=='meltano':
        cfg=[]
        for part in range(parts):
            project=PRIVATE/f'{name}_m{part}';project.mkdir()
            stream='fair21-'+dataset
            lo=1+part*n//parts;hi=1+(part+1)*n//parts
            tap={'name':'tap-postgres','namespace':'tap_postgres','pip_url':'meltanolabs-tap-postgres==0.10.0','executable':'tap-postgres','capabilities':['catalog','discover','state','stream-maps'],'config':{'sqlalchemy_url':uri('source').replace('postgresql://','postgresql+psycopg://'),'filter_schemas':['fair21'],'default_replication_method':'FULL_TABLE','stream_options':{stream:{'custom_where_clauses':[f'id >= {lo}',f'id < {hi}']}}},'select':[stream+'.*'],'metadata':{stream:{'replication-method':'FULL_TABLE'}}}
            target={'name':'target-postgres','namespace':'target_postgres','pip_url':'meltanolabs-target-postgres==0.8.0','executable':'target-postgres','capabilities':['about','stream-maps'],'config':{'sqlalchemy_url':uri('pg').replace('postgresql://','postgresql+psycopg://'),'default_target_schema':'fair21','use_copy':True,'activate_version':False,'add_record_metadata':False,'load_method':'append-only','stream_maps':{stream:{'__alias__':name}}}}
            sub={'version':1,'default_environment':'prod','environments':[{'name':'prod'}],'plugins':{'extractors':[tap],'loaders':[target]}}
            (project/'meltano.yml').write_text(yaml.safe_dump(sub));(project/'meltano.yml').chmod(0o600)
            for kind,plugin in [('extractors','tap-postgres'),('loaders','target-postgres')]:
                folder=project/'.meltano'/kind/plugin;folder.mkdir(parents=True);(folder/'venv').symlink_to('/opt/meltano')
            commands.append(['/opt/meltano/bin/meltano','--cwd','/work/private/'+project.name,'run','--no-install','--full-refresh','tap-postgres','target-postgres'])
            cfg.append(sub)
        variant='external-ranges-copy'
    elif product=='spark':
        cfg={'password':CONFIG['password'],'rows':n,'parts':parts,'source_url':jdbc('source'),'target_url':jdbc('pg'),'source_table':src,'target_table':dst}
        commands=[['/opt/spark/bin/spark-submit','--master',f'local[{parts}]','--driver-memory','8g','--jars','/jars/postgresql.jar','--conf','spark.ui.enabled=false','/work/spark_job.py','/work/private/'+path.name]]
        variant='explicit-ranges'
    elif product=='sling':
        cfg=[]
        for part in range(parts):
            lo=1+part*n//parts; hi=1+(part+1)*n//parts
            p=PRIVATE/f'{name}_{part}.json'
            sub={'source':uri('source'),'target':uri('pg'),'streams':{src:{'object':dst,'mode':'snapshot','sql':f'SELECT {",".join(COLS)} FROM {src} WHERE id >= {lo} AND id < {hi}','target_options':{'table_tmp':f'fair21.{name}_tmp{part}','table_ddl':f'CREATE TABLE "fair21"."{name}" ({DDL})'},'source_options':{}}}}
            p.write_text(json.dumps(sub));p.chmod(0o600);cfg.append(sub)
            commands.append(['sling','run','-r','/work/private/'+p.name])
        variant='external-ranges'
    else: raise ValueError(product)
    if route=='pg-ch':
        target='db1.'+name
        if product in ('rust','rust_ranges'):
            cfg['sink']={'clickhouse':{'hosts':[CONFIG['ch_host']],'port':9440,'http_port':8443,'trusted_plaintext':False,'tls_ca_file':'/cert/RootCA.crt','data_host_count':2,'database':'db1','username':'user1','password':CONFIG['password'],'shard_group':'','insert_target_rows':65536,'insert_concurrency':1,'compression':'zstd'}}
            fields=['hosts','port','http_port','trusted_plaintext','tls_ca_file','data_host_count']
            cfg['sink']['clickhouse']['installation']={'type':'on_premise',**{k:cfg['sink']['clickhouse'].pop(k) for k in fields}}
            cfg['middlewares']=[{'rename_table':{'mode':'exact','name':target}}]
        elif product=='go':
            sink={'Hosts':[CONFIG['ch_host']],'Database':'db1','User':'user1','Password':CONFIG['password'],'SSLEnabled':True,'HTTPPort':8443,'NativePort':9440,'Cleanup':'Disabled','IsSchemaMigrationDisabled':True,'UseSchemaInTableName':False,'RootCACertPaths':['/cert/RootCA.crt'],'BufferTriggingSize':268435456,'InflightBuffer':65536,'Interval':-1,'ShardsList':[{'Name':'shard1','Hosts':[CONFIG['ch_host']]}]}
            cfg['dst']={'type':'ch','params':json.dumps(sink)}
            variant='native-ch-row-batches'
            cfg['transformation']=json.dumps({'transformers':[{'renameTables':{'renameTables':[{'originalName':{'nameSpace':'fair21','name':dataset},'newName':{'nameSpace':'db1','name':name}}]}}]})
        elif product=='seatunnel':
            cfg['sink']=[{'plugin_name':'Clickhouse','host':CONFIG['ch_host']+':8443','database':'db1','table':name,'username':'user1','password':CONFIG['password'],'bulk_size':65536,'clickhouse.config':{'ssl':'true','sslmode':'strict','sslrootcert':'/cert/RootCA.crt','compress':'false','decompress':'false'}}]
        elif product=='sling':
            for part,sub in enumerate(cfg):
                sub['target']=f"clickhouse://user1:{urllib.parse.quote(CONFIG['password'],safe='')}@{CONFIG['ch_host']}:9440/db1?secure=true&skip_verify=false"
                stream=sub['streams'][src];stream['object']=target
                stream['target_options']={'table_tmp':f'db1.{name}_tmp{part}','table_ddl':f'CREATE TABLE `db1`.`{name}` (id Int64, group_id Int64, amount Int64, label String, payload String) ENGINE=MergeTree ORDER BY id','batch_limit':65536}
                (PRIVATE/f'{name}_{part}.json').write_text(json.dumps(sub))
        elif product=='datax':
            writer=cfg['job']['content'][0]['writer'];writer['name']='clickhousewriter'
            writer['parameter']['batchSize']=65536
            writer['parameter']['connection']=[{'jdbcUrl':f"jdbc:clickhouse://{CONFIG['ch_host']}:8443/db1?ssl=true&sslmode=strict&sslrootcert=/cert/RootCA.crt&compress=false&decompress=false",'table':[name]}]
        elif product=='spark':
            cfg['target_url']=f"jdbc:clickhouse:https://{CONFIG['ch_host']}:8443/db1?ssl=true&sslmode=strict&sslrootcert=/cert/RootCA.crt&jdbc_ignore_unsupported_values=true"
            cfg['target_table']=target;cfg['target_driver']='com.clickhouse.jdbc.DriverV1';cfg['batch_size']=65536
            commands[0][commands[0].index('--jars')+1]+=',/work/tools/clickhouse-jdbc-0.9.8-all.jar'
        else: raise ValueError('No verified ClickHouse adapter for '+product)
    path.write_text(json.dumps(cfg));path.chmod(0o600)
    save(ROOT/'results/configs'/path.name,json.loads(redact(json.dumps(cfg))))
    return commands,variant

class Probe:
    def __init__(self,pid):
        self.pid=pid; self.paths={}
        for line in pathlib.Path(f'/proc/{pid}/cgroup').read_text().splitlines():
            _,controllers,path=line.split(':',2)
            for controller in controllers.split(','): self.paths[controller]=path
    def read(self):
        mem=pathlib.Path('/sys/fs/cgroup/memory'+self.paths['memory'])
        cpu=int(pathlib.Path('/sys/fs/cgroup/cpuacct'+self.paths['cpuacct']+'/cpuacct.usage').read_text())/1e9
        rss=0
        try:
            root=psutil.Process(self.pid)
            for p in [root]+root.children(recursive=True):
                try: rss+=p.memory_info().rss
                except psutil.Error: pass
        except psutil.Error: pass
        return {'cpu_seconds':cpu,'rss_bytes':rss,'cgroup_memory_bytes':int((mem/'memory.usage_in_bytes').read_text()),'cgroup_peak_bytes':int((mem/'memory.max_usage_in_bytes').read_text())}

def verify(name,dataset):
    n=10000 if dataset=='smoke' else (10000000 if dataset=='narrow10m' else 1000000); width=1024 if dataset=='wide' else 96
    with connect('pg') as c:
        row=c.execute(f"SELECT count(*),count(distinct id),min(id),max(id),count(*) FILTER(WHERE group_id IS DISTINCT FROM (id*17)%1000 OR amount IS DISTINCT FROM (id*7919)%100000000-50000000 OR label IS DISTINCT FROM 'row-'||id::text OR payload IS DISTINCT FROM left(repeat('value-'||id::text||'-abcdefghijklmnopqrstuvwxyz-',100),{width})) FROM fair21.{name}").fetchone()
    if row != (n,n,1,n,0): raise RuntimeError('destination mismatch: '+str(row))
    return {'verified_rows':n,'verification':'all five cells of every row, deterministic expression; count/distinct/min/max identity check'}

def _run(product,dataset,parts,rep,timeout,route):
    name=f'{product}_{dataset}_p{parts}_r{rep}_{int(time.time())}'+('_ch' if route=='pg-ch' else '')
    commands,variant=configure(product,dataset,parts,name,route)
    target_name=dataset if product=='airbyte' else name
    if route=='pg-ch': clickhouse.prepare(name,product)
    else:
        with connect('pg') as c:
            if product=='airbyte': c.execute(f'DROP TABLE IF EXISTS fair21.{target_name}')
            c.execute(f'CREATE TABLE fair21.{target_name} ({DDL})')
            if product=='estuary': c.execute(f'ALTER TABLE fair21.{name} ADD COLUMN flow_document JSON')
            if product in ('rust','rust_ranges'):
                c.execute(f'ALTER TABLE fair21.{name} ADD COLUMN _system_topic TEXT, ADD COLUMN _system_partition BIGINT, ADD COLUMN _system_offset BIGINT, ADD COLUMN _system_message_index NUMERIC(20,0)')
    work=ROOT/'private'/name;work.mkdir();work.chmod(0o777)
    script='#!/bin/bash\nset +e\n'
    if len(commands)==1: script+=shlex.join(commands[0])+'\nrc=$?\n'
    else:
        script+='pids=""\n'
        for cmd in commands: script+=shlex.join(cmd)+' &\npids="$pids $!"\n'
        script+='rc=0\nfor pid in $pids; do wait "$pid" || rc=$?; done\n'
    script+=f'echo "$rc" > /work/private/{name}/done\nwhile [ ! -e /work/private/{name}/release ]; do sleep 0.1; done\nexit "$rc"\n'
    (work/'run.sh').write_text(script)
    container='fair21_'+name
    cmd=['docker','create','--name',container,'--user','0','--network','host','--cpuset-cpus','0-15','--memory','24g','--memory-swap','24g','-v','/home/timmyb32r/benchmark-tools-20260919/postgresql-42.7.13.jar:/jars/postgresql.jar:ro','-e','JAVA_TOOL_OPTIONS=-XX:ActiveProcessorCount=16 -Xmx4g','-e','MELTANO_SEND_ANONYMOUS_USAGE_STATS=false','-e','MELTANO_AUTO_INSTALL=false','-e','SSL_CERT_FILE=/cert/RootCA.crt','-e','SLING_SEND_TELEMETRY=false','-e','SLING_LOADED_AT_COLUMN=false','-v',f'{ROOT}:/work','-v',f'{CA}:/cert/RootCA.crt:ro','-v',f'{ROOT}/bin:/go:ro','-v','/home/timmyb32r/benchmark-tools-20260919/datax:/datax','--entrypoint','bash',IMAGES[product],f'/work/private/{name}/run.sh']
    subprocess.run(cmd,check=True,stdout=subprocess.DEVNULL)
    result={'name':name,'product':product,'dataset':dataset,'parts_requested':parts,'variant':variant,'repetition':rep,'route':route,'cpu_affinity':'0-15','memory_limit_bytes':24*1024**3,'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'status':'running'}
    samples=[]
    database_before=db_stats.capture()
    observer=Observer(dataset);observer.start()
    start=time.monotonic(); subprocess.run(['docker','start',container],check=True,stdout=subprocess.DEVNULL)
    pid=int(subprocess.check_output(['docker','inspect','--format','{{.State.Pid}}',container])); probe=Probe(pid)
    completion_poll=0.0
    try:
        while True:
            stat=probe.read();samples.append({'elapsed_seconds':time.monotonic()-start}|stat)
            if product in ('debezium','debezium_bulk') and time.monotonic()-completion_poll>1 and not (work/'finish').exists():
                completion_poll=time.monotonic()
                expected=(10000 if dataset=='smoke' else (10000000 if dataset=='narrow10m' else 1000000))
                with connect('pg') as c:
                    inserted=c.execute('SELECT n_tup_ins FROM pg_stat_user_tables WHERE schemaname=%s AND relname=%s',('fair21',name)).fetchone()[0]
                    count=c.execute(f'SELECT count(*) FROM fair21.{name}').fetchone()[0] if inserted>=expected else 0
                if count >= expected:
                    result['all_rows_committed_seconds']=time.monotonic()-start
                    (work/'finish').touch()
            if (work/'done').exists():
                result['returncode']=int((work/'done').read_text());break
            if time.monotonic()-start>timeout: raise TimeoutError('configured run deadline exceeded')
            time.sleep(.1)
        result['elapsed_seconds']=time.monotonic()-start
        observer.stop()
        result['observed_max_active_source_queries']=max((len(s['queries']) for s in observer.samples),default=0)
        result['observer_error']=observer.error
        result.update(cpu_seconds=stat['cpu_seconds'],peak_rss_bytes=max(s['rss_bytes'] for s in samples),cgroup_peak_bytes=max(s['cgroup_peak_bytes'] for s in samples))
        result['cpu_percent']=100*result['cpu_seconds']/result['elapsed_seconds']
        if result['returncode']: raise RuntimeError('engine exit code '+str(result['returncode']))
        result['database_sql_work_delta']=db_stats.delta(database_before,db_stats.capture())
        result.update(clickhouse.verify(target_name,dataset) if route=='pg-ch' else verify(target_name,dataset)); result['status']='verified'
        result['rows_per_second']=result['verified_rows']/result['elapsed_seconds']; result['rows_per_cpu_second']=result['verified_rows']/result['cpu_seconds']
    except Exception as exc:
        result.update(status='failed',error=redact(str(exc)))
        result.setdefault('elapsed_seconds',time.monotonic()-start)
        if samples:
            result.setdefault('cpu_seconds',samples[-1]['cpu_seconds'])
            result.setdefault('peak_rss_bytes',max(s['rss_bytes'] for s in samples))
            result.setdefault('cgroup_peak_bytes',max(s['cgroup_peak_bytes'] for s in samples))
            result.setdefault('cpu_percent',100*result['cpu_seconds']/result['elapsed_seconds'])
    finally:
        observer.stop()
        save(ROOT/'results/activity'/(name+'.json'),observer.samples)
        (work/'release').touch()
        subprocess.run(['docker','stop','-t','3',container],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        logs=subprocess.run(['docker','logs',container],capture_output=True,text=True).stdout
        # Docker logs may write engine stderr to the CLI stderr stream.
        p=subprocess.run(['docker','logs',container],capture_output=True,text=True)
        logdir=ROOT/'results/logs';logdir.mkdir(exist_ok=True)
        (logdir/(name+'.log')).write_text(redact(p.stdout+p.stderr))
        save(ROOT/'results/runs'/(name+'.json'),result);save(ROOT/'results/samples'/(name+'.json'),samples)
        subprocess.run(['docker','rm',container],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        if product in ('flink','debezium','debezium_bulk','estuary'):
            with connect() as c:
                slots=c.execute('SELECT slot_name FROM pg_replication_slots WHERE NOT active AND slot_name LIKE %s',(name+'%',)).fetchall()
                for slot, in slots: c.execute('SELECT pg_drop_replication_slot(%s)',(slot,))
                pubs=c.execute('SELECT pubname FROM pg_publication WHERE pubname LIKE %s',(name+'%',)).fetchall()
                for pub, in pubs: c.execute(f'DROP PUBLICATION IF EXISTS {pub}')
    print(json.dumps(result),flush=True)
    return result
def run(product,dataset,parts,rep,timeout,route="pg-pg"):
    with (ROOT/'measurement.lock').open('w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        return _run(product,dataset,parts,rep,timeout,route)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('product',choices=IMAGES);p.add_argument('--dataset',default='smoke',choices=['smoke','narrow','narrow10m','wide']);p.add_argument('--parts',type=int,choices=[1,4,16],default=1);p.add_argument('--rep',type=int,default=0);p.add_argument('--timeout',type=int,default=600);p.add_argument('--route',choices=['pg-pg','pg-ch'],default='pg-pg');a=p.parse_args()
    run(a.product,a.dataset,a.parts,a.rep,a.timeout,a.route)
