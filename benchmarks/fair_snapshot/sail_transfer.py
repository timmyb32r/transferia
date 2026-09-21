"""Sail engine with official ConnectorX or benchmark ADBC source + shared sink."""
import json
import sys
import socket
import subprocess
import time
import urllib.parse
from pathlib import Path
from pysail.spark import SparkConnectServer
from pysail.spark.datasource.jdbc import JdbcDataSource
from pyspark.sql import SparkSession
from sail_adapters import AdbcSource, ArrowSink

cfg=json.load(open(sys.argv[1]))
# Both sources share this transport. Both the local and remote legs use TLS; the
# remote PostgreSQL connection requires TLS, trusted CA and hostname validation.
with socket.socket() as listener:
    listener.bind(('127.0.0.1',0))
    proxy_port=listener.getsockname()[1]
with socket.socket() as listener:
    listener.bind(('127.0.0.1',0))
    upstream_port=listener.getsockname()[1]
proxy_config=Path(sys.argv[1]).with_suffix('.stunnel.conf')
proxy_config.write_text(f"""foreground = yes
pid =
debug = err
[postgres-local]
client = no
accept = 127.0.0.1:{proxy_port}
connect = 127.0.0.1:{upstream_port}
protocol = pgsql
cert = /work/private/sail-local-cert.pem
key = /work/private/sail-local-key.pem
TIMEOUTclose = 0
[postgres-source]
client = yes
accept = 127.0.0.1:{upstream_port}
connect = {cfg['source_host']}:6432
protocol = pgsql
verifyChain = yes
CAfile = /cert/RootCA.crt
checkHost = {cfg.get('tls_expected_host',cfg['source_host'])}
sni = {cfg['source_host']}
sslVersionMin = TLSv1.2
TIMEOUTclose = 0
""")
proxy=subprocess.Popen(['/usr/bin/stunnel',str(proxy_config)])
for attempt in range(100):
    if proxy.poll() is not None:raise RuntimeError('TLS proxy exited before readiness')
    try:
        with socket.create_connection(('127.0.0.1',proxy_port),timeout=.1):pass
        break
    except OSError:time.sleep(.05)
else:
    proxy.terminate();proxy.wait();raise RuntimeError('TLS proxy readiness timeout')
# Disable channel binding because local and upstream TLS certificates differ.
# ConnectorX validates the pinned local CA; stunnel validates upstream CA+host.
base=f'localhost:{proxy_port}/db1?sslrootcert=/work/private/sail-local-cert.pem&channel_binding=disable'
cfg['source_url']='jdbc:postgresql://'+base+'&sslmode=require'
cfg['source_uri']='postgresql://user1:'+urllib.parse.quote(cfg['password'],safe='')+'@'+base+'&sslmode=verify-full'
print('SAIL_TLS_PROXY verifyChain=yes checkHost='+cfg['source_host'],flush=True)
server=SparkConnectServer('127.0.0.1',0)
server.start()
_,port=server.listening_address
spark=SparkSession.builder.remote(f'sc://127.0.0.1:{port}').getOrCreate()
try:
    spark.dataSource.register(ArrowSink)
    if cfg['source_kind']=='jdbc':
        spark.dataSource.register(JdbcDataSource)
        reader=(spark.read.format('jdbc').option('url',cfg['source_url'])
            .option('dbtable',cfg['source_table']).option('user','user1')
            .option('password',cfg['password']).option('fetchsize',8192))
        if cfg['parts']>1:
            reader=reader.option('partitionColumn','id').option('lowerBound',1).option('upperBound',cfg['rows']+1).option('numPartitions',cfg['parts'])
    else:
        spark.dataSource.register(AdbcSource)
        reader=(spark.read.format('benchmark_adbc').option('uri',cfg['source_uri'])
            .option('table',cfg['source_table']).option('rows',cfg['rows']).option('parts',cfg['parts']))
    df=reader.load()
    print('SAIL_SOURCE_SCHEMA',df.schema.simpleString(),flush=True)
    print('SAIL_PLAN',flush=True)
    df.explain()
    writer=(df.write.format('benchmark_arrow_sink').mode('append')
        .option('route',cfg['route']).option('rows',cfg['rows']).option('table',cfg['target_name']))
    if cfg['route']=='pg-pg':
        writer=writer.option('uri',cfg['target_uri'])
    else:
        writer=writer.option('host',cfg['ch_host']).option('password',cfg['password'])
    writer.save()
finally:
    spark.stop()
    server.stop()
    proxy.terminate()
    proxy.wait(timeout=10)
