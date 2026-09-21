"""Sail compatibility probe; no substitute writer is silently installed."""
import json
import sys
from pysail.spark import SparkConnectServer
from pysail.spark.datasource.jdbc import JdbcDataSource
from pyspark.sql import SparkSession

cfg=json.load(open(sys.argv[1]))
server=SparkConnectServer('127.0.0.1',0)
server.start()
_,port=server.listening_address
spark=SparkSession.builder.remote(f'sc://127.0.0.1:{port}').getOrCreate()
try:
    spark.dataSource.register(JdbcDataSource)
    # Capability probe: reach the stock writer independently of source TLS support.
    df=spark.range(1)
    print('SAIL_NATIVE_WRITER_ATTEMPT',cfg['route'],flush=True)
    (df.write.format('jdbc').option('url',cfg['target_url'])
        .option('dbtable',cfg['target_table']).option('user','user1')
        .option('password',cfg['password']).mode('append').save())
finally:
    spark.stop()
    server.stop()
