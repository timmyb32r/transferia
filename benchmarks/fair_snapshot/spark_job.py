"""Spark's native JDBC reader/writer; exact common PK predicates, no Python rows."""
import json,sys
from pyspark.sql import SparkSession
cfg=json.load(open(sys.argv[1]))
spark=SparkSession.builder.appName('fair21').getOrCreate()
properties={'user':'user1','password':cfg['password'],'driver':'org.postgresql.Driver','fetchsize':'8192','batchsize':'1000','isolationLevel':'READ_COMMITTED'}
n=cfg['rows']; p=cfg['parts']
predicates=[f'id >= {1+i*n//p} AND id < {1+(i+1)*n//p}' for i in range(p)]
df=spark.read.jdbc(cfg['source_url'],cfg['source_table'],predicates=predicates,properties=properties)
df.write.jdbc(cfg['target_url'],cfg['target_table'],mode='append',properties={**properties,'driver':cfg.get('target_driver','org.postgresql.Driver'),'batchsize':str(cfg.get('batch_size',1000))})
spark.stop()
