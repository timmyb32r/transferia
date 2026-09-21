"""One cgroup contains Kafka, Connect and both Debezium connectors."""
import json,os,pathlib,signal,socket,subprocess,sys,time
cfg=json.load(open(sys.argv[1]));root=pathlib.Path(cfg['directory']);children=[]
logconfig=root/'log4j2.xml'
logconfig.write_text('''<Configuration status="WARN"><Appenders><Console name="console" target="SYSTEM_OUT"><PatternLayout pattern="%d{ISO8601} %-5p %c - %m%n"/></Console></Appenders><Loggers><Logger name="io.debezium.relational.RelationalSnapshotChangeEventSource" level="INFO"/><Logger name="io.debezium.pipeline.source.snapshot" level="INFO"/><Root level="WARN"><AppenderRef ref="console"/></Root></Loggers></Configuration>''')
os.environ['KAFKA_LOG4J_OPTS']='-Dlog4j2.configurationFile='+str(logconfig)
def properties(name,values):
 p=root/name;p.write_text('\n'.join(k+'='+str(v).replace('\\','\\\\').replace('\n','\\n') for k,v in values.items())+'\n');p.chmod(0o600);return str(p)
def wait_port(port,timeout=90):
 deadline=time.monotonic()+timeout
 while time.monotonic()<deadline:
  try:
   with socket.create_connection(('127.0.0.1',port),timeout=1): return
  except OSError: time.sleep(.2)
 raise RuntimeError('service startup deadline exceeded')
broker={'process.roles':'broker,controller','node.id':1,'controller.quorum.voters':'1@127.0.0.1:19093','listeners':'PLAINTEXT://127.0.0.1:19092,CONTROLLER://127.0.0.1:19093','advertised.listeners':'PLAINTEXT://127.0.0.1:19092','listener.security.protocol.map':'PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT','controller.listener.names':'CONTROLLER','inter.broker.listener.name':'PLAINTEXT','log.dirs':str(root/'kafka-data'),'offsets.topic.replication.factor':1,'transaction.state.log.replication.factor':1,'transaction.state.log.min.isr':1,'group.initial.rebalance.delay.ms':0,'num.partitions':cfg['parts'],'auto.create.topics.enable':'true'}
worker={'bootstrap.servers':'127.0.0.1:19092','key.converter':'org.apache.kafka.connect.json.JsonConverter','value.converter':'org.apache.kafka.connect.json.JsonConverter','key.converter.schemas.enable':'true','value.converter.schemas.enable':'true','offset.storage.file.filename':str(root/'offsets'),'offset.flush.interval.ms':1000,'plugin.path':'/kafka/connect','listeners':'http://127.0.0.1:18083','connector.client.config.override.policy':'All'}
try:
 bp=properties('kafka.properties',broker)
 subprocess.run(['/kafka/bin/kafka-storage.sh','format','-t','MkU3OEVBNTcwNTJENDM2Qk','-c',bp],check=True)
 children.append(subprocess.Popen(['/kafka/bin/kafka-server-start.sh',bp]))
 wait_port(19092)
 children.append(subprocess.Popen(['/kafka/bin/connect-standalone.sh',properties('worker.properties',worker),properties('source.properties',cfg['source']),properties('sink.properties',cfg['sink'])]))
 while not (root/'finish').exists():
  if any(p.poll() is not None for p in children): raise RuntimeError('Kafka or Connect exited before completion')
  time.sleep(.1)
finally:
 for p in reversed(children):
  if p.poll() is None: p.terminate()
 for p in reversed(children):
  try:p.wait(15)
  except subprocess.TimeoutExpired:p.kill();p.wait()
