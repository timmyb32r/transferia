#!/usr/bin/env python3
"""Version-pinned official C++ Native controls, including local reserialization.

Never use --format Null: server 26.9 suppresses data transmission for it.
"""
import pathlib,json,sys,time,os,subprocess,xml.sax.saxutils,threading
root=pathlib.Path.home()/'ch-extraction-20261006';c=json.load(open(root/'private/base.json'));sys.path.insert(0,'/tmp');from ch_control import sql
esc=xml.sax.saxutils.escape
config=root/'private/cpp.xml';config.write_text('<config>'+''.join(f'<{k}>{esc(str(v))}</{k}>'for k,v in dict(host=c['host'],port=9440,secure='true',user=c['user'],password=c['password'],database=c['database']).items())+'<openSSL><client><caConfig>'+esc(c['ca'])+'</caConfig><verificationMode>strict</verificationMode><extendedVerification>true</extendedVerification><loadDefaultCAFile>true</loadDefaultCAFile><invalidCertificateHandler><name>RejectCertificateHandler</name></invalidCertificateHandler></client></openSSL></config>');os.chmod(config,0o600)
cols=[x.split('\t')[0]for x in sql('DESCRIBE db1.cb_w105_10m').decode().splitlines()];query='SELECT '+','.join('`'+x+'`'for x in cols)+' FROM db1.cb_w105_10m'
for version,folder in [('26.9.5.2','cpp'),('26.7.5.10','cpp-old')]:
 binary=root/folder/'usr/bin/clickhouse'
 for codec in ['none','lz4','zstd']:
  tag=f'cpp-{version}-105-{codec}';out=root/'results'/f'{tag}.json'
  if out.exists():continue
  print(time.strftime('%FT%T'),tag,flush=True)
  def command(qid):return [str(binary),'client','--config-file',str(config),'--query',query,'--format','Native','--query_id',qid,'--max_threads','16','--max_block_size','65409','--preferred_block_size_bytes','0','--compression',str(int(codec!='none')),'--network_compression_method','zstd'if codec=='zstd'else'lz4','--network_zstd_compression_level','1']
  with open(root/'results'/f'{tag}-warmup.log','wb')as err:
   warmup=subprocess.run(command(f'ch-extraction-cpp-warmup-{version}-{codec}'),stdout=subprocess.DEVNULL,stderr=err)
  cell=dict(width=105,format=f'NativeTCP-C++-{version}',codec=codec,http_codec='none',mode='decode',threads=16,decode_threads=1)
  if warmup.returncode:
   out.write_text(json.dumps(dict(cell=cell,exit_code=warmup.returncode,error='C++ warmup failed; see log')));continue
  observations=[];total_cpu=0.;peak=0;start=time.monotonic();epoch=time.time();i=0;error=None
  while time.monotonic()-start<120:
   i+=1;qid=f'ch-extraction-cpp-{version}-{codec}-{i}';t=time.monotonic()
   with open(root/'results'/f'{tag}.log','ab')as err:
    proc=subprocess.Popen(command(qid),stdout=subprocess.DEVNULL,stderr=err)
    timer=threading.Timer(600,proc.kill);timer.start()
    try:
     _,status,usage=os.wait4(proc.pid,0);proc.returncode=os.waitstatus_to_exitcode(status)
    finally:timer.cancel()
   if proc.returncode:error=f'exit {proc.returncode}; see log';break
   total_cpu+=usage.ru_utime+usage.ru_stime;peak=max(peak,usage.ru_maxrss*1024);observations.append(dict(query_id=qid,wall_s=time.monotonic()-t));print(tag,i,observations[-1]['wall_s'],flush=True)
  elapsed=time.monotonic()-start;rows=len(observations)*10000000
  result=dict(cell=cell,exit_code=0 if error is None else proc.returncode,rows=rows,cycles=len(observations),wall_s=elapsed,cpu_s=total_cpu,peak_rss_bytes=peak,peak_rss_scope='kernel per-child maxrss',observations=observations,started_epoch=epoch,finished_epoch=time.time(),error=error,measurement_scope='Native network decode plus client Native reserialization to /dev/null; not Arrow materialization')
  if error is None:result.update(rows_per_s=rows/elapsed,rows_per_cpu_s=rows/total_cpu)
  out.write_text(json.dumps(result,indent=2))
(root/'cpp-complete.json').write_text(json.dumps(dict(finished_epoch=time.time())))
