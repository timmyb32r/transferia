#!/usr/bin/env python3
"""Full release Transferia source -> benchmark discard, including startup."""
import pathlib,json,time,subprocess,os,threading,statistics
root=pathlib.Path.home()/'ch-extraction-20261006';base=json.load(open(root/'private/base.json'));binary=root/'bin/transferia-fixed'
profiles=[dict(format=fmt,codec=codec,threads=16,decode_threads=8,row_group_rows=250000)for fmt,codec in [('parquet','zstd'),('parquet','lz4'),('native','zstd'),('native','lz4'),('native','none')]]
groups={}
for file in (root/'results').glob('*.json'):
 if file.name.startswith(('warmup','verify','production')):continue
 x=json.load(open(file));c=x.get('cell',{})
 if x.get('exit_code')!=0 or c.get('width')!=105 or c.get('format')!='Parquet' or c.get('mode')!='decode' or c.get('codec')not in ['lz4','zstd'] or c.get('http_codec')!='none' or not c.get('binary_strings') or c.get('format_level')is not None or c.get('sql_settings') or c.get('block_rows')is not None or c.get('preferred_block_bytes')is not None:continue
 key=(c['codec'],c['threads'],c['decode_threads'],c.get('row_group_rows',250000));groups.setdefault(key,[]).append(x['rows_per_s'])
if groups:
 codec,threads,decoders,group_rows=max(groups,key=lambda k:statistics.median(groups[k]));selected=dict(format='parquet',codec=codec,threads=threads,decode_threads=decoders,row_group_rows=group_rows)
 if selected not in profiles:profiles.append(selected)
(root/'schedule-production.json').write_text(json.dumps(profiles,indent=2))
for profile in profiles:
 fmt=profile['format'];codec=profile['codec'];threads=profile['threads'];decoders=profile['decode_threads'];group_rows=profile['row_group_rows']
 tag=f'production-105-{fmt}-{codec}-t{threads}-d{decoders}-g{group_rows}';out=root/'results'/f'{tag}.json'
 if out.exists():continue
 observations=[];cpu=0.;peak=0;start=time.monotonic();epoch=time.time();index=0;error=None
 while time.monotonic()-start<120:
  index+=1;identity=f'ch-extraction-{fmt}-{codec}-{index}-{time.time_ns()}';state=root/'private'/identity
  reader=dict(type=fmt,compression=codec,max_threads=threads)
  if fmt=='parquet':reader.update(row_group_rows=group_rows,decode_threads=decoders,max_response_bytes=16*1024**3)
  source=dict(hosts=[base['host']],port=9440,http_port=8443,trusted_plaintext=False,tls_ca_file=base['ca'],username=base['user'],password=base['password'],tables=dict(type='selected',rules=[dict(include='db1.cb_w105_10m')]),batch_rows=65409,snapshot_reader=reader,unsupported_types='fail',connect_timeout_ms=30000,request_timeout_ms=600000)
  source['installation']=dict(type='on_premise',**{key:source.pop(key)for key in ['hosts','port','http_port','trusted_plaintext','tls_ca_file']})
  cfg=dict(delivery_id=identity,delivery_name='ClickHouse extraction benchmark',delivery_type='batch',durable_storage=dict(type='local_file',path=str(state)),source=dict(clickhouse=source),sink=dict(discard={}),middlewares=[],pipeline_memory_limit_bytes=16*1024**3,metrics=dict(interval_ms=1000,per_partition=True))
  config=root/'private/production.json';config.write_text(json.dumps(cfg));os.chmod(config,0o600);t=time.monotonic()
  with open(root/'results'/f'{tag}.log','ab')as log:
   proc=subprocess.Popen([str(binary),'--config',str(config)],stdout=log,stderr=log)
   timer=threading.Timer(600,proc.kill);timer.start()
   try:
    _,status,usage=os.wait4(proc.pid,0);proc.returncode=os.waitstatus_to_exitcode(status)
   finally:timer.cancel()
  if proc.returncode:error=f'exit {proc.returncode}; see log';break
  cpu+=usage.ru_utime+usage.ru_stime;peak=max(peak,usage.ru_maxrss*1024);observations.append(dict(cycle=index,wall_s=time.monotonic()-t))
 elapsed=time.monotonic()-start;rows=len(observations)*10000000
 result=dict(cell=dict(width=105,format='Production-'+fmt,codec=codec,http_codec='none',mode='decode',threads=threads,decode_threads=decoders,row_group_rows=group_rows,binary_strings=fmt=='parquet'),exit_code=0 if error is None else proc.returncode,rows=rows,cycles=len(observations),wall_s=elapsed,cpu_s=cpu,peak_rss_bytes=peak,peak_rss_scope='kernel per-process maxrss',observations=observations,started_epoch=epoch,finished_epoch=time.time(),error=error,measurement_scope='complete finite CLI, discovery, pipeline and discard; SQL count verified separately',binary_stat=dict(size=binary.stat().st_size,mtime=binary.stat().st_mtime))
 if error is None:result.update(rows_per_s=rows/elapsed,rows_per_cpu_s=rows/cpu)
 out.write_text(json.dumps(result,indent=2));print(tag,error or result['rows_per_s'],flush=True)
