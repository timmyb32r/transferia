#!/usr/bin/env python3
"""Orchestration only: all timed I/O and decoding execute in release Rust."""
import json,pathlib,subprocess,time,random,os,sys,resource
root=pathlib.Path.home()/'ch-extraction-20261006';base=json.load(open(root/'private/base.json'));sys.path.insert(0,'/tmp');from ch_control import sql
binary=str(pathlib.Path.home()/'cursor/transferia/target/release/examples/extraction_probe')
cols=[r.split('\t')[0] for r in sql('DESCRIBE db1.cb_w105_10m').decode().splitlines()]
w10=['WatchID','JavaEnable','Title','EventTime','CounterID','ClientIP','RegionID','UserID','URL','Referer']
projections={10:w10,50:cols[:50],105:cols}
cells=[]
for width in projections:
 for fmt in ['ArrowStream','Parquet']:
  for codec in ['none','lz4','zstd']:
   for mode in ['raw','decode']: cells.append(dict(width=width,format=fmt,codec=codec,http_codec='none',mode=mode,threads=16,decode_threads=8))
 for fmt in ['RowBinaryWithNamesAndTypes','CSVWithNames','JSONEachRow']:
  for hc in ['none','lz4','zstd','gzip']:cells.append(dict(width=width,format=fmt,codec='none',http_codec=hc,mode='raw',threads=16,decode_threads=8))
 for fmt in ['ArrowStream','Parquet']:
  for hc in ['lz4','zstd']:cells.append(dict(width=width,format=fmt,codec='none',http_codec=hc,mode='raw',threads=16,decode_threads=8))
for width in projections:
 for codec in ['none','lz4','zstd']:cells.append(dict(width=width,format='NativeTCP',codec=codec,http_codec='none',mode='decode',threads=16,decode_threads=8))
 if width!=105:continue
 for codec in ['snappy','gzip','brotli']:
  for mode in ['raw','decode']:cells.append(dict(width=width,format='Parquet',codec=codec,http_codec='none',mode=mode,threads=16,decode_threads=8))
random.Random(20261006).shuffle(cells)
(root/'results').mkdir(exist_ok=True);(root/'schedule.json').write_text(json.dumps(cells,indent=2))
def run(cell,seconds,tag):
 cfg=base|cell|dict(query='SELECT '+','.join('`'+n+'`' for n in projections[cell['width']])+' FROM db1.cb_w105_10m',seconds=seconds,expected_rows=10000000)
 private=root/'private/current.json';private.write_text(json.dumps(cfg));os.chmod(private,0o600)
 out=root/'results'/f'{tag}.json';log=root/'results'/f'{tag}.log';tmp=root/'results'/f'{tag}.stdout'
 with open(log,'w') as err,open(tmp,'w') as stdout:
  usage_before=resource.getrusage(resource.RUSAGE_CHILDREN);t=time.monotonic();p=subprocess.Popen([binary,str(private)],stdout=stdout,stderr=err);samples=[]
  while p.poll() is None:
   try:
    stat=pathlib.Path(f'/proc/{p.pid}/stat').read_text().split();rss=int(stat[23])*os.sysconf('SC_PAGE_SIZE');cpu=(int(stat[13])+int(stat[14]))/os.sysconf('SC_CLK_TCK');samples.append(dict(elapsed=time.monotonic()-t,rss=rss,cpu_s=cpu))
   except FileNotFoundError: pass
   if time.monotonic()-t>seconds+1800:p.terminate()
   time.sleep(.5)
 rc=p.returncode
 result=dict(cell=cell,exit_code=rc,samples=samples)
 if rc==0:
  result.update(json.loads(tmp.read_text()));result['peak_rss_bytes']=max((x['rss']for x in samples),default=0);usage_after=resource.getrusage(resource.RUSAGE_CHILDREN);result['cpu_s']=(usage_after.ru_utime+usage_after.ru_stime)-(usage_before.ru_utime+usage_before.ru_stime)
  if result['cpu_s']:result['rows_per_cpu_s']=result['rows']/result['cpu_s']
 else:result['error']=log.read_text()[-1500:]
 out.write_text(json.dumps(result,indent=2));return rc
for rep in range(1):
 for i,cell in enumerate(cells):
  tag=f"r{rep+1}-{cell['width']}-{cell['format']}-{cell['codec']}-{cell['http_codec']}-{cell['mode']}"
  if(root/'results'/f'{tag}.json').exists():continue
  print(time.strftime('%FT%T'),tag,flush=True)
  if rep==0:
   rc=run(cell,3,'warmup-'+tag)
   if rc:
    (root/'results'/f'{tag}.json').write_text(json.dumps(dict(cell=cell,status='unsupported_or_failed',warmup_exit=rc)));continue
  run(cell,120,tag)
