#!/usr/bin/env python3
"""Sequential release-client controller. Credentials never enter result files."""
import json,pathlib,subprocess,time,random,os,sys,argparse
p=argparse.ArgumentParser();p.add_argument('phase',choices=['repeat','controls','verify','final','robustness','confirm_row_group']);args=p.parse_args()
root=pathlib.Path.home()/'ch-extraction-20261006';base=json.load(open(root/'private/base.json'));sys.path.insert(0,'/tmp');from ch_control import sql
binary=str(root/'bin'/('probe-v1' if args.phase=='repeat' else 'probe-v2'))
description=[json.loads(line)for line in sql('DESCRIBE db1.cb_w105_10m FORMAT JSONEachRow').decode().splitlines()]
types={column['name']:column['type']for column in description}
assert all(types.get(name)=="DateTime('UTC')"for name in ['EventTime','ClientEventTime','LocalEventTime']), 'Canonical source timezone contract changed'
cols=[column['name']for column in description]
projections={10:['WatchID','JavaEnable','Title','EventTime','CounterID','ClientIP','RegionID','UserID','URL','Referer'],50:cols[:50],105:cols}
def cell(width=105,fmt='ArrowStream',codec='zstd',mode='decode',**kw):return dict(width=width,format=fmt,codec=codec,http_codec='none',mode=mode,threads=16,decode_threads=8)|kw
cells=[]
if args.phase=='repeat':
 for file in sorted((root/'results').glob('r1-*.json')):
  x=json.load(open(file));c=x['cell']
  if x.get('exit_code')!=0 or c['http_codec']!='none' or c['codec']not in ['none','lz4','zstd'] or c['format']not in ['ArrowStream','Parquet','NativeTCP']:continue
  if c['width']==105 and c['mode']=='decode':cells.append(c)
 for fmt in ['ArrowStream','Parquet']:
  candidates=[json.load(open(f))for f in (root/'results').glob('r1-*.json')]
  candidates=[x for x in candidates if x.get('exit_code')==0 and x['cell']['width']==105 and x['cell']['format']==fmt and x['cell']['mode']=='raw' and x['cell']['http_codec']=='none' and x['cell']['codec']in['none','lz4','zstd']]
  if candidates:cells.append(max(candidates,key=lambda x:x['rows_per_s'])['cell'])
 for width in [10,50]:
  for fmt in ['ArrowStream','Parquet','NativeTCP']:
   candidates=[json.load(open(f))for f in (root/'results').glob('r1-*.json')];candidates=[x for x in candidates if x.get('exit_code')==0 and x['cell']['width']==width and x['cell']['format']==fmt and x['cell']['mode']=='decode' and x['cell']['http_codec']=='none' and x['cell']['codec']in['none','lz4','zstd']]
   if candidates:
    best=max(x['rows_per_s']for x in candidates)
    cells.extend(x['cell']for x in candidates if x['rows_per_s']>=best*.95)
elif args.phase=='controls':
 for w in [10,50,105]:
  for m in ['raw','decode']:cells.append(cell(w,'Parquet',mode=m,format_level=1))
 for fmt in ['ArrowStream','Parquet','RowBinaryWithNamesAndTypes','CSVWithNames','JSONEachRow']:
  cells.append(cell(fmt=fmt,codec='none',mode='raw',http_codec='lz4',http_level=0))
 for fmt in ['ArrowStream','Parquet']:
  for codec in ['none','lz4','zstd']:cells.append(cell(fmt=fmt,codec=codec,binary_strings=True))
 for codec in ['none','lz4','zstd']:
  cells.append(cell(codec=codec,parallel_arrow=True));cells.append(cell(codec=codec,parallel_arrow=True,binary_strings=True))
 for codec in ['none','lz4','zstd']:
  for mode in ['raw','decode']:cells.append(cell(fmt='Arrow',codec=codec,mode=mode))
 for fmt in ['ArrowStream','Parquet']:
  for wait in [False,True]:cells.append(cell(fmt=fmt,codec='none',mode='raw',server_wait=wait))
 for hc in ['none','lz4','zstd','gzip']:cells.append(cell(fmt='Native',codec='none',mode='raw',http_codec=hc,http_level=0 if hc=='lz4' else 1))
 for codec in ['none','lz4','zstd','snappy','zlib']:cells.append(cell(fmt='ORC',codec=codec,mode='raw'))
 for fmt in ['ArrowStream','Parquet']:
  for threads in [8,32]:cells.append(cell(fmt=fmt,threads=threads,binary_strings=True,parallel_arrow=fmt=='ArrowStream'))
 for decoders in [1,4,16]:
  for fmt in ['ArrowStream','Parquet']:cells.append(cell(fmt=fmt,decode_threads=decoders,binary_strings=True,parallel_arrow=fmt=='ArrowStream'))
 for fmt in ['ArrowStream','Parquet']:
  for codec in ['lz4','zstd']:cells.append(cell(fmt=fmt,codec=codec,binary_strings=True,block_rows=65409,preferred_block_bytes=0,parallel_arrow=fmt=='ArrowStream'))
 for batches in [65536,250000]:
  for parallel in [False,True]:cells.append(cell(binary_strings=True,parallel_arrow=parallel,sql_settings=f' SETTINGS output_format_arrow_record_batch_size={batches}'))
 for codec,mode in [('none','raw'),('zstd','decode')]:cells.append(cell(fmt='Parquet',codec=codec,mode=mode,binary_strings=True,sql_settings=' SETTINGS output_format_parquet_max_dictionary_size=0'))
 for groups in [62500,1000000]:cells.append(cell(fmt='Parquet',row_group_rows=groups,binary_strings=True))
elif args.phase=='final':
 import statistics
 groups={}
 for file in (root/'results').glob('*.json'):
  if file.name.startswith(('warmup','verify','production','cpp','final')):continue
  x=json.load(open(file));c=x.get('cell',{})
  if x.get('exit_code')!=0 or c.get('mode')!='decode' or c.get('format')not in ['ArrowStream','Parquet','NativeTCP','Arrow']:continue
  groups.setdefault(json.dumps(c,sort_keys=True),[]).append(x['rows_per_s'])
 for w in [10,50,105]:
  for fmt in ['ArrowStream','Parquet','NativeTCP','Arrow']:
   candidates=[(statistics.median(rates),json.loads(key),len(rates))for key,rates in groups.items()if json.loads(key)['width']==w and json.loads(key)['format']==fmt]
   if not candidates:continue
   _,best,n=max(candidates,key=lambda x:x[0])
   if w==105 and fmt in ['ArrowStream','Arrow','Parquet'] and not best.get('binary_strings',False):
    best=best|dict(binary_strings=True);n=len(groups.get(json.dumps(best,sort_keys=True),[]))
   if fmt in ['ArrowStream','Arrow']:
    if w!=105:continue
    best=best|dict(restore_temporal_types=True);n=0
   if n<2:cells.extend([best]*(2-n))
elif args.phase=='confirm_row_group':
 cells.append(cell(fmt='Parquet',binary_strings=True,row_group_rows=62500))
 for codec in ['lz4','zstd']:
  cells.append(cell(fmt='Parquet',codec=codec,binary_strings=True,sql_settings=' SETTINGS output_format_parquet_write_page_index=0, output_format_parquet_write_bloom_filter=0'))
elif args.phase=='robustness':
 for _ in range(2):
  cells.append(cell(fmt='Parquet',binary_strings=True))
  cells.append(cell(fmt='Parquet',binary_strings=True,sql_settings=' SETTINGS output_format_parquet_max_dictionary_size=0'))
else:
 for fmt in ['NativeTCP','ArrowStream','Parquet','Arrow']:
  cells.append(cell(fmt=fmt,verify_values=True,binary_strings=True,restore_temporal_types=True))
 cells.append(cell(verify_values=True,binary_strings=True,parallel_arrow=True,restore_temporal_types=True))
random.Random(20261007).shuffle(cells);(root/f'schedule-{args.phase}.json').write_text(json.dumps(cells,indent=2))
def run(c,seconds,tag):
 out=root/'results'/f'{tag}.json'
 if out.exists():return
 cfg=base|c|dict(query='SELECT '+','.join('`'+n+'`' for n in projections[c['width']])+' FROM db1.cb_w105_10m'+c.get('sql_settings',''),seconds=seconds,expected_rows=10000000)
 private=root/'private/current.json';private.write_text(json.dumps(cfg));os.chmod(private,0o600)
 log=root/'results'/f'{tag}.log';tmp=root/'results'/f'{tag}.stdout';samples=[];t=time.monotonic();epoch=time.time()
 with open(log,'w')as err,open(tmp,'w')as stdout:
  proc=subprocess.Popen([binary,str(private)],stdout=stdout,stderr=err)
  while True:
   pid,status,usage=os.wait4(proc.pid,os.WNOHANG)
   if pid:proc.returncode=os.waitstatus_to_exitcode(status);break
   try:
    s=pathlib.Path(f'/proc/{proc.pid}/stat').read_text().split();samples.append(dict(elapsed=time.monotonic()-t,rss=int(s[23])*os.sysconf('SC_PAGE_SIZE'),cpu_s=(int(s[13])+int(s[14]))/os.sysconf('SC_CLK_TCK')))
   except FileNotFoundError:pass
   if time.monotonic()-t>seconds+600:proc.kill()
   time.sleep(.5)
 result=dict(cell=c,exit_code=proc.returncode,samples=samples,started_epoch=epoch,finished_epoch=time.time(),requested_seconds=seconds,binary_version='v1'if args.phase=='repeat'else'v2')
 if proc.returncode==0:
  result.update(json.loads(tmp.read_text()));result['cpu_s']=usage.ru_utime+usage.ru_stime;result['rows_per_cpu_s']=result['rows']/result['cpu_s'];result['sampled_peak_rss_bytes']=max((s['rss']for s in samples),default=0);result['peak_rss_bytes']=usage.ru_maxrss*1024;result['peak_rss_scope']='kernel per-child maxrss'
 else:result['error']=log.read_text()[-1500:]
 out.write_text(json.dumps(result,indent=2))
for i,c in enumerate(cells):
 tag=f'{args.phase}-{i:03d}';print(time.strftime('%FT%T'),tag,json.dumps(c),flush=True)
 if args.phase=='verify':run(c,1,tag)
 elif args.phase in ['repeat','final','robustness','confirm_row_group']:run(c,120,tag)
 else:run(c,60,tag)
(root/f'{args.phase}-complete.json').write_text(json.dumps(dict(finished_epoch=time.time(),cells=len(cells))))
