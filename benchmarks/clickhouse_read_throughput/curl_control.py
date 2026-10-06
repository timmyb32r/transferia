#!/usr/bin/env python3
"""C/libcurl HTTP throughput ceiling, outside the Rust decoder rankings."""
import pathlib,json,time,subprocess,os,sys,urllib.parse,threading
root=pathlib.Path.home()/'ch-extraction-20261006';base=json.load(open(root/'private/base.json'));sys.path.insert(0,'/tmp');from ch_control import sql
cols=[r.split('\t')[0]for r in sql('DESCRIBE db1.cb_w105_10m').decode().splitlines()];query='SELECT '+','.join('`'+n+'`'for n in cols)+' FROM db1.cb_w105_10m'
(root/'curl-version.txt').write_text(subprocess.check_output(['curl','--version'],text=True))
def quoted(v):return '"'+str(v).replace('\\','\\\\').replace('"','\\"')+'"'
for fmt in ['ArrowStream','Parquet']:
 tag=f'curl-105-{fmt}-none';out=root/'results'/f'{tag}.json'
 if out.exists():continue
 observations=[];cpu=0.;peak=0;index=0;epoch=time.time();start=time.monotonic();error=None
 while time.monotonic()-start<60:
  index+=1;qid=f'ch-extraction-{tag}-{index}';params=dict(query_id=qid,max_threads=16,wait_end_of_query=0,enable_http_compression=0,output_format_arrow_compression_method='none',output_format_parquet_compression_method='none',output_format_parquet_row_group_size=250000,output_format_arrow_string_as_string=1,output_format_parquet_string_as_string=1)
  url='https://'+base['host']+':8443/?'+urllib.parse.urlencode(params)
  config=root/'private/curl.conf';config.write_text('\n'.join(['url = '+quoted(url),'cacert = '+quoted(base['ca'])]+['header = '+quoted(k+': '+v)for k,v in {'X-ClickHouse-User':base['user'],'X-ClickHouse-Key':base['password'],'X-ClickHouse-Database':base['database'],'Accept-Encoding':'identity'}.items()]+['silent','show-error','fail','http1.1','noproxy = "*"','max-time = 600','output = "/dev/null"','write-out = '+quoted('{"transfer_s":%{time_total},"bytes":%{size_download},"http_code":%{http_code},"headers_s":%{time_starttransfer}}')])+'\n');os.chmod(config,0o600)
  body=root/'private/curl-query.sql';body.write_text(query+' FORMAT '+fmt);os.chmod(body,0o600);t=time.monotonic()
  stdout=root/'results'/f'{tag}.stdout'
  with open(root/'results'/f'{tag}.log','ab')as err,open(stdout,'w')as output:
   proc=subprocess.Popen(['curl','--config',str(config),'--data-binary','@'+str(body)],stdout=output,stderr=err)
   timer=threading.Timer(610,proc.kill);timer.start()
   try:
    _,status,usage=os.wait4(proc.pid,0);proc.returncode=os.waitstatus_to_exitcode(status)
   finally:timer.cancel()
  if proc.returncode:error=f'exit {proc.returncode}; see log';break
  obs=json.loads(stdout.read_text());obs.update(query_id=qid,wall_s=time.monotonic()-t);observations.append(obs);cpu+=usage.ru_utime+usage.ru_stime;peak=max(peak,usage.ru_maxrss*1024)
 elapsed=time.monotonic()-start;rows=len(observations)*10000000
 result=dict(cell=dict(width=105,format=fmt+'-curl',codec='none',http_codec='none',mode='raw',threads=16,decode_threads=1),exit_code=0 if error is None else proc.returncode,rows=rows,cycles=len(observations),wall_s=elapsed,cpu_s=cpu,peak_rss_bytes=peak,peak_rss_scope='kernel per-child maxrss',observations=observations,bytes=sum(o['bytes']for o in observations),started_epoch=epoch,finished_epoch=time.time(),error=error,measurement_scope='C/libcurl completed HTTP responses, no decoding; process/TLS setup per query included')
 if error is None:result.update(rows_per_s=rows/elapsed,rows_per_cpu_s=rows/cpu,mib_per_s=result['bytes']/elapsed/1048576)
 out.write_text(json.dumps(result,indent=2));print(tag,error or result['rows_per_s'],flush=True)
