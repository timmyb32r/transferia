#!/usr/bin/env python3
"""Correlate complete response counts with authoritative server QueryFinish.

V2/C++ query IDs match exactly. V1 has no IDs, so its last N identical SQL
queries within the measured process interval are a temporal correlation.
A mismatch is a validation failure, never a zero-throughput result.
"""
import argparse,json,pathlib,statistics,re
p=argparse.ArgumentParser();p.add_argument('results',type=pathlib.Path);p.add_argument('query_log',type=pathlib.Path);p.add_argument('output',type=pathlib.Path);a=p.parse_args()
timestamp_file=a.results.parent/'file-timestamps.json'
captured_timestamps=json.load(open(timestamp_file))if timestamp_file.exists()else{}
queries=[json.loads(line)for line in a.query_log.read_text().splitlines()];by_id={q['query_id']:q for q in queries if q['type']=='QueryFinish'};rows=[]
for file in sorted(a.results.glob('*.json')):
 if file.name.startswith('warmup'):continue
 x=json.load(open(file));c=x.get('cell',{});r=dict(file=file.name,cell=c,status='failed_client' if x.get('exit_code')!=0 else 'pending_server_verification')
 if x.get('exit_code')!=0:rows.append(r);continue
 ids=[o['query_id']for o in x.get('observations',[])if o.get('query_id')]
 if ids:
  matched=[by_id[i]for i in ids if i in by_id];method='exact_query_id'
 else:
  end=x.get('finished_epoch',captured_timestamps.get(file.name,{}).get('mtime_epoch',file.stat().st_mtime));begin=x.get('started_epoch',end-x['wall_s']-2)
  candidates=[q for q in queries if q['type']=='QueryFinish'and begin-2<=int(q['finished_epoch_us'])/1e6<=end+1 and int(q['result_rows'])==10000000]
  fmt=c.get('format','');is_native=fmt in ['NativeTCP','NativeTCP-C++']or fmt=='Production-native'
  if not fmt.startswith('Production-'):
   candidates=[q for q in candidates if (' FORMAT '+fmt in q['query']if not is_native else not re.search(r' FORMAT (Arrow|Parquet|ORC|CSV|JSON|RowBinary)',q['query']))]
  matched=sorted(candidates,key=lambda q:int(q['finished_epoch_us']))[-x['cycles']:];method='temporal_last_N_completed_queries'
 expected=x.get('cycles',0);good=len(matched)==expected and all(int(q['result_rows'])==10000000 and int(q['network_send_bytes'])>0 for q in matched)
 pipeline_rows=None
 if c.get('format','').startswith('Production-'):
  log=file.with_suffix('.log')
  content=re.sub(r'\x1b\[[0-9;]*m','',log.read_text())if log.exists()else''
  pipeline_rows=[int(n)for n in re.findall(r'finite snapshot output row count completed[^\n]*?output_rows=(\d+)',content)]
  good=good and len(pipeline_rows)==expected and all(n==10000000 for n in pipeline_rows)
  r['pipeline_output_rows']=pipeline_rows
 r.update(status='verified_rows'if good else'correlation_failed',method=method,expected_cycles=expected,matched_cycles=len(matched),query_ids=[q['query_id']for q in matched])
 if matched:
  server_cpu=sum((int(q['cpu_user_us'])+int(q['cpu_sys_us']))/1e6 for q in matched)
  r.update(server_cpu_s=server_cpu,server_rows_per_cpu_s=expected*10000000/server_cpu if server_cpu else None,server_peak_query_memory_bytes=max(int(q['memory_usage'])for q in matched),server_wire_bytes=sum(int(q['network_send_bytes'])for q in matched),server_query_duration_s=sum(int(q['query_duration_ms'])/1000 for q in matched),server_network_send_s=sum(int(q.get('network_send_us',0))/1e6 for q in matched))
 if matched:
  for source,target in [('file_read_bytes','server_file_read_bytes'),('file_write_bytes','server_file_write_bytes'),('disk_read_us','server_disk_read_us'),('disk_write_us','server_disk_write_us'),('temp_files','server_temp_files'),('temp_compressed_bytes','server_temp_compressed_bytes'),('temp_uncompressed_bytes','server_temp_uncompressed_bytes')]:r[target]=sum(int(q.get(source,0))for q in matched)
 durations=[o['wall_s']for o in x.get('observations',[])];r.update(query_duration_cv=statistics.pstdev(durations)/statistics.mean(durations)if len(durations)>1 else None,query_cycles=len(durations))
 rows.append(r)
a.output.write_text(json.dumps(dict(validations=rows,counts={status:sum(r['status']==status for r in rows)for status in sorted(set(r['status']for r in rows))}),indent=2))
