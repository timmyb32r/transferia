#!/usr/bin/env python3
"""Analyze completed release probes. Never treat failures as zero throughput."""
import argparse,csv,json,pathlib,statistics
p=argparse.ArgumentParser();p.add_argument('results',type=pathlib.Path);p.add_argument('output',type=pathlib.Path);p.add_argument('--validation',type=pathlib.Path);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=True)
proof={r['file']:r for r in json.load(open(a.validation))['validations']} if a.validation else {}
groups={};failed=[]
for file in sorted(a.results.glob('*.json')):
 if file.name.startswith(('warmup','verify')):continue
 x=json.load(open(file))
 if x.get('exit_code')!=0 or not x.get('rows_per_s'):
  failed.append(dict(file=file.name,cell=x.get('cell'),error=x.get('error'),status=x.get('status')));continue
 if a.validation and file.name not in proof:
  failed.append(dict(file=file.name,cell=x.get('cell'),error='missing authoritative server validation',status='missing_server_validation'));continue
 if file.name in proof:
  v=proof[file.name];x['server_validation']=v
  if v['status']!='verified_rows':failed.append(dict(file=file.name,cell=x.get('cell'),error='server row correlation failed',status=v['status']));continue
 c=x['cell'];key=(c['width'],c['format'],c['codec'],c['http_codec'],c['mode'],c['threads'],c['decode_threads'],c.get('sql_settings',''),c.get('binary_strings',False),c.get('parallel_arrow',False),c.get('http_level'),c.get('format_level'),c.get('row_group_rows',250000),c.get('block_rows'),c.get('preferred_block_bytes'),c.get('server_wait',False),c.get('restore_temporal_types',False))
 groups.setdefault(key,[]).append(x)
rows=[]
for key,runs in groups.items():
 rates=[x['rows_per_s'] for x in runs];cpu_rates=[x['rows_per_cpu_s'] for x in runs if x.get('rows_per_cpu_s')];r=dict(zip(['width','format','codec','http_codec','mode','threads','decode_threads','sql_settings','binary_strings','parallel_arrow','http_level','format_level','row_group_rows','block_rows','preferred_block_bytes','server_wait','restore_temporal_types'],key));r.update(n=len(runs),min_wall_s=min(x["wall_s"]for x in runs),max_wall_s=max(x["wall_s"]for x in runs),median_rows_per_s=statistics.median(rates),min_rows_per_s=min(rates),max_rows_per_s=max(rates),median_rows_per_cpu_s=statistics.median(cpu_rates)if cpu_rates else None,peak_rss_mib=max(x['peak_rss_bytes']for x in runs)/1048576,median_client_cores=statistics.median(x['cpu_s']/x['wall_s']for x in runs),median_mib_per_s=statistics.median(x['mib_per_s']for x in runs if x.get('mib_per_s')is not None)if any('mib_per_s'in x for x in runs)else None)
 verified=[x for x in runs if x.get('server_validation',{}).get('status')=='verified_rows']
 r.update(median_server_rows_per_cpu_s=statistics.median(x['server_validation']['server_rows_per_cpu_s']for x in verified)if verified else None,median_total_rows_per_cpu_s=statistics.median(x['rows']/(x['cpu_s']+x['server_validation']['server_cpu_s'])for x in verified)if verified else None,server_peak_query_memory_mib=max((x['server_validation']['server_peak_query_memory_bytes']/1048576 for x in verified),default=None))
 observations=[o for x in runs for o in x.get('observations',[])]
 def observed(name):
  values=[o[name]for o in observations if o.get(name)is not None]
  return statistics.median(values)if values else None
 batches=[o['decoded_rows']/o['decoded_batches']for o in observations if o.get('decoded_batches')and o.get('decoded_rows')]
 r.update(median_query_payload_mib=(observed('bytes')/1048576 if observed('bytes')is not None else None),median_headers_s=observed('headers_s'),median_body_finished_s=observed('body_finished_s'),median_post_transfer_decode_s=observed('post_transfer_decode_s'),median_mean_rows_per_batch=statistics.median(batches)if batches else None,median_query_duration_cv=statistics.median([x['server_validation']['query_duration_cv']for x in verified if x['server_validation'].get('query_duration_cv')is not None])if any(x['server_validation'].get('query_duration_cv')is not None for x in verified)else None)
 rows.append(r)
rows.sort(key=lambda x:(x['width'],x['mode'],-x['median_rows_per_s']))
(a.output/'summary.json').write_text(json.dumps(dict(summary=rows,failed=failed),indent=2))
if rows:
 with open(a.output/'summary.csv','w')as f:w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
try:
 import matplotlib;matplotlib.use('Agg');import matplotlib.pyplot as plt
except ImportError: raise SystemExit('matplotlib required for charts; summary saved')
plt.rcParams.update({'axes.spines.top':False,'axes.spines.right':False,'axes.spines.left':False,'axes.edgecolor':'#cbd5e1','text.color':'#0f172a','axes.labelcolor':'#475569','xtick.color':'#475569','ytick.color':'#0f172a'})
for metric,title,scale,label in [('median_rows_per_s','Transport decode throughput',1e6,'Million decoded rows / second'),('median_rows_per_cpu_s','Client CPU efficiency',1e6,'Million decoded rows / aggregate CPU-second'),('peak_rss_mib','Client peak RSS',1024,'GiB (sampled baseline; kernel peak for later runs)')]:
 fig,axes=plt.subplots(1,3,figsize=(18,7));fig.patch.set_facecolor('#f8fafc')
 for ax,width in zip(axes,[10,50,105]):
  data=[r for r in rows if r['width']==width and r['mode']=='decode' and r['http_codec']=='none' and r['codec']in['none','lz4','zstd'] and not r['sql_settings'] and not r['binary_strings'] and not r['parallel_arrow'] and r['format_level'] is None and r['http_level'] is None and r['row_group_rows']==250000 and r['block_rows'] is None and r['preferred_block_bytes'] is None and not r['server_wait'] and not r['restore_temporal_types'] and r['format'] in ['ArrowStream','Parquet','NativeTCP']]
  data.sort(key=lambda r:r[metric]or 0);labels=[r['format']+' · '+r['codec']for r in data];values=[(r[metric]or 0)/scale for r in data];colors=['#0d8290' if r['format']=='Parquet' else '#7896b3' if r['format']=='ArrowStream' else '#b59a6a'for r in data]
  ax.set_facecolor('#f8fafc');ax.barh(labels,values,color=colors,xerr=([[ (r['median_rows_per_s']-r['min_rows_per_s'])/scale for r in data ],[ (r['max_rows_per_s']-r['median_rows_per_s'])/scale for r in data ]] if metric=='median_rows_per_s' else None),error_kw=dict(ecolor='#475569',capsize=3));ax.set_xlim(0,max(values,default=1)*1.3);ax.set_title(f'{width} columns');ax.set_xlabel(label);ax.grid(axis='x',alpha=.2);ax.set_axisbelow(True)
  for i,v in enumerate(values):ax.text(v,i,f' {v:.2f} (n={data[i]["n"]})',va='center',fontsize=9)
 limit=max(ax.get_xlim()[1]for ax in axes)
 for ax in axes:ax.set_xlim(0,limit)
 fig.suptitle(title+' | ClickBench wire batches → Rust',fontsize=19,fontweight='bold')
 fig.text(.015,.015,'Complete repeated queries; 120s minimum per measured run. Client CPU excludes database CPU. Compression levels differ in primary sweep; level-1 controls reported separately.',fontsize=9,color='#64748b')
 fig.tight_layout(rect=[0,.045,1,.94]);fig.savefig(a.output/(metric+'.png'),dpi=160);plt.close(fig)
fig,axes=plt.subplots(1,3,figsize=(19,11));fig.patch.set_facecolor('#f8fafc')
for ax,width in zip(axes,[10,50,105]):
 data=[r for r in rows if r['width']==width and r['mode']=='raw' and not r['format'].endswith('-curl') and not r['sql_settings'] and not r['binary_strings'] and not r['parallel_arrow'] and r['format_level'] is None and r['http_level'] is None and r['row_group_rows']==250000 and r['block_rows'] is None and r['preferred_block_bytes'] is None and not r['server_wait'] and not r['restore_temporal_types']];data.sort(key=lambda r:r['median_rows_per_s']);labels=[r['format']+' · '+(r['codec'] if r['http_codec']=='none' else 'HTTP '+r['http_codec'])for r in data];values=[r['median_rows_per_s']/1e6 for r in data];ax.set_facecolor('#f8fafc');ax.barh(labels,values,color='#0d8290');ax.set_xlim(0,max(values,default=1)*1.2);ax.set_title(f'{width} columns');ax.set_xlabel('Million source rows / second');ax.grid(axis='x',alpha=.2);ax.set_axisbelow(True);ax.tick_params(axis='y',labelsize=9)
limit=max(ax.get_xlim()[1]for ax in axes)
for ax in axes:ax.set_xlim(0,limit)
fig.suptitle('Raw HTTP extraction | Completed responses, without format decoding',fontsize=18,fontweight='bold');fig.text(.015,.012,'Complete queries; baseline runs ≥120s, format diagnostics ≥60s. Different encodings move different byte counts per row. Never compare these bars to decoded throughput as if the contracts were identical.',fontsize=9,color='#64748b');fig.tight_layout(rect=[0,.04,1,.94]);fig.savefig(a.output/'raw-throughput.png',dpi=160);plt.close(fig)
