#!/usr/bin/env python3
"""Evidence-only report tables and control figures; no imputation of failures."""
import argparse,json,pathlib,statistics
p=argparse.ArgumentParser();p.add_argument('directory',type=pathlib.Path);a=p.parse_args();root=a.directory
rows=json.load(open(root/'summary.json'))['summary'];raw={f.name:json.load(open(f))for f in (root/'raw').glob('*.json')if not f.name.startswith('warmup')};proof=json.load(open(root/'query-validation.json'))
def rate(v):return f'{v/1e6:.3f}'if v is not None else '—'
def profile(r):
 flags=[]
 for key,label in [('binary_strings','binary'),('parallel_arrow','parallel IPC'),('restore_temporal_types','UTC restored'),('server_wait','wait=1')]:
  if r[key]:flags.append(label)
 for key,label in [('format_level','level'),('http_level','HTTP level'),('block_rows','block'),('preferred_block_bytes','preferred bytes')]:
  if r[key]is not None:flags.append(f'{label}={r[key]}')
 if r['row_group_rows']!=250000:flags.append(f"group={r['row_group_rows']}")
 if r['sql_settings']:flags.append(r['sql_settings'].strip())
 return ', '.join(flags)or'default wire profile'
def table(data):
 text=['| Profile | Mode | Columns | Codec / HTTP | M rows/s | M rows/client CPU-s | Client cores | RSS MiB | M rows/server CPU-s | n | Seconds/run |','|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|']
 for r in data:
  name=f"{r['format']} · t{r['threads']}/d{r['decode_threads']} · {profile(r)}".replace('|','\\|')
  text.append(f"| {name} | {r['mode']} | {r['width']} | {r['codec']} / {r['http_codec']} | {rate(r['median_rows_per_s'])} | {rate(r['median_rows_per_cpu_s'])} | {r['median_client_cores']:.2f} | {r['peak_rss_mib']:.0f} | {rate(r['median_server_rows_per_cpu_s'])} | {r['n']} | {r['min_wall_s']:.0f}–{r['max_wall_s']:.0f} |")
 return '\n'.join(text)
def primary(r):return r['mode']=='decode'and r['format']in['ArrowStream','Parquet','NativeTCP']and r['http_codec']=='none'and r['codec']in['none','lz4','zstd']and profile(r)=='default wire profile'
sections=['# Measured tables','## Primary transport decoding',table(sorted([r for r in rows if primary(r)],key=lambda r:(r['width'],r['format'],r['codec'])))]
final=[]
for fmt in ['ArrowStream','Arrow','Parquet','NativeTCP']:
 candidates=[r for r in rows if r['width']==105 and r['format']==fmt and r['mode']=='decode'and r['n']>=2 and (fmt not in ['ArrowStream','Arrow','Parquet']or r['binary_strings'])and (fmt not in ['ArrowStream','Arrow']or r['restore_temporal_types'])]
 if candidates:final.append(max(candidates,key=lambda r:r['median_rows_per_s']))
sections+=['## Confirmed wide decoded finalists',table(sorted(final,key=lambda r:-r['median_rows_per_s'])),'## Official C++ / C transport controls',table([r for r in rows if 'C++'in r['format']or'-curl'in r['format']]),'## Full production source → discard',table([r for r in rows if r['format'].startswith('Production-')]),'## Every completed profile',table(rows)]
(root/'TABLES.md').write_text('\n\n'.join(sections)+'\n')
import matplotlib;matplotlib.use('Agg');import matplotlib.pyplot as plt
plt.rcParams.update({'font.family':'DejaVu Sans','axes.spines.top':False,'axes.spines.right':False,'axes.spines.left':False})
def figure(data,metric,name,title,label,scale=1e6):
 if not data:return
 data=sorted(data,key=lambda r:r[metric]or 0);fig,ax=plt.subplots(figsize=(12,max(4,len(data)*.43+1.8)));fig.patch.set_facecolor('#f8fafc');ax.set_facecolor('#f8fafc')
 labels=[('Arrow file'if r['format']=='Arrow'else r['format'])+' · '+r['codec']+' · t'+str(r['threads'])+('/d'+str(r['decode_threads'])if 'parquet'in r['format'].lower()or r['parallel_arrow']else(' · no decode'if r['mode']=='raw'else' · serial decode'))+(' · rg'+f"{r['row_group_rows']/1000:g}k"if 'parquet'in r['format'].lower()else'')for r in data];values=[(r[metric]or 0)/scale for r in data]
 ax.spines['bottom'].set_color('#cbd5e1')
 colors=['#0d8290'if 'parquet'in r['format'].lower()else'#c69b48'if'native'in r['format'].lower()else'#7896b3'for r in data]
 errors=[[max(0,r[metric]-r['min_rows_per_s'])/scale for r in data],[max(0,r['max_rows_per_s']-r[metric])/scale for r in data]]if metric=='median_rows_per_s'else None
 ax.barh(labels,values,color=colors,xerr=errors,error_kw=dict(ecolor='#475569',capsize=3));ax.set_xlim(0,max((r['max_rows_per_s']/scale if metric=='median_rows_per_s'else v)for r,v in zip(data,values))*1.28);ax.grid(axis='x',alpha=.2);ax.set_axisbelow(True);ax.set_xlabel(label)
 for i,v in enumerate(values):ax.text(data[i]['max_rows_per_s']/scale if metric=='median_rows_per_s'else v,i,f' {v:.3f} (n={data[i]["n"]})',va='center')
 ax.set_title(title,fontweight='bold',fontsize=16,pad=16);fig.text(.015,.015,'10M × 105; complete queries. See report for boundaries, compression levels, restored schema and repetitions.',fontsize=9,color='#64748b');fig.tight_layout(rect=[0,.05,1,1]);fig.savefig(root/name,dpi=170);plt.close(fig)
figure(final,'median_rows_per_s','tuned-wide-throughput.png','Confirmed wide decoding · explicit UTC restoration for IPC','Million rows / second')
figure(final,'median_rows_per_cpu_s','tuned-wide-cpu.png','Confirmed wide decoding · client CPU efficiency','Million rows / aggregate client CPU-second')
figure(final,'median_server_rows_per_cpu_s','tuned-wide-server-cpu.png','Confirmed wide decoding · source query CPU efficiency','Million rows / aggregate server CPU-second')
figure(final,'peak_rss_mib','tuned-wide-memory.png','Confirmed wide decoding · peak client RSS','GiB',1024)
figure(final,'server_peak_query_memory_mib','tuned-wide-server-memory.png','Confirmed wide decoding · source peak query memory','GiB per query, not whole-server RSS',1024)
figure([r for r in rows if r['width']==105 and (r['format'].startswith('NativeTCP-C++')or(r['format']=='NativeTCP'and primary(r)))],'median_rows_per_s','native-client-controls.png','Native clients · different decode/output contracts','Million rows / second')
figure([r for r in rows if r['format'].startswith('Production-')],'median_rows_per_s','production-throughput.png','Full Transferia source → discard · startup included','Million rows / second')
figure([r for r in rows if r['format'].startswith('Production-')],'median_rows_per_cpu_s','production-cpu.png','Full Transferia source → discard · client CPU efficiency','Million rows / aggregate client CPU-second')
figure([r for r in rows if r['format'].startswith('Production-')],'peak_rss_mib','production-memory.png','Full Transferia source → discard · peak client RSS','GiB',1024)
figure([r for r in rows if r['width']==105 and r['mode']=='raw'and r['codec']=='none'and r['http_codec']=='none'and r['format']in['ArrowStream','Parquet','ArrowStream-curl','Parquet-curl']and not r['binary_strings']and not r['server_wait']and not r['sql_settings']and r['format_level']is None and r['http_level']is None],'median_rows_per_s','http-client-controls.png','Uncompressed HTTP clients · complete raw responses, no decoding','Million rows / second')
failed=json.load(open(root/'summary.json'))['failed'];(root/'FAILURES.md').write_text('# Explicit unsuccessful / unverified cells\n\n'+('\n'.join('- `'+f['file']+'`: '+str(f.get('status')or f.get('error')) for f in failed)or'None.')+'\n')

archived=[]
for directory in ['setup-failures','before-native-fix']:
 for file in sorted((root/'raw'/directory).rglob('*.json')):
  result=json.load(open(file))
  if result.get('exit_code')!=0:
   relative=file.relative_to(root).as_posix();archived.append(f'- [{relative}]({relative}): {result.get("error", "see log")}')
if archived:
 with (root/'FAILURES.md').open('a')as output:
  output.write('\n## Archived preparation / pre-fix runtime failures\n\nThese earlier attempts are preserved and excluded from final performance charts. Final CLI measurements use the corrected configuration and rebuilt binary.\n\n'+'\n'.join(archived)+'\n')
