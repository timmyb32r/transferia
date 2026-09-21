#!/usr/bin/env python3
"""Summarize the later Sail block and its contemporaneous P4 controls."""
import csv
import datetime
import json
import os
import tempfile
import statistics
import sys
from collections import defaultdict
from pathlib import Path
os.environ.setdefault('MPLCONFIGDIR',str(Path(tempfile.gettempdir())/'fair21-mpl'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

root=Path(sys.argv[1])
entries=json.loads((root/'sail-followup.json').read_text())
assert len(entries)==80, 'Final report requires all 80 planned outcomes'
groups=defaultdict(list)
for entry in entries:
    r=entry['result']
    if r['status']=='verified':
        assert r['verified_rows']==(10000000 if r['dataset']=='narrow10m' else 1000000)
        assert r['cpu_affinity']=='0-15' and r['memory_limit_bytes']==24*1024**3
        assert r['cpu_seconds']>0 and r['elapsed_seconds']>0
        assert abs(r['rows_per_second']-r['verified_rows']/r['elapsed_seconds'])<1e-6
        assert abs(r['rows_per_cpu_second']-r['verified_rows']/r['cpu_seconds'])<1e-6
        groups[entry['product'],entry['route'],entry['dataset'],entry['parts']].append(r)
assert len([e for e in entries if e['product'].startswith('sail_')])==52
identities=[(e['product'],e['route'],e['dataset'],e['parts'],e['followup_repetition']) for e in entries]
assert len(identities)==len(set(identities))
ordered=sorted((e['result'] for e in entries),key=lambda r:r['started_utc'])
for a,b in zip(ordered,ordered[1:]):
    assert datetime.datetime.fromisoformat(a['started_utc'])+datetime.timedelta(seconds=a['elapsed_seconds'])<=datetime.datetime.fromisoformat(b['started_utc'])
labels={'sail_jdbc':'Sail JDBC/ConnectorX + sink','sail_adbc':'Sail ADBC buffered + sink','rust':'Rust Auto · fresh control','spark':'Spark · fresh control'}
fields=('rows_per_second','rows_per_cpu_second','cpu_seconds','elapsed_seconds','cpu_percent','peak_rss_bytes','cgroup_peak_bytes')
summaries=[]
for (tool,route,dataset,parts),runs in groups.items():
    row=dict(product=tool,route=route,dataset=dataset,parts=parts,n=len(runs))
    for field in fields:
        values=[r[field] for r in runs]
        for suffix,fn in [('median',statistics.median),('min',min),('max',max)]:row[field+'_'+suffix]=fn(values)
    assert len(runs)==(1 if dataset=='narrow10m' else 3), (tool,route,dataset,parts)
    summaries.append(row)
(root/'sail-summary.json').write_text(json.dumps(summaries,indent=2)+'\n')
with (root/'sail-summary.csv').open('w') as f:
    writer=csv.DictWriter(f,fieldnames=list(summaries[0]));writer.writeheader();writer.writerows(summaries)
index={(r['product'],r['route'],r['dataset'],r['parts']):r for r in summaries}
lines=['# Sail: подробные результаты отдельного блока','','Медианы успешных повторов. 1M — три повтора; 10M/P4 — один exploratory-прогон. Fresh control — Rust/Spark, запущенные вместе с Sail, а не старые медианы. CPU всех клиентских процессов, включая прокси, учтён; CPU серверов БД не входит.','', '| Путь | Приёмник | Данные | Части | n | Строк/с | Строк/CPU-с | Средние CPU | Peak RSS GiB | Cgroup peak GiB |','|---|---|---|---:|---:|---:|---:|---:|---:|---:|']
for row in sorted(summaries,key=lambda r:(r['route'],r['dataset'],r['product'],r['parts'])):
    lines.append(f"| {labels[row['product']]} | {row['route']} | {row['dataset']} | {row['parts']} | {row['n']} | {row['rows_per_second_median']:,.0f} | {row['rows_per_cpu_second_median']:,.0f} | {row['cpu_percent_median']/100:.2f} | {row['peak_rss_bytes_median']/1024**3:.2f} | {row['cgroup_peak_bytes_median']/1024**3:.2f} |")
failed=[e for e in entries if e['result']['status']!='verified']
lines+=['','## Покрытие','',f"Успешно {len(entries)-len(failed)} из {len(entries)} запущенных. План: 48 Sail 1M + 4 Sail 10M + 24 контрольных 1M + 4 контрольных 10M = 80. Отсутствующие/неудачные случаи не имеют значения throughput.",'']
for e in failed:lines.append(f"- {e['result']['name']}: {e['result'].get('error','failed')} ({e['result']['elapsed_seconds']:.1f} с)")
(root/'SAIL-TABLES.md').write_text('\n'.join(lines)+'\n')
plt.rcParams.update({'font.family':'DejaVu Sans','font.size':10,'axes.spines.top':False,'axes.spines.right':False,'axes.spines.left':False,'figure.facecolor':'#f8fafc','axes.facecolor':'#f8fafc','savefig.facecolor':'#f8fafc','text.color':'#0f172a','axes.labelcolor':'#334155','xtick.color':'#64748b','ytick.color':'#334155'})
colors={'rust':'#087f8c','sail_adbc':'#2773b8','sail_jdbc':'#8064ad','spark':'#8fa5be'}
for route in ('pg-pg','pg-ch'):
    fig,axes=plt.subplots(3,3,figsize=(18,11))
    fig.subplots_adjust(left=.22,right=.95,top=.85,bottom=.11,wspace=.32,hspace=.55)
    for row,dataset in enumerate(('narrow','wide','narrow10m')):
        tools=list(labels)
        tools.sort(key=lambda tool:index.get((tool,route,dataset,4),{}).get('rows_per_second_median',-1),reverse=True)
        for col,(field,title,scale) in enumerate((('rows_per_second','Rows / second · higher is better',1),('rows_per_cpu_second','Rows / CPU-second · higher is better',1),('peak_rss_bytes','Peak RSS GiB · lower is better',1024**3))):
            ax=axes[row,col]
            for i,tool in enumerate(tools):
                if (tool,route,dataset,4) not in index:
                    ax.annotate('no verified result',(0,i),xytext=(4,0),textcoords='offset points',va='center',fontsize=9,color='#9a3412')
                    continue
                rr=index[tool,route,dataset,4];value=rr[field+'_median']/scale
                lo,hi=rr[field+'_min']/scale,rr[field+'_max']/scale
                ax.barh(i,value,color=colors[tool],height=.62)
                ax.errorbar(value,i,xerr=[[value-lo],[hi-value]],fmt='none',ecolor='#334155',capsize=2,lw=.8)
                label=f'{value:.2f}' if scale!=1 else (f'{value/1e6:.2f}M' if value>=1e6 else f'{value/1000:.1f}k')
                ax.annotate(label,(hi,i),xytext=(4,0),textcoords='offset points',va='center',fontsize=9)
            ax.set_yticks(range(len(tools)),[labels[t] if col==0 else '' for t in tools]);ax.set_ylim(len(tools)-.5,-.5);ax.tick_params(axis='y',length=0)
            ax.set_xlim(0,ax.get_xlim()[1]*1.3);ax.grid(axis='x',alpha=.18);ax.set_axisbelow(True)
            ax.set_title(('1M narrow · 96 B' if dataset=='narrow' else '1M wide · 1024 B' if dataset=='wide' else '10M narrow · one run')+'\n'+title,loc='left',fontsize=11)
            if scale==1:ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x,p:f'{x/1000:,.0f}k'))
    fig.suptitle('Sail sources and fresh controls  |  '+route.upper(),x=.035,y=.965,ha='left',fontsize=23,fontweight='bold')
    fig.text(.035,.91,'4 parts · same host / 16 CPU / 24 GiB · finite-job startup included · 1M medians, whiskers min–max',fontsize=12,color='#64748b')
    fig.text(.035,.035,'Sail = Rust engine + official ConnectorX or custom ADBC source + common benchmark Arrow sink + verifying TLS proxy.\nSail variants share the proxy; Rust/Spark controls keep their native transports. Client resources only. Sink and proxy costs are included.',fontsize=10,color='#64748b')
    for ext in ('png','svg'):fig.savefig(root/'charts'/f'sail-{route}.{ext}',dpi=180)
    plt.close(fig)
print(f'Sail block: {len(entries)-len(failed)} verified, {len(failed)} failed; {len(groups)} groups')
