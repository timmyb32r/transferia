#!/usr/bin/env python3
"""Regenerate descriptive tables and figures from verified, non-smoke runs.

Min/max whiskers describe observed runs, not confidence intervals. No throughput
is assigned to failed or excluded runs. All client resource scopes include the
complete adapter process tree, but exclude managed database hosts.
"""
import argparse,csv,json,statistics,os,tempfile
from collections import defaultdict
from pathlib import Path
os.environ.setdefault('MPLCONFIGDIR',str(Path(tempfile.gettempdir())/'fair21-mpl'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
LABELS={'debezium_bulk':'Debezium bulk-tuned⁴','sail_jdbc':'Sail JDBC + sink⁷','sail_adbc':'Sail ADBC buffered + sink⁷','rust':'Transferia Rust Auto','rust_ranges':'Transferia Rust exact PK¹','go':'Transferia Go · typed path','seatunnel':'SeaTunnel','sling':'Sling²','datax':'DataX · default polling','airbyte':'Airbyte connector pair³','flink':'Flink CDC','debezium':'Debezium + Kafka + JDBC','inlong':'InLong Sort','meltano':'Meltano²','spark':'Spark','sqoop':'Sqoop import + export','estuary':'Estuary local preview² ³'}
COLORS={1:'#8fa5be',4:'#087f8c'}
FIELDS=['rows_per_second','rows_per_cpu_second','rows_per_allocated_cpu_second','cpu_seconds','elapsed_seconds','peak_rss_bytes','cgroup_peak_bytes','cpu_percent']
def compact(x):
    return f'{x/1e6:.2f}M' if x>=1e6 else (f'{x/1e3:.1f}k' if x>=1e3 else f'{x:.1f}')
def main():
    p=argparse.ArgumentParser();p.add_argument('directory',type=Path);a=p.parse_args();root=a.directory
    runs=[json.loads(s) for s in (root/'runs.jsonl').read_text().splitlines() if s.strip()]
    sail_manifest=root/'sail-followup.json'
    if sail_manifest.exists():
        for entry in json.loads(sail_manifest.read_text()):
            # Fresh Rust/Spark controls have their own paired report, not new
            # repeats silently pooled into the historical comparison.
            if entry['product'] in ('sail_jdbc','sail_adbc'):
                runs.append(dict(entry['result'],repetition=entry['followup_repetition']))
    groups=defaultdict(list)
    for r in runs:
        if r['repetition']>0 and r['status']=='verified' and not r.get('exclude_from_comparison'):
            groups[r['route'],r['dataset'],r['product'],r['parts_requested']].append(r)
    summaries=[]
    for (route,dataset,tool,parts),rr in sorted(groups.items()):
        row={'route':route,'dataset':dataset,'product':tool,'parts_requested':parts,'n':len(rr),'configuration_variants':' | '.join(sorted({r['variant'] for r in rr})),'run_names':[r['name'] for r in rr]}
        for field in FIELDS:
            if field=='rows_per_allocated_cpu_second':
                if any(r['cpu_affinity']!='0-15' for r in rr):raise RuntimeError('allocated CPU metric requires the declared 16-CPU budget')
                vv=[r['rows_per_second']/16 for r in rr]
            else:vv=[r[field] for r in rr]
            for suffix,fn in [('median',statistics.median),('min',min),('max',max)]:row[field+'_'+suffix]=fn(vv)
        summaries.append(row)
    (root/'summary.json').write_text(json.dumps(summaries,indent=2)+'\n')
    if summaries:
        with (root/'summary.csv').open('w') as f:
            w=csv.DictWriter(f,fieldnames=[x for x in summaries[0] if x!='run_names']);w.writeheader();w.writerows({k:v for k,v in s.items() if k!='run_names'} for s in summaries)
    plt.rcParams.update({'font.family':'DejaVu Sans','font.size':10,'axes.spines.top':False,'axes.spines.right':False,'axes.spines.left':False,'axes.edgecolor':'#d3dce6','axes.labelcolor':'#334155','text.color':'#0f172a','xtick.color':'#64748b','ytick.color':'#334155','figure.facecolor':'#f8fafc','axes.facecolor':'#f8fafc','savefig.facecolor':'#f8fafc'})
    charts=root/'charts';charts.mkdir(exist_ok=True)
    for route in ('pg-pg','pg-ch'):
        tools=sorted({k[2] for k in groups if k[0]==route and k[1]=='narrow'},key=lambda t:statistics.median([r['rows_per_second'] for r in groups.get((route,'narrow',t,4),groups.get((route,'narrow',t,1),[]))]),reverse=True)
        if not tools:continue
        for field,title,xlabel,stem in [('rows_per_second','Snapshot throughput','Verified rows / second','throughput'),('rows_per_cpu_second','Client CPU efficiency','Verified rows / aggregate CPU-second','cpu-efficiency')]:
            fig,axes=plt.subplots(1,2,figsize=(17,max(7,len(tools)*.53+2.1)),sharey=True)
            fig.subplots_adjust(left=.21,right=.97,top=.82,bottom=.13,wspace=.19)
            for ax,dataset in zip(axes,('narrow','wide')):
                for i,tool in enumerate(tools):
                    for parts,offset in [(1,-.17),(4,.17)]:
                        rr=groups.get((route,dataset,tool,parts),[])
                        if not rr:continue
                        vv=[r[field] for r in rr];m=statistics.median(vv)
                        ax.barh(i+offset,m,height=.29,color=COLORS[parts],label=f'{parts} part'+('s' if parts>1 else '') if i==0 else None)
                        ax.errorbar(m,i+offset,xerr=[[m-min(vv)],[max(vv)-m]],fmt='none',ecolor='#334155',capsize=2,lw=.8)
                        ax.annotate(compact(m),(max(vv),i+offset),xytext=(4,0),textcoords='offset points',va='center',fontsize=8,color='#334155')
                ax.set_title(('Narrow · 96 B payload' if dataset=='narrow' else 'Wide · 1024 B payload'),loc='left',fontsize=12,pad=14)
                ax.set_xlabel(xlabel);ax.grid(axis='x',alpha=.18);ax.set_axisbelow(True);ax.tick_params(axis='y',length=0);ax.set_yticks(range(len(tools)),[LABELS[t] for t in tools]);ax.set_ylim(len(tools)-.5,-.5);ax.set_xlim(0,ax.get_xlim()[1]*1.16)
                ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x,p:compact(x)))
            fig.suptitle(f'{title}  |  {"PostgreSQL → PostgreSQL" if route=="pg-pg" else "PostgreSQL → ClickHouse"}',x=.03,y=.97,ha='left',fontsize=21,fontweight='bold')
            fig.text(.03,.915,'1,000,000 rows · finite-job startup included · 16 logical CPUs / 24 GiB · median; whiskers = observed min–max',fontsize=11,color='#64748b')
            axes[1].legend(frameon=False,loc='lower right');fig.text(.03,.035,'¹ Benchmark-only exact-PK override   ² External parallel pipelines   ³ Connector/local-preview path, not managed service\nClient CPU excludes database hosts. Verified runs only; actual repetition counts are in summary.csv. Physical CTID and PK ranges differ. ⁴ Kafka buffering/compression variant.\n⁷ Sail + benchmark Arrow sink and shared TLS proxy; later block, see SAIL.md.',fontsize=9,color='#64748b')
            for ext in ('png','svg'):fig.savefig(charts/f'{route}-{stem}.{ext}',dpi=180)
            plt.close(fig)
        fig,axes=plt.subplots(1,2,figsize=(16,max(7,len(tools)*.45+2)),sharey=True);fig.subplots_adjust(left=.23,right=.96,top=.82,bottom=.14,wspace=.2)
        for ax,field,scale,title in [(axes[0],'peak_rss_bytes',1024**3,'Peak summed RSS · GiB'),(axes[1],'cpu_percent',100,'Average occupied logical CPUs')]:
            for i,t in enumerate(tools):
                for d,off,color in [('narrow',-.17,'#8fa5be'),('wide',.17,'#087f8c')]:
                    rr=groups.get((route,d,t,4),[])
                    if not rr:continue
                    vv=[r[field]/scale for r in rr];m=statistics.median(vv)
                    ax.barh(i+off,m,height=.29,color=color,label=d if i==0 else None)
                    label_x=max(vv)
                    if field=='peak_rss_bytes':
                        charged=statistics.median(r['cgroup_peak_bytes']/scale for r in rr)
                        ax.scatter(charged,i+off,marker='D',s=22,facecolors='none',edgecolors='#b45309',zorder=4,label='cgroup peak incl. cache' if i==0 and d=='narrow' else None)
                        label_x=max(label_x,charged)
                    ax.errorbar(m,i+off,xerr=[[m-min(vv)],[max(vv)-m]],fmt='none',ecolor='#334155',capsize=2,lw=.8)
                    label = f'RSS {m:.2f}' if field == 'peak_rss_bytes' else f'{m:.2f}'
                    ax.annotate(label,(label_x,i+off),xytext=(4,0),textcoords='offset points',va='center',fontsize=8)
            ax.set_title(title,loc='left');ax.grid(axis='x',alpha=.18);ax.set_axisbelow(True);ax.tick_params(axis='y',length=0);ax.set_yticks(range(len(tools)),[LABELS[t] for t in tools]);ax.set_ylim(len(tools)-.5,-.5);ax.set_xlim(0,ax.get_xlim()[1]*1.15)
        fig.suptitle(f'Client resources · 4 parts  |  {route.upper()}',x=.03,y=.97,ha='left',fontsize=21,fontweight='bold')
        fig.text(.03,.91,'1,000,000 rows · all engine/helper processes in one cgroup · median; whiskers = observed min–max',fontsize=11,color='#64748b')
        axes[0].legend(frameon=False,fontsize=8,loc='upper right');axes[1].legend(frameon=False);fig.text(.03,.035,'RSS sampled every 100 ms; shared pages may be counted more than once. CPU includes user + system time.\nDatabase hosts are outside this measurement. ¹ Exact-PK experiment  ² External pipelines  ³ Local connector/preview path  ⁴ Kafka bulk configuration.\n⁷ Sail + benchmark Arrow sink and shared TLS proxy; later block, see SAIL.md.',fontsize=9,color='#64748b')
        for ext in ('png','svg'):fig.savefig(charts/f'{route}-resources.{ext}',dpi=180)
        plt.close(fig)
    for route in ('pg-pg','pg-ch'):
        large_tools={k[2] for k in groups if k[0]==route and k[1]=='narrow10m' and k[3]==4}
        if not large_tools:continue
        tools=sorted({k[2] for k in groups if k[0]==route and k[1]=='narrow' and k[3]==4},key=lambda t:statistics.median([r['rows_per_second'] for r in groups.get((route,'narrow10m',t,4),groups.get((route,'narrow',t,4),[]))]),reverse=True)
        fig,ax=plt.subplots(figsize=(13,max(6,len(tools)*.5+2)))
        fig.subplots_adjust(left=.29,right=.93,top=.81,bottom=.14)
        for i,t in enumerate(tools):
            for dataset,off,color,label in [('narrow',-.17,'#8fa5be','1M rows'),('narrow10m',.17,'#087f8c','10M rows')]:
                rr=groups.get((route,dataset,t,4),[])
                if not rr:
                    ax.annotate('no verified 10M result',(0,i+off),xytext=(4,0),textcoords='offset points',va='center',fontsize=8,color='#9a3412');continue
                vv=[r['rows_per_second'] for r in rr];m=statistics.median(vv)
                ax.barh(i+off,m,height=.29,color=color,label=label if i==0 else None)
                ax.errorbar(m,i+off,xerr=[[m-min(vv)],[max(vv)-m]],fmt='none',ecolor='#334155',capsize=2,lw=.8)
                ax.annotate(compact(m),(max(vv),i+off),xytext=(4,0),textcoords='offset points',va='center',fontsize=8)
        ax.set_yticks(range(len(tools)),[LABELS[t]+(' · large heap⁵' if t=='flink' else '') for t in tools]);ax.set_ylim(len(tools)-.5,-.5);ax.tick_params(axis='y',length=0)
        ax.set_xlim(0,ax.get_xlim()[1]*1.16);ax.grid(axis='x',alpha=.18);ax.set_axisbelow(True);ax.set_xlabel('Verified rows / second');ax.legend(frameon=False,loc='lower right')
        ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x,p:compact(x)))
        fig.suptitle(f'Large-table follow-up · 4 parts  |  {route.upper()}',x=.03,y=.97,ha='left',fontsize=20,fontweight='bold')
        fig.text(.03,.905,'96 B payload · startup included · same 16-CPU / 24-GiB budget',fontsize=11,color='#64748b')
        footer='10M follow-ups are exploratory single runs unless summary.csv records repeats. Missing/failed runs are not zeros.\n¹ Exact-PK experiment  ² External pipelines  ³ Local connector/preview  ⁴ Kafka bulk configuration\n⁷ Sail + benchmark Arrow sink and TLS proxy; later block, see SAIL.md.'
        if route=='pg-pg':footer+='\n⁵ Flink 10M TaskManager: 18 GiB / managed fraction 0.1; 1M: 8 GiB. Same 24-GiB cgroup.'
        fig.text(.03,.035,footer,fontsize=8,color='#64748b')
        for ext in ('png','svg'):fig.savefig(charts/f'{route}-scale.{ext}',dpi=180)
        plt.close(fig)
    if any(k[3]==16 for k in groups):
        fig,axes=plt.subplots(1,2,figsize=(13,6))
        fig.subplots_adjust(left=.08,right=.96,top=.76,bottom=.2,wspace=.28)
        for ax,field,title in [(axes[0],'rows_per_second','Delivery throughput · rows/s'),(axes[1],'rows_per_cpu_second','Client efficiency · rows/CPU-s')]:
            for i,parts in enumerate((1,4,16)):
                for tool,offset,color,label in [('debezium',-.18,'#8fa5be','Baseline'),('debezium_bulk',.18,'#087f8c','Bulk-tuned')]:
                    rr=groups.get(('pg-pg','narrow',tool,parts),[])
                    if not rr:continue
                    vv=[r[field] for r in rr];m=statistics.median(vv)
                    ax.bar(i+offset,m,width=.32,color=color,label=label if i==0 else None)
                    ax.errorbar(i+offset,m,yerr=[[m-min(vv)],[max(vv)-m]],fmt='none',ecolor='#334155',capsize=3,lw=.8)
                    ax.annotate(compact(m),(i+offset,max(vv)),xytext=(0,5),textcoords='offset points',ha='center',fontsize=9)
            ax.set_xticks(range(3),['1 part','4 parts','16 parts']);ax.set_title(title,loc='left',pad=15)
            ax.set_ylim(0,ax.get_ylim()[1]*1.12);ax.grid(axis='y',alpha=.18);ax.set_axisbelow(True)
            ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x,p:compact(x)))
        axes[1].legend(frameon=False,loc='upper right')
        fig.suptitle('Debezium · native snapshot chunks + Kafka + JDBC',x=.04,y=.96,ha='left',fontsize=20,fontweight='bold')
        fig.text(.04,.885,'1M narrow rows · same 16-CPU / 24-GiB client budget · median; whiskers = observed min–max',fontsize=10,color='#64748b')
        fig.text(.04,.045,'P16 is a separate scaling experiment, not a ranking against competitors at P4.\nBulk changes buffering + LZ4 together. Client CPU includes broker and Connect; database hosts are excluded.',fontsize=9,color='#64748b')
        for ext in ('png','svg'):fig.savefig(charts/f'debezium-parallelism.{ext}',dpi=180)
        plt.close(fig)
    diagnostic_paths=[root/'datax-poll-probe.json',root/'estuary-delta-probe.json']
    if all(p.exists() for p in diagnostic_paths):
        fig,axes=plt.subplots(1,2,figsize=(15,6))
        fig.subplots_adjust(left=.07,right=.96,top=.76,bottom=.23,wspace=.24)
        for ax,tool,path,cases,baseline_label,tuned_label in [
            (axes[0],'datax',diagnostic_paths[0],[('pg-pg','narrow'),('pg-pg','wide'),('pg-ch','narrow'),('pg-ch','wide')],'Default 10s completion poll','100ms completion poll'),
            (axes[1],'estuary',diagnostic_paths[1],[('pg-pg','narrow'),('pg-pg','wide')],'Standard keyed preview','delta_updates preview')]:
            diagnostics=json.loads(path.read_text())
            for i,(route,dataset) in enumerate(cases):
                control=groups.get((route,dataset,tool,4),[])
                tuned=[x['result'] for x in diagnostics if x['dataset']==dataset and x.get('route','pg-pg')==route and x['result']['status']=='verified']
                for rr,offset,color,label in [(control,-.18,'#8fa5be',baseline_label),(tuned,.18,'#087f8c',tuned_label)]:
                    if not rr:continue
                    vv=[r['rows_per_second'] for r in rr];m=statistics.median(vv)
                    ax.bar(i+offset,m,width=.32,color=color,label=label if i==0 else None)
                    ax.errorbar(i+offset,m,yerr=[[m-min(vv)],[max(vv)-m]],fmt='none',ecolor='#334155',capsize=3,lw=.8)
                    ax.annotate(compact(m),(i+offset,max(vv)),xytext=(0,5),textcoords='offset points',ha='center',fontsize=9)
            ax.set_title('DataX' if tool=='datax' else 'Estuary local preview',loc='left',pad=15)
            ax.set_xticks(range(len(cases)),[r.upper()+'\n'+d for r,d in cases])
            ax.set_ylim(0,ax.get_ylim()[1]*1.25);ax.grid(axis='y',alpha=.18);ax.set_axisbelow(True)
            ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x,p:compact(x)))
            ax.set_ylabel('Verified rows / second');ax.legend(frameon=False,fontsize=8,loc='upper left')
        fig.suptitle('Configuration matters · 4 parts, 1M rows',x=.04,y=.96,ha='left',fontsize=21,fontweight='bold')
        fig.text(.04,.885,'Finite jobs including startup · same 16-CPU / 24-GiB budget · median and observed min–max',fontsize=11,color='#64748b')
        fig.text(.04,.035,'Diagnostic blocks were later than the main controls, not temporally paired. See DIAGNOSTICS.md for n and exact settings.\nDataX changes completion polling only; Estuary changes materialization mode. Local preview is not managed Flow.',fontsize=9,color='#64748b')
        for ext in ('png','svg'):fig.savefig(charts/f'configuration-effects.{ext}',dpi=180)
        plt.close(fig)
    print(f'{len(summaries)} measured groups; {sum(s["n"] for s in summaries)} verified runs')
if __name__=='__main__':main()
