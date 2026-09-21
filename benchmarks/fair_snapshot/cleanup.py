#!/usr/bin/env python3
"""Restore borrowed CPU affinity; remove only this campaign's inactive CDC state."""
import fcntl,json,subprocess
from setup import ROOT,connect
if not (ROOT/'pause-campaign').exists():raise RuntimeError('pause campaign scheduling before cleanup')
with (ROOT/'measurement.lock').open('w') as lock:
 fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
 active=subprocess.check_output(['docker','ps','--filter','name=fair21_','--format','{{.Names}}'],text=True).strip()
 if active:raise RuntimeError('benchmark containers are still running; do not restore affinity yet')
 restored=[];affinity_details=[]
 for row in json.loads((ROOT/'results/background-affinity-before.json').read_text()):
  # Docker ignores an empty cpuset update. Restore the effective unrestricted
  # host CPU set explicitly and record that persisted-config distinction.
  requested=row['cpuset'] or __import__('pathlib').Path('/sys/devices/system/cpu/online').read_text().strip()
  subprocess.run(['docker','update','--cpuset-cpus',requested,row['name']],check=True,stdout=subprocess.DEVNULL)
  actual=subprocess.check_output(['docker','inspect','--format','{{.HostConfig.CpusetCpus}}',row['name']],text=True).strip()
  if actual!=requested:raise RuntimeError('affinity restoration failed')
  restored.append(row['name'])
  affinity_details.append({'name':row['name'],'original':row['cpuset'],'restored':actual,'note':'Explicit online CPU set because Docker ignores empty updates' if not row['cpuset'] else 'Original explicit affinity restored'})
 pattern=r'^(flink|debezium|debezium_bulk|estuary)_(smoke|narrow|wide|narrow10m)_p(1|4|16)_r[0-9]+_[0-9]+'
 removed_slots=[];removed_pubs=[]
 with connect() as c:
  slots=c.execute('SELECT slot_name FROM pg_replication_slots WHERE NOT active AND slot_name ~ %s',(pattern,)).fetchall()
  for slot, in slots:
   c.execute('SELECT pg_drop_replication_slot(%s)',(slot,));removed_slots.append(slot)
  pubs=c.execute('SELECT pubname FROM pg_publication WHERE pubname ~ %s',(pattern,)).fetchall()
  for pub, in pubs:
   # Names originate only from our alphanumeric benchmark names.
   if not all(ch.isalnum() or ch=='_' for ch in pub):raise RuntimeError('unexpected publication identifier')
   c.execute('DROP PUBLICATION '+pub);removed_pubs.append(pub)
 result={'restored_background_affinity':restored,'affinity_details':affinity_details,'removed_inactive_slots':removed_slots,'removed_publications':removed_pubs}
 (ROOT/'results/cleanup.json').write_text(json.dumps(result,indent=2)+'\n')
 print(json.dumps(result))
