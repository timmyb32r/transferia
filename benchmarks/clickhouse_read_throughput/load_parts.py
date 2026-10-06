import json,pathlib,time,sys
sys.path.insert(0,'/tmp');from ch_control import sql
root=pathlib.Path.home()/'ch-extraction-20261006'
for o in json.load(open(root/'parts.json')):
 if o['part']==0:continue
 p=root/'data'/o['name'];marker=root/f"loaded-{o['part']}"
 if marker.exists():continue
 while not p.exists() or p.stat().st_size!=o['bytes']:time.sleep(2)
 print('Loading',o['name'],flush=True);sql('INSERT INTO db1.cb_w105_10m FORMAT Parquet',p.read_bytes());marker.touch()
print(sql('SELECT count() FROM db1.cb_w105_10m').decode(),flush=True)
sql('OPTIMIZE TABLE db1.cb_w105_10m FINAL')
print(sql('SELECT count(),min(EventTime),max(EventTime),sum(length(Title)),sum(length(URL)) FROM db1.cb_w105_10m').decode(),flush=True)
(root/'data-ready').touch()
