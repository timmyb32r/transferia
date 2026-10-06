import json,ssl,urllib.request,sys,os,pathlib
root=pathlib.Path.home()/'ch-extraction-20261006';root.mkdir(exist_ok=True);(root/'private').mkdir(exist_ok=True);os.chmod(root/'private',0o700)
pw=json.load(open(pathlib.Path.home()/'benchmark-20260921/private/connections.json'))['password']
host='vla-njm09gqas15eplpr.db.yandex.net';ca='/usr/local/share/ca-certificates/YandexInternalRootCA.crt'
cfg=dict(host=host,password=pw,user='user1',database='db1',ca=ca)
p=root/'private/base.json';p.write_text(json.dumps(cfg));os.chmod(p,0o600)
def sql(query,data=None):
 req=urllib.request.Request('https://'+host+':8443/?'+urllib.parse.urlencode({'query':query}) if data is not None else 'https://'+host+':8443/',data=data if data is not None else query.encode(),headers={'X-ClickHouse-User':'user1','X-ClickHouse-Key':pw,'X-ClickHouse-Database':'db1'})
 try: return urllib.request.urlopen(req,context=ssl.create_default_context(cafile=ca),timeout=1800).read()
 except urllib.error.HTTPError as e: raise RuntimeError(e.read().decode()[:2000]) from None
if __name__=='__main__':
 if sys.argv[1]=='sql': print(sql(sys.stdin.read()).decode())
 elif sys.argv[1]=='insert': print(sql('INSERT INTO db1.cb_w105_10m FORMAT Parquet',open(sys.argv[2],'rb').read()).decode())
