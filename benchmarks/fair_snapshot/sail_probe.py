"""Run bounded native Sail compatibility probes inside the common cgroup."""
import fcntl
import json
import runner
from runner import ROOT, PRIVATE, CONFIG, _run, redact
runner.IMAGES['sail']='transferia/benchmark-sail:0.7.1'

def configure(product,dataset,parts,name,route):
    rows=10000 if dataset=='smoke' else (10000000 if dataset=='narrow10m' else 1000000)
    source=f"jdbc:postgresql://{CONFIG['source_host']}:6432/db1?sslmode=verify-full&sslrootcert=/cert/RootCA.crt"
    target=(f"jdbc:postgresql://{CONFIG['pg_host']}:6432/db1?sslmode=verify-full&sslrootcert=/cert/RootCA.crt" if route=='pg-pg' else f"jdbc:clickhouse://{CONFIG['ch_host']}:8443/db1?ssl=true")
    cfg={'source_url':source,'target_url':target,'source_table':'fair21.'+dataset,'target_table':('fair21.' if route=='pg-pg' else 'db1.')+name,'password':CONFIG['password'],'rows':rows,'parts':parts,'route':route}
    path=PRIVATE/(name+'.json');path.write_text(json.dumps(cfg));path.chmod(0o600)
    (ROOT/'results/configs'/path.name).write_text(redact(json.dumps(cfg)))
    return [['/opt/sail/bin/python','/work/sail_job.py','/work/private/'+path.name]],'native-jdbc-compatibility-probe'

runner.configure=configure
with (ROOT/'measurement.lock').open('w') as lock:
    fcntl.flock(lock,fcntl.LOCK_EX)
    results=[_run('sail','smoke',1,0,120,route) for route in ('pg-pg','pg-ch')]
    (ROOT/'results/sail-native-probe.json').write_text(json.dumps(results,indent=2)+'\n')
