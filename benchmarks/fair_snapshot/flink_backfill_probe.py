#!/usr/bin/env python3
"""One immutable-source diagnostic: skip snapshot backfill, same heap and P4.

This changes consistency semantics on a mutating source. It is deliberately
separate from the primary snapshot configuration and never a production default.
"""
import fcntl
import json
import runner
from runner import ROOT, PRIVATE, _run, redact

original_configure = runner.configure

def configure(product, dataset, parts, name, route):
    commands, variant = original_configure(product, dataset, parts, name, route)
    sql_path = PRIVATE / (name + '.sql')
    sql = sql_path.read_text()
    marker = "'scan.startup.mode'='snapshot',"
    if sql.count(marker) != 1:
        raise RuntimeError('unexpected source SQL shape')
    sql_path.write_text(sql.replace(marker, marker + "\n'scan.incremental.snapshot.backfill.skip'='true',", 1))
    path = PRIVATE / (name + '.json')
    cfg = json.loads(path.read_text())
    cfg['source']['scan.incremental.snapshot.backfill.skip'] = 'true'
    path.write_text(json.dumps(cfg))
    (ROOT / 'results/configs' / path.name).write_text(redact(json.dumps(cfg)))
    return commands, variant + '-diagnostic-skip-backfill'

runner.configure = configure
with (ROOT / 'measurement.lock').open('w') as lock:
    fcntl.flock(lock, fcntl.LOCK_EX)
    result = _run('flink', 'narrow10m', 4, 0, 300, 'pg-pg')
    output = [{'dataset': 'narrow10m', 'parts': 4, 'skip_snapshot_backfill': True, 'result': result}]
    (ROOT / 'results/flink-backfill-probe.json').write_text(json.dumps(output, indent=2) + '\n')
    if result['status'] != 'verified':
        raise RuntimeError('backfill diagnostic failed; inspect before interpreting')
print('Flink immutable-source backfill diagnostic complete; excluded from main ranking.')
