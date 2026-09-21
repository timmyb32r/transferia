#!/usr/bin/env python3
"""Audit the declared comparison matrix without treating missing cases as zeros."""
import json
import sys
from collections import Counter
from pathlib import Path

root = Path(sys.argv[1])
runs = [json.loads(line) for line in (root / 'runs.jsonl').read_text().splitlines() if line.strip()]
pg = ['rust', 'rust_ranges', 'go', 'seatunnel', 'sling', 'datax', 'airbyte', 'flink', 'debezium', 'debezium_bulk', 'inlong', 'meltano', 'spark', 'sqoop', 'estuary']
ch = ['rust', 'rust_ranges', 'go', 'seatunnel', 'sling', 'spark', 'datax']
expected = []
for route, tools in [('pg-pg', pg), ('pg-ch', ch)]:
    for tool in tools:
        for dataset in ('narrow', 'wide'):
            for parts in (1, 4):
                for rep in (1, 2, 3):
                    expected.append(('primary', route, tool, dataset, parts, rep))
        for parts in (4, 1):
            expected.append(('scale', route, tool, 'narrow10m', parts, 1))
for tool in ('debezium', 'debezium_bulk'):
    for rep in (1, 2, 3):
        expected.append(('p16', 'pg-pg', tool, 'narrow', 16, rep))

def identity(r):
    return r['route'], r['product'], r['dataset'], r['parts_requested'], r['repetition']

accepted = {}
for r in runs:
    if r['repetition'] and not r.get('exclude_from_comparison'):
        accepted.setdefault(identity(r), []).append(r)
rows = []
for phase, *key in expected:
    matches = accepted.get(tuple(key), [])
    if len(matches) > 1:
        raise RuntimeError('Duplicate nonexcluded measurement identity: ' + str(key))
    r = matches[0] if matches else None
    rows.append(dict(zip(('phase', 'route', 'product', 'dataset', 'parts', 'repetition'), (phase, *key))) )
    rows[-1].update(status=r['status'] if r else 'not_run', run=r['name'] if r else None)
counts = {phase: dict(Counter(r['status'] for r in rows if r['phase'] == phase)) for phase in ('primary', 'p16', 'scale')}
(root / 'coverage.json').write_text(json.dumps({'counts': counts, 'cases': rows}, indent=2) + '\n')
out = ['# Measurement coverage', '', 'This is the predeclared matrix. Missing and failed cases have no throughput value.', '', '| Phase | Expected | Verified | Failed | Not run |', '|---|---:|---:|---:|---:|']
for phase, cc in counts.items():
    out.append(f"| {phase} | {sum(cc.values())} | {cc.get('verified', 0)} | {cc.get('failed', 0)} | {cc.get('not_run', 0)} |")
out += ['', '## Incomplete cases', '', '| Phase | Route | Tool | Dataset | Parts | Repetition | Status |', '|---|---|---|---|---:|---:|---|']
for r in rows:
    if r['status'] != 'verified':
        out.append('| ' + ' | '.join(str(r[k]) for k in ('phase', 'route', 'product', 'dataset', 'parts', 'repetition', 'status')) + ' |')
if all(r['status'] == 'verified' for r in rows):
    out.append('| — | — | — | — | — | — | Complete |')
out += ['', 'PG→CH was scoped to six products (plus the separately labeled Rust exact-range variant). No claim is made that the other seven products lack ClickHouse support; they have no measured adapter in this campaign.', '']
(root / 'COVERAGE.md').write_text('\n'.join(out))
print(json.dumps(counts))
