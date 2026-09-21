#!/usr/bin/env python3
"""Reclaim only verified campaign staging, preserving size evidence and diagnostics.

Explicit --apply is required. The common measurement lock excludes any transfer
while files are removed. Configs, logs, samples, binaries, DB tables and unrelated
Docker state are never touched. A previous size record survives future pruning.
"""
import argparse
import fcntl
import json
import pathlib
import re
import subprocess
from setup import ROOT

parser = argparse.ArgumentParser()
parser.add_argument('--apply', action='store_true')
args = parser.parse_args()
with (ROOT / 'measurement.lock').open('w') as lock:
    fcntl.flock(lock, fcntl.LOCK_EX)
    active = subprocess.check_output(['docker', 'ps', '--filter', 'name=fair21_', '--format', '{{.Names}}'], text=True).strip()
    if active:
        raise RuntimeError('refusing cleanup while benchmark containers are active')
    # Import performs the cheap post-run size census and preserves earlier rows.
    import artifact_sizes
    targets = []
    for p in sorted((ROOT / 'results/runs').glob('*.json')):
        r = json.loads(p.read_text())
        if r['status'] != 'verified' or r['repetition'] <= 0:
            continue
        name = r['name']
        if not re.fullmatch(r'(debezium|debezium_bulk|sqoop|estuary)_(narrow|wide|narrow10m)_p(1|4|16)_r[0-9]+_[0-9]+', name):
            continue
        if r['product'] in ('debezium', 'debezium_bulk'):
            candidates = [ROOT / 'private' / name / 'kafka-data']
        elif r['product'] == 'sqoop':
            candidates = [ROOT / 'private' / (name + '_staging')]
        else:
            candidates = list((ROOT / 'private').glob(name + '_e*.fixture'))
        for target in candidates:
            if not target.exists():
                continue
            if target.is_symlink() or not target.resolve().is_relative_to((ROOT / 'private').resolve()):
                raise RuntimeError('unexpected staging path')
            targets.append({'run': name, 'path': str(target.relative_to(ROOT)), 'directory': target.is_dir()})
    if args.apply:
        for item in targets:
            target = ROOT / item['path']
            if item['directory']:
                # Docker created nested staging directories as root. The target
                # has already passed the exact run-name and path checks above.
                subprocess.run(['sudo', '-n', 'rm', '-rf', '--', str(target)], check=True)
            else:
                target.unlink()
        journal = ROOT / 'results/pruned-intermediates.jsonl'
        with journal.open('a') as f:
            for item in targets:
                f.write(json.dumps(item) + '\n')
    print(json.dumps({'applied': args.apply, 'paths': len(targets)}))
