#!/usr/bin/env python3
"""Run only the compression regression in an isolated vendored-crate harness.

The upstream crate is not a workspace member, and its unrelated unit modules
currently fail to compile. Disable those modules only in a temporary copy;
leave production code and the two new regression tests unchanged.
"""
import argparse,pathlib,shutil,subprocess,tempfile
p=argparse.ArgumentParser();p.add_argument('--old-lz4-behavior',action='store_true');a=p.parse_args()
root=pathlib.Path(__file__).resolve().parents[2]
with tempfile.TemporaryDirectory(prefix='transferia-native-regression-')as directory:
 copy=pathlib.Path(directory)/'clickhouse-arrow';shutil.copytree(root/'vendor/clickhouse-arrow',copy)
 manifest=copy/'Cargo.toml';manifest.write_text(manifest.read_text()+'\n[workspace]\n')
 for file in (copy/'src').rglob('*.rs'):
  source=file.read_text().replace('#[cfg(test)]','#[cfg(any())]')
  if file.name=='connection.rs':
   source=source.replace('#[cfg(any())]\n#[path = "tests/compression_settings.rs"]','#[cfg(test)]\n#[path = "tests/compression_settings.rs"]')
   if a.old_lz4_behavior:
    before='CompressionMethod::LZ4 => vec![("network_compression_method", "lz4")].into(),'
    assert before in source,'Expected codec pinning implementation changed'
    source=source.replace(before,'CompressionMethod::LZ4 => Settings::default(),',1)
  file.write_text(source)
 command=['cargo','test','--offline','--manifest-path',str(manifest),'--target-dir',str(root/'target'),'--no-default-features','--features','inner_pool','--lib','compression_settings_tests']
 raise SystemExit(subprocess.run(command,cwd=root).returncode)
