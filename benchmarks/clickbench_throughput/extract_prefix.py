#!/usr/bin/env python3
"""Extract an exact, byte-preserving prefix of complete CSV records."""

from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.murmur3_x64_128 import Murmur3X64_128


class CsvPrefixError(ValueError):
    pass


def _positive(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def _write_all(descriptor: int, payload: bytes) -> None:
    remaining = memoryview(payload)
    while remaining:
        written = os.write(descriptor, remaining)
        if written <= 0:
            raise OSError("CSV prefix output made no write progress")
        remaining = remaining[written:]


class _BinaryCsvLines:
    """Feed latin-1 CSV lines while retaining their exact source bytes."""

    def __init__(self, input_file, max_record_bytes: int) -> None:
        self._input = input_file
        self._max_record_bytes = max_record_bytes
        self._record = bytearray()
        self.record_number = 1

    def __iter__(self):
        return self

    def __next__(self) -> str:
        line = self._input.readline()
        if not line:
            raise StopIteration
        self._record.extend(line)
        if len(self._record) > self._max_record_bytes:
            raise CsvPrefixError(
                f"CSV record {self.record_number} exceeds "
                f"--max-record-bytes={self._max_record_bytes}"
            )
        return line.decode("latin-1")

    def take_record(self) -> bytes:
        record = bytes(self._record)
        self._record.clear()
        self.record_number += 1
        return record


def extract_prefix(
    source: Path,
    destination: Path,
    *,
    rows: int,
    columns: int,
    max_record_bytes: int,
) -> dict[str, int | str]:
    if source.resolve() == destination.resolve():
        raise CsvPrefixError("input and output paths must differ")
    if destination.exists():
        raise CsvPrefixError(f"output already exists: {destination}")

    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = destination.parent / f".{destination.name}.tmp.{os.getpid()}"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    output_fd = os.open(temporary, flags, 0o600)
    digest = Murmur3X64_128()
    written_bytes = 0
    written_rows = 0

    try:
        with source.open("rb", buffering=1024 * 1024) as input_file:
            lines = _BinaryCsvLines(input_file, max_record_bytes)
            reader = csv.reader(lines, strict=True)
            while written_rows < rows:
                try:
                    fields = next(reader)
                except StopIteration as error:
                    raise CsvPrefixError(
                        f"input contains only {written_rows} complete records; requested {rows}"
                    ) from error
                except csv.Error as error:
                    raise CsvPrefixError(
                        f"CSV record {written_rows + 1} is malformed: {error}"
                    ) from error
                if len(fields) != columns:
                    raise CsvPrefixError(
                        f"CSV record {written_rows + 1} has {len(fields)} fields; expected {columns}"
                    )
                payload = lines.take_record()
                _write_all(output_fd, payload)
                digest.update(payload)
                written_bytes += len(payload)
                written_rows += 1

        os.fsync(output_fd)
        actual_bytes = os.fstat(output_fd).st_size
        if actual_bytes != written_bytes:
            raise CsvPrefixError(
                f"output contains {actual_bytes} bytes after fsync; expected {written_bytes}"
            )
        os.close(output_fd)
        output_fd = -1
        os.link(temporary, destination)
        temporary.unlink()
    except BaseException:
        if output_fd >= 0:
            os.close(output_fd)
        temporary.unlink(missing_ok=True)
        raise

    return {
        "rows": written_rows,
        "columns": columns,
        "bytes": written_bytes,
        "murmur3_x64_128": digest.hexdigest(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rows", type=_positive, required=True)
    parser.add_argument("--columns", type=_positive, default=105)
    parser.add_argument("--max-record-bytes", type=_positive, default=64 * 1024 * 1024)
    args = parser.parse_args()
    try:
        result = extract_prefix(
            args.input,
            args.output,
            rows=args.rows,
            columns=args.columns,
            max_record_bytes=args.max_record_bytes,
        )
    except (CsvPrefixError, OSError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
