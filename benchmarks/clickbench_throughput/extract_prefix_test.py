#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest import mock


MODULE_PATH = Path(__file__).with_name("extract_prefix.py")
SPEC = importlib.util.spec_from_file_location("extract_prefix", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class ExtractPrefixTest(unittest.TestCase):
    def extract(self, payload: bytes, rows: int = 2, **kwargs: int) -> tuple[bytes, dict]:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.csv"
            output = root / "output.csv"
            source.write_bytes(payload)
            result = MODULE.extract_prefix(
                source,
                output,
                rows=rows,
                columns=kwargs.get("columns", 3),
                max_record_bytes=kwargs.get("max_record_bytes", 1024),
            )
            return output.read_bytes(), result

    def test_preserves_quoted_newlines_commas_quotes_and_binary_bytes(self) -> None:
        payload = b'1,"a,\n""b",\xff\n2,plain,value\n3,not,copied\n'
        output, result = self.extract(payload)
        self.assertEqual(output, b'1,"a,\n""b",\xff\n2,plain,value\n')
        self.assertEqual(result["rows"], 2)
        self.assertEqual(result["bytes"], len(output))
        self.assertEqual(
            result["murmur3_x64_128"], "20ab99412578969de583db8168d5e26e"
        )
        self.assertNotIn("sha256", result)

    def test_preserves_crlf_record_boundaries(self) -> None:
        payload = b'1,"a\r\nb",c\r\n2,d,e\r\n'
        output, result = self.extract(payload)
        self.assertEqual(output, payload)
        self.assertEqual(result["rows"], 2)

    def test_rejects_wrong_field_count_without_publishing_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.csv"
            output = root / "output.csv"
            source.write_bytes(b"1,2\n")
            with self.assertRaisesRegex(MODULE.CsvPrefixError, "has 2 fields; expected 3"):
                MODULE.extract_prefix(
                    source, output, rows=1, columns=3, max_record_bytes=1024
                )
            self.assertFalse(output.exists())

    def test_rejects_unterminated_last_record(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.csv"
            output = root / "output.csv"
            source.write_bytes(b'1,"unterminated\n')
            with self.assertRaisesRegex(MODULE.CsvPrefixError, "malformed"):
                MODULE.extract_prefix(
                    source, output, rows=1, columns=3, max_record_bytes=1024
                )
            self.assertFalse(output.exists())

    def test_rejects_record_over_explicit_limit(self) -> None:
        with self.assertRaisesRegex(MODULE.CsvPrefixError, "max-record-bytes=5"):
            self.extract(b"1,22,3\n", rows=1, max_record_bytes=5)

    def test_refuses_to_replace_existing_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.csv"
            output = root / "output.csv"
            source.write_bytes(b"1,2,3\n")
            output.write_bytes(b"owned")
            with self.assertRaisesRegex(MODULE.CsvPrefixError, "already exists"):
                MODULE.extract_prefix(
                    source, output, rows=1, columns=3, max_record_bytes=1024
                )
            self.assertEqual(output.read_bytes(), b"owned")

    def test_write_all_retries_short_writes_and_rejects_no_progress(self) -> None:
        accepted = bytearray()

        def short_write(_descriptor: int, payload: memoryview) -> int:
            count = min(3, len(payload))
            accepted.extend(payload[:count])
            return count

        with mock.patch.object(MODULE.os, "write", side_effect=short_write):
            MODULE._write_all(7, b"complete-record")
        self.assertEqual(accepted, b"complete-record")

        with mock.patch.object(MODULE.os, "write", return_value=0):
            with self.assertRaisesRegex(OSError, "no write progress"):
                MODULE._write_all(7, b"record")


if __name__ == "__main__":
    unittest.main()
