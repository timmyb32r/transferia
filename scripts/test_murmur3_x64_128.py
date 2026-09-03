#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

try:
    from scripts.murmur3_x64_128 import (
        Murmur3X64_128,
        murmur3_x64_128,
        murmur3_x64_128_file,
    )
except ModuleNotFoundError:
    from murmur3_x64_128 import Murmur3X64_128, murmur3_x64_128, murmur3_x64_128_file


class Murmur3X64128Test(unittest.TestCase):
    def test_matches_reference_vectors(self) -> None:
        self.assertEqual(murmur3_x64_128(b""), "00000000000000000000000000000000")
        self.assertEqual(murmur3_x64_128(b"hello"), "5b1e906a48ae1d19cbd8a7b341bd9b02")
        self.assertEqual(
            murmur3_x64_128(b"hello world"),
            "ab97467d60eb63b1533f6046eb7f610e",
        )

    def test_chunk_boundaries_do_not_change_the_digest(self) -> None:
        payload = bytes(range(251)) * 3
        expected = murmur3_x64_128(payload)
        for width in (1, 7, 15, 16, 17, 127):
            digest = Murmur3X64_128()
            for offset in range(0, len(payload), width):
                digest.update(payload[offset : offset + width])
            self.assertEqual(digest.hexdigest(), expected)

    def test_file_helper_streams_the_same_bytes(self) -> None:
        payload = bytes(range(256)) * 2049
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "artifact"
            path.write_bytes(payload)
            self.assertEqual(murmur3_x64_128_file(path), murmur3_x64_128(payload))


if __name__ == "__main__":
    unittest.main()
