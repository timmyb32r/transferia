"""Streaming MurmurHash3 x64 128-bit for non-cryptographic fingerprints."""

from __future__ import annotations

from pathlib import Path


_MASK_64 = (1 << 64) - 1
_C1 = 0x87C37B91114253D5
_C2 = 0x4CF5AD432745937F


def _rotate_left(value: int, amount: int) -> int:
    return ((value << amount) | (value >> (64 - amount))) & _MASK_64


def _fmix64(value: int) -> int:
    value ^= value >> 33
    value = (value * 0xFF51AFD7ED558CCD) & _MASK_64
    value ^= value >> 33
    value = (value * 0xC4CEB9FE1A85EC53) & _MASK_64
    value ^= value >> 33
    return value


class Murmur3X64_128:
    """Incremental seed-zero MurmurHash3 compatible with the Rust `murmur3` crate."""

    def __init__(self, seed: int = 0) -> None:
        if not 0 <= seed <= 0xFFFFFFFF:
            raise ValueError("Murmur3 seed must fit u32")
        self._h1 = seed
        self._h2 = seed
        self._length = 0
        self._tail = bytearray()

    def update(self, payload: bytes | bytearray | memoryview) -> None:
        view = memoryview(payload).cast("B")
        self._length += len(view)
        if self._tail:
            needed = 16 - len(self._tail)
            self._tail.extend(view[:needed])
            view = view[needed:]
            if len(self._tail) == 16:
                self._mix_block(memoryview(self._tail))
                self._tail.clear()
        complete = len(view) - len(view) % 16
        for offset in range(0, complete, 16):
            self._mix_block(view[offset : offset + 16])
        self._tail.extend(view[complete:])

    def _mix_block(self, block: memoryview) -> None:
        k1 = int.from_bytes(block[:8], "little")
        k2 = int.from_bytes(block[8:16], "little")
        self._h1 ^= (_rotate_left((k1 * _C1) & _MASK_64, 31) * _C2) & _MASK_64
        self._h1 = (
            ((_rotate_left(self._h1, 27) + self._h2) & _MASK_64) * 5
            + 0x52DCE729
        ) & _MASK_64
        self._h2 ^= (_rotate_left((k2 * _C2) & _MASK_64, 33) * _C1) & _MASK_64
        self._h2 = (
            ((_rotate_left(self._h2, 31) + self._h1) & _MASK_64) * 5
            + 0x38495AB5
        ) & _MASK_64

    def digest_u128(self) -> int:
        h1 = self._h1
        h2 = self._h2
        tail = self._tail
        k1 = int.from_bytes(tail[:8], "little")
        k2 = int.from_bytes(tail[8:], "little")
        if k2:
            h2 ^= (_rotate_left((k2 * _C2) & _MASK_64, 33) * _C1) & _MASK_64
        if k1:
            h1 ^= (_rotate_left((k1 * _C1) & _MASK_64, 31) * _C2) & _MASK_64
        h1 ^= self._length
        h2 ^= self._length
        h1 = (h1 + h2) & _MASK_64
        h2 = (h2 + h1) & _MASK_64
        h1 = _fmix64(h1)
        h2 = _fmix64(h2)
        h1 = (h1 + h2) & _MASK_64
        h2 = (h2 + h1) & _MASK_64
        return (h2 << 64) | h1

    def hexdigest(self) -> str:
        return f"{self.digest_u128():032x}"


def murmur3_x64_128(payload: bytes, seed: int = 0) -> str:
    digest = Murmur3X64_128(seed)
    digest.update(payload)
    return digest.hexdigest()


def murmur3_x64_128_file(path: Path) -> str:
    digest = Murmur3X64_128()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
