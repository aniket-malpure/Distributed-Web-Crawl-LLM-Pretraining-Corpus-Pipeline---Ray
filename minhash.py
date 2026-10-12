"""Deterministic MinHash signatures + LSH banding (pure numpy, no per-process hash seeds).

Python's built-in ``hash`` is salted per process, which silently breaks distributed dedup
(different workers would produce different signatures). Everything here uses CRC32 / BLAKE2b.
"""
from __future__ import annotations

import re
import zlib

import numpy as np

_P32 = np.uint64(4294967311)  # prime just above 2**32
_WORD = re.compile(r"\w+", re.U)
_CHUNK = 4096


def optimal_bands(threshold: float, num_perm: int, fp_weight: float = 0.5) -> tuple[int, int]:
    """Choose (bands, rows) with bands*rows <= num_perm minimising weighted FP+FN area.

    P(candidate | J) = 1 - (1 - J^r)^b. We integrate the false-positive area below the
    threshold and the false-negative area above it (same idea as datasketch).
    """
    xs = np.linspace(0.0, 1.0, 513)
    best, best_err = (1, num_perm), float("inf")
    for b in range(1, num_perm + 1):
        for r in range(1, num_perm // b + 1):
            p = 1.0 - (1.0 - xs**r) ** b
            fp = np.trapezoid(p[xs < threshold], xs[xs < threshold]) if (xs < threshold).any() else 0.0
            fn = np.trapezoid(1.0 - p[xs >= threshold], xs[xs >= threshold])
            err = fp_weight * fp + (1.0 - fp_weight) * fn
            if err < best_err:
                best, best_err = (b, r), err
    return best


class MinHasher:
    def __init__(self, num_perm: int = 128, ngram: int = 5, seed: int = 1):
        self.num_perm, self.ngram = num_perm, ngram
        rng = np.random.RandomState(seed)
        # a, b < 2**31 keeps a*x (x < 2**32) inside uint64.
        self.a = rng.randint(1, 2**31 - 1, size=num_perm, dtype=np.uint64)
        self.b = rng.randint(0, 2**31 - 1, size=num_perm, dtype=np.uint64)

    def shingles(self, text: str) -> np.ndarray:
        words = _WORD.findall(text.lower())
        n = self.ngram
        if len(words) < n:
            grams = [" ".join(words)] if words else [""]
        else:
            grams = [" ".join(words[i : i + n]) for i in range(len(words) - n + 1)]
        return np.fromiter(
            (zlib.crc32(g.encode("utf-8")) for g in grams), dtype=np.uint64, count=len(grams)
        )

    def signature(self, text: str) -> np.ndarray:
        sh = np.unique(self.shingles(text))
        sig = np.full(self.num_perm, np.iinfo(np.uint64).max, dtype=np.uint64)
        for i in range(0, len(sh), _CHUNK):
            chunk = sh[i : i + _CHUNK, None]  # (n,1)
            hv = (chunk * self.a[None, :] + self.b[None, :]) % _P32  # (n, num_perm)
            sig = np.minimum(sig, hv.min(axis=0))
        return sig

    @staticmethod
    def band_hashes(sig: np.ndarray, bands: int, rows: int) -> list[int]:
        """One signed-int64 hash per band (band index is part of the Spark grouping key)."""
        import hashlib

        out = []
        for i in range(bands):
            chunk = sig[i * rows : (i + 1) * rows].tobytes()
            out.append(int.from_bytes(hashlib.blake2b(chunk, digest_size=8).digest(), "little", signed=True))
        return out


def jaccard_estimate(sig_a: np.ndarray, sig_b: np.ndarray) -> float:
    return float(np.mean(sig_a == sig_b))
