import random
import subprocess
import sys

import numpy as np

from crawlpipe.minhash import MinHasher, jaccard_estimate, optimal_bands
from crawlpipe.sample_data import make_article, near_duplicate


def test_optimal_bands_fits_budget_and_tracks_threshold():
    for t in (0.5, 0.8, 0.9):
        b, r = optimal_bands(t, 128)
        assert b * r <= 128
        implied = (1 / b) ** (1 / r)
        assert abs(implied - t) < 0.15
    # higher threshold -> more rows per band (stricter)
    assert optimal_bands(0.9, 128)[1] >= optimal_bands(0.5, 128)[1]


def test_signature_estimates_jaccard():
    rng = random.Random(5)
    a = make_article(rng)
    b = near_duplicate(rng, a, 0.01)
    c = make_article(rng)
    h = MinHasher(256, 5, 1)
    sa, sb, sc = h.signature(a), h.signature(b), h.signature(c)
    assert jaccard_estimate(sa, sb) > 0.7
    assert jaccard_estimate(sa, sc) < 0.1


def test_signatures_are_stable_across_processes():
    """Built-in hash() is salted per process; ours must not be."""
    code = (
        "from crawlpipe.minhash import MinHasher;"
        "print(MinHasher(64,5,1).signature('the quick brown fox jumps over the lazy dog again and again')[:4].tolist())"
    )
    outs = {
        subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True).stdout
        for _ in range(2)
    }
    assert len(outs) == 1


def test_band_hashes_count_and_determinism():
    sig = np.arange(128, dtype=np.uint64)
    h1 = MinHasher.band_hashes(sig, 9, 13)
    assert len(h1) == 9 and h1 == MinHasher.band_hashes(sig.copy(), 9, 13)
