import random

from crawlpipe import langid
from crawlpipe.config import QualityConfig
from crawlpipe.extract import decode_html, get_extractor
from crawlpipe.quality import assess
from crawlpipe.sample_data import BOILER, make_article, make_foreign, to_html


def test_trafilatura_drops_boilerplate_keeps_article():
    rng = random.Random(1)
    body = make_article(rng)
    html = to_html("Title", body, BOILER)
    out = get_extractor("trafilatura_precision")(html, "https://x.example/a")
    assert out and len(out) > 0.8 * len(body)
    assert BOILER[0] not in out and "Privacy Policy" not in out
    naive = get_extractor("naive_strip")(html, None)
    assert BOILER[0] in naive  # sanity: the naive baseline leaks boilerplate


def test_decode_html_charset_fallbacks():
    assert decode_html("café".encode("latin-1"), "text/html; charset=latin-1") == "café"
    assert decode_html("café".encode("utf-8"), "") == "café"
    assert "caf" in decode_html(b"caf\xe9", "")  # invalid utf-8 -> latin-1 fallback


def test_langid_english_vs_foreign():
    rng = random.Random(2)
    assert langid.identify(make_article(rng))[0] == "en"
    assert langid.identify(make_foreign(rng, "es"))[0] == "es"
    assert langid.identify(make_foreign(rng, "fr"))[0] == "fr"
    assert langid.identify("tiny")[0] == "und"


def test_quality_accepts_good_and_rejects_bad():
    cfg = QualityConfig()
    rng = random.Random(3)
    good = make_article(rng)
    assert assess(good, cfg)[0] is None
    assert assess("too short to be useful", cfg)[0] == "min_words"
    para = " ".join(make_article(rng, 1).split()[:60])
    assert assess("\n\n".join([para] * 10), cfg)[0] in {"dup_lines", "dup_paragraphs"}
    bullets = "\n".join("- " + make_article(rng, 1).split(".")[0] for _ in range(60))
    assert assess(bullets, cfg)[0] == "bullet_lines"
    noisy = " ".join(w + " #" for w in good.split())
    assert assess(noisy, cfg)[0] == "symbol_ratio"
