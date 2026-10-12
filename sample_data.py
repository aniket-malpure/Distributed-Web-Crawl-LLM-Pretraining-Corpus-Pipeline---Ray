"""Deterministic synthetic web-crawl generator (WARC files + gold labels).

Why synthetic: the sandbox / CI has no guaranteed access to Common Crawl, and extraction and
dedup need *ground truth* to be measurable. The generator plants known structure:

* boilerplate (nav, cookie banner, footer, ads) around the true article body
* exact duplicates, near-duplicates (<=1% word edits), mirrors on other domains
* low-quality pages (short, symbol-heavy, repetitive, bullet-only)
* non-English pages, non-HTML payloads, HTTP errors

``gold.jsonl`` records, per URL, the article body, boilerplate phrases and the planted label so
extraction F1, boilerplate leakage and expected dedup removals can be computed exactly.
"""
from __future__ import annotations

import json
import random
from pathlib import Path

from .warc_io import write_warc

_DET = ["the", "a", "this", "that", "each", "every", "our", "their"]
_ADJ = (
    "robust scalable distributed efficient reliable modern practical careful detailed "
    "lightweight resilient flexible efficient transparent incremental deterministic "
    "versatile compact thorough rigorous unusual stable quiet bright steady".split()
)
_NOUN = (
    "system pipeline dataset crawler engine cluster archive project library service "
    "network model benchmark index shard record corpus workflow module platform "
    "experiment schema scheduler runtime cache queue partition snapshot signal "
    "garden river bridge village market season harvest mountain journey workshop".split()
)
_VERB = (
    "improves processes handles validates transforms organizes stores measures "
    "reduces tracks balances monitors coordinates documents compares selects "
    "describes supports protects delivers explores connects replaces".split()
)
_ADV = "often usually carefully quickly gradually rarely mostly directly reliably".split()
_TOPIC = (
    "gardening urban cycling coffee roasting astronomy chess openings sourdough baking "
    "trail running amateur radio woodworking birdwatching jazz history railway "
    "architecture local elections marine biology vintage cameras board games".split()
)
_CLAUSE = [
    "{det} {adj} {noun} {adv} {verb} {det2} {adj2} {noun2} when conditions change",
    "because {det} {noun} {verb} {det2} {noun2}, people {adv} rely on {det3} {adj} {noun3}",
    "{det} {adj} {noun} {verb} {det2} {noun2} and {adv} {verb2} {det3} {adj2} {noun3}",
    "in practice {det} {noun} {adv} {verb} {det2} {adj} {noun2} for {det3} {noun3}",
    "although {det} {noun} seems {adj}, {det2} {noun2} {adv} {verb} {det3} {adj2} {noun3}",
    "many readers say that {det} {adj} {noun} {verb} {det2} {noun2} better than {det3} {noun3}",
]

_ES = (
    "el equipo de la ciudad se reunio con los vecinos para hablar de los problemas que tiene "
    "el barrio y todos estaban de acuerdo en que es necesario hacer mas por las familias que "
    "viven en la zona porque el trabajo de la comunidad es muy importante para todos los que "
    "quieren un futuro mejor y no solo para los que tienen mas recursos en esta region"
).split()
_FR = (
    "le groupe de la ville a rencontre les habitants pour parler des projets que le quartier "
    "attend depuis longtemps et tout le monde etait d accord pour dire que les familles ont "
    "besoin de plus de soutien dans une region ou le travail de la communaute est une chose "
    "tres importante pour ceux qui veulent un avenir meilleur et pas seulement pour les autres"
).split()

NAV = ["Home", "About us", "Contact", "Privacy Policy", "Terms of Service", "Sitemap"]
BOILER = [
    "We use cookies to improve your experience. Accept all cookies to continue browsing.",
    "Subscribe to our newsletter and get exclusive offers delivered to your inbox every week.",
    "Copyright 2026 Example Media Group. All rights reserved. Powered by ExampleCMS.",
    "Advertisement: Click here to claim your free trial today and save big on everything.",
    "Follow us on social media and share this page with your friends and family now.",
]


def _sentence(rng: random.Random) -> str:
    tpl = rng.choice(_CLAUSE)
    s = tpl.format(
        det=rng.choice(_DET), det2=rng.choice(_DET), det3=rng.choice(_DET),
        adj=rng.choice(_ADJ), adj2=rng.choice(_ADJ),
        noun=rng.choice(_NOUN), noun2=rng.choice(_NOUN), noun3=rng.choice(_NOUN),
        verb=rng.choice(_VERB), verb2=rng.choice(_VERB), adv=rng.choice(_ADV),
    )
    return s[0].upper() + s[1:] + "."


def make_article(rng: random.Random, paragraphs: int | None = None) -> str:
    topic = rng.choice(_TOPIC)
    paras = []
    for _ in range(paragraphs or rng.randint(4, 8)):
        sents = [_sentence(rng) for _ in range(rng.randint(5, 9))]
        sents[0] = f"On {topic}, " + sents[0][0].lower() + sents[0][1:]
        paras.append(" ".join(sents))
    return "\n\n".join(paras)


def make_foreign(rng: random.Random, lang: str) -> str:
    pool = _ES if lang == "es" else _FR
    paras = []
    for _ in range(4):
        paras.append(" ".join(" ".join(rng.choices(pool, k=rng.randint(12, 20))).capitalize() + "." for _ in range(6)))
    return "\n\n".join(paras)


def near_duplicate(rng: random.Random, text: str, edit_frac: float = 0.01) -> str:
    words = text.split(" ")
    n_edit = max(1, int(len(words) * edit_frac))
    for _ in range(n_edit):
        i = rng.randrange(len(words))
        words[i] = rng.choice(_NOUN)
    return " ".join(words)


def to_html(title: str, body: str, boiler: list[str]) -> str:
    paras = "\n".join(f"<p>{p}</p>" for p in body.split("\n\n"))
    nav = "".join(f'<li><a href="/{n.lower().replace(" ", "-")}">{n}</a></li>' for n in NAV)
    return (
        "<!DOCTYPE html><html lang='en'><head><meta charset='utf-8'>"
        f"<title>{title}</title><style>body{{margin:0}}</style>"
        "<script>window.dataLayer=[];function track(){return 1}</script></head><body>"
        f"<header><nav><ul>{nav}</ul></nav></header>"
        f"<div class='cookie-banner'><p>{boiler[0]}</p></div>"
        f"<aside class='ad'><p>{boiler[3]}</p></aside>"
        f"<main><article><h1>{title}</h1>{paras}</article></main>"
        f"<section class='newsletter'><p>{boiler[1]}</p></section>"
        f"<footer><p>{boiler[2]}</p><p>{boiler[4]}</p></footer></body></html>"
    )


def generate(
    out_dir: str | Path,
    n_pages: int = 600,
    n_warcs: int = 3,
    seed: int = 7,
    n_domains: int = 40,
) -> dict:
    """Write ``n_warcs`` WARC.gz files and ``gold.jsonl``; return the plan summary."""
    rng = random.Random(seed)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    domains = [f"site{i:02d}.example.org" for i in range(n_domains)]
    # Zipf-ish domain popularity so concentration analysis is non-trivial.
    weights = [1.0 / (i + 1) for i in range(n_domains)]

    pages: list[tuple[str, int, str, bytes, str]] = []
    gold: list[dict] = []
    originals: list[tuple[str, str]] = []  # (url, body) of clean English articles
    counts = {k: 0 for k in (
        "good", "exact_dup", "near_dup", "mirror_dup", "short", "symbols", "repetitive",
        "bullets", "foreign", "non_html", "http_error",
    )}

    def add(url: str, status: int, ctype: str, body: bytes, label: str, text: str = "") -> None:
        date = f"2026-0{rng.randint(1, 9)}-{rng.randint(10, 28)}T12:00:00Z"
        pages.append((url, status, ctype, body, date))
        counts[label] += 1
        gold.append({"url": url, "label": label, "body": text, "boilerplate": BOILER + NAV})

    def new_url(dom: str | None = None) -> str:
        dom = dom or rng.choices(domains, weights)[0]
        return f"https://{dom}/articles/{rng.randrange(10**8):08d}"

    for _ in range(n_pages):
        roll = rng.random()
        boiler = BOILER
        if roll < 0.52 or not originals:
            body = make_article(rng)
            url = new_url()
            add(url, 200, "text/html; charset=utf-8", to_html("Notes on " + rng.choice(_TOPIC), body, boiler).encode(), "good", body)
            originals.append((url, body))
        elif roll < 0.60:
            _, body = rng.choice(originals)  # same URL path different host => exact content dup
            url = new_url()
            add(url, 200, "text/html; charset=utf-8", to_html("Notes (copy)", body, boiler).encode(), "exact_dup", body)
        elif roll < 0.72:
            _, body = rng.choice(originals)
            nb = near_duplicate(rng, body)
            add(new_url(), 200, "text/html; charset=utf-8", to_html("Notes (updated)", nb, boiler).encode(), "near_dup", nb)
        elif roll < 0.76:
            _, body = rng.choice(originals)
            nb = body + "\n\n" + _sentence(rng) + " " + _sentence(rng)
            add(new_url(), 200, "text/html; charset=utf-8", to_html("Mirror", nb, boiler).encode(), "mirror_dup", nb)
        elif roll < 0.80:
            body = " ".join(_sentence(rng) for _ in range(2))
            add(new_url(), 200, "text/html; charset=utf-8", to_html("Short", body, boiler).encode(), "short", body)
        elif roll < 0.83:
            base = make_article(rng, 4).split()
            body = " ".join(w + (" # ..." if i % 3 == 0 else "") for i, w in enumerate(base))
            add(new_url(), 200, "text/html", to_html("Symbols", body, boiler).encode(), "symbols", body)
        elif roll < 0.86:
            para = _sentence(rng) + " " + _sentence(rng) + " " + _sentence(rng)
            body = "\n\n".join([para] * 12)
            add(new_url(), 200, "text/html", to_html("Repeated", body, boiler).encode(), "repetitive", body)
        elif roll < 0.88:
            body = "\n\n".join("- " + _sentence(rng) for _ in range(60))
            add(new_url(), 200, "text/html", to_html("List", body, boiler).encode(), "bullets", body)
        elif roll < 0.93:
            lang = rng.choice(["es", "fr"])
            body = make_foreign(rng, lang)
            add(new_url(), 200, "text/html", to_html("Noticias", body, boiler).encode(), "foreign", body)
        elif roll < 0.96:
            add(new_url() + ".pdf", 200, "application/pdf", b"%PDF-1.4 fake pdf payload " * 20, "non_html")
        else:
            add(new_url(), rng.choice([404, 500, 503]), "text/html", b"<html><body>Not found</body></html>", "http_error")

    rng.shuffle(pages)
    gold_by_url = {g["url"]: g for g in gold}
    n_warcs = max(1, n_warcs)
    chunk = (len(pages) + n_warcs - 1) // n_warcs
    paths = []
    for i in range(n_warcs):
        part = pages[i * chunk : (i + 1) * chunk]
        if not part:
            continue
        p = out / f"sample-{i:03d}.warc.gz"
        write_warc(str(p), part)
        paths.append(str(p))
    with open(out / "gold.jsonl", "w", encoding="utf-8") as fh:
        for url, *_ in pages:
            fh.write(json.dumps(gold_by_url[url]) + "\n")

    summary = {
        "warcs": paths,
        "gold": str(out / "gold.jsonl"),
        "pages": len(pages),
        "planted": counts,
        "planted_duplicates": counts["exact_dup"] + counts["near_dup"] + counts["mirror_dup"],
        "seed": seed,
    }
    (out / "plan.json").write_text(json.dumps(summary, indent=2))
    return summary
