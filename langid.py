"""Dependency-free, deterministic language identification (stopword profile scoring).

This is intentionally simple so the pipeline runs offline and is reproducible. For
production, swap ``identify`` for fastText ``lid.176`` (see README) - the interface
``identify(text) -> (lang, confidence)`` is all the rest of the pipeline relies on.
"""
from __future__ import annotations

import re

_STOPWORDS: dict[str, frozenset[str]] = {
    "en": frozenset(
        "the be to of and a in that have i it for not on with he as you do at this but his "
        "by from they we say her she or an will my one all would there their what so up out "
        "if about who get which go me when make can like time no just him know take is are "
        "was were been has had".split()
    ),
    "es": frozenset(
        "el la de que y a en un ser se no haber por con su para como estar tener le lo todo "
        "pero mas hacer o poder decir este ir otro ese si me ya ver porque dar cuando muy "
        "sin vez mucho saber sobre tambien es son esta los las del una".split()
    ),
    "fr": frozenset(
        "le de un etre et a en avoir que pour dans ce il qui ne sur se pas plus pouvoir par "
        "je avec tout faire son mettre autre on mais nous comme ou si leur y dire elle "
        "devoir avant deux meme prendre aussi les des est une sont cette".split()
    ),
    "de": frozenset(
        "der die und in den von zu das mit sich des auf fur ist im dem nicht ein eine als "
        "auch es an werden aus er hat dass sie nach wird bei einer um am sind noch wie "
        "einem uber einen so zum war haben nur oder aber vor zur bis mehr durch man".split()
    ),
    "it": frozenset(
        "il di che e la un a per in non una sono mi si lo ha le ma con del ci da come io "
        "questo ho se te era anche tutto della al piu suo nel gli dei sono essere cosa "
        "quando molto perche loro dove poi ancora fare".split()
    ),
    "pt": frozenset(
        "de a o que e do da em um para e com nao uma os no se na por mais as dos como mas "
        "foi ao ele das tem seu sua ou ser quando muito ha nos ja esta eu tambem so pelo "
        "pela ate isso ela entre era depois sem mesmo aos".split()
    ),
}
# "e", "a" etc. overlap across languages; short words get a small weight cap below.

_WORD = re.compile(r"[^\W\d_]+", re.U)
_ACCENTS = str.maketrans("áàâäãåéèêëíìîïóòôöõúùûüçñ", "aaaaaaeeeeiiiiooooouuuucn")


def identify(text: str, max_chars: int = 5000) -> tuple[str, float]:
    """Return (lang_code, confidence in [0,1]). ``("und", 0.0)`` when undecidable."""
    words = [w.lower().translate(_ACCENTS) for w in _WORD.findall(text[:max_chars])]
    if len(words) < 8:
        return "und", 0.0
    n = len(words)
    scores: dict[str, float] = {}
    for lang, sw in _STOPWORDS.items():
        hits = sum(1 for w in words if w in sw)
        scores[lang] = hits / n
    ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
    (best, s1), (_, s2) = ranked[0], ranked[1]
    # Natural prose is ~35-50% stopwords; rescale and penalise ambiguity vs runner-up.
    if s1 < 0.12:
        return "und", round(s1, 3)
    conf = min(1.0, s1 / 0.35) * (1.0 - 0.5 * (s2 / s1 if s1 else 0))
    return best, round(conf, 3)
