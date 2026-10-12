"""Heuristic document quality filters (Gopher / C4-style rules)."""
from __future__ import annotations

from collections import Counter

from .config import QualityConfig

_EN_STOP = {"the", "be", "to", "of", "and", "that", "have", "with"}
_BULLETS = ("-", "*", "•", "·", "–", "—")


def _dup_stats(items: list[str]) -> tuple[float, float]:
    """(fraction of items that are repeats, fraction of chars in repeats)."""
    if not items:
        return 0.0, 0.0
    seen: set[str] = set()
    dup_n = dup_chars = 0
    for it in items:
        if it in seen:
            dup_n += 1
            dup_chars += len(it)
        else:
            seen.add(it)
    total_chars = sum(len(i) for i in items) or 1
    return dup_n / len(items), dup_chars / total_chars


def _top_ngram_frac(words: list[str], n: int) -> float:
    if len(words) < n:
        return 0.0
    grams = Counter(tuple(words[i : i + n]) for i in range(len(words) - n + 1))
    gram, cnt = grams.most_common(1)[0]
    return cnt * sum(len(w) for w in gram) / max(1, sum(len(w) for w in words))


def _dup_ngram_frac(words: list[str], n: int) -> float:
    if len(words) < n:
        return 0.0
    seen: set[tuple[str, ...]] = set()
    covered = [False] * len(words)
    for i in range(len(words) - n + 1):
        g = tuple(words[i : i + n])
        if g in seen:
            for j in range(i, i + n):
                covered[j] = True
        else:
            seen.add(g)
    total = sum(len(w) for w in words) or 1
    return sum(len(w) for w, c in zip(words, covered) if c) / total


_TOP_NGRAM_LIMITS = {2: 0.20, 3: 0.18, 4: 0.16}
_DUP_NGRAM_LIMITS = {5: 0.15, 6: 0.14, 7: 0.13, 8: 0.12, 9: 0.11, 10: 0.10}


def assess(text: str, cfg: QualityConfig, lang: str = "en") -> tuple[str | None, dict[str, float]]:
    """Return ``(reject_reason | None, features)``. ``None`` means the document passes."""
    words = text.split()
    n_words = len(words)
    feats: dict[str, float] = {"n_words": float(n_words)}

    if n_words < cfg.min_words:
        return "min_words", feats
    if n_words > cfg.max_words:
        return "max_words", feats

    mean_len = sum(len(w) for w in words) / n_words
    feats["mean_word_len"] = mean_len
    if not cfg.min_mean_word_len <= mean_len <= cfg.max_mean_word_len:
        return "mean_word_len", feats

    symbols = text.count("#") + text.count("...") + text.count("…")
    if symbols / n_words > cfg.max_symbol_word_ratio:
        return "symbol_ratio", feats

    alpha = sum(1 for w in words if any(c.isalpha() for c in w)) / n_words
    feats["alpha_ratio"] = alpha
    if alpha < cfg.min_alpha_word_frac:
        return "alpha_ratio", feats

    if lang == "en":
        lowered = {w.lower().strip(".,;:!?\"'()") for w in words[:2000]}
        if len(_EN_STOP & lowered) < cfg.min_stopwords:
            return "stopwords", feats

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if lines:
        if sum(ln.startswith(_BULLETS) for ln in lines) / len(lines) > cfg.max_bullet_line_frac:
            return "bullet_lines", feats
        if sum(ln.endswith(("...", "…")) for ln in lines) / len(lines) > cfg.max_ellipsis_line_frac:
            return "ellipsis_lines", feats

    dl, dlc = _dup_stats(lines)
    if dl > cfg.max_dup_line_frac or dlc > cfg.max_dup_line_char_frac:
        return "dup_lines", feats
    paras = [p.strip() for p in text.split("\n\n") if p.strip()]
    dp, dpc = _dup_stats(paras)
    if dp > cfg.max_dup_para_frac or dpc > cfg.max_dup_para_char_frac:
        return "dup_paragraphs", feats

    lw = [w.lower() for w in words]
    for n, lim in _TOP_NGRAM_LIMITS.items():
        if _top_ngram_frac(lw, n) > lim:
            return f"top_{n}gram", feats
    for n, lim in _DUP_NGRAM_LIMITS.items():
        if _dup_ngram_frac(lw, n) > lim:
            return f"dup_{n}gram", feats

    return None, feats
