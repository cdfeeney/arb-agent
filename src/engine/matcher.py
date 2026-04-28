import logging
import re
from collections import Counter
from typing import List, Tuple, Optional
from rapidfuzz import fuzz

log = logging.getLogger(__name__)


def filter_binary_kalshi(markets: List[dict], mutex_sum_max: float = 1.15) -> List[dict]:
    """Drop only the markets whose event_ticker group is MUTUALLY EXCLUSIVE.

    Kalshi groups several shapes of markets under one `event_ticker`:
      1. Mutually exclusive ("Who wins NBA Finals?" — exactly one YES). YES
         prices sum to ≈ $1.00. NOT arbitrageable as binaries — drop.
      2. Independent binaries ("Will Fed hike >25bps Y/N?" alongside "Will
         Fed cut Y/N?"). YES prices sum well above $1.00. Each child IS its
         own binary — keep them.
      3. Range buckets (CPI 3.0–3.1%, 3.1–3.2%, ...). Mutually exclusive but
         compose into thresholds. Out of scope for v1 — drop with the others.

    Heuristic: group markets by event_ticker, sum the yes_price across the
    group; if sum ≤ mutex_sum_max it's effectively mutually exclusive and
    we drop all; otherwise we keep all members. Singletons always pass.
    """
    by_event: dict[str, list[dict]] = {}
    for m in markets:
        evt = m.get("event_ticker", "")
        if not evt:
            by_event.setdefault("__no_event__", []).append(m)
        else:
            by_event.setdefault(evt, []).append(m)

    kept: list[dict] = []
    for evt, group in by_event.items():
        if evt == "__no_event__" or len(group) == 1:
            kept.extend(group)
            continue
        yes_sum = sum(m.get("yes_price", 0.0) for m in group)
        if yes_sum > mutex_sum_max:
            kept.extend(group)
    return kept


_STOPWORDS = {
    "will", "the", "a", "an", "in", "by", "at", "on", "to", "be",
    "is", "are", "for", "of", "and", "or", "who", "what", "when",
    "which", "this", "that", "with", "from", "has", "have",
}

def _preprocess(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    words = [w for w in text.split() if w not in _STOPWORDS and len(w) > 1]
    return " ".join(words)

def _expiry_ok(a: dict, b: dict, max_hours: int) -> bool:
    ca, cb = a.get("closes_at"), b.get("closes_at")
    if ca is None or cb is None:
        return True  # can't check, allow
    delta_hours = abs((ca - cb).total_seconds()) / 3600
    return delta_hours <= max_hours

def match_markets(
    kalshi_markets: List[dict],
    poly_markets: List[dict],
    similarity_threshold: int = 85,
    expiry_proximity_hours: int = 72,
) -> List[Tuple[dict, dict]]:
    pairs = []
    near_misses: List[Tuple[int, str, str]] = []
    for k in kalshi_markets:
        k_text = _preprocess(k["question"])
        if not k_text:
            continue
        for p in poly_markets:
            p_text = _preprocess(p["question"])
            if not p_text:
                continue
            score = fuzz.token_sort_ratio(k_text, p_text)
            if score >= similarity_threshold and _expiry_ok(k, p, expiry_proximity_hours):
                pairs.append((k, p))
            elif score >= 50:
                near_misses.append((score, k["question"][:70], p["question"][:70]))

    if not pairs and near_misses:
        near_misses.sort(reverse=True)
        log.info("Matcher: 0 pairs at threshold=%d. Top near-misses (score / Kalshi / Poly):", similarity_threshold)
        for score, kq, pq in near_misses[:5]:
            log.info("  %d  K: %s  ||  P: %s", score, kq, pq)
    return pairs
