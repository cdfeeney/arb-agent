import logging
import re
from collections import Counter
from typing import List, Tuple, Optional
from rapidfuzz import fuzz

log = logging.getLogger(__name__)


def filter_binary_kalshi(markets: List[dict]) -> List[dict]:
    """Pass-through. Kept for API compatibility with the poller.

    Originally this dropped multi-outcome event groups (later: only mutex
    ones, by yes-price sum). Both versions over-filtered: head-to-head
    games and N-team championship markets are mutually exclusive WITHIN
    Kalshi but each child is its own binary against a single-question
    Polymarket market. We now keep everything and rely on the LLM verifier
    to discriminate same-event from sub-question / range / unrelated.
    """
    return list(markets)


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
