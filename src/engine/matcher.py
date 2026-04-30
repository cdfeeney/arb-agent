import logging
import re
from typing import List, Tuple
from rapidfuzz import fuzz, process
import numpy as np

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
    """Pair Kalshi × Polymarket markets by fuzzy question-text similarity.

    Performance note: uses rapidfuzz.process.cdist (vectorised C kernel) and
    pre-computes per-market normalised text exactly once. The previous
    implementation re-tokenised every Polymarket question inside the inner
    loop (K×P preprocess calls) and made K×P Python-level fuzz calls — at
    K≈3000 / P≈30000 (post-365-day window) that's ~90M ops per cycle and
    blew past the 30s polling interval.
    """
    if not kalshi_markets or not poly_markets:
        return []

    # Pre-compute normalised text once per market.
    k_texts = [_preprocess(k["question"]) for k in kalshi_markets]
    p_texts = [_preprocess(p["question"]) for p in poly_markets]

    # Build index of valid (non-empty) entries on each side so cdist only
    # works on real text.
    k_valid_idx = [i for i, t in enumerate(k_texts) if t]
    p_valid_idx = [j for j, t in enumerate(p_texts) if t]
    if not k_valid_idx or not p_valid_idx:
        return []

    # Vectorised K×P score matrix in C. Returns int8 0–100 scores.
    scores = process.cdist(
        [k_texts[i] for i in k_valid_idx],
        [p_texts[j] for j in p_valid_idx],
        scorer=fuzz.token_sort_ratio,
        score_cutoff=50,   # entries below 50 come back as 0 — saves time
        dtype=np.uint8,
    )

    near_misses: List[Tuple[int, str, str]] = []
    # Find all (k, p) with score >= threshold and collect with their score
    # so we can sort highest-confidence first. The LLM verifier downstream
    # has a per-cycle cap, so the order matters: feeding it the most
    # plausible pairs first maximises catch rate.
    above_thresh = np.argwhere(scores >= similarity_threshold)
    scored: List[Tuple[int, dict, dict]] = []
    for ki, pj in above_thresh:
        k = kalshi_markets[k_valid_idx[ki]]
        p = poly_markets[p_valid_idx[pj]]
        if _expiry_ok(k, p, expiry_proximity_hours):
            scored.append((int(scores[ki, pj]), k, p))
    scored.sort(key=lambda t: t[0], reverse=True)
    pairs: List[Tuple[dict, dict]] = [(k, p) for _, k, p in scored]

    if not pairs:
        # Only build the near-miss diagnostic when we found nothing — saves
        # work on healthy cycles.
        rows, cols = np.where((scores >= 50) & (scores < similarity_threshold))
        for ki, pj in zip(rows, cols):
            near_misses.append((
                int(scores[ki, pj]),
                kalshi_markets[k_valid_idx[ki]]["question"][:70],
                poly_markets[p_valid_idx[pj]]["question"][:70],
            ))
        if near_misses:
            near_misses.sort(reverse=True)
            log.info(
                "Matcher: 0 pairs at threshold=%d. Top near-misses (score / Kalshi / Poly):",
                similarity_threshold,
            )
            for score, kq, pq in near_misses[:5]:
                log.info("  %d  K: %s  ||  P: %s", score, kq, pq)
    return pairs
