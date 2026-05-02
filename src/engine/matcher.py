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
    # Inverse phrasing tokens — they carry no semantic match value and
    # actively hurt token-sort ratio between "X happens" and "X does
    # not happen", which the LLM should still verify as the same event.
    "not", "no", "yes", "any",
    # Generic time/scope words that match across unrelated questions.
    "before", "after", "during", "next", "first", "last",
}

# Tokens that look distinctive but are actually noise — bare years,
# 2/4-digit numbers, ordinals — when used as the sole "anchor" they
# overmatch (every 2026 election shares "2026"). The anchor-bypass path
# excludes these so two questions need real proper-noun overlap, not
# just a shared year.
_NOISE_ANCHOR = re.compile(r"^(?:\d{1,4}|\d{1,2}(?:st|nd|rd|th)|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)$")


def _preprocess(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    words = [w for w in text.split() if w not in _STOPWORDS and len(w) > 1]
    return " ".join(words)


def _anchor_tokens(preprocessed: str) -> set[str]:
    """Distinctive tokens for shared-anchor matching (excludes years/months)."""
    return {
        w for w in preprocessed.split()
        if not _NOISE_ANCHOR.match(w) and len(w) > 2
    }

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
    anchor_min_shared: int = 3,
) -> List[Tuple[dict, dict]]:
    """Pair Kalshi × Polymarket markets by fuzzy text similarity OR shared anchors.

    Two-tier matching to capture both phrasing-similar and inverse-phrased
    arbs that the LLM verifier ultimately confirms:

      Tier 1 (high-confidence): fuzz token-sort ratio ≥ similarity_threshold.
      Tier 2 (anchor-bypass):   shared distinctive tokens ≥ anchor_min_shared
                                 even when fuzz < threshold.

    Tier 2 catches "Will OpenAI not IPO by Dec 31, 2026?" vs "Who will IPO
    before 2027? – OpenAI" — these share {openai, ipo, dec/2026/2027 stripped}
    but token-sort ratio is ~50% because of all the differing scaffolding
    words. The LLM verifier downstream resolves whether the inverse phrasing
    actually means the same event.

    Performance note: uses rapidfuzz.process.cdist (vectorised C kernel) and
    pre-computes per-market normalised text + anchor sets exactly once.
    """
    if not kalshi_markets or not poly_markets:
        return []

    # Structural reject: Polymarket sub-outcomes of neg-risk multi-outcome
    # baskets cannot legitimately pair with Kalshi YES/NO binaries. By
    # construction the basket resolves at most one outcome to YES, so the
    # binary "Will X happen?" and the sub-outcome "Will X be the FIRST to
    # happen?" have correlated-but-distinct payoffs — directional risk, not
    # arb. The signal is `neg_risk=True AND group_item_title!=""`; a true
    # binary like "trump-out-as-president-before-2027" has neg_risk=False
    # and empty groupItemTitle and is unaffected.
    n_before = len(poly_markets)
    poly_markets = [
        p for p in poly_markets
        if not (p.get("neg_risk") and p.get("group_item_title"))
    ]
    n_rejected = n_before - len(poly_markets)
    if n_rejected:
        log.info(
            "Matcher: rejected %d Polymarket neg-risk sub-outcomes pre-match",
            n_rejected,
        )
    if not poly_markets:
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
        score_cutoff=40,   # entries below 40 come back as 0 — saves time
        dtype=np.uint8,
    )

    # Pre-compute anchor tokens for the inverse-phrasing bypass. Built once
    # per market — comparison below is O(K×P) set ops but only over indices
    # not already captured by the fuzz threshold.
    k_anchors = [_anchor_tokens(k_texts[i]) for i in k_valid_idx]
    p_anchors = [_anchor_tokens(p_texts[j]) for j in p_valid_idx]

    above_thresh = set(
        (int(ki), int(pj)) for ki, pj in np.argwhere(scores >= similarity_threshold)
    )

    # Tier 2: scan everything below the fuzz threshold for shared-anchor hits.
    # Uses a per-Polymarket inverted index by anchor token to avoid the
    # naive K×P pass when one side is large.
    p_token_index: dict[str, list[int]] = {}
    for pj, anchors in enumerate(p_anchors):
        for tok in anchors:
            p_token_index.setdefault(tok, []).append(pj)

    anchor_pairs: set[tuple[int, int]] = set()
    for ki, k_set in enumerate(k_anchors):
        if len(k_set) < anchor_min_shared:
            continue
        # Candidate Polymarket indices: any P that shares ≥1 anchor token.
        candidates: dict[int, int] = {}
        for tok in k_set:
            for pj in p_token_index.get(tok, []):
                candidates[pj] = candidates.get(pj, 0) + 1
        for pj, n_shared in candidates.items():
            if n_shared >= anchor_min_shared and (ki, pj) not in above_thresh:
                anchor_pairs.add((ki, pj))

    # Combine. Tier 1 keeps fuzz score for ranking; Tier 2 uses the count of
    # shared anchors mapped onto a comparable scale (×10 + 50 puts a 3-anchor
    # match at score 80, between weak and strong fuzz matches). Always-priorities
    # high-fuzz matches first since they're the cheapest signal of true match.
    scored: List[Tuple[int, dict, dict]] = []
    for ki, pj in above_thresh:
        k = kalshi_markets[k_valid_idx[ki]]
        p = poly_markets[p_valid_idx[pj]]
        if _expiry_ok(k, p, expiry_proximity_hours):
            scored.append((int(scores[ki, pj]), k, p))
    n_anchor_added = 0
    for ki, pj in anchor_pairs:
        k = kalshi_markets[k_valid_idx[ki]]
        p = poly_markets[p_valid_idx[pj]]
        if not _expiry_ok(k, p, expiry_proximity_hours):
            continue
        n_shared = len(k_anchors[ki] & p_anchors[pj])
        synthetic = min(99, 50 + n_shared * 10)
        scored.append((synthetic, k, p))
        n_anchor_added += 1
    scored.sort(key=lambda t: t[0], reverse=True)
    pairs: List[Tuple[dict, dict]] = [(k, p) for _, k, p in scored]

    if n_anchor_added:
        log.info(
            "Matcher: %d fuzz-tier + %d anchor-tier pairs (anchor catches inverse phrasing)",
            len(above_thresh), n_anchor_added,
        )

    if not pairs:
        # Only build the near-miss diagnostic when we found nothing — saves
        # work on healthy cycles.
        near_misses: List[Tuple[int, str, str]] = []
        rows, cols = np.where((scores >= 40) & (scores < similarity_threshold))
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
