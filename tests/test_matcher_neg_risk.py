"""Regression tests for the neg-risk-sub-outcome matcher reject.

Bug: paper trade #395 (2026-05-02) paired Kalshi binary
KXTRUMPOUT27-27-DJT ("Donald Trump out before 2027?") with the Polymarket
sub-outcome of a neg-risk multi-outcome RACE ("Will Donald Trump be the next
leader out before 2027?", id 1485220, negRisk=true, groupItemTitle="Trump - USA
President"). These markets have correlated-but-distinct payoffs — directional
risk, not arbitrage.

Fix surfaces negRisk + groupItemTitle through the Polymarket normalizer and
makes match_markets pre-reject any Polymarket row where both are set.
"""

from __future__ import annotations

from src.engine.matcher import match_markets
from src.engine.normalizer import normalize_polymarket


def test_normalize_polymarket_surfaces_neg_risk_flags():
    raw = {
        "id": "1485220",
        "question": "Will Donald Trump be the next leader out before 2027?",
        "slug": "next-leader-out-trump",
        "outcomePrices": '["0.05", "0.95"]',
        "bestAsk": 0.05,
        "bestBid": 0.04,
        "volume": "100000",
        "liquidity": 5000,
        "endDate": "2026-12-31T00:00:00Z",
        "negRisk": True,
        "groupItemTitle": "Trump - USA President",
        "events": [{"slug": "next-leader-out"}],
    }
    norm = normalize_polymarket(raw)
    assert norm is not None
    assert norm["neg_risk"] is True
    assert norm["group_item_title"] == "Trump - USA President"


def test_normalize_polymarket_true_binary_has_no_group_flags():
    """The legitimate binary trump-out-as-president-before-2027 (id 666861)
    has negRisk=False and empty groupItemTitle. It must pass the filter."""
    raw = {
        "id": "666861",
        "question": "Trump out as President before 2027?",
        "slug": "trump-out-as-president-before-2027",
        "outcomePrices": '["0.135", "0.865"]',
        "bestAsk": 0.14,
        "bestBid": 0.13,
        "volume": "8034992",
        "liquidity": 753069.0755,
        "endDate": "2026-12-31T00:00:00Z",
        "negRisk": False,
        "groupItemTitle": "",
        "events": [{"slug": "trump-out-as-president-before-2027"}],
    }
    norm = normalize_polymarket(raw)
    assert norm is not None
    assert norm["neg_risk"] is False
    assert norm["group_item_title"] == ""


_QUESTION = "Donald Trump out before 2027?"


def _kalshi_trump_binary() -> dict:
    return {
        "platform": "kalshi",
        "ticker": "KXTRUMPOUT27-27-DJT",
        "event_ticker": "KXTRUMPOUT27-27",
        "question": _QUESTION,
        "yes_sub_title": "",
        "no_sub_title": "",
        "yes_price": 0.13,
        "no_price": 0.87,
        "volume": 1_000_000,
        "yes_ask_depth_usd": 5000,
        "no_ask_depth_usd": 5000,
        "closes_at": None,
    }


# Tests use an identical question on both sides so fuzz=100 — that isolates
# the filter behaviour from the matcher's underlying token-sort ratio. The
# real-world bug pair (kalshi "Donald Trump out before 2027?" vs poly "Will
# Donald Trump be the next leader out before 2027?") scores ~85 after stopword
# stripping, which is exactly why the bot accepted it; that's tested as a
# regression at the lower threshold below.


def test_match_markets_rejects_neg_risk_sub_outcome():
    """Even with a perfect token-sort match, a Polymarket sub-outcome of a
    neg-risk multi-outcome basket must not be paired with a Kalshi binary."""
    kalshi = [_kalshi_trump_binary()]
    poly = [{
        "platform": "polymarket",
        "ticker": "1485220",
        "question": _QUESTION,
        "yes_price": 0.05,
        "no_price": 0.95,
        "volume": 100_000,
        "yes_ask_depth_usd": 2500,
        "no_ask_depth_usd": 2500,
        "closes_at": None,
        "neg_risk": True,
        "group_item_title": "Trump - USA President",
    }]
    pairs = match_markets(kalshi, poly, similarity_threshold=85)
    assert pairs == [], (
        "neg-risk sub-outcome must be filtered before matching, "
        "regardless of fuzz score"
    )


def test_match_markets_rejects_real_world_bug_pair():
    """Regression for paper trade #395: the actual question strings from the
    bug, with the actual Polymarket flags. Must not pair."""
    kalshi = [{
        "platform": "kalshi",
        "ticker": "KXTRUMPOUT27-27-DJT",
        "event_ticker": "KXTRUMPOUT27-27",
        "question": "Donald Trump out before 2027?",
        "yes_sub_title": "",
        "no_sub_title": "",
        "yes_price": 0.13,
        "no_price": 0.87,
        "volume": 1_000_000,
        "yes_ask_depth_usd": 5000,
        "no_ask_depth_usd": 5000,
        "closes_at": None,
    }]
    poly = [{
        "platform": "polymarket",
        "ticker": "1485220",
        "question": "Will Donald Trump be the next leader out before 2027?",
        "yes_price": 0.05,
        "no_price": 0.95,
        "volume": 100_000,
        "yes_ask_depth_usd": 2500,
        "no_ask_depth_usd": 2500,
        "closes_at": None,
        "neg_risk": True,
        "group_item_title": "Trump - USA President",
    }]
    pairs = match_markets(kalshi, poly, similarity_threshold=85)
    assert pairs == []


def test_match_markets_accepts_true_binary_with_neg_risk_false():
    """The legitimate binary pair (negRisk=false, empty groupItemTitle) must
    still pass the filter. Identical questions guarantee fuzz=100 so this
    test isolates the filter, not the underlying fuzz behaviour."""
    kalshi = [_kalshi_trump_binary()]
    poly = [{
        "platform": "polymarket",
        "ticker": "666861",
        "question": _QUESTION,
        "yes_price": 0.14,
        "no_price": 0.87,
        "volume": 8_034_992,
        "yes_ask_depth_usd": 50000,
        "no_ask_depth_usd": 50000,
        "closes_at": None,
        "neg_risk": False,
        "group_item_title": "",
    }]
    pairs = match_markets(kalshi, poly, similarity_threshold=85)
    assert len(pairs) == 1
    k, p = pairs[0]
    assert k["ticker"] == "KXTRUMPOUT27-27-DJT"
    assert p["ticker"] == "666861"


def test_match_markets_accepts_neg_risk_with_empty_group_title():
    """neg_risk=True alone (without groupItemTitle) is not enough to reject —
    the filter requires BOTH. This guards against over-filtering if Polymarket
    starts setting negRisk on standalone markets."""
    kalshi = [_kalshi_trump_binary()]
    poly = [{
        "platform": "polymarket",
        "ticker": "999",
        "question": _QUESTION,
        "yes_price": 0.14,
        "no_price": 0.87,
        "volume": 100_000,
        "yes_ask_depth_usd": 5000,
        "no_ask_depth_usd": 5000,
        "closes_at": None,
        "neg_risk": True,
        "group_item_title": "",
    }]
    pairs = match_markets(kalshi, poly, similarity_threshold=85)
    assert len(pairs) == 1
