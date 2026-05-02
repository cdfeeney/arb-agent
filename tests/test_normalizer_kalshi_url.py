"""Kalshi URL pattern regression test.

The bare /markets/{ticker} pattern returns 404, and so does
/markets/{event_ticker}/{ticker}. The /markets/{series_lower} (series-page)
URL is the simplest pattern that resolves on kalshi.com and is derivable
from the API alone (no SEO slug needed).

Pattern verified 2026-05-02 via Kalshi search index — see search results
for KXVETOCOUNT (/markets/kxvetocount), KXTRUMPADMINLEAVE
(/markets/kxtrumpadminleave/...), KXTRUMPOUT27 (/markets/kxtrumpout27/...).
"""

from __future__ import annotations

from src.engine.normalizer import _kalshi_series_url, normalize_kalshi


def test_series_url_from_event_ticker():
    assert _kalshi_series_url("KXTRUMPOUT27-27") == "https://kalshi.com/markets/kxtrumpout27"


def test_series_url_from_market_ticker_with_subticker():
    # Market ticker with sub-suffix (KXDFBPOKAL-26-BMU) collapses to series.
    assert _kalshi_series_url("KXDFBPOKAL-26-BMU") == "https://kalshi.com/markets/kxdfbpokal"


def test_series_url_handles_no_hyphens():
    # Some series have no event suffix (e.g. KXVETOCOUNT directly).
    assert _kalshi_series_url("KXVETOCOUNT") == "https://kalshi.com/markets/kxvetocount"


def test_series_url_lowercases():
    assert _kalshi_series_url("KXIMPEACH-26") == "https://kalshi.com/markets/kximpeach"


def test_series_url_empty_input_safe():
    # Don't generate a 404 with a malformed slug; fall back to markets root.
    assert _kalshi_series_url("") == "https://kalshi.com/markets"


def test_normalize_kalshi_uses_series_url():
    raw = {
        "ticker": "KXTRUMPOUT27-27-DJT",
        "event_ticker": "KXTRUMPOUT27-27",
        "title": "Donald Trump out before 2027?",
        "yes_bid_dollars": "0.13",
        "yes_ask_dollars": "0.14",
        "no_bid_dollars": "0.86",
        "no_ask_dollars": "0.87",
        "last_price_dollars": "0.13",
        "volume_fp": "100000",
        "liquidity_dollars": "5000",
    }
    norm = normalize_kalshi(raw)
    assert norm is not None
    assert norm["url"] == "https://kalshi.com/markets/kxtrumpout27"
