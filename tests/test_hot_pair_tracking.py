"""Tests for hot-pair tracking (#24 two-tier polling).

Verifies that PollingAgent._update_hot_pairs correctly:
  * Adds new verified pairs
  * Refreshes timestamps for re-seen pairs
  * Evicts pairs older than hot_pair_ttl_seconds
"""

from __future__ import annotations

import time

from src.agent.poller import PollingAgent


def _agent(ttl_seconds: int = 1800):
    """Minimal PollingAgent instance — bypasses __init__ to avoid needing
    Kalshi/Polymarket credentials."""
    a = PollingAgent.__new__(PollingAgent)
    a._hot_pairs = {}
    a.cfg = {"polling": {"hot_pair_ttl_seconds": ttl_seconds}}
    return a


def _market(platform: str, ticker: str) -> dict:
    return {"platform": platform, "ticker": ticker}


def test_adds_new_pair():
    a = _agent()
    pair = (_market("kalshi", "KX-FOO"), _market("polymarket", "pm-bar"))
    a._update_hot_pairs([pair])
    assert len(a._hot_pairs) == 1
    key = ("kalshi", "KX-FOO", "polymarket", "pm-bar")
    assert key in a._hot_pairs


def test_repeated_add_refreshes_timestamp():
    a = _agent()
    pair = (_market("kalshi", "KX-FOO"), _market("polymarket", "pm-bar"))
    a._update_hot_pairs([pair])
    key = ("kalshi", "KX-FOO", "polymarket", "pm-bar")
    t0 = a._hot_pairs[key][2]
    time.sleep(0.05)
    a._update_hot_pairs([pair])
    t1 = a._hot_pairs[key][2]
    assert t1 > t0
    assert len(a._hot_pairs) == 1


def test_evicts_old_pairs():
    a = _agent(ttl_seconds=1)
    pair = (_market("kalshi", "KX-OLD"), _market("polymarket", "pm-old"))
    a._update_hot_pairs([pair])
    assert len(a._hot_pairs) == 1
    time.sleep(1.2)
    # Adding a different pair triggers TTL sweep on the old one
    new_pair = (_market("kalshi", "KX-NEW"), _market("polymarket", "pm-new"))
    a._update_hot_pairs([new_pair])
    keys = list(a._hot_pairs.keys())
    assert keys == [("kalshi", "KX-NEW", "polymarket", "pm-new")]


def test_keeps_multiple_distinct_pairs():
    a = _agent()
    pairs = [
        (_market("kalshi", "KX-A"), _market("polymarket", "pm-a")),
        (_market("kalshi", "KX-B"), _market("polymarket", "pm-b")),
        (_market("kalshi", "KX-C"), _market("polymarket", "pm-c")),
    ]
    a._update_hot_pairs(pairs)
    assert len(a._hot_pairs) == 3


def test_pair_key_distinguishes_swapped_orders():
    """A pair with markets in opposite order is a DIFFERENT key.

    This is intentional — the matcher orders pairs deterministically (by
    leg assignment after detect_arb), so we trust that order. If we ever
    see an unexpected duplicate, it would surface in the hot-pair count
    as 2 rather than 1, which is visible in logs.
    """
    a = _agent()
    p1 = (_market("kalshi", "KX-FOO"), _market("polymarket", "pm-bar"))
    p2 = (_market("polymarket", "pm-bar"), _market("kalshi", "KX-FOO"))
    a._update_hot_pairs([p1, p2])
    assert len(a._hot_pairs) == 2
