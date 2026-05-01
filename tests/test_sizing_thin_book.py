"""Tests for sizing.py thin-bid-book rejection (profit ideas #2).

Verifies the layered defense:
  * Zero/missing bid depth                  -> reject "missing_bid_depth"
  * Bid depth below min_bid_depth_usd floor -> reject "thin_bid_depth"
  * Bid depth above floor                   -> proceed, sizing applied
"""

from __future__ import annotations

import pytest

from src.engine.sizing import size_position


def _opp(yes_bid_depth: float, no_bid_depth: float):
    return {
        "pair_id": "TEST",
        "profit_pct": 0.05,
        "implied_sum": 0.95,
        "buy_yes": {
            "platform": "kalshi", "ticker": "KX-FOO",
            "yes_price": 0.54, "no_price": 0.46,
            "volume": 5000,
            "yes_bid": 0.53, "no_bid": 0.45,
            "yes_bid_depth_usd": yes_bid_depth, "no_bid_depth_usd": 100.0,
        },
        "buy_no": {
            "platform": "polymarket", "ticker": "pm-bar",
            "yes_price": 0.59, "no_price": 0.41,
            "volume": 5000,
            "yes_bid": 0.58, "no_bid": 0.40,
            "yes_bid_depth_usd": 100.0, "no_bid_depth_usd": no_bid_depth,
        },
    }


_CFG = {
    "bankroll": 100,
    "max_position_pct": 0.10,
    "kelly_fraction": 1.0,
    "min_bet": 0.5,
    "max_bet": 10,
    "liquidity_cap_pct": 0.10,
    "book_depth_fraction": 0.25,
    "min_bid_depth_usd": 20,
    "fees": {},
}


def test_rejects_zero_bid_depth():
    """Zero on the unwind side fires the older missing_bid_depth check."""
    sizing = size_position(_opp(yes_bid_depth=0, no_bid_depth=50), _CFG)
    assert sizing["bet_size"] == 0
    assert sizing["limiting_rule"] == "REJECTED:missing_bid_depth"


def test_rejects_thin_bid_depth_below_floor():
    """$5 bid depth on yes-leg < $20 floor → reject as thin sub-outcome."""
    sizing = size_position(_opp(yes_bid_depth=5, no_bid_depth=50), _CFG)
    assert sizing["bet_size"] == 0
    assert sizing["limiting_rule"] == "REJECTED:thin_bid_depth"


def test_rejects_thin_on_either_leg():
    """Floor must apply to BOTH legs — thin no-leg also rejects."""
    sizing = size_position(_opp(yes_bid_depth=50, no_bid_depth=10), _CFG)
    assert sizing["bet_size"] == 0
    assert sizing["limiting_rule"] == "REJECTED:thin_bid_depth"


def test_passes_when_both_above_floor():
    """$25 / $30 bid depth > $20 floor → sizing proceeds normally."""
    sizing = size_position(_opp(yes_bid_depth=25, no_bid_depth=30), _CFG)
    assert sizing["bet_size"] > 0
    assert not sizing["limiting_rule"].startswith("REJECTED:")


def test_floor_is_configurable():
    """Lowering the floor to $1 lets a $5 bid book through."""
    cfg = {**_CFG, "min_bid_depth_usd": 1}
    sizing = size_position(_opp(yes_bid_depth=5, no_bid_depth=5), cfg)
    assert sizing["bet_size"] > 0
    assert not sizing["limiting_rule"].startswith("REJECTED:")
