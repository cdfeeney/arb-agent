"""Tests for the dynamic Kelly fraction (profit ideas #4).

Lower-edge arbs use a smaller fraction of Kelly to protect against
variance flipping a thin positive-EV arb into negative-EV.
"""

from __future__ import annotations

from src.engine.sizing import _dynamic_kelly_fraction, size_position


def test_low_edge_clamps_to_half():
    f = _dynamic_kelly_fraction(
        edge=0.02, base_fraction=1.0, low_edge=0.03, high_edge=0.07,
    )
    assert f == 0.5


def test_high_edge_returns_base():
    f = _dynamic_kelly_fraction(
        edge=0.10, base_fraction=1.0, low_edge=0.03, high_edge=0.07,
    )
    assert f == 1.0


def test_at_low_threshold_is_half():
    f = _dynamic_kelly_fraction(
        edge=0.03, base_fraction=1.0, low_edge=0.03, high_edge=0.07,
    )
    assert f == 0.5


def test_at_high_threshold_is_full():
    f = _dynamic_kelly_fraction(
        edge=0.07, base_fraction=1.0, low_edge=0.03, high_edge=0.07,
    )
    assert f == 1.0


def test_midpoint_is_three_quarters():
    """5% edge midway between 3% and 7% → fraction = 0.5 + 0.5×0.5 = 0.75"""
    f = _dynamic_kelly_fraction(
        edge=0.05, base_fraction=1.0, low_edge=0.03, high_edge=0.07,
    )
    assert abs(f - 0.75) < 1e-9


def test_respects_base_fraction_scaling():
    """If user sets kelly_fraction=0.5, full Kelly = 0.5, half = 0.25"""
    f = _dynamic_kelly_fraction(
        edge=0.02, base_fraction=0.5, low_edge=0.03, high_edge=0.07,
    )
    assert abs(f - 0.25) < 1e-9


def _opp(edge_pct: float):
    """Build a test opportunity with controlled edge_pct."""
    # Pick prices that give the desired edge: yes + no = 1 - edge_pct
    yes_price = (1 - edge_pct) / 2
    no_price = (1 - edge_pct) / 2
    return {
        "pair_id": "TEST",
        "profit_pct": edge_pct,
        "implied_sum": yes_price + no_price,
        "buy_yes": {
            "platform": "kalshi", "ticker": "KX-FOO",
            "yes_price": yes_price, "no_price": no_price,
            "volume": 10_000,
            "yes_bid": yes_price - 0.01, "no_bid": no_price - 0.01,
            "yes_bid_depth_usd": 100.0, "no_bid_depth_usd": 100.0,
        },
        "buy_no": {
            "platform": "polymarket", "ticker": "pm-bar",
            "yes_price": yes_price, "no_price": no_price,
            "volume": 10_000,
            "yes_bid": yes_price - 0.01, "no_bid": no_price - 0.01,
            "yes_bid_depth_usd": 100.0, "no_bid_depth_usd": 100.0,
        },
    }


_CFG = {
    "bankroll": 100,
    "max_position_pct": 0.50,    # high enough that bankroll cap doesn't bind
    "kelly_fraction": 1.0,
    "min_bet": 0.5,
    "max_bet": 50,               # high enough that max_bet doesn't bind on test cases
    "liquidity_cap_pct": 0.50,
    "book_depth_fraction": 0.50,
    "min_bid_depth_usd": 0,      # disable thin-book rejection for this test
    "kelly_low_edge_threshold": 0.03,
    "kelly_high_edge_threshold": 0.07,
    "fees": {},
}


def test_low_edge_reduces_position_vs_high_edge():
    """A 3% arb gets half the position size that a 7% arb does, controlling
    for everything else. (Both well below max_bet so Kelly is binding.)"""
    s_low = size_position(_opp(0.03), _CFG)
    s_high = size_position(_opp(0.07), _CFG)
    # Both should produce non-zero positions
    assert s_low["bet_size"] > 0
    assert s_high["bet_size"] > 0
    # Half-Kelly at 3%, full at 7% — but Kelly raw also scales with edge.
    # Actual ratio: (0.03 × 0.5) / (0.07 × 1.0) = 0.015 / 0.07 ≈ 0.214
    # So low-edge stake should be ~21% of high-edge stake.
    ratio = s_low["bet_size"] / s_high["bet_size"]
    assert ratio < 0.4, f"low/high ratio={ratio:.3f}, expected ≪ 1"
