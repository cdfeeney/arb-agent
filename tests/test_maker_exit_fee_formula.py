"""Regression test: maker-exit Kalshi fee uses the canonical formula.

Bug fixed 2026-05-03: position_monitor._handle_resting_maker inlined the
Kalshi taker fee as `rate × contracts × price`, dropping the `(1-price)`
factor and the cent-ceiling rounding. For high-priced legs (~$0.80) this
overstated fees ~5× and turned real-money winners into phantom paper
losses (Kashkari trades 398–402 each booked -$0.13 on what was +$0.28).

This test pins the maker-exit fee math to the canonical kalshi_taker_fee()
in fees.py so the bug can't drift back in.
"""

from __future__ import annotations

from math import ceil

from src.engine.fees import kalshi_taker_fee


def test_canonical_fee_at_high_priced_leg():
    """At price=0.82, 9 contracts, rate=0.07: fee should be $0.10, not $0.52."""
    fee = kalshi_taker_fee(9, 0.82, 0.07)
    # Formula: ceil(0.07 * 9 * 0.82 * 0.18 * 100)/100 = ceil(9.30)/100 = $0.10
    assert fee == 0.10
    # The buggy formula (rate * contracts * price) produced $0.5166 — guard
    # against regression by asserting the result is dramatically smaller.
    buggy = 0.07 * 9 * 0.82
    assert fee < buggy / 4, f"fee {fee} suspiciously close to buggy formula {buggy}"


def test_canonical_fee_at_low_priced_leg():
    """At price=0.13, 9 contracts: fee should be $0.08."""
    # ceil(0.07 * 9 * 0.13 * 0.87 * 100)/100 = ceil(7.12)/100 = $0.08
    fee = kalshi_taker_fee(9, 0.13, 0.07)
    assert fee == 0.08


def test_position_monitor_uses_canonical_fee():
    """Sanity: the maker-exit module imports kalshi_taker_fee, not a copy."""
    from src.engine import position_monitor as pm

    assert pm.kalshi_taker_fee is kalshi_taker_fee, (
        "position_monitor must use the canonical fees.kalshi_taker_fee — "
        "any local re-implementation has historically had the (1-price) "
        "factor wrong"
    )


def test_kashkari_trade_398_round_trip_with_correct_fees():
    """Recompute trade 398's realized with the correct formula.

    Trade 398: bought 9 yes@0.12 (poly) + 9 no@0.80 (kalshi) for $8.28.
    Maker exit: sold 9 yes@0.16 (poly maker, 0 fee) + 9 no@0.82 (kalshi taker).
    Entry fees recorded at $0.1575 (entry only, both legs taker).
    """
    contracts = 9.0
    cost_per_contract = (0.12 * 9 + 0.80 * 9) / 9  # = 0.92
    sell_yes = 0.16
    sell_no = 0.82
    entry_fees = 0.1575

    gross_per_contract = sell_yes + sell_no - cost_per_contract  # 0.06
    gross_realized = gross_per_contract * contracts               # 0.54
    exit_fee = kalshi_taker_fee(contracts, sell_no, 0.07)         # 0.10

    partial_realized = round(gross_realized - exit_fee, 4)        # 0.44
    final_realized = round(partial_realized - entry_fees, 4)      # ~0.28

    assert exit_fee == 0.10
    assert partial_realized == 0.44
    # Final round-trip is positive — trade actually made money.
    assert final_realized > 0.27 and final_realized < 0.29
