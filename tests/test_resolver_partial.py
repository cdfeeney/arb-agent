"""Regression test: resolver must NOT overwrite partial_realized_usd on
trades that reached resolution after partial unwinds.

Bug history (audit finding 2026-05-04): _compute_realized used original
yes_contracts + yes_size_usd, ignoring contracts_remaining and the
already-banked partial_realized_usd. On a partially-unwound trade
reaching resolution, the resolver computed the full-position math and
overwrote realized_profit_usd, silently deleting the banked partials.
"""

from __future__ import annotations

import pytest

from src.agent.resolver import _compute_realized


def _trade(
    *,
    yes_contracts: float = 10.0,
    no_contracts: float = 10.0,
    yes_observed_price: float = 0.40,
    no_observed_price: float = 0.55,
    yes_size_usd: float | None = None,
    no_size_usd: float | None = None,
    contracts_remaining: float | None = None,
    partial_realized_usd: float = 0.0,
    fees_estimated_usd: float = 0.10,
) -> dict:
    if yes_size_usd is None:
        yes_size_usd = yes_contracts * yes_observed_price
    if no_size_usd is None:
        no_size_usd = no_contracts * no_observed_price
    if contracts_remaining is None:
        contracts_remaining = yes_contracts
    return {
        "yes_contracts": yes_contracts,
        "no_contracts": no_contracts,
        "yes_observed_price": yes_observed_price,
        "no_observed_price": no_observed_price,
        "yes_size_usd": yes_size_usd,
        "no_size_usd": no_size_usd,
        "contracts_remaining": contracts_remaining,
        "partial_realized_usd": partial_realized_usd,
        "fees_estimated_usd": fees_estimated_usd,
    }


def test_full_resolution_no_partials_unchanged():
    """Trade with no partial unwinds should compute as before."""
    t = _trade()  # 10 contracts, no partials, $0.95 cost/pair
    payout, profit = _compute_realized(t, yes_won=1, no_won=1)
    # YES leg paid: 10 × $1 = $10. NO leg lost: $0
    # cost = 10 × 0.95 = $9.50, fees $0.10
    assert payout == 10.0
    assert abs(profit - (10.0 - 9.50 - 0.10)) < 0.001  # = $0.40


def test_partial_unwind_then_resolution_preserves_banked_profit():
    """Trade had a partial unwind that banked $0.30, then 5 contracts
    remained and resolved. Total realized must include both."""
    t = _trade(
        yes_contracts=10.0, no_contracts=10.0,
        contracts_remaining=5.0,
        partial_realized_usd=0.30,
    )
    payout, profit = _compute_realized(t, yes_won=1, no_won=1)
    # Resolution side: 5 contracts remaining
    #   YES leg paid 5 × $1 = $5, NO leg lost
    #   remaining cost = 5 × 0.95 = $4.75
    #   prorated fees = $0.10 × (5/10) = $0.05
    #   resolution profit = $5 - $4.75 - $0.05 = $0.20
    # Plus banked partial: $0.30
    # Total profit: $0.50
    assert payout == 5.0
    assert abs(profit - 0.50) < 0.001


def test_full_unwind_no_resolution_remaining():
    """Edge: contracts_remaining=0 (fully unwound before resolution).
    All profit comes from partials; resolution adds nothing."""
    t = _trade(
        yes_contracts=10.0, no_contracts=10.0,
        contracts_remaining=0.0,
        partial_realized_usd=0.45,
    )
    payout, profit = _compute_realized(t, yes_won=1, no_won=1)
    # Nothing left to settle
    assert payout == 0.0
    # Resolution profit on 0 contracts: 0 - 0 - prorated 0 fees = 0
    assert abs(profit - 0.45) < 0.001


def test_partial_unwind_with_no_won():
    """When NO leg wins (yes_market resolves NO), the no_leg pays out."""
    t = _trade(
        yes_contracts=10.0, no_contracts=10.0,
        contracts_remaining=4.0,
        partial_realized_usd=0.20,
    )
    payout, profit = _compute_realized(t, yes_won=0, no_won=0)
    # Resolution: NO leg paid 4 × $1, YES leg lost
    # cost = 4 × 0.95 = 3.80, fees prorated = $0.04
    # resolution profit = 4 - 3.80 - 0.04 = $0.16
    # + partials $0.20 = $0.36
    assert payout == 4.0
    assert abs(profit - 0.36) < 0.001


def test_legacy_row_without_contracts_remaining_field():
    """Pre-migration rows have contracts_remaining = None (or absent).
    Resolver must fall back to yes_contracts as the remaining size."""
    t = _trade()
    t.pop("contracts_remaining", None)
    payout, profit = _compute_realized(t, yes_won=1, no_won=1)
    # Should behave exactly like full-resolution case
    assert payout == 10.0
    assert abs(profit - 0.40) < 0.001
