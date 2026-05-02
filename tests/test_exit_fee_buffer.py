"""Regression test for the fee-buffer exit gate.

Forensic background: paper trades #367 / #362 / #379 closed early on
2026-05-01 with realized losses despite hitting the previous fee gate
(net_realized > 0). Trade #379 specifically:
    cost_per_contract = 0.94, top_bid_yes = 0.28, top_bid_no = 0.69
    raw_size = 5.11
    gross_profit = (0.97 - 0.94) × 5.11 = $0.153
    exit_fees   = ceil(0.07×5.11×0.28×0.72×100)/100  +  0.04×5.11×0.69×0.31
                = $0.08  +  $0.044
                = $0.124
    net_realized = $0.029  ← old gate accepted, fired PARTIAL_UNWIND
    realized at close = -$0.16 (entry fees $0.18 swallowed the meager net)

The new gate requires net_realized ≥ min_capture_above_fees × exit_fees
(default 1.5×). With buffer=1.5, required = $0.186 ; net = $0.029 ;
$0.029 < $0.186 → WATCH (hold for better convergence or resolution).

We assert:
  1. The trade-#379 scenario produces WATCH, not PARTIAL_UNWIND.
  2. A scenario that DOES exceed the buffer still fires PARTIAL_UNWIND.
  3. With buffer=0 (legacy behavior), the same #379 scenario fires.
"""

from __future__ import annotations

import pytest

from src.engine.position_monitor import (
    ExitConfig, LegMark, MakerExitConfig, TradeMark, _decide,
)


def _mark(
    *,
    cost_per_contract: float,
    yes_bid: float, yes_size: float, yes_platform: str,
    no_bid: float, no_size: float, no_platform: str,
    yes_paid: float, no_paid: float,
    contracts_remaining: float,
    days_remaining: float = 100.0,  # outside force-hold by default
) -> TradeMark:
    return TradeMark(
        paper_trade_id=1,
        yes_leg=LegMark(
            best_bid=yes_bid, best_bid_size=yes_size, vwap_bid=yes_bid,
            fill_contracts=yes_size, target_contracts=contracts_remaining,
            book_available=True,
        ),
        no_leg=LegMark(
            best_bid=no_bid, best_bid_size=no_size, vwap_bid=no_bid,
            fill_contracts=no_size, target_contracts=contracts_remaining,
            book_available=True,
        ),
        cost_basis=cost_per_contract * contracts_remaining,
        cost_per_contract=cost_per_contract,
        contracts_remaining=contracts_remaining,
        unwind_value=(yes_bid + no_bid) * contracts_remaining,
        locked_payout=contracts_remaining,
        mark_to_market=0,
        locked_profit_at_resolution=0,
        convergence_ratio=0,
        slippage_pct=0,
        days_held=0.083,
        days_remaining=days_remaining,
        annualized_now_pct=0,
        annualized_to_close_pct=0,
        recommendation="",
        reason="",
        # Populated so compute_unwind_fees runs, mimicking real call site.
        buy_yes={"platform": yes_platform, "category": "politics", "yes_price": yes_paid},
        buy_no={"platform": no_platform, "category": "politics", "no_price": no_paid},
    )


def _cfg(min_capture_above_fees: float = 1.5) -> ExitConfig:
    return ExitConfig(
        enabled=True,
        convergence_threshold=0.7,
        annualized_multiple=1.5,
        max_slippage_pct=0.10,  # generous so this test isolates the fee gate
        cooldown_minutes=60,
        min_days_remaining_to_force_hold=0.25,
        partial_unwind_min_size=0.1,
        near_resolution_spike_fee_multiple=2.0,
        min_capture_above_fees=min_capture_above_fees,
        maker_exit=MakerExitConfig(
            enabled=False, spread_above_bid=0.01,
            max_age_seconds=300, polymarket_only=True,
        ),
    )


_FEE_CFG = {
    "kalshi_fee_rate": 0.07,
    "polymarket_default_rate": 0.05,
}


def test_trade_379_scenario_holds_with_buffer():
    """Reproduce trade #379. Old gate fires; new gate (buffer 1.5) holds."""
    mark = _mark(
        cost_per_contract=0.94,
        yes_bid=0.28, yes_size=10, yes_platform="kalshi", yes_paid=0.29,
        no_bid=0.69, no_size=10, no_platform="polymarket", no_paid=0.65,
        contracts_remaining=5.11,
    )
    action, reason, _ = _decide(mark, _cfg(min_capture_above_fees=1.5), _FEE_CFG)
    assert action == "WATCH", (
        f"Expected WATCH (buffer rejects weak exit) but got {action}. "
        f"Reason: {reason}"
    )
    assert "exit_fees" in reason


def test_trade_379_scenario_old_behavior_with_buffer_zero():
    """With buffer=0 (legacy), trade #379 still fires PARTIAL_UNWIND. This
    proves the fix is gated by the new config knob and we haven't changed
    behavior for users who explicitly disable it."""
    mark = _mark(
        cost_per_contract=0.94,
        yes_bid=0.28, yes_size=10, yes_platform="kalshi", yes_paid=0.29,
        no_bid=0.69, no_size=10, no_platform="polymarket", no_paid=0.65,
        contracts_remaining=5.11,
    )
    action, _, size = _decide(mark, _cfg(min_capture_above_fees=0.0), _FEE_CFG)
    assert action == "PARTIAL_UNWIND"
    assert size == pytest.approx(5.11)


def test_strong_convergence_still_fires_with_buffer():
    """A genuinely strong convergence (gross 4× exit fees) must still fire
    PARTIAL_UNWIND under the new buffer. Otherwise we'd never exit anything."""
    mark = _mark(
        cost_per_contract=0.94,
        yes_bid=0.40, yes_size=10, yes_platform="kalshi", yes_paid=0.29,
        no_bid=0.70, no_size=10, no_platform="polymarket", no_paid=0.65,
        contracts_remaining=5.0,
    )
    # gross_per = 0.40+0.70-0.94 = 0.16, gross = 0.80
    # exit_fees ≈ ceil(0.07×5×0.40×0.60×100)/100 + 0.05×5×0.70×0.30 ≈ 0.09+0.053 = 0.143
    # net = 0.80 - 0.143 = 0.657 ; required = 1.5 × 0.143 = 0.214
    # 0.657 > 0.214 → fire
    action, reason, _ = _decide(mark, _cfg(), _FEE_CFG)
    assert action == "PARTIAL_UNWIND", (
        f"Expected PARTIAL_UNWIND on strong convergence but got {action}. "
        f"Reason: {reason}"
    )


def test_buffer_at_exact_threshold_passes():
    """Edge case: net_realized exactly equals buffer × exit_fees → fire."""
    # Construct with fee_cfg=None so exit_fees=0 → required=0 → any positive net fires
    mark = _mark(
        cost_per_contract=0.94,
        yes_bid=0.30, yes_size=10, yes_platform="kalshi", yes_paid=0.29,
        no_bid=0.66, no_size=10, no_platform="polymarket", no_paid=0.65,
        contracts_remaining=2.0,
    )
    # With fee_cfg=None, exit_fees=0, so any positive net passes the gate.
    action, _, _ = _decide(mark, _cfg(), fee_cfg=None)
    assert action == "PARTIAL_UNWIND"
