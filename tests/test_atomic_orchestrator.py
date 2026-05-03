"""Tests for the atomic two-leg entry orchestrator.

Run with:  py -3 -m pytest tests/test_atomic_orchestrator.py -v

These tests exercise the decision tree without touching any real exchange
or external API. SimulatedExchange returns deterministic, configurable
fill states so each branch (happy / one-rejected / both-rejected /
naked-leg / per-leg-timeout) is verifiable.
"""

from __future__ import annotations

import asyncio
import os
import tempfile

import pytest

from src.exec import (
    SimSpec,
    SimulatedExchange,
    build_entry_plan,
    execute_atomic_entry,
    init_orders_schema,
)
from src.exec.order_state import list_orders_for_paper_trade


def _make_opp_sizing(contracts: float = 5.0):
    opp = {
        "pair_id": "TEST/yes|BAR/no",
        "profit_pct": 0.05,
        "implied_sum": 0.95,
        "buy_yes": {
            "platform": "kalshi", "ticker": "KXTEST",
            "question": "q1", "url": "u1",
            "yes_price": 0.54, "no_price": 0.46, "closes_at": None,
        },
        "buy_no": {
            "platform": "polymarket", "ticker": "pm-test",
            "question": "q2", "url": "u2",
            "yes_price": 0.59, "no_price": 0.41,
            "no_token": "TOKEN-NO", "closes_at": None,
        },
    }
    sizing = {
        "leg_yes": {"contracts": contracts, "usd": contracts * 0.54, "platform": "kalshi"},
        "leg_no": {"contracts": contracts, "usd": contracts * 0.41, "platform": "polymarket"},
        "net_profit": 0.20, "guaranteed_payout": contracts,
    }
    return opp, sizing


@pytest.fixture
async def db_path():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    await init_orders_schema(path)
    try:
        yield path
    finally:
        os.unlink(path)


@pytest.mark.asyncio
async def test_happy_path_both_filled(db_path):
    opp, sizing = _make_opp_sizing()
    plan = build_entry_plan(opp, sizing, paper_trade_id=1)
    exchanges = {
        "kalshi": SimulatedExchange("kalshi"),
        "polymarket": SimulatedExchange("polymarket"),
    }
    result = await execute_atomic_entry(
        plan=plan, exchanges=exchanges, db_path=db_path,
        naked_leg_timeout_seconds=0.5, per_leg_timeout_seconds=2.0,
    )
    assert result.success is True
    assert result.leg_yes.status == "filled"
    assert result.leg_no.status == "filled"
    assert result.leg_yes.filled_contracts == 5.0
    assert result.leg_no.filled_contracts == 5.0
    assert result.naked_leg_unwound is False

    rows = await list_orders_for_paper_trade(db_path, 1)
    assert len(rows) == 2
    assert {r["status"] for r in rows} == {"filled"}


@pytest.mark.asyncio
async def test_naked_leg_defended(db_path):
    """One leg fills, the other never does — orchestrator cancels the
    laggard and market-sells the filled leg's contracts."""
    opp, sizing = _make_opp_sizing()
    plan = build_entry_plan(opp, sizing, paper_trade_id=2)
    exchanges = {
        "kalshi": SimulatedExchange("kalshi", SimSpec(fill_status="filled")),
        # polymarket leg is stuck — never fills
        "polymarket": SimulatedExchange(
            "polymarket",
            SimSpec(fill_status="submitted",
                    market_sell_price_per_contract=0.30),
        ),
    }
    # The kalshi side fills immediately; we want naked-leg trigger fast
    result = await execute_atomic_entry(
        plan=plan, exchanges=exchanges, db_path=db_path,
        naked_leg_timeout_seconds=0.2, per_leg_timeout_seconds=2.0,
        poll_interval_seconds=0.05,
    )
    assert result.success is False
    assert result.naked_leg_unwound is True
    assert result.leg_yes.status == "filled"  # the one that filled
    assert result.leg_no.status == "cancelled"
    # kalshi market_sell default is 0.40/contract; cost was 0.54/contract;
    # 5 contracts: cost $2.70, recovered $2.00, pnl ~ -$0.70
    assert result.naked_leg_realized_usd < 0
    assert result.naked_leg_realized_usd > -3.0  # sanity bound

    rows = await list_orders_for_paper_trade(db_path, 2)
    assert len(rows) == 2
    statuses = {r["leg"]: r["status"] for r in rows}
    assert statuses["yes"] == "filled"
    assert statuses["no"] == "cancelled"


@pytest.mark.asyncio
async def test_per_leg_timeout_neither_filled(db_path):
    """Neither leg fills before per_leg_timeout — both cancelled, no naked leg."""
    opp, sizing = _make_opp_sizing()
    plan = build_entry_plan(opp, sizing, paper_trade_id=3)
    exchanges = {
        "kalshi": SimulatedExchange("kalshi", SimSpec(fill_status="submitted")),
        "polymarket": SimulatedExchange("polymarket", SimSpec(fill_status="submitted")),
    }
    result = await execute_atomic_entry(
        plan=plan, exchanges=exchanges, db_path=db_path,
        naked_leg_timeout_seconds=0.5, per_leg_timeout_seconds=0.3,
        poll_interval_seconds=0.05,
    )
    assert result.success is False
    assert result.naked_leg_unwound is False
    assert result.leg_yes.status == "cancelled"
    assert result.leg_no.status == "cancelled"
    assert "per_leg_timeout" in (result.error or "")

    rows = await list_orders_for_paper_trade(db_path, 3)
    assert {r["status"] for r in rows} == {"cancelled"}


@pytest.mark.asyncio
async def test_one_leg_rejected_at_place(db_path):
    """If polymarket rejects at place, kalshi (which accepted) is cancelled."""
    opp, sizing = _make_opp_sizing()
    plan = build_entry_plan(opp, sizing, paper_trade_id=4)
    exchanges = {
        "kalshi": SimulatedExchange("kalshi"),
        "polymarket": SimulatedExchange(
            "polymarket", SimSpec(accept=False, place_error="insufficient balance"),
        ),
    }
    result = await execute_atomic_entry(
        plan=plan, exchanges=exchanges, db_path=db_path,
        naked_leg_timeout_seconds=0.5, per_leg_timeout_seconds=1.0,
    )
    assert result.success is False
    assert result.naked_leg_unwound is False
    assert result.leg_yes.status == "cancelled"  # kalshi got cancelled
    assert result.leg_no.status == "failed"      # polymarket rejected

    rows = await list_orders_for_paper_trade(db_path, 4)
    statuses = {r["leg"]: r["status"] for r in rows}
    assert statuses["yes"] == "cancelled"
    assert statuses["no"] == "failed"


@pytest.mark.asyncio
async def test_both_rejected_at_place(db_path):
    opp, sizing = _make_opp_sizing()
    plan = build_entry_plan(opp, sizing, paper_trade_id=5)
    exchanges = {
        "kalshi": SimulatedExchange("kalshi", SimSpec(accept=False, place_error="auth")),
        "polymarket": SimulatedExchange("polymarket", SimSpec(accept=False, place_error="auth")),
    }
    result = await execute_atomic_entry(
        plan=plan, exchanges=exchanges, db_path=db_path,
    )
    assert result.success is False
    assert result.leg_yes.status == "failed"
    assert result.leg_no.status == "failed"

    rows = await list_orders_for_paper_trade(db_path, 5)
    assert {r["status"] for r in rows} == {"failed"}


@pytest.mark.asyncio
async def test_partial_fill_both_legs_full_size_succeeds(db_path):
    """Both legs report status='partial' but at the same fill_fraction —
    the hedge is intact at the partial size; no residual to unwind. The
    bug pre-fix: orchestrator polled forever waiting for status=='filled'
    and timed out on a perfectly hedged position."""
    opp, sizing = _make_opp_sizing(contracts=10.0)
    plan = build_entry_plan(opp, sizing, paper_trade_id=10)
    exchanges = {
        "kalshi": SimulatedExchange(
            "kalshi", SimSpec(fill_status="partial", fill_fraction=0.7),
        ),
        "polymarket": SimulatedExchange(
            "polymarket", SimSpec(fill_status="partial", fill_fraction=0.7),
        ),
    }
    result = await execute_atomic_entry(
        plan=plan, exchanges=exchanges, db_path=db_path,
        naked_leg_timeout_seconds=0.5, per_leg_timeout_seconds=2.0,
        poll_interval_seconds=0.05,
    )
    # Both legs at 7.0 contracts each → fully hedged at 7, no residual
    assert result.success is True, f"got error: {result.error}"
    assert result.leg_yes.status == "partial"
    assert result.leg_no.status == "partial"
    assert result.leg_yes.filled_contracts == 7.0
    assert result.leg_no.filled_contracts == 7.0
    assert result.naked_leg_unwound is False  # nothing to unwind
    assert result.naked_leg_realized_usd == 0.0


@pytest.mark.asyncio
async def test_partial_fill_unequal_unwinds_residual(db_path):
    """yes leg fills 100%, no leg fills 50% → residual on yes side must
    be market-sold to flatten naked exposure."""
    opp, sizing = _make_opp_sizing(contracts=10.0)
    plan = build_entry_plan(opp, sizing, paper_trade_id=11)
    exchanges = {
        "kalshi": SimulatedExchange("kalshi", SimSpec(fill_status="filled")),
        "polymarket": SimulatedExchange(
            "polymarket",
            SimSpec(fill_status="partial", fill_fraction=0.5,
                    market_sell_price_per_contract=0.30),
        ),
    }
    result = await execute_atomic_entry(
        plan=plan, exchanges=exchanges, db_path=db_path,
        naked_leg_timeout_seconds=0.5, per_leg_timeout_seconds=2.0,
        poll_interval_seconds=0.05,
    )
    # yes filled 10, no filled 5 → hedge 5, residual 5 on yes side
    assert result.leg_yes.filled_contracts == 10.0
    assert result.leg_no.filled_contracts == 5.0
    assert result.naked_leg_unwound is True, f"error: {result.error}"
    assert result.success is True  # residual was unwound cleanly
    assert "partial_hedge" in (result.error or "")


@pytest.mark.asyncio
async def test_naked_defense_branches_on_real_fills_not_status(db_path):
    """REGRESSION (audit 2026-05-03): _defend_naked_leg used to branch on
    status=='filled', but _has_real_fills accepts 'partial' too. When yes
    was 'partial' with real contracts and no was 'cancelled', the old
    code tagged 'no' as the filled leg and called market_sell with 0
    contracts — leaving the real Polymarket position naked. Fix: branch
    on _has_real_fills."""
    opp, sizing = _make_opp_sizing(contracts=10.0)
    plan = build_entry_plan(opp, sizing, paper_trade_id=13)
    exchanges = {
        # YES leg fills 70% as 'partial'
        "kalshi": SimulatedExchange(
            "kalshi",
            SimSpec(fill_status="partial", fill_fraction=0.7,
                    market_sell_price_per_contract=0.45),
        ),
        # NO leg cancels with zero fills
        "polymarket": SimulatedExchange(
            "polymarket", SimSpec(fill_status="cancelled"),
        ),
    }
    result = await execute_atomic_entry(
        plan=plan, exchanges=exchanges, db_path=db_path,
        naked_leg_timeout_seconds=0.5, per_leg_timeout_seconds=2.0,
        poll_interval_seconds=0.05,
    )
    # YES leg had 7 real partial-filled contracts. Defense should:
    # 1. Identify yes (not no) as the filled side
    # 2. Cancel no leg (already cancelled but be safe)
    # 3. Market-sell 7 contracts on kalshi
    assert result.success is False
    assert result.naked_leg_unwound is True, (
        f"naked unwind should fire for the partial-filled yes leg; "
        f"got error={result.error}"
    )
    assert result.leg_yes.filled_contracts == 7.0
    # naked_leg_realized_usd should be non-zero (real contracts sold)
    assert result.naked_leg_realized_usd != 0.0


@pytest.mark.asyncio
async def test_partial_fill_one_below_min_residual_treated_as_naked(db_path):
    """yes leg fills 100%, no leg fills 0.1 contracts (below 0.5
    MIN_RESIDUAL) → no leg is treated as having no real fill; naked-leg
    defense fires on the yes side."""
    opp, sizing = _make_opp_sizing(contracts=10.0)
    plan = build_entry_plan(opp, sizing, paper_trade_id=12)
    exchanges = {
        "kalshi": SimulatedExchange("kalshi", SimSpec(fill_status="filled")),
        "polymarket": SimulatedExchange(
            "polymarket",
            SimSpec(fill_status="partial", fill_fraction=0.01,  # 0.1c on 10c
                    market_sell_price_per_contract=0.30),
        ),
    }
    result = await execute_atomic_entry(
        plan=plan, exchanges=exchanges, db_path=db_path,
        naked_leg_timeout_seconds=0.5, per_leg_timeout_seconds=2.0,
        poll_interval_seconds=0.05,
    )
    # yes filled 10, no filled 0.1 (treated as zero) → naked leg
    assert result.success is False
    assert result.naked_leg_unwound is True
    assert result.leg_yes.status == "filled"


@pytest.mark.asyncio
async def test_idempotency_retry_does_not_duplicate(db_path):
    """Calling execute_atomic_entry twice with the same plan within the
    same ms-bucket should not produce duplicate orders rows — the unique
    idempotency_key constraint catches the second insert."""
    opp, sizing = _make_opp_sizing()
    plan = build_entry_plan(opp, sizing, paper_trade_id=6)
    exchanges = {
        "kalshi": SimulatedExchange("kalshi"),
        "polymarket": SimulatedExchange("polymarket"),
    }
    await execute_atomic_entry(
        plan=plan, exchanges=exchanges, db_path=db_path,
        per_leg_timeout_seconds=1.0,
    )
    await execute_atomic_entry(
        plan=plan, exchanges=exchanges, db_path=db_path,
        per_leg_timeout_seconds=1.0,
    )
    rows = await list_orders_for_paper_trade(db_path, 6)
    assert len(rows) == 2  # not 4
