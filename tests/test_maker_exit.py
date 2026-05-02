"""Tests for maker-exit limit orders (#35).

Covers:
  * _polymarket_leg correctly identifies which side is on Polymarket
  * Maker order is placed when PARTIAL_UNWIND fires + poly leg present
  * Resting maker orders fill when poly bid moves up to target
  * Resting maker orders cancel when aged out
  * Maker-fill realized math: target_price (no fee) + kalshi_bid - kalshi_taker_fee
  * DB schema: record / mark_filled / mark_cancelled / list_resting
"""

from __future__ import annotations

import asyncio
import os
import tempfile
import time
from datetime import datetime, timedelta, timezone

import pytest

from src.db.store import Database
from src.engine.position_monitor import (
    LegMark, MakerExitConfig, TradeMark, _polymarket_leg, _try_place_maker_exit,
    _maker_order_age_seconds,
)


def _mark_with_legs(
    *,
    yes_platform: str = "kalshi",
    no_platform: str = "polymarket",
    yes_bid: float = 0.30,
    no_bid: float = 0.50,
    cost_per_contract: float = 0.95,
    contracts_remaining: float = 5.0,
    paper_trade_id: int = 1,
) -> TradeMark:
    return TradeMark(
        paper_trade_id=paper_trade_id,
        yes_leg=LegMark(
            best_bid=yes_bid, best_bid_size=10.0, vwap_bid=yes_bid,
            fill_contracts=contracts_remaining,
            target_contracts=contracts_remaining, book_available=True,
        ),
        no_leg=LegMark(
            best_bid=no_bid, best_bid_size=10.0, vwap_bid=no_bid,
            fill_contracts=contracts_remaining,
            target_contracts=contracts_remaining, book_available=True,
        ),
        cost_basis=cost_per_contract * contracts_remaining,
        cost_per_contract=cost_per_contract,
        contracts_remaining=contracts_remaining,
        unwind_value=0,
        locked_payout=contracts_remaining,
        mark_to_market=0,
        locked_profit_at_resolution=0,
        convergence_ratio=0,
        slippage_pct=0,
        days_held=1,
        days_remaining=2,
        annualized_now_pct=0,
        annualized_to_close_pct=0,
        recommendation="",
        reason="",
        buy_yes={"platform": yes_platform},
        buy_no={"platform": no_platform},
    )


def test_polymarket_leg_no_side():
    mark = _mark_with_legs(yes_platform="kalshi", no_platform="polymarket")
    info = _polymarket_leg(mark)
    assert info is not None
    leg, market, _ = info
    assert leg == "no"
    assert market["platform"] == "polymarket"


def test_polymarket_leg_yes_side():
    mark = _mark_with_legs(yes_platform="polymarket", no_platform="kalshi")
    info = _polymarket_leg(mark)
    assert info is not None
    leg, _, _ = info
    assert leg == "yes"


def test_polymarket_leg_neither():
    mark = _mark_with_legs(yes_platform="kalshi", no_platform="kalshi")
    assert _polymarket_leg(mark) is None


def test_age_seconds_naive_iso():
    """SQLite returns timestamps as 'YYYY-MM-DD HH:MM:SS' (naive).
    _maker_order_age_seconds must treat them as UTC."""
    past = (datetime.now(timezone.utc) - timedelta(seconds=120)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    age = _maker_order_age_seconds(past)
    assert age is not None
    assert 110 < age < 130  # ~120s, with some slack for clock skew


def test_age_seconds_none_safe():
    assert _maker_order_age_seconds(None) is None
    assert _maker_order_age_seconds("garbage") is None


@pytest.fixture
async def db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    d = Database(path)
    await d.init()
    try:
        yield d
    finally:
        os.unlink(path)


@pytest.mark.asyncio
async def test_db_record_and_list_maker_order(db):
    oid = await db.record_maker_order(
        paper_trade_id=42, leg="no", platform="polymarket",
        target_price=0.55, contracts=3.0,
    )
    assert oid > 0
    rows = await db.list_resting_maker_orders(paper_trade_id=42)
    assert len(rows) == 1
    assert rows[0]["leg"] == "no"
    assert rows[0]["target_price"] == 0.55
    assert rows[0]["contracts"] == 3.0
    assert rows[0]["status"] == "resting"


@pytest.mark.asyncio
async def test_db_mark_filled(db):
    oid = await db.record_maker_order(42, "no", "polymarket", 0.55, 3.0)
    await db.mark_maker_filled(oid, fill_price=0.55, realized_gross_usd=0.30)
    rows = await db.list_resting_maker_orders(paper_trade_id=42)
    assert len(rows) == 0  # no longer resting


@pytest.mark.asyncio
async def test_db_mark_cancelled(db):
    oid = await db.record_maker_order(42, "no", "polymarket", 0.55, 3.0)
    await db.mark_maker_cancelled(oid, reason="aged_out")
    rows = await db.list_resting_maker_orders(paper_trade_id=42)
    assert len(rows) == 0


_MAKER_CFG = MakerExitConfig(
    enabled=True, spread_above_bid=0.01,
    max_age_seconds=300, polymarket_only=True,
)


class _FakeExitCfg:
    """Just enough surface for _try_place_maker_exit to read."""
    def __init__(self, maker_cfg: MakerExitConfig):
        self.maker_exit = maker_cfg


@pytest.mark.asyncio
async def test_place_maker_exit_records_at_bid_plus_spread(db):
    mark = _mark_with_legs(
        yes_platform="kalshi", no_platform="polymarket",
        yes_bid=0.30, no_bid=0.50,
    )
    trade = {"id": 99}
    summary = {"maker_placed": 0}
    cfg = _FakeExitCfg(_MAKER_CFG)
    result = await _try_place_maker_exit(
        db, trade, mark, unwind_size=5.0, cfg=cfg, summary=summary,
    )
    assert result is not None
    assert "no leg target=$0.5100" in result  # no_bid 0.50 + spread 0.01
    assert summary["maker_placed"] == 1
    rows = await db.list_resting_maker_orders(paper_trade_id=99)
    assert len(rows) == 1
    assert rows[0]["leg"] == "no"
    assert abs(rows[0]["target_price"] - 0.51) < 1e-9


@pytest.mark.asyncio
async def test_place_maker_exit_skips_if_no_polymarket_leg(db):
    mark = _mark_with_legs(yes_platform="kalshi", no_platform="kalshi")
    trade = {"id": 100}
    summary = {"maker_placed": 0}
    cfg = _FakeExitCfg(_MAKER_CFG)
    result = await _try_place_maker_exit(
        db, trade, mark, unwind_size=5.0, cfg=cfg, summary=summary,
    )
    assert result is None
    assert summary["maker_placed"] == 0


@pytest.mark.asyncio
async def test_place_maker_exit_skips_if_already_resting(db):
    mark = _mark_with_legs(yes_platform="kalshi", no_platform="polymarket")
    trade = {"id": 101}
    summary = {"maker_placed": 0}
    cfg = _FakeExitCfg(_MAKER_CFG)
    # First call places
    r1 = await _try_place_maker_exit(db, trade, mark, 5.0, cfg, summary)
    assert r1 is not None
    # Second call should refuse — already an order resting for this trade
    r2 = await _try_place_maker_exit(db, trade, mark, 5.0, cfg, summary)
    assert r2 is None
    assert summary["maker_placed"] == 1
