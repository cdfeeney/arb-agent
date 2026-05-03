"""Tests for startup reconciliation.

Scenarios:
  * No open orders → clean noop
  * Order is 'submitted' on DB, exchange says filled → DB updates
  * Order is 'submitted', exchange says cancelled → DB updates
  * Order is 'submitted', exchange still says submitted → defensive cancel
  * Order is 'pending' with NO external_order_id → emergency halt
  * Naked leg detected (one filled, other cancelled in same correlation)
    → emergency halt
"""

from __future__ import annotations

import asyncio
import os
import tempfile

import aiosqlite
import pytest

from src.exec import SimSpec, SimulatedExchange, init_orders_schema
from src.exec.reconciler import reconcile_open_orders


@pytest.fixture
async def db_path():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    await init_orders_schema(path)
    try:
        yield path
    finally:
        os.unlink(path)


async def _insert_order(
    db_path: str,
    *,
    correlation_id: str,
    leg: str,
    platform: str,
    status: str,
    external_order_id: str | None,
    contracts_filled: float = 0.0,
) -> int:
    async with aiosqlite.connect(db_path) as db:
        cur = await db.execute(
            """INSERT INTO orders (
                correlation_id, paper_trade_id, pair_id, leg, platform,
                ticker, side, order_type, price_limit, contracts_intended,
                contracts_filled, status, external_order_id, idempotency_key,
                execution_mode
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                correlation_id, 1, "PAIR/yes|PAIR/no", leg, platform,
                "TICKER", "buy", "ioc", 0.50, 5.0, contracts_filled,
                status, external_order_id, f"idem-{correlation_id}-{leg}",
                "live",
            ),
        )
        await db.commit()
        return cur.lastrowid


async def _get_status(db_path: str, order_id: int) -> dict:
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM orders WHERE id = ?", (order_id,))
        return dict(await cur.fetchone())


@pytest.mark.asyncio
async def test_no_open_orders_is_noop(db_path):
    halts: list[str] = []
    async def fake_halt(_db, *, reason):
        halts.append(reason)
    report = await reconcile_open_orders(
        db_path, exchanges={}, emergency_halt_fn=fake_halt,
    )
    assert report.checked == 0
    assert report.halts_triggered == 0
    assert halts == []


@pytest.mark.asyncio
async def test_submitted_with_filled_at_exchange_updates(db_path):
    """DB says submitted, exchange says filled — reconcile both DB rows
    so the pair is recognized as a complete hedge, no halt."""
    yes_id = await _insert_order(
        db_path, correlation_id="C1", leg="yes", platform="kalshi",
        status="submitted", external_order_id="kalshi-1",
    )
    no_id = await _insert_order(
        db_path, correlation_id="C1", leg="no", platform="polymarket",
        status="submitted", external_order_id="poly-1",
    )
    exchanges = {
        "kalshi": SimulatedExchange("kalshi", SimSpec(fill_status="filled")),
        "polymarket": SimulatedExchange("polymarket", SimSpec(fill_status="filled")),
    }
    # SimulatedExchange.get_order needs the order in its internal _orders;
    # since we didn't place_order, simulate by registering directly.
    for ex_name, ext in [("kalshi", "kalshi-1"), ("polymarket", "poly-1")]:
        ex = exchanges[ex_name]
        ex._orders[ext] = {
            "plan": _FakePlan(),
            "submit_time": asyncio.get_event_loop().time() - 10,
        }
    halts: list[str] = []
    async def fake_halt(_db, *, reason):
        halts.append(reason)
    report = await reconcile_open_orders(
        db_path, exchanges=exchanges, emergency_halt_fn=fake_halt,
    )
    assert report.checked == 2
    assert report.updated_to_filled == 2
    assert report.halts_triggered == 0
    assert halts == []
    yes_row = await _get_status(db_path, yes_id)
    no_row = await _get_status(db_path, no_id)
    assert yes_row["status"] == "filled"
    assert no_row["status"] == "filled"


@pytest.mark.asyncio
async def test_pending_without_external_id_halts(db_path):
    """Order is 'pending' but external_order_id is NULL — we have no way
    to verify exchange state, so halt."""
    await _insert_order(
        db_path, correlation_id="C2", leg="yes", platform="kalshi",
        status="pending", external_order_id=None,
    )
    exchanges = {"kalshi": SimulatedExchange("kalshi")}
    halts: list[str] = []
    async def fake_halt(_db, *, reason):
        halts.append(reason)
    report = await reconcile_open_orders(
        db_path, exchanges=exchanges, emergency_halt_fn=fake_halt,
    )
    assert report.halts_triggered == 1
    assert any("NO external_order_id" in h for h in halts)


@pytest.mark.asyncio
async def test_naked_leg_detected_at_restart_halts(db_path):
    """One leg filled at exchange, other cancelled — naked exposure.
    Don't auto-defend (price has moved); halt for human review."""
    await _insert_order(
        db_path, correlation_id="C3", leg="yes", platform="kalshi",
        status="submitted", external_order_id="kalshi-3",
    )
    await _insert_order(
        db_path, correlation_id="C3", leg="no", platform="polymarket",
        status="submitted", external_order_id="poly-3",
    )
    exchanges = {
        "kalshi": SimulatedExchange("kalshi", SimSpec(fill_status="filled")),
        "polymarket": SimulatedExchange("polymarket", SimSpec(fill_status="cancelled")),
    }
    for ex_name, ext in [("kalshi", "kalshi-3"), ("polymarket", "poly-3")]:
        ex = exchanges[ex_name]
        ex._orders[ext] = {
            "plan": _FakePlan(),
            "submit_time": asyncio.get_event_loop().time() - 10,
        }
    halts: list[str] = []
    async def fake_halt(_db, *, reason):
        halts.append(reason)
    report = await reconcile_open_orders(
        db_path, exchanges=exchanges, emergency_halt_fn=fake_halt,
    )
    assert report.naked_legs_defended == 1
    assert report.halts_triggered == 1
    assert any("NAKED LEG" in h for h in halts)


@pytest.mark.asyncio
async def test_resting_order_defensively_cancelled(db_path):
    """Order is still resting on exchange after restart — cancel it.
    We don't trust unattended live orders."""
    yes_id = await _insert_order(
        db_path, correlation_id="C4", leg="yes", platform="kalshi",
        status="submitted", external_order_id="kalshi-4",
    )
    exchanges = {
        "kalshi": SimulatedExchange("kalshi", SimSpec(fill_status="submitted")),
    }
    exchanges["kalshi"]._orders["kalshi-4"] = {
        "plan": _FakePlan(),
        "submit_time": asyncio.get_event_loop().time() - 10,
    }
    halts: list[str] = []
    async def fake_halt(_db, *, reason):
        halts.append(reason)
    report = await reconcile_open_orders(
        db_path, exchanges=exchanges, emergency_halt_fn=fake_halt,
    )
    assert report.cancelled_resting == 1
    assert report.halts_triggered == 0
    yes_row = await _get_status(db_path, yes_id)
    assert yes_row["status"] == "cancelled"


class _FakePlan:
    """Minimum stand-in for OrderPlan that SimulatedExchange.get_order needs."""
    price_limit = 0.50
    contracts = 5.0
