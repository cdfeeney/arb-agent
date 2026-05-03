"""Tests for portfolio-level stop-loss watcher."""

from __future__ import annotations

import os
import tempfile

import aiosqlite
import pytest

from src.exec.stop_loss import check_portfolio_stop_loss, cumulative_realized_usd


@pytest.fixture
async def db_path():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    async with aiosqlite.connect(path) as db:
        # Minimal paper_trades schema for test
        await db.execute(
            """CREATE TABLE paper_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair_id TEXT NOT NULL,
                status TEXT DEFAULT 'open',
                realized_profit_usd REAL,
                partial_realized_usd REAL DEFAULT 0,
                pair_quality TEXT DEFAULT 'good'
            )"""
        )
        await db.commit()
    try:
        yield path
    finally:
        os.unlink(path)


async def _insert(db_path, *, status, realized=None, partial=0.0, quality="good"):
    async with aiosqlite.connect(db_path) as db:
        await db.execute(
            """INSERT INTO paper_trades
               (pair_id, status, realized_profit_usd, partial_realized_usd, pair_quality)
               VALUES (?, ?, ?, ?, ?)""",
            ("PAIR", status, realized, partial, quality),
        )
        await db.commit()


@pytest.mark.asyncio
async def test_cumulative_sums_closed_and_partial(db_path):
    await _insert(db_path, status="closed", realized=2.50)
    await _insert(db_path, status="closed", realized=-0.30)
    await _insert(db_path, status="open", partial=1.20)
    total = await cumulative_realized_usd(db_path)
    assert abs(total - 3.40) < 0.001


@pytest.mark.asyncio
async def test_cumulative_excludes_archived(db_path):
    await _insert(db_path, status="closed", realized=5.0)
    await _insert(db_path, status="archived", realized=100.0)  # excluded by status
    await _insert(db_path, status="closed", realized=2.0, quality="broken_neg_risk_sub")  # excluded by quality
    total = await cumulative_realized_usd(db_path)
    assert abs(total - 5.0) < 0.001


@pytest.mark.asyncio
async def test_threshold_zero_or_positive_disables_check(db_path):
    await _insert(db_path, status="closed", realized=-500.0)  # would breach any negative
    halts: list[str] = []
    async def fake_halt(_db, *, reason):
        halts.append(reason)
    cumul, halted = await check_portfolio_stop_loss(
        db_path, 0.0, emergency_halt_fn=fake_halt,
    )
    assert halted is False
    assert halts == []


@pytest.mark.asyncio
async def test_breach_triggers_halt(db_path):
    await _insert(db_path, status="closed", realized=-50.0)
    await _insert(db_path, status="closed", realized=-80.0)
    halts: list[str] = []
    async def fake_halt(_db, *, reason):
        halts.append(reason)
    cumul, halted = await check_portfolio_stop_loss(
        db_path, -120.0, emergency_halt_fn=fake_halt,
    )
    assert halted is True
    assert abs(cumul - (-130.0)) < 0.001
    assert any("portfolio_stop_loss" in h for h in halts)


@pytest.mark.asyncio
async def test_above_threshold_no_halt(db_path):
    await _insert(db_path, status="closed", realized=-50.0)
    halts: list[str] = []
    async def fake_halt(_db, *, reason):
        halts.append(reason)
    cumul, halted = await check_portfolio_stop_loss(
        db_path, -120.0, emergency_halt_fn=fake_halt,
    )
    assert halted is False
    assert halts == []
