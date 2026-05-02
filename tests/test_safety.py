"""Tests for src/exec/safety.py.

Covers:
  * STOP file: create / detect / remove cycle, parent-dir creation
  * Daily live-order counter: atomic increments via UPSERT-RETURNING
  * safety_gate: STOP-first short-circuit, increment-first cap consumption,
                 cap-exceeded cascades to STOP file, fail-closed on DB error
  * Concurrency: N parallel gate calls cannot exceed the cap
  * KalshiExchange refuses real send when STOP file is present (integration)
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest

from src.exec.safety import (
    DEFAULT_STOP_FILE,
    create_stop_file,
    get_live_order_count_today,
    incr_live_order_counter,
    init_safety_schema,
    is_stopped,
    remove_stop_file,
    safety_gate,
)


@pytest.fixture
def stop_file_path(tmp_path: Path) -> str:
    return str(tmp_path / "STOP")


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    return str(tmp_path / "safety.db")


# ---------- STOP file ----------

def test_default_stop_file_is_absolute() -> None:
    """Anchored to repo root so cwd doesn't matter."""
    assert os.path.isabs(DEFAULT_STOP_FILE)


def test_is_stopped_returns_false_when_absent(stop_file_path: str) -> None:
    stopped, reason = is_stopped(stop_file_path)
    assert stopped is False
    assert reason is None


def test_create_then_is_stopped_reads_reason(stop_file_path: str) -> None:
    create_stop_file("custom reason here", stop_file_path)
    stopped, reason = is_stopped(stop_file_path)
    assert stopped is True
    assert "custom reason here" in (reason or "")


def test_remove_stop_file(stop_file_path: str) -> None:
    create_stop_file("x", stop_file_path)
    assert remove_stop_file(stop_file_path) is True
    assert os.path.exists(stop_file_path) is False
    assert remove_stop_file(stop_file_path) is False


def test_create_stop_file_creates_parent_dir(tmp_path: Path) -> None:
    nested = str(tmp_path / "a" / "b" / "STOP")
    create_stop_file("nested", nested)
    assert os.path.exists(nested)


# ---------- Atomic counter ----------

@pytest.mark.asyncio
async def test_counter_starts_at_zero(db_path: str) -> None:
    await init_safety_schema(db_path)
    assert await get_live_order_count_today(db_path) == 0


@pytest.mark.asyncio
async def test_counter_returns_post_increment_value(db_path: str) -> None:
    await init_safety_schema(db_path)
    assert await incr_live_order_counter(db_path) == 1
    assert await incr_live_order_counter(db_path) == 2
    assert await incr_live_order_counter(db_path) == 3
    assert await get_live_order_count_today(db_path) == 3


@pytest.mark.asyncio
async def test_concurrent_increments_are_unique(db_path: str) -> None:
    """RETURNING + SQLite write serialization → each concurrent caller
    receives a distinct monotonically increasing value. This is the
    property that makes the cap atomic across coroutines."""
    await init_safety_schema(db_path)
    results = await asyncio.gather(
        *(incr_live_order_counter(db_path) for _ in range(20))
    )
    assert sorted(results) == list(range(1, 21))
    assert await get_live_order_count_today(db_path) == 20


# ---------- safety_gate ----------

@pytest.mark.asyncio
async def test_safety_gate_passes_when_clean_and_consumes_one_slot(
    db_path: str, stop_file_path: str
) -> None:
    """Allowed result means the gate ALREADY consumed a slot — there is
    no separate post-success increment step."""
    await init_safety_schema(db_path)
    allowed, reason = await safety_gate(db_path, 10, stop_file_path)
    assert allowed is True
    assert reason is None
    assert await get_live_order_count_today(db_path) == 1


@pytest.mark.asyncio
async def test_safety_gate_blocks_on_stop_file(
    db_path: str, stop_file_path: str
) -> None:
    await init_safety_schema(db_path)
    create_stop_file("manual halt", stop_file_path)
    allowed, reason = await safety_gate(db_path, 10, stop_file_path)
    assert allowed is False
    assert "STOPPED" in (reason or "")
    # STOP-first short-circuit means counter was NOT touched.
    assert await get_live_order_count_today(db_path) == 0


@pytest.mark.asyncio
async def test_safety_gate_stop_takes_precedence_over_cap(
    stop_file_path: str,
) -> None:
    """STOP file is checked first; cap path skipped entirely. Pass an
    unwritten DB path — if STOP-first short-circuit is broken the gate
    would fail trying to talk to it."""
    create_stop_file("first", stop_file_path)
    allowed, reason = await safety_gate(
        "/nonexistent/should-not-be-touched.db", 1, stop_file_path,
    )
    assert allowed is False
    assert "STOPPED" in (reason or "")


@pytest.mark.asyncio
async def test_safety_gate_no_db_skips_cap(stop_file_path: str) -> None:
    allowed, reason = await safety_gate(None, 10, stop_file_path)
    assert allowed is True
    assert reason is None


@pytest.mark.asyncio
async def test_safety_gate_zero_cap_disables_check(
    db_path: str, stop_file_path: str
) -> None:
    await init_safety_schema(db_path)
    allowed, reason = await safety_gate(db_path, 0, stop_file_path)
    assert allowed is True
    assert reason is None


@pytest.mark.asyncio
async def test_safety_gate_blocks_at_cap_and_creates_stop(
    db_path: str, stop_file_path: str
) -> None:
    """First N calls allowed; call N+1 blocks AND creates STOP file."""
    await init_safety_schema(db_path)
    for _ in range(3):
        allowed, _ = await safety_gate(db_path, 3, stop_file_path)
        assert allowed is True
    # 4th call: count goes 3→4, exceeds cap=3 → block + cascade
    allowed, reason = await safety_gate(db_path, 3, stop_file_path)
    assert allowed is False
    assert "cap exceeded" in (reason or "")
    assert os.path.exists(stop_file_path) is True


@pytest.mark.asyncio
async def test_safety_gate_concurrent_callers_cannot_exceed_cap(
    db_path: str, stop_file_path: str
) -> None:
    """Fire 20 gate calls in parallel against cap=5. Exactly 5 should
    be allowed; the rest must be blocked. This is the property the
    increment-first design exists to guarantee."""
    await init_safety_schema(db_path)
    results = await asyncio.gather(
        *(safety_gate(db_path, 5, stop_file_path) for _ in range(20))
    )
    allowed_count = sum(1 for ok, _ in results if ok)
    blocked_count = sum(1 for ok, _ in results if not ok)
    assert allowed_count == 5, (
        f"cap was 5 but {allowed_count} gates allowed — TOCTOU race"
    )
    assert blocked_count == 15
    assert os.path.exists(stop_file_path) is True


@pytest.mark.asyncio
async def test_safety_gate_fails_closed_on_db_error(
    stop_file_path: str, tmp_path: Path
) -> None:
    """Bad DB path → gate must reject AND create STOP file (fail-closed)."""
    # Path inside a non-existent directory that can't be created
    bad_db = "/nonexistent/dir/that/does/not/exist.db"
    allowed, reason = await safety_gate(bad_db, 10, stop_file_path)
    assert allowed is False
    assert "counter error" in (reason or "")
    assert os.path.exists(stop_file_path) is True


# ---------- KalshiExchange integration ----------

@pytest.mark.asyncio
async def test_kalshi_exchange_blocks_when_stopped(
    db_path: str, stop_file_path: str
) -> None:
    """allow_send=True + STOP file → place_order returns rejected, no POST."""
    from src.exec import build_entry_plan
    from src.exec.kalshi_exchange import KalshiExchange

    await init_safety_schema(db_path)
    create_stop_file("test halt", stop_file_path)

    class _FK:
        BASE_URL = "https://example.invalid/trade-api/v2"
        def _auth_headers(self, method, path):
            return {}

    ex = KalshiExchange(
        _FK(),
        allow_send=True,
        db_path=db_path,
        max_orders_per_day=10,
    )
    # Override the default STOP path so this test's tmp file is checked
    import src.exec.safety as safety_mod
    original_default = safety_mod.DEFAULT_STOP_FILE
    safety_mod.DEFAULT_STOP_FILE = stop_file_path
    try:
        opp = {
            "pair_id": "T",
            "profit_pct": 0.05, "implied_sum": 0.95,
            "buy_yes": {"platform": "kalshi", "ticker": "KX-X",
                        "question": "q", "url": "u",
                        "yes_price": 0.5, "no_price": 0.5, "closes_at": None},
            "buy_no":  {"platform": "polymarket", "ticker": "pm",
                        "question": "q", "url": "u",
                        "yes_price": 0.5, "no_price": 0.5,
                        "no_token": "T", "closes_at": None},
        }
        sizing = {
            "leg_yes": {"contracts": 5.0, "usd": 2.5, "platform": "kalshi"},
            "leg_no":  {"contracts": 5.0, "usd": 2.5, "platform": "polymarket"},
            "net_profit": 0.2, "guaranteed_payout": 5.0,
        }
        plan = build_entry_plan(opp, sizing, paper_trade_id=99)
        result = await ex.place_order(plan.leg_yes)
        assert result.accepted is False
        assert "safety_gate" in (result.error or "")
    finally:
        safety_mod.DEFAULT_STOP_FILE = original_default
