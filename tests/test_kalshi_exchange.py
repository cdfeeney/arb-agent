"""Tests for KalshiExchange — request body construction + kill switch.

These tests don't actually hit Kalshi. We exercise:
  * _build_order_body shape (cents conversion, side mapping, IOC, idempotency)
  * allow_send=False short-circuits the POST and returns a fake order id
  * place_order against a monkeypatched httpx flow does the right thing
"""

from __future__ import annotations

import os
import tempfile
import types
from unittest.mock import patch

import httpx
import pytest

from src.exec import build_entry_plan, init_orders_schema
from src.exec.kalshi_exchange import KalshiExchange, _price_in_cents


class _FakeKalshiClient:
    """Minimal stand-in for KalshiClient — exposes BASE_URL + _auth_headers."""
    BASE_URL = "https://example.invalid/trade-api/v2"

    def _auth_headers(self, method: str, path: str) -> dict:
        return {
            "KALSHI-ACCESS-KEY": "fake-key",
            "KALSHI-ACCESS-TIMESTAMP": "0",
            "KALSHI-ACCESS-SIGNATURE": "fake-sig",
        }

    async def fetch_orderbook(self, ticker: str):
        return {
            "yes_bids": [(0.50, 10.0), (0.49, 5.0)],
            "no_bids":  [(0.45, 8.0)],
            "yes_asks": [], "no_asks": [],
        }

    @staticmethod
    def walk_bids(bids, target):
        if not bids or target <= 0:
            return 0.0, 0.0
        rem, spent, filled = target, 0.0, 0.0
        for p, s in bids:
            t = min(rem, s)
            spent += t * p
            filled += t
            rem -= t
            if rem <= 0:
                break
        return (spent / filled if filled else 0.0), filled


def _opp_sizing():
    opp = {
        "pair_id": "TEST",
        "profit_pct": 0.05, "implied_sum": 0.95,
        "buy_yes": {"platform": "kalshi", "ticker": "KX-FOO",
                    "question": "q", "url": "u",
                    "yes_price": 0.54, "no_price": 0.46, "closes_at": None},
        "buy_no":  {"platform": "polymarket", "ticker": "pm-bar",
                    "question": "q", "url": "u",
                    "yes_price": 0.59, "no_price": 0.41,
                    "no_token": "TOKEN", "closes_at": None},
    }
    sizing = {
        "leg_yes": {"contracts": 5.0, "usd": 2.70, "platform": "kalshi"},
        "leg_no":  {"contracts": 5.0, "usd": 2.05, "platform": "polymarket"},
        "net_profit": 0.20, "guaranteed_payout": 5.0,
    }
    return opp, sizing


def test_price_in_cents_clamps():
    assert _price_in_cents(0.54) == 54
    assert _price_in_cents(0.001) == 1   # clamps to min 1
    assert _price_in_cents(0.999) == 99  # clamps to max 99
    assert _price_in_cents(0.5455) == 55


def test_build_order_body_yes_side():
    opp, sizing = _opp_sizing()
    plan = build_entry_plan(opp, sizing, paper_trade_id=1)
    ex = KalshiExchange(_FakeKalshiClient(), allow_send=False)
    body = ex._build_order_body(plan.leg_yes, action="buy")
    assert body["ticker"] == "KX-FOO"
    assert body["side"] == "yes"
    assert body["yes_price"] == 54
    assert "no_price" not in body
    assert body["count"] == 5
    assert body["type"] == "limit"
    assert body["time_in_force"] == "IOC"
    assert body["action"] == "buy"
    assert body["client_order_id"] == plan.leg_yes.idempotency_key


@pytest.mark.asyncio
async def test_place_order_allow_send_false_short_circuits():
    """allow_send=False must NOT make any HTTP call. We assert by patching
    httpx.AsyncClient.post to raise — if it gets called, the test fails."""
    opp, sizing = _opp_sizing()
    plan = build_entry_plan(opp, sizing, paper_trade_id=2)
    ex = KalshiExchange(_FakeKalshiClient(), allow_send=False)

    async def _no(*a, **kw):
        raise AssertionError("HTTP POST attempted in allow_send=False mode")

    with patch.object(httpx.AsyncClient, "post", _no):
        result = await ex.place_order(plan.leg_yes)
    assert result.accepted is True
    assert result.external_order_id.startswith("DRY-KALSHI-")


@pytest.mark.asyncio
async def test_get_order_dry_returns_filled():
    ex = KalshiExchange(_FakeKalshiClient(), allow_send=False)
    state = await ex.get_order("DRY-KALSHI-abc123")
    assert state.status == "filled"


@pytest.mark.asyncio
async def test_market_sell_dry_uses_walked_bid():
    """In dry mode, market_sell estimates realized via walk_bids and
    returns deterministic result without POSTing."""
    opp, sizing = _opp_sizing()
    plan = build_entry_plan(opp, sizing, paper_trade_id=3)
    ex = KalshiExchange(_FakeKalshiClient(), allow_send=False)

    async def _no(*a, **kw):
        raise AssertionError("HTTP POST attempted in allow_send=False mode")

    with patch.object(httpx.AsyncClient, "post", _no):
        result = await ex.market_sell(plan.leg_yes, contracts=5.0)
    # bids: 10@0.50 + (5 contracts wanted, all from top of book)
    # → vwap = 0.50, realized = 5 × 0.50 = 2.50
    assert result.sold_contracts == 5.0
    assert abs(result.avg_price - 0.50) < 1e-9
    assert abs(result.realized_usd - 2.50) < 1e-9
