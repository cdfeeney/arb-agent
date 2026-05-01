"""Real Kalshi exchange writer — conforms to the Exchange protocol.

Endpoints used:
    POST /trade-api/v2/portfolio/orders                  — submit order
    GET  /trade-api/v2/portfolio/orders/{order_id}       — fill status
    DELETE /trade-api/v2/portfolio/orders/{order_id}     — cancel
    POST /trade-api/v2/portfolio/orders                  — naked-leg market sell
                                                            (action=sell at best bid)

Signing reuses src/clients/kalshi.py (RSA-PSS + SHA256). Idempotency key
goes in as `client_order_id` so retries don't double-fire on Kalshi's side.

Kill switch:
    allow_send=False  → builds the FULL signed request, logs it, but never
                        actually .post()s. Returns a deterministic fake
                        PlaceResult so the orchestrator exercises end-to-end.
                        Use this for dress-rehearsal in live mode without
                        spending money.
    allow_send=True   → real submission.
"""

from __future__ import annotations

import json
import logging
import time

import httpx

from .base import OrderPlan
from .exchange import FillState, MarketSellResult, PlaceResult

log = logging.getLogger(__name__)


def _price_in_cents(price_dollars: float) -> int:
    """Kalshi /portfolio/orders takes prices as integer cents (1..99)."""
    return max(1, min(99, int(round(price_dollars * 100))))


class KalshiExchange:
    name = "kalshi"

    def __init__(self, kalshi_client, allow_send: bool = False, timeout: float = 15.0):
        self._kc = kalshi_client
        self.allow_send = bool(allow_send)
        self.timeout = timeout

    # ---- helpers ----

    def _build_order_body(self, plan: OrderPlan, action: str = "buy") -> dict:
        side = "yes" if plan.side in ("buy_yes", "sell_yes") else "no"
        body = {
            "ticker": plan.ticker,
            "client_order_id": plan.idempotency_key,
            "type": "limit",
            "action": action,
            "side": side,
            "count": int(plan.contracts),
            "time_in_force": "IOC",  # taker — fills now or cancels
        }
        if side == "yes":
            body["yes_price"] = _price_in_cents(plan.price_limit)
        else:
            body["no_price"] = _price_in_cents(plan.price_limit)
        return body

    async def _send(self, method: str, path_no_prefix: str, body: dict | None = None) -> tuple[int, dict]:
        path = f"/trade-api/v2{path_no_prefix}"
        url = f"{self._kc.BASE_URL}{path_no_prefix}"
        headers = self._kc._auth_headers(method, path)
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            if method == "POST":
                resp = await client.post(url, headers=headers, json=body or {})
            elif method == "GET":
                resp = await client.get(url, headers=headers)
            elif method == "DELETE":
                resp = await client.delete(url, headers=headers)
            else:
                raise ValueError(f"unsupported method: {method}")
        try:
            data = resp.json()
        except Exception:
            data = {"raw": resp.text}
        return resp.status_code, data

    # ---- Exchange protocol ----

    async def place_order(self, plan: OrderPlan) -> PlaceResult:
        body = self._build_order_body(plan, action="buy")
        if not self.allow_send:
            fake_id = f"DRY-KALSHI-{plan.idempotency_key[:10]}"
            log.warning(
                "[KALSHI allow_send=False] would POST /portfolio/orders "
                "body=%s — returning fake order id %s",
                json.dumps(body), fake_id,
            )
            return PlaceResult(external_order_id=fake_id, accepted=True)
        try:
            code, data = await self._send("POST", "/portfolio/orders", body)
        except Exception as e:
            log.error("Kalshi place_order error: %s body=%s", e, body)
            return PlaceResult("", False, error=str(e))
        if code >= 400:
            err = data.get("error", {}).get("message") if isinstance(data, dict) else str(data)
            log.error("Kalshi place_order rejected (HTTP %d): %s", code, err)
            return PlaceResult("", False, error=err or f"HTTP {code}")
        order = (data or {}).get("order", {}) or {}
        oid = order.get("order_id") or order.get("id") or ""
        if not oid:
            return PlaceResult("", False, error=f"no order_id in response: {data}")
        log.info("Kalshi order placed id=%s ticker=%s side=%s count=%d",
                 oid, plan.ticker, body["side"], body["count"])
        return PlaceResult(external_order_id=oid, accepted=True)

    async def get_order(self, external_order_id: str) -> FillState:
        if external_order_id.startswith("DRY-KALSHI-"):
            # IOC + dry-rehearsal assumes immediate full fill at limit
            return FillState("filled", 0.0, 0.0)  # contracts/price filled in by orchestrator from plan
        try:
            code, data = await self._send("GET", f"/portfolio/orders/{external_order_id}")
        except Exception as e:
            return FillState("failed", 0.0, 0.0, error=str(e))
        if code >= 400:
            return FillState("failed", 0.0, 0.0, error=f"HTTP {code}: {data}")
        order = (data or {}).get("order", {}) or {}
        status = (order.get("status") or "").lower()
        # Kalshi statuses: resting | executed | canceled. IOC orders never rest;
        # they fill or cancel.
        if status == "executed":
            filled = float(order.get("count", 0) or 0) - float(order.get("remaining_count", 0) or 0)
            yes_p = order.get("yes_price")
            no_p = order.get("no_price")
            avg = (yes_p if yes_p is not None else no_p)
            avg_price = float(avg) / 100.0 if avg is not None else 0.0
            return FillState("filled", filled, avg_price)
        if status == "canceled":
            return FillState("cancelled", 0.0, 0.0)
        if status == "resting":
            return FillState("submitted", 0.0, 0.0)
        return FillState("submitted", 0.0, 0.0)

    async def cancel_order(self, external_order_id: str) -> bool:
        if external_order_id.startswith("DRY-KALSHI-"):
            log.info("[KALSHI allow_send=False] would DELETE /orders/%s", external_order_id)
            return True
        try:
            code, _ = await self._send("DELETE", f"/portfolio/orders/{external_order_id}")
        except Exception as e:
            log.error("Kalshi cancel_order error: %s", e)
            return False
        return code < 400 or code == 404  # 404 = already terminal, treat as success

    async def market_sell(self, plan: OrderPlan, contracts: float) -> MarketSellResult:
        """Naked-leg unwinder: walk the bid book to estimate price, then
        submit a SELL IOC at price=1¢ (worst case, fills against any bid)."""
        # Fetch orderbook to estimate realized price (post-trade reporting)
        book = await self._kc.fetch_orderbook(plan.ticker)
        side_book = (book or {}).get(
            "yes_bids" if plan.side in ("buy_yes", "sell_yes") else "no_bids", []
        )
        avg_price, filled = self._kc.walk_bids(side_book, contracts)
        # Submit a sell limit at 1¢ (IOC) — Kalshi will fill against any bid
        # at or above this. Effectively a market order in a venue that
        # doesn't support market orders explicitly.
        sell_plan = OrderPlan(
            leg=plan.leg, platform=plan.platform, ticker=plan.ticker,
            side="sell_yes" if plan.side in ("buy_yes", "sell_yes") else "sell_no",
            price_limit=0.01,
            contracts=contracts,
            order_type="taker",
            idempotency_key=plan.idempotency_key + "-unwind",
            token=None,
        )
        body = self._build_order_body(sell_plan, action="sell")
        if not self.allow_send:
            log.warning(
                "[KALSHI allow_send=False] would market-sell %s %g contracts "
                "estimated avg=%.4f body=%s",
                plan.ticker, contracts, avg_price, json.dumps(body),
            )
            return MarketSellResult(
                sold_contracts=contracts,
                realized_usd=round(contracts * avg_price, 4),
                avg_price=round(avg_price, 4),
            )
        try:
            code, data = await self._send("POST", "/portfolio/orders", body)
        except Exception as e:
            return MarketSellResult(0.0, 0.0, 0.0, error=str(e))
        if code >= 400:
            return MarketSellResult(0.0, 0.0, 0.0, error=f"HTTP {code}: {data}")
        # Realized = contracts × avg_fill_price (Kalshi response will give this
        # in `order.executed_price` or similar; for now use estimated avg)
        return MarketSellResult(
            sold_contracts=contracts,
            realized_usd=round(contracts * avg_price, 4),
            avg_price=round(avg_price, 4),
        )
