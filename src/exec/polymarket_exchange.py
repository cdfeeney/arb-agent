"""Polymarket exchange writer — conforms to the Exchange protocol.

Polymarket's CLOB requires EIP-712 signed orders (the order struct is
signed with the user's wallet/proxy private key on Polygon). The
official `py-clob-client` library handles signing, nonce management,
and L2 API key auth — we lazy-import it so the bot still loads without
the dep, and the missing-dep case surfaces only when an actual order is
attempted.

To use this exchange you need:
    POLYMARKET_PRIVATE_KEY   — EOA / proxy-wallet private key (0x...)
    POLYMARKET_FUNDER        — funding address (USDC holder); often same as EOA
    pip install py-clob-client

Kill switch (same as Kalshi):
    allow_send=False  → constructs the full OrderArgs, logs it, but does
                        NOT call create_and_post_order. Returns a fake
                        PlaceResult so the orchestrator runs end-to-end.

Sprint 2b NOTE: py-clob-client is sync — we wrap calls in asyncio.to_thread
to keep the orchestrator non-blocking.
"""

from __future__ import annotations

import asyncio
import logging
import os

from .base import OrderPlan
from .exchange import FillState, MarketSellResult, PlaceResult

log = logging.getLogger(__name__)


def _import_clob_client():
    """Lazy import — module loads even when py-clob-client isn't installed."""
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.constants import POLYGON
        from py_clob_client.order_builder.constants import BUY, SELL
        return {
            "ClobClient": ClobClient,
            "OrderArgs": OrderArgs,
            "OrderType": OrderType,
            "POLYGON": POLYGON,
            "BUY": BUY,
            "SELL": SELL,
        }
    except ImportError as e:
        return {"_import_error": str(e)}


class PolymarketExchange:
    name = "polymarket"

    def __init__(
        self,
        poly_client,
        private_key: str,
        funder: str | None = None,
        allow_send: bool = False,
        host: str = "https://clob.polymarket.com",
    ):
        self._pc = poly_client
        self._private_key = private_key
        self._funder = funder or _derive_funder_from_key(private_key)
        self.allow_send = bool(allow_send)
        self._host = host
        self._clob = None  # lazy — only constructed if/when we actually trade
        self._import = _import_clob_client()

    def _ensure_client(self):
        if self._clob is not None:
            return self._clob
        if "_import_error" in self._import:
            raise RuntimeError(
                "py-clob-client not installed: "
                f"{self._import['_import_error']}. "
                "Run: pip install py-clob-client"
            )
        ClobClient = self._import["ClobClient"]
        POLYGON = self._import["POLYGON"]
        # signature_type=2 = email-login proxy wallet (most common for retail).
        # If the user's wallet is a regular EOA, this should be 0. Keep
        # configurable later if needed.
        self._clob = ClobClient(
            host=self._host,
            key=self._private_key,
            chain_id=POLYGON,
            signature_type=2,
            funder=self._funder,
        )
        # Generate L2 API credentials lazily — needed for /order POSTs.
        try:
            creds = self._clob.create_or_derive_api_creds()
            self._clob.set_api_creds(creds)
        except Exception as e:
            log.error("Polymarket create_or_derive_api_creds failed: %s", e)
            raise
        return self._clob

    # ---- Exchange protocol ----

    async def place_order(self, plan: OrderPlan) -> PlaceResult:
        if not plan.token:
            return PlaceResult("", False, error="missing CLOB token id on plan")
        if not self.allow_send:
            fake = f"DRY-POLY-{plan.idempotency_key[:10]}"
            log.warning(
                "[POLY allow_send=False] would create_and_post_order "
                "token=%s side=%s price=%.4f size=%g — returning fake id %s",
                plan.token[:16], plan.side, plan.price_limit, plan.contracts, fake,
            )
            return PlaceResult(external_order_id=fake, accepted=True)
        try:
            return await asyncio.to_thread(self._place_sync, plan)
        except Exception as e:
            log.error("Polymarket place_order error: %s", e, exc_info=True)
            return PlaceResult("", False, error=str(e))

    def _place_sync(self, plan: OrderPlan) -> PlaceResult:
        clob = self._ensure_client()
        OrderArgs = self._import["OrderArgs"]
        OrderType = self._import["OrderType"]
        BUY = self._import["BUY"]
        SELL = self._import["SELL"]
        side = BUY if plan.side in ("buy_yes", "buy_no") else SELL
        args = OrderArgs(
            price=float(plan.price_limit),
            size=float(plan.contracts),
            side=side,
            token_id=plan.token,
        )
        # FOK: fill-or-kill (taker). FAK: fill-and-kill (partial-OK taker).
        # Use FAK so we accept partial fills and let orchestrator re-decide.
        resp = clob.create_and_post_order(args, order_type=OrderType.FAK)
        if not (resp or {}).get("success"):
            err = (resp or {}).get("errorMsg") or "create_and_post_order returned no success"
            return PlaceResult("", False, error=err)
        oid = resp.get("orderID") or resp.get("orderId") or ""
        return PlaceResult(external_order_id=oid, accepted=True)

    async def place_maker_sell(
        self,
        *,
        token: str,
        target_price: float,
        contracts: float,
        idempotency_key: str,
    ) -> PlaceResult:
        """Rest a SELL limit order on Polymarket at target_price (GTC).

        Maker order — captures the spread and pays 0% fee on Polymarket
        when filled. Used by position_monitor's maker-exit path. Returns
        a fake DRY id when allow_send=False so callers can record state
        without spending money.
        """
        if not token:
            return PlaceResult("", False, error="missing CLOB token")
        if not self.allow_send:
            fake = f"DRY-POLY-MAKER-{idempotency_key[:10]}"
            log.warning(
                "[POLY allow_send=False] would place GTC maker SELL "
                "token=%s @$%.4f size=%g — returning fake id %s",
                token[:16], target_price, contracts, fake,
            )
            return PlaceResult(external_order_id=fake, accepted=True)
        try:
            return await asyncio.to_thread(
                self._place_maker_sync, token, target_price, contracts,
            )
        except Exception as e:
            log.error(
                "Polymarket place_maker_sell error: %s", e, exc_info=True,
            )
            return PlaceResult("", False, error=str(e))

    def _place_maker_sync(
        self, token: str, target_price: float, contracts: float,
    ) -> PlaceResult:
        clob = self._ensure_client()
        OrderArgs = self._import["OrderArgs"]
        OrderType = self._import["OrderType"]
        SELL = self._import["SELL"]
        args = OrderArgs(
            price=float(target_price),
            size=float(contracts),
            side=SELL,
            token_id=token,
        )
        # GTC = Good-Till-Cancelled. The order rests on the book until
        # filled or cancelled. This is the maker workflow.
        resp = clob.create_and_post_order(args, order_type=OrderType.GTC)
        if not (resp or {}).get("success"):
            err = (resp or {}).get("errorMsg") or "create_and_post_order failed"
            return PlaceResult("", False, error=err)
        oid = resp.get("orderID") or resp.get("orderId") or ""
        return PlaceResult(external_order_id=oid, accepted=True)

    async def get_order(self, external_order_id: str) -> FillState:
        if external_order_id.startswith("DRY-POLY-"):
            return FillState("filled", 0.0, 0.0)
        try:
            return await asyncio.to_thread(self._get_sync, external_order_id)
        except Exception as e:
            return FillState("failed", 0.0, 0.0, error=str(e))

    def _get_sync(self, external_order_id: str) -> FillState:
        clob = self._ensure_client()
        order = clob.get_order(external_order_id)
        if not order:
            return FillState("failed", 0.0, 0.0, error="order not found")
        status = (order.get("status") or "").lower()
        size = float(order.get("size_matched") or 0)
        avg = float(order.get("price") or 0)
        if status in ("matched", "complete", "filled"):
            return FillState("filled", size, avg)
        if status in ("partial", "partially_matched"):
            return FillState("partial", size, avg)
        if status in ("canceled", "cancelled"):
            return FillState("cancelled", size, avg)
        return FillState("submitted", size, avg)

    async def cancel_order(self, external_order_id: str) -> bool:
        if external_order_id.startswith("DRY-POLY-"):
            log.info("[POLY allow_send=False] would cancel order %s", external_order_id)
            return True
        try:
            await asyncio.to_thread(self._cancel_sync, external_order_id)
            return True
        except Exception as e:
            log.error("Polymarket cancel_order error: %s", e)
            return False

    def _cancel_sync(self, external_order_id: str) -> None:
        clob = self._ensure_client()
        clob.cancel(external_order_id)

    async def market_sell(self, plan: OrderPlan, contracts: float) -> MarketSellResult:
        if not plan.token:
            return MarketSellResult(0.0, 0.0, 0.0, error="no CLOB token on plan")
        # Estimate realized via top-of-book bid walk
        book = await self._pc.fetch_clob_book(plan.token)
        avg, filled = self._pc.walk_bids(book, contracts)
        sell_plan = OrderPlan(
            leg=plan.leg, platform=plan.platform, ticker=plan.ticker,
            side="sell_yes" if plan.side in ("buy_yes", "sell_yes") else "sell_no",
            price_limit=max(0.001, avg * 0.5),  # accept any decent bid
            contracts=contracts,
            order_type="taker",
            idempotency_key=plan.idempotency_key + "-unwind",
            token=plan.token,
        )
        if not self.allow_send:
            log.warning(
                "[POLY allow_send=False] would market-sell %g contracts "
                "token=%s estimated avg=%.4f",
                contracts, plan.token[:16], avg,
            )
            return MarketSellResult(
                sold_contracts=contracts,
                realized_usd=round(contracts * avg, 4),
                avg_price=round(avg, 4),
            )
        place = await self.place_order(sell_plan)
        if not place.accepted:
            return MarketSellResult(0.0, 0.0, 0.0, error=place.error)
        return MarketSellResult(
            sold_contracts=contracts,
            realized_usd=round(contracts * avg, 4),
            avg_price=round(avg, 4),
        )


def _derive_funder_from_key(_private_key: str) -> str:
    """Best effort: most retail Polymarket users have funder == proxy address.
    If the user has a separate funder, they should set POLYMARKET_FUNDER.
    Returning empty string forces ClobClient to use its default behavior."""
    return ""
