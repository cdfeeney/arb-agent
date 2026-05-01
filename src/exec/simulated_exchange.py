"""Simulated exchange for testing the orchestrator without API keys.

Configurable scenarios per leg:
  * happy path:           accept=True, fill_status="filled" (default)
  * stuck order:          fill_status="submitted" (never fills) — drives naked leg
  * partial fill:         fill_status="partial", fill_fraction=0.5
  * placed but cancelled: fill_status="cancelled"
  * place reject:         accept=False
  * slippage:             fill_price_offset=0.02 (2¢ worse than limit)
  * delayed fill:         fill_delay_seconds=1.5 (sleeps before transitioning to filled)

The simulated clock ticks through asyncio.get_event_loop().time(). Tests
that want to advance time use asyncio.sleep — there's no fake clock here,
just real time with very short delays.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from .exchange import FillState, MarketSellResult, PlaceResult

log = logging.getLogger(__name__)


@dataclass
class SimSpec:
    accept: bool = True
    place_error: str | None = None
    fill_status: str = "filled"               # filled|partial|submitted|cancelled|failed
    fill_fraction: float = 1.0
    fill_delay_seconds: float = 0.0           # appears as 'submitted' until elapsed
    fill_price_offset: float = 0.0            # added to limit (positive = worse for taker)
    market_sell_price_per_contract: float = 0.40


class SimulatedExchange:
    """Deterministic Exchange for tests."""

    def __init__(self, name: str, spec: SimSpec | None = None):
        self.name = name
        self.spec = spec or SimSpec()
        self._orders: dict[str, dict] = {}
        self._counter = 0
        self._cancelled: set[str] = set()

    async def place_order(self, plan) -> PlaceResult:
        if not self.spec.accept:
            return PlaceResult(
                external_order_id="",
                accepted=False,
                error=self.spec.place_error or "rejected",
            )
        self._counter += 1
        ext = f"{self.name}-SIM-{self._counter}"
        self._orders[ext] = {
            "plan": plan,
            "submit_time": asyncio.get_event_loop().time(),
        }
        return PlaceResult(external_order_id=ext, accepted=True)

    async def get_order(self, external_order_id: str) -> FillState:
        if external_order_id in self._cancelled:
            return FillState("cancelled", 0.0, 0.0)
        rec = self._orders.get(external_order_id)
        if rec is None:
            return FillState("failed", 0.0, 0.0, error="unknown order id")
        plan = rec["plan"]
        elapsed = asyncio.get_event_loop().time() - rec["submit_time"]
        # Pre-delay: still sitting on the book
        if elapsed < self.spec.fill_delay_seconds:
            return FillState("submitted", 0.0, 0.0)
        price = plan.price_limit + self.spec.fill_price_offset
        if self.spec.fill_status == "filled":
            return FillState("filled", plan.contracts, price)
        if self.spec.fill_status == "partial":
            filled = plan.contracts * self.spec.fill_fraction
            return FillState("partial", filled, price)
        if self.spec.fill_status == "submitted":
            return FillState("submitted", 0.0, 0.0)
        if self.spec.fill_status == "cancelled":
            return FillState("cancelled", 0.0, 0.0)
        return FillState("failed", 0.0, 0.0, error="sim configured fail")

    async def cancel_order(self, external_order_id: str) -> bool:
        self._cancelled.add(external_order_id)
        return True

    async def market_sell(self, plan, contracts: float) -> MarketSellResult:
        avg = self.spec.market_sell_price_per_contract
        realized = contracts * avg
        log.info(
            "[SIM-%s] market_sell %g %s contracts @ ~%.4f = $%.2f",
            self.name, contracts, plan.platform, avg, realized,
        )
        return MarketSellResult(
            sold_contracts=contracts, realized_usd=realized, avg_price=avg,
        )
