"""Exchange Protocol — what the orchestrator needs from any venue.

Both the real Polymarket/Kalshi writers (Sprint 2b) and the simulated
exchange used in tests conform to this. Orchestrator code never touches
HTTP/signing/retry details — it only sees place / poll / cancel / market_sell.

Why these four methods:
  * place_order   — submit, return external id IMMEDIATELY (no fill wait).
                    Order placement and order watching are decoupled so we
                    can place both legs in parallel and watch them in parallel.
  * get_order     — poll fill state. Returns FillState that distinguishes
                    submitted (resting), partial (some fills), filled, cancelled.
                    No "open vs closed" boolean — naked-leg defense needs the
                    granularity to decide whether to wait or unwind.
  * cancel_order  — best-effort. Returns True if accepted by venue (cancel
                    requested or already terminal). Idempotent — calling on
                    an already-filled order is fine, just returns True.
  * market_sell   — naked-leg unwinder. Takes a filled leg and dumps the
                    contracts back into the bid book at any price. Returns
                    realized USD so the orchestrator can compute the loss
                    and persist it on the entry result.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol


@dataclass(frozen=True)
class PlaceResult:
    """Returned by place_order. external_order_id is set on accept only."""
    external_order_id: str
    accepted: bool
    error: Optional[str] = None


@dataclass(frozen=True)
class FillState:
    """Snapshot of an order's fill state from get_order."""
    status: str                       # submitted | partial | filled | cancelled | failed
    filled_contracts: float
    avg_fill_price: float
    error: Optional[str] = None


@dataclass(frozen=True)
class MarketSellResult:
    """Outcome of a naked-leg unwind. realized_usd is what we got back."""
    sold_contracts: float
    realized_usd: float
    avg_price: float
    error: Optional[str] = None


class Exchange(Protocol):
    """Minimum surface the orchestrator needs from any venue."""

    name: str

    async def place_order(self, plan) -> PlaceResult: ...

    async def get_order(self, external_order_id: str) -> FillState: ...

    async def cancel_order(self, external_order_id: str) -> bool: ...

    async def market_sell(self, plan, contracts: float) -> MarketSellResult: ...
