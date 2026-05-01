"""Dataclasses + protocol for the execution layer.

OrderPlan = single-leg intent. EntryPlan = two-leg arb entry intent.
LegResult / EntryResult capture what actually happened.

Idempotency keys are deterministic per (pair_id, leg, ms-bucket) so retries
within the same millisecond return the same key — the orders table has a
UNIQUE constraint on it, so the second insert is a no-op rather than
double-submitting an order. Correlation ids tie both legs of one entry
together for log + DB tracing.
"""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from typing import Literal, Optional, Protocol

OrderSide = Literal["buy_yes", "buy_no", "sell_yes", "sell_no"]
OrderType = Literal["taker", "maker"]
OrderStatus = Literal[
    "pending", "submitted", "partial", "filled", "cancelled", "failed"
]
ExecutionMode = Literal["log_only", "live"]


@dataclass(frozen=True)
class OrderPlan:
    """Single-leg order intent."""
    leg: str                          # 'yes' or 'no'
    platform: str                     # 'kalshi' or 'polymarket'
    ticker: str
    side: OrderSide
    price_limit: float                # max price we'll pay (taker) or rest at (maker)
    contracts: float                  # integer contracts (sizing has already floored)
    order_type: OrderType
    idempotency_key: str
    token: Optional[str] = None       # Polymarket CLOB token id; None for Kalshi


@dataclass(frozen=True)
class EntryPlan:
    """Atomic two-leg entry intent — both legs must fill or we unwind."""
    pair_id: str
    paper_trade_id: int
    leg_yes: OrderPlan
    leg_no: OrderPlan
    expected_cost_usd: float
    expected_net_profit_usd: float
    expected_payout_usd: float
    correlation_id: str


@dataclass(frozen=True)
class LegResult:
    plan: OrderPlan
    status: OrderStatus
    filled_contracts: float
    avg_fill_price: float
    external_order_id: Optional[str] = None
    error: Optional[str] = None


@dataclass(frozen=True)
class EntryResult:
    plan: EntryPlan
    leg_yes: LegResult
    leg_no: LegResult
    success: bool
    naked_leg_unwound: bool = False
    naked_leg_realized_usd: float = 0.0
    error: Optional[str] = None


class Executor(Protocol):
    mode: ExecutionMode

    async def execute_entry(self, plan: EntryPlan) -> EntryResult: ...


def make_idempotency_key(pair_id: str, leg: str, t: float | None = None) -> str:
    """Stable id for a leg. Same (pair, leg, ms) → same key.

    Bucketed at millisecond granularity so a same-cycle retry collapses
    onto the original key (orders table has UNIQUE on idempotency_key —
    second insert no-ops). Cycles further apart get distinct keys.
    """
    t = t if t is not None else time.time()
    seed = f"{pair_id}|{leg}|{int(t * 1000)}"
    return hashlib.sha1(seed.encode()).hexdigest()[:16]


def make_correlation_id(pair_id: str, t: float | None = None) -> str:
    t = t if t is not None else time.time()
    seed = f"{pair_id}|entry|{int(t * 1000)}"
    return hashlib.sha1(seed.encode()).hexdigest()[:12]
