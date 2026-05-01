"""Order execution layer.

Sprint 1 (this file ships): all infrastructure, NO live order placement.

Two executors live here:
  * LogOnlyExecutor — current dry-run behavior, but writes structured
    order rows to the `orders` DB table with idempotency keys, correlation
    ids, and the full price/size/leg breakdown. Same SQL queries that will
    work in live mode.
  * LiveExecutor    — STUBBED. Sprint 2 implements Polymarket Ed25519 +
    Kalshi RSA signing, two-leg orchestration, naked-leg defense.

The active executor is chosen by config:
    execution:
      mode: log_only        # log_only | live
      naked_leg_timeout_seconds: 2

Default mode is log_only so a config without the section runs unchanged.
"""

from .base import (
    EntryPlan,
    EntryResult,
    LegResult,
    OrderPlan,
    make_correlation_id,
    make_idempotency_key,
)
from .atomic_entry import build_entry_plan
from .log_only_executor import LogOnlyExecutor
from .live_executor import LiveExecutor
from .order_state import init_orders_schema

__all__ = [
    "EntryPlan",
    "EntryResult",
    "LegResult",
    "OrderPlan",
    "build_entry_plan",
    "init_orders_schema",
    "LogOnlyExecutor",
    "LiveExecutor",
    "make_correlation_id",
    "make_idempotency_key",
]
