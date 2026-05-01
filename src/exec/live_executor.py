"""Live executor — STUBBED for Sprint 1.

Sprint 2 will wire:
  * src/exec/polymarket_writer.py — Ed25519-signed CLOB POST /order with
    integer-contract sizing and order_type=GTC for makers / FOK for takers.
  * src/exec/kalshi_writer.py     — RSA-signed POST /trade-api/v2/portfolio
    /orders with action=buy, side=yes|no, type=limit, time_in_force=IOC.
  * Atomic two-leg orchestration: place both legs concurrently, watch for
    fills with a budget of `naked_leg_timeout_seconds`. If one leg fills and
    the other doesn't within the budget, market-sell the orphan back into
    its top bid (cap loss at the spread) and mark the entry FAILED.
  * Idempotency: same idempotency_key passed to both APIs as
    client_order_id, so retries don't double-fire.

Until Sprint 2: refuses to place orders. Set execution.mode: log_only in
config.yaml. The bot will continue paper-trading via LogOnlyExecutor.
"""

from __future__ import annotations

import logging

from .base import EntryPlan, EntryResult

log = logging.getLogger(__name__)


class LiveExecutor:
    mode = "live"

    def __init__(
        self,
        db_path: str,
        kalshi,
        poly,
        naked_leg_timeout_seconds: float = 2.0,
    ):
        self.db_path = db_path
        self.kalshi = kalshi
        self.poly = poly
        self.naked_leg_timeout_seconds = naked_leg_timeout_seconds

    async def execute_entry(self, plan: EntryPlan) -> EntryResult:
        log.error(
            "LiveExecutor.execute_entry not yet implemented — refusing to place "
            "real orders. Use execution.mode: log_only in config.yaml until "
            "Sprint 2 lands. Dropped plan: corr=%s pair=%s",
            plan.correlation_id,
            plan.pair_id,
        )
        raise NotImplementedError(
            "LiveExecutor: real order placement not implemented in Sprint 1. "
            "Set execution.mode: log_only in config.yaml."
        )
