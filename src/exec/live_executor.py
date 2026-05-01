"""Live executor — wraps the orchestrator with real exchange registry.

Sprint 2a (this file ships): orchestration is fully wired. Two-leg atomic
entry with naked-leg defense runs end-to-end against any Exchange impl.
The simulated exchange (src/exec/simulated_exchange.py) is used in tests
to prove the decision tree.

Sprint 2b (still pending — needs API keys to verify): real Exchange
implementations for Polymarket and Kalshi. Until those land, constructing
LiveExecutor without a non-empty `exchanges` map raises NotImplementedError
at construction time so the bot won't even start in live mode.
"""

from __future__ import annotations

import logging

from .atomic_orchestrator import execute_atomic_entry
from .base import EntryPlan, EntryResult
from .exchange import Exchange

log = logging.getLogger(__name__)


class LiveExecutor:
    mode = "live"

    def __init__(
        self,
        db_path: str,
        kalshi=None,
        poly=None,
        naked_leg_timeout_seconds: float = 2.0,
        per_leg_timeout_seconds: float = 5.0,
        exchanges: dict[str, Exchange] | None = None,
    ):
        self.db_path = db_path
        self.kalshi = kalshi
        self.poly = poly
        self.naked_leg_timeout_seconds = naked_leg_timeout_seconds
        self.per_leg_timeout_seconds = per_leg_timeout_seconds

        if exchanges:
            self.exchanges = exchanges
        else:
            # Sprint 2b will populate this from kalshi/poly clients. Until
            # then, refuse to construct so the bot crashes loudly at startup
            # rather than silently dropping orders later.
            raise NotImplementedError(
                "LiveExecutor: no real Exchange implementations registered. "
                "Sprint 2b will wire PolymarketExchange (Ed25519 signing) and "
                "KalshiExchange (RSA signing). Until then, set "
                "execution.mode: log_only in config.yaml."
            )

    async def execute_entry(self, plan: EntryPlan) -> EntryResult:
        return await execute_atomic_entry(
            plan=plan,
            exchanges=self.exchanges,
            db_path=self.db_path,
            naked_leg_timeout_seconds=self.naked_leg_timeout_seconds,
            per_leg_timeout_seconds=self.per_leg_timeout_seconds,
            execution_mode=self.mode,
        )
