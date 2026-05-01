"""Log-only executor — writes structured order rows but never hits an exchange.

Replaces the previous `[DRY RUN] Would place orders — skipping execution`
log line with: a real `orders` row per leg, idempotency-keyed, tied to the
paper_trade via paper_trade_id, all states walked (pending → filled).

Fill assumption: deterministic at the planned price (price_limit). This
mirrors paper P&L's "fills at observed_price" assumption today, which the
position monitor's mark-to-market then validates against actual bid books.

What this gives us going forward:
  * The same DB schema + queries that work in live mode work in log_only.
    When we flip the flag, the only thing that changes is which writer is
    plugged in — every dashboard/probe/script keeps working unchanged.
  * Concrete idempotency keys we can grep for in the logs and DB.
  * A correlation_id linking both legs so we can verify atomicity at the
    DB level later (every correlation_id should have exactly two legs in
    matching states).
"""

from __future__ import annotations

import logging

from .base import EntryPlan, EntryResult, LegResult, OrderPlan
from . import order_state

log = logging.getLogger(__name__)


class LogOnlyExecutor:
    mode = "log_only"

    def __init__(self, db_path: str):
        self.db_path = db_path

    async def execute_entry(self, plan: EntryPlan) -> EntryResult:
        leg_results: list[LegResult] = []
        for leg in (plan.leg_yes, plan.leg_no):
            order_id = await order_state.insert_pending(
                self.db_path,
                leg,
                correlation_id=plan.correlation_id,
                paper_trade_id=plan.paper_trade_id,
                pair_id=plan.pair_id,
                execution_mode=self.mode,
            )
            external_id = f"LOGONLY-{order_id}"
            await order_state.update_status(
                self.db_path,
                order_id,
                status="filled",
                filled_contracts=leg.contracts,
                avg_fill_price=leg.price_limit,
                external_order_id=external_id,
            )
            leg_results.append(
                LegResult(
                    plan=leg,
                    status="filled",
                    filled_contracts=leg.contracts,
                    avg_fill_price=leg.price_limit,
                    external_order_id=external_id,
                )
            )
            log.info(
                "[LOG_ONLY] order #%d %s/%s %s n=%g @%.4f cpc=$%.2f "
                "(idemp=%s corr=%s)",
                order_id,
                leg.platform,
                leg.ticker,
                leg.side,
                leg.contracts,
                leg.price_limit,
                leg.contracts * leg.price_limit,
                leg.idempotency_key,
                plan.correlation_id,
            )
        return EntryResult(
            plan=plan,
            leg_yes=leg_results[0],
            leg_no=leg_results[1],
            success=True,
        )
