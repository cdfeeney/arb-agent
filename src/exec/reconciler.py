"""Startup reconciliation across exchanges.

Why this exists: the atomic_orchestrator can defend a naked leg only while
its asyncio task is alive. If the bot crashes mid-fill (yes filled, no
still resting; or a place_order returned but the DB write didn't), the
position can desync silently between the orders table and the exchange
state. On the next start, this module reconciles.

Reconciliation rules per non-terminal order in the DB:

  * `pending` with no external_order_id     → unknown state. Emergency-
    halt. We cannot query the exchange (no id), and proceeding might
    place a duplicate.

  * `submitted`/`accepted` with external_id → query exchange:
      - exchange says filled/partial → update DB, defend if the OTHER
        leg of the same correlation_id is naked.
      - exchange says cancelled/failed → update DB, no further action.
      - exchange says still resting → unusual after a restart. Cancel
        defensively (we don't trust unattended limit orders) and update
        DB.

After reconciliation, the bot is safe to resume normal operation. Any
emergency-halt path writes data/STOP via _emergency_halt (defined in
atomic_orchestrator).
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field

import aiosqlite

from .exchange import Exchange, FillState

log = logging.getLogger(__name__)


@dataclass
class ReconcileReport:
    checked: int = 0
    already_terminal: int = 0
    updated_to_filled: int = 0
    updated_to_cancelled: int = 0
    cancelled_resting: int = 0
    naked_legs_defended: int = 0
    halts_triggered: int = 0
    errors: list[str] = field(default_factory=list)

    def summary(self) -> str:
        return (
            f"reconcile: checked={self.checked} terminal={self.already_terminal} "
            f"→filled={self.updated_to_filled} →cancelled={self.updated_to_cancelled} "
            f"resting_cancelled={self.cancelled_resting} "
            f"naked_defended={self.naked_legs_defended} "
            f"halts={self.halts_triggered} errors={len(self.errors)}"
        )


_NON_TERMINAL = ("pending", "submitted", "accepted", "partial")


async def _fetch_open_orders(db_path: str) -> list[dict]:
    placeholders = ",".join("?" for _ in _NON_TERMINAL)
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            f"""SELECT id, correlation_id, paper_trade_id, pair_id, leg,
                       platform, ticker, status, external_order_id,
                       contracts_intended, contracts_filled, avg_fill_price
                FROM orders
                WHERE status IN ({placeholders})
                ORDER BY correlation_id, leg""",
            _NON_TERMINAL,
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


async def reconcile_open_orders(
    db_path: str,
    exchanges: dict[str, Exchange],
    *,
    emergency_halt_fn=None,
) -> ReconcileReport:
    """Walk the orders table for non-terminal rows and bring DB in sync
    with the exchanges. Halts the bot if a row is in an unrecoverable
    state (pending with no external_order_id, exchange unreachable, etc.).
    """
    # Avoid circular import: atomic_orchestrator imports order_state which
    # is fine, but reconciler needs _emergency_halt — pass it in or import
    # lazily.
    if emergency_halt_fn is None:
        from .atomic_orchestrator import _emergency_halt
        emergency_halt_fn = _emergency_halt

    from . import order_state  # local — avoid module-load cycles

    report = ReconcileReport()
    rows = await _fetch_open_orders(db_path)
    if not rows:
        log.info("reconcile: no open orders in DB — clean state")
        return report

    log.warning(
        "reconcile: %d non-terminal orders found at startup — auditing",
        len(rows),
    )

    by_corr: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_corr[r["correlation_id"]].append(r)

    # Two-pass: update DB to match exchanges, then handle naked legs once
    # we know which correlation_ids ended up with one filled / one not.
    for corr_id, legs in by_corr.items():
        for row in legs:
            report.checked += 1
            ex = exchanges.get(row["platform"])
            if ex is None:
                msg = (
                    f"reconcile: order {row['id']} corr={corr_id} "
                    f"on platform={row['platform']} but no exchange registered"
                )
                report.errors.append(msg)
                log.error(msg)
                continue

            if not row["external_order_id"]:
                # We placed (or tried to) but never recorded the exchange's
                # ack. Cannot recover automatically — could be live on the
                # exchange or never made it. Halt.
                msg = (
                    f"reconcile: order {row['id']} corr={corr_id} "
                    f"status={row['status']} has NO external_order_id "
                    f"— cannot verify exchange state"
                )
                report.errors.append(msg)
                report.halts_triggered += 1
                await emergency_halt_fn(db_path, reason=msg)
                continue

            try:
                state: FillState = await ex.get_order(row["external_order_id"])
            except Exception as e:
                msg = (
                    f"reconcile: get_order failed for order {row['id']} "
                    f"corr={corr_id} ext={row['external_order_id']}: {e}"
                )
                report.errors.append(msg)
                report.halts_triggered += 1
                await emergency_halt_fn(db_path, reason=msg)
                continue

            # Reconcile DB to exchange state
            if state.status in ("filled", "partial"):
                await order_state.update_status(
                    db_path, row["id"], status=state.status,
                    filled_contracts=state.filled_contracts,
                    avg_fill_price=state.avg_fill_price,
                    external_order_id=row["external_order_id"],
                )
                report.updated_to_filled += 1
                # Annotate the row so the second pass can spot naked legs
                row["_reconciled_status"] = state.status
                row["_reconciled_filled"] = state.filled_contracts
            elif state.status in ("cancelled", "failed"):
                await order_state.update_status(
                    db_path, row["id"], status=state.status,
                    error=f"reconcile: exchange reported {state.status}",
                )
                report.updated_to_cancelled += 1
                row["_reconciled_status"] = state.status
                row["_reconciled_filled"] = 0.0
            elif state.status == "submitted":
                # Exchange says still resting — unusual after a restart.
                # Cancel defensively; we don't want unattended live orders.
                try:
                    await ex.cancel_order(row["external_order_id"])
                except Exception as e:
                    log.warning(
                        "reconcile: defensive cancel failed for order %d: %s",
                        row["id"], e,
                    )
                await order_state.update_status(
                    db_path, row["id"], status="cancelled",
                    error="reconcile: defensive cancel after restart",
                )
                report.cancelled_resting += 1
                row["_reconciled_status"] = "cancelled"
                row["_reconciled_filled"] = 0.0
            else:
                msg = (
                    f"reconcile: order {row['id']} unknown exchange status "
                    f"{state.status!r}"
                )
                report.errors.append(msg)
                report.halts_triggered += 1
                await emergency_halt_fn(db_path, reason=msg)

    # Second pass: detect naked legs (one side filled/partial, other not).
    # Note: we DO NOT auto-defend here — by the time the bot restarts the
    # naked window has long passed, prices have moved, and an unattended
    # market_sell could lock in a bigger loss. Halt instead so a human
    # decides.
    for corr_id, legs in by_corr.items():
        if len(legs) != 2:
            continue
        a, b = legs
        a_filled = a.get("_reconciled_filled", 0.0) > 0
        b_filled = b.get("_reconciled_filled", 0.0) > 0
        if a_filled != b_filled:
            naked_leg = a if a_filled else b
            msg = (
                f"reconcile: NAKED LEG detected at restart — corr={corr_id} "
                f"{naked_leg['leg']} leg on {naked_leg['platform']} "
                f"({naked_leg['ticker']}) has {naked_leg['_reconciled_filled']:.2f} "
                f"contracts filled but other leg did not. "
                f"Manual intervention required."
            )
            report.naked_legs_defended += 1
            report.halts_triggered += 1
            await emergency_halt_fn(db_path, reason=msg)

    log.warning("%s", report.summary())
    return report
