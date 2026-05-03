"""Two-leg atomic-entry orchestration with naked-leg defense.

This is the core risk-management primitive for live mode. The promise:
  * If both legs fill, we have an arbitrage hedge.
  * If only one leg fills, we are exposed to the underlying — we MUST
    market-sell the orphaned leg back into its bid book before the
    market moves and locks in a real loss.
  * If neither fills, no harm done — cancel both and walk away.

Decision tree:

    place(yes), place(no)  in parallel
        │
        ├─ both rejected at place → fail, no orders alive
        ├─ one rejected, other accepted → cancel accepted leg, fail
        └─ both accepted
              │
              poll fills with budget = per_leg_timeout_seconds
              │
              ├─ both filled                                    → SUCCESS
              ├─ neither filled before timeout                  → cancel both, fail
              └─ one filled, other still resting/partial after
                 naked_leg_timeout_seconds since first fill     → DEFEND:
                                                                    cancel laggard,
                                                                    market-sell filled leg,
                                                                    fail with naked_leg_unwound=True

Every state transition writes through to the `orders` table so post-mortem
analysis (`scripts/inspect_orders.py`) can reconstruct what happened.
"""

from __future__ import annotations

import asyncio
import logging

from datetime import datetime, timezone
from pathlib import Path

from . import order_state
from .base import EntryPlan, EntryResult, LegResult
from .exchange import Exchange, FillState
from .safety import create_stop_file

log = logging.getLogger(__name__)

# Anchor to repo root, mirroring safety.py's DEFAULT_STOP_FILE pattern, so
# the alert log lives next to the STOP file regardless of cwd.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
CRITICAL_ALERTS_LOG = _REPO_ROOT / "data" / "CRITICAL_ALERTS.log"

# A leg with this little residual after the IOC settles is treated as fully
# matched — paying the hedged amount and abandoning a tiny stub avoids
# thrashing the book on dust. Polymarket fractional contracts can leave
# rounding noise; Kalshi is integer so any value < 1 means zero. 0.5 is
# half a contract on either venue — well below any size we'd actually trade.
MIN_RESIDUAL_CONTRACTS = 0.5


def _is_terminal(state: FillState) -> bool:
    """Order has reached a final state — fill quantity won't change.

    Polymarket IOC orders land in 'partial' when book depth was insufficient;
    that is a TERMINAL state for the order, not a 'still resting' one. We
    must treat partial as a real fill at filled_contracts.
    """
    return state.status in ("filled", "partial", "cancelled", "failed")


def _has_real_fills(state: FillState) -> bool:
    """Order executed enough contracts to count as a fill (vs noise)."""
    return state.filled_contracts >= MIN_RESIDUAL_CONTRACTS


async def execute_atomic_entry(
    *,
    plan: EntryPlan,
    exchanges: dict[str, Exchange],
    db_path: str,
    naked_leg_timeout_seconds: float = 2.0,
    per_leg_timeout_seconds: float = 5.0,
    poll_interval_seconds: float = 0.1,
    execution_mode: str = "live",
) -> EntryResult:
    yes_ex = exchanges.get(plan.leg_yes.platform)
    no_ex = exchanges.get(plan.leg_no.platform)
    if yes_ex is None or no_ex is None:
        missing = [
            p for p, ex in [
                (plan.leg_yes.platform, yes_ex),
                (plan.leg_no.platform, no_ex),
            ] if ex is None
        ]
        err = f"no exchange registered for: {missing}"
        return _all_failed(plan, err)

    # Persist pending rows BEFORE placing — if the bot crashes between
    # place and DB write, the orders table still shows we intended to fire.
    yes_id = await order_state.insert_pending(
        db_path, plan.leg_yes,
        correlation_id=plan.correlation_id,
        paper_trade_id=plan.paper_trade_id,
        pair_id=plan.pair_id,
        execution_mode=execution_mode,
    )
    no_id = await order_state.insert_pending(
        db_path, plan.leg_no,
        correlation_id=plan.correlation_id,
        paper_trade_id=plan.paper_trade_id,
        pair_id=plan.pair_id,
        execution_mode=execution_mode,
    )

    yes_place, no_place = await asyncio.gather(
        yes_ex.place_order(plan.leg_yes),
        no_ex.place_order(plan.leg_no),
    )

    # Both rejected at place
    if not yes_place.accepted and not no_place.accepted:
        await order_state.update_status(
            db_path, yes_id, status="failed", error=yes_place.error,
        )
        await order_state.update_status(
            db_path, no_id, status="failed", error=no_place.error,
        )
        log.warning(
            "Atomic entry corr=%s: BOTH legs rejected at place — yes=%s no=%s",
            plan.correlation_id, yes_place.error, no_place.error,
        )
        return EntryResult(
            plan=plan,
            leg_yes=LegResult(plan.leg_yes, "failed", 0.0, 0.0, error=yes_place.error),
            leg_no=LegResult(plan.leg_no, "failed", 0.0, 0.0, error=no_place.error),
            success=False,
            error="both legs rejected at place",
        )

    # One rejected at place — cancel the accepted one (it never executed,
    # so no naked leg risk; just clean up the resting order).
    if not yes_place.accepted:
        await no_ex.cancel_order(no_place.external_order_id)
        await order_state.update_status(
            db_path, yes_id, status="failed", error=yes_place.error,
        )
        await order_state.update_status(
            db_path, no_id, status="cancelled",
            external_order_id=no_place.external_order_id,
        )
        log.warning(
            "Atomic entry corr=%s: yes leg rejected at place (%s) — "
            "cancelled no leg (id=%s)",
            plan.correlation_id, yes_place.error, no_place.external_order_id,
        )
        return EntryResult(
            plan=plan,
            leg_yes=LegResult(plan.leg_yes, "failed", 0.0, 0.0, error=yes_place.error),
            leg_no=LegResult(plan.leg_no, "cancelled", 0.0, 0.0,
                             external_order_id=no_place.external_order_id),
            success=False,
            error="yes leg rejected; no leg cancelled",
        )
    if not no_place.accepted:
        await yes_ex.cancel_order(yes_place.external_order_id)
        await order_state.update_status(
            db_path, no_id, status="failed", error=no_place.error,
        )
        await order_state.update_status(
            db_path, yes_id, status="cancelled",
            external_order_id=yes_place.external_order_id,
        )
        log.warning(
            "Atomic entry corr=%s: no leg rejected at place (%s) — "
            "cancelled yes leg (id=%s)",
            plan.correlation_id, no_place.error, yes_place.external_order_id,
        )
        return EntryResult(
            plan=plan,
            leg_yes=LegResult(plan.leg_yes, "cancelled", 0.0, 0.0,
                              external_order_id=yes_place.external_order_id),
            leg_no=LegResult(plan.leg_no, "failed", 0.0, 0.0, error=no_place.error),
            success=False,
            error="no leg rejected; yes leg cancelled",
        )

    # Both accepted — promote to submitted, watch fills
    await order_state.update_status(
        db_path, yes_id, status="submitted",
        external_order_id=yes_place.external_order_id,
    )
    await order_state.update_status(
        db_path, no_id, status="submitted",
        external_order_id=no_place.external_order_id,
    )

    loop = asyncio.get_event_loop()
    deadline = loop.time() + per_leg_timeout_seconds
    yes_filled_at: float | None = None
    no_filled_at: float | None = None
    yes_state = FillState("submitted", 0.0, 0.0)
    no_state = FillState("submitted", 0.0, 0.0)

    while loop.time() < deadline:
        yes_state, no_state = await asyncio.gather(
            yes_ex.get_order(yes_place.external_order_id),
            no_ex.get_order(no_place.external_order_id),
        )
        now = loop.time()
        # Track first-fill time off filled_contracts so partial fills also
        # arm the naked-leg timer. Status string can lag the real fill.
        if _has_real_fills(yes_state) and yes_filled_at is None:
            yes_filled_at = now
        if _has_real_fills(no_state) and no_filled_at is None:
            no_filled_at = now

        # Both legs reached terminal state (filled / partial / cancelled /
        # failed). Branch on the actual fill quantities, not status strings:
        # partial is a real fill that just stopped short of the requested
        # size, and our hedge is min(yes_filled, no_filled).
        if _is_terminal(yes_state) and _is_terminal(no_state):
            yes_filled = yes_state.filled_contracts
            no_filled = no_state.filled_contracts
            yes_has = _has_real_fills(yes_state)
            no_has = _has_real_fills(no_state)

            if yes_has and no_has:
                return await _settle_with_residual(
                    plan=plan,
                    yes_ex=yes_ex, no_ex=no_ex,
                    yes_id=yes_id, no_id=no_id,
                    yes_place=yes_place, no_place=no_place,
                    yes_state=yes_state, no_state=no_state,
                    db_path=db_path,
                )
            if yes_has != no_has:
                # One side has real fills, other terminal-with-zero
                # (cancelled/failed/partial-below-MIN). Defend the filled
                # side immediately — waiting for naked timeout when we
                # already know the other side won't fill is pure exposure.
                return await _defend_naked_leg(
                    plan=plan,
                    yes_ex=yes_ex, no_ex=no_ex,
                    yes_id=yes_id, no_id=no_id,
                    yes_place=yes_place, no_place=no_place,
                    yes_state=yes_state, no_state=no_state,
                    db_path=db_path,
                )
            # Both terminal, neither filled — both cancelled/failed.
            # Fall through to per-leg-timeout cleanup.
            break

        # Naked-leg trigger: one side has real fills, the other is still
        # resting (not terminal yet). After naked timeout, defend.
        first_filled_at = yes_filled_at if yes_filled_at is not None else no_filled_at
        if first_filled_at is not None and now - first_filled_at >= naked_leg_timeout_seconds:
            return await _defend_naked_leg(
                plan=plan,
                yes_ex=yes_ex, no_ex=no_ex,
                yes_id=yes_id, no_id=no_id,
                yes_place=yes_place, no_place=no_place,
                yes_state=yes_state, no_state=no_state,
                db_path=db_path,
            )

        await asyncio.sleep(poll_interval_seconds)

    # Total timeout — neither leg filled in budget, nothing to defend
    await asyncio.gather(
        yes_ex.cancel_order(yes_place.external_order_id),
        no_ex.cancel_order(no_place.external_order_id),
    )
    await order_state.update_status(
        db_path, yes_id, status="cancelled",
        external_order_id=yes_place.external_order_id, error="per_leg_timeout",
    )
    await order_state.update_status(
        db_path, no_id, status="cancelled",
        external_order_id=no_place.external_order_id, error="per_leg_timeout",
    )
    log.warning(
        "Atomic entry corr=%s: per_leg_timeout — neither leg filled, "
        "both cancelled",
        plan.correlation_id,
    )
    return EntryResult(
        plan=plan,
        leg_yes=LegResult(plan.leg_yes, "cancelled",
                          yes_state.filled_contracts, yes_state.avg_fill_price,
                          external_order_id=yes_place.external_order_id),
        leg_no=LegResult(plan.leg_no, "cancelled",
                         no_state.filled_contracts, no_state.avg_fill_price,
                         external_order_id=no_place.external_order_id),
        success=False,
        error="per_leg_timeout — neither leg filled in budget",
    )


async def _settle_with_residual(
    *,
    plan: EntryPlan,
    yes_ex: Exchange,
    no_ex: Exchange,
    yes_id: int,
    no_id: int,
    yes_place,
    no_place,
    yes_state: FillState,
    no_state: FillState,
    db_path: str,
) -> EntryResult:
    """Both legs have real fills. Hedged amount is min(yes, no); any excess
    on the overfilled side is naked exposure that must be unwound.

    Common case: both fully filled (residual = 0) — straight success path.
    Partial case: e.g. yes=5, no=4.7 → hedge 4.7, market-sell 0.3 from yes.
    """
    yes_filled = yes_state.filled_contracts
    no_filled = no_state.filled_contracts
    hedged = min(yes_filled, no_filled)
    residual = abs(yes_filled - no_filled)

    # Persist whatever-actually-filled, using the real status (filled vs
    # partial) so post-mortem can tell which leg fell short.
    await order_state.update_status(
        db_path, yes_id, status=yes_state.status,
        filled_contracts=yes_filled, avg_fill_price=yes_state.avg_fill_price,
    )
    await order_state.update_status(
        db_path, no_id, status=no_state.status,
        filled_contracts=no_filled, avg_fill_price=no_state.avg_fill_price,
    )

    residual_realized = 0.0
    residual_unwound = True
    if residual >= MIN_RESIDUAL_CONTRACTS:
        # Overfilled side has unhedged contracts. Market-sell them on the
        # overfilled venue to flatten exposure on that side.
        if yes_filled > no_filled:
            over_ex, over_plan, over_label = yes_ex, plan.leg_yes, "yes"
        else:
            over_ex, over_plan, over_label = no_ex, plan.leg_no, "no"
        log.warning(
            "Atomic entry corr=%s: PARTIAL HEDGE — yes=%.2f no=%.2f, "
            "hedged=%.2f, unwinding %.2f from %s leg",
            plan.correlation_id, yes_filled, no_filled, hedged, residual, over_label,
        )
        try:
            sell = await over_ex.market_sell(over_plan, residual)
            residual_realized = sell.realized_usd - residual * (
                yes_state.avg_fill_price if over_label == "yes"
                else no_state.avg_fill_price
            )
        except Exception as e:
            log.error(
                "Atomic entry corr=%s: residual unwind FAILED on %s leg: %s "
                "— %.2f contracts still naked. Manual unwind required.",
                plan.correlation_id, over_label, e, residual,
            )
            residual_unwound = False
            await _emergency_halt(
                db_path,
                reason=(
                    f"residual_unwind_failed corr={plan.correlation_id} "
                    f"leg={over_label} contracts={residual:.2f} err={e}"
                ),
            )
    else:
        log.info(
            "Atomic entry corr=%s: both legs filled — yes %.2f@%.4f no %.2f@%.4f "
            "(residual %.4f below %.2f, ignored)",
            plan.correlation_id, yes_filled, yes_state.avg_fill_price,
            no_filled, no_state.avg_fill_price, residual, MIN_RESIDUAL_CONTRACTS,
        )

    return EntryResult(
        plan=plan,
        leg_yes=LegResult(
            plan.leg_yes,
            yes_state.status,
            yes_filled, yes_state.avg_fill_price,
            external_order_id=yes_place.external_order_id,
        ),
        leg_no=LegResult(
            plan.leg_no,
            no_state.status,
            no_filled, no_state.avg_fill_price,
            external_order_id=no_place.external_order_id,
        ),
        success=residual_unwound,
        naked_leg_unwound=(residual >= MIN_RESIDUAL_CONTRACTS) and residual_unwound,
        naked_leg_realized_usd=round(residual_realized, 4) if residual >= MIN_RESIDUAL_CONTRACTS else 0.0,
        error=(
            None if residual < MIN_RESIDUAL_CONTRACTS
            else f"partial_hedge: hedged {hedged:.2f}, residual {residual:.2f} "
                 f"{'unwound' if residual_unwound else 'NAKED — manual intervention'}"
        ),
    )


async def _defend_naked_leg(
    *,
    plan: EntryPlan,
    yes_ex: Exchange,
    no_ex: Exchange,
    yes_id: int,
    no_id: int,
    yes_place,
    no_place,
    yes_state: FillState,
    no_state: FillState,
    db_path: str,
) -> EntryResult:
    """One leg filled, other didn't — cancel laggard, market-sell filled leg."""
    if yes_state.status == "filled":
        filled_leg, filled_id, filled_state, filled_place, filled_ex = (
            "yes", yes_id, yes_state, yes_place, yes_ex,
        )
        naked_leg, naked_id, naked_place, naked_ex = (
            "no", no_id, no_place, no_ex,
        )
        filled_plan = plan.leg_yes
    else:
        filled_leg, filled_id, filled_state, filled_place, filled_ex = (
            "no", no_id, no_state, no_place, no_ex,
        )
        naked_leg, naked_id, naked_place, naked_ex = (
            "yes", yes_id, yes_place, yes_ex,
        )
        filled_plan = plan.leg_no

    log.warning(
        "Atomic entry corr=%s: NAKED LEG — %s filled, %s didn't. "
        "Cancelling %s order, market-selling %s contracts",
        plan.correlation_id, filled_leg, naked_leg,
        naked_place.external_order_id, filled_state.filled_contracts,
    )

    # Cancel the laggard
    await naked_ex.cancel_order(naked_place.external_order_id)
    await order_state.update_status(
        db_path, naked_id, status="cancelled",
        external_order_id=naked_place.external_order_id,
        error=f"naked_leg_timeout (other leg filled)",
    )

    # Mark the filled leg's state in DB before unwinding so we have a
    # complete trail even if market_sell fails
    await order_state.update_status(
        db_path, filled_id, status="filled",
        filled_contracts=filled_state.filled_contracts,
        avg_fill_price=filled_state.avg_fill_price,
        external_order_id=filled_place.external_order_id,
    )

    # Market-sell the filled leg
    try:
        sell = await filled_ex.market_sell(
            filled_plan, filled_state.filled_contracts,
        )
    except Exception as e:
        log.error(
            "Atomic entry corr=%s: market_sell FAILED: %s — leg %s is still "
            "naked! Manual unwind required.",
            plan.correlation_id, e, filled_leg,
        )
        # Naked exposure with no automated remediation. Halt the bot so we
        # don't pile on more risk while one position is broken open.
        await _emergency_halt(
            db_path,
            reason=(
                f"naked_leg_market_sell_failed corr={plan.correlation_id} "
                f"leg={filled_leg} contracts={filled_state.filled_contracts:.2f} "
                f"avg_price={filled_state.avg_fill_price:.4f} err={e}"
            ),
        )
        return _build_naked_result(
            plan, yes_state, no_state, yes_place, no_place,
            naked_unwound=False, naked_realized=0.0,
            error=f"naked_leg market_sell failed: {e}",
        )

    cost = filled_state.filled_contracts * filled_state.avg_fill_price
    pnl = sell.realized_usd - cost
    log.warning(
        "Atomic entry corr=%s: naked-leg unwound — %.2f contracts cost $%.2f, "
        "sold for $%.2f, realized %s$%.2f",
        plan.correlation_id, filled_state.filled_contracts, cost,
        sell.realized_usd, "+" if pnl >= 0 else "-", abs(pnl),
    )

    return _build_naked_result(
        plan, yes_state, no_state, yes_place, no_place,
        naked_unwound=True, naked_realized=pnl,
        error=f"naked_leg defended: {filled_leg} filled, {naked_leg} cancelled",
    )


async def _emergency_halt(db_path: str, *, reason: str) -> None:
    """Stop the bot and leave a permanent record. Called when an automated
    unwind path fails — naked exposure remains and a human must intervene.

    Three actions, none of which can fail-silent:
      1. Write data/STOP — main loop's safety_gate refuses new orders.
      2. Append to data/CRITICAL_ALERTS.log — auditable post-mortem trail
         that survives bot restarts (data/STOP gets removed on resolve).
      3. Log at CRITICAL severity so any log-watcher / shipping pipeline
         escalates appropriately.
    """
    timestamp = datetime.now(timezone.utc).isoformat()
    full_reason = f"EMERGENCY_HALT: {reason}"

    try:
        create_stop_file(full_reason)
    except Exception as e:
        log.error("emergency halt: STOP file write failed: %s", e)

    try:
        CRITICAL_ALERTS_LOG.parent.mkdir(parents=True, exist_ok=True)
        with open(CRITICAL_ALERTS_LOG, "a", encoding="utf-8") as f:
            f.write(f"{timestamp}\t{full_reason}\n")
    except Exception as e:
        log.error("emergency halt: CRITICAL_ALERTS.log write failed: %s", e)

    log.critical(
        "EMERGENCY HALT %s — bot stopped via STOP file. Reason: %s",
        timestamp, reason,
    )


def _build_naked_result(
    plan: EntryPlan,
    yes_state: FillState,
    no_state: FillState,
    yes_place,
    no_place,
    naked_unwound: bool,
    naked_realized: float,
    error: str,
) -> EntryResult:
    return EntryResult(
        plan=plan,
        leg_yes=LegResult(
            plan.leg_yes,
            "filled" if yes_state.status == "filled" else "cancelled",
            yes_state.filled_contracts, yes_state.avg_fill_price,
            external_order_id=yes_place.external_order_id,
        ),
        leg_no=LegResult(
            plan.leg_no,
            "filled" if no_state.status == "filled" else "cancelled",
            no_state.filled_contracts, no_state.avg_fill_price,
            external_order_id=no_place.external_order_id,
        ),
        success=False,
        naked_leg_unwound=naked_unwound,
        naked_leg_realized_usd=round(naked_realized, 4),
        error=error,
    )


def _all_failed(plan: EntryPlan, err: str) -> EntryResult:
    return EntryResult(
        plan=plan,
        leg_yes=LegResult(plan.leg_yes, "failed", 0.0, 0.0, error=err),
        leg_no=LegResult(plan.leg_no, "failed", 0.0, 0.0, error=err),
        success=False,
        error=err,
    )
