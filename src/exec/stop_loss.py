"""Portfolio-level stop-loss watcher.

Sums realized_profit_usd + partial_realized_usd across live-mode paper
trades. When the total falls below the configured threshold (a negative
number, e.g. -120), writes data/STOP via _emergency_halt so the safety
gate refuses further sends.

Why this is separate from per-trade caps: a single trade can lose at
most ~$10-20 with current sizing, but a string of unlucky correlated
losses, or an undetected matcher bug, can rack up many small losses.
We want a circuit breaker on the *aggregate*.
"""

from __future__ import annotations

import logging

import aiosqlite

log = logging.getLogger(__name__)


async def cumulative_realized_usd(db_path: str) -> float:
    """Sum realized + partial across closed and still-open paper trades.

    Includes:
      - status IN ('closed','exited','resolved'): realized_profit_usd
      - status = 'open' with partial unwinds: partial_realized_usd

    Excludes:
      - 'archived' / 'paper_archived' / 'legacy_broken' (operator-flagged
        bad pairs that don't reflect strategy P&L)
      - 'rejected' / 'failed'
    """
    async with aiosqlite.connect(db_path) as db:
        cur = await db.execute(
            """SELECT
                 COALESCE(SUM(CASE
                     WHEN status IN ('closed','exited','resolved')
                       THEN realized_profit_usd
                     WHEN status = 'open'
                       THEN COALESCE(partial_realized_usd, 0)
                     ELSE 0
                   END), 0) AS total
               FROM paper_trades
               WHERE pair_quality = 'good'"""
        )
        row = await cur.fetchone()
        return float(row[0] or 0.0)


async def check_portfolio_stop_loss(
    db_path: str,
    threshold_usd: float,
    *,
    emergency_halt_fn=None,
) -> tuple[float, bool]:
    """Returns (cumulative, halted). If halted, threshold was breached and
    the halt function was invoked.

    Disabled when threshold_usd >= 0 (a stop-loss above zero makes no
    sense — would halt at first profit).
    """
    if threshold_usd >= 0:
        return 0.0, False

    if emergency_halt_fn is None:
        from .atomic_orchestrator import _emergency_halt
        emergency_halt_fn = _emergency_halt

    cumulative = await cumulative_realized_usd(db_path)
    if cumulative <= threshold_usd:
        await emergency_halt_fn(
            db_path,
            reason=(
                f"portfolio_stop_loss: cumulative realized ${cumulative:.2f} "
                f"crossed threshold ${threshold_usd:.2f} — bot halted"
            ),
        )
        return cumulative, True
    return cumulative, False
