"""Backtest exit thresholds against captured paper_trade_marks.

For each closed (exited or resolved) paper trade, replay its marks and ask:
  - what would total realized P&L have been at convergence_threshold X?
  - average days saved vs hold-to-resolution?

Run from project root:
    py -3 -m scripts.exit_analysis
    py -3 -m scripts.exit_analysis --thresholds 0.5,0.6,0.7,0.8,0.9
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from collections import defaultdict
from pathlib import Path

import aiosqlite

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.config import load_config


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--thresholds", type=str, default="0.5,0.6,0.7,0.8,0.9",
        help="Comma-separated convergence thresholds to evaluate",
    )
    args = ap.parse_args()
    thresholds = [float(t) for t in args.thresholds.split(",") if t]

    cfg = load_config("config.yaml")
    db_path = cfg["database"]["path"]

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row

        # All paper trades that have at least one mark (open or closed)
        cur = await db.execute(
            """SELECT pt.id, pt.pair_id, pt.detected_at, pt.closes_at,
                      pt.predicted_net_usd, pt.realized_profit_usd, pt.status,
                      pt.yes_size_usd + pt.no_size_usd AS cost_basis
               FROM paper_trades pt
               WHERE EXISTS (
                   SELECT 1 FROM paper_trade_marks m WHERE m.paper_trade_id = pt.id
               )
               ORDER BY pt.detected_at""",
        )
        trades = list(await cur.fetchall())

        print("\n=== Exit Threshold Backtest ===\n")
        if not trades:
            print("No paper trades with marks yet. Run the bot for at least one cycle"
                  " with open positions to generate marks.\n")
            return

        print(f"Analyzing {len(trades)} paper trades with mark history.\n")

        # For each threshold, simulate exits.
        for thresh in thresholds:
            results = await simulate_threshold(db, trades, thresh)
            print(
                f"Convergence threshold = {thresh*100:.0f}%:"
                f"\n  trades exited early: {results['n_exited']}/{len(trades)}"
                f"\n  total realized: ${results['total_realized']:.2f}"
                f"\n  total hypothetical hold-to-close: ${results['total_hold']:.2f}"
                f"\n  avg days saved per exited trade: {results['avg_days_saved']:.1f}"
                f"\n  avg annualized realized: {results['avg_annualized']:.1f}%"
                f"\n  avg annualized hold-only: {results['avg_annualized_hold']:.1f}%"
                f"\n",
            )

        # Per-trade detail.
        #
        # Two different "P&L" numbers tell different stories — display both:
        #   * MTM (mark-to-market): unwind value at today's bid books minus
        #     cost basis. Goes deeply negative on thin books even when the
        #     arb is fine, because we can't sell into nothing. NOT a real
        #     loss unless we actually try to exit.
        #   * Resolution P&L: locked_payout (= min(yes,no)*$1) minus cost
        #     basis. This is what we'd actually collect by holding to
        #     resolution. For a real arb this should ≈ predicted_net_usd.
        #
        # If MTM craters but resolution P&L ≈ predicted, the position is
        # fine — just hold to close. If both crater, the entry math was
        # wrong (true phantom).
        print(
            "Per-trade detail — pred = expected at entry, hold = expected at"
            " resolution, mtm = liquidation today (top 20):\n"
        )
        cur = await db.execute(
            """SELECT pt.id, pt.pair_id, pt.predicted_net_usd, pt.status,
                      pt.yes_size_usd + pt.no_size_usd                AS cost,
                      pt.partial_realized_usd                         AS partial_realized,
                      pt.contracts_remaining                          AS remaining,
                      pt.yes_contracts                                AS orig_contracts,
                      MAX(m.locked_payout_usd) - (pt.yes_size_usd + pt.no_size_usd)
                                                                       AS hold_pnl,
                      MAX(m.mark_to_market_usd)                       AS max_mtm,
                      MIN(m.mark_to_market_usd)                       AS min_mtm,
                      MAX(m.convergence_ratio)                        AS max_conv
               FROM paper_trades pt
               JOIN paper_trade_marks m ON m.paper_trade_id = pt.id
               GROUP BY pt.id
               ORDER BY pt.detected_at DESC LIMIT 20""",
        )
        for r in await cur.fetchall():
            hold = r["hold_pnl"] if r["hold_pnl"] is not None else 0.0
            partial = r["partial_realized"] or 0.0
            orig = r["orig_contracts"] or 0
            remaining = r["remaining"] if r["remaining"] is not None else orig
            unwound_pct = (
                (orig - remaining) / orig * 100 if orig > 0 else 0.0
            )
            print(
                f"  #{r['id']:>4}  status={r['status']:<8} "
                f"cost=${(r['cost'] or 0):>6.2f}  "
                f"pred=${r['predicted_net_usd']:>6.2f}  "
                f"hold=${hold:>6.2f}  "
                f"partial=${partial:>5.2f} ({unwound_pct:>4.0f}% unwound)  "
                f"mtm=[{(r['min_mtm'] or 0):>7.2f}..{(r['max_mtm'] or 0):>7.2f}]"
            )
            print(f"    {r['pair_id'][:80]}")


async def simulate_threshold(
    db: aiosqlite.Connection, trades: list, threshold: float,
) -> dict:
    """For each trade, find first mark where convergence >= threshold.
    If found, that's the realized exit value. Otherwise hold-to-close
    realized value applies.
    """
    n_exited = 0
    total_realized = 0.0
    total_hold = 0.0
    days_saved_acc: list[float] = []
    annualized_realized: list[float] = []
    annualized_hold: list[float] = []

    for t in trades:
        cur = await db.execute(
            """SELECT observed_at, mark_to_market_usd, convergence_ratio,
                      days_held, days_remaining, slippage_pct
               FROM paper_trade_marks
               WHERE paper_trade_id = ?
               ORDER BY observed_at""",
            (t["id"],),
        )
        marks = list(await cur.fetchall())
        if not marks:
            continue

        cost_basis = float(t["cost_basis"] or 0) or 1.0
        hold_realized = float(t["realized_profit_usd"] or 0)
        # If trade is open / no resolution yet, hold value is unknown — use
        # final mark's mtm as a proxy.
        if t["status"] in ("open", "exited"):
            hold_realized = (
                float(marks[-1]["mark_to_market_usd"]) if marks else 0.0
            )

        first_trigger = next(
            (m for m in marks
             if (m["convergence_ratio"] or 0) >= threshold
             and (m["slippage_pct"] or 1) <= 0.02),
            None,
        )
        if first_trigger:
            n_exited += 1
            realized = float(first_trigger["mark_to_market_usd"])
            days_held = float(first_trigger["days_held"] or 0)
            total_held = days_held + float(first_trigger["days_remaining"] or 0)
            days_saved_acc.append(max(0.0, total_held - days_held))
            if days_held > 0:
                annualized_realized.append(
                    (realized / cost_basis) * (365.0 / days_held) * 100
                )
            if total_held > 0:
                annualized_hold.append(
                    (hold_realized / cost_basis) * (365.0 / total_held) * 100
                )
        else:
            realized = hold_realized

        total_realized += realized
        total_hold += hold_realized

    def _avg(xs: list[float]) -> float:
        return sum(xs) / len(xs) if xs else 0.0

    return {
        "n_exited": n_exited,
        "total_realized": total_realized,
        "total_hold": total_hold,
        "avg_days_saved": _avg(days_saved_acc),
        "avg_annualized": _avg(annualized_realized),
        "avg_annualized_hold": _avg(annualized_hold),
    }


if __name__ == "__main__":
    asyncio.run(main())
