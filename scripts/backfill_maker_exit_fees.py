"""Recompute realized P/L on maker-exited trades using the correct Kalshi fee.

Bug background: position_monitor._handle_resting_maker used to compute the
Kalshi taker fee as `rate × contracts × price`, dropping the (1-price)
factor and the cent-ceiling. For high-priced legs (~$0.80) this overstated
fees ~5×, recording phantom losses on trades that actually made money.

This script identifies every paper_trade closed via maker-exit (status
'closed' AND has at least one row in maker_exit_orders with status
'filled'), recomputes the correct fee, and updates:
  * partial_unwind_realized_usd in the latest paper_trade_marks row
  * partial_realized_usd on paper_trades
  * realized_profit_usd on paper_trades

Idempotent enough: only updates when the recomputed value materially
differs from what's stored.

Usage:
    python -m scripts.backfill_maker_exit_fees             # dry-run
    python -m scripts.backfill_maker_exit_fees --execute   # commit changes
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

import aiosqlite

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.config import load_config
from src.engine.fees import kalshi_taker_fee


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true",
                    help="Actually write changes. Default is dry-run.")
    args = ap.parse_args()

    cfg = load_config("config.yaml")
    db_path = cfg["database"]["path"]
    kalshi_rate = float(cfg.get("fees", {}).get("kalshi_fee_rate", 0.07))

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row

        # Find all maker-filled orders with their parent trade
        cur = await db.execute("""
            SELECT
                meo.paper_trade_id        AS trade_id,
                meo.contracts             AS unwind_contracts,
                meo.fill_price            AS poly_fill_price,
                meo.realized_gross_usd    AS recorded_gross,
                pt.no_platform, pt.yes_platform,
                pt.no_observed_price, pt.yes_observed_price,
                pt.no_contracts, pt.yes_contracts,
                pt.fees_estimated_usd     AS entry_fees,
                pt.partial_realized_usd   AS stored_partial,
                pt.realized_profit_usd    AS stored_realized,
                pt.status,
                pt.pair_quality
            FROM maker_exit_orders meo
            JOIN paper_trades pt ON pt.id = meo.paper_trade_id
            WHERE meo.status = 'filled'
              AND pt.status = 'closed'
            ORDER BY meo.paper_trade_id
        """)
        rows = list(await cur.fetchall())

        if not rows:
            print("No maker-filled closed trades. Nothing to backfill.")
            return

        print(f"Found {len(rows)} maker-filled closed trades to inspect.\n")

        updates = []
        for r in rows:
            # Identify the kalshi side (the one we taker-sold at exit)
            if r["no_platform"] == "kalshi":
                kalshi_exit_price = float(r["no_observed_price"] or 0)
                # We need the EXIT price (best_bid at fill) — closest proxy is
                # to look at the most recent paper_trade_mark row's no_bid_now.
                cur2 = await db.execute("""
                    SELECT no_bid_now, yes_bid_now FROM paper_trade_marks
                    WHERE paper_trade_id = ?
                    ORDER BY observed_at DESC LIMIT 1
                """, (r["trade_id"],))
                m = await cur2.fetchone()
                kalshi_exit_price = float(m["no_bid_now"] or kalshi_exit_price)
                kalshi_leg = "no"
            elif r["yes_platform"] == "kalshi":
                cur2 = await db.execute("""
                    SELECT no_bid_now, yes_bid_now FROM paper_trade_marks
                    WHERE paper_trade_id = ?
                    ORDER BY observed_at DESC LIMIT 1
                """, (r["trade_id"],))
                m = await cur2.fetchone()
                kalshi_exit_price = float((m and m["yes_bid_now"]) or r["yes_observed_price"] or 0)
                kalshi_leg = "yes"
            else:
                # No kalshi leg — no fee bug to fix
                continue

            unwind = float(r["unwind_contracts"])
            entry_fees = float(r["entry_fees"] or 0)
            recorded_gross = float(r["recorded_gross"] or 0)

            # Buggy fee was rate * contracts * price (no 1-P factor)
            buggy_fee = kalshi_rate * unwind * kalshi_exit_price
            # Correct fee
            correct_fee = kalshi_taker_fee(unwind, kalshi_exit_price, kalshi_rate)

            buggy_partial = round(recorded_gross - buggy_fee, 4)
            correct_partial = round(recorded_gross - correct_fee, 4)
            buggy_final = round(buggy_partial - entry_fees, 4)
            correct_final = round(correct_partial - entry_fees, 4)

            stored_partial = float(r["stored_partial"] or 0)
            stored_realized = float(r["stored_realized"] or 0)

            delta_partial = round(correct_partial - stored_partial, 4)
            delta_final = round(correct_final - stored_realized, 4)

            # Only update if the recomputed value is materially different
            if abs(delta_final) < 0.0001:
                continue

            updates.append({
                "trade_id": int(r["trade_id"]),
                "kalshi_leg": kalshi_leg,
                "kalshi_exit_price": kalshi_exit_price,
                "unwind": unwind,
                "buggy_fee": buggy_fee,
                "correct_fee": correct_fee,
                "stored_partial": stored_partial,
                "correct_partial": correct_partial,
                "stored_realized": stored_realized,
                "correct_realized": correct_final,
                "delta": delta_final,
            })

        print(f"{'Trade':>5} {'leg':>3} {'sellP':>6} {'C':>4} "
              f"{'old fee':>8} {'new fee':>8} "
              f"{'old realized':>14} {'new realized':>14} {'Δ':>7}")
        print("-" * 80)
        total_delta = 0.0
        for u in updates:
            print(f"{u['trade_id']:>5} {u['kalshi_leg']:>3} {u['kalshi_exit_price']:>6.4f} "
                  f"{u['unwind']:>4.0f} "
                  f"{u['buggy_fee']:>8.4f} {u['correct_fee']:>8.4f} "
                  f"{u['stored_realized']:>14.4f} {u['correct_realized']:>14.4f} "
                  f"{u['delta']:>+7.4f}")
            total_delta += u["delta"]
        print("-" * 80)
        print(f"Total realized change: {total_delta:+.4f} on {len(updates)} trades")

        if not args.execute:
            print("\nDry-run only. Re-run with --execute to commit.")
            return

        for u in updates:
            await db.execute("""
                UPDATE paper_trades SET
                    partial_realized_usd = ?,
                    realized_profit_usd  = ?
                WHERE id = ?
            """, (u["correct_partial"], u["correct_realized"], u["trade_id"]))
            # Also patch the latest mark row so paper_summary / debugging
            # sees the corrected partial.
            await db.execute("""
                UPDATE paper_trade_marks
                SET partial_unwind_realized_usd = ?
                WHERE id = (
                    SELECT id FROM paper_trade_marks
                    WHERE paper_trade_id = ? AND partial_unwind_size IS NOT NULL
                    ORDER BY observed_at DESC LIMIT 1
                )
            """, (u["correct_partial"], u["trade_id"]))
        await db.commit()
        print(f"\nApplied {len(updates)} updates.")


if __name__ == "__main__":
    asyncio.run(main())
