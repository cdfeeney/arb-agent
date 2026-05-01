"""Tabulate WHY each open paper trade is being held — distinguish 'market
hasn't moved' from 'data missing' from 'fees swallow profit'.

Run from project root:
    py -3 -m scripts.why_hold

Reads the most recent mark per open paper_trade and groups by decision_reason.
This is the diagnostic we run before tweaking exit thresholds — if everything
is HOLD because top_bids <= cost, the markets just haven't converged. If
everything is HOLD because of missing books, there's a different bug.
"""
import asyncio
import sys
from collections import Counter
from pathlib import Path

import aiosqlite

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.config import load_config


def categorize_reason(reason: str | None) -> str:
    if not reason:
        return "(empty)"
    r = reason.lower()
    if "missing bid book" in r:
        return "missing_bid_book"
    if "zero best-bid" in r:
        return "zero_best_bid"
    if "<= cost" in r:
        return "market_not_moved"
    if "below min" in r and "wait for thicker" in r:
        return "watch_too_thin"
    if "slippage" in r and "max" in r:
        return "watch_slippage"
    if "swallow the gross" in r:
        return "watch_fees_swallow"
    if "resolves in" in r and "hold for full" in r:
        return "near_resolution"
    if "fully unwound" in r:
        return "fully_unwound"
    if "monitor disabled" in r:
        return "monitor_disabled"
    return "other"


async def main():
    cfg = load_config("config.yaml")
    db_path = cfg["database"]["path"]

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("""
            SELECT pt.id AS trade_id, pt.pair_id,
                   m.exit_recommendation, m.decision_reason,
                   m.yes_bid_now, m.no_bid_now,
                   (COALESCE(m.yes_bid_now,0) + COALESCE(m.no_bid_now,0)) AS sum_bids,
                   m.cost_basis_usd, m.unwind_value_usd,
                   m.convergence_ratio, m.observed_at
            FROM paper_trade_marks m
            JOIN paper_trades pt ON pt.id = m.paper_trade_id
            JOIN (
                SELECT paper_trade_id, MAX(observed_at) AS latest
                FROM paper_trade_marks
                GROUP BY paper_trade_id
            ) latest_m ON m.paper_trade_id = latest_m.paper_trade_id
                       AND m.observed_at = latest_m.latest
            WHERE pt.status = 'open'
            ORDER BY m.observed_at DESC
        """)
        rows = list(await cur.fetchall())

    if not rows:
        print("No open paper trades with marks. Bot may not have run yet, "
              "or all trades have been archived.")
        return

    cats: Counter = Counter()
    for r in rows:
        rec = r["exit_recommendation"] or "?"
        cat = categorize_reason(r["decision_reason"])
        cats[(rec, cat)] += 1

    print(f"\n=== Why are {len(rows)} open paper trades held? ===\n")
    print(f"{'Recommendation':<14} {'Reason category':<22} {'Count':>6}")
    print("-" * 46)
    for (rec, cat), n in cats.most_common():
        print(f"{rec:<14} {cat:<22} {n:>6}")

    if not cats:
        return
    (top_rec, top_cat), _ = cats.most_common(1)[0]
    print(f"\nTop category — sample rows ({top_rec} / {top_cat}):")
    shown = 0
    for r in rows:
        if r["exit_recommendation"] == top_rec and categorize_reason(r["decision_reason"]) == top_cat:
            sb = r["sum_bids"] if r["sum_bids"] is not None else 0.0
            cb = r["cost_basis_usd"] or 0.0
            uv = r["unwind_value_usd"] or 0.0
            cpc = (cb / max(0.0001, (uv if uv > 0 else cb))) if cb > 0 else 0
            reason = (r["decision_reason"] or "")[:80]
            print(f"  trade #{r['trade_id']:5d}  sum_bids={sb:.4f}  "
                  f"cost_basis=${cb:.2f}  unwind_val=${uv:.2f}  "
                  f"reason: {reason}")
            shown += 1
            if shown >= 10:
                break


if __name__ == "__main__":
    asyncio.run(main())
