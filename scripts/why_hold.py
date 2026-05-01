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

    # Specific drill-down on missing_bid_book: which leg, which platform?
    if any(categorize_reason(r["decision_reason"]) == "missing_bid_book" for r in rows):
        print("\n=== missing_bid_book drill-down ===")
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("""
                SELECT pt.id AS trade_id, pt.pair_id,
                       pt.yes_platform, pt.yes_ticker, pt.yes_token,
                       pt.no_platform,  pt.no_ticker,  pt.no_token,
                       m.yes_bid_now, m.no_bid_now
                FROM paper_trade_marks m
                JOIN paper_trades pt ON pt.id = m.paper_trade_id
                JOIN (
                    SELECT paper_trade_id, MAX(observed_at) AS latest
                    FROM paper_trade_marks GROUP BY paper_trade_id
                ) latest_m ON m.paper_trade_id = latest_m.paper_trade_id
                           AND m.observed_at = latest_m.latest
                WHERE pt.status='open' AND m.exit_recommendation='HOLD'
                  AND m.decision_reason LIKE '%missing bid book%'
            """)
            detail = list(await cur.fetchall())

        leg_status = Counter()
        for r in detail:
            yes_ok = (r["yes_bid_now"] or 0) > 0
            no_ok = (r["no_bid_now"] or 0) > 0
            yes_has_token = r["yes_token"] is not None and r["yes_token"] != ""
            no_has_token = r["no_token"] is not None and r["no_token"] != ""
            if yes_ok and not no_ok:
                kind = f"NO  leg empty ({r['no_platform']:10})"
                if r["no_platform"] == "polymarket" and not no_has_token:
                    kind += " [no_token NULL]"
            elif no_ok and not yes_ok:
                kind = f"YES leg empty ({r['yes_platform']:10})"
                if r["yes_platform"] == "polymarket" and not yes_has_token:
                    kind += " [yes_token NULL]"
            elif not yes_ok and not no_ok:
                kind = "BOTH legs empty"
            else:
                kind = "BOTH legs have bids (anomaly)"
            leg_status[kind] += 1

        print(f"\n{'Pattern':<50} {'Count':>6}")
        print("-" * 60)
        for k, n in leg_status.most_common():
            print(f"{k:<50} {n:>6}")

        # Show 5 samples of the dominant pattern with full ticker info
        if leg_status:
            top_pattern, _ = leg_status.most_common(1)[0]
            print(f"\nSamples of '{top_pattern}':")
            shown = 0
            for r in detail:
                yes_ok = (r["yes_bid_now"] or 0) > 0
                no_ok = (r["no_bid_now"] or 0) > 0
                if "NO  leg empty" in top_pattern and yes_ok and not no_ok:
                    pass
                elif "YES leg empty" in top_pattern and no_ok and not yes_ok:
                    pass
                elif "BOTH legs empty" in top_pattern and not yes_ok and not no_ok:
                    pass
                else:
                    continue
                print(f"  #{r['trade_id']:4d}  yes={r['yes_platform']:10}:{r['yes_ticker'][:25]:<25} "
                      f"no={r['no_platform']:10}:{r['no_ticker'][:25]:<25}  "
                      f"yes_bid={r['yes_bid_now'] or 0:.3f}  no_bid={r['no_bid_now'] or 0:.3f}")
                shown += 1
                if shown >= 5:
                    break


if __name__ == "__main__":
    asyncio.run(main())
