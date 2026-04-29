"""Print a summary of correlated-lag signal activity.

Run from project root:
    py -3 -m scripts.lag_summary
    py -3 -m scripts.lag_summary --recent 12   # only last 12h
"""
import argparse
import asyncio
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import aiosqlite

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.config import load_config


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--recent", type=float, default=None,
        help="Show only signals from last N hours",
    )
    ap.add_argument(
        "--top", type=int, default=10,
        help="How many top signals (by strength) to show",
    )
    args = ap.parse_args()

    cfg = load_config("config.yaml")
    db_path = cfg["database"]["path"]

    where = ""
    params: tuple = ()
    if args.recent is not None:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(hours=args.recent)
        ).isoformat()
        where = "WHERE detected_at >= ?"
        params = (cutoff,)

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row

        # Headline counts by direction + status
        cur = await db.execute(
            f"""SELECT direction, status, COUNT(*) n,
                       AVG(ABS(btc_pct_change)) avg_btc,
                       AVG(signal_strength) avg_strength,
                       SUM(CASE WHEN market_repriced=1 THEN 1 ELSE 0 END) repriced,
                       SUM(CASE WHEN market_repriced=0 THEN 1 ELSE 0 END) not_repriced
                FROM lag_signals {where}
                GROUP BY direction, status
                ORDER BY direction, status""",
            params,
        )
        rows = list(await cur.fetchall())

        suffix = f" (last {args.recent}h)" if args.recent else ""
        print(f"\n=== Lag Signal Summary{suffix} ===\n")
        if not rows:
            print("No lag signals captured yet.\n")
        else:
            print(
                f"{'Direction':<10} {'Status':<10} {'Count':>6} "
                f"{'Avg|BTC%|':>10} {'AvgStr':>7} {'Repriced':>9} {'NotMoved':>9}"
            )
            print("-" * 75)
            total = 0
            for r in rows:
                total += r["n"]
                rep = r["repriced"] if r["repriced"] is not None else 0
                nrep = r["not_repriced"] if r["not_repriced"] is not None else 0
                print(
                    f"{r['direction']:<10} {r['status']:<10} {r['n']:>6} "
                    f"{(r['avg_btc'] or 0):>9.2f}% {(r['avg_strength'] or 0):>7.1f} "
                    f"{rep:>9} {nrep:>9}"
                )
            print("-" * 75)
            print(f"{'TOTAL':<10} {'':<10} {total:>6}\n")

        # Top signals by strength
        cur = await db.execute(
            f"""SELECT detected_at, direction, market_ticker, market_question,
                       btc_pct_change, market_pp_change, signal_strength,
                       market_url, market_repriced, revert_seconds
                FROM lag_signals {where}
                ORDER BY signal_strength DESC LIMIT ?""",
            params + (args.top,),
        )
        top = list(await cur.fetchall())
        if top:
            print(f"Top {len(top)} signals by strength:\n")
            for r in top:
                outcome = ""
                if r["market_repriced"] == 1:
                    outcome = f" [REPRICED in {r['revert_seconds']}s]"
                elif r["market_repriced"] == 0:
                    outcome = " [did NOT reprice]"
                print(
                    f"  {r['detected_at'][:19]}  {r['direction']:<8} "
                    f"BTC {r['btc_pct_change']:+.2f}%  "
                    f"market {r['market_pp_change']:+.2f}pp  "
                    f"str={r['signal_strength']:.1f}{outcome}"
                )
                print(f"    {(r['market_question'] or '')[:80]}")
                print(f"    {r['market_url']}")
            print()

        # Hit rate among observed signals
        cur = await db.execute(
            f"""SELECT direction,
                       SUM(CASE WHEN market_repriced=1 THEN 1 ELSE 0 END) hits,
                       COUNT(*) total,
                       AVG(revert_seconds) avg_revert_s
                FROM lag_signals
                {('WHERE detected_at >= ? AND' if args.recent else 'WHERE')}
                  status='observed'
                GROUP BY direction""",
            params,
        )
        obs = list(await cur.fetchall())
        if obs:
            print("Hit rate (observed signals only — observation requires next cycle to fire):")
            for r in obs:
                hits = r["hits"] or 0
                total = r["total"]
                avg_revert = r["avg_revert_s"] or 0
                rate = 100 * hits / total if total else 0
                print(
                    f"  {r['direction']:<8} {hits}/{total} ({rate:.1f}%) "
                    f"  avg revert: {avg_revert:.0f}s"
                )
            print()


if __name__ == "__main__":
    asyncio.run(main())
