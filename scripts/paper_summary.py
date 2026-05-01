"""Print a summary of paper-trade activity — predicted vs realized P&L.

Run from project root:
    py -3 -m scripts.paper_summary
    py -3 -m scripts.paper_summary --recent 24    # only last 24h
"""
import argparse
import asyncio
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import aiosqlite

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.config import load_config


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--recent", type=float, default=None, help="Show only trades from last N hours")
    args = ap.parse_args()

    cfg = load_config("config.yaml")
    db_path = cfg["database"]["path"]

    where = ""
    params: tuple = ()
    if args.recent is not None:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=args.recent)).isoformat()
        where = "WHERE detected_at >= ?"
        params = (cutoff,)

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row

        # Headline counts + totals
        cur = await db.execute(
            f"""SELECT status, COUNT(*) n,
                       SUM(predicted_net_usd) pred,
                       SUM(realized_profit_usd) real,
                       AVG(edge_gross_pct) avg_edge
                FROM paper_trades {where}
                GROUP BY status""",
            params,
        )
        rows = list(await cur.fetchall())

        print(f"\n=== Paper Trade Summary{' (last %sh)' % args.recent if args.recent else ''} ===\n")
        print(f"{'Status':<14} {'Count':>6} {'Avg Edge':>9} {'Predicted P&L':>15} {'Realized P&L':>15}")
        print("-" * 64)
        total_n = 0
        total_pred = 0.0
        total_real = 0.0
        legacy_n = 0
        legacy_pred = 0.0
        for r in rows:
            n = r["n"]; pred = r["pred"] or 0; real = r["real"]
            avg = r["avg_edge"] or 0
            real_str = f"${real:.2f}" if real is not None else "(unresolved)"
            print(f"{r['status']:<14} {n:>6} {avg*100:>8.2f}% ${pred:>13.2f} {real_str:>15}")
            if r["status"] == "legacy_broken":
                legacy_n += n
                legacy_pred += pred
                continue
            total_n += n
            total_pred += pred
            if real is not None: total_real += real
        print("-" * 64)
        print(f"{'TOTAL (live)':<14} {total_n:>6} {'':>9} ${total_pred:>13.2f} ${total_real:>13.2f}")
        if legacy_n:
            print(
                f"  + {legacy_n} legacy_broken trades (${legacy_pred:.2f} pred) "
                f"excluded — closed by stale code without realized P&L; "
                f"see triggers in store.py."
            )
        print()

        # Recently-closed unresolved (resolution lagging)
        cur = await db.execute(
            """SELECT pair_id, closes_at, predicted_net_usd
               FROM paper_trades
               WHERE status='open' AND closes_at < ?
               ORDER BY closes_at DESC LIMIT 10""",
            (datetime.now(timezone.utc).isoformat(),),
        )
        lag = list(await cur.fetchall())
        if lag:
            print("Closed but unresolved (resolver still pending):")
            for r in lag:
                print(f"  {r['closes_at'][:19]}  ${r['predicted_net_usd']:>7.2f}  {r['pair_id'][:60]}")
            print()

        # Top open opportunities by predicted profit
        cur = await db.execute(
            """SELECT pair_id, edge_gross_pct, predicted_net_usd, yes_url, no_url
               FROM paper_trades WHERE status='open'
               ORDER BY predicted_net_usd DESC LIMIT 10""",
        )
        top = list(await cur.fetchall())
        if top:
            print("Top open opportunities by predicted net profit:")
            for r in top:
                print(f"  {r['edge_gross_pct']*100:>5.2f}% gross  ${r['predicted_net_usd']:>7.2f} predicted")
                print(f"    YES: {r['yes_url']}")
                print(f"    NO:  {r['no_url']}")
            print()

        # Predicted-vs-realized accuracy (resolved only)
        cur = await db.execute(
            """SELECT predicted_net_usd, realized_profit_usd
               FROM paper_trades WHERE status='resolved'""",
        )
        pairs = list(await cur.fetchall())
        if pairs:
            errs = [r["realized_profit_usd"] - r["predicted_net_usd"] for r in pairs]
            mae = sum(abs(e) for e in errs) / len(errs)
            wins = sum(1 for r in pairs if r["realized_profit_usd"] > 0)
            print(f"Resolved trades: {len(pairs)}  |  win rate: {wins}/{len(pairs)} ({100*wins/len(pairs):.1f}%)")
            print(f"Mean abs prediction error: ${mae:.2f}")


if __name__ == "__main__":
    asyncio.run(main())
