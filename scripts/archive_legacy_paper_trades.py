"""One-shot cleanup: archive all currently-open paper trades.

Use case: after a strategy pivot, mark all in-flight trades as 'archived'
so the allocator stops counting their capital as deployed. The trades
remain in the DB for historical analysis but won't be marked-to-market,
won't trigger exits, and won't appear in health_check's "open positions".

Status values:
  open       — actively managed (allocator counts capital, monitor marks-to-market)
  closed     — fully unwound, realized_profit_usd populated (trigger-enforced)
  exited     — paper-mode exit, realized_profit_usd populated (trigger-enforced)
  resolved   — market resolved, payout collected
  archived   — strategy-pivot reset; ignored by allocator + monitor

Usage:
    python -m scripts.archive_legacy_paper_trades             # dry-run, prints what WOULD be archived
    python -m scripts.archive_legacy_paper_trades --execute   # actually archive

Idempotent: re-running on an empty open set is a no-op.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

import aiosqlite

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.config import load_config


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--execute", action="store_true",
        help="Actually perform the archive. Default is dry-run.",
    )
    ap.add_argument(
        "--reason", default="strategy pivot 2026-05-02 — sub-week velocity",
        help="Note attached to the archive operation (logged, not stored)",
    )
    args = ap.parse_args()

    cfg = load_config("config.yaml")
    db_path = cfg["database"]["path"]

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT id, pair_id, yes_size_usd, no_size_usd, "
            "       yes_observed_price, no_observed_price, "
            "       contracts_remaining, edge_gross_pct, predicted_net_usd, "
            "       detected_at, closes_at "
            "FROM paper_trades WHERE status='open' "
            "ORDER BY detected_at DESC"
        )
        rows = list(await cur.fetchall())

        if not rows:
            print("No open paper trades. Nothing to archive.")
            return

        total_deployed = 0.0
        total_predicted = 0.0
        for r in rows:
            cpc = (r["yes_observed_price"] or 0) + (r["no_observed_price"] or 0)
            remaining = r["contracts_remaining"]
            if remaining is None:
                remaining = (r["yes_size_usd"] or 0) / max(r["yes_observed_price"] or 1, 0.0001)
            total_deployed += float(remaining) * float(cpc)
            total_predicted += float(r["predicted_net_usd"] or 0)

        print(f"\n{'WOULD ARCHIVE' if not args.execute else 'ARCHIVING'} "
              f"{len(rows)} open paper trades")
        print(f"  Reason: {args.reason}")
        print(f"  Total deployed capital to free: ${total_deployed:.2f}")
        print(f"  Total predicted profit forfeited: ${total_predicted:.2f}")
        print(f"  (No realized P&L computed — these trades are paused, not closed.")
        print(f"   They retain all data for historical analysis.)")
        print()
        print(f"  {'#':<5} {'edge':>6} {'pred':>7} {'closes':>12} {'pair'}")
        for r in rows[:15]:
            edge = (r["edge_gross_pct"] or 0) * 100
            pred = r["predicted_net_usd"] or 0
            closes = (r["closes_at"] or "")[:10]
            pair = (r["pair_id"] or "")[:50]
            print(f"  {r['id']:<5} {edge:>5.1f}% ${pred:>5.2f} {closes:>12} {pair}")
        if len(rows) > 15:
            print(f"  ... and {len(rows) - 15} more")

        if not args.execute:
            print()
            print("Dry-run only. Re-run with --execute to actually archive.")
            return

        # Confirm before destructive action
        print()
        print(f"Archiving {len(rows)} trades...")
        await db.execute(
            "UPDATE paper_trades SET status='archived' WHERE status='open'"
        )
        await db.commit()
        print(f"Done. {len(rows)} trades moved 'open' -> 'archived'.")
        print(f"Allocator will now see ${total_deployed:.2f} of capital as free.")
        print(f"Restart the bot to pick up the freed capacity:")
        print(f"  screen -r arb-agent")
        print(f"  # Ctrl-C, then: python3 main.py 2>&1 | tee -a data/agent.log")
        print(f"  # Ctrl-A, D to detach")


if __name__ == "__main__":
    asyncio.run(main())
