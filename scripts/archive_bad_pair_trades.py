"""Archive paper_trades by id list — for structurally-broken pairs.

Use case: after a matcher bug is fixed, clear specific in-flight trades that
the bot opened on the bad logic. Sets status='archived' so the allocator
frees their capital and the monitor stops marking them to market.

Targeted variant of archive_legacy_paper_trades.py — takes an explicit id
list rather than archiving all opens (useful when a subset of opens are
genuine and you only want to clear the broken ones).

Usage:
    # Dry-run
    python -m scripts.archive_bad_pair_trades --ids 395,396,397
    # Execute
    python -m scripts.archive_bad_pair_trades --ids 395,396,397 --execute

Only operates on rows currently in 'open' status — closed/archived trades are
skipped to avoid disturbing the SQLite trigger that enforces realized P&L
invariants on closed rows.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

import aiosqlite

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.config import load_config


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--ids", required=True,
        help="Comma-separated trade ids to archive (e.g. 395,396,397)",
    )
    ap.add_argument(
        "--execute", action="store_true",
        help="Actually perform the archive. Default is dry-run.",
    )
    ap.add_argument(
        "--reason", default="bad pair (neg-risk sub-outcome) — matcher fix 2026-05-02",
        help="Note attached to the archive operation (logged, not stored)",
    )
    args = ap.parse_args()

    try:
        ids = [int(x.strip()) for x in args.ids.split(",") if x.strip()]
    except ValueError:
        print(f"--ids must be comma-separated integers, got: {args.ids}", file=sys.stderr)
        sys.exit(2)

    if not ids:
        print("No ids provided. Nothing to do.")
        return

    cfg = load_config("config.yaml")
    db_path = cfg["database"]["path"]

    placeholders = ",".join("?" for _ in ids)
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            f"SELECT id, status, pair_id, yes_platform, yes_ticker, no_platform, no_ticker "
            f"FROM paper_trades WHERE id IN ({placeholders})",
            ids,
        )
        rows = list(await cur.fetchall())

        found_ids = {r["id"] for r in rows}
        missing = [i for i in ids if i not in found_ids]
        if missing:
            print(f"WARN: ids not found in paper_trades: {missing}")

        open_rows = [r for r in rows if r["status"] == "open"]
        skipped = [r for r in rows if r["status"] != "open"]

        print(f"\n{'WOULD ARCHIVE' if not args.execute else 'ARCHIVING'} "
              f"{len(open_rows)} open trades (skipping {len(skipped)} non-open)")
        print(f"  Reason: {args.reason}")
        print()
        for r in open_rows:
            print(f"  id={r['id']:>4}  status=open  "
                  f"pair={r['pair_id']}  "
                  f"{r['yes_platform']}:{r['yes_ticker']} ↔ "
                  f"{r['no_platform']}:{r['no_ticker']}")
        if skipped:
            print()
            print("  Skipped (non-open):")
            for r in skipped:
                print(f"    id={r['id']:>4}  status={r['status']}")

        if not open_rows:
            print("\nNo open trades in id list — nothing to do.")
            return

        if not args.execute:
            print("\nDry-run only. Re-run with --execute to actually archive.")
            return

        await db.execute(
            f"UPDATE paper_trades SET status='archived' "
            f"WHERE id IN ({placeholders}) AND status='open'",
            ids,
        )
        await db.commit()
        print(f"\nDone. {len(open_rows)} trades moved 'open' -> 'archived'.")


if __name__ == "__main__":
    asyncio.run(main())
