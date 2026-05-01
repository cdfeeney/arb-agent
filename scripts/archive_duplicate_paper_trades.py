"""Archive duplicate open paper_trades — keep newest per pair_id.

Pre-fix, when the dedup window expired every 60 min, the SAME arb got
saved as a fresh paper_trade row each cycle. Result: 193 status='open'
rows but only 24 distinct pair_ids. Each duplicate counts in the allocator's
deployed-capital calculation, even though they're the same arb. Without
archiving, free_capital = bankroll - sum(193 × bet_size) is negative,
and the allocator will never permit new entries.

This script keeps the NEWEST row per pair_id (freshest sizing/prices) and
sets older duplicates to status='paper_archived'. Archived rows stay in
the DB for analysis; they just don't count toward deployed capital.

Run from project root:
    py -3 -m scripts.archive_duplicate_paper_trades --dry-run     # preview
    py -3 -m scripts.archive_duplicate_paper_trades               # apply
"""
import argparse
import asyncio
import sys
from pathlib import Path

import aiosqlite

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.config import load_config


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--dry-run", action="store_true",
        help="Print plan without modifying the database",
    )
    args = ap.parse_args()

    cfg = load_config("config.yaml")
    db_path = cfg["database"]["path"]

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("""
            WITH latest AS (
                SELECT pair_id, MAX(detected_at) AS latest_at
                FROM paper_trades
                WHERE status='open'
                GROUP BY pair_id
            )
            SELECT pt.id, pt.pair_id, pt.detected_at, pt.predicted_net_usd
            FROM paper_trades pt
            JOIN latest l ON pt.pair_id = l.pair_id
            WHERE pt.status='open' AND pt.detected_at < l.latest_at
            ORDER BY pt.pair_id, pt.detected_at
        """)
        to_archive = list(await cur.fetchall())

        cur = await db.execute(
            "SELECT COUNT(*), COUNT(DISTINCT pair_id) FROM paper_trades WHERE status='open'"
        )
        before = await cur.fetchone()

    print(f"Before: {before[0]} open paper trades / {before[1]} distinct pair_ids")
    print(f"Plan:   archive {len(to_archive)} duplicate rows, keep newest per pair_id")
    print(f"After:  {before[0] - len(to_archive)} open / {before[1]} distinct (1 per pair)\n")

    if args.dry_run:
        print("--dry-run: no changes made. Sample rows that would be archived:\n")
        for r in to_archive[:20]:
            pred = r["predicted_net_usd"] or 0
            print(f"  archive #{r['id']:5d}  {r['detected_at']}  "
                  f"${pred:>6.2f}  {r['pair_id'][:60]}")
        if len(to_archive) > 20:
            print(f"  ... and {len(to_archive) - 20} more")
        return

    if not to_archive:
        print("Nothing to archive — every pair_id has exactly one open row.")
        return

    ids = tuple(r["id"] for r in to_archive)
    placeholders = ",".join(["?"] * len(ids))
    async with aiosqlite.connect(db_path) as db:
        # 'paper_archived' is a new status that does not trip the
        # enforce_closed_has_realized / enforce_exited_has_realized triggers
        # (those only ABORT on status IN ('closed','exited') with NULL realized).
        await db.execute(
            f"UPDATE paper_trades SET status='paper_archived' WHERE id IN ({placeholders})",
            ids,
        )
        await db.commit()

    print(f"Archived {len(ids)} duplicate paper_trade rows.")
    print("Run scripts.paper_summary to see the new state.")


if __name__ == "__main__":
    asyncio.run(main())
