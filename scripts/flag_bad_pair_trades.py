"""Add pair_quality column to paper_trades and flag known-bad rows.

Defense in depth for backtest integrity: trades opened on a structurally
broken pair (e.g. neg-risk sub-outcome × Kalshi binary) have realized PnL
that doesn't reflect a real arb opportunity. Marking them lets analytics
filter `pair_quality = 'good'` without touching status (which would risk
the SQLite invariant trigger on closed rows).

Schema added (idempotent):
    paper_trades.pair_quality TEXT DEFAULT 'unknown'

Values used:
    good                      — passes current matcher checks
    broken_neg_risk_sub       — Polymarket negRisk=true + groupItemTitle
                                paired with Kalshi YES/NO binary

This script (a) adds the column if missing, (b) defaults all rows to
'good', then (c) flags the explicit list of known-bad ids passed via
--ids. Re-run safely.

Usage:
    python -m scripts.flag_bad_pair_trades --ids 1,198,205,296,309,321,332,344,362,367,382,395,396,397
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

import aiosqlite

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.config import load_config


async def column_exists(db: aiosqlite.Connection, table: str, col: str) -> bool:
    cur = await db.execute(f"PRAGMA table_info({table})")
    rows = await cur.fetchall()
    return any(r[1] == col for r in rows)


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ids", required=True,
                    help="Comma-separated trade ids to flag as broken_neg_risk_sub")
    ap.add_argument("--quality", default="broken_neg_risk_sub",
                    help="Quality label to apply (default: broken_neg_risk_sub)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Show what would change without writing")
    args = ap.parse_args()

    try:
        ids = [int(x.strip()) for x in args.ids.split(",") if x.strip()]
    except ValueError:
        print(f"--ids must be comma-separated ints, got: {args.ids}", file=sys.stderr)
        sys.exit(2)

    cfg = load_config("config.yaml")
    db_path = cfg["database"]["path"]

    async with aiosqlite.connect(db_path) as db:
        # 1. Add column if missing
        has_col = await column_exists(db, "paper_trades", "pair_quality")
        if not has_col:
            print("Adding paper_trades.pair_quality TEXT DEFAULT 'good'")
            if not args.dry_run:
                await db.execute(
                    "ALTER TABLE paper_trades ADD COLUMN pair_quality TEXT DEFAULT 'good'"
                )
                await db.commit()
        else:
            print("paper_trades.pair_quality already exists")

        # 2. Backfill any nulls to 'good' (column default only applies to new rows)
        if not args.dry_run:
            await db.execute(
                "UPDATE paper_trades SET pair_quality='good' WHERE pair_quality IS NULL"
            )
            await db.commit()

        # 3. Flag the known-bad rows
        placeholders = ",".join("?" for _ in ids)
        cur = await db.execute(
            f"SELECT id, status, pair_quality FROM paper_trades WHERE id IN ({placeholders})",
            ids,
        )
        rows = list(await cur.fetchall())
        found = {r[0] for r in rows}
        missing = [i for i in ids if i not in found]
        if missing:
            print(f"WARN: ids not in paper_trades: {missing}")

        print(f"\n{'WOULD FLAG' if args.dry_run else 'FLAGGING'} {len(rows)} trades as pair_quality='{args.quality}'")
        for r in rows:
            print(f"  id={r[0]:>4}  status={r[1]:<14}  pair_quality (current) = {r[2]}")

        if args.dry_run:
            print("\nDry-run only.")
            return

        await db.execute(
            f"UPDATE paper_trades SET pair_quality=? WHERE id IN ({placeholders})",
            [args.quality, *ids],
        )
        await db.commit()
        print(f"\nDone. {len(rows)} rows flagged.")

        # Final summary
        cur = await db.execute(
            "SELECT pair_quality, COUNT(*) FROM paper_trades GROUP BY pair_quality"
        )
        print("\npair_quality distribution:")
        for q, n in await cur.fetchall():
            print(f"  {q:<25}  {n}")


if __name__ == "__main__":
    asyncio.run(main())
