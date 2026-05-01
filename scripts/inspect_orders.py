"""Inspect the orders table — what the executor wrote out this cycle/day.

Each row is one leg of an arb entry. correlation_id ties two legs together.
idempotency_key is the safety net against double-submission.

Usage:
    py -3 -m scripts.inspect_orders                  # latest 20 orders
    py -3 -m scripts.inspect_orders --since 1h       # last hour
    py -3 -m scripts.inspect_orders --paper 364      # all orders for paper_trade #364
    py -3 -m scripts.inspect_orders --corr abc123    # both legs of one correlation
"""

import argparse
import asyncio
import sys
from pathlib import Path

import aiosqlite

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.config import load_config


def parse_since(s: str) -> str:
    """'1h', '30m', '2d' → '-1 hours' / '-30 minutes' etc. for sqlite datetime()."""
    s = s.strip().lower()
    if not s:
        return "-1 hours"
    n = "".join(c for c in s if c.isdigit() or c == ".") or "1"
    unit = "".join(c for c in s if not (c.isdigit() or c == "."))
    mapping = {"m": "minutes", "h": "hours", "d": "days"}
    return f"-{n} {mapping.get(unit, 'hours')}"


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default=None, help="e.g. 1h, 30m, 2d")
    ap.add_argument("--paper", type=int, default=None)
    ap.add_argument("--corr", type=str, default=None)
    ap.add_argument("--limit", type=int, default=20)
    args = ap.parse_args()

    cfg = load_config("config.yaml")
    db_path = cfg["database"]["path"]

    where, params = [], []
    if args.since:
        where.append(f"created_at > datetime('now', ?)")
        params.append(parse_since(args.since))
    if args.paper is not None:
        where.append("paper_trade_id=?")
        params.append(args.paper)
    if args.corr:
        where.append("correlation_id=?")
        params.append(args.corr)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            f"""SELECT id, correlation_id, paper_trade_id, leg, platform,
                       substr(ticker,1,30) AS ticker,
                       side, order_type, price_limit, contracts_intended,
                       contracts_filled, avg_fill_price, status,
                       execution_mode, idempotency_key,
                       substr(created_at, 12, 8) AS t_created,
                       error
                FROM orders {where_sql}
                ORDER BY id DESC LIMIT ?""",
            (*params, args.limit),
        )
        rows = list(await cur.fetchall())

    if not rows:
        print("(no orders)")
        return

    print(f"{'id':>5} {'t':>9} {'mode':>9} {'corr':>13} {'paper':>6} "
          f"{'plat':>10} {'leg':>3} {'side':>9} {'price':>7} {'ord':>6} "
          f"{'fill':>6} {'fillP':>7} {'status':>10}  ticker")
    for r in rows:
        print(
            f"{r['id']:>5} {r['t_created']:>9} {r['execution_mode']:>9} "
            f"{(r['correlation_id'] or '')[:13]:>13} "
            f"{(r['paper_trade_id'] or '-'):>6} "
            f"{r['platform']:>10} {r['leg']:>3} {r['side']:>9} "
            f"{r['price_limit']:>7.4f} {r['contracts_intended']:>6.2f} "
            f"{r['contracts_filled']:>6.2f} "
            f"{(r['avg_fill_price'] or 0):>7.4f} {r['status']:>10}  "
            f"{r['ticker']}"
        )

    # Summary by correlation_id — every pair should have exactly 2 legs
    print()
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            f"""SELECT correlation_id, COUNT(*) AS n_legs,
                       SUM(CASE WHEN status='filled' THEN 1 ELSE 0 END) AS n_filled,
                       SUM(contracts_intended * price_limit) AS planned_cost
                FROM orders {where_sql}
                GROUP BY correlation_id
                ORDER BY MAX(id) DESC LIMIT ?""",
            (*params, args.limit),
        )
        groups = list(await cur.fetchall())
    if groups:
        print("By correlation_id (atomicity check — every entry should have 2 legs):")
        for g in groups:
            tag = "OK" if g["n_legs"] == 2 and g["n_filled"] == 2 else "INCOMPLETE"
            print(f"  {g['correlation_id']:<14} {g['n_legs']} legs, "
                  f"{g['n_filled']} filled, planned ${g['planned_cost']:.2f}  [{tag}]")


if __name__ == "__main__":
    asyncio.run(main())
