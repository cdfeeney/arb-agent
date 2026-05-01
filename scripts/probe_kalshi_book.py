"""Hit the Kalshi /orderbook endpoint directly for a specific paper trade and
compare against what the bot recorded.

Counterpart to scripts/probe_clob_book.py — the CLOB probe verified
Polymarket bid books are healthy. This probe verifies the Kalshi side,
where our diagnostic data shows most "empty book" cases are.

Run:
    py -3 -m scripts.probe_kalshi_book --trade 364
    py -3 -m scripts.probe_kalshi_book --search KXIPOOPENAI

Prints raw `yes` and `no` arrays from Kalshi's orderbook response so we
can see whether the API is genuinely returning empty bid books, or whether
our code is misinterpreting the response shape.
"""
import argparse
import asyncio
import os
import sys
from pathlib import Path

import aiosqlite
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.clients.kalshi import KalshiClient
from src.config import load_config


async def probe(kalshi: KalshiClient, ticker: str, our_recorded_bid: float | None):
    print(f"  ticker={ticker}")
    book = await kalshi.fetch_orderbook(ticker)
    if not book:
        print("    fetch_orderbook returned None (HTTP non-200 or exception)")
        return
    yes_bids = book.get("yes_bids", [])
    yes_asks = book.get("yes_asks", [])
    no_bids = book.get("no_bids", [])
    no_asks = book.get("no_asks", [])
    print(f"    yes_bids={len(yes_bids)}  yes_asks={len(yes_asks)}  "
          f"no_bids={len(no_bids)}  no_asks={len(no_asks)}")
    if yes_bids:
        print("    top 3 YES bids:")
        for p, s in yes_bids[:3]:
            print(f"      price={p:.4f}  size={s}")
    if no_bids:
        print("    top 3 NO bids:")
        for p, s in no_bids[:3]:
            print(f"      price={p:.4f}  size={s}")
    if our_recorded_bid is not None:
        print(f"    bot recorded: {our_recorded_bid:.4f}")


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trade", type=int)
    ap.add_argument("--search", type=str)
    ap.add_argument("--limit", type=int, default=5)
    args = ap.parse_args()

    load_dotenv()
    cfg = load_config("config.yaml")
    db_path = cfg["database"]["path"]

    api_key_id = os.environ.get(cfg["kalshi"]["api_key_env"], "")
    private_key_path = cfg["kalshi"]["private_key_path"]
    kalshi = KalshiClient(api_key_id=api_key_id, private_key_path=private_key_path)

    where = "WHERE pt.status='open'"
    params: list = []
    if args.trade is not None:
        where += " AND pt.id = ?"
        params.append(args.trade)
    elif args.search:
        where += " AND (pt.yes_ticker LIKE ? OR pt.no_ticker LIKE ?)"
        params.extend([f"%{args.search}%", f"%{args.search}%"])

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            f"""SELECT pt.id, pt.pair_id,
                       pt.yes_platform, pt.yes_ticker,
                       pt.no_platform, pt.no_ticker,
                       m.yes_bid_now, m.no_bid_now
                FROM paper_trades pt
                LEFT JOIN paper_trade_marks m ON m.id = (
                    SELECT id FROM paper_trade_marks
                    WHERE paper_trade_id = pt.id
                    ORDER BY observed_at DESC LIMIT 1
                )
                {where}
                ORDER BY pt.detected_at DESC LIMIT ?""",
            (*params, args.limit),
        )
        rows = list(await cur.fetchall())

    if not rows:
        print("No matching trades.")
        return

    for r in rows:
        print(f"\n=== trade #{r['id']}  {r['pair_id']} ===")
        if r["yes_platform"] == "kalshi":
            await probe(kalshi, r["yes_ticker"], r["yes_bid_now"])
        else:
            print(f"  YES leg is {r['yes_platform']} — not Kalshi")
        if r["no_platform"] == "kalshi":
            await probe(kalshi, r["no_ticker"], r["no_bid_now"])
        else:
            print(f"  NO  leg is {r['no_platform']} — not Kalshi")


if __name__ == "__main__":
    asyncio.run(main())
