"""Hit the Polymarket CLOB book endpoint directly for a specific paper trade
and compare what comes back against what the bot has been recording.

Run from project root:
    py -3 -m scripts.probe_clob_book                  # all open Polymarket legs
    py -3 -m scripts.probe_clob_book --trade 364      # one specific trade id
    py -3 -m scripts.probe_clob_book --search bitcoin # match on pair_id

For each Polymarket leg of an open trade we:
  1. Print the saved token_id (truncated for readability)
  2. Fetch the live CLOB /book endpoint with that token
  3. Print top 3 bids and top 3 asks from the raw response
  4. Print what our best_bid_from_book extracts
  5. Print the bot's most recent recorded yes_bid_now / no_bid_now

If the website shows bids but the raw CLOB response is empty, the API is
giving us stale/bad data (or we have a wrong token). If the raw response
HAS bids but our parser returns 0, the parser is broken. If raw is empty
AND the website shows bids, the token is wrong.
"""
import argparse
import asyncio
import json
import sys
from pathlib import Path

import aiosqlite
import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.clients.polymarket import PolymarketClient
from src.config import load_config


async def probe(token_id: str, label: str):
    if not token_id:
        print(f"  {label}: no token saved (NULL/empty)")
        return
    print(f"  {label}: token={token_id[:24]}...")
    async with httpx.AsyncClient(timeout=15) as c:
        resp = await c.get(
            "https://clob.polymarket.com/book",
            params={"token_id": token_id},
        )
    print(f"    HTTP {resp.status_code}")
    if resp.status_code != 200:
        print(f"    body: {resp.text[:400]}")
        return
    data = resp.json()
    bids = data.get("bids") or []
    asks = data.get("asks") or []
    print(f"    raw response: {len(bids)} bids, {len(asks)} asks")
    if bids:
        bids_sorted = sorted(bids, key=lambda x: -float(x.get("price", 0)))
        print("    top 3 bids:")
        for b in bids_sorted[:3]:
            print(f"      price={b.get('price')}  size={b.get('size')}")
    if asks:
        asks_sorted = sorted(asks, key=lambda x: float(x.get("price", 1)))
        print("    top 3 asks:")
        for a in asks_sorted[:3]:
            print(f"      price={a.get('price')}  size={a.get('size')}")

    # Run through our actual parser
    parsed = {"asks": sorted(asks, key=lambda x: float(x.get("price", 1))),
              "bids": sorted(bids, key=lambda x: -float(x.get("price", 0)))}
    bb_price, bb_size = PolymarketClient.best_bid_from_book(parsed)
    ba_price, ba_size = PolymarketClient.best_ask_from_book(parsed)
    print(f"    parser sees: best_bid=({bb_price}, {bb_size})  best_ask=({ba_price}, {ba_size})")


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trade", type=int, help="Probe one specific trade id")
    ap.add_argument("--search", type=str, help="Substring match on pair_id")
    ap.add_argument("--limit", type=int, default=5, help="Max trades to probe")
    args = ap.parse_args()

    cfg = load_config("config.yaml")
    db_path = cfg["database"]["path"]

    where = "WHERE pt.status='open'"
    params: list = []
    if args.trade is not None:
        where += " AND pt.id = ?"
        params.append(args.trade)
    elif args.search:
        where += " AND pt.pair_id LIKE ?"
        params.append(f"%{args.search}%")

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            f"""SELECT pt.id, pt.pair_id, pt.yes_platform, pt.yes_ticker, pt.yes_token,
                       pt.no_platform, pt.no_ticker, pt.no_token,
                       pt.yes_url, pt.no_url,
                       m.yes_bid_now, m.no_bid_now, m.observed_at
                FROM paper_trades pt
                LEFT JOIN paper_trade_marks m ON pt.id = m.paper_trade_id
                LEFT JOIN (SELECT paper_trade_id, MAX(observed_at) latest
                           FROM paper_trade_marks GROUP BY paper_trade_id) lm
                  ON m.paper_trade_id=lm.paper_trade_id AND m.observed_at=lm.latest
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
        print(f"  yes_url: {r['yes_url']}")
        print(f"  no_url:  {r['no_url']}")
        print(f"  bot's last recorded: yes_bid_now={r['yes_bid_now']}  "
              f"no_bid_now={r['no_bid_now']}  at {r['observed_at']}")

        if r["yes_platform"] == "polymarket":
            await probe(r["yes_token"] or "", "POLY YES leg")
        else:
            print(f"  YES leg is {r['yes_platform']} ({r['yes_ticker']}) — not a Polymarket book to probe")
        if r["no_platform"] == "polymarket":
            await probe(r["no_token"] or "", "POLY NO leg")
        else:
            print(f"  NO  leg is {r['no_platform']} ({r['no_ticker']}) — not a Polymarket book to probe")


if __name__ == "__main__":
    asyncio.run(main())
