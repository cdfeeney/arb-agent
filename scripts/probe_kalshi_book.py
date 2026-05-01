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

    # Hit the raw HTTP endpoint ourselves so we can see what Kalshi actually
    # returns — fetch_orderbook silently swallows non-200 and shape mismatches.
    import httpx, time
    path = f"/trade-api/v2/markets/{ticker}/orderbook"
    url = f"{kalshi.BASE_URL}/markets/{ticker}/orderbook"
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(url, headers=kalshi._auth_headers("GET", path))
    print(f"    raw HTTP {resp.status_code}")
    if resp.status_code != 200:
        print(f"    body (first 500 chars): {resp.text[:500]}")
        return
    raw = resp.json()
    print(f"    response keys: {sorted(raw.keys())}")
    # Kalshi appears to have renamed `orderbook` → `orderbook_fp` (same
    # pattern as `volume` → `volume_fp` on the markets endpoint). Probe both.
    for ob_key in ("orderbook", "orderbook_fp"):
        ob = raw.get(ob_key)
        if ob is None:
            continue
        print(f"    {ob_key} keys: {sorted(ob.keys()) if isinstance(ob, dict) else type(ob).__name__}")
        if isinstance(ob, dict):
            for k in sorted(ob.keys()):
                v = ob[k]
                if isinstance(v, list):
                    print(f"      {ob_key}[{k!r}]: {len(v)} entries", end="")
                    if v:
                        print(f"  sample first: {v[0]}")
                    else:
                        print()
                else:
                    print(f"      {ob_key}[{k!r}]: {v!r}")

    # Now run through our parser
    book = await kalshi.fetch_orderbook(ticker)
    if not book:
        print("    parser returned None")
        return
    yes_bids = book.get("yes_bids", [])
    no_bids = book.get("no_bids", [])
    print(f"    parser sees: yes_bids={len(yes_bids)}  no_bids={len(no_bids)}")
    if yes_bids:
        print(f"      top YES bid: price={yes_bids[0][0]:.4f}  size={yes_bids[0][1]}")
    if no_bids:
        print(f"      top NO  bid: price={no_bids[0][0]:.4f}  size={no_bids[0][1]}")
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

    # config.py overlays env-var values into cfg["kalshi"]["api_key_id"] /
    # cfg["kalshi"]["private_key_path"] when KALSHI_API_KEY_ID and
    # KALSHI_PRIVATE_KEY_PATH are set. Read directly from cfg.
    kalshi = KalshiClient(
        api_key_id=cfg["kalshi"]["api_key_id"],
        private_key_path=cfg["kalshi"]["private_key_path"],
    )

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
