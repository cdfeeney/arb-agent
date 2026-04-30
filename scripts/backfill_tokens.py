"""Backfill Polymarket clob token ids onto pre-migration paper_trades rows.

The token-capture migration added yes_token/no_token columns but only new
captures get them populated. Older rows have NULL tokens, which means the
position monitor can't fetch their bid books and reports them as
"book unavailable". This script looks up each missing token via gamma's
single-market endpoint (id == our stored ticker) and UPDATEs the row.

Idempotent: safe to re-run; only touches rows where the relevant token is
still NULL.

Run on the droplet from project root:
    python3 -m scripts.backfill_tokens
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import aiosqlite
import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.config import load_config

GAMMA_MARKET_URL = "https://gamma-api.polymarket.com/markets/{id}"


async def fetch_tokens(
    client: httpx.AsyncClient, market_id: str,
) -> tuple[str | None, str | None]:
    """Fetch a single Polymarket market by id, return (yes_token, no_token).

    Returns (None, None) if the market 404s or has no clobTokenIds.
    """
    try:
        resp = await client.get(GAMMA_MARKET_URL.format(id=market_id), timeout=15)
    except Exception as e:
        print(f"  fetch error for {market_id}: {e}")
        return None, None
    if resp.status_code == 404:
        return None, None
    if resp.status_code != 200:
        print(f"  HTTP {resp.status_code} for {market_id}")
        return None, None

    raw = resp.json()
    tokens = raw.get("clobTokenIds")
    if isinstance(tokens, str):
        try:
            tokens = json.loads(tokens)
        except Exception:
            tokens = None
    if not tokens or len(tokens) < 2:
        return None, None
    return str(tokens[0]), str(tokens[1])


async def main() -> None:
    cfg = load_config("config.yaml")
    db_path = cfg["database"]["path"]

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row

        cur = await db.execute(
            """SELECT id, yes_platform, yes_ticker, yes_token,
                          no_platform,  no_ticker,  no_token
               FROM paper_trades
               WHERE (yes_platform = 'polymarket' AND yes_token IS NULL)
                  OR (no_platform  = 'polymarket' AND no_token  IS NULL)
               ORDER BY id""",
        )
        rows = list(await cur.fetchall())

        if not rows:
            print("No rows need backfill — all Polymarket legs already have tokens.")
            return

        unique_market_ids: set[str] = set()
        for r in rows:
            if r["yes_platform"] == "polymarket" and r["yes_token"] is None:
                unique_market_ids.add(r["yes_ticker"])
            if r["no_platform"] == "polymarket" and r["no_token"] is None:
                unique_market_ids.add(r["no_ticker"])
        unique_market_ids.discard(None)
        unique_market_ids.discard("")

        print(
            f"\n=== Token Backfill ===\n"
            f"Rows needing backfill: {len(rows)}\n"
            f"Unique Polymarket markets to look up: {len(unique_market_ids)}\n"
        )

        token_cache: dict[str, tuple[str | None, str | None]] = {}
        async with httpx.AsyncClient() as client:
            for i, mid in enumerate(sorted(unique_market_ids), 1):
                yes_tok, no_tok = await fetch_tokens(client, mid)
                token_cache[mid] = (yes_tok, no_tok)
                status = "OK " if yes_tok and no_tok else "MISS"
                if i % 10 == 0 or i == len(unique_market_ids):
                    print(f"  [{i}/{len(unique_market_ids)}] {status} id={mid}")

        n_hit = sum(1 for v in token_cache.values() if v[0] and v[1])
        n_miss = len(token_cache) - n_hit
        print(f"\nLookup result: {n_hit} found, {n_miss} missing (likely closed/delisted)\n")

        updates_yes = 0
        updates_no = 0
        for r in rows:
            new_yes = r["yes_token"]
            new_no = r["no_token"]
            if r["yes_platform"] == "polymarket" and r["yes_token"] is None:
                yes_tok, _no_tok_for_yes_market = token_cache.get(
                    r["yes_ticker"], (None, None),
                )
                if yes_tok:
                    new_yes = yes_tok
            if r["no_platform"] == "polymarket" and r["no_token"] is None:
                _yes_tok_for_no_market, no_tok = token_cache.get(
                    r["no_ticker"], (None, None),
                )
                if no_tok:
                    new_no = no_tok

            if new_yes != r["yes_token"]:
                await db.execute(
                    "UPDATE paper_trades SET yes_token = ? WHERE id = ?",
                    (new_yes, r["id"]),
                )
                updates_yes += 1
            if new_no != r["no_token"]:
                await db.execute(
                    "UPDATE paper_trades SET no_token = ? WHERE id = ?",
                    (new_no, r["id"]),
                )
                updates_no += 1

        await db.commit()
        print(
            f"Updated yes_token on {updates_yes} rows, "
            f"no_token on {updates_no} rows.\n"
            f"Position monitor will price these on its next cycle.",
        )


if __name__ == "__main__":
    asyncio.run(main())
