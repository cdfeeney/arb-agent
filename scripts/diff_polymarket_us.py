"""Compare Polymarket US prices to international Polymarket prices.

This is THE gating question for the bot's paper-trade validity. Today the
bot reads prices from clob.polymarket.com (offshore). If the user trades
through the US-regulated app at api.polymarket.us, every "arb" we capture
is fictional unless those two books quote the same prices.

The script:
  1. Pulls top N markets by volume from international Polymarket Gamma.
  2. For each, fetches the order book from both:
       - clob.polymarket.com (per-token-id YES/NO books)
       - api.polymarket.us   (single book per slug)
  3. Reports best-bid / best-ask on each side, and the diff.

Run from project root:
    py -3 -m scripts.diff_polymarket_us
    py -3 -m scripts.diff_polymarket_us --slugs musk-trillionaire-2027,bitcoin-200k-2026
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.clients.polymarket import PolymarketClient
from src.clients.polymarket_us import PolymarketUSClient
from src.config import load_config


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--slugs", type=str, default="",
        help="Comma-separated list of market slugs to compare. "
             "If empty, picks top 5 by volume from Gamma.",
    )
    ap.add_argument(
        "--limit", type=int, default=5,
        help="When --slugs is empty, sample this many markets.",
    )
    args = ap.parse_args()

    cfg = load_config("config.yaml")
    poly_intl = PolymarketClient(rate_limit_per_min=120)
    poly_us = PolymarketUSClient(rate_limit_per_min=120)

    print(f"\n=== Polymarket US vs International Price Diff ===\n")
    print(
        f"US client authenticated: {poly_us.authenticated}  "
        f"(read-only diff still works without creds)\n"
    )

    slugs: list[str]
    intl_markets: dict[str, dict] = {}

    if args.slugs:
        slugs = [s.strip() for s in args.slugs.split(",") if s.strip()]
        # No quick reverse lookup from slug → market on Gamma; fetch by-slug
        # via filter param.
        for s in slugs:
            ms = await poly_intl.fetch_markets(
                limit=1, max_markets=1,
                max_days_to_close=400, min_volume=0,
            )
            for m in ms:
                if m.get("slug") == s:
                    intl_markets[s] = m
                    break
    else:
        candidates = await poly_intl.fetch_markets(
            limit=50, max_markets=50,
            max_days_to_close=400, min_volume=10000,
        )
        # Top N by volume
        candidates.sort(key=lambda m: float(m.get("volume", 0) or 0), reverse=True)
        for m in candidates[: args.limit]:
            slug = m.get("slug")
            if slug:
                intl_markets[slug] = m
        slugs = list(intl_markets.keys())

    if not slugs:
        print("No markets found.\n")
        return

    print(f"Comparing {len(slugs)} markets:\n")
    diffs: list[dict] = []
    for slug in slugs:
        intl = intl_markets.get(slug)
        if not intl:
            print(f"  {slug:50s} — not found on Gamma")
            continue

        # International: use Gamma bestBid/bestAsk + CLOB top-of-book if tokens.
        intl_yes_ask = float(intl.get("bestAsk", 0) or 0)
        intl_yes_bid = float(intl.get("bestBid", 0) or 0)

        # International CLOB if token ids present
        intl_clob_yes_bid = intl_clob_yes_ask = None
        clob_tokens = intl.get("clobTokenIds")
        if isinstance(clob_tokens, str):
            import json
            try:
                clob_tokens = json.loads(clob_tokens)
            except Exception:
                clob_tokens = None
        if clob_tokens and len(clob_tokens) >= 1:
            yes_book = await poly_intl.fetch_clob_book(clob_tokens[0])
            if yes_book:
                intl_clob_yes_ask, _ = poly_intl.best_ask_from_book(yes_book)
                intl_clob_yes_bid, _ = poly_intl.best_bid_from_book(yes_book)

        # US side
        us_book = await poly_us.fetch_market_book(slug)
        us_yes_bid = us_yes_ask = None
        if us_book:
            us_yes_bid, _ = PolymarketUSClient.best_bid_from_book(us_book)
            us_yes_ask, _ = PolymarketUSClient.best_ask_from_book(us_book)

        question = (intl.get("question") or "")[:70]
        print(f"  --- {slug} ---")
        print(f"      Q: {question}")
        print(f"      Intl (Gamma):     YES bid={intl_yes_bid:.4f}  ask={intl_yes_ask:.4f}")
        if intl_clob_yes_bid is not None:
            print(f"      Intl (CLOB live): YES bid={intl_clob_yes_bid:.4f}  ask={intl_clob_yes_ask:.4f}")
        if us_book is not None:
            print(f"      US   (api.us):    YES bid={us_yes_bid or 0:.4f}  ask={us_yes_ask or 0:.4f}")
            if intl_clob_yes_ask is not None and us_yes_ask:
                ask_diff = us_yes_ask - intl_clob_yes_ask
                print(f"      Δ ASK (US - intl-CLOB): {ask_diff:+.4f}")
                diffs.append({
                    "slug": slug,
                    "ask_diff": ask_diff,
                    "intl_ask": intl_clob_yes_ask,
                    "us_ask": us_yes_ask,
                })
        else:
            print(f"      US   (api.us):    BOOK NOT AVAILABLE (404 or empty)")
        print()

    if diffs:
        print("\n=== Summary ===")
        avg = sum(d["ask_diff"] for d in diffs) / len(diffs)
        max_abs = max(abs(d["ask_diff"]) for d in diffs)
        print(f"Markets compared:    {len(diffs)}")
        print(f"Mean ask diff:       {avg:+.4f}")
        print(f"Max |ask diff|:      {max_abs:.4f}")
        if max_abs <= 0.005:
            print("\nVERDICT: prices effectively identical (<= 0.5¢ max). "
                  "Current bot reads can stay on clob.polymarket.com — "
                  "only need US execution adapter for live trading.")
        elif max_abs <= 0.02:
            print("\nVERDICT: small drift (<= 2¢ max). Probably normal "
                  "latency/aggregation lag. Acceptable for paper-trade fidelity "
                  "but worth re-running periodically.")
        else:
            print("\nVERDICT: prices DIFFER MATERIALLY. Bot must read from "
                  "api.polymarket.us for paper data to reflect what the user "
                  "would actually pay. Need to swap Polymarket reads.")


if __name__ == "__main__":
    asyncio.run(main())
