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
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# Load .env so POLYMARKET_US_KEY_ID / POLYMARKET_US_SECRET_KEY are available
# when running this script standalone (the bot's main.py loads it via systemd
# already, but `python -m scripts.X` doesn't go through main).
from dotenv import load_dotenv  # noqa: E402
load_dotenv(Path(__file__).parent.parent / ".env")

from src.clients.polymarket import PolymarketClient  # noqa: E402
from src.clients.polymarket_us import PolymarketUSClient  # noqa: E402
from src.config import load_config  # noqa: E402


# Polymarket US is CFTC-regulated; doesn't list state-regulated sports books.
# Gamma's `category` field on individual markets is often empty/inconsistent,
# so rely on question-text matching for sports keywords as the primary filter.
SPORTS_KEYWORDS = (
    "fifa", "world cup", "nba", "nba finals", "stanley cup", "nhl", "nfl",
    "super bowl", "mlb", "world series", "ufc", "boxing", "tennis", "f1",
    "formula 1", "premier league", "champions league", "uefa", "olympics",
    "ncaa", "march madness", "wimbledon", "us open", "masters",
    "heavyweight", "middleweight", "bantamweight", "flyweight",
    "drivers' champion", "constructors' champion",
    "wins the", "win the 20",  # "wins the 2026 X" / "win the 2026 Y" pattern
)


def _is_sports(market: dict) -> bool:
    """Best-effort sports detection via category OR question text."""
    cat = (market.get("category") or market.get("categorySlug") or "").lower()
    if cat == "sports":
        return True
    q = (market.get("question") or "").lower()
    return any(kw in q for kw in SPORTS_KEYWORDS)


# Fallback slugs known to exist on Polymarket US (politics / IPO / crypto /
# economics) — used if the Gamma top-by-volume path returns mostly sports
# even after filtering. User can override with --slugs.
FALLBACK_SLUGS = [
    "will-openai-not-ipo-by-december-31-2026",
    "will-bitcoin-reach-200000-by-december-31-2026",
    "will-the-republican-party-control-the-house-after-the-2026-midterm-elections",
    "another-us-debt-downgrade-before-2027",
    "will-no-fed-rate-cuts-happen-in-2026",
]


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

    # Sanity probe: dump what US actually has so we can see the slug format
    # they use vs international. If 404s persist on slugs we know exist on
    # intl, the slugs almost certainly differ between the two platforms.
    print("\n=== US side market sample (top 10 by volume) ===\n")
    us_sample = await poly_us.fetch_markets(limit=50, active=True, closed=False)
    us_sample.sort(
        key=lambda m: float(m.get("volumeNum", 0) or m.get("volume", 0) or 0),
        reverse=True,
    )
    if not us_sample:
        print("  (US returned NO markets at all — endpoint or auth issue)")
    else:
        print(f"  Found {len(us_sample)} active markets on US gateway. Top 10:")
        for m in us_sample[:10]:
            slug = m.get("slug") or "(no-slug)"
            q = (m.get("question") or "")[:65]
            print(f"    {slug:55s}  {q}")
    print()

    print(f"=== Polymarket US vs International Price Diff ===\n")
    print(
        f"US client authenticated: {poly_us.authenticated}  "
        f"(read-only diff still works without creds)"
    )
    if not poly_us.authenticated:
        # Diagnostic: did .env at least have the variables?
        kid = os.environ.get("POLYMARKET_US_KEY_ID", "")
        sec = os.environ.get("POLYMARKET_US_SECRET_KEY", "")
        print(
            f"  Diagnostic: POLYMARKET_US_KEY_ID set={bool(kid)} (len={len(kid)}), "
            f"POLYMARKET_US_SECRET_KEY set={bool(sec)} (len={len(sec)})"
        )
        if not kid or not sec:
            print(
                "  -> .env not found or missing POLYMARKET_US_* keys. "
                "Add them to /root/arb-agent/.env"
            )
    print()

    slugs: list[str]
    intl_markets: dict[str, dict] = {}

    if args.slugs:
        slugs = [s.strip() for s in args.slugs.split(",") if s.strip()]
        # Direct slug lookup via Gamma's slug filter param.
        import httpx
        async with httpx.AsyncClient(timeout=30) as client:
            for s in slugs:
                try:
                    resp = await client.get(
                        "https://gamma-api.polymarket.com/markets",
                        params={"slug": s, "limit": 1},
                    )
                    if resp.status_code == 200:
                        batch = resp.json() or []
                        if batch:
                            intl_markets[s] = batch[0]
                except Exception:
                    pass
    else:
        candidates = await poly_intl.fetch_markets(
            limit=300, max_markets=300,
            max_days_to_close=400, min_volume=10000,
        )
        non_sports = [m for m in candidates if not _is_sports(m)]
        non_sports.sort(key=lambda m: float(m.get("volume", 0) or 0), reverse=True)
        n_skipped = len(candidates) - len(non_sports)
        if n_skipped:
            print(
                f"  Filtered out {n_skipped} sports markets (Polymarket US "
                f"doesn't list sports).\n"
            )
        for m in non_sports[: args.limit]:
            slug = m.get("slug")
            if slug:
                intl_markets[slug] = m

        # If filtering still left us with mostly sports (e.g. Gamma is
        # 100% sports right now), fall back to known politics/IPO/crypto
        # slugs that should exist on US.
        if len(intl_markets) < args.limit:
            print(
                f"  Only {len(intl_markets)} non-sports markets matched; "
                f"using fallback slug list.\n"
            )
            import httpx
            async with httpx.AsyncClient(timeout=30) as client:
                for s in FALLBACK_SLUGS:
                    if s in intl_markets:
                        continue
                    if len(intl_markets) >= args.limit:
                        break
                    try:
                        resp = await client.get(
                            "https://gamma-api.polymarket.com/markets",
                            params={"slug": s, "limit": 1},
                        )
                        if resp.status_code == 200:
                            batch = resp.json() or []
                            if batch:
                                intl_markets[s] = batch[0]
                    except Exception:
                        pass
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
