"""Compare Polymarket US prices to international Polymarket prices.

This is THE gating question for the bot's paper-trade validity. Today the
bot reads prices from clob.polymarket.com (offshore). If the user trades
through the US-regulated app at api.polymarket.us, every "arb" we capture
is fictional unless those two books quote the same prices.

Strategy: pick markets from what US ACTUALLY HAS first, then look them up
on intl by question text. Reverse direction is the only reliable one
because US slugs differ from intl slugs and US carries a different
catalog (different categories, different naming).

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


def _norm_tokens(text: str) -> set[str]:
    """Tokenize for question-text matching: lowercase, strip punctuation,
    drop short common words. Used to score whether two questions describe
    the same event."""
    import re
    if not text:
        return set()
    raw = re.sub(r"[^a-z0-9 ]+", " ", text.lower()).split()
    stop = {"the", "a", "an", "is", "be", "by", "in", "on", "of", "to", "for",
            "will", "or", "and", "with", "vs"}
    return {t for t in raw if len(t) >= 3 and t not in stop}


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

    print(f"=== Polymarket US vs International Price Diff ===")
    print(f"US client authenticated: {poly_us.authenticated}\n")

    # Strategy: pick markets from US first (smaller catalog), then look them
    # up on intl by question text. Reverse direction (start from intl) fails
    # because US doesn't carry every intl category and slugs don't match.
    us_picks: list[dict] = []

    if args.slugs:
        # Direct US slug lookup via /v1/markets?slug=
        import httpx
        for s in [s.strip() for s in args.slugs.split(",") if s.strip()]:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(
                    f"{poly_us.PUBLIC_URL}/v1/markets",
                    params={"slug": s, "limit": 1},
                )
            if resp.status_code == 200:
                batch = (resp.json() or {}).get("markets") or []
                if batch:
                    us_picks.append(batch[0])
                else:
                    print(f"  US slug not found: {s}")
            else:
                print(f"  US lookup failed for {s}: {resp.status_code}")
    else:
        print(f"Pulling US active markets…")
        us_all = await poly_us.fetch_markets(limit=200, active=True, closed=False)

        # US uses different volume/activity field names than intl. Try a list
        # of plausible names and use the first non-zero one. The previous
        # `volumeNum` was 0 across all 1144 markets, so the sort was random.
        VOLUME_FIELDS = (
            "volumeNum", "volume", "volume24Hr", "volume24hr", "volume24h",
            "totalVolume", "openInterest", "liquidity", "liquidityNum",
            "tradeCount", "lastPriceSampleVolume",
        )

        def _activity_score(m: dict) -> float:
            for f in VOLUME_FIELDS:
                v = m.get(f)
                if v is None:
                    continue
                try:
                    n = float(v)
                except (TypeError, ValueError):
                    continue
                if n > 0:
                    return n
            return 0.0

        us_all.sort(key=_activity_score, reverse=True)

        # Diagnostic: which field is actually populated on the first market?
        if us_all:
            sample = us_all[0]
            populated = {k: sample.get(k) for k in VOLUME_FIELDS if sample.get(k)}
            print(
                f"  Diagnostic — activity fields populated on top market: "
                f"{populated or '(NONE — all zero/missing)'}"
            )
            print(
                f"  All non-empty top-level field names on first US market: "
                f"{sorted(k for k, v in sample.items() if v not in (None, '', 0, [], {}))}"
            )

        print(f"\n  US has {len(us_all)} active markets. Top 15 by activity:")
        for m in us_all[:15]:
            cat = m.get("category") or ""
            print(
                f"    [{cat:12s}] act={_activity_score(m):>12,.2f}  "
                f"{(m.get('slug') or '')[:50]:50s}  "
                f"{(m.get('question') or '')[:55]}"
            )
        print()

        # Skip "future-event" markets with zero activity — they will all
        # have flat 50¢ prices and dominate the list when nothing has
        # actual volume yet.
        active_only = [m for m in us_all if _activity_score(m) > 0]
        if active_only:
            us_picks = active_only[: args.limit]
        else:
            print(
                "  WARNING: no US market has any populated activity field — "
                "falling back to first N regardless.\n"
            )
            us_picks = us_all[: args.limit]

    if not us_picks:
        print("No US markets to compare.\n")
        return

    print(f"Comparing {len(us_picks)} markets (US-side first):\n")
    diffs: list[dict] = []
    for usm in us_picks:
        us_slug = usm.get("slug") or ""
        us_question = usm.get("question") or ""
        us_category = usm.get("category") or ""

        # US prices: prefer BBO (light), fall back to /book.
        us_bbo = await poly_us.fetch_market_bbo(us_slug)
        us_yes_bid = us_yes_ask = None
        if us_bbo:
            bb = us_bbo.get("bestBid") or {}
            ba = us_bbo.get("bestAsk") or {}
            try:
                us_yes_bid = float(bb.get("value", 0) or 0)
            except (TypeError, ValueError):
                pass
            try:
                us_yes_ask = float(ba.get("value", 0) or 0)
            except (TypeError, ValueError):
                pass
        if us_yes_bid is None and us_yes_ask is None:
            book = await poly_us.fetch_market_book(us_slug)
            if book:
                us_yes_bid, _ = PolymarketUSClient.best_bid_from_book(book)
                us_yes_ask, _ = PolymarketUSClient.best_ask_from_book(book)
        # Fallback to bestBid/bestAsk on the market record itself.
        if us_yes_bid is None:
            try:
                us_yes_bid = float(usm.get("bestBid", 0) or 0)
            except (TypeError, ValueError):
                us_yes_bid = 0.0
        if us_yes_ask is None:
            try:
                us_yes_ask = float(usm.get("bestAsk", 0) or 0)
            except (TypeError, ValueError):
                us_yes_ask = 0.0

        # Find matching intl market by question text via Gamma full-text search.
        # Then validate the match by token overlap so a junk hit doesn't
        # silently distort the diff. Use a *ratio* rule rather than an
        # absolute count so short questions ("NBA MVP" = 2 tokens) can match
        # while still rejecting unrelated long-question pairings (Hungary PM
        # vs FlyQuest LoL = 0 overlap).
        intl: dict | None = None
        intl_match_question = None
        if us_question:
            import httpx
            try:
                async with httpx.AsyncClient(timeout=30) as client:
                    resp = await client.get(
                        "https://gamma-api.polymarket.com/markets",
                        params={"q": us_question, "limit": 5, "active": "true"},
                    )
                if resp.status_code == 200:
                    candidates = resp.json() or []
                    us_tokens = _norm_tokens(us_question)
                    best_overlap = 0
                    best_min_len = 0
                    for c in candidates:
                        c_q = c.get("question") or ""
                        c_tokens = _norm_tokens(c_q)
                        if not c_tokens or not us_tokens:
                            continue
                        overlap = len(us_tokens & c_tokens)
                        if overlap > best_overlap:
                            best_overlap = overlap
                            best_min_len = min(len(us_tokens), len(c_tokens))
                            intl = c
                            intl_match_question = c_q
                    # Match rule: at least 2 shared tokens AND those tokens
                    # cover ≥50% of the SHORTER question. This passes
                    # "NBA MVP"↔"NBA MVP" (2/2=100%) and rejects
                    # "Bitcoin $150k"↔"Bitcoin $200k" (1 shared, 1/3=33%).
                    needed = max(2, (best_min_len + 1) // 2)
                    if best_overlap < needed:
                        intl = None
                        intl_match_question = None
            except Exception as e:
                print(f"  intl search error: {e}")

        intl_clob_yes_bid = intl_clob_yes_ask = None
        intl_yes_bid = intl_yes_ask = 0.0
        if intl:
            try:
                intl_yes_ask = float(intl.get("bestAsk", 0) or 0)
                intl_yes_bid = float(intl.get("bestBid", 0) or 0)
            except (TypeError, ValueError):
                pass
            clob_tokens = intl.get("clobTokenIds")
            if isinstance(clob_tokens, str):
                import json
                try:
                    clob_tokens = json.loads(clob_tokens)
                except Exception:
                    clob_tokens = None
            if clob_tokens:
                yes_book = await poly_intl.fetch_clob_book(clob_tokens[0])
                if yes_book:
                    intl_clob_yes_ask, _ = poly_intl.best_ask_from_book(yes_book)
                    intl_clob_yes_bid, _ = poly_intl.best_bid_from_book(yes_book)

        print(f"  --- {us_slug} ---")
        print(f"      US   Q: [{us_category}] {us_question[:70]}")
        if intl:
            print(f"      Intl Q: {(intl_match_question or '')[:70]}")
            print(f"      Intl (Gamma):     YES bid={intl_yes_bid:.4f}  ask={intl_yes_ask:.4f}")
            if intl_clob_yes_bid is not None:
                print(f"      Intl (CLOB live): YES bid={intl_clob_yes_bid:.4f}  ask={intl_clob_yes_ask:.4f}")
        else:
            print(f"      Intl Q: NOT FOUND (no intl market with sufficient question overlap)")
        print(f"      US   (api.us):    YES bid={us_yes_bid or 0:.4f}  ask={us_yes_ask or 0:.4f}")
        if intl_clob_yes_ask is not None and us_yes_ask:
            ask_diff = us_yes_ask - intl_clob_yes_ask
            print(f"      Δ ASK (US - intl-CLOB): {ask_diff:+.4f}")
            diffs.append({
                "us_slug": us_slug,
                "ask_diff": ask_diff,
                "intl_ask": intl_clob_yes_ask,
                "us_ask": us_yes_ask,
            })
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
