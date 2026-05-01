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
        # Pull recently-paper-traded intl Polymarket slugs from the bot's own
        # DB. These are the markets we ACTUALLY care about — "are the prices
        # we paper-traded against the prices a US account would pay?". Way
        # more useful than picking random markets from US's 1144-market list
        # (which has no volume field, defaulting to NBA MVP candidate clutter).
        import aiosqlite
        import re

        db_path = cfg["database"]["path"]
        intl_slugs_in_play: list[tuple[str, str]] = []  # (slug, question)
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            # Pull most-recent unique polymarket.com slugs from recent paper trades.
            cur = await db.execute(
                """SELECT yes_url, no_url, yes_question, no_question,
                          detected_at
                   FROM paper_trades
                   ORDER BY detected_at DESC
                   LIMIT 200""",
            )
            seen: set[str] = set()
            slug_re = re.compile(r"polymarket\.com/event/([a-z0-9\-]+)")
            for r in await cur.fetchall():
                for url, q in [
                    (r["yes_url"] or "", r["yes_question"] or ""),
                    (r["no_url"] or "", r["no_question"] or ""),
                ]:
                    m = slug_re.search(url)
                    if not m:
                        continue
                    slug = m.group(1)
                    if slug in seen:
                        continue
                    seen.add(slug)
                    intl_slugs_in_play.append((slug, q))
                    if len(intl_slugs_in_play) >= args.limit * 4:
                        break
                if len(intl_slugs_in_play) >= args.limit * 4:
                    break

        if not intl_slugs_in_play:
            print(
                "  No polymarket.com slugs found in paper_trades. "
                "Falling back to US-side market scan.\n"
            )
            us_all = await poly_us.fetch_markets(limit=50, active=True, closed=False)
            us_picks = us_all[: args.limit]
        else:
            print(
                f"Found {len(intl_slugs_in_play)} unique intl slugs from "
                f"recent paper_trades. Looking each up on US…\n"
            )
            for intl_slug, intl_question in intl_slugs_in_play:
                if len(us_picks) >= args.limit:
                    break
                # Search US for this question.
                us_match = None
                events = await poly_us.search(intl_question or intl_slug, limit=5)
                intl_t = _norm_tokens(intl_question)
                best_overlap = 0
                for ev in events:
                    for cand in (ev.get("markets") or []):
                        cand_q = cand.get("question") or ev.get("title") or ""
                        cand_t = _norm_tokens(cand_q)
                        if not cand_t:
                            continue
                        overlap = len(intl_t & cand_t)
                        needed = max(2, (min(len(intl_t), len(cand_t)) + 1) // 2)
                        if overlap >= needed and overlap > best_overlap:
                            best_overlap = overlap
                            us_match = cand
                if us_match:
                    us_match["_intl_slug"] = intl_slug
                    us_match["_intl_question"] = intl_question
                    us_picks.append(us_match)
                    print(
                        f"  ✓ matched: {intl_slug[:50]} → "
                        f"US slug={us_match.get('slug', '?')[:50]}"
                    )
                else:
                    print(f"  ✗ no US match: {intl_slug[:60]}  ({intl_question[:50]})")

    if not us_picks:
        print("No US markets to compare.\n")
        return

    print(f"\nComparing {len(us_picks)} markets:\n")
    diffs: list[dict] = []
    for usm in us_picks:
        us_slug = usm.get("slug") or ""
        us_question = usm.get("question") or usm.get("title") or ""
        us_category = usm.get("category") or ""

        # US prices: prefer live BBO (handles bestBid/bestAsk wrapper),
        # fall back to /book, then to fields on the market list record
        # (bestBidQuote/bestAskQuote — different field names than BBO uses).
        us_bbo = await poly_us.fetch_market_bbo(us_slug)
        us_yes_bid, us_yes_ask = PolymarketUSClient.extract_bid_ask(us_bbo or {})
        if us_yes_bid == 0 and us_yes_ask == 0:
            book = await poly_us.fetch_market_book(us_slug)
            if book:
                us_yes_bid, _ = PolymarketUSClient.best_bid_from_book(book)
                us_yes_ask, _ = PolymarketUSClient.best_ask_from_book(book)
        if us_yes_bid == 0 and us_yes_ask == 0:
            us_yes_bid, us_yes_ask = PolymarketUSClient.extract_bid_ask(usm)

        # Find matching intl market. If we carried _intl_slug from the
        # paper_trades-pivot path, look it up directly on Gamma — that's
        # exact, no fuzzy matching needed. Otherwise (--slugs mode or US-
        # first fallback), search Gamma by US question text and validate
        # token overlap.
        intl: dict | None = None
        intl_match_question = None
        carried_intl_slug = usm.get("_intl_slug")
        import httpx
        if carried_intl_slug:
            try:
                async with httpx.AsyncClient(timeout=30) as client:
                    resp = await client.get(
                        "https://gamma-api.polymarket.com/markets",
                        params={"slug": carried_intl_slug, "limit": 1},
                    )
                if resp.status_code == 200:
                    batch = resp.json() or []
                    if batch:
                        intl = batch[0]
                        intl_match_question = intl.get("question") or ""
            except Exception as e:
                print(f"  intl Gamma slug lookup error: {e}")
        elif us_question:
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
