"""Diagnose what categories of markets we're scanning vs missing.

Critical question: when 0 arbs are found, is it because:
  A) markets ARE efficient (no edges to capture), or
  B) we're not even LOOKING at the right markets (filter problem).

Categorizes Kalshi binary survivors AND Kalshi multi-outcome events
(which our filter currently drops) by topic, plus shows the same for
Polymarket. Output is a category-level coverage map.

Run: py -3 -m scripts.diagnose_coverage
"""
import asyncio
import os
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv(Path(__file__).parent.parent / ".env")

from src.clients.kalshi import KalshiClient
from src.clients.polymarket import PolymarketClient
from src.engine.normalizer import normalize_kalshi, normalize_polymarket


CATEGORIES = [
    ("politics-pres",      r"\b(president|presidential|trump|biden|harris|approval|approve)\b"),
    ("politics-cong",      r"\b(senate|house|congress|midterm|speaker|filibuster)\b"),
    ("politics-elect",     r"\b(election|primary|caucus|nominee|governor|gubernatorial|mayor)\b"),
    ("politics-policy",    r"\b(impeach|veto|bill|legislation|tariff|sanction|executive order)\b"),
    ("macro-fed",          r"\b(fed|fomc|rate cut|rate hike|powell|interest rate|fed funds)\b"),
    ("macro-data",         r"\b(cpi|inflation|gdp|unemployment|jobs report|recession|payroll)\b"),
    ("crypto",             r"\b(bitcoin|btc|ethereum|eth|crypto|solana|sol|dogecoin|coinbase)\b"),
    ("sports-nfl",         r"\b(nfl|super bowl|chiefs|cowboys|patriots|49ers|eagles|bills)\b"),
    ("sports-nba",         r"\b(nba|lakers|celtics|warriors|mvp|finals|playoffs)\b"),
    ("sports-mlb",         r"\b(mlb|world series|yankees|dodgers|red sox)\b"),
    ("sports-nhl",         r"\b(nhl|stanley cup|hockey)\b"),
    ("sports-other",       r"\b(soccer|fifa|tennis|golf|f1|ufc|olympics|wimbledon|french open)\b"),
    ("entertainment",      r"\b(oscar|grammy|emmy|movie|album|song|spotify|netflix|gta|game release)\b"),
    ("tech-ai",            r"\b(openai|anthropic|gpt|claude|gemini|llm|ai|chatgpt|google|apple)\b"),
    ("geopolitics",        r"\b(ukraine|russia|china|iran|israel|gaza|nato|war|treaty|ceasefire)\b"),
    ("weather-disaster",   r"\b(hurricane|earthquake|wildfire|storm|temperature|snow)\b"),
]


def categorize(text: str) -> str:
    text = text.lower()
    for name, pattern in CATEGORIES:
        if re.search(pattern, text):
            return name
    return "other"


async def main():
    k = KalshiClient(
        os.environ["KALSHI_API_KEY_ID"],
        os.environ["KALSHI_PRIVATE_KEY_PATH"],
        60,
    )
    p = PolymarketClient(120)

    print("Fetching markets (max 60d, min vol $500)...")
    k_raw, p_raw = await asyncio.gather(
        k.fetch_markets(max_days_to_close=60, min_hours_to_close=24),
        p.fetch_markets(max_days_to_close=60, min_volume=500),
    )

    k_norm = [m for m in (normalize_kalshi(r) for r in k_raw) if m]
    p_norm = [m for m in (normalize_polymarket(r) for r in p_raw) if m]

    # Group Kalshi by event_ticker to identify multi-outcome groups
    by_event = defaultdict(list)
    for m in k_norm:
        by_event[m.get("event_ticker", "")].append(m)

    binary_singletons = []  # what our pipeline currently sees
    multi_outcome_dropped = []  # what we're filtering out
    for evt, markets in by_event.items():
        if not evt:
            binary_singletons.extend(markets)
        elif len(markets) == 1:
            binary_singletons.extend(markets)
        else:
            multi_outcome_dropped.extend(markets)

    print(f"\n=== KALSHI ({len(k_raw)} raw, {len(k_norm)} normalized) ===")
    print(f"Binary singletons (currently scanned): {len(binary_singletons)}")
    print(f"Multi-outcome dropped (currently SKIPPED): {len(multi_outcome_dropped)}")
    print(f"Multi-outcome event groups: {sum(1 for v in by_event.values() if len(v) > 1)}")

    # Volume-weighted coverage
    def cat_summary(markets, label):
        cat_counts = Counter()
        cat_volume = defaultdict(float)
        for m in markets:
            c = categorize(m.get("question", ""))
            cat_counts[c] += 1
            cat_volume[c] += m.get("volume", 0)
        print(f"\n  {label} — categories (count / total $vol):")
        for c, n in cat_counts.most_common():
            print(f"    {c:<22} {n:>4}  ${cat_volume[c]:>12,.0f}")

    cat_summary(binary_singletons, "KALSHI BINARY (scanned)")
    cat_summary(multi_outcome_dropped, "KALSHI MULTI-OUTCOME (dropped)")

    print(f"\n=== POLYMARKET ({len(p_raw)} raw, {len(p_norm)} normalized) ===")
    cat_summary(p_norm, "POLYMARKET")

    # Highlight: large multi-outcome events on Kalshi by category
    print("\n=== Largest Kalshi multi-outcome event groups (by total volume) ===")
    evt_summary = []
    for evt, markets in by_event.items():
        if len(markets) > 1:
            total_vol = sum(m.get("volume", 0) for m in markets)
            sample_q = markets[0].get("question", "")[:80]
            evt_summary.append((total_vol, len(markets), evt, sample_q))
    evt_summary.sort(reverse=True)
    for total_vol, n, evt, q in evt_summary[:15]:
        print(f"  ${total_vol:>10,.0f}  ({n:>3} options)  {evt:<35}  {q}")


if __name__ == "__main__":
    asyncio.run(main())
