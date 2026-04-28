"""Smoke test for the LLM market verifier.

Runs three hand-crafted pairs through the real Anthropic API:
  1. TRUE  match — same election, both phrased differently
  2. FALSE match — broad event vs sub-question (the failure mode that
                   motivated building this — "Fed cuts" vs "who dissents")
  3. FALSE match — totally unrelated low-priced markets that happened to
                   share a noun (the kind of false positive fuzzy matching
                   was producing before)

Run from project root:
    py -3 -m scripts.test_verifier
"""
import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv(Path(__file__).parent.parent / ".env")

from src.db.store import Database
from src.engine.llm_verifier import LLMVerifier


PAIRS = [
    {
        "label": "TRUE-same-election",
        "expected": True,
        "kalshi": {
            "platform": "kalshi",
            "ticker": "PRES-2028-DEM",
            "question": "Will a Democrat win the 2028 US presidential election?",
            "yes_sub_title": "Democratic nominee wins 270+ electoral votes in Nov 2028",
            "no_sub_title":  "Any non-Democrat wins or election doesn't resolve",
        },
        "poly": {
            "platform": "polymarket",
            "ticker": "0x123",
            "question": "Will the Democratic Party win the 2028 presidential election?",
        },
    },
    {
        "label": "FALSE-sub-question",
        "expected": False,
        "kalshi": {
            "platform": "kalshi",
            "ticker": "KXFEDDISSENT-DEC25-POWELL",
            "question": "Will Powell dissent at the December 2025 FOMC meeting?",
            "yes_sub_title": "Powell casts a dissenting vote",
            "no_sub_title":  "Powell votes with the majority",
        },
        "poly": {
            "platform": "polymarket",
            "ticker": "0x456",
            "question": "Will the Fed cut rates at the December 2025 FOMC meeting?",
        },
    },
    {
        "label": "FALSE-unrelated",
        "expected": False,
        "kalshi": {
            "platform": "kalshi",
            "ticker": "OSCAR-BEST-PIC-2026",
            "question": "Will Oppenheimer win Best Picture at the 2026 Oscars?",
            "yes_sub_title": "Oppenheimer named Best Picture",
            "no_sub_title":  "Any other film wins",
        },
        "poly": {
            "platform": "polymarket",
            "ticker": "0x789",
            "question": "Will Oppenheimer be the top-grossing biopic of 2026?",
        },
    },
]


async def main():
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        print("FAIL: ANTHROPIC_API_KEY not set in .env")
        sys.exit(1)

    db = Database("data/test_verifier.db")
    await db.init()
    verifier = LLMVerifier(db=db, api_key=api_key, cache_hours=0)  # always fresh for the test

    passed = 0
    for pair in PAIRS:
        got = await verifier.verify(pair["kalshi"], pair["poly"])
        ok = got == pair["expected"]
        flag = "PASS" if ok else "FAIL"
        print(f"  [{flag}] {pair['label']}: expected={pair['expected']} got={got}")
        if ok:
            passed += 1

    print(f"\n{passed}/{len(PAIRS)} passed")
    sys.exit(0 if passed == len(PAIRS) else 1)


if __name__ == "__main__":
    asyncio.run(main())
