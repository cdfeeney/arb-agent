import asyncio
import json
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Optional

log = logging.getLogger(__name__)

VERIFY_PROMPT = """You are verifying whether two binary prediction markets resolve on the IDENTICAL underlying event. Cross-platform arbitrage requires that "YES on market A" and "NO on market B" are perfect economic opposites.

Return a single JSON object: {{"is_match": true|false, "reasoning": "<1-2 sentences>"}}

Mark is_match=true ONLY if BOTH conditions hold:
1. The same real-world outcome resolves both markets (same event, same threshold, same time window).
2. YES on market A and YES on market B mean the same thing — a YES on one would be a YES on the other.

Mark is_match=false if:
- One is a sub-question of the other (e.g. "Fed cuts rates" vs "Who dissents at FOMC?").
- Different thresholds or time windows ("BTC > $100k by Dec 31" vs "BTC > $100k by Jan 31").
- One is multi-outcome and the other is binary on a different slice.
- Different resolution sources that could disagree.
- Phrasing is similar but the events differ.

Be strict. False positives cost real money; false negatives just skip an opp.

Market A (Kalshi):
  Question: {a_question}
  YES means: {a_yes_sub}
  NO means:  {a_no_sub}

Market B (Polymarket):
  Question: {b_question}

Respond with JSON only, no prose."""


class LLMVerifier:
    def __init__(
        self,
        db,
        api_key: str,
        model: str = "claude-haiku-4-5-20251001",
        cache_hours: int = 24,
    ):
        self.db = db
        self.api_key = api_key
        self.model = model
        self.cache_hours = cache_hours
        self._client = None

    def _get_client(self):
        if self._client is None:
            from anthropic import Anthropic
            self._client = Anthropic(api_key=self.api_key)
        return self._client

    async def verify(self, market_a: dict, market_b: dict) -> Optional[bool]:
        """Returns True if markets are the same event, False if not, None on API error."""
        pair_id = self._pair_id(market_a, market_b)

        cached = await self.db.get_verification(pair_id, self.cache_hours)
        if cached is not None:
            return cached["is_match"]

        prompt = VERIFY_PROMPT.format(
            a_question=market_a.get("question", ""),
            a_yes_sub=market_a.get("yes_sub_title", "") or "(not specified)",
            a_no_sub=market_a.get("no_sub_title", "") or "(not specified)",
            b_question=market_b.get("question", ""),
        )

        try:
            result = await asyncio.get_event_loop().run_in_executor(
                None, self._call_anthropic, prompt
            )
        except Exception as e:
            log.warning("LLM verify failed for %s: %s", pair_id, e)
            return None

        is_match = bool(result.get("is_match", False))
        reasoning = str(result.get("reasoning", ""))[:500]
        await self.db.save_verification(pair_id, is_match, reasoning)
        log.info(
            "LLM verify %s → %s | %s",
            "MATCH" if is_match else "SKIP",
            pair_id,
            reasoning,
        )
        return is_match

    def _call_anthropic(self, prompt: str) -> dict:
        client = self._get_client()
        msg = client.messages.create(
            model=self.model,
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        text = msg.content[0].text.strip()
        # strip code fences if the model wraps the JSON
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text.strip())

    @staticmethod
    def _pair_id(a: dict, b: dict) -> str:
        ka = f"{a.get('platform')}:{a.get('ticker')}"
        kb = f"{b.get('platform')}:{b.get('ticker')}"
        return "|".join(sorted([ka, kb]))
