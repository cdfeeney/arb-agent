import asyncio
import hashlib
import json
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Optional

log = logging.getLogger(__name__)

VERIFY_PROMPT = """You are verifying whether two binary prediction markets resolve on the IDENTICAL underlying event. Cross-platform arbitrage requires that "YES on market A" and "YES on market B" mean the same thing — that the same real-world outcome would resolve both to YES.

Return a single JSON object: {{"is_match": true|false, "reasoning": "<1-2 sentences>"}}

Mark is_match=TRUE when:
- The same real-world outcome resolves both markets to YES, AND
- The resolution date/window aligns (within a few days), AND
- The threshold (if any) is the same, AND
- The resolution-conditions text describes the same triggering event with the same
  specificity (e.g. both require explicit recognition of the SAME person, not just
  related state actions; both define the same threshold computation).

Wording differences that DO NOT matter (still match):
- "Will X win?" vs "Who will win? – X"
- "X out by date Y?" vs "Will X be out before date Y+1?" (close-of-day Y resolves both)
- "Will X receive the most votes?" vs "Who will win the primary? – X"
- Verbose Kalshi rules vs Polymarket short questions, when the underlying event is identical.
- Multi-outcome Kalshi event ("Who will IPO before 2027? – Kraken") matching a binary
  Polymarket question ("Kraken IPO by Dec 31, 2026?") — these ARE legitimate arbs as long
  as the date window matches.
- INVERSE PHRASING. "Will X NOT happen by Y?" matches "Who will happen? – X" with the
  SAME date Y, because YES on the negative-phrased market resolves true exactly when the
  positive one resolves false. Example: "Will OpenAI not IPO by December 31, 2026?" MATCHES
  "Who will IPO before 2027? – OpenAI" — these resolve on the same OpenAI-IPO event.
- INVERSE PARTY phrasing. "Will Republicans win [seat]?" matches "Will Democratics win
  [same seat]? – Democratic party" — these are the same head-to-head race; YES on one
  is NO on the other. The two-party race is a binary outcome regardless of which side
  the question asks about.

Mark is_match=FALSE when:
- Different thresholds or time windows ("BTC > $100k by Dec 31" vs "BTC > $100k by Jan 31",
  "Fed funds rate above 4.25% by July" vs "above 4.50% by July").
- One is a strict sub-question of the other ("Fed cuts rates" vs "Who dissents at FOMC?").
- Different resolution sources that could plausibly disagree on outcome.
- The underlying events are genuinely different.
- ★ EXCLUSIVE-RACE vs UNIVERSAL-BINARY mismatch. A "first/next/who-will" race outcome
  resolves YES only when the named entity is the FIRST to do X (relative comparison
  among the basket members). A binary "Will X do Y?" resolves YES whenever X does Y,
  independent of others. These are NOT the same event.
  Example of NO match: "Donald Trump out before 2027?" (universal binary, YES if Trump
  leaves for any reason) vs "Will Donald Trump be the next leader out before 2027?"
  (exclusive race, YES only if Trump is the FIRST among a basket of world leaders to
  leave). Trump can leave second → first market YES, second market NO. Mark is_match=FALSE.
- ★ DATE-BUCKET MISMATCH inside a parametric series. Markets like "Will [X] before
  April 1?" vs "Will [X] before August 1?" are date-bucketed siblings, NOT the same
  event. The shorter window is a proper subset of the longer. Use the close-time
  fields below as the discriminator: if the two close times differ by more than
  3 days within a date-bucketed series, mark FALSE.
- ★ RESOLUTION-CONDITIONS MISMATCH. The rules text below describes WHEN each market
  resolves YES. If one requires X but the other requires X-and-Y, or one accepts a
  broader set of triggers than the other, they are not the same event. Example:
  Polymarket "US officially recognizes person Z" requiring direct US-government action
  vs Kalshi "US recognizes person Z" allowing any branch of government — usually still
  match if the government-action trigger is identical. But Polymarket "Trump declares
  X" vs Kalshi "any US official declares X" is a real divergence — mark FALSE.

Calibration: false positives cost real money; false negatives leave money on the table.
Both are bad. Lean toward MATCH when the resolution criterion is clearly the same despite
different phrasing — your job is to spot semantic equivalence, not punish word variation.
But the EXCLUSIVE-RACE vs UNIVERSAL-BINARY mismatch is a hard rule: if Polymarket's market
is part of an exclusive basket (signaled below) and the Kalshi market is a standalone
binary "Will X do Y?", mark FALSE regardless of question similarity.

Market A (Kalshi):
  Question: {a_question}
  YES means: {a_yes_sub}
  NO means:  {a_no_sub}
  Event ticker: {a_event_ticker}
  Closes at: {a_closes_at}
  Resolution rules:
    {a_rules}

Market B (Polymarket):
  Question: {b_question}
  Closes at: {b_closes_at}
  Polymarket structural flags:
    negRisk = {b_neg_risk}            (true = exclusive-basket sub-outcome)
    groupItemTitle = "{b_group_item_title}"  (the sub-item label inside its basket)
  Resolution description:
    {b_description}

Respond with JSON only, no prose."""


class LLMVerifier:
    def __init__(
        self,
        db,
        api_key: str,
        model: str = "claude-haiku-4-5-20251001",
        cache_hours: int = 720,  # 30 days; markets don't change semantics on a 24h timer
        max_concurrency: int = 10,
    ):
        self.db = db
        self.api_key = api_key
        self.model = model
        self.cache_hours = cache_hours
        self._client = None
        # Bounded concurrency for the API. Anthropic Haiku tier handles
        # well above 10 concurrent requests; cap conservatively to stay
        # within rate limits and avoid spamming on retry storms.
        self._semaphore = asyncio.Semaphore(max_concurrency)

    def _get_client(self):
        if self._client is None:
            from anthropic import Anthropic
            self._client = Anthropic(api_key=self.api_key)
        return self._client

    async def verify(self, market_a: dict, market_b: dict) -> Optional[bool]:
        """Returns True if markets are the same event, False if not, None on API error."""
        pair_id = self._pair_id(market_a, market_b)
        content_hash = self._content_hash(market_a, market_b)

        cached = await self.db.get_verification(pair_id, self.cache_hours)
        # If cache exists but the underlying market text/close-time has
        # changed since we cached, force a re-verify. This catches the
        # rare case where Polymarket extends a deadline or amends the
        # description, or Kalshi rewrites rules_secondary.
        if cached is not None and cached.get("content_hash") == content_hash:
            return cached["is_match"]

        prompt = VERIFY_PROMPT.format(
            a_question=market_a.get("question", ""),
            a_yes_sub=market_a.get("yes_sub_title", "") or "(not specified)",
            a_no_sub=market_a.get("no_sub_title", "") or "(not specified)",
            a_event_ticker=market_a.get("event_ticker", "") or "(unknown)",
            a_closes_at=str(market_a.get("closes_at", "") or "(unknown)"),
            a_rules=self._kalshi_rules_text(market_a),
            b_question=market_b.get("question", ""),
            b_closes_at=str(market_b.get("closes_at", "") or "(unknown)"),
            b_neg_risk=str(market_b.get("neg_risk", False)).lower(),
            b_group_item_title=market_b.get("group_item_title", "") or "",
            b_description=(market_b.get("description", "") or "(none provided)")[:2000],
        )

        try:
            async with self._semaphore:
                result = await asyncio.get_event_loop().run_in_executor(
                    None, self._call_anthropic, prompt
                )
        except Exception as e:
            log.warning("LLM verify failed for %s: %s", pair_id, e)
            return None

        is_match = bool(result.get("is_match", False))
        reasoning = str(result.get("reasoning", ""))[:500]
        await self.db.save_verification(
            pair_id, is_match, reasoning, content_hash=content_hash,
        )
        log.info(
            "LLM verify %s → %s | %s",
            "MATCH" if is_match else "SKIP",
            pair_id,
            reasoning,
        )
        return is_match

    @staticmethod
    def _kalshi_rules_text(market: dict) -> str:
        """Concatenate Kalshi rules_primary + rules_secondary, truncated. Both
        are normalized to '' when missing, so this is safe on partial data."""
        primary = (market.get("rules_primary") or "").strip()
        secondary = (market.get("rules_secondary") or "").strip()
        text = (primary + ("\n" + secondary if secondary else "")).strip()
        return text[:2000] if text else "(none provided)"

    @staticmethod
    def _content_hash(a: dict, b: dict) -> str:
        """Hash of the inputs that affect verification outcome. If any of
        these change after a cache write, we re-verify."""
        payload = "|".join([
            (a.get("question", "") or ""),
            (a.get("yes_sub_title", "") or ""),
            (a.get("no_sub_title", "") or ""),
            str(a.get("closes_at", "") or ""),
            (a.get("rules_primary", "") or ""),
            (a.get("rules_secondary", "") or ""),
            (b.get("question", "") or ""),
            str(b.get("closes_at", "") or ""),
            str(b.get("neg_risk", False)),
            (b.get("group_item_title", "") or ""),
            (b.get("description", "") or ""),
        ])
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]

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
