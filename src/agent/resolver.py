"""Resolves paper trades by fetching the actual outcome of closed markets.

For every paper trade whose markets have already closed (closes_at <= now),
fetch each leg's market from its platform, determine YES/NO outcome, and
compute realized payout + profit (both legs combined).

Atomic-arbitrage payout structure: regardless of which side wins, you get
exactly N contracts × $1 = $N total payout (one leg pays out, the other
expires worthless). Realized profit = payout − stake − actual_fees.
"""
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx

log = logging.getLogger(__name__)


async def _resolve_kalshi(client: httpx.AsyncClient, kalshi, ticker: str) -> Optional[int]:
    """Returns 1 if YES won, 0 if NO won, None if not yet resolved."""
    try:
        path = f"/trade-api/v2/markets/{ticker}"
        resp = await client.get(
            f"{kalshi.BASE_URL}/markets/{ticker}",
            headers=kalshi._auth_headers("GET", path),
            timeout=20,
        )
        if resp.status_code != 200:
            return None
        m = resp.json().get("market", {})
        result = (m.get("result") or "").lower().strip()
        if result == "yes":
            return 1
        if result == "no":
            return 0
        return None
    except Exception as e:
        log.warning("Kalshi resolve failed for %s: %s", ticker, e)
        return None


async def _resolve_polymarket(client: httpx.AsyncClient, market_id: str) -> Optional[int]:
    """Returns 1 if YES won, 0 if NO won, None if not yet resolved."""
    try:
        resp = await client.get(
            f"https://gamma-api.polymarket.com/markets/{market_id}",
            timeout=20,
        )
        if resp.status_code != 200:
            return None
        m = resp.json()
        if not m.get("closed"):
            return None
        # outcomePrices on a closed market are ["1", "0"] for YES winner, ["0", "1"] for NO
        prices_raw = m.get("outcomePrices", "[]")
        import json as _json
        prices = _json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
        if not prices or len(prices) < 2:
            return None
        try:
            yes_p, no_p = float(prices[0]), float(prices[1])
        except Exception:
            return None
        if yes_p > 0.5:
            return 1
        if no_p > 0.5:
            return 0
        return None
    except Exception as e:
        log.warning("Polymarket resolve failed for %s: %s", market_id, e)
        return None


async def _resolve_one_leg(client, kalshi, platform: str, ticker: str) -> Optional[int]:
    if platform == "kalshi":
        return await _resolve_kalshi(client, kalshi, ticker)
    if platform == "polymarket":
        return await _resolve_polymarket(client, ticker)
    return None


def _compute_realized(trade: dict, yes_won: int, no_won: int) -> tuple[float, float]:
    """Given the resolution of each leg, compute payout and profit.

    The 'YES' leg of an arb pays $1 × contracts iff its market resolved YES.
    The 'NO' leg pays $1 × contracts iff its market resolved NO.
    For a properly-matched arb, exactly ONE side pays — total payout =
    contracts × $1 regardless of which way the underlying event went.

    Partial-unwind aware: if the trade has been partially unwound (some
    contracts already exited via maker-fill / taker-unwind), only the
    REMAINING contracts settle at resolution. The banked partials already
    contributed `partial_realized_usd` net of exit fees. We compute the
    resolution-side profit on `contracts_remaining` and add it to the
    banked partials.

    Pre-fix behavior: this function used `yes_contracts` (the ORIGINAL
    size) and ignored `contracts_remaining` + `partial_realized_usd`. On
    a partially-unwound trade reaching resolution, the resolver overwrote
    realized_profit_usd with the full-position math, deleting the banked
    partials. Fixed 2026-05-04 after audit finding.
    """
    original_contracts = trade["yes_contracts"]
    remaining = trade.get("contracts_remaining")
    if remaining is None:
        # Legacy rows pre-migration; assume no partial activity.
        remaining = original_contracts
    partial_realized = trade.get("partial_realized_usd") or 0.0

    # Resolution-side payout on REMAINING contracts only
    yes_payout = remaining * 1.0 if yes_won == 1 else 0.0
    no_payout  = remaining * 1.0 if no_won  == 0 else 0.0
    resolution_payout = yes_payout + no_payout

    # Cost basis on remaining + prorated entry fees. Entry fees were paid
    # on the original full position; the share attributable to whatever
    # contracts are still open is proportional.
    yes_price = trade.get("yes_observed_price") or 0.0
    no_price  = trade.get("no_observed_price")  or 0.0
    cost_per_contract = yes_price + no_price
    remaining_cost = remaining * cost_per_contract
    total_entry_fees = trade.get("fees_estimated_usd") or 0.0
    remaining_fee_share = (
        total_entry_fees * (remaining / original_contracts)
        if original_contracts > 0 else 0.0
    )

    resolution_profit = resolution_payout - remaining_cost - remaining_fee_share
    total_profit = partial_realized + resolution_profit

    # Total payout for legacy reporting: resolution payout (the partials
    # already happened, no additional payout there).
    return resolution_payout, total_profit


async def resolve_pending(db, kalshi):
    """One pass: find closed paper trades, fetch outcomes, write realized P&L."""
    pending = await db.list_unresolved_paper_trades()
    if not pending:
        log.info("Resolver: nothing pending")
        return 0
    log.info("Resolver: %d trades pending resolution", len(pending))
    resolved = 0
    async with httpx.AsyncClient(timeout=20) as client:
        for t in pending:
            yes_won = await _resolve_one_leg(client, kalshi, t["yes_platform"], t["yes_ticker"])
            no_won  = await _resolve_one_leg(client, kalshi, t["no_platform"],  t["no_ticker"])
            if yes_won is None or no_won is None:
                # Markets technically closed but result not yet posted; try again later
                continue
            # Sanity check: in a true arb, exactly ONE side should be YES (1)
            #   YES leg won → yes_won=1, no_won=1   (NO market also resolved YES = bad)
            #   NO leg won  → yes_won=0, no_won=0   (YES market also resolved NO = bad)
            # If both legs agree on direction we have a true arb payout.
            # If they disagree, we either lose both or win both — that means the
            # markets DIDN'T actually resolve on the same event (LLM error or
            # subtle definition mismatch).
            if yes_won == 1 and no_won == 1:
                # Both said YES — only the yes_leg pays out; no_leg lost (we bet NO and YES happened)
                pass
            payout, profit = _compute_realized(t, yes_won, no_won)
            await db.resolve_paper_trade(t["id"], yes_won, no_won, payout, profit)
            log.info(
                "Resolver: trade #%d %s → predicted=$%.2f realized=$%.2f delta=%+.2f",
                t["id"], t["pair_id"][:50],
                t["predicted_net_usd"], profit, profit - t["predicted_net_usd"],
            )
            resolved += 1
    return resolved
