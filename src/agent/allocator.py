"""Capital-aware opportunity allocator.

Detection produces N candidate arbs per cycle; without a capacity gate we
"deploy" capital we don't have on paper, and the predicted-vs-realized
comparison is fictional. This module enforces the same constraint live
execution will: bankroll - cost basis of currently-held contracts = free
capital, allocate to the best-EV candidates that fit.

Two functions:

  compute_free_capital(db, bankroll)
      Free $ available for new entries this cycle. Sum of (contracts_remaining
      × cost_per_contract) across all open paper trades is "deployed"; the
      remainder of bankroll is free.

  allocate(candidates, free_capital, bankroll)
      Greedy by net_profit descending. Each candidate consumes its bet_size
      from the remaining capacity. Per-pair diversification cap prevents
      stacking on a single arb. Returns (chosen, stats) for logging.

Sits AFTER the existing dedup/cooldown/min_bet filter chain — those decide
*eligibility*; this decides *capacity*.
"""
from __future__ import annotations

import logging
from typing import Iterable

log = logging.getLogger(__name__)


async def compute_free_capital(db, bankroll: float) -> float:
    """Free capital = bankroll - cost basis of all currently-held contracts.

    cost_per_contract is fixed at entry as yes_observed_price + no_observed_price.
    Deployed = sum(contracts_remaining × cost_per_contract) across status='open'
    trades. We don't add back realized cash from prior partial unwinds — that
    cash is already free (no longer in cost basis), so the formula naturally
    accounts for it as contracts_remaining decreases.
    """
    rows = await db.list_open_paper_trades()
    deployed = 0.0
    for r in rows:
        cpc = float(r.get("yes_observed_price") or 0) + float(r.get("no_observed_price") or 0)
        remaining = r.get("contracts_remaining")
        if remaining is None:
            remaining = float(r.get("yes_contracts") or 0)
        deployed += float(remaining) * cpc
    return max(0.0, bankroll - deployed)


def allocate(
    candidates: list[tuple[dict, dict]],
    free_capital: float,
    *,
    bankroll: float,
    max_per_pair_pct: float = 0.30,
) -> tuple[list[tuple[dict, dict]], dict]:
    """Greedy capacity-aware allocator.

    Args:
        candidates: list of (opp, sizing) tuples already past dedup/cooldown/min_bet.
        free_capital: $ available this cycle (from compute_free_capital).
        bankroll: total $ — used to scale per-pair cap.
        max_per_pair_pct: hard cap on single-pair concentration (default 30%).

    Returns:
        chosen: subset of candidates, sorted by net_profit desc, that fit.
        stats:  counts and dollar totals for logging.
    """
    sorted_cands = sorted(
        candidates, key=lambda os: float(os[1].get("net_profit") or 0), reverse=True,
    )
    chosen: list[tuple[dict, dict]] = []
    skipped_capacity = 0
    skipped_diversification = 0
    pair_used: dict[str, float] = {}
    pair_cap = bankroll * max_per_pair_pct
    remaining = free_capital

    for opp, sizing in sorted_cands:
        bet = float(sizing.get("bet_size") or 0)
        if bet <= 0:
            continue
        pid = opp.get("pair_id", "")
        if bet > remaining:
            skipped_capacity += 1
            continue
        already_in_pair = pair_used.get(pid, 0.0)
        if already_in_pair + bet > pair_cap:
            skipped_diversification += 1
            continue
        chosen.append((opp, sizing))
        pair_used[pid] = already_in_pair + bet
        remaining -= bet

    stats = {
        "candidates": len(candidates),
        "chosen": len(chosen),
        "skipped_capacity": skipped_capacity,
        "skipped_diversification": skipped_diversification,
        "free_capital_start": round(free_capital, 2),
        "free_capital_end": round(remaining, 2),
        "deployed_this_cycle": round(free_capital - remaining, 2),
        "pair_cap": round(pair_cap, 2),
    }
    return chosen, stats
