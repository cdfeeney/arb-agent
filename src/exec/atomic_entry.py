"""Translate (opportunity, sizing) → EntryPlan.

This is mode-agnostic — both LogOnlyExecutor and LiveExecutor consume the
same EntryPlan. The actual two-leg orchestration with naked-leg defense
lives inside LiveExecutor (Sprint 2); for log_only this module is just
plan-building plus a deterministic idempotency key per leg.
"""

from __future__ import annotations

from .base import (
    EntryPlan,
    OrderPlan,
    make_correlation_id,
    make_idempotency_key,
)


def build_entry_plan(opp: dict, sizing: dict, paper_trade_id: int) -> EntryPlan:
    pair_id = opp["pair_id"]
    correlation_id = make_correlation_id(pair_id)
    yes_market = opp["buy_yes"]
    no_market = opp["buy_no"]

    contracts = float(sizing["leg_yes"]["contracts"])

    yes_token = (
        yes_market.get("yes_token")
        if yes_market.get("platform") == "polymarket"
        else None
    )
    no_token = (
        no_market.get("no_token")
        if no_market.get("platform") == "polymarket"
        else None
    )

    leg_yes = OrderPlan(
        leg="yes",
        platform=yes_market["platform"],
        ticker=yes_market["ticker"],
        side="buy_yes",
        price_limit=float(yes_market["yes_price"]),
        contracts=contracts,
        order_type="taker",
        idempotency_key=make_idempotency_key(pair_id, "yes"),
        token=yes_token,
    )
    leg_no = OrderPlan(
        leg="no",
        platform=no_market["platform"],
        ticker=no_market["ticker"],
        side="buy_no",
        price_limit=float(no_market["no_price"]),
        contracts=contracts,
        order_type="taker",
        idempotency_key=make_idempotency_key(pair_id, "no"),
        token=no_token,
    )

    expected_cost = (
        leg_yes.price_limit * contracts + leg_no.price_limit * contracts
    )
    return EntryPlan(
        pair_id=pair_id,
        paper_trade_id=paper_trade_id,
        leg_yes=leg_yes,
        leg_no=leg_no,
        expected_cost_usd=round(expected_cost, 2),
        expected_net_profit_usd=float(sizing.get("net_profit", 0.0)),
        expected_payout_usd=float(sizing.get("guaranteed_payout", 0.0)),
        correlation_id=correlation_id,
    )
