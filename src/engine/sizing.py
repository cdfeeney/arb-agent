"""
Position sizing rules engine.
All rules are driven by config.yaml [sizing] block — no hardcoded numbers here.

For true cross-platform arbitrage, both legs must buy the SAME NUMBER OF CONTRACTS
(not the same USD), so payout is identical regardless of outcome:

    N contracts × $1 payout each = $N guaranteed return
    Cost = N × (yes_price + no_price)
    Gross profit = N × (1 - yes_price - no_price)
    Net profit   = Gross profit - worst-case fees

Rules applied in order (each can only reduce the bet size):
  1. Kelly criterion   — bankroll × edge / cost_per_contract × kelly_fraction
  2. Bankroll cap      — total stake never exceeds max_position_pct × bankroll
  3. Liquidity cap     — neither leg's USD exceeds liquidity_cap_pct × that side's volume
  4. Book-depth cap    — never enter more than book_depth_fraction × min(yes_bid_depth,
                          no_bid_depth) on the unwind side. Stops "phantom MTM" losses
                          from positions too large to exit at top-of-book.
  5. Max bet           — hard ceiling from config (total stake)
  6. Min bet floor     — hard floor from config
"""

import logging

from .fees import compute_arb_fees

log = logging.getLogger(__name__)


def size_position(opportunity: dict, cfg: dict) -> dict:
    edge = opportunity["profit_pct"]
    yes_price = opportunity["buy_yes"]["yes_price"]
    no_price = opportunity["buy_no"]["no_price"]

    # Reject if either price is missing/zero. Previously we floored
    # cost_per_contract at 0.01 which inflated Kelly 100x for 0-priced
    # legs (e.g. fake-cheap due to a normalizer bug). Distinguish "missing
    # data" from "1 cent contract" — both shouldn't be silently equated.
    if yes_price <= 0 or no_price <= 0:
        log.warning(
            "size_position rejected: yes=%.4f no=%.4f (zero price = missing "
            "data, not 1¢ contract)",
            yes_price, no_price,
        )
        return _reject_sizing(opportunity, "zero_or_missing_price")
    cost_per_contract = yes_price + no_price

    vol_yes_usd = opportunity["buy_yes"].get("volume", 0)
    vol_no_usd = opportunity["buy_no"].get("volume", 0)

    # 1. Kelly: edge as fraction of stake, scaled by fractional Kelly
    kelly_raw = edge / cost_per_contract
    kelly_stake = cfg["bankroll"] * kelly_raw * cfg["kelly_fraction"]

    # 2. Bankroll cap (total stake across both legs)
    bankroll_cap = cfg["bankroll"] * cfg["max_position_pct"]

    # 3. Liquidity cap — neither leg's USD can exceed cap_pct × that side's volume.
    # Translate that constraint back to total stake: cap_pct × min(vol_yes/yes_price,
    # vol_no/no_price) gives max contracts; multiply by cost to get total stake cap.
    if vol_yes_usd > 0 and vol_no_usd > 0:
        max_contracts_by_yes_side = vol_yes_usd * cfg["liquidity_cap_pct"] / yes_price
        max_contracts_by_no_side = vol_no_usd * cfg["liquidity_cap_pct"] / no_price
        max_contracts_liquidity = min(max_contracts_by_yes_side, max_contracts_by_no_side)
        liquidity_cap = max_contracts_liquidity * cost_per_contract
    else:
        # Volume missing on at least one leg — clamp to min_bet floor instead
        # of releasing the cap. "Don't know" should shrink the bet, not
        # remove the constraint entirely.
        liquidity_cap = float(cfg.get("min_bet", 5))
        log.info(
            "sizing liquidity cap: missing volume (yes=%s no=%s) → clamp to "
            "min_bet $%.2f", vol_yes_usd, vol_no_usd, liquidity_cap,
        )

    # 4. Book-depth cap — bound entry by the bid-side liquidity we'll need for
    # unwind. Each leg's contract count is capped by `book_depth_fraction` of
    # that side's bid-book USD depth. Use BID prices in the denominator (not
    # ask) since bid-side USD/bid-side price is the contract count we can
    # actually unload at the top of book.
    #
    # CRITICAL CHANGE: missing bid depth on EITHER side now REJECTS the trade
    # outright. Previously we clamped to min_bet, which produced positions
    # that were inherently unexitable (one leg has no buyers → can't sell).
    # On a $100 bankroll where every dollar of capital lockup costs us, we
    # only enter arbs where both sides have a real takeable bid book.
    depth_fraction = float(cfg.get("book_depth_fraction", 0.25))
    yes_bid_depth = float(opportunity["buy_yes"].get("yes_bid_depth_usd", 0) or 0)
    no_bid_depth = float(opportunity["buy_no"].get("no_bid_depth_usd", 0) or 0)
    yes_bid = float(opportunity["buy_yes"].get("yes_bid", 0) or 0)
    no_bid = float(opportunity["buy_no"].get("no_bid", 0) or 0)
    if yes_bid_depth <= 0 or no_bid_depth <= 0 or yes_bid <= 0 or no_bid <= 0:
        log.warning(
            "size_position rejected: missing bid depth (yes_dep=$%.2f no_dep=$%.2f "
            "yes_bid=%.4f no_bid=%.4f) — position would be unexitable",
            yes_bid_depth, no_bid_depth, yes_bid, no_bid,
        )
        return _reject_sizing(opportunity, "missing_bid_depth")

    # Tier-1 profit-ideas backlog #2: reject thin sub-outcome arbs at entry.
    # Multi-outcome event UIs inflate "liquidity" at the parent-event level —
    # a sub-outcome can show $5 of bid depth even when the parent event shows
    # $5000. We already cap by `book_depth_fraction × bid_depth`, but a $5
    # bid book × 25% × min_bet floor still produces a position we can't
    # actually unwind cleanly. Hard floor: reject any arb where either leg's
    # bid depth is below `min_bid_depth_usd` (default $20).
    min_bid_depth = float(cfg.get("min_bid_depth_usd", 20))
    if yes_bid_depth < min_bid_depth or no_bid_depth < min_bid_depth:
        log.info(
            "size_position rejected: thin bid book "
            "(yes_dep=$%.2f no_dep=$%.2f, floor=$%.2f) — sub-outcome "
            "liquidity won't support clean unwind",
            yes_bid_depth, no_bid_depth, min_bid_depth,
        )
        return _reject_sizing(opportunity, "thin_bid_depth")

    max_yes_unwind_usd = yes_bid_depth * depth_fraction
    max_no_unwind_usd = no_bid_depth * depth_fraction
    max_contracts_yes_depth = max_yes_unwind_usd / yes_bid
    max_contracts_no_depth = max_no_unwind_usd / no_bid
    max_contracts_depth = min(max_contracts_yes_depth, max_contracts_no_depth)
    book_depth_cap = max_contracts_depth * cost_per_contract

    # Apply all caps. Note: NO min_bet floor here — if computed stake is
    # below min_bet, the caller should drop the opportunity entirely
    # (the previous floor silently up-sized past safety caps).
    total_stake = min(
        kelly_stake, bankroll_cap, liquidity_cap, book_depth_cap, cfg["max_bet"],
    )
    if total_stake < float(cfg.get("min_bet", 5)):
        log.info(
            "size_position rejected: total_stake $%.2f < min_bet $%.2f "
            "(limiting cap was %s)",
            total_stake, cfg["min_bet"],
            _find_limiting_rule(kelly_stake, bankroll_cap, liquidity_cap,
                                book_depth_cap, cfg["max_bet"]),
        )
        return _reject_sizing(opportunity, "below_min_bet")

    # Translate total stake to contracts, then split into legs.
    # CRITICAL: round DOWN to whole contracts because Kalshi (and Polymarket
    # in live mode) only trades integer contracts. Predicted P&L must match
    # what live execution will actually realize, otherwise paper-mode
    # systematically overstates profit by ~7% (the fractional remainder).
    n_contracts_raw = total_stake / cost_per_contract
    import math
    n_contracts = float(math.floor(n_contracts_raw))
    if n_contracts < 1:
        log.info(
            "size_position rejected: total_stake $%.2f / cpc $%.4f = %.3f contracts "
            "(<1 whole contract)", total_stake, cost_per_contract, n_contracts_raw,
        )
        return _reject_sizing(opportunity, "below_one_contract")
    # Recompute total_stake from integer contracts so all downstream math
    # reflects the actual amount committed.
    total_stake = n_contracts * cost_per_contract
    yes_leg_usd = round(n_contracts * yes_price, 2)
    no_leg_usd = round(n_contracts * no_price, 2)
    guaranteed_payout = round(n_contracts, 2)
    gross_profit = n_contracts * (1 - cost_per_contract)

    # Subtract fees (worst-case) to get net profit
    fee_cfg = cfg.get("fees", {})
    fees = compute_arb_fees(opportunity["buy_yes"], opportunity["buy_no"], n_contracts, fee_cfg)
    net_profit = round(gross_profit - fees["worst_case_total"], 2)
    net_profit_pct = round(net_profit / total_stake, 4) if total_stake > 0 else 0.0

    limiting_rule = _find_limiting_rule(
        kelly_stake, bankroll_cap, liquidity_cap, book_depth_cap, cfg["max_bet"],
    )

    return {
        "bet_size": round(total_stake, 2),  # total $ committed across both legs
        "contracts": round(n_contracts, 2),
        "leg_yes": {
            "platform": opportunity["buy_yes"]["platform"],
            "usd": yes_leg_usd,
            "contracts": round(n_contracts, 2),
        },
        "leg_no": {
            "platform": opportunity["buy_no"]["platform"],
            "usd": no_leg_usd,
            "contracts": round(n_contracts, 2),
        },
        "guaranteed_payout": guaranteed_payout,
        "gross_profit": round(gross_profit, 2),
        "net_profit": net_profit,
        "net_profit_pct": net_profit_pct,
        "fees": fees,
        "kelly_raw": round(kelly_raw, 4),
        "limiting_rule": limiting_rule,
        "sizing_caps": {
            "kelly_fractional": round(kelly_stake, 2),
            "bankroll_pct": round(bankroll_cap, 2),
            "liquidity_pct": round(liquidity_cap, 2),
            "book_depth": round(book_depth_cap, 2),
            "max_bet": cfg["max_bet"],
        },
    }


def _find_limiting_rule(
    kelly: float, bankroll: float, liquidity: float,
    book_depth: float, max_bet: float,
) -> str:
    caps = {
        "kelly_fractional": kelly,
        "bankroll_pct": bankroll,
        "liquidity_pct": liquidity,
        "book_depth": book_depth,
        "max_bet": max_bet,
    }
    return min(caps, key=lambda k: caps[k])


def _reject_sizing(opportunity: dict, reason: str) -> dict:
    """Return a zero-size sizing dict so the caller can detect rejection
    via `bet_size == 0` and skip the opportunity rather than silently
    proceeding with a corrupt position size."""
    return {
        "bet_size": 0.0,
        "contracts": 0.0,
        "leg_yes": {
            "platform": opportunity.get("buy_yes", {}).get("platform", ""),
            "usd": 0.0, "contracts": 0.0,
        },
        "leg_no": {
            "platform": opportunity.get("buy_no", {}).get("platform", ""),
            "usd": 0.0, "contracts": 0.0,
        },
        "guaranteed_payout": 0.0,
        "gross_profit": 0.0,
        "net_profit": 0.0,
        "net_profit_pct": 0.0,
        "fees": {"worst_case_total": 0.0, "entry_total": 0.0,
                 "entry_yes": 0.0, "entry_no": 0.0},
        "kelly_raw": 0.0,
        "limiting_rule": f"REJECTED:{reason}",
        "sizing_caps": {},
    }
