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

from .fees import compute_arb_fees


def size_position(opportunity: dict, cfg: dict) -> dict:
    edge = opportunity["profit_pct"]
    yes_price = opportunity["buy_yes"]["yes_price"]
    no_price = opportunity["buy_no"]["no_price"]
    cost_per_contract = max(yes_price + no_price, 0.01)

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
        liquidity_cap = bankroll_cap  # fallback when volume data missing

    # 4. Book-depth cap — bound entry by the bid-side liquidity we'll need for
    # unwind. Each leg's contract count is capped by `book_depth_fraction` of
    # that side's bid-book USD depth. Convert leg-USD caps back to a total-
    # stake cap via cost_per_contract. This enforces the user's "$10 → $1"
    # principle: take small positions in shallow markets, larger in deep ones.
    depth_fraction = float(cfg.get("book_depth_fraction", 0.25))
    yes_bid_depth = float(opportunity["buy_yes"].get("yes_bid_depth_usd", 0) or 0)
    no_bid_depth = float(opportunity["buy_no"].get("no_bid_depth_usd", 0) or 0)
    if yes_bid_depth > 0 and no_bid_depth > 0:
        max_yes_unwind_usd = yes_bid_depth * depth_fraction
        max_no_unwind_usd = no_bid_depth * depth_fraction
        max_contracts_yes_depth = max_yes_unwind_usd / max(yes_price, 0.01)
        max_contracts_no_depth = max_no_unwind_usd / max(no_price, 0.01)
        max_contracts_depth = min(max_contracts_yes_depth, max_contracts_no_depth)
        book_depth_cap = max_contracts_depth * cost_per_contract
    else:
        # Bid depth unavailable — fall back to bankroll cap so we don't reject
        # the trade entirely. Logged as "depth unknown" via limiting_rule.
        book_depth_cap = bankroll_cap

    # Apply all caps
    total_stake = min(
        kelly_stake, bankroll_cap, liquidity_cap, book_depth_cap, cfg["max_bet"],
    )
    total_stake = max(total_stake, cfg["min_bet"])

    # Translate total stake to contracts, then split into legs
    n_contracts = total_stake / cost_per_contract
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
