"""
Promotions sizing rules.

Sportsbook promotions change the EV calculation significantly:
- free_bet: stake is NOT returned on a win. Optimal play = hedge the NO side
  to lock in profit regardless of outcome.
- deposit_match: bonus funds with rollover — treat like reduced free-bet value.
- odds_boost: enhanced payout on a specific outcome — usually best played straight.
"""
from typing import List, Dict, Optional

def calculate_free_bet_arb(
    free_bet_amount: float,
    yes_price: float,
    no_price_hedge: float,
) -> Optional[Dict]:
    """
    Free bet SNR (stake not returned):
    - If YES wins: profit = free_bet * (1/yes_price - 1)
    - If NO wins: profit = 0 (stake was free, no loss)
    Hedge the NO side for `no_stake` to lock in guaranteed profit.
    """
    if yes_price <= 0 or no_price_hedge <= 0 or yes_price >= 1:
        return None

    win_profit = free_bet_amount * (1.0 / yes_price - 1.0)
    no_stake = win_profit * no_price_hedge
    locked_profit = win_profit - no_stake

    if locked_profit <= 0:
        return None

    return {
        "free_bet_size": free_bet_amount,
        "no_hedge_size": round(no_stake, 2),
        "locked_profit": round(locked_profit, 2),
        "profit_pct": round(locked_profit / free_bet_amount, 4),
    }

def apply_active_promos(opportunities: List[dict], promos: List[dict]) -> List[dict]:
    for opp in opportunities:
        opp["promos"] = []
        for promo in promos:
            if promo.get("platform") != opp["buy_yes"]["platform"]:
                continue
            promo_type = promo.get("type", "")
            if promo_type == "free_bet":
                result = calculate_free_bet_arb(
                    promo.get("amount", 0),
                    opp["buy_yes"]["yes_price"],
                    opp["buy_no"]["no_price"],
                )
                if result:
                    opp["promos"].append({"label": promo.get("label", ""), **result})
    return opportunities
