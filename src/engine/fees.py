"""
Per-platform trading fee model.

Used by sizing.py so guaranteed_profit reflects what actually lands in the
account, not just the raw arb edge.

Kalshi:
  fee = ceil(fee_rate × N × p × (1 - p)) cents per contract trade
  Default fee_rate = 0.07 (Kalshi's standard tier)
  Charged only on the side that wins (when contracts settle to $1)

Polymarket:
  0% trading fees on the CLOB.
  Per-order gas costs on Polygon — typically $0.05–0.20 per order.
"""

from math import ceil


def kalshi_fee_usd(contracts: float, price: float, fee_rate: float = 0.07) -> float:
    if contracts <= 0:
        return 0.0
    fee_cents = ceil(fee_rate * contracts * price * (1 - price) * 100)
    return fee_cents / 100


def polymarket_fee_usd(gas_per_order: float = 0.10) -> float:
    return gas_per_order


def compute_arb_fees(buy_yes: dict, buy_no: dict, contracts: float, cfg: dict) -> dict:
    """
    Returns a fee breakdown for a two-leg arb.

    always_paid       — fees paid regardless of outcome (e.g., Polymarket gas on both orders)
    yes_leg_if_wins   — Kalshi-style fee charged only if the YES leg wins
    no_leg_if_wins    — Kalshi-style fee charged only if the NO leg wins
    worst_case_total  — always_paid + max(yes_leg_if_wins, no_leg_if_wins)
                        Use this for guaranteed-profit calculations (conservative).
    """
    kalshi_rate = cfg.get("kalshi_fee_rate", 0.07)
    poly_gas = cfg.get("polymarket_gas_per_order", 0.10)

    fees = {
        "always_paid": 0.0,
        "yes_leg_if_wins": 0.0,
        "no_leg_if_wins": 0.0,
    }

    # YES leg
    if buy_yes["platform"] == "kalshi":
        fees["yes_leg_if_wins"] = kalshi_fee_usd(contracts, buy_yes["yes_price"], kalshi_rate)
    elif buy_yes["platform"] == "polymarket":
        fees["always_paid"] += polymarket_fee_usd(poly_gas)

    # NO leg
    if buy_no["platform"] == "kalshi":
        fees["no_leg_if_wins"] = kalshi_fee_usd(contracts, buy_no["no_price"], kalshi_rate)
    elif buy_no["platform"] == "polymarket":
        fees["always_paid"] += polymarket_fee_usd(poly_gas)

    fees["worst_case_total"] = round(
        fees["always_paid"] + max(fees["yes_leg_if_wins"], fees["no_leg_if_wins"]),
        2,
    )
    fees["always_paid"] = round(fees["always_paid"], 2)
    fees["yes_leg_if_wins"] = round(fees["yes_leg_if_wins"], 2)
    fees["no_leg_if_wins"] = round(fees["no_leg_if_wins"], 2)
    return fees
