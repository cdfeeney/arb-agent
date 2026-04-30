"""
Per-platform trading fee model.

Used by sizing.py so guaranteed_profit reflects what actually lands in the
account, not just the raw arb edge. Used by position_monitor.py to make
partial-unwind decisions fee-aware.

Both platforms charge ONLY the taker (the side that hits a resting order).
Our arbs always cross the spread to fill instantly, so we are always takers
on entry. If we partially unwind by selling at the bid we're also takers
on exit. Holding to resolution incurs no exit fee — payout is automatic.

Kalshi taker fee (per side, per fill):
  fee = ceil(rate × C × P × (1-P) × 100) / 100   # rounded up to nearest cent
  Standard rate: 0.07. Some categorised contracts have lower rates.

Polymarket taker fee (per side, per fill):
  fee = rate × C × P × (1-P)                       # rounded to 0.00001 USDC
  Rate varies by category (Politics 0.04, Crypto 0.072, Sports 0.03,
  Geopolitics 0, default 0.05). Polymarket gas is paid by the relayer
  for users — gas cost to us = $0.

Fee dollars peak symmetrically around P=0.5: a $0.30 trade pays the same
fee as a $0.70 trade on the same contract count.
"""

from math import ceil


# Polymarket per-category taker rates from docs.polymarket.com/fees.
POLYMARKET_FEE_RATES = {
    "politics": 0.04,
    "finance": 0.04,
    "mentions": 0.04,
    "tech": 0.04,
    "crypto": 0.072,
    "sports": 0.03,
    "economics": 0.05,
    "culture": 0.05,
    "weather": 0.05,
    "other": 0.05,
    "geopolitics": 0.0,
}
POLYMARKET_DEFAULT_RATE = 0.05   # generic / unknown category


def kalshi_taker_fee(contracts: float, price: float, rate: float = 0.07) -> float:
    """Kalshi taker fee in dollars. Round up to nearest cent."""
    if contracts <= 0 or price <= 0 or price >= 1:
        return 0.0
    fee_cents = ceil(rate * contracts * price * (1 - price) * 100)
    return fee_cents / 100


def polymarket_taker_fee(
    contracts: float, price: float, rate: float = POLYMARKET_DEFAULT_RATE,
) -> float:
    """Polymarket taker fee in USDC. Rounded to 5 decimal places."""
    if contracts <= 0 or price <= 0 or price >= 1 or rate <= 0:
        return 0.0
    return round(rate * contracts * price * (1 - price), 5)


def polymarket_rate_for(market: dict, default_rate: float = POLYMARKET_DEFAULT_RATE) -> float:
    """Look up Polymarket taker rate from a normalized market dict."""
    cat = (market.get("category") or "").lower().strip()
    return POLYMARKET_FEE_RATES.get(cat, default_rate)


def _leg_taker_fee(leg: dict, price: float, contracts: float, cfg: dict) -> float:
    """Compute the taker fee for one leg-trade at the given price."""
    if leg["platform"] == "kalshi":
        return kalshi_taker_fee(contracts, price, cfg.get("kalshi_fee_rate", 0.07))
    if leg["platform"] == "polymarket":
        rate = polymarket_rate_for(leg, cfg.get("polymarket_default_rate", POLYMARKET_DEFAULT_RATE))
        return polymarket_taker_fee(contracts, price, rate)
    return 0.0


def compute_arb_fees(buy_yes: dict, buy_no: dict, contracts: float, cfg: dict) -> dict:
    """
    Fee breakdown for a two-leg arb across its full lifecycle.

    Returns:
      entry_total       — fees paid on initial entry, both legs (always paid)
      exit_at_resolution — 0; resolution is fee-free
      worst_case_total  — entry_total. Conservative for sizing because we
                           don't yet know whether we'll unwind early.
      exit_if_unwound_at — function exit_fee(yes_bid, no_bid) → fees we'd
                           pay if we partially-unwind both legs at given
                           bid prices on `contracts` contracts. Use this in
                           the position monitor to gate exits on real net.

    Sizing wants `worst_case_total` to be subtracted from gross profit so
    predicted-net P&L is the after-entry-fee number. Resolution-side
    realized P&L matches that prediction; partial unwinds reduce net by
    the additional exit fee burned per contract sold.
    """
    yes_entry = _leg_taker_fee(buy_yes, buy_yes["yes_price"], contracts, cfg)
    no_entry = _leg_taker_fee(buy_no, buy_no["no_price"], contracts, cfg)
    entry_total = yes_entry + no_entry

    return {
        "entry_yes": round(yes_entry, 4),
        "entry_no": round(no_entry, 4),
        "entry_total": round(entry_total, 4),
        "exit_at_resolution": 0.0,
        "worst_case_total": round(entry_total, 4),
        "always_paid": round(entry_total, 4),  # back-compat name
        "yes_leg_if_wins": round(yes_entry, 4),  # back-compat
        "no_leg_if_wins": round(no_entry, 4),    # back-compat
    }


def compute_unwind_fees(
    buy_yes: dict, buy_no: dict, yes_bid: float, no_bid: float,
    contracts: float, cfg: dict,
) -> float:
    """Fees for a partial unwind: sell `contracts` on each leg at top-of-book bids.

    Used by position_monitor to ensure partial unwinds beat their own fees
    before we commit. Both legs are still takers when selling into bids.
    """
    yes_exit = _leg_taker_fee(buy_yes, yes_bid, contracts, cfg)
    no_exit = _leg_taker_fee(buy_no, no_bid, contracts, cfg)
    return round(yes_exit + no_exit, 4)
