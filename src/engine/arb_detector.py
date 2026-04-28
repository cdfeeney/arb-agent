from datetime import datetime, timezone
from typing import Optional

def detect_arb(
    market_a: dict,
    market_b: dict,
    threshold: float = 0.95,
    min_hours_to_close: int = 24,
    min_implied_sum: float = 0.70,
) -> Optional[dict]:
    """
    Real cross-platform arbs have implied_sum in roughly 0.90–0.99.
    Anything below ~0.70 means the two markets are not actually the same event —
    just two unrelated long-shots both priced near zero. Filter those out.
    """
    now = datetime.now(timezone.utc)

    # Skip markets closing too soon
    for m in [market_a, market_b]:
        closes = m.get("closes_at")
        if closes:
            hours_left = (closes - now).total_seconds() / 3600
            if hours_left < min_hours_to_close:
                return None

    # Check both bet directions
    for buyer, seller in [(market_a, market_b), (market_b, market_a)]:
        implied_sum = buyer["yes_price"] + seller["no_price"]
        # Real arb window: between min_implied_sum (sanity floor — same event)
        # and threshold (max implied_sum to be a profitable arb)
        if min_implied_sum <= implied_sum < threshold:
            profit_pct = round(1.0 - implied_sum, 4)
            return {
                "profit_pct": profit_pct,
                "implied_sum": round(implied_sum, 4),
                "buy_yes": buyer,
                "buy_no": seller,
                "pair_id": (
                    f"{buyer['platform']}:{buyer['ticker']}"
                    f"|{seller['platform']}:{seller['ticker']}"
                ),
            }
    return None
