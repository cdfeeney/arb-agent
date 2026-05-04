"""Structural pair-quality classification — defense-in-depth at entry.

The matcher already rejects known-bad pair classes upstream (e.g. commit
0bcef8b for Polymarket negRisk-sub × Kalshi binary). This module re-runs
the same structural checks at entry time as a safety net: if the matcher
ever misses one (regression, new bug class, anchor-tier collision), the
allocator/executor still refuses.

Returns a tuple (quality, reason). 'good' = ship it. Anything else =
reject this opportunity. Quality strings are stable strings stored on
paper_trades.pair_quality so historical bad pairs can be found.
"""

from __future__ import annotations

from typing import Tuple


# Maximum acceptable close-time delta between the two legs. Beyond this
# we have a date-bucket mismatch (one leg resolves materially later than
# the other), even if the question text matches. 3 days is generous —
# tighter would also reject legitimate cross-platform pairs where one
# venue closes at 23:59 ET and the other at 00:00 UTC the next day.
MAX_CLOSE_TIME_DELTA_HOURS = 72.0


def classify_pair_structural(opp: dict) -> Tuple[str, str]:
    """Return (quality_label, reason). 'good' = entry allowed.

    opp is the dict produced by detect_arb — has buy_yes and buy_no, each
    holding the full normalized market with structural flags.
    """
    buy_yes = opp.get("buy_yes") or {}
    buy_no = opp.get("buy_no") or {}

    # Polymarket negRisk sub-outcome on either leg → reject.
    # A market with negRisk=True AND a non-empty groupItemTitle is a
    # sub-outcome of an exclusive basket. Pairing it against a binary
    # on the other platform creates a fake arb (the bug behind trade
    # #395). Matcher rejects this upstream; check both legs here as
    # defense-in-depth.
    for leg, label in [(buy_yes, "yes"), (buy_no, "no")]:
        if (
            leg.get("platform") == "polymarket"
            and leg.get("neg_risk")
            and leg.get("group_item_title")
        ):
            return (
                "broken_neg_risk_sub",
                f"polymarket {label}-leg is negRisk sub-outcome "
                f"(group={leg.get('group_item_title')!r})",
            )

    # Close-time mismatch beyond tolerance → date-bucket risk.
    yes_closes = buy_yes.get("closes_at")
    no_closes = buy_no.get("closes_at")
    if yes_closes and no_closes:
        delta_seconds = abs((yes_closes - no_closes).total_seconds())
        delta_hours = delta_seconds / 3600.0
        if delta_hours > MAX_CLOSE_TIME_DELTA_HOURS:
            return (
                "broken_date_bucket",
                f"close-time delta {delta_hours:.1f}h > "
                f"{MAX_CLOSE_TIME_DELTA_HOURS:.0f}h threshold",
            )

    return ("good", "")
