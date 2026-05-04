"""Tests for the structural pair-quality classifier."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.engine.pair_quality import classify_pair_structural


def _legs(
    *,
    yes_neg_risk: bool = False, yes_group: str = "",
    no_neg_risk: bool = False, no_group: str = "",
    yes_closes: datetime | None = None, no_closes: datetime | None = None,
) -> dict:
    base_close = datetime(2026, 6, 1, tzinfo=timezone.utc)
    return {
        "buy_yes": {
            "platform": "polymarket",
            "neg_risk": yes_neg_risk,
            "group_item_title": yes_group,
            "closes_at": yes_closes or base_close,
        },
        "buy_no": {
            "platform": "kalshi",
            "neg_risk": no_neg_risk,
            "group_item_title": no_group,
            "closes_at": no_closes or base_close,
        },
    }


def test_clean_pair_is_good():
    quality, reason = classify_pair_structural(_legs())
    assert quality == "good"
    assert reason == ""


def test_polymarket_yes_neg_risk_sub_rejected():
    """Polymarket leg with negRisk=True + groupItemTitle = sub-outcome.
    Trade #395 bug class."""
    quality, reason = classify_pair_structural(
        _legs(yes_neg_risk=True, yes_group="Trump - USA President"),
    )
    assert quality == "broken_neg_risk_sub"
    assert "Trump" in reason


def test_polymarket_no_neg_risk_sub_rejected():
    """Same check applied to NO leg too."""
    legs = _legs(no_neg_risk=True, no_group="Drake")
    legs["buy_no"]["platform"] = "polymarket"  # the no-leg is poly here
    legs["buy_yes"]["platform"] = "kalshi"
    quality, _ = classify_pair_structural(legs)
    assert quality == "broken_neg_risk_sub"


def test_neg_risk_without_group_title_is_ok():
    """negRisk=True without a groupItemTitle is a TOP-LEVEL multi-outcome
    market (the parent), which CAN legitimately pair against a Kalshi
    binary on the same event. Don't reject."""
    quality, _ = classify_pair_structural(
        _legs(yes_neg_risk=True, yes_group=""),
    )
    assert quality == "good"


def test_close_time_within_tolerance_ok():
    """1-day delta is within 72h tolerance."""
    base = datetime(2026, 6, 1, tzinfo=timezone.utc)
    quality, _ = classify_pair_structural(
        _legs(yes_closes=base, no_closes=base + timedelta(hours=23)),
    )
    assert quality == "good"


def test_close_time_beyond_tolerance_rejected():
    """4-day delta crosses date-bucket boundary."""
    base = datetime(2026, 6, 1, tzinfo=timezone.utc)
    quality, reason = classify_pair_structural(
        _legs(yes_closes=base, no_closes=base + timedelta(days=4)),
    )
    assert quality == "broken_date_bucket"
    assert "delta" in reason


def test_missing_close_times_does_not_reject():
    """If either side has no close-time data, skip the date-bucket check
    rather than treating None as zero. Better to ship a slightly riskier
    pair than reject everything."""
    legs = _legs()
    legs["buy_yes"]["closes_at"] = None
    quality, _ = classify_pair_structural(legs)
    assert quality == "good"
