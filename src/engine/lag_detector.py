"""Correlated-lag signal detector.

Once per arb cycle, scan crypto-related markets and check whether the
underlying (BTC) has moved significantly while the market price has not.
Emit a signal row when the divergence exceeds thresholds.

This is a *directional* signal, not arbitrage. The signal row is the input
to a future paper-trade evaluator that checks whether the market repriced
within ~60s of detection.

See LAG_DESIGN.md for the v1 scope and signal model.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Sequence

from src.clients.btc_feed import BTCFeed
from src.db.store import Database

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class LagConfig:
    enabled: bool
    underlying: str
    window_seconds: int
    btc_threshold_pct: float
    market_flat_threshold_pp: float
    min_market_volume: float
    ticker_prefixes: tuple[str, ...]
    question_keywords: tuple[str, ...]

    @classmethod
    def from_dict(cls, d: dict) -> "LagConfig":
        det = d.get("detection", {})
        return cls(
            enabled=bool(d.get("enabled", False)),
            underlying=str(d.get("underlying", "BTC")).upper(),
            window_seconds=int(det.get("window_seconds", 60)),
            btc_threshold_pct=float(det.get("btc_threshold_pct", 2.0)),
            market_flat_threshold_pp=float(det.get("market_flat_threshold_pp", 0.5)),
            min_market_volume=float(det.get("min_market_volume", 500)),
            ticker_prefixes=tuple(d.get("ticker_prefixes", ["KXBTC"])),
            question_keywords=tuple(
                k.lower() for k in d.get("question_keywords", ["bitcoin", "btc"])
            ),
        )


def is_crypto_market(market: dict, cfg: LagConfig) -> bool:
    """True if the market references the underlying we're tracking.

    Two paths so we don't miss markets — ticker prefix OR question keyword.
    """
    if market.get("platform") != "kalshi":
        return False  # v1: Kalshi only
    if market.get("volume", 0) < cfg.min_market_volume:
        return False
    ticker = (market.get("event_ticker") or market.get("ticker") or "").upper()
    if any(ticker.startswith(p) for p in cfg.ticker_prefixes):
        return True
    q = (market.get("question") or "").lower()
    return any(k in q for k in cfg.question_keywords)


def _mid(market: dict) -> float | None:
    yes = market.get("yes_price")
    no = market.get("no_price")
    if yes is None or no is None or yes <= 0 or no <= 0:
        return None
    return (yes + no) / 2.0


async def scan(
    crypto_markets: Sequence[dict],
    btc_feed: BTCFeed,
    db: Database,
    cfg: LagConfig,
) -> dict:
    """Run one pass of lag detection.

    Returns a summary dict for logging:
        {n_markets, btc_price, btc_pct_change, signals_emitted, skipped_*}
    """
    summary = {
        "n_markets": len(crypto_markets),
        "btc_price": None,
        "btc_pct_change": None,
        "signals_emitted": 0,
        "skipped_no_feed": 0,
        "skipped_no_history": 0,
        "skipped_btc_flat": 0,
        "skipped_market_moved": 0,
    }

    if not cfg.enabled or not crypto_markets:
        return summary

    if not btc_feed.is_fresh(max_staleness_seconds=cfg.window_seconds):
        log.warning(
            "Lag detector: BTC feed stale (buffer=%s); skipping cycle",
            btc_feed.buffer_summary(),
        )
        summary["skipped_no_feed"] = len(crypto_markets)
        return summary

    now_ts = time.time()
    btc_t1 = btc_feed.latest_price()
    btc_t0 = btc_feed.price_at(now_ts - cfg.window_seconds)
    if btc_t0 is None or btc_t1 is None or btc_t0 <= 0:
        log.warning(
            "Lag detector: insufficient BTC history (need %ds, have %s); skipping",
            cfg.window_seconds, btc_feed.buffer_summary(),
        )
        summary["skipped_no_feed"] = len(crypto_markets)
        return summary

    btc_pct = (btc_t1 - btc_t0) / btc_t0 * 100.0
    summary["btc_price"] = btc_t1
    summary["btc_pct_change"] = round(btc_pct, 3)

    # Snapshot every crypto market's current price into history first — we
    # need this even on cycles that don't emit signals so the next cycle has
    # a t0 to compare against.
    snapshots = []
    for m in crypto_markets:
        mid = _mid(m)
        if mid is None:
            continue
        snapshots.append({
            "platform": m["platform"],
            "ticker": m["ticker"],
            "yes_price": m.get("yes_price"),
            "no_price": m.get("no_price"),
            "mid_price": mid,
        })
    await db.record_market_prices(snapshots)

    # Cheap exit: if BTC barely moved, no point scanning markets.
    if abs(btc_pct) < cfg.btc_threshold_pct:
        summary["skipped_btc_flat"] = len(crypto_markets)
        return summary

    direction = "BUY_YES" if btc_pct > 0 else "BUY_NO"
    target = datetime.now(timezone.utc) - timedelta(seconds=cfg.window_seconds)

    for m in crypto_markets:
        mid_now = _mid(m)
        if mid_now is None:
            continue
        prior = await db.market_price_at_or_before(
            m["platform"], m["ticker"], target,
        )
        if prior is None or prior.get("mid_price") is None:
            summary["skipped_no_history"] += 1
            continue
        mid_t0 = float(prior["mid_price"])
        mid_pp_change = (mid_now - mid_t0) * 100.0  # in percentage points

        # If the market already moved >threshold in the SAME direction as BTC,
        # there's no lag to capture — the price has already responded.
        if direction == "BUY_YES" and mid_pp_change >= cfg.market_flat_threshold_pp:
            summary["skipped_market_moved"] += 1
            continue
        if direction == "BUY_NO" and mid_pp_change <= -cfg.market_flat_threshold_pp:
            summary["skipped_market_moved"] += 1
            continue

        # |market_pp_change| is roughly < flat_threshold (or moving the
        # WRONG way, which is also a signal). Emit.
        signal_strength = abs(btc_pct) / max(abs(mid_pp_change), 0.1)
        signal = {
            "market_platform": m["platform"],
            "market_ticker": m["ticker"],
            "market_event_ticker": m.get("event_ticker"),
            "market_question": m.get("question"),
            "market_url": m.get("url"),
            "market_closes_at": m.get("closes_at"),
            "underlying": cfg.underlying,
            "btc_price_t0": round(btc_t0, 2),
            "btc_price_t1": round(btc_t1, 2),
            "btc_pct_change": round(btc_pct, 3),
            "window_seconds": cfg.window_seconds,
            "market_price_t0": round(mid_t0, 4),
            "market_price_t1": round(mid_now, 4),
            "market_pp_change": round(mid_pp_change, 3),
            "direction": direction,
            "signal_strength": round(signal_strength, 2),
        }
        signal_id = await db.save_lag_signal(signal)
        summary["signals_emitted"] += 1
        log.info(
            "LAG SIGNAL #%d %s %s | BTC %+.2f%% in %ds | market %+.2fpp | "
            "strength=%.1f | %s",
            signal_id, direction, m.get("ticker"),
            btc_pct, cfg.window_seconds, mid_pp_change, signal_strength,
            m.get("url"),
        )

    return summary


async def observe_pending_signals(
    crypto_markets: Sequence[dict],
    db: Database,
    cfg: LagConfig,
) -> int:
    """Fill in t2 observation for previously-emitted signals.

    Looks at signals detected within the last ~5 minutes and updates them with
    the current market mid + whether the market moved in the predicted direction.
    Returns the number of signals updated.
    """
    if not cfg.enabled:
        return 0
    by_ticker = {(m["platform"], m["ticker"]): m for m in crypto_markets}
    updated = 0
    for sig in await db.open_lag_signals(max_age_minutes=5):
        key = (sig["market_platform"], sig["market_ticker"])
        m = by_ticker.get(key)
        if not m:
            continue
        mid_now = _mid(m)
        if mid_now is None:
            continue
        t1 = float(sig["market_price_t1"])
        direction = sig["direction"]
        moved_correct = (
            (direction == "BUY_YES" and (mid_now - t1) * 100.0 >= cfg.market_flat_threshold_pp)
            or
            (direction == "BUY_NO" and (mid_now - t1) * 100.0 <= -cfg.market_flat_threshold_pp)
        )
        # Compute revert seconds if we can parse detected_at
        revert_seconds: int | None = None
        try:
            detected_at = datetime.fromisoformat(sig["detected_at"])
            if detected_at.tzinfo is None:
                detected_at = detected_at.replace(tzinfo=timezone.utc)
            revert_seconds = int(
                (datetime.now(timezone.utc) - detected_at).total_seconds()
            )
        except Exception:
            pass
        await db.update_lag_signal_observation(
            sig["id"], round(mid_now, 4), moved_correct, revert_seconds,
        )
        updated += 1
    return updated
