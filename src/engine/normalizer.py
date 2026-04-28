import json
import logging
from datetime import datetime, timezone
from typing import Optional
from dateutil import parser as dateutil_parser

log = logging.getLogger(__name__)

def normalize_kalshi(raw: dict) -> Optional[dict]:
    try:
        # New Kalshi schema (~2026): price fields are strings in USD (0.0000 to 1.0000),
        # suffixed with _dollars. Volume is volume_fp (fixed-point string).
        def _f(key: str) -> float:
            v = raw.get(key)
            return float(v) if v not in (None, "") else 0.0

        yes_bid = _f("yes_bid_dollars")
        yes_ask = _f("yes_ask_dollars")
        no_bid = _f("no_bid_dollars")
        no_ask = _f("no_ask_dollars")
        last = _f("last_price_dollars")

        # Detect "junk quote" — bid=0 and ask=1 means no real market (default placeholder).
        # Fall back to last_price if available, else skip.
        def _pick_price(bid: float, ask: float, last_p: float) -> float:
            spread_ok = bid > 0 and ask > 0 and (ask - bid) < 0.5
            if spread_ok:
                return (bid + ask) / 2
            if last_p > 0:
                return last_p
            if 0 < bid:
                return bid
            if 0 < ask < 0.95:  # ignore junk-$1 asks
                return ask
            return 0.0

        yes_price = _pick_price(yes_bid, yes_ask, last)
        if yes_price <= 0:
            return None

        no_price = _pick_price(no_bid, no_ask, max(0.0, 1.0 - last) if last > 0 else 0.0)
        if no_price <= 0:
            no_price = max(0.01, 1.0 - yes_price)

        contracts = _f("volume_fp")
        liquidity_usd = _f("liquidity_dollars")
        mid_price = (yes_price + no_price) / 2 if (yes_price + no_price) > 0 else 0.5
        # Use the larger of historical volume × mid OR live liquidity as our "depth" proxy
        volume_usd = max(contracts * mid_price, liquidity_usd)

        return {
            "platform": "kalshi",
            "ticker": raw.get("ticker", ""),
            "event_ticker": raw.get("event_ticker", ""),
            "question": raw.get("title", ""),
            "yes_sub_title": raw.get("yes_sub_title", "") or "",
            "no_sub_title": raw.get("no_sub_title", "") or "",
            "yes_price": round(yes_price, 4),
            "no_price": round(no_price, 4),
            "volume": round(volume_usd, 2),
            "volume_contracts": contracts,
            "liquidity_usd": round(liquidity_usd, 2),
            "closes_at": _parse_dt(raw.get("close_time")),
            "url": f"https://kalshi.com/markets/{raw.get('ticker', '')}",
        }
    except Exception as e:
        log.debug(f"normalize_kalshi failed: {e}")
        return None


def normalize_polymarket(raw: dict) -> Optional[dict]:
    try:
        prices_raw = raw.get("outcomePrices", "[]")
        if isinstance(prices_raw, str):
            prices = json.loads(prices_raw)
        else:
            prices = prices_raw

        if not prices or len(prices) < 2:
            return None

        yes_price = float(prices[0])
        no_price = float(prices[1])

        if yes_price <= 0 or no_price <= 0:
            return None

        slug = raw.get("slug") or raw.get("id", "")
        return {
            "platform": "polymarket",
            "ticker": str(raw.get("id", "")),
            "question": raw.get("question", ""),
            "yes_price": round(yes_price, 4),
            "no_price": round(no_price, 4),
            "volume": float(raw.get("volume", 0)),
            "closes_at": _parse_dt(raw.get("endDate")),
            "url": f"https://polymarket.com/event/{slug}",
        }
    except Exception as e:
        log.debug(f"normalize_polymarket failed: {e}")
        return None


def _parse_dt(s) -> Optional[datetime]:
    if not s:
        return None
    try:
        dt = dateutil_parser.parse(str(s))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None
