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

        # CRITICAL: arb pricing must use the ASK side — what you'd actually pay
        # to BUY the contract. Mid-price (bid+ask)/2 is NOT takeable in size on
        # markets with wide spreads (we observed Roma yes spread = 2¢/34¢, mid
        # = 18¢ but ask = 34¢ — the difference between "phantom 6% arb" and
        # "real 19¢ guaranteed loss"). Only fall back to last/bid when ask is
        # missing; never average for executable price.
        def _pick_ask(bid: float, ask: float, last_p: float) -> float:
            # Use the real ask. The only ask we treat as junk is exactly $1.00
            # (Kalshi's placeholder when no one is actually selling). $0.99 is
            # a real, very-tight quote and must be respected — falling back to
            # last_price there creates phantom arbs (we observed Napoli yes_ask
            # = 0.99 being replaced with last 0.85, making a fake 12% edge).
            if 0 < ask < 1.0:
                return ask
            if last_p > 0:            # genuinely no ask quoted; last is the best signal
                return last_p
            if 0 < bid < 1.0:         # last resort
                return bid
            return 0.0

        yes_price = _pick_ask(yes_bid, yes_ask, last)
        if yes_price <= 0:
            return None

        no_price = _pick_ask(no_bid, no_ask, max(0.0, 1.0 - last) if last > 0 else 0.0)
        if no_price <= 0:
            no_price = max(0.01, 1.0 - yes_price)

        contracts = _f("volume_fp")
        liquidity_usd = _f("liquidity_dollars")
        mid_price = (yes_price + no_price) / 2 if (yes_price + no_price) > 0 else 0.5
        # Use the larger of historical volume × mid OR live liquidity as our "depth" proxy
        volume_usd = max(contracts * mid_price, liquidity_usd)

        # Best-ask depth: actual contracts you could buy at the current ask price.
        # This is what bounds atomic-fill arbitrage; once exhausted, you walk the book.
        yes_ask_size = _f("yes_ask_size_fp")
        no_ask_size = _f("no_ask_size_fp")
        yes_ask_depth_usd = yes_ask_size * yes_price
        no_ask_depth_usd  = no_ask_size  * no_price

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
            "yes_ask_depth_usd": round(yes_ask_depth_usd, 2),
            "no_ask_depth_usd": round(no_ask_depth_usd, 2),
            "yes_ask_depth_contracts": yes_ask_size,
            "no_ask_depth_contracts": no_ask_size,
            "closes_at": _parse_dt(raw.get("close_time")),
            "url": f"https://kalshi.com/markets/{raw.get('ticker', '')}",
        }
    except Exception as e:
        log.debug(f"normalize_kalshi failed: {e}")
        return None


def normalize_polymarket(raw: dict) -> Optional[dict]:
    try:
        # Prefer takeable prices (bestAsk for YES, 1-bestBid for NO ask).
        # outcomePrices is the displayed mid; using it for arb math overstates
        # edge by the spread on each side. Fall back to outcomePrices only if
        # bestBid/bestAsk aren't in the response.
        best_bid = raw.get("bestBid")
        best_ask = raw.get("bestAsk")
        if best_bid is not None and best_ask is not None:
            yes_ask = float(best_ask)
            yes_bid = float(best_bid)
            if yes_ask <= 0 or yes_ask >= 1:
                return None
            yes_price = yes_ask                   # to BUY yes
            no_price = max(0.0, 1.0 - yes_bid)    # to BUY no = 1 - yes_bid
        else:
            prices_raw = raw.get("outcomePrices", "[]")
            prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
            if not prices or len(prices) < 2:
                return None
            yes_price = float(prices[0])
            no_price = float(prices[1])

        if yes_price <= 0 or no_price <= 0:
            return None

        slug = raw.get("slug") or raw.get("id", "")
        liq = float(raw.get("liquidity", 0) or 0)
        per_leg_depth = liq / 2 if liq > 0 else 0
        # Capture clobTokenIds so we can re-fetch live order-book prices via CLOB
        # before alerting. Gamma's bestBid/bestAsk are aggregated/stale (we
        # observed Gamma 0.78 vs CLOB 0.94 on Juventus — 16¢ discrepancy that
        # turned a phantom "11% arb" into a 5% guaranteed loss).
        clob_tokens = raw.get("clobTokenIds")
        if isinstance(clob_tokens, str):
            try:
                clob_tokens = json.loads(clob_tokens)
            except Exception:
                clob_tokens = None
        return {
            "platform": "polymarket",
            "ticker": str(raw.get("id", "")),
            "question": raw.get("question", ""),
            "yes_price": round(yes_price, 4),
            "no_price": round(no_price, 4),
            "volume": float(raw.get("volume", 0)),
            "liquidity_usd": round(liq, 2),
            "yes_ask_depth_usd": round(per_leg_depth, 2),
            "no_ask_depth_usd": round(per_leg_depth, 2),
            "yes_token": clob_tokens[0] if clob_tokens and len(clob_tokens) >= 1 else None,
            "no_token":  clob_tokens[1] if clob_tokens and len(clob_tokens) >= 2 else None,
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
