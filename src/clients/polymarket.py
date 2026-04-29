import httpx
import asyncio
import logging
from typing import List, Dict, Any, Optional

log = logging.getLogger(__name__)

class PolymarketClient:
    GAMMA_URL = "https://gamma-api.polymarket.com"
    CLOB_URL = "https://clob.polymarket.com"

    def __init__(self, rate_limit_per_min: int = 120):
        self._semaphore = asyncio.Semaphore(max(1, rate_limit_per_min // 10))
        self._clob_semaphore = asyncio.Semaphore(5)

    async def fetch_clob_book(self, token_id: str) -> Optional[Dict[str, Any]]:
        """Fetch the live order book for a Polymarket binary outcome token.

        Returns dict with 'asks' (sorted ascending — cheapest seller first)
        and 'bids' (sorted descending — highest buyer first), each as
        list of {price: str, size: str}. Returns None on error.

        Use this to get TAKEABLE prices. Gamma's bestBid/bestAsk are stale
        aggregates and have been observed off by 10–20¢ on real markets.
        """
        if not token_id:
            return None
        try:
            async with self._clob_semaphore:
                async with httpx.AsyncClient(timeout=15) as client:
                    resp = await client.get(f"{self.CLOB_URL}/book", params={"token_id": token_id})
            if resp.status_code != 200:
                return None
            data = resp.json()
            asks = sorted(data.get("asks", []) or [], key=lambda x: float(x.get("price", 1)))
            bids = sorted(data.get("bids", []) or [], key=lambda x: -float(x.get("price", 0)))
            return {"asks": asks, "bids": bids}
        except Exception as e:
            log.debug("CLOB book fetch failed for %s: %s", token_id[:20], e)
            return None

    @staticmethod
    def best_ask_from_book(book: Optional[dict]) -> tuple[float, float]:
        """Returns (price, size) of cheapest ask, or (0.0, 0.0) if no asks."""
        if not book or not book.get("asks"):
            return 0.0, 0.0
        top = book["asks"][0]
        return float(top.get("price", 0)), float(top.get("size", 0))

    @staticmethod
    def best_bid_from_book(book: Optional[dict]) -> tuple[float, float]:
        """Returns (price, size) of highest bid, or (0.0, 0.0) if no bids.

        Used by the position monitor to estimate the unwind price.
        """
        if not book or not book.get("bids"):
            return 0.0, 0.0
        top = book["bids"][0]
        return float(top.get("price", 0)), float(top.get("size", 0))

    @staticmethod
    def walk_bids(book: Optional[dict], target_contracts: float) -> tuple[float, float]:
        """Walk the bid book to fill `target_contracts`, return (vwap, filled).

        Returns the volume-weighted average price across however many
        contracts we could fill (may be less than target on a thin book).
        """
        if target_contracts <= 0 or not book or not book.get("bids"):
            return 0.0, 0.0
        remaining = target_contracts
        spent = 0.0
        filled = 0.0
        for entry in book["bids"]:
            if remaining <= 0:
                break
            try:
                price = float(entry.get("price", 0))
                size = float(entry.get("size", 0))
            except (TypeError, ValueError):
                continue
            if price <= 0 or size <= 0:
                continue
            take = min(remaining, size)
            spent += take * price
            filled += take
            remaining -= take
        avg = spent / filled if filled > 0 else 0.0
        return avg, filled

    async def fetch_markets(
        self,
        limit: int = 100,
        max_markets: int = 10000,
        max_days_to_close: int = 14,
        min_volume: int = 5000,
    ) -> List[Dict[str, Any]]:
        from datetime import datetime, timedelta, timezone
        end_max = (datetime.now(timezone.utc) + timedelta(days=max_days_to_close)).isoformat()
        markets = []
        offset = 0
        async with httpx.AsyncClient(timeout=30) as client:
            while len(markets) < max_markets:
                params = {
                    "limit": limit,
                    "offset": offset,
                    "active": "true",
                    "closed": "false",
                    "volume_num_min": min_volume,
                    "end_date_max": end_max,
                }
                async with self._semaphore:
                    resp = await client.get(f"{self.GAMMA_URL}/markets", params=params)
                resp.raise_for_status()
                batch = resp.json()
                if not batch:
                    break
                markets.extend(batch)
                if len(batch) < limit:
                    break
                offset += limit
        log.info(f"Polymarket: fetched {len(markets)} markets (closing within {max_days_to_close}d, vol≥${min_volume})")
        return markets
