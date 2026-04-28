import httpx
import asyncio
import logging
from typing import List, Dict, Any

log = logging.getLogger(__name__)

class PolymarketClient:
    GAMMA_URL = "https://gamma-api.polymarket.com"
    CLOB_URL = "https://clob.polymarket.com"

    def __init__(self, rate_limit_per_min: int = 120):
        self._semaphore = asyncio.Semaphore(max(1, rate_limit_per_min // 10))

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
