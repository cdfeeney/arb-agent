import httpx
import asyncio
import base64
import logging
import os
import time
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from typing import List, Dict, Any

log = logging.getLogger(__name__)

class KalshiClient:
    BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

    def __init__(self, api_key_id: str, private_key_path: str, rate_limit_per_min: int = 60):
        self.api_key_id = api_key_id
        self._private_key = self._load_key(private_key_path)
        # Kalshi /markets has a tighter per-endpoint burst limit than the
        # advertised 200/sec — observed 429s with 3 concurrent. Use 2 in-flight
        # plus a small min-gap between request starts to land at ~20 req/sec
        # sustained, well under any plausible burst cap.
        self._semaphore = asyncio.Semaphore(2)
        self._last_request_at = 0.0
        self._min_gap_sec = 0.05

    def _load_key(self, path: str):
        if not path or not os.path.exists(path):
            raise FileNotFoundError(f"Kalshi private key not found: {path}")
        with open(path, "rb") as f:
            return serialization.load_pem_private_key(f.read(), password=None)

    def _sign(self, timestamp_ms: int, method: str, path: str) -> str:
        message = f"{timestamp_ms}{method}{path}".encode()
        signature = self._private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode()

    def _auth_headers(self, method: str, path: str) -> dict:
        ts = int(time.time() * 1000)
        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": str(ts),
            "KALSHI-ACCESS-SIGNATURE": self._sign(ts, method, path),
            "Content-Type": "application/json",
        }

    async def fetch_markets(
        self,
        limit: int = 200,
        max_per_category: int = 5000,
        max_days_to_close: int = 14,
        min_hours_to_close: int = 24,
        categories: List[str] | None = None,
    ) -> List[Dict[str, Any]]:
        """Fetch open markets, optionally restricted to a list of categories.

        When `categories` is given, each category is fetched in parallel and
        results are unioned + deduplicated by ticker. This dodges the global
        default-sort cap that hides niche segments (e.g. esports markets get
        buried behind political/finance markets in the unfiltered firehose).
        """
        now = int(time.time())
        min_close = now + min_hours_to_close * 3600
        max_close = now + max_days_to_close * 86400

        async with httpx.AsyncClient(timeout=30) as client:
            if categories:
                results = await asyncio.gather(
                    *[
                        self._fetch_one_segment(client, limit, max_per_category, min_close, max_close, cat)
                        for cat in categories
                    ]
                )
                merged: dict[str, dict] = {}
                for batch in results:
                    for m in batch:
                        t = m.get("ticker")
                        if t and t not in merged:
                            merged[t] = m
                markets = list(merged.values())
                log.info(
                    "Kalshi: fetched %d unique markets across %d categories (closing in %dh–%dd)",
                    len(markets), len(categories), min_hours_to_close, max_days_to_close,
                )
                return markets

            # Legacy unfiltered path (single firehose)
            markets = await self._fetch_one_segment(
                client, limit, max_per_category, min_close, max_close, category=None
            )
            log.info(
                "Kalshi: fetched %d markets (closing in %dh–%dd, no category filter)",
                len(markets), min_hours_to_close, max_days_to_close,
            )
            return markets

    async def _fetch_one_segment(
        self,
        client: httpx.AsyncClient,
        limit: int,
        cap: int,
        min_close: int,
        max_close: int,
        category: str | None,
    ) -> List[Dict[str, Any]]:
        path = "/trade-api/v2/markets"
        markets: list[dict] = []
        cursor: str | None = None
        while len(markets) < cap:
            params: dict = {
                "limit": limit,
                "status": "open",
                "min_close_ts": min_close,
                "max_close_ts": max_close,
            }
            if category:
                params["category"] = category
            if cursor:
                params["cursor"] = cursor
            async with self._semaphore:
                # Pace requests so the global rate stays ~20 req/sec
                now = time.monotonic()
                gap = self._min_gap_sec - (now - self._last_request_at)
                if gap > 0:
                    await asyncio.sleep(gap)
                self._last_request_at = time.monotonic()
                resp = await client.get(
                    f"{self.BASE_URL}/markets",
                    params=params,
                    headers=self._auth_headers("GET", path),
                )
            resp.raise_for_status()
            data = resp.json()
            batch = data.get("markets", [])
            markets.extend(batch)
            cursor = data.get("cursor")
            if not cursor or not batch:
                break
        return markets
