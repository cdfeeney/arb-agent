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
        self._semaphore = asyncio.Semaphore(max(1, rate_limit_per_min // 10))

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
        max_markets: int = 2000,
        max_days_to_close: int = 14,
        min_hours_to_close: int = 24,
    ) -> List[Dict[str, Any]]:
        now = int(time.time())
        min_close = now + min_hours_to_close * 3600
        max_close = now + max_days_to_close * 86400
        markets = []
        cursor = None
        path = "/trade-api/v2/markets"
        async with httpx.AsyncClient(timeout=30) as client:
            while len(markets) < max_markets:
                params: dict = {
                    "limit": limit,
                    "status": "open",
                    "min_close_ts": min_close,
                    "max_close_ts": max_close,
                }
                if cursor:
                    params["cursor"] = cursor
                async with self._semaphore:
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
        log.info(f"Kalshi: fetched {len(markets)} markets (closing in {min_hours_to_close}h–{max_days_to_close}d)")
        return markets
