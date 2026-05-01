import httpx
import asyncio
import base64
import logging
import os
import time
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from typing import List, Dict, Any, Optional

log = logging.getLogger(__name__)


def _sort_book(raw: list, descending: bool) -> list[tuple[float, float]]:
    """Normalize Kalshi book entries to (price_dollars, size_contracts) tuples.

    Kalshi historically returned price as cents (int) AND/OR strings; clamp
    to floats and sort. Sizes are always contracts. Drop malformed rows.
    """
    out: list[tuple[float, float]] = []
    for entry in raw or []:
        if not entry:
            continue
        try:
            if isinstance(entry, dict):
                p = float(entry.get("price", 0))
                s = float(entry.get("size", 0))
            else:
                p = float(entry[0])
                s = float(entry[1])
        except (TypeError, ValueError, IndexError, KeyError):
            continue
        # If the book quotes in cents (price > 1), convert to dollars.
        if p > 1.5:
            p /= 100.0
        if p <= 0 or p >= 1.0 or s <= 0:
            continue
        out.append((p, s))
    out.sort(key=lambda x: -x[0] if descending else x[0])
    return out

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

    async def fetch_orderbook(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Fetch the live order book for a Kalshi market.

        Returns dict:
            {
              "yes_bids": [(price, size), ...]  # descending by price
              "yes_asks": [(price, size), ...]  # ascending by price
              "no_bids":  [(price, size), ...]  # descending by price
              "no_asks":  [(price, size), ...]  # ascending by price
            }
        Prices are floats in dollars (0.00 - 1.00).
        Returns None if the market isn't quoting or request fails.

        Used by the position monitor to estimate VWAP unwind price by
        walking the bid book.
        """
        if not ticker:
            return None
        path = f"/trade-api/v2/markets/{ticker}/orderbook"
        url = f"{self.BASE_URL}/markets/{ticker}/orderbook"
        try:
            async with self._semaphore:
                now = time.monotonic()
                gap = self._min_gap_sec - (now - self._last_request_at)
                if gap > 0:
                    await asyncio.sleep(gap)
                self._last_request_at = time.monotonic()
                async with httpx.AsyncClient(timeout=15) as client:
                    resp = await client.get(url, headers=self._auth_headers("GET", path))
            if resp.status_code != 200:
                return None
            payload = resp.json()
            # Kalshi changed the orderbook envelope ~2026: response now wraps
            # in `orderbook_fp` with `yes_dollars`/`no_dollars` arrays of
            # [price_string_in_dollars, size_string] pairs. Old format used
            # `orderbook` with `yes`/`no`. Read whichever the API returned.
            ob = payload.get("orderbook_fp") or payload.get("orderbook") or {}
            yes_raw = ob.get("yes_dollars") or ob.get("yes") or []
            no_raw  = ob.get("no_dollars")  or ob.get("no")  or []
            yes_asks_raw = ob.get("yes_asks_dollars") or ob.get("yes_asks") or []
            no_asks_raw  = ob.get("no_asks_dollars")  or ob.get("no_asks")  or []
            return {
                "yes_bids": _sort_book(yes_raw, descending=True),
                "yes_asks": _sort_book(yes_asks_raw or yes_raw, descending=False),
                "no_bids":  _sort_book(no_raw,  descending=True),
                "no_asks":  _sort_book(no_asks_raw or no_raw,  descending=False),
            }
        except Exception as e:
            log.debug("Kalshi orderbook fetch failed for %s: %s", ticker, e)
            return None

    @staticmethod
    def walk_bids(bids: list[tuple[float, float]], target_contracts: float) -> tuple[float, float]:
        """Walk the bid book to fill `target_contracts`, return (avg_fill_price, filled_contracts).

        bids: list of (price_dollars, size_contracts) sorted descending by price.
        Returns the volume-weighted average price of the fill, plus how many
        contracts we could actually fill (may be less than target if book is thin).
        """
        if target_contracts <= 0 or not bids:
            return 0.0, 0.0
        remaining = target_contracts
        spent = 0.0
        filled = 0.0
        for price, size in bids:
            if remaining <= 0:
                break
            take = min(remaining, size)
            spent += take * price
            filled += take
            remaining -= take
        avg = spent / filled if filled > 0 else 0.0
        return avg, filled

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
