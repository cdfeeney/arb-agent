"""Polymarket US (CFTC-regulated DCM) read-only client.

Built so we can verify whether US prices match the international
clob.polymarket.com prices the bot reads today. If they diverge, every
"arb" we paper-trade today is fictional for a US-resident account because
the user can't actually trade at the international quotes.

Auth pattern (per docs.polymarket.us/api-reference/authentication.md):
  - Ed25519 signature over the canonical string "{timestamp_ms}{METHOD}{path}"
  - Headers: X-PM-Access-Key, X-PM-Timestamp, X-PM-Signature
  - Secret key is base64; first 32 bytes after decode are the raw Ed25519
    private key material.

Read-only endpoints used:
  GET /v1/markets                 — list markets (paginated)
  GET /v1/markets/{slug}/book     — full bid/offer book for one market

Order-placement endpoints exist but are NOT implemented here. We're
read-only until the price-diff in #33 confirms architecture decisions.
"""
from __future__ import annotations

import asyncio
import base64
import logging
import os
import time
from typing import Any, Optional

import httpx
from cryptography.hazmat.primitives.asymmetric import ed25519

log = logging.getLogger(__name__)


class PolymarketUSClient:
    AUTH_URL = "https://api.polymarket.us"
    PUBLIC_URL = "https://gateway.polymarket.us"

    def __init__(
        self,
        key_id: str | None = None,
        secret_key_b64: str | None = None,
        rate_limit_per_min: int = 120,
    ):
        # Lazy: clients can be constructed without credentials so the bot
        # boots fine even if POLYMARKET_US_* env vars aren't set yet.
        # Authenticated calls will raise; unauthenticated reads still work.
        self.key_id = key_id or os.environ.get("POLYMARKET_US_KEY_ID", "")
        secret_b64 = secret_key_b64 or os.environ.get("POLYMARKET_US_SECRET_KEY", "")
        self._private_key: Optional[ed25519.Ed25519PrivateKey] = None
        if secret_b64:
            try:
                raw = base64.b64decode(secret_b64)[:32]
                self._private_key = ed25519.Ed25519PrivateKey.from_private_bytes(raw)
            except Exception as e:
                log.warning("Failed to load Polymarket US secret key: %s", e)
                self._private_key = None
        self._semaphore = asyncio.Semaphore(max(1, rate_limit_per_min // 10))

    @property
    def authenticated(self) -> bool:
        return bool(self.key_id and self._private_key)

    def _auth_headers(self, method: str, path: str) -> dict:
        if not self.authenticated:
            raise RuntimeError(
                "Polymarket US credentials not loaded — set "
                "POLYMARKET_US_KEY_ID and POLYMARKET_US_SECRET_KEY env vars."
            )
        ts = str(int(time.time() * 1000))
        message = f"{ts}{method}{path}".encode()
        sig = base64.b64encode(self._private_key.sign(message)).decode()
        return {
            "X-PM-Access-Key": self.key_id,
            "X-PM-Timestamp": ts,
            "X-PM-Signature": sig,
            "Content-Type": "application/json",
        }

    async def fetch_markets(
        self,
        limit: int = 100,
        active: bool = True,
        closed: bool = False,
        volume_num_min: int | None = None,
        end_date_max: str | None = None,
    ) -> list[dict]:
        """List markets, paginated. Tries public gateway first, then signed
        api host as fallback. The docs label /v1/markets as 'public' but
        empirically gateway has returned empty results so we retry with auth."""
        params_base: dict[str, Any] = {
            "active": str(active).lower(),
            "closed": str(closed).lower(),
        }
        if volume_num_min is not None:
            params_base["volumeNumMin"] = volume_num_min
        if end_date_max is not None:
            params_base["endDateMax"] = end_date_max

        async def _paginate(host: str, signed: bool) -> list[dict]:
            out: list[dict] = []
            offset = 0
            path_base = "/v1/markets"
            async with httpx.AsyncClient(timeout=30) as client:
                while True:
                    params = {**params_base, "limit": limit, "offset": offset}
                    headers = self._auth_headers("GET", path_base) if signed else {}
                    async with self._semaphore:
                        resp = await client.get(
                            f"{host}{path_base}", params=params, headers=headers,
                        )
                    if resp.status_code != 200:
                        log.warning(
                            "Polymarket US fetch_markets %s failed: %d %s",
                            "(auth)" if signed else "(public)",
                            resp.status_code, resp.text[:200],
                        )
                        break
                    data = resp.json()
                    batch = data.get("markets") or []
                    if not batch:
                        break
                    out.extend(batch)
                    if len(batch) < limit:
                        break
                    offset += limit
            return out

        # Public first
        markets = await _paginate(self.PUBLIC_URL, signed=False)
        if not markets and self.authenticated:
            log.info("Polymarket US: public gateway empty, trying signed api host")
            markets = await _paginate(self.AUTH_URL, signed=True)
        log.info("Polymarket US: fetched %d markets", len(markets))
        return markets

    async def fetch_market_book(self, slug: str) -> Optional[dict]:
        """Live order book for one market. Returns:
            {"bids": [{"px": str, "qty": str}, ...],
             "offers": [{"px": str, "qty": str}, ...]}
        bids descending by price, offers ascending. None on error.

        Tries the public gateway first; falls back to the authenticated
        host with signed headers if creds are loaded. The docs are
        ambiguous about whether book queries are public or auth-only —
        try both rather than guess.
        """
        if not slug:
            return None
        path = f"/v1/markets/{slug}/book"
        # Public attempt
        try:
            async with self._semaphore:
                async with httpx.AsyncClient(timeout=15) as client:
                    resp = await client.get(f"{self.PUBLIC_URL}{path}")
            if resp.status_code == 200:
                return self._parse_book(resp.json())
            log.debug(
                "Polymarket US public book fetch failed for %s: %d %s",
                slug, resp.status_code, resp.text[:120],
            )
        except Exception as e:
            log.debug("Polymarket US public book error for %s: %s", slug, e)

        # Authenticated fallback
        if not self.authenticated:
            return None
        try:
            async with self._semaphore:
                async with httpx.AsyncClient(timeout=15) as client:
                    resp = await client.get(
                        f"{self.AUTH_URL}{path}",
                        headers=self._auth_headers("GET", path),
                    )
            if resp.status_code == 200:
                return self._parse_book(resp.json())
            log.debug(
                "Polymarket US auth book fetch failed for %s: %d %s",
                slug, resp.status_code, resp.text[:120],
            )
            return None
        except Exception as e:
            log.debug("Polymarket US auth book error for %s: %s", slug, e)
            return None

    async def search(self, query: str, limit: int = 5) -> list[dict]:
        """Full-text search across US events/markets. Returns event objects;
        each event has a `markets` array with `slug` and `id`. This is how
        we discover what slug US uses for a given question (US slugs differ
        from international slugs even for the same event)."""
        if not query:
            return []
        path = "/v1/search"
        try:
            async with self._semaphore:
                async with httpx.AsyncClient(timeout=15) as client:
                    resp = await client.get(
                        f"{self.PUBLIC_URL}{path}",
                        params={"query": query, "limit": limit},
                    )
            if resp.status_code == 200:
                data = resp.json()
                return data.get("events") or []
            log.debug(
                "Polymarket US search failed for %r: %d %s",
                query, resp.status_code, resp.text[:120],
            )
            return []
        except Exception as e:
            log.debug("Polymarket US search error for %r: %s", query, e)
            return []

    async def fetch_market_bbo(self, slug: str) -> Optional[dict]:
        """Best-bid-offer + depth for one market. Lighter than /book.
        Returns the marketData dict or None on error."""
        if not slug:
            return None
        path = f"/v1/markets/{slug}/bbo"
        try:
            async with self._semaphore:
                async with httpx.AsyncClient(timeout=15) as client:
                    resp = await client.get(f"{self.PUBLIC_URL}{path}")
            if resp.status_code == 200:
                data = resp.json()
                return data.get("marketData") or data
            log.debug(
                "Polymarket US bbo fetch failed for %s: %d %s",
                slug, resp.status_code, resp.text[:120],
            )
            return None
        except Exception as e:
            log.debug("Polymarket US bbo error for %s: %s", slug, e)
            return None

    @staticmethod
    def extract_bid_ask(market_or_bbo: dict) -> tuple[float, float]:
        """Pull (bid, ask) from either a /v1/markets list record OR a
        /v1/markets/{slug}/bbo response. US returns different field names
        in different places — bbo uses bestBid/bestAsk wrapped in
        {value, currency}, while the markets list uses bestBidQuote/
        bestAskQuote with the same wrapper. Returns (0.0, 0.0) if neither
        is present."""
        if not market_or_bbo:
            return 0.0, 0.0

        def _val(field: str) -> float:
            obj = market_or_bbo.get(field)
            if obj is None:
                return 0.0
            if isinstance(obj, dict):
                v = obj.get("value")
            else:
                v = obj
            try:
                return float(v) if v is not None else 0.0
            except (TypeError, ValueError):
                return 0.0

        bid = _val("bestBid") or _val("bestBidQuote")
        ask = _val("bestAsk") or _val("bestAskQuote")
        return bid, ask

    @staticmethod
    def _parse_book(data: dict) -> dict:
        md = data.get("marketData") or data
        bids = sorted(
            md.get("bids") or [],
            key=lambda x: -float(x.get("px", 0)),
        )
        offers = sorted(
            md.get("offers") or [],
            key=lambda x: float(x.get("px", 1)),
        )
        return {"bids": bids, "offers": offers}

    async def whoami(self) -> Optional[dict]:
        """Authenticated identity check. Used to validate credentials are loaded
        and accepted by the server. Returns the response dict or None on error."""
        path = "/v1/accounts/me"
        url = f"{self.AUTH_URL}{path}"
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(url, headers=self._auth_headers("GET", path))
            if resp.status_code == 200:
                return resp.json()
            log.warning(
                "whoami failed: %d %s", resp.status_code, resp.text[:200],
            )
            return None
        except Exception as e:
            log.warning("whoami error: %s", e)
            return None

    @staticmethod
    def best_bid_from_book(book: Optional[dict]) -> tuple[float, float]:
        if not book or not book.get("bids"):
            return 0.0, 0.0
        top = book["bids"][0]
        return float(top.get("px", 0)), float(top.get("qty", 0))

    @staticmethod
    def best_ask_from_book(book: Optional[dict]) -> tuple[float, float]:
        if not book or not book.get("offers"):
            return 0.0, 0.0
        top = book["offers"][0]
        return float(top.get("px", 0)), float(top.get("qty", 0))
