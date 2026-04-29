"""Live BTC/USD price feed with pluggable source.

Subscribes to a public WebSocket ticker stream and maintains an in-memory ring
buffer of recent ticks. The lag detector queries this buffer per cycle to
compute "BTC price now" vs "BTC price N seconds ago".

Run as a long-lived background asyncio task. Reconnects on disconnect with
exponential backoff. If the feed is stale (no ticks for >max_staleness)
the lookups return None and the lag detector skips that cycle.

Sources:
    - "coinbase" (default): wss://ws-feed.exchange.coinbase.com — works from
      US IPs, no auth, ticker channel pushes on every trade.
    - "binance": wss://stream.binance.com:9443 — geo-blocked from US (HTTP 451).
      Use only on non-US infrastructure or via Binance.US (set
      `binance_us_endpoint: true` to use stream.binance.us).
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import websockets

log = logging.getLogger(__name__)

DEFAULT_BUFFER_SECONDS = 600  # keep 10 minutes of ticks


@dataclass(frozen=True)
class Tick:
    timestamp: float  # epoch seconds (float)
    price: float


class BTCFeed:
    """WebSocket subscriber + ring buffer for a single trading pair.

    Usage:
        feed = BTCFeed(source="coinbase", symbol="BTC-USD")
        asyncio.create_task(feed.run())
        ...
        price_now = feed.latest_price()
        price_60s_ago = feed.price_at(time.time() - 60)
    """

    def __init__(
        self,
        source: str = "coinbase",
        symbol: str = "BTC-USD",
        buffer_seconds: int = DEFAULT_BUFFER_SECONDS,
        reconnect_seconds: float = 5.0,
        binance_us_endpoint: bool = False,
    ):
        self.source = source.lower()
        self.symbol = symbol
        self.buffer_seconds = buffer_seconds
        self.reconnect_seconds = reconnect_seconds
        self.binance_us_endpoint = binance_us_endpoint
        self._ticks: deque[Tick] = deque()
        self._stop = False
        self._last_tick_at: float = 0.0

    # ---------- public ----------

    async def run(self) -> None:
        """Main loop: connect, subscribe, consume, reconnect on failure."""
        attempt = 0
        while not self._stop:
            try:
                url, subscribe_msg, parser = self._dispatch_source()
                log.info("BTCFeed[%s]: connecting to %s", self.source, url)
                async with websockets.connect(
                    url, ping_interval=20, ping_timeout=20,
                ) as ws:
                    if subscribe_msg is not None:
                        await ws.send(json.dumps(subscribe_msg))
                    log.info(
                        "BTCFeed[%s]: connected, streaming %s",
                        self.source, self.symbol,
                    )
                    attempt = 0
                    async for raw in ws:
                        if self._stop:
                            break
                        self._ingest(raw, parser)
            except asyncio.CancelledError:
                log.info("BTCFeed: cancelled")
                raise
            except Exception as e:
                attempt += 1
                backoff = min(self.reconnect_seconds * (2 ** min(attempt, 5)), 60.0)
                log.warning(
                    "BTCFeed[%s]: disconnected (%s); reconnecting in %.1fs",
                    self.source, e, backoff,
                )
                await asyncio.sleep(backoff)

    def stop(self) -> None:
        self._stop = True

    def is_fresh(self, max_staleness_seconds: float = 30.0) -> bool:
        if not self._ticks:
            return False
        return (time.time() - self._last_tick_at) < max_staleness_seconds

    def latest_price(self) -> Optional[float]:
        if not self._ticks:
            return None
        return self._ticks[-1].price

    def price_at(self, target_ts: float) -> Optional[float]:
        """Return the price of the tick closest to (and at or before) target_ts.

        None if the buffer doesn't reach back that far. Linear scan, OK because
        the buffer is bounded.
        """
        if not self._ticks:
            return None
        chosen: Optional[Tick] = None
        for t in self._ticks:
            if t.timestamp <= target_ts:
                chosen = t
            else:
                break
        return chosen.price if chosen else None

    def buffer_summary(self) -> dict:
        if not self._ticks:
            return {"size": 0, "oldest_age_s": None, "freshest_age_s": None}
        now = time.time()
        return {
            "size": len(self._ticks),
            "oldest_age_s": round(now - self._ticks[0].timestamp, 1),
            "freshest_age_s": round(now - self._ticks[-1].timestamp, 1),
            "latest_price": self._ticks[-1].price,
        }

    # ---------- source dispatch + parsers ----------

    def _dispatch_source(self):
        """Returns (url, subscribe_msg_or_None, parser_callable)."""
        if self.source == "coinbase":
            return (
                "wss://ws-feed.exchange.coinbase.com",
                {
                    "type": "subscribe",
                    "channels": [
                        {"name": "ticker", "product_ids": [self.symbol]},
                    ],
                },
                self._parse_coinbase,
            )
        if self.source == "binance":
            host = (
                "stream.binance.us:9443"
                if self.binance_us_endpoint
                else "stream.binance.com:9443"
            )
            stream = f"{self.symbol.lower()}@aggTrade"
            return (
                f"wss://{host}/ws/{stream}",
                None,  # Binance subscribes via URL, no message needed
                self._parse_binance,
            )
        raise ValueError(f"Unknown BTC feed source: {self.source}")

    def _ingest(self, raw, parser) -> None:
        try:
            msg = json.loads(raw)
        except Exception:
            return
        tick = parser(msg)
        if tick is None:
            return
        self._ticks.append(tick)
        self._last_tick_at = tick.timestamp
        self._evict_old(tick.timestamp)

    def _evict_old(self, now: float) -> None:
        cutoff = now - self.buffer_seconds
        while self._ticks and self._ticks[0].timestamp < cutoff:
            self._ticks.popleft()

    @staticmethod
    def _parse_coinbase(msg: dict) -> Optional[Tick]:
        # Subscribed-channel acknowledgements have type="subscriptions"
        if msg.get("type") != "ticker":
            return None
        price_str = msg.get("price")
        time_str = msg.get("time")
        if price_str is None:
            return None
        try:
            price = float(price_str)
        except (TypeError, ValueError):
            return None
        if time_str:
            try:
                # ISO-8601 with trailing Z
                ts = datetime.fromisoformat(time_str.replace("Z", "+00:00")).timestamp()
            except ValueError:
                ts = time.time()
        else:
            ts = time.time()
        return Tick(timestamp=ts, price=price)

    @staticmethod
    def _parse_binance(msg: dict) -> Optional[Tick]:
        # aggTrade payload: {"e":"aggTrade","E":<event_ms>,"s":"BTCUSDT","p":"63215.10",...}
        price_str = msg.get("p")
        event_ms = msg.get("E")
        if price_str is None or event_ms is None:
            return None
        try:
            price = float(price_str)
            ts = float(event_ms) / 1000.0
        except (TypeError, ValueError):
            return None
        return Tick(timestamp=ts, price=price)
