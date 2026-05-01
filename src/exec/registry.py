"""Build the {platform: Exchange} map for the orchestrator.

Reads execution.allow_send + per-platform credentials from env. Whichever
exchanges have valid configuration are included; missing ones are absent
from the map. The orchestrator fails loudly with "no exchange registered
for X" rather than silently using a stub.
"""

from __future__ import annotations

import logging
import os

from .exchange import Exchange

log = logging.getLogger(__name__)


def build_exchange_registry(
    config: dict,
    kalshi_client,
    poly_client,
) -> dict[str, Exchange]:
    exec_cfg = (config.get("execution", {}) or {})
    allow_send = bool(exec_cfg.get("allow_send", False))
    registry: dict[str, Exchange] = {}

    # ---- Kalshi: keys already required by the bot for read access, so a live
    #              KalshiClient existing means we can also write.
    if kalshi_client is not None:
        try:
            from .kalshi_exchange import KalshiExchange
            registry["kalshi"] = KalshiExchange(
                kalshi_client=kalshi_client,
                allow_send=allow_send,
            )
            log.info(
                "Registered KalshiExchange (allow_send=%s)", allow_send,
            )
        except Exception as e:
            log.warning("KalshiExchange not registered: %s", e)

    # ---- Polymarket: needs a wallet private key; without it, refuse to register
    pm_key = os.environ.get("POLYMARKET_PRIVATE_KEY")
    pm_funder = os.environ.get("POLYMARKET_FUNDER")
    if pm_key:
        try:
            from .polymarket_exchange import PolymarketExchange
            registry["polymarket"] = PolymarketExchange(
                poly_client=poly_client,
                private_key=pm_key,
                funder=pm_funder,
                allow_send=allow_send,
            )
            log.info(
                "Registered PolymarketExchange (allow_send=%s, funder=%s)",
                allow_send, "set" if pm_funder else "default",
            )
        except Exception as e:
            log.warning("PolymarketExchange not registered: %s", e)
    else:
        log.warning(
            "PolymarketExchange not registered: POLYMARKET_PRIVATE_KEY env "
            "var not set. Polymarket-side legs cannot trade until this is "
            "set + execution.allow_send=true."
        )

    return registry
