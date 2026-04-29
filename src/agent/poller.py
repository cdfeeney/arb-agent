import asyncio
import logging
import os
import time
from src.clients.kalshi import KalshiClient
from src.clients.polymarket import PolymarketClient
from src.clients.btc_feed import BTCFeed
from src.engine.normalizer import normalize_kalshi, normalize_polymarket
from src.engine.matcher import match_markets, filter_binary_kalshi
from src.engine.arb_detector import detect_arb
from src.engine.sizing import size_position
from src.engine.llm_verifier import LLMVerifier
from src.engine import lag_detector
from src.agent.resolver import resolve_pending
from src.promotions.tracker import apply_active_promos
from src.alerts.notifier import alert_terminal, alert_sms
from src.db.store import Database

log = logging.getLogger(__name__)

class PollingAgent:
    def __init__(self, config: dict, db: Database):
        self.cfg = config
        self.db = db
        self.kalshi = KalshiClient(
            api_key_id=config["kalshi"]["api_key_id"],
            private_key_path=config["kalshi"]["private_key_path"],
            rate_limit_per_min=config["kalshi"]["rate_limit_per_min"],
        )
        self.poly = PolymarketClient(
            rate_limit_per_min=config["polymarket"]["rate_limit_per_min"],
        )
        self.verifier = self._build_verifier()
        self.lag_cfg = lag_detector.LagConfig.from_dict(config.get("lag", {}))
        self.btc_feed: BTCFeed | None = None
        if self.lag_cfg.enabled:
            feed_cfg = config.get("lag", {}).get("feed", {})
            self.btc_feed = BTCFeed(
                source=feed_cfg.get("source", "coinbase"),
                symbol=feed_cfg.get("symbol", "BTC-USD"),
                reconnect_seconds=float(feed_cfg.get("reconnect_seconds", 5)),
                binance_us_endpoint=bool(feed_cfg.get("binance_us_endpoint", False)),
            )

    def _build_verifier(self) -> LLMVerifier | None:
        llm_cfg = self.cfg.get("llm", {})
        if not llm_cfg.get("enabled"):
            return None
        api_key = os.environ.get(llm_cfg.get("api_key_env", "ANTHROPIC_API_KEY"), "")
        if not api_key:
            log.warning("LLM enabled but %s not set — skipping verifier", llm_cfg.get("api_key_env"))
            return None
        return LLMVerifier(
            db=self.db,
            api_key=api_key,
            model=llm_cfg.get("model", "claude-haiku-4-5-20251001"),
            cache_hours=int(llm_cfg.get("cache_hours", 24)),
        )

    async def run(self):
        mode = "DRY RUN" if self.cfg.get("dry_run") else "LIVE"
        log.info("Arb agent started [%s] — polling every %ds", mode, self.cfg["polling"]["interval_seconds"])
        last_resolve = 0.0
        resolve_interval = float(self.cfg.get("polling", {}).get("resolve_interval_seconds", 3600))
        while True:
            try:
                await self._poll_once()
            except Exception as e:
                log.error("Poll cycle error: %s", e, exc_info=True)
            now = time.monotonic()
            if now - last_resolve >= resolve_interval:
                try:
                    await resolve_pending(self.db, self.kalshi)
                except Exception as e:
                    log.error("Resolver error: %s", e, exc_info=True)
                last_resolve = now
            await asyncio.sleep(self.cfg["polling"]["interval_seconds"])

    async def _poll_once(self):
        flt = self.cfg["filters"]
        kalshi_raw, poly_raw = await asyncio.gather(
            self.kalshi.fetch_markets(
                max_days_to_close=flt["max_days_to_close"],
                min_hours_to_close=flt["min_hours_to_close"],
                categories=self.cfg.get("kalshi", {}).get("categories"),
            ),
            self.poly.fetch_markets(
                max_days_to_close=flt["max_days_to_close"],
                min_volume=flt["min_volume"],
            ),
            return_exceptions=True,
        )

        if isinstance(kalshi_raw, Exception):
            log.warning("Kalshi fetch failed: %s", kalshi_raw)
            kalshi_raw = []
        if isinstance(poly_raw, Exception):
            log.warning("Polymarket fetch failed: %s", poly_raw)
            poly_raw = []

        if kalshi_raw and not getattr(self, "_debug_dumped", False):
            with_prices = [m for m in kalshi_raw if m.get("yes_bid") or m.get("yes_ask") or m.get("last_price")]
            log.info(
                "DEBUG Kalshi: %d/%d markets have any price field. First market keys: %s",
                len(with_prices), len(kalshi_raw), sorted(kalshi_raw[0].keys()),
            )
            if with_prices:
                m = with_prices[0]
                log.info(
                    "DEBUG first PRICED Kalshi market — ticker=%s title=%s yes_bid=%s yes_ask=%s last=%s vol=%s status=%s",
                    m.get("ticker"), (m.get("title") or "")[:60],
                    m.get("yes_bid"), m.get("yes_ask"), m.get("last_price"),
                    m.get("volume"), m.get("status"),
                )
            else:
                log.info("DEBUG no priced markets — first raw market dump: %s", kalshi_raw[0])
            self._debug_dumped = True
        k_normalized = [m for m in (normalize_kalshi(r) for r in kalshi_raw) if m]
        k_binary = filter_binary_kalshi(k_normalized)
        k_markets = [m for m in k_binary if m["volume"] >= flt["min_volume"]]
        p_normalized = [m for m in (normalize_polymarket(r) for r in poly_raw) if m]
        p_markets = [m for m in p_normalized if m["volume"] >= flt["min_volume"]]
        log.info(
            "Pipeline: Kalshi raw=%d normalized=%d binary=%d after_vol=%d | Poly raw=%d normalized=%d after_vol=%d",
            len(kalshi_raw), len(k_normalized), len(k_binary), len(k_markets),
            len(poly_raw), len(p_normalized), len(p_markets),
        )

        # Lag detection runs alongside arb detection on the same Kalshi
        # market snapshot. Failures here must not abort the arb cycle.
        await self._run_lag_detection(k_markets)

        pairs = match_markets(
            k_markets,
            p_markets,
            similarity_threshold=self.cfg["matching"]["similarity_threshold"],
            expiry_proximity_hours=self.cfg["matching"]["expiry_proximity_hours"],
        )

        verified_pairs = await self._verify_pairs(pairs)
        verified_pairs = await self._refresh_polymarket_clob(verified_pairs)

        raw_opps = []
        for a, b in verified_pairs:
            opp = detect_arb(
                a, b,
                threshold=1.0 - flt["min_profit_pct"],
                min_hours_to_close=flt["min_hours_to_close"],
            )
            if opp:
                raw_opps.append(opp)

        opportunities = apply_active_promos(raw_opps, self.cfg["promotions"]["active"])

        alerted = 0
        for opp in opportunities:
            if await self.db.seen_recently(opp["pair_id"], self.cfg["alerts"]["dedup_window_minutes"]):
                continue
            sizing = size_position(opp, {**self.cfg["sizing"], "fees": self.cfg.get("fees", {})})
            if sizing["bet_size"] < self.cfg["sizing"]["min_bet"]:
                continue
            dry = self.cfg.get("dry_run", True)
            alert_terminal(opp, sizing, dry_run=dry)
            await alert_sms(
                opp, sizing,
                to=self.cfg["alerts"]["sms_to"],
                from_=self.cfg["alerts"]["sms_from"],
                dry_run=dry,
            )
            await self.db.save_opportunity(opp, sizing)
            paper_id = await self.db.save_paper_trade(opp, sizing)
            log.info("Paper trade #%d recorded (pair=%s edge=%.2f%% predicted=$%.2f)",
                     paper_id, opp["pair_id"], opp["profit_pct"]*100, sizing["net_profit"])
            if dry:
                log.info("[DRY RUN] Would place orders — skipping execution")
            else:
                pass  # order execution goes here in Phase 3
            alerted += 1

        log.info(
            "Poll done — Kalshi:%d Poly:%d pairs:%d verified:%d opps:%d alerted:%d",
            len(k_markets), len(p_markets), len(pairs), len(verified_pairs), len(raw_opps), alerted,
        )

    async def _refresh_polymarket_clob(self, pairs):
        """Replace Polymarket Gamma prices with live CLOB ask prices.

        Gamma's bestBid/bestAsk lag the order book by minutes-to-hours
        (observed 16¢ discrepancy on Juventus). Without this, half our
        'arbs' are phantoms based on stale displayed prices. Run this AFTER
        LLM verification so we only burn CLOB calls on real candidates.
        """
        async def refresh_market(m: dict) -> dict:
            if m.get("platform") != "polymarket":
                return m
            yes_book = await self.poly.fetch_clob_book(m.get("yes_token") or "")
            no_book  = await self.poly.fetch_clob_book(m.get("no_token") or "")
            yes_ask, yes_ask_size = self.poly.best_ask_from_book(yes_book)
            no_ask,  no_ask_size  = self.poly.best_ask_from_book(no_book)
            if yes_ask <= 0 or no_ask <= 0:
                # CLOB unavailable — keep Gamma price but flag low confidence
                return m
            return {
                **m,
                "yes_price": round(yes_ask, 4),
                "no_price": round(no_ask, 4),
                "yes_ask_depth_usd": round(yes_ask_size * yes_ask, 2),
                "no_ask_depth_usd": round(no_ask_size * no_ask, 2),
                "_clob_refreshed": True,
            }

        # Each pair has unique markets; refresh by ticker to dedupe the work
        unique_markets: dict[str, dict] = {}
        for a, b in pairs:
            for m in (a, b):
                if m.get("platform") == "polymarket":
                    unique_markets.setdefault(m["ticker"], m)
        if not unique_markets:
            return pairs
        refreshed = await asyncio.gather(*[refresh_market(m) for m in unique_markets.values()])
        by_ticker = {m["ticker"]: m for m in refreshed}

        rebuilt = []
        for a, b in pairs:
            a2 = by_ticker.get(a.get("ticker"), a) if a.get("platform") == "polymarket" else a
            b2 = by_ticker.get(b.get("ticker"), b) if b.get("platform") == "polymarket" else b
            rebuilt.append((a2, b2))
        n_refreshed = sum(1 for m in refreshed if m.get("_clob_refreshed"))
        log.info("CLOB refresh: %d/%d Polymarket legs updated with live order book", n_refreshed, len(unique_markets))
        return rebuilt

    async def _run_lag_detection(self, k_markets: list[dict]) -> None:
        """Scan crypto markets for BTC-vs-market lag signals + observe pending."""
        if not self.lag_cfg.enabled or self.btc_feed is None:
            return
        try:
            crypto = [m for m in k_markets if lag_detector.is_crypto_market(m, self.lag_cfg)]
            if not crypto:
                log.info("Lag: no crypto markets in this cycle")
                return
            observed = await lag_detector.observe_pending_signals(crypto, self.db, self.lag_cfg)
            summary = await lag_detector.scan(crypto, self.btc_feed, self.db, self.lag_cfg)
            log.info(
                "Lag: %d crypto mkts | BTC=%s (%+.2f%%) | signals=%d | "
                "observed_prev=%d | skipped: feed=%d hist=%d btcflat=%d mvd=%d",
                summary["n_markets"],
                summary["btc_price"], summary["btc_pct_change"] or 0.0,
                summary["signals_emitted"], observed,
                summary["skipped_no_feed"], summary["skipped_no_history"],
                summary["skipped_btc_flat"], summary["skipped_market_moved"],
            )
        except Exception as e:
            log.error("Lag detection error: %s", e, exc_info=True)

    async def _verify_pairs(self, pairs):
        """Run LLM verification on fuzzy-matched pairs to filter out lookalikes.

        Returns the subset where the LLM confirmed both markets resolve on the
        same underlying event. If the verifier is disabled, returns pairs as-is.
        Cap is applied per cycle to bound API cost.
        """
        if not self.verifier or not pairs:
            return pairs
        cap = int(self.cfg.get("llm", {}).get("max_pairs_per_cycle", 50))
        verified = []
        for i, (a, b) in enumerate(pairs):
            if i >= cap:
                log.info("LLM cap reached (%d) — %d pairs unverified", cap, len(pairs) - cap)
                break
            ok = await self.verifier.verify(a, b)
            if ok:
                verified.append((a, b))
        return verified
