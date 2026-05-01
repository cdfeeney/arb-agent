import asyncio
import logging
import os
import time
from src.clients.kalshi import KalshiClient
from src.clients.polymarket import PolymarketClient
from src.clients.btc_feed import BTCFeed
from src.engine.normalizer import normalize_kalshi, normalize_polymarket
from src.agent.allocator import allocate, compute_free_capital
from src.engine.matcher import match_markets, filter_binary_kalshi
from src.engine.arb_detector import detect_arb
from src.engine.sizing import size_position
from src.engine.llm_verifier import LLMVerifier
from src.engine import lag_detector
from src.engine import position_monitor
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
        self.exit_cfg = position_monitor.ExitConfig.from_dict(config.get("exit", {}))

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
            with_prices = [
                m for m in kalshi_raw
                if m.get("yes_bid_dollars") or m.get("yes_ask_dollars") or m.get("last_price_dollars")
            ]
            log.info(
                "DEBUG Kalshi: %d/%d markets have any price field. First market keys: %s",
                len(with_prices), len(kalshi_raw), sorted(kalshi_raw[0].keys()),
            )
            if with_prices:
                m = with_prices[0]
                log.info(
                    "DEBUG first PRICED Kalshi market — ticker=%s title=%s yes_bid=%s yes_ask=%s last=%s vol=%s status=%s",
                    m.get("ticker"), (m.get("title") or "")[:60],
                    m.get("yes_bid_dollars"), m.get("yes_ask_dollars"), m.get("last_price_dollars"),
                    m.get("volume_fp"), m.get("status"),
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
            anchor_min_shared=int(self.cfg["matching"].get("anchor_min_shared", 3)),
        )

        verified_pairs = await self._verify_pairs(pairs)
        verified_pairs = await self._refresh_polymarket_clob(verified_pairs)
        verified_pairs = await self._fetch_kalshi_books(verified_pairs)

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

        # Phase 1: eligibility — dedup, cooldown, sizing min_bet
        eligible: list[tuple[dict, dict]] = []
        cooldown_minutes = self.exit_cfg.cooldown_minutes
        for opp in opportunities:
            if await self.db.seen_recently(opp["pair_id"], self.cfg["alerts"]["dedup_window_minutes"]):
                continue
            if cooldown_minutes > 0 and await self.db.is_in_cooldown(
                opp["pair_id"], cooldown_minutes,
            ):
                log.info("Skipping %s — in re-entry cooldown (%dmin)",
                         opp["pair_id"][:60], cooldown_minutes)
                continue
            sizing = size_position(opp, {**self.cfg["sizing"], "fees": self.cfg.get("fees", {})})
            if sizing["bet_size"] < self.cfg["sizing"]["min_bet"]:
                continue
            eligible.append((opp, sizing))

        # Phase 2: capacity gate — pick the highest-EV subset that fits the
        # remaining bankroll. Without this we "deploy" more capital on paper
        # than we have, and the predicted P&L is fictional.
        bankroll = float(self.cfg["sizing"].get("bankroll", 100.0))
        free_capital = await compute_free_capital(self.db, bankroll)
        chosen, alloc_stats = allocate(eligible, free_capital, bankroll=bankroll)
        log.info(
            "Allocator: %d eligible, $%.2f free of $%.2f bankroll → picked %d, "
            "deployed $%.2f, skipped capacity=%d diversification=%d",
            alloc_stats["candidates"], alloc_stats["free_capital_start"], bankroll,
            alloc_stats["chosen"], alloc_stats["deployed_this_cycle"],
            alloc_stats["skipped_capacity"], alloc_stats["skipped_diversification"],
        )

        # Phase 3: alert + persist the chosen
        alerted = 0
        for opp, sizing in chosen:
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
            alerted += 1

        # NOTE: position_monitor used to run here at the end of each entry-side
        # cycle (~2-3 min cadence). It was moved to a dedicated 15s hot loop
        # (monitor_loop) so we react to bid-book moves at sub-minute cadence
        # without waiting for the slow market-wide scan. See main.py for the
        # task wiring.

        log.info(
            "Poll done — Kalshi:%d Poly:%d pairs:%d verified:%d opps:%d alerted:%d",
            len(k_markets), len(p_markets), len(pairs), len(verified_pairs), len(raw_opps), alerted,
        )

    async def monitor_loop(self):
        """Hot loop for marking and exiting open positions.

        Runs independently of the entry-side poll cycle. Every
        monitor_interval_seconds we fetch bid books for ALL open positions
        concurrently and re-decide HOLD/WATCH/PARTIAL_UNWIND. The entry
        scan touches 15,000 markets and takes ~2-3 min; arb-converged
        windows on positions we already hold can open and close in 30s.
        Polling positions on the slow cycle systematically misses them.
        """
        polling_cfg = self.cfg.get("polling", {})
        interval = float(polling_cfg.get("monitor_interval_seconds", 15))
        max_concurrent = int(polling_cfg.get("monitor_max_concurrent", 8))
        log.info(
            "Position monitor loop started — interval %.0fs, max %d concurrent book fetches",
            interval, max_concurrent,
        )
        while True:
            try:
                mon = await position_monitor.monitor_open_positions(
                    self.db, self.kalshi, self.poly, self.exit_cfg,
                    dry_run=self.cfg.get("dry_run", True),
                    fee_cfg=self.cfg.get("fees", {}),
                    max_concurrent=max_concurrent,
                )
                if mon["n_open"] > 0:
                    log.info(
                        "Monitor: open=%d marked=%d UNWIND=%d CLOSED=%d WATCH=%d HOLD=%d skipped=%d realized_this_cycle=$%.2f",
                        mon["n_open"], mon["n_marked"],
                        mon["partial_unwinds"], mon["fully_closed"],
                        mon["watches"], mon["holds"], mon["skipped"],
                        mon["realized_this_cycle"],
                    )
            except Exception as e:
                log.error("Monitor loop error: %s", e, exc_info=True)
            await asyncio.sleep(interval)

    async def _refresh_polymarket_clob(self, pairs):
        """Replace Polymarket Gamma prices with live CLOB ask prices.

        Gamma's bestBid/bestAsk lag the order book by minutes-to-hours
        (observed 16¢ discrepancy on Juventus). Without this, half our
        'arbs' are phantoms based on stale displayed prices. Run this AFTER
        LLM verification so we only burn CLOB calls on real candidates.

        Also captures bid-side depth (yes_bid_depth_usd / no_bid_depth_usd)
        on the SAME side as our entry — that's the unwind side later, used
        by the sizing engine to cap bets at a fraction of the bid book so
        positions remain exitable at top-of-book without slippage.
        """
        async def refresh_market(m: dict) -> dict:
            if m.get("platform") != "polymarket":
                return m
            yes_book = await self.poly.fetch_clob_book(m.get("yes_token") or "")
            no_book  = await self.poly.fetch_clob_book(m.get("no_token") or "")
            yes_ask, yes_ask_size = self.poly.best_ask_from_book(yes_book)
            no_ask,  no_ask_size  = self.poly.best_ask_from_book(no_book)
            if yes_ask <= 0 or no_ask <= 0:
                # CLOB unavailable — explicit flag so downstream filtering
                # can drop the pair rather than trusting stale Gamma prices
                # (Gamma has been observed off by 16¢ in our pipeline-lessons
                # memory). Don't return Gamma prices unmodified.
                log.warning(
                    "CLOB unavailable for %s — yes_ask=%.4f no_ask=%.4f; "
                    "marking _clob_refreshed=False so pair is dropped",
                    m.get("ticker", "?"), yes_ask, no_ask,
                )
                return {**m, "_clob_refreshed": False}
            yes_bid, yes_bid_size = self.poly.best_bid_from_book(yes_book)
            no_bid,  no_bid_size  = self.poly.best_bid_from_book(no_book)
            return {
                **m,
                "yes_price": round(yes_ask, 4),
                "no_price": round(no_ask, 4),
                "yes_bid": round(yes_bid, 4) if yes_bid > 0 else 0.0,
                "no_bid": round(no_bid, 4) if no_bid > 0 else 0.0,
                "yes_ask_depth_usd": round(yes_ask_size * yes_ask, 2),
                "no_ask_depth_usd": round(no_ask_size * no_ask, 2),
                "yes_bid_depth_usd": round(yes_bid_size * yes_bid, 2) if yes_bid > 0 else 0.0,
                "no_bid_depth_usd": round(no_bid_size * no_bid, 2) if no_bid > 0 else 0.0,
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
        n_dropped = 0
        for a, b in pairs:
            a2 = by_ticker.get(a.get("ticker"), a) if a.get("platform") == "polymarket" else a
            b2 = by_ticker.get(b.get("ticker"), b) if b.get("platform") == "polymarket" else b
            # Drop the entire pair if either Polymarket leg failed CLOB
            # refresh. We won't trade on stale Gamma prices that the
            # 16¢-stale incident proved we can't trust.
            poly_legs = [
                m for m in (a2, b2) if m.get("platform") == "polymarket"
            ]
            if poly_legs and not all(m.get("_clob_refreshed") for m in poly_legs):
                n_dropped += 1
                continue
            rebuilt.append((a2, b2))
        n_refreshed = sum(1 for m in refreshed if m.get("_clob_refreshed"))
        log.info(
            "CLOB refresh: %d/%d Polymarket legs updated; dropped %d pair(s) "
            "with stale-only Gamma data",
            n_refreshed, len(unique_markets), n_dropped,
        )
        return rebuilt

    async def _fetch_kalshi_books(self, pairs):
        """Attach yes/no bid-depth to Kalshi legs of verified pairs.

        Kalshi's market metadata gives us best ASK price + size (what we'd
        pay to enter), but no bid-side depth. The sizing engine needs bid
        depth on the leg we'll later sell back into to cap entry size at a
        fraction of the unwind book — otherwise we open positions we can't
        exit cleanly. Runs only on verified pairs so we don't burn the
        rate limit on rejected candidates.
        """
        async def fetch_one(m: dict) -> dict:
            if m.get("platform") != "kalshi":
                return m
            book = await self.kalshi.fetch_orderbook(m.get("ticker") or "")
            if not book:
                return m
            yes_bids = book.get("yes_bids", [])
            no_bids = book.get("no_bids", [])
            # Use TOP-OF-BOOK only to match Polymarket's depth definition.
            # Previously Kalshi summed top-3 levels while Polymarket summed
            # top-1; the same field had ~3x different semantics across
            # platforms, systematically undersizing Polymarket arbs.
            yes_bid = yes_bids[0][0] if yes_bids else 0.0
            yes_bid_size = yes_bids[0][1] if yes_bids else 0.0
            no_bid = no_bids[0][0] if no_bids else 0.0
            no_bid_size = no_bids[0][1] if no_bids else 0.0
            return {
                **m,
                "yes_bid": round(yes_bid, 4),
                "no_bid": round(no_bid, 4),
                "yes_bid_depth_usd": round(yes_bid * yes_bid_size, 2),
                "no_bid_depth_usd": round(no_bid * no_bid_size, 2),
                "_kalshi_book_fetched": True,
            }

        # Dedupe by ticker — one market may appear in multiple verified pairs.
        unique: dict[str, dict] = {}
        for a, b in pairs:
            for m in (a, b):
                if m.get("platform") == "kalshi":
                    unique.setdefault(m["ticker"], m)
        if not unique:
            return pairs
        refreshed = await asyncio.gather(*[fetch_one(m) for m in unique.values()])
        by_ticker = {m["ticker"]: m for m in refreshed}

        rebuilt = []
        for a, b in pairs:
            a2 = by_ticker.get(a.get("ticker"), a) if a.get("platform") == "kalshi" else a
            b2 = by_ticker.get(b.get("ticker"), b) if b.get("platform") == "kalshi" else b
            rebuilt.append((a2, b2))
        n_fetched = sum(1 for m in refreshed if m.get("_kalshi_book_fetched"))
        log.info("Kalshi book fetch: %d/%d markets got bid-depth", n_fetched, len(unique))
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

        Calls fire concurrently bounded by the verifier's internal semaphore.
        Cache hits (24h cache_hours by default) return instantly without
        an API round-trip, so cycles after the first are dominated by genuine
        new pairs rather than re-verifying yesterday's matches.
        """
        if not self.verifier or not pairs:
            return pairs
        cap = int(self.cfg.get("llm", {}).get("max_pairs_per_cycle", 50))
        capped = pairs[:cap]
        if len(pairs) > cap:
            log.info("LLM cap reached (%d) — %d pairs unverified", cap, len(pairs) - cap)
        results = await asyncio.gather(
            *[self.verifier.verify(a, b) for a, b in capped],
            return_exceptions=True,
        )
        verified = []
        for (a, b), ok in zip(capped, results):
            if ok is True:  # explicit True only — None/False/Exception → skip
                verified.append((a, b))
        return verified
