import asyncio
import logging
import os
from src.clients.kalshi import KalshiClient
from src.clients.polymarket import PolymarketClient
from src.engine.normalizer import normalize_kalshi, normalize_polymarket
from src.engine.matcher import match_markets, filter_binary_kalshi
from src.engine.arb_detector import detect_arb
from src.engine.sizing import size_position
from src.engine.llm_verifier import LLMVerifier
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
        while True:
            try:
                await self._poll_once()
            except Exception as e:
                log.error("Poll cycle error: %s", e, exc_info=True)
            await asyncio.sleep(self.cfg["polling"]["interval_seconds"])

    async def _poll_once(self):
        flt = self.cfg["filters"]
        kalshi_raw, poly_raw = await asyncio.gather(
            self.kalshi.fetch_markets(
                max_days_to_close=flt["max_days_to_close"],
                min_hours_to_close=flt["min_hours_to_close"],
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

        pairs = match_markets(
            k_markets,
            p_markets,
            similarity_threshold=self.cfg["matching"]["similarity_threshold"],
            expiry_proximity_hours=self.cfg["matching"]["expiry_proximity_hours"],
        )

        verified_pairs = await self._verify_pairs(pairs)

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
            if dry:
                log.info("[DRY RUN] Would place orders — skipping execution")
            else:
                pass  # order execution goes here in Phase 3
            alerted += 1

        log.info(
            "Poll done — Kalshi:%d Poly:%d pairs:%d verified:%d opps:%d alerted:%d",
            len(k_markets), len(p_markets), len(pairs), len(verified_pairs), len(raw_opps), alerted,
        )

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
