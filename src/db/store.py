import aiosqlite
import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

log = logging.getLogger(__name__)

class Database:
    def __init__(self, path: str):
        self.path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)

    async def init(self):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS opportunities (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    pair_id   TEXT NOT NULL,
                    profit_pct REAL,
                    bet_size  REAL,
                    data      TEXT,
                    seen_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_pair_seen ON opportunities(pair_id, seen_at)"
            )
            await db.execute("""
                CREATE TABLE IF NOT EXISTS verifications (
                    pair_id    TEXT PRIMARY KEY,
                    is_match   INTEGER NOT NULL,
                    reasoning  TEXT,
                    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS paper_trades (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    pair_id             TEXT NOT NULL,
                    detected_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    closes_at           TIMESTAMP,

                    yes_platform        TEXT,
                    yes_ticker          TEXT,
                    yes_question        TEXT,
                    yes_url             TEXT,
                    yes_observed_price  REAL,
                    yes_size_usd        REAL,
                    yes_contracts       REAL,

                    no_platform         TEXT,
                    no_ticker           TEXT,
                    no_question         TEXT,
                    no_url              TEXT,
                    no_observed_price   REAL,
                    no_size_usd         REAL,
                    no_contracts        REAL,

                    yes_token           TEXT,
                    no_token            TEXT,

                    edge_gross_pct      REAL,
                    implied_sum         REAL,
                    fees_estimated_usd  REAL,
                    predicted_net_usd   REAL,
                    predicted_net_pct   REAL,

                    -- Filled in by resolver after market closes
                    yes_resolved        INTEGER,
                    no_resolved         INTEGER,
                    realized_payout_usd REAL,
                    realized_profit_usd REAL,
                    resolved_at         TIMESTAMP,

                    status              TEXT DEFAULT 'open'  -- open, resolved, expired, error
                )
            """)
            await db.execute("CREATE INDEX IF NOT EXISTS idx_paper_status ON paper_trades(status, closes_at)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_paper_pair ON paper_trades(pair_id, detected_at)")

            # Lag signals: directional bets driven by underlying price moves
            # (e.g. BTC moves but Kalshi crypto market hasn't repriced yet).
            # See LAG_DESIGN.md for the data flow and signal model.
            await db.execute("""
                CREATE TABLE IF NOT EXISTS lag_signals (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    detected_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                    market_platform     TEXT NOT NULL,
                    market_ticker       TEXT NOT NULL,
                    market_event_ticker TEXT,
                    market_question     TEXT,
                    market_url          TEXT,
                    market_closes_at    TIMESTAMP,

                    underlying          TEXT NOT NULL,
                    btc_price_t0        REAL,
                    btc_price_t1        REAL,
                    btc_pct_change      REAL,
                    window_seconds      INTEGER,

                    market_price_t0     REAL,
                    market_price_t1     REAL,
                    market_pp_change    REAL,

                    direction           TEXT,
                    signal_strength     REAL,

                    market_price_t2     REAL,
                    market_repriced     INTEGER,
                    revert_seconds      INTEGER,

                    status              TEXT DEFAULT 'open'
                )
            """)
            await db.execute("CREATE INDEX IF NOT EXISTS idx_lag_status ON lag_signals(status, detected_at)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_lag_market ON lag_signals(market_ticker, detected_at)")

            # Per-market price history for the lag detector. Stored separately
            # from signals so we have continuous time-series for comparison
            # even on cycles that don't produce a signal.
            await db.execute("""
                CREATE TABLE IF NOT EXISTS market_price_history (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform    TEXT NOT NULL,
                    ticker      TEXT NOT NULL,
                    observed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    yes_price   REAL,
                    no_price    REAL,
                    mid_price   REAL
                )
            """)
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_mph_ticker_time "
                "ON market_price_history(platform, ticker, observed_at)"
            )

            # Mark-to-market history of open paper trades. One row per
            # monitor cycle per open position. Used to backtest exit
            # thresholds and surface live recommendations.
            await db.execute("""
                CREATE TABLE IF NOT EXISTS paper_trade_marks (
                    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
                    paper_trade_id           INTEGER NOT NULL,
                    observed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                    yes_bid_now              REAL,
                    yes_bid_vwap             REAL,
                    yes_bid_fill_contracts   REAL,
                    no_bid_now               REAL,
                    no_bid_vwap              REAL,
                    no_bid_fill_contracts    REAL,

                    cost_basis_usd           REAL,
                    unwind_value_usd         REAL,
                    locked_payout_usd        REAL,
                    mark_to_market_usd       REAL,
                    convergence_ratio        REAL,
                    slippage_pct             REAL,

                    days_held                REAL,
                    days_remaining           REAL,
                    annualized_now_pct       REAL,
                    annualized_to_close_pct  REAL,

                    exit_recommendation      TEXT,
                    decision_reason          TEXT
                )
            """)
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_marks_trade_time "
                "ON paper_trade_marks(paper_trade_id, observed_at)"
            )

            # Track exit cooldowns to prevent re-entry into pairs we just
            # exited (whose own exit may have widened the apparent spread).
            await db.execute("""
                CREATE TABLE IF NOT EXISTS pair_cooldowns (
                    pair_id    TEXT PRIMARY KEY,
                    exited_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    reason     TEXT
                )
            """)

            # Migration: older paper_trades rows were saved before we captured
            # the Polymarket clob token ids. Position monitor needs them to
            # fetch live bid books for the unwind-value calculation. Add the
            # columns if they're missing — old rows get NULL (monitor falls
            # back to "no book available" for them, same behavior as before).
            await self._maybe_add_column(db, "paper_trades", "yes_token", "TEXT")
            await self._maybe_add_column(db, "paper_trades", "no_token", "TEXT")

            # Partial-unwind tracking. Each cycle the monitor may sell a slice
            # of an open position at top-of-book if both legs' top bids sum
            # above cost-per-contract. We track:
            #   contracts_remaining   = how many contracts of the original
            #                            hedge are still open
            #   partial_realized_usd  = cumulative realized profit from
            #                            partial unwinds so far
            # When contracts_remaining hits 0 the trade transitions to
            # status='closed' and realized_profit_usd = partial_realized_usd.
            await self._maybe_add_column(
                db, "paper_trades", "contracts_remaining", "REAL",
            )
            await self._maybe_add_column(
                db, "paper_trades", "partial_realized_usd", "REAL DEFAULT 0",
            )
            # Backfill contracts_remaining for legacy rows: assume the original
            # hedge size still holds (no historical partial unwinds before
            # this feature shipped).
            await db.execute(
                "UPDATE paper_trades SET contracts_remaining = yes_contracts "
                "WHERE contracts_remaining IS NULL"
            )

            # paper_trade_marks: track per-cycle partial unwinds so backtest
            # scripts can replay convergence behavior leg-by-leg.
            await self._maybe_add_column(
                db, "paper_trade_marks", "partial_unwind_size", "REAL",
            )
            await self._maybe_add_column(
                db, "paper_trade_marks", "partial_unwind_realized_usd", "REAL",
            )
            await db.commit()
        log.info(f"Database ready: {self.path}")

    @staticmethod
    async def _maybe_add_column(db, table: str, col: str, sql_type: str) -> None:
        """Idempotently add a column to a table (PRAGMA-checks first)."""
        cur = await db.execute(f"PRAGMA table_info({table})")
        existing = {row[1] for row in await cur.fetchall()}
        if col not in existing:
            await db.execute(f"ALTER TABLE {table} ADD COLUMN {col} {sql_type}")
            log.info("Schema migration: added %s.%s (%s)", table, col, sql_type)

    async def get_verification(self, pair_id: str, ttl_hours: int) -> dict | None:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=ttl_hours)).isoformat()
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                "SELECT is_match, reasoning FROM verifications WHERE pair_id=? AND checked_at>?",
                (pair_id, cutoff),
            )
            row = await cur.fetchone()
            if row is None:
                return None
            return {"is_match": bool(row[0]), "reasoning": row[1]}

    async def save_verification(self, pair_id: str, is_match: bool, reasoning: str):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "INSERT OR REPLACE INTO verifications (pair_id, is_match, reasoning, checked_at) "
                "VALUES (?,?,?,?)",
                (pair_id, 1 if is_match else 0, reasoning, datetime.now(timezone.utc).isoformat()),
            )
            await db.commit()

    async def seen_recently(self, pair_id: str, window_minutes: int = 60) -> bool:
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=window_minutes)).isoformat()
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                "SELECT 1 FROM opportunities WHERE pair_id=? AND seen_at>? LIMIT 1",
                (pair_id, cutoff),
            )
            return await cur.fetchone() is not None

    async def save_opportunity(self, opp: dict, sizing: dict):
        payload = {**opp, **sizing}
        # datetimes aren't JSON-serialisable
        payload.pop("buy_yes", None)
        payload.pop("buy_no", None)
        payload["buy_yes_platform"] = opp["buy_yes"]["platform"]
        payload["buy_yes_url"] = opp["buy_yes"]["url"]
        payload["buy_no_platform"] = opp["buy_no"]["platform"]
        payload["buy_no_url"] = opp["buy_no"]["url"]

        # Pass seen_at explicitly in ISO format so the dedup query in
        # seen_recently() can compare apples-to-apples. SQLite's DEFAULT
        # CURRENT_TIMESTAMP writes "YYYY-MM-DD HH:MM:SS" (space) but the
        # cutoff in seen_recently is Python isoformat "YYYY-MM-DDTHH:MM:SS+TZ"
        # (T-separator). String comparison fails on the separator: " " (0x20)
        # < "T" (0x54), so seen_at > cutoff was always False — broke dedup
        # entirely. We were saving a duplicate row every cycle (this is why
        # 131 of 132 paper trades hit the same Gallego pair).
        seen_at = datetime.now(timezone.utc).isoformat()
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "INSERT INTO opportunities (pair_id, profit_pct, bet_size, data, seen_at)"
                " VALUES (?,?,?,?,?)",
                (opp["pair_id"], opp["profit_pct"], sizing["bet_size"],
                 json.dumps(payload), seen_at),
            )
            await db.commit()

    async def save_paper_trade(self, opp: dict, sizing: dict) -> int:
        """Record a hypothetical fill at observed prices for later P&L tracking."""
        yes = opp["buy_yes"]
        no  = opp["buy_no"]
        fees = sizing.get("fees", {}) or {}
        closes_at = min(
            (yes.get("closes_at") or no.get("closes_at")) or datetime.now(timezone.utc),
            (no.get("closes_at") or yes.get("closes_at")) or datetime.now(timezone.utc),
        )
        # Polymarket markets carry token ids on the normalized dict; Kalshi
        # markets don't (uses the ticker for lookups). Record whichever side
        # is on Polymarket so the position monitor can fetch the live bid
        # book to value our unwind. None for Kalshi legs.
        yes_token = yes.get("yes_token") if yes.get("platform") == "polymarket" else None
        no_token  = no.get("no_token")   if no.get("platform")  == "polymarket" else None

        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                """INSERT INTO paper_trades (
                    pair_id, closes_at,
                    yes_platform, yes_ticker, yes_question, yes_url,
                    yes_observed_price, yes_size_usd, yes_contracts,
                    no_platform, no_ticker, no_question, no_url,
                    no_observed_price, no_size_usd, no_contracts,
                    yes_token, no_token,
                    edge_gross_pct, implied_sum, fees_estimated_usd,
                    predicted_net_usd, predicted_net_pct,
                    contracts_remaining, partial_realized_usd, status
                ) VALUES (?,?, ?,?,?,?, ?,?,?, ?,?,?,?, ?,?,?, ?,?, ?,?,?, ?,?, ?,0, 'open')""",
                (
                    opp["pair_id"], closes_at.isoformat() if closes_at else None,
                    yes["platform"], yes["ticker"], yes["question"], yes["url"],
                    yes["yes_price"], sizing["leg_yes"]["usd"], sizing["leg_yes"]["contracts"],
                    no["platform"], no["ticker"], no["question"], no["url"],
                    no["no_price"], sizing["leg_no"]["usd"], sizing["leg_no"]["contracts"],
                    yes_token, no_token,
                    opp["profit_pct"], opp["implied_sum"], fees.get("worst_case_total", 0),
                    sizing["net_profit"], sizing["net_profit_pct"],
                    sizing["leg_yes"]["contracts"],
                ),
            )
            await db.commit()
            return cur.lastrowid

    async def list_unresolved_paper_trades(self, due_before: datetime | None = None) -> list[dict]:
        """Trades whose markets have already closed but P&L isn't recorded yet."""
        cutoff = (due_before or datetime.now(timezone.utc)).isoformat()
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT * FROM paper_trades WHERE status='open' AND closes_at <= ? ORDER BY closes_at",
                (cutoff,),
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def resolve_paper_trade(
        self,
        trade_id: int,
        yes_resolved: int,
        no_resolved: int,
        realized_payout: float,
        realized_profit: float,
    ):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """UPDATE paper_trades SET
                       yes_resolved=?, no_resolved=?,
                       realized_payout_usd=?, realized_profit_usd=?,
                       resolved_at=?, status='resolved'
                   WHERE id=?""",
                (
                    yes_resolved, no_resolved,
                    round(realized_payout, 4), round(realized_profit, 4),
                    datetime.now(timezone.utc).isoformat(), trade_id,
                ),
            )
            await db.commit()

    async def mark_paper_trade_error(self, trade_id: int, note: str):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "UPDATE paper_trades SET status='error', resolved_at=? WHERE id=?",
                (datetime.now(timezone.utc).isoformat() + " " + note[:200], trade_id),
            )
            await db.commit()

    async def paper_trade_summary(self) -> dict:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                """SELECT status, COUNT(*) as n,
                          SUM(predicted_net_usd) as predicted_pnl,
                          SUM(realized_profit_usd) as realized_pnl
                   FROM paper_trades GROUP BY status"""
            )
            return {r["status"]: dict(r) for r in await cur.fetchall()}

    # ---- Lag-detector helpers ----

    async def record_market_prices(self, snapshots: list[dict]) -> None:
        """Bulk-insert price observations. Each snapshot:
            {platform, ticker, yes_price, no_price, mid_price}
        Called once per arb cycle for every crypto market we track.
        """
        if not snapshots:
            return
        now = datetime.now(timezone.utc).isoformat()
        rows = [
            (
                s["platform"], s["ticker"], now,
                s.get("yes_price"), s.get("no_price"), s.get("mid_price"),
            )
            for s in snapshots
        ]
        async with aiosqlite.connect(self.path) as db:
            await db.executemany(
                "INSERT INTO market_price_history "
                "(platform, ticker, observed_at, yes_price, no_price, mid_price) "
                "VALUES (?,?,?,?,?,?)",
                rows,
            )
            await db.commit()

    async def market_price_at_or_before(
        self, platform: str, ticker: str, target: datetime,
    ) -> dict | None:
        """Most recent price observation for (platform, ticker) at or before target."""
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                """SELECT yes_price, no_price, mid_price, observed_at
                   FROM market_price_history
                   WHERE platform=? AND ticker=? AND observed_at <= ?
                   ORDER BY observed_at DESC LIMIT 1""",
                (platform, ticker, target.isoformat()),
            )
            row = await cur.fetchone()
            return dict(row) if row else None

    async def save_lag_signal(self, signal: dict) -> int:
        """Insert a lag-signal row. Returns the new id."""
        # Same dedup-format trap as save_opportunity: open_lag_signals queries
        # by `detected_at >= cutoff` where cutoff is Python isoformat. Pass
        # detected_at explicitly so they're string-comparable.
        detected_at = datetime.now(timezone.utc).isoformat()
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                """INSERT INTO lag_signals (
                    detected_at,
                    market_platform, market_ticker, market_event_ticker,
                    market_question, market_url, market_closes_at,
                    underlying,
                    btc_price_t0, btc_price_t1, btc_pct_change, window_seconds,
                    market_price_t0, market_price_t1, market_pp_change,
                    direction, signal_strength
                ) VALUES (?, ?,?,?, ?,?,?, ?, ?,?,?,?, ?,?,?, ?,?)""",
                (
                    detected_at,
                    signal["market_platform"], signal["market_ticker"],
                    signal.get("market_event_ticker"),
                    signal.get("market_question"), signal.get("market_url"),
                    signal["market_closes_at"].isoformat()
                        if signal.get("market_closes_at") else None,
                    signal["underlying"],
                    signal.get("btc_price_t0"), signal.get("btc_price_t1"),
                    signal.get("btc_pct_change"), signal.get("window_seconds"),
                    signal.get("market_price_t0"), signal.get("market_price_t1"),
                    signal.get("market_pp_change"),
                    signal.get("direction"), signal.get("signal_strength"),
                ),
            )
            await db.commit()
            return cur.lastrowid

    async def update_lag_signal_observation(
        self, signal_id: int, market_price_t2: float,
        market_repriced: bool, revert_seconds: int | None,
    ) -> None:
        """Fill in resolution fields for a previously-emitted signal."""
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """UPDATE lag_signals SET
                       market_price_t2=?, market_repriced=?,
                       revert_seconds=?, status='observed'
                   WHERE id=? AND status='open'""",
                (market_price_t2, 1 if market_repriced else 0,
                 revert_seconds, signal_id),
            )
            await db.commit()

    # ---- Position-monitor helpers ----

    async def list_open_paper_trades(self) -> list[dict]:
        """All paper trades still in 'open' status (regardless of close time)."""
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT * FROM paper_trades WHERE status='open' ORDER BY detected_at"
            )
            return [dict(r) for r in await cur.fetchall()]

    async def save_paper_trade_mark(self, mark: dict) -> int:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                """INSERT INTO paper_trade_marks (
                    paper_trade_id,
                    yes_bid_now, yes_bid_vwap, yes_bid_fill_contracts,
                    no_bid_now,  no_bid_vwap,  no_bid_fill_contracts,
                    cost_basis_usd, unwind_value_usd, locked_payout_usd,
                    mark_to_market_usd, convergence_ratio, slippage_pct,
                    days_held, days_remaining,
                    annualized_now_pct, annualized_to_close_pct,
                    exit_recommendation, decision_reason,
                    partial_unwind_size, partial_unwind_realized_usd
                ) VALUES (?,
                          ?,?,?, ?,?,?,
                          ?,?,?, ?,?,?,
                          ?,?, ?,?,
                          ?,?,
                          ?,?)""",
                (
                    mark["paper_trade_id"],
                    mark.get("yes_bid_now"), mark.get("yes_bid_vwap"),
                    mark.get("yes_bid_fill_contracts"),
                    mark.get("no_bid_now"), mark.get("no_bid_vwap"),
                    mark.get("no_bid_fill_contracts"),
                    mark.get("cost_basis_usd"), mark.get("unwind_value_usd"),
                    mark.get("locked_payout_usd"),
                    mark.get("mark_to_market_usd"), mark.get("convergence_ratio"),
                    mark.get("slippage_pct"),
                    mark.get("days_held"), mark.get("days_remaining"),
                    mark.get("annualized_now_pct"), mark.get("annualized_to_close_pct"),
                    mark.get("exit_recommendation"), mark.get("decision_reason"),
                    mark.get("partial_unwind_size"),
                    mark.get("partial_unwind_realized_usd"),
                ),
            )
            await db.commit()
            return cur.lastrowid

    async def mark_paper_trade_exited(
        self, trade_id: int, mark_to_market_usd: float, reason: str,
    ) -> None:
        """Move a paper trade to 'exited' status (paper-only — no real sell)."""
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """UPDATE paper_trades SET
                       status='exited',
                       resolved_at=?,
                       realized_profit_usd=?
                   WHERE id=? AND status='open'""",
                (
                    datetime.now(timezone.utc).isoformat(),
                    round(mark_to_market_usd, 4), trade_id,
                ),
            )
            await db.commit()

    async def apply_partial_unwind(
        self,
        trade_id: int,
        unwind_size: float,
        realized_usd: float,
    ) -> dict:
        """Decrement contracts_remaining + accumulate partial realized profit.

        `realized_usd` should already be NET of exit fees on this partial.
        We accumulate into `partial_realized_usd`. When the trade fully
        closes (remaining = 0) we subtract the entry fees that were paid
        once at the start so `realized_profit_usd` reflects the true
        end-to-end net dollars: sum(net partial unwinds) - entry_fees.

        Atomic in a single transaction so concurrent monitor cycles can't
        race the same trade into negative remainders.
        """
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT contracts_remaining, partial_realized_usd, "
                "       fees_estimated_usd "
                "FROM paper_trades WHERE id=? AND status='open'",
                (trade_id,),
            )
            row = await cur.fetchone()
            if row is None:
                return {"contracts_remaining": 0, "partial_realized_usd": 0,
                        "fully_closed": True}
            cur_remaining = float(row["contracts_remaining"] or 0)
            cur_realized = float(row["partial_realized_usd"] or 0)
            entry_fees = float(row["fees_estimated_usd"] or 0)
            new_remaining = max(0.0, cur_remaining - unwind_size)
            new_realized = round(cur_realized + realized_usd, 4)
            fully_closed = new_remaining <= 0.0001  # float tolerance
            if fully_closed:
                final_realized = round(new_realized - entry_fees, 4)
                await db.execute(
                    """UPDATE paper_trades SET
                           contracts_remaining=0,
                           partial_realized_usd=?,
                           realized_profit_usd=?,
                           status='closed',
                           resolved_at=?
                       WHERE id=?""",
                    (new_realized, final_realized,
                     datetime.now(timezone.utc).isoformat(), trade_id),
                )
            else:
                await db.execute(
                    "UPDATE paper_trades SET "
                    "contracts_remaining=?, partial_realized_usd=? "
                    "WHERE id=?",
                    (round(new_remaining, 4), new_realized, trade_id),
                )
            await db.commit()
            return {
                "contracts_remaining": round(new_remaining, 4),
                "partial_realized_usd": new_realized,
                "fully_closed": fully_closed,
            }

    async def add_pair_cooldown(self, pair_id: str, reason: str) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "INSERT OR REPLACE INTO pair_cooldowns (pair_id, exited_at, reason) "
                "VALUES (?, ?, ?)",
                (pair_id, datetime.now(timezone.utc).isoformat(), reason),
            )
            await db.commit()

    async def is_in_cooldown(self, pair_id: str, cooldown_minutes: int) -> bool:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(minutes=cooldown_minutes)
        ).isoformat()
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                "SELECT 1 FROM pair_cooldowns WHERE pair_id=? AND exited_at>? LIMIT 1",
                (pair_id, cutoff),
            )
            return await cur.fetchone() is not None

    # ---- Lag-detector helpers ----

    async def open_lag_signals(self, max_age_minutes: int = 5) -> list[dict]:
        """Recent open lag signals awaiting a t2 observation."""
        cutoff = (
            datetime.now(timezone.utc) - timedelta(minutes=max_age_minutes)
        ).isoformat()
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT * FROM lag_signals WHERE status='open' AND detected_at>=? "
                "ORDER BY detected_at",
                (cutoff,),
            )
            return [dict(r) for r in await cur.fetchall()]
