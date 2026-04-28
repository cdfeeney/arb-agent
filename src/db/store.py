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
            await db.commit()
        log.info(f"Database ready: {self.path}")

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

        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "INSERT INTO opportunities (pair_id, profit_pct, bet_size, data) VALUES (?,?,?,?)",
                (opp["pair_id"], opp["profit_pct"], sizing["bet_size"], json.dumps(payload)),
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
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                """INSERT INTO paper_trades (
                    pair_id, closes_at,
                    yes_platform, yes_ticker, yes_question, yes_url,
                    yes_observed_price, yes_size_usd, yes_contracts,
                    no_platform, no_ticker, no_question, no_url,
                    no_observed_price, no_size_usd, no_contracts,
                    edge_gross_pct, implied_sum, fees_estimated_usd,
                    predicted_net_usd, predicted_net_pct, status
                ) VALUES (?,?, ?,?,?,?, ?,?,?, ?,?,?,?, ?,?,?, ?,?,?, ?,?, 'open')""",
                (
                    opp["pair_id"], closes_at.isoformat() if closes_at else None,
                    yes["platform"], yes["ticker"], yes["question"], yes["url"],
                    yes["yes_price"], sizing["leg_yes"]["usd"], sizing["leg_yes"]["contracts"],
                    no["platform"], no["ticker"], no["question"], no["url"],
                    no["no_price"], sizing["leg_no"]["usd"], sizing["leg_no"]["contracts"],
                    opp["profit_pct"], opp["implied_sum"], fees.get("worst_case_total", 0),
                    sizing["net_profit"], sizing["net_profit_pct"],
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
