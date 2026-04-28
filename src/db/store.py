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
