"""SQLite-backed order state machine.

One row per leg. The same schema and the same writers are used for both
log_only and live modes so that flipping the mode flag changes nothing
about the data model — only what the writer actually does on the wire.

State machine:

    pending ──submit──▶ submitted ──fill──▶ filled
                            │
                            └──partial fill──▶ partial ──fill──▶ filled
                            │
                            └──reject/timeout─▶ cancelled | failed

Idempotency:
    UNIQUE INDEX on idempotency_key. A repeat insert with the same key
    returns the existing row id rather than creating a duplicate. This
    is the safety net against double-submission on retry.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import aiosqlite

log = logging.getLogger(__name__)


_CREATE_ORDERS_SQL = """
CREATE TABLE IF NOT EXISTS orders (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    correlation_id      TEXT NOT NULL,
    paper_trade_id      INTEGER,
    pair_id             TEXT NOT NULL,
    leg                 TEXT NOT NULL,
    platform            TEXT NOT NULL,
    ticker              TEXT NOT NULL,
    side                TEXT NOT NULL,
    order_type          TEXT NOT NULL,
    price_limit         REAL NOT NULL,
    contracts_intended  REAL NOT NULL,
    contracts_filled    REAL DEFAULT 0,
    avg_fill_price      REAL,
    status              TEXT NOT NULL,
    external_order_id   TEXT,
    idempotency_key     TEXT NOT NULL,
    execution_mode      TEXT NOT NULL,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error               TEXT
)
"""

_CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_orders_corr ON orders(correlation_id)",
    "CREATE INDEX IF NOT EXISTS idx_orders_paper ON orders(paper_trade_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_idemp ON orders(idempotency_key)",
    "CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status, created_at)",
]


async def init_orders_schema(db_path: str) -> None:
    """Idempotent — safe to call every startup."""
    async with aiosqlite.connect(db_path) as db:
        await db.execute(_CREATE_ORDERS_SQL)
        for sql in _CREATE_INDEXES:
            await db.execute(sql)
        await db.commit()


async def insert_pending(
    db_path: str,
    plan,                       # OrderPlan — typed at call site
    *,
    correlation_id: str,
    paper_trade_id: int | None,
    pair_id: str,
    execution_mode: str,
) -> int:
    """Insert a row in 'pending' state. Returns its id.

    On duplicate idempotency_key, returns the existing row's id instead of
    raising — that's the whole point of the unique constraint.
    """
    async with aiosqlite.connect(db_path) as db:
        try:
            cur = await db.execute(
                """INSERT INTO orders (
                    correlation_id, paper_trade_id, pair_id, leg, platform,
                    ticker, side, order_type, price_limit, contracts_intended,
                    status, idempotency_key, execution_mode
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    correlation_id, paper_trade_id, pair_id, plan.leg,
                    plan.platform, plan.ticker, plan.side, plan.order_type,
                    plan.price_limit, plan.contracts,
                    "pending", plan.idempotency_key, execution_mode,
                ),
            )
            await db.commit()
            return cur.lastrowid
        except aiosqlite.IntegrityError:
            cur = await db.execute(
                "SELECT id FROM orders WHERE idempotency_key=?",
                (plan.idempotency_key,),
            )
            row = await cur.fetchone()
            if row is not None:
                log.info(
                    "order idempotency hit: key=%s id=%d (skipping duplicate insert)",
                    plan.idempotency_key, row[0],
                )
                return row[0]
            raise


async def update_status(
    db_path: str,
    order_id: int,
    *,
    status: str,
    filled_contracts: float | None = None,
    avg_fill_price: float | None = None,
    external_order_id: str | None = None,
    error: str | None = None,
) -> None:
    sets: list[str] = ["status=?", "updated_at=?"]
    args: list = [status, datetime.now(timezone.utc).isoformat()]
    if filled_contracts is not None:
        sets.append("contracts_filled=?")
        args.append(filled_contracts)
    if avg_fill_price is not None:
        sets.append("avg_fill_price=?")
        args.append(avg_fill_price)
    if external_order_id is not None:
        sets.append("external_order_id=?")
        args.append(external_order_id)
    if error is not None:
        sets.append("error=?")
        args.append(error)
    args.append(order_id)
    async with aiosqlite.connect(db_path) as db:
        await db.execute(
            f"UPDATE orders SET {', '.join(sets)} WHERE id=?", args,
        )
        await db.commit()


async def list_orders_for_paper_trade(db_path: str, paper_trade_id: int) -> list[dict]:
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM orders WHERE paper_trade_id=? ORDER BY id",
            (paper_trade_id,),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]
