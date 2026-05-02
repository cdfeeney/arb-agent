"""Safety primitives for live execution.

Two kill mechanisms layered on top of execution.allow_send:

1. STOP file (data/STOP) — if present, every executor refuses to place
   real orders regardless of allow_send. One-touch halt without restart.
   Touch to stop (or run `python -m scripts.stop`); delete to resume.
   Idea: an emergency button you can press from any shell or even
   from a webhook handler.

2. Daily live-order cap — DB-backed counter, resets per UTC day. When
   exceeded the gate returns (False, reason) AND auto-creates the
   STOP file so subsequent attempts are blocked even after the counter
   resets at midnight. Prevents runaway behavior on a logic bug from
   draining the account in one cycle.

CRITICAL design points (post code-review hardening):

* The cap is **increment-first, atomic**. `safety_gate` calls
  `incr_live_order_counter` (UPSERT with RETURNING) BEFORE the caller
  sends to the exchange. This makes over-cap impossible regardless of
  concurrency — SQLite serializes the writes and the over-cap branch
  never reaches the exchange. Rejected sends still burn a slot, which
  is the safe direction (a misbehaving bot halting earlier is better
  than later).

* The gate **fails closed**. Any DB error inside `safety_gate` returns
  (False, ...) AND auto-creates the STOP file. Disk-full, locked DB,
  schema drift — none of them silently let live sends through.

* `DEFAULT_STOP_FILE` is anchored to the repo root via __file__, NOT
  resolved against cwd. A systemd unit or cron job with a different
  working directory still hits the same file the agent watches.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import aiosqlite

log = logging.getLogger(__name__)

# Anchor to repo root so the STOP file path is stable across launch
# contexts (cwd, systemd, cron, IDE). __file__ → src/exec/safety.py →
# parents[2] is the repo root.
_REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_STOP_FILE = str(_REPO_ROOT / "data" / "STOP")


# ----- STOP file -----

def _resolve_stop_path(stop_file: str | None) -> str:
    """Late-binds DEFAULT_STOP_FILE so monkey-patching the module
    attribute (in tests, or to swap paths at runtime) actually takes
    effect. Default-argument binding happens at function-def time,
    which would freeze the path."""
    return stop_file if stop_file is not None else DEFAULT_STOP_FILE


def is_stopped(stop_file: str | None = None) -> tuple[bool, str | None]:
    """Returns (stopped, reason). Reason is the file's contents (or a
    generic message if the file isn't readable)."""
    path = _resolve_stop_path(stop_file)
    if not os.path.exists(path):
        return False, None
    try:
        with open(path) as f:
            reason = f.read().strip() or "STOP file present (empty body)"
    except Exception:
        reason = "STOP file present (not readable)"
    return True, reason


def create_stop_file(reason: str, stop_file: str | None = None) -> None:
    path = _resolve_stop_path(stop_file)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        f.write(f"{datetime.now(timezone.utc).isoformat()}\n{reason}\n")
    log.warning("STOP file created at %s: %s", path, reason)


def remove_stop_file(stop_file: str | None = None) -> bool:
    """Returns True if removed, False if it didn't exist."""
    path = _resolve_stop_path(stop_file)
    if os.path.exists(path):
        os.unlink(path)
        log.info("STOP file removed: %s", path)
        return True
    return False


# ----- Daily order cap -----

CREATE_COUNTERS_SQL = """
CREATE TABLE IF NOT EXISTS live_order_counters (
    date  TEXT PRIMARY KEY,        -- 'YYYY-MM-DD' UTC
    count INTEGER NOT NULL DEFAULT 0
)
"""


async def init_safety_schema(db_path: str) -> None:
    async with aiosqlite.connect(db_path) as db:
        await db.execute(CREATE_COUNTERS_SQL)
        await db.commit()


def _today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


async def get_live_order_count_today(db_path: str) -> int:
    async with aiosqlite.connect(db_path) as db:
        cur = await db.execute(
            "SELECT count FROM live_order_counters WHERE date=?", (_today_utc(),),
        )
        row = await cur.fetchone()
        return int(row[0]) if row else 0


async def incr_live_order_counter(db_path: str) -> int:
    """Atomic increment using UPSERT + RETURNING. The returned value is
    the post-increment count for THIS caller — concurrent callers each
    receive a unique monotonically increasing value because SQLite
    serializes writes via the database lock.
    """
    async with aiosqlite.connect(db_path) as db:
        cur = await db.execute(
            "INSERT INTO live_order_counters (date, count) VALUES (?, 1) "
            "ON CONFLICT(date) DO UPDATE SET count=count+1 "
            "RETURNING count",
            (_today_utc(),),
        )
        row = await cur.fetchone()
        await db.commit()
        return int(row[0]) if row else 0


async def safety_gate(
    db_path: str | None,
    max_per_day: int,
    stop_file: str | None = None,
) -> tuple[bool, str | None]:
    """Atomic check-and-consume gate. Call ONCE per real send attempt
    immediately before the exchange POST.

    Order of checks:
      1. STOP file present? → reject, no DB touched.
      2. cap disabled (db_path None or max_per_day<=0)? → allow, no
         counter touched.
      3. Atomic increment via UPSERT-RETURNING. If the post-increment
         value exceeds max_per_day, reject AND auto-create the STOP
         file so the cascade persists past midnight UTC.

    Returns (allowed, reason). When allowed=True the caller has ALREADY
    consumed one slot in the daily cap — there is no separate post-send
    increment step. Rejected sends still burn a slot, which is the
    intended behavior: a bot churning rejections is misbehaving and
    should halt earlier, not later.

    Fails CLOSED on any DB error: returns (False, ...) AND tries to
    create the STOP file so subsequent calls also halt.
    """
    stop_file = _resolve_stop_path(stop_file)
    # 1. STOP file (cheap, no DB)
    try:
        stopped, stop_reason = is_stopped(stop_file)
    except Exception as e:  # pragma: no cover — os.path.exists doesn't really raise
        return False, f"safety_gate stop-check error: {e}"
    if stopped:
        return False, f"STOPPED: {stop_reason}"

    # 2. Cap disabled → allow
    if not db_path or max_per_day <= 0:
        return True, None

    # 3. Atomic increment + cap check
    try:
        new_count = await incr_live_order_counter(db_path)
    except Exception as e:
        # Fail CLOSED. Try to lock further sends out via the STOP file
        # so even if this is transient (locked DB, etc.) operators get
        # a clear signal that something is wrong.
        log.error("safety_gate DB error during increment: %s", e)
        try:
            create_stop_file(f"safety_gate counter error: {e}", stop_file)
        except Exception as e2:  # pragma: no cover — last resort
            log.error("safety_gate could not create STOP file: %s", e2)
        return False, f"safety_gate counter error: {e}"

    if new_count > max_per_day:
        reason = f"daily live order cap exceeded: {new_count}/{max_per_day}"
        if not os.path.exists(stop_file):
            try:
                create_stop_file(reason, stop_file)
                log.error("AUTO-STOP: %s — created %s", reason, stop_file)
            except Exception as e:  # pragma: no cover
                log.error("AUTO-STOP create failed: %s", e)
        return False, reason

    return True, None
