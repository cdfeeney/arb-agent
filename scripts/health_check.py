"""One-shot health snapshot for the running arb-agent.

Usage:
    python -m scripts.health_check

Sections:
  1. Safety state          — STOP file present? today's live order count.
  2. Open positions        — count + total exposure.
  3. Resting maker orders  — count + per-platform breakdown.
  4. Recent realized P&L   — last 24h closed/exited.
  5. Predicted vs realized — MAE on resolved trades.
  6. Process freshness     — most recent paper_trade_marks timestamp
                              (proxy for "is the monitor running?").

Designed for ad-hoc operator use during early live trading. Makes no
exchange calls. The only DB write is an idempotent
`init_safety_schema` (CREATE TABLE IF NOT EXISTS) so the script works
on a database that pre-dates the safety feature deploy.
"""

from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import aiosqlite

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import load_config
from src.exec.safety import (
    DEFAULT_STOP_FILE,
    get_live_order_count_today,
    is_stopped,
)


def _hr(label: str) -> None:
    print(f"\n=== {label} ===")


async def _safety_section(db_path: str, max_per_day: int) -> None:
    _hr("Safety state")
    stopped, reason = is_stopped()
    if stopped:
        print(f"  STOP FILE PRESENT  ({DEFAULT_STOP_FILE})")
        print(f"    reason: {reason}")
        print(f"    resume: python -m scripts.start")
    else:
        print(f"  STOP file: absent ({DEFAULT_STOP_FILE})")
    # Idempotent: ensures the counter table exists even if main.py
    # hasn't run since the safety-feature deploy.
    from src.exec.safety import init_safety_schema
    try:
        await init_safety_schema(db_path)
        count = await get_live_order_count_today(db_path)
    except Exception as e:
        print(f"  live order counter: ERROR ({e})")
        return
    cap_str = f"/{max_per_day}" if max_per_day > 0 else " (no cap)"
    print(f"  live orders today (UTC): {count}{cap_str}")
    if max_per_day > 0 and count >= max_per_day:
        print("    !! cap reached — auto-STOP should be active")


async def _positions_section(db: aiosqlite.Connection) -> None:
    _hr("Open positions")
    cur = await db.execute(
        """SELECT COUNT(*) n,
                  SUM(yes_size_usd + no_size_usd) exposure,
                  SUM(predicted_net_usd) pred
           FROM paper_trades WHERE status='open'""",
    )
    row = await cur.fetchone()
    n = row["n"] or 0
    exposure = row["exposure"] or 0.0
    pred = row["pred"] or 0.0
    print(f"  open trades: {n}")
    print(f"  total exposure: ${exposure:.2f}")
    print(f"  total predicted profit: ${pred:.2f}")


async def _maker_orders_section(db: aiosqlite.Connection) -> None:
    _hr("Resting maker orders")
    cur = await db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='maker_exit_orders'",
    )
    if not await cur.fetchone():
        print("  (no maker_exit_orders table yet)")
        return
    cur = await db.execute(
        """SELECT platform, COUNT(*) n, SUM(contracts) total_contracts
           FROM maker_exit_orders WHERE status='resting'
           GROUP BY platform""",
    )
    rows = list(await cur.fetchall())
    if not rows:
        print("  none")
        return
    for r in rows:
        print(
            f"  {r['platform']:<12} {r['n']:>3} resting  "
            f"{(r['total_contracts'] or 0):>8.2f} contracts"
        )


async def _realized_section(db: aiosqlite.Connection) -> None:
    _hr("Recent realized P&L (last 24h)")
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    cur = await db.execute(
        """SELECT status, COUNT(*) n,
                  SUM(realized_profit_usd) realized,
                  SUM(predicted_net_usd) predicted
           FROM paper_trades
           WHERE status IN ('closed','exited','resolved')
             AND (resolved_at >= ? OR detected_at >= ?)
           GROUP BY status""",
        (cutoff, cutoff),
    )
    rows = list(await cur.fetchall())
    if not rows:
        print("  no closed/exited/resolved trades in window")
        return
    total_real = 0.0
    total_pred = 0.0
    for r in rows:
        real = r["realized"] or 0.0
        pred = r["predicted"] or 0.0
        total_real += real
        total_pred += pred
        print(
            f"  {r['status']:<10} n={r['n']:>3}  "
            f"predicted=${pred:>8.2f}  realized=${real:>8.2f}"
        )
    delta = total_real - total_pred
    sign = "+" if delta >= 0 else ""
    print(
        f"  TOTAL       predicted=${total_pred:>8.2f}  "
        f"realized=${total_real:>8.2f}  delta={sign}${delta:.2f}"
    )


async def _accuracy_section(db: aiosqlite.Connection) -> None:
    _hr("Prediction accuracy (resolved trades, all-time)")
    cur = await db.execute(
        """SELECT predicted_net_usd, realized_profit_usd
           FROM paper_trades WHERE status='resolved'""",
    )
    rows = list(await cur.fetchall())
    if not rows:
        print("  no resolved trades yet")
        return
    errs = [
        (r["realized_profit_usd"] or 0) - (r["predicted_net_usd"] or 0)
        for r in rows
    ]
    mae = sum(abs(e) for e in errs) / len(errs)
    wins = sum(1 for r in rows if (r["realized_profit_usd"] or 0) > 0)
    print(f"  resolved: {len(rows)}  win-rate: {wins}/{len(rows)} "
          f"({100*wins/len(rows):.1f}%)")
    print(f"  mean abs prediction error: ${mae:.2f}")


async def _freshness_section(db: aiosqlite.Connection) -> None:
    _hr("Process freshness")
    cur = await db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='paper_trade_marks'",
    )
    if not await cur.fetchone():
        print("  (no paper_trade_marks table yet — monitor never ran)")
        return
    cur = await db.execute(
        "SELECT MAX(observed_at) last_mark FROM paper_trade_marks",
    )
    row = await cur.fetchone()
    last = row["last_mark"] if row else None
    if not last:
        print("  no marks recorded yet")
        return
    try:
        when = datetime.fromisoformat(last.replace("Z", "+00:00"))
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - when).total_seconds()
    except Exception:
        print(f"  last mark: {last} (could not parse age)")
        return
    print(f"  last position mark: {last}  ({age:.0f}s ago)")
    if age > 300:
        print("  !! WARNING: > 5 min since last mark — monitor may be stalled")


async def main() -> None:
    cfg = load_config("config.yaml")
    db_path = cfg["database"]["path"]
    max_per_day = int(
        ((cfg.get("execution") or {}).get("max_live_orders_per_day") or 0)
    )
    allow_send = bool((cfg.get("execution") or {}).get("allow_send", False))
    mode = ((cfg.get("execution") or {}).get("mode") or "log_only")

    print(f"\narb-agent health check  ({datetime.now(timezone.utc).isoformat()})")
    print(f"  config: mode={mode} allow_send={allow_send} db={db_path}")

    await _safety_section(db_path, max_per_day)
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        await _positions_section(db)
        await _maker_orders_section(db)
        await _realized_section(db)
        await _accuracy_section(db)
        await _freshness_section(db)
    print()


if __name__ == "__main__":
    asyncio.run(main())
