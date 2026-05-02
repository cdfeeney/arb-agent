"""Per-trade diagnostic: why did each closed paper trade earn what it did?

Usage:
    python -m scripts.why_closed                # last 10 closed
    python -m scripts.why_closed --recent 24    # closed in last 24h
    python -m scripts.why_closed --trade 12345  # one specific trade

For each trade, prints:
  * Headline: predicted_net vs realized_profit, delta in $ and as % of stake
  * Entry: observed prices, sizes, fees estimated
  * Exit: every partial-unwind mark (price, size, realized, slippage, reason)
  * Bleed decomposition: how much of the delta is fees vs slippage vs price drift

Aggregate footer:
  * Total predicted vs realized
  * Mean delta as % of stake
  * Most common exit reason
  * Worst-bleeder pair
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections import Counter
from datetime import datetime, timezone, timedelta
from pathlib import Path

import aiosqlite

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.config import load_config


def _hr() -> None:
    print("-" * 78)


def _fmt_pct(v: float | None) -> str:
    return f"{v*100:>6.2f}%" if v is not None else "    n/a"


def _fmt_usd(v: float | None) -> str:
    return f"${v:>7.2f}" if v is not None else "      n/a"


async def _fetch_trades(db: aiosqlite.Connection, args) -> list:
    if args.trade:
        cur = await db.execute(
            "SELECT * FROM paper_trades WHERE id=?", (args.trade,),
        )
        return list(await cur.fetchall())

    where = "WHERE status IN ('closed','exited')"
    params: tuple = ()
    if args.recent is not None:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=args.recent)).isoformat()
        where += " AND resolved_at >= ?"
        params = (cutoff,)
    cur = await db.execute(
        f"SELECT * FROM paper_trades {where} "
        f"ORDER BY resolved_at DESC LIMIT ?",
        params + (args.limit,),
    )
    return list(await cur.fetchall())


async def _fetch_marks(db: aiosqlite.Connection, trade_id: int) -> list:
    cur = await db.execute(
        """SELECT observed_at, yes_bid_now, no_bid_now,
                  yes_bid_vwap, no_bid_vwap,
                  partial_unwind_size, partial_unwind_realized_usd,
                  slippage_pct, decision_reason, exit_recommendation,
                  cost_basis_usd, unwind_value_usd, mark_to_market_usd
           FROM paper_trade_marks
           WHERE paper_trade_id=?
           ORDER BY observed_at""",
        (trade_id,),
    )
    return list(await cur.fetchall())


def _diagnose_one(trade, marks) -> dict:
    """Print one trade's breakdown; return aggregate-friendly dict."""
    pid = trade["pair_id"]
    print(f"\nTrade #{trade['id']}  pair={pid[:64]}")
    print(f"  detected: {(trade['detected_at'] or '')[:19]}   "
          f"closes: {(trade['closes_at'] or '')[:19] if trade['closes_at'] else 'n/a'}")
    print(f"  resolved: {(trade['resolved_at'] or '')[:19]}   "
          f"status: {trade['status']}")

    yes_p = trade["yes_observed_price"] or 0
    no_p = trade["no_observed_price"] or 0
    yes_usd = trade["yes_size_usd"] or 0
    no_usd = trade["no_size_usd"] or 0
    yes_c = trade["yes_contracts"] or 0
    no_c = trade["no_contracts"] or 0
    edge = trade["edge_gross_pct"] or 0
    fees_est = trade["fees_estimated_usd"] or 0
    pred = trade["predicted_net_usd"] or 0
    real = trade["realized_profit_usd"] or 0
    delta = real - pred
    stake = yes_usd + no_usd
    delta_pct_stake = (delta / stake) if stake else 0

    print(f"  ENTRY  yes={yes_p:.4f}@{yes_c:.2f}c (${yes_usd:.2f})   "
          f"no={no_p:.4f}@{no_c:.2f}c (${no_usd:.2f})   stake=${stake:.2f}")
    print(f"         gross edge={_fmt_pct(edge)}   fees(est)={_fmt_usd(fees_est)}   "
          f"implied_sum={trade['implied_sum'] or 0:.4f}")
    print(f"  HEAD   predicted_net={_fmt_usd(pred)}   "
          f"realized_net={_fmt_usd(real)}   "
          f"delta={_fmt_usd(delta)}  ({delta_pct_stake*100:+.2f}% of stake)")

    # Decompose: sum of partial unwind realized vs entry fees
    sum_partial_realized = sum(
        (m["partial_unwind_realized_usd"] or 0) for m in marks
    )
    print(f"  EXIT   sum(partial unwind realized)={_fmt_usd(sum_partial_realized)}   "
          f"− entry fees {_fmt_usd(fees_est)}   "
          f"= net realized {_fmt_usd(sum_partial_realized - fees_est)}")

    # Per-unwind detail (cap to last 6 to keep output readable)
    unwinds = [m for m in marks if (m["partial_unwind_size"] or 0) > 0]
    if unwinds:
        print(f"  UNWINDS ({len(unwinds)} events):")
        for m in unwinds[-6:]:
            slip = m["slippage_pct"]
            slip_str = _fmt_pct(slip) if slip is not None else "    n/a"
            print(
                f"    {(m['observed_at'] or '')[:19]}   "
                f"size={(m['partial_unwind_size'] or 0):>6.2f}c   "
                f"realized={_fmt_usd(m['partial_unwind_realized_usd'])}   "
                f"slip={slip_str}   "
                f"reason={(m['decision_reason'] or '')[:40]}"
            )
    else:
        print("  (no partial-unwind marks recorded — closed via direct path)")

    # Best-effort attribution
    fee_drag = fees_est
    other_drag = -delta - fee_drag if delta < 0 else None
    if delta < 0:
        print(f"  ATTRIB fees drag={_fmt_usd(fee_drag)}   "
              f"other drag={_fmt_usd(other_drag)}  (slippage/price drift/partial fills)")

    reasons = [m["decision_reason"] for m in unwinds if m["decision_reason"]]
    return {
        "pair_id": pid,
        "predicted": pred,
        "realized": real,
        "delta": delta,
        "stake": stake,
        "fees_est": fees_est,
        "edge": edge,
        "reasons": reasons,
        "n_unwinds": len(unwinds),
    }


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--recent", type=float, default=None,
                    help="Only trades closed in last N hours")
    ap.add_argument("--limit", type=int, default=10,
                    help="Max trades to show (default 10)")
    ap.add_argument("--trade", type=int, default=None,
                    help="Diagnose one specific trade by id")
    args = ap.parse_args()

    cfg = load_config("config.yaml")
    db_path = cfg["database"]["path"]
    print(f"\nClosed-trade diagnostics  ({datetime.now(timezone.utc).isoformat()})")
    print(f"  db: {db_path}")

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        trades = await _fetch_trades(db, args)
        if not trades:
            print("\nNo closed/exited trades match the filter.")
            return

        results = []
        for t in trades:
            marks = await _fetch_marks(db, t["id"])
            results.append(_diagnose_one(t, marks))
            _hr()

        total_pred = sum(r["predicted"] for r in results)
        total_real = sum(r["realized"] for r in results)
        total_delta = total_real - total_pred
        total_stake = sum(r["stake"] for r in results)
        mean_delta_pct = (total_delta / total_stake * 100) if total_stake else 0
        all_reasons: Counter = Counter()
        for r in results:
            for reason in r["reasons"]:
                all_reasons[reason[:40]] += 1
        worst = min(results, key=lambda r: r["delta"]) if results else None

        print("\n=== Aggregate over %d trades ===" % len(results))
        print(f"  Total predicted: {_fmt_usd(total_pred)}")
        print(f"  Total realized:  {_fmt_usd(total_real)}")
        print(f"  Total delta:     {_fmt_usd(total_delta)}  "
              f"({mean_delta_pct:+.2f}% of cumulative stake)")
        if worst:
            print(f"  Worst bleeder:   trade {worst['pair_id'][:60]}  "
                  f"delta={_fmt_usd(worst['delta'])}")
        if all_reasons:
            print("  Most common exit reasons:")
            for reason, n in all_reasons.most_common(5):
                print(f"    {n:>3}× {reason}")

        # Quick verdict
        if total_stake and abs(mean_delta_pct) < 0.5:
            verdict = "tracking predicted within 0.5% — model is roughly calibrated"
        elif mean_delta_pct < -1:
            verdict = "REALIZED < PREDICTED by >1% — model is over-optimistic, fix before live"
        elif mean_delta_pct > 1:
            verdict = "realized > predicted by >1% — model is under-estimating, headroom exists"
        else:
            verdict = "small bias, watch as sample grows"
        print(f"  Verdict:         {verdict}")
        print()


if __name__ == "__main__":
    asyncio.run(main())
