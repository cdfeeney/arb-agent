"""Data-driven sizing analysis: how many concurrent positions, at what stake,
at what edge floor, given THIS bot's actual opportunity flow.

Usage:
    python -m scripts.opportunity_analysis
    python -m scripts.opportunity_analysis --days 14    # window
    python -m scripts.opportunity_analysis --bankroll 100

Output sections:
  1. Edge distribution — histogram of detected opportunities by gross edge%
  2. Daily supply — how many opps/day clear each edge threshold
  3. Time-to-resolve / time-to-close — capital velocity from closed trades
  4. Concentration math - for each candidate (#positions, edge_floor, stake)
     combo, expected weekly bankroll growth assuming top-N selection
  5. Recommendation — the combo that maximizes expected growth given supply
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


EDGE_BUCKETS = [0.03, 0.05, 0.07, 0.10, 0.15, 0.20]


def _hr(label: str) -> None:
    print(f"\n=== {label} ===")


def _bar(n: int, max_n: int, width: int = 40) -> str:
    if max_n == 0:
        return ""
    filled = int(round((n / max_n) * width))
    return "#" * filled + "." * (width - filled)


async def _edge_distribution(db: aiosqlite.Connection, since: str) -> list[float]:
    """Pull edge_gross_pct from paper_trades (the ones that passed all filters
    and got allocated). The opportunities table has profit_pct but is only
    populated when the allocator dedupes — paper_trades is more representative
    of real flow."""
    cur = await db.execute(
        "SELECT edge_gross_pct FROM paper_trades "
        "WHERE detected_at >= ? AND edge_gross_pct IS NOT NULL "
        "ORDER BY edge_gross_pct DESC",
        (since,),
    )
    rows = await cur.fetchall()
    return [float(r[0]) for r in rows if r[0] is not None]


async def _daily_supply(db: aiosqlite.Connection, since: str) -> dict[str, list[float]]:
    """Group edges by detection day."""
    cur = await db.execute(
        "SELECT date(detected_at) AS day, edge_gross_pct "
        "FROM paper_trades "
        "WHERE detected_at >= ? AND edge_gross_pct IS NOT NULL "
        "ORDER BY day",
        (since,),
    )
    out: dict[str, list[float]] = {}
    for day, edge in await cur.fetchall():
        out.setdefault(day, []).append(float(edge))
    return out


async def _close_velocity(db: aiosqlite.Connection, since: str) -> list[tuple[float, float]]:
    """Returns list of (hours_held, edge_gross) for each closed/exited trade."""
    cur = await db.execute(
        "SELECT detected_at, resolved_at, edge_gross_pct "
        "FROM paper_trades "
        "WHERE status IN ('closed','exited','resolved') "
        "AND detected_at >= ? AND resolved_at IS NOT NULL",
        (since,),
    )
    out = []
    for det, res, edge in await cur.fetchall():
        try:
            d1 = datetime.fromisoformat(str(det).replace("Z", "+00:00").replace(" ", "T"))
            d2 = datetime.fromisoformat(str(res).replace("Z", "+00:00").replace(" ", "T"))
            if d1.tzinfo is None: d1 = d1.replace(tzinfo=timezone.utc)
            if d2.tzinfo is None: d2 = d2.replace(tzinfo=timezone.utc)
            hours = (d2 - d1).total_seconds() / 3600.0
            out.append((hours, float(edge or 0)))
        except Exception:
            continue
    return out


def _print_edge_histogram(edges: list[float]) -> None:
    _hr("Edge distribution (gross %, last window)")
    if not edges:
        print("  (no opportunities in window)")
        return
    buckets: Counter = Counter()
    for e in edges:
        for thr in reversed(EDGE_BUCKETS):
            if e >= thr:
                buckets[thr] += 1
                break
        else:
            buckets[0] += 1
    max_n = max(buckets.values()) if buckets else 1
    print(f"  Total opportunities: {len(edges)}")
    print(f"  Mean edge: {sum(edges)/len(edges)*100:.2f}%   "
          f"Median: {sorted(edges)[len(edges)//2]*100:.2f}%   "
          f"Max: {max(edges)*100:.2f}%")
    print()
    for thr in EDGE_BUCKETS:
        n = sum(1 for e in edges if e >= thr)
        print(f"  >={thr*100:>4.1f}% : {n:>4}  {_bar(n, len(edges))}")


def _print_daily_supply(daily: dict[str, list[float]]) -> None:
    _hr("Daily supply (opps/day clearing each edge threshold)")
    if not daily:
        print("  (no daily data)")
        return
    days = sorted(daily.keys())
    print(f"  {'Day':<12} {'>=3%':>5} {'>=5%':>5} {'>=7%':>5} {'>=10%':>5} {'>=15%':>5}")
    counts_at = {thr: [] for thr in [0.03, 0.05, 0.07, 0.10, 0.15]}
    for d in days:
        edges = daily[d]
        row = []
        for thr in [0.03, 0.05, 0.07, 0.10, 0.15]:
            n = sum(1 for e in edges if e >= thr)
            counts_at[thr].append(n)
            row.append(f"{n:>5}")
        print(f"  {d:<12} {' '.join(row)}")
    print()
    print(f"  {'AVG/day':<12} " + " ".join(
        f"{sum(counts_at[thr])/len(days):>5.1f}"
        for thr in [0.03, 0.05, 0.07, 0.10, 0.15]
    ))


def _print_velocity(velocity: list[tuple[float, float]]) -> None:
    _hr("Capital velocity (time from detection -> close, closed trades only)")
    if not velocity:
        print("  (no closed trades in window)")
        return
    hours = [h for h, _ in velocity]
    hours.sort()
    median_h = hours[len(hours)//2]
    p25 = hours[len(hours)//4]
    p75 = hours[(len(hours)*3)//4]
    print(f"  N closed: {len(velocity)}")
    print(f"  Hold time (hours):  p25={p25:.1f}  median={median_h:.1f}  p75={p75:.1f}")
    print(f"  Hold time (days):   p25={p25/24:.2f}  median={median_h/24:.2f}  p75={p75/24:.2f}")
    weekly_turnover = (24 * 7) / median_h if median_h > 0 else 0
    print(f"  Implied capital turnover: {weekly_turnover:.2f}x per week "
          f"(based on median hold)")


def _print_concentration_math(
    edges: list[float],
    daily: dict[str, list[float]],
    velocity: list[tuple[float, float]],
    bankroll: float,
) -> None:
    _hr("Concentration math - projected weekly bankroll growth")
    if not edges or not daily:
        print("  (insufficient data)")
        return

    days_with_data = max(1, len(daily))

    def avg_per_day_at(thr: float) -> float:
        return sum(1 for e in edges if e >= thr) / days_with_data

    avg_per_day = {
        thr: avg_per_day_at(thr) for thr in [0.03, 0.05, 0.07, 0.10]
    }

    # Velocity assumption: median hold time on closed trades. If we have no
    # closed trades, assume 1 week per trade (conservative).
    if velocity:
        median_h = sorted([h for h, _ in velocity])[len(velocity)//2]
        weeks_per_trade = max(median_h / (24 * 7), 0.1)
    else:
        weeks_per_trade = 1.0
    turnover_weekly = 1.0 / weeks_per_trade

    # Round-trip fees (rough): 4-5% at small stake, 3-4% at large stake.
    # Use a simple stake-aware function.
    def fees_pct(stake: float) -> float:
        if stake < 5: return 0.06
        if stake < 15: return 0.045
        if stake < 50: return 0.035
        return 0.03

    print(f"  Bankroll: ${bankroll:.2f}")
    print(f"  Median hold (weeks): {weeks_per_trade:.2f}   "
          f"-> turnover: {turnover_weekly:.2f}x/week")
    print(f"  Avg opps/day at edge threshold: "
          f"3%={avg_per_day[0.03]:.1f} 5%={avg_per_day[0.05]:.1f} "
          f"7%={avg_per_day[0.07]:.1f} 10%={avg_per_day[0.10]:.1f}")
    print()
    print(f"  {'#pos':>5} {'stake':>8} {'edge_floor':>10} {'avg_edge':>9} "
          f"{'fees':>6} {'net':>6} {'$/wk':>8} {'supply_ok':>10}")
    print("  " + "-" * 76)

    candidates = [
        # (n_positions, edge_floor)
        (3, 0.10), (5, 0.10), (5, 0.08), (8, 0.07), (10, 0.05),
        (15, 0.05), (20, 0.05), (30, 0.03), (43, 0.03),
    ]
    best = None
    for n, floor in candidates:
        # Top N edges that clear floor
        qualifying = [e for e in edges if e >= floor]
        if not qualifying:
            print(f"  {n:>5} {'—':>8} {floor*100:>9.1f}% {'—':>9} {'—':>6} "
                  f"{'—':>6} {'—':>8} {'no opps':>10}")
            continue
        top_n = sorted(qualifying, reverse=True)[:n]
        if not top_n:
            continue
        avg_edge = sum(top_n) / len(top_n)
        stake = bankroll / n
        f = fees_pct(stake)
        net = avg_edge - f
        per_week = net * stake * len(top_n) * turnover_weekly
        supply_ok = avg_per_day_at(floor) * 7 * weeks_per_trade >= n
        marker = "OK" if supply_ok else "thin"
        print(f"  {n:>5} ${stake:>6.2f} {floor*100:>9.1f}% {avg_edge*100:>8.2f}% "
              f"{f*100:>5.2f}% {net*100:>5.2f}% ${per_week:>6.2f} {marker:>10}")
        if supply_ok and (best is None or per_week > best[3]):
            best = (n, stake, floor, per_week, avg_edge, f, net)

    if best:
        n, stake, floor, per_week, avg_edge, f, net = best
        _hr("Recommendation")
        print(f"  -> {n} concurrent positions @ ${stake:.2f} each, "
              f"edge floor {floor*100:.0f}%")
        print(f"  -> expected avg edge {avg_edge*100:.1f}% gross, "
              f"{net*100:.2f}% net after ~{f*100:.1f}% round-trip fees")
        print(f"  -> projected weekly growth: ${per_week:.2f} on ${bankroll:.0f} "
              f"= {per_week/bankroll*100:.2f}%/wk = {per_week*52/bankroll*100:.0f}%/yr")
        print(f"  -> supply check: opps at this edge floor are sufficient")
        print()
        print(f"  Suggested config diffs:")
        print(f"    sizing.bankroll: {bankroll:.0f}")
        print(f"    sizing.min_bet: {stake:.2f}")
        print(f"    sizing.max_bet: {stake * 1.5:.2f}")
        print(f"    sizing.max_position_pct: {1/n:.3f}  # ≈ 1/n_positions")
        print(f"    filters.min_profit_pct: {floor:.3f}")
        print(f"    allocator.max_open_positions: {n}  # NEW knob")
    else:
        _hr("Recommendation")
        print("  (could not converge on a recommendation — supply too thin "
              "for any candidate. Lower edge_floor or wait for more data.)")


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7,
                    help="Window for analysis (default 7 days)")
    ap.add_argument("--bankroll", type=float, default=None,
                    help="Override config bankroll for projections")
    args = ap.parse_args()

    cfg = load_config("config.yaml")
    db_path = cfg["database"]["path"]
    bankroll = args.bankroll or float(cfg.get("sizing", {}).get("bankroll", 100))
    since = (datetime.now(timezone.utc) - timedelta(days=args.days)).isoformat()

    print(f"\nOpportunity analysis  ({datetime.now(timezone.utc).isoformat()})")
    print(f"  db: {db_path}")
    print(f"  window: last {args.days} days")
    print(f"  bankroll: ${bankroll:.2f}")

    async with aiosqlite.connect(db_path) as db:
        edges = await _edge_distribution(db, since)
        daily = await _daily_supply(db, since)
        velocity = await _close_velocity(db, since)

    _print_edge_histogram(edges)
    _print_daily_supply(daily)
    _print_velocity(velocity)
    _print_concentration_math(edges, daily, velocity, bankroll)
    print()


if __name__ == "__main__":
    asyncio.run(main())
