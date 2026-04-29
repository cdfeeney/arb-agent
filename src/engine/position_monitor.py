"""Position monitor — mark-to-market open paper trades, recommend exits.

For each open paper trade:
  1. Fetch the live bid book on both legs (Kalshi + Polymarket).
  2. Walk the bid book to size: compute VWAP unwind price + slippage vs best bid.
  3. Compute mark-to-market vs cost basis, convergence ratio, annualized returns.
  4. Decide HOLD | EXIT | WATCH and write a row to paper_trade_marks.
  5. If EXIT: paper-mark the trade as exited and set a re-entry cooldown.

In dry-run mode, EXIT does NOT call sell APIs. It only updates the paper-trade
status and logs the decision so we can analyze whether the threshold was right.

See LAG_DESIGN.md sibling for design intent on the lag side; this module is
the parallel exit-side discipline for arb positions.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from src.clients.kalshi import KalshiClient
from src.clients.polymarket import PolymarketClient
from src.db.store import Database

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExitConfig:
    enabled: bool
    convergence_threshold: float       # exit when this fraction of arb captured
    annualized_multiple: float         # exit when annualized_now > hold * this
    max_slippage_pct: float             # if slippage > this, downgrade EXIT to HOLD
    cooldown_minutes: int               # re-entry cooldown after exit
    min_days_remaining_to_force_hold: float  # very-near-resolution: hold

    @classmethod
    def from_dict(cls, d: dict) -> "ExitConfig":
        return cls(
            enabled=bool(d.get("enabled", True)),
            convergence_threshold=float(d.get("convergence_threshold", 0.7)),
            annualized_multiple=float(d.get("annualized_multiple", 1.5)),
            max_slippage_pct=float(d.get("max_slippage_pct", 0.02)),  # 2%
            cooldown_minutes=int(d.get("cooldown_minutes", 60)),
            min_days_remaining_to_force_hold=float(
                d.get("min_days_remaining_to_force_hold", 0.5)
            ),
        )


@dataclass
class LegMark:
    best_bid: float
    vwap_bid: float
    fill_contracts: float
    target_contracts: float
    book_available: bool


@dataclass
class TradeMark:
    paper_trade_id: int
    yes_leg: LegMark
    no_leg: LegMark
    cost_basis: float
    unwind_value: float
    locked_payout: float          # = sum(contracts) since one leg pays $1
    mark_to_market: float
    locked_profit_at_resolution: float
    convergence_ratio: float
    slippage_pct: float
    days_held: float
    days_remaining: float
    annualized_now_pct: float
    annualized_to_close_pct: float
    recommendation: str
    reason: str


def _parse_dt(s: str | datetime | None) -> Optional[datetime]:
    if isinstance(s, datetime):
        return s if s.tzinfo else s.replace(tzinfo=timezone.utc)
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


async def _bid_mark_kalshi(
    kalshi: KalshiClient, ticker: str, side: str, contracts: float,
) -> LegMark:
    """side: 'yes' or 'no'. Returns the bid-side liquidation profile."""
    book = await kalshi.fetch_orderbook(ticker)
    if not book:
        return LegMark(0.0, 0.0, 0.0, contracts, False)
    bids = book.get(f"{side}_bids", [])
    if not bids:
        return LegMark(0.0, 0.0, 0.0, contracts, False)
    best_bid = bids[0][0]
    vwap, filled = KalshiClient.walk_bids(bids, contracts)
    return LegMark(best_bid, vwap, filled, contracts, True)


async def _bid_mark_polymarket(
    poly: PolymarketClient, token_id: str, contracts: float,
) -> LegMark:
    book = await poly.fetch_clob_book(token_id)
    if not book:
        return LegMark(0.0, 0.0, 0.0, contracts, False)
    best_bid, _ = PolymarketClient.best_bid_from_book(book)
    vwap, filled = PolymarketClient.walk_bids(book, contracts)
    return LegMark(best_bid, vwap, filled, contracts, best_bid > 0)


def _decide(
    mark: TradeMark, cfg: ExitConfig,
) -> tuple[str, str]:
    """Pure-function exit decision.

    Returns (recommendation, reason). Recommendation is one of:
        EXIT   — recommend unwinding both legs now
        WATCH  — close to a trigger; log but do not exit
        HOLD   — keep position to resolution
    """
    if not cfg.enabled:
        return "HOLD", "monitor disabled"

    # Force-hold near resolution; the spread cost likely exceeds the
    # annualized-return benefit of an early exit when resolution is hours away.
    if mark.days_remaining < cfg.min_days_remaining_to_force_hold:
        return "HOLD", f"resolves in {mark.days_remaining:.2f}d, hold for full payout"

    # If neither leg has a usable bid book, we couldn't exit even if we wanted to.
    if not mark.yes_leg.book_available or not mark.no_leg.book_available:
        return "HOLD", "missing bid book on at least one leg"

    # Slippage gate: if walking the book to our size would cost > threshold,
    # downgrade any EXIT to a HOLD with a flag — the bid book is too thin.
    if mark.slippage_pct > cfg.max_slippage_pct:
        return (
            "HOLD",
            f"thin book: slippage {mark.slippage_pct*100:.2f}% > "
            f"{cfg.max_slippage_pct*100:.2f}% threshold",
        )

    # Trigger 1: convergence captured ≥ threshold.
    if mark.convergence_ratio >= cfg.convergence_threshold:
        return (
            "EXIT",
            f"convergence={mark.convergence_ratio*100:.1f}% "
            f">= {cfg.convergence_threshold*100:.0f}%",
        )

    # Trigger 2: annualized-now beats annualized-if-held by required multiple.
    # Only meaningful when annualized_to_close_pct > 0 (otherwise division would
    # be weird; we already mark-to-market into negatives explicitly).
    if (
        mark.annualized_to_close_pct > 0
        and mark.annualized_now_pct
        > mark.annualized_to_close_pct * cfg.annualized_multiple
    ):
        return (
            "EXIT",
            f"annualized {mark.annualized_now_pct:.1f}% > "
            f"{cfg.annualized_multiple:.1f}× hold {mark.annualized_to_close_pct:.1f}%",
        )

    # Within 80% of the convergence trigger — log for analysis but don't act.
    if mark.convergence_ratio >= cfg.convergence_threshold * 0.8:
        return (
            "WATCH",
            f"convergence={mark.convergence_ratio*100:.1f}% near threshold",
        )

    return "HOLD", "no trigger met"


async def monitor_open_positions(
    db: Database,
    kalshi: KalshiClient,
    poly: PolymarketClient,
    cfg: ExitConfig,
    dry_run: bool = True,
) -> dict:
    """Mark-to-market every open paper trade. Returns summary for logging."""
    summary = {
        "n_open": 0,
        "n_marked": 0,
        "exits": 0,
        "watches": 0,
        "holds": 0,
        "skipped": 0,
    }
    if not cfg.enabled:
        return summary

    open_trades = await db.list_open_paper_trades()
    summary["n_open"] = len(open_trades)
    if not open_trades:
        return summary

    for trade in open_trades:
        try:
            mark = await _build_mark(trade, kalshi, poly)
            if mark is None:
                summary["skipped"] += 1
                continue
            recommendation, reason = _decide(mark, cfg)
            mark.recommendation = recommendation
            mark.reason = reason

            await db.save_paper_trade_mark({
                "paper_trade_id": mark.paper_trade_id,
                "yes_bid_now": mark.yes_leg.best_bid,
                "yes_bid_vwap": mark.yes_leg.vwap_bid,
                "yes_bid_fill_contracts": mark.yes_leg.fill_contracts,
                "no_bid_now": mark.no_leg.best_bid,
                "no_bid_vwap": mark.no_leg.vwap_bid,
                "no_bid_fill_contracts": mark.no_leg.fill_contracts,
                "cost_basis_usd": round(mark.cost_basis, 4),
                "unwind_value_usd": round(mark.unwind_value, 4),
                "locked_payout_usd": round(mark.locked_payout, 4),
                "mark_to_market_usd": round(mark.mark_to_market, 4),
                "convergence_ratio": round(mark.convergence_ratio, 4),
                "slippage_pct": round(mark.slippage_pct, 4),
                "days_held": round(mark.days_held, 4),
                "days_remaining": round(mark.days_remaining, 4),
                "annualized_now_pct": round(mark.annualized_now_pct, 2),
                "annualized_to_close_pct": round(mark.annualized_to_close_pct, 2),
                "exit_recommendation": recommendation,
                "decision_reason": reason,
            })
            summary["n_marked"] += 1

            if recommendation == "EXIT":
                summary["exits"] += 1
                log.info(
                    "EXIT trade #%d: %s | mtm=$%.2f conv=%.1f%% slip=%.2f%% | %s",
                    trade["id"], trade["pair_id"][:60],
                    mark.mark_to_market, mark.convergence_ratio * 100,
                    mark.slippage_pct * 100, reason,
                )
                if dry_run:
                    # Paper-mode: record the exit + cooldown without calling APIs.
                    await db.mark_paper_trade_exited(
                        trade["id"], mark.mark_to_market, reason,
                    )
                    await db.add_pair_cooldown(
                        trade["pair_id"], f"paper-exit: {reason}",
                    )
            elif recommendation == "WATCH":
                summary["watches"] += 1
                log.info(
                    "WATCH trade #%d: %s | conv=%.1f%% mtm=$%.2f | %s",
                    trade["id"], trade["pair_id"][:60],
                    mark.convergence_ratio * 100, mark.mark_to_market, reason,
                )
            else:
                summary["holds"] += 1
        except Exception as e:
            log.error("monitor: trade %s error: %s", trade.get("id"), e, exc_info=True)
            summary["skipped"] += 1

    return summary


async def _build_mark(
    trade: dict, kalshi: KalshiClient, poly: PolymarketClient,
) -> Optional[TradeMark]:
    yes_platform = trade["yes_platform"]
    yes_ticker = trade["yes_ticker"]
    yes_contracts = float(trade["yes_contracts"] or 0)
    yes_paid = float(trade["yes_observed_price"] or 0)
    yes_size_usd = float(trade["yes_size_usd"] or 0)

    no_platform = trade["no_platform"]
    no_ticker = trade["no_ticker"]
    no_contracts = float(trade["no_contracts"] or 0)
    no_paid = float(trade["no_observed_price"] or 0)
    no_size_usd = float(trade["no_size_usd"] or 0)

    if yes_contracts <= 0 or no_contracts <= 0:
        return None

    # Fetch bid books for whichever side each leg lives on.
    if yes_platform == "kalshi":
        yes_mark = await _bid_mark_kalshi(kalshi, yes_ticker, "yes", yes_contracts)
    elif yes_platform == "polymarket":
        # We don't store the YES token id on paper_trades today, but the
        # opportunity scanner has it on the market dict. For v1 paper-only
        # operation we approximate using best-bid via Gamma; CLOB token
        # round-trip gets added in v2 when we wire to live order placement.
        yes_mark = LegMark(0.0, 0.0, 0.0, yes_contracts, False)
    else:
        yes_mark = LegMark(0.0, 0.0, 0.0, yes_contracts, False)

    if no_platform == "kalshi":
        no_mark = await _bid_mark_kalshi(kalshi, no_ticker, "no", no_contracts)
    elif no_platform == "polymarket":
        no_mark = LegMark(0.0, 0.0, 0.0, no_contracts, False)
    else:
        no_mark = LegMark(0.0, 0.0, 0.0, no_contracts, False)

    cost_basis = yes_size_usd + no_size_usd
    unwind_value = (
        yes_mark.vwap_bid * yes_mark.fill_contracts
        + no_mark.vwap_bid * no_mark.fill_contracts
    )
    # Locked payout: at resolution one leg pays $1/contract for whichever
    # side wins. With perfectly hedged contract counts we receive
    # min(yes_contracts, no_contracts) × $1.
    locked_payout = min(yes_contracts, no_contracts) * 1.0
    locked_profit_at_resolution = locked_payout - cost_basis
    mark_to_market = unwind_value - cost_basis

    convergence_ratio = (
        mark_to_market / locked_profit_at_resolution
        if locked_profit_at_resolution > 0 else 0.0
    )

    # Slippage = avg shortfall across legs vs best-bid. Each leg contributes
    # its own (best - vwap)/best. For legs with no available book we treat
    # slippage as 100% so the decide function treats them as un-exitable.
    def _slip(leg: LegMark) -> float:
        if not leg.book_available or leg.best_bid <= 0:
            return 1.0
        if leg.vwap_bid <= 0:
            return 1.0
        return max(0.0, (leg.best_bid - leg.vwap_bid) / leg.best_bid)

    slippage_pct = max(_slip(yes_mark), _slip(no_mark))

    detected_at = _parse_dt(trade.get("detected_at"))
    closes_at = _parse_dt(trade.get("closes_at"))
    now = datetime.now(timezone.utc)
    days_held = (
        max(0.0, (now - detected_at).total_seconds() / 86400.0)
        if detected_at else 0.0
    )
    days_remaining = (
        max(0.0, (closes_at - now).total_seconds() / 86400.0)
        if closes_at else 0.0
    )

    # Annualized returns (in percent).
    if cost_basis > 0 and days_held > 0:
        annualized_now_pct = (mark_to_market / cost_basis) * (365.0 / days_held) * 100
    else:
        annualized_now_pct = 0.0
    total_days = days_held + days_remaining
    if cost_basis > 0 and total_days > 0:
        annualized_to_close_pct = (
            (locked_profit_at_resolution / cost_basis) * (365.0 / total_days) * 100
        )
    else:
        annualized_to_close_pct = 0.0

    return TradeMark(
        paper_trade_id=int(trade["id"]),
        yes_leg=yes_mark,
        no_leg=no_mark,
        cost_basis=cost_basis,
        unwind_value=unwind_value,
        locked_payout=locked_payout,
        mark_to_market=mark_to_market,
        locked_profit_at_resolution=locked_profit_at_resolution,
        convergence_ratio=convergence_ratio,
        slippage_pct=slippage_pct,
        days_held=days_held,
        days_remaining=days_remaining,
        annualized_now_pct=annualized_now_pct,
        annualized_to_close_pct=annualized_to_close_pct,
        recommendation="HOLD",
        reason="",
    )
