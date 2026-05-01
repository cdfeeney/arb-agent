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

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from src.clients.kalshi import KalshiClient
from src.clients.polymarket import PolymarketClient
from src.db.store import Database
from src.engine.fees import compute_unwind_fees

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExitConfig:
    enabled: bool
    convergence_threshold: float       # exit when this fraction of arb captured
    annualized_multiple: float         # exit when annualized_now > hold * this
    max_slippage_pct: float             # if slippage > this, downgrade EXIT to HOLD
    cooldown_minutes: int               # re-entry cooldown after exit
    min_days_remaining_to_force_hold: float  # very-near-resolution: hold
    partial_unwind_min_size: float      # smallest unwind worth executing (contracts)

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
            partial_unwind_min_size=float(d.get("partial_unwind_min_size", 1.0)),
        )


@dataclass
class LegMark:
    best_bid: float
    best_bid_size: float
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
    cost_per_contract: float
    contracts_remaining: float
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
    # Buy-side leg dicts kept for fee calculation at unwind time. Carry the
    # platform + question + category from the original entry so the fee
    # engine can pick the right rate without re-fetching market metadata.
    buy_yes: dict = None
    buy_no: dict = None
    partial_unwind_size: float = 0.0
    partial_unwind_realized: float = 0.0


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
        log.debug("kalshi orderbook unavailable for %s (%s side)", ticker, side)
        return LegMark(0.0, 0.0, 0.0, 0.0, contracts, False)
    bids = book.get(f"{side}_bids", [])
    if not bids:
        log.debug("kalshi orderbook %s: no %s_bids (book exists but side empty)",
                  ticker, side)
        return LegMark(0.0, 0.0, 0.0, 0.0, contracts, False)
    best_bid, best_bid_size = bids[0][0], bids[0][1]
    vwap, filled = KalshiClient.walk_bids(bids, contracts)
    return LegMark(best_bid, best_bid_size, vwap, filled, contracts, True)


async def _bid_mark_polymarket(
    poly: PolymarketClient, token_id: str, contracts: float,
) -> LegMark:
    book = await poly.fetch_clob_book(token_id)
    if not book:
        log.debug("polymarket CLOB unavailable for token %s...", (token_id or "")[:16])
        return LegMark(0.0, 0.0, 0.0, 0.0, contracts, False)
    best_bid, best_bid_size = PolymarketClient.best_bid_from_book(book)
    if best_bid <= 0:
        log.debug("polymarket CLOB empty bids for token %s...", (token_id or "")[:16])
    vwap, filled = PolymarketClient.walk_bids(book, contracts)
    return LegMark(best_bid, best_bid_size, vwap, filled, contracts, best_bid > 0)


def _decide(
    mark: TradeMark, cfg: ExitConfig, fee_cfg: dict | None = None,
) -> tuple[str, str, float]:
    """Pure-function exit decision returning (recommendation, reason, unwind_size).

    Recommendation is one of:
        PARTIAL_UNWIND — sell `unwind_size` contracts on each leg at top-of-book
                          right now (zero slippage by construction)
        WATCH          — top-of-book breakeven but books too thin to act this cycle
        HOLD           — keep position; no favorable unwind available

    The strategy: each cycle, check if the SUM of best bids on YES leg and
    NO leg exceeds the per-contract cost we paid. If so, we can sell some
    number of contracts on each leg at the top of the bid book and lock in
    profit IMMEDIATELY with zero slippage. Size = min of (yes top-bid size,
    no top-bid size, contracts remaining). Capture profit, hold remainder for
    later cycles or resolution. Books may refresh and offer another partial
    unwind on the next cycle.
    """
    if not cfg.enabled:
        return "HOLD", "monitor disabled", 0.0

    if mark.contracts_remaining <= 0:
        return "HOLD", "fully unwound", 0.0

    # Force-hold near resolution: the spread cost on a partial unwind likely
    # exceeds the annualized-return benefit when resolution is hours away.
    if mark.days_remaining < cfg.min_days_remaining_to_force_hold:
        return "HOLD", f"resolves in {mark.days_remaining:.2f}d, hold for full payout", 0.0

    # Either bid book unavailable → can't unwind either leg.
    if not mark.yes_leg.book_available or not mark.no_leg.book_available:
        return "HOLD", "missing bid book on at least one leg", 0.0

    yes_bid = mark.yes_leg.best_bid
    no_bid = mark.no_leg.best_bid
    if yes_bid <= 0 or no_bid <= 0:
        return "HOLD", "zero best-bid on at least one leg", 0.0

    # Profitability test: can we sell ONE contract on each leg at top-of-book
    # for more than we paid per pair? If yes_bid + no_bid > cost_per_contract,
    # every unwound contract realizes (yes_bid + no_bid - cost_per_contract)
    # GROSS — but exit fees are real and we'd be paying them on top of the
    # entry fees already burned. A partial unwind that doesn't clear its own
    # exit fees is straight-up worse than just holding to resolution.
    sum_bids = yes_bid + no_bid
    gross_profit_per_contract = sum_bids - mark.cost_per_contract
    if gross_profit_per_contract <= 0:
        return (
            "HOLD",
            f"top bids {yes_bid:.4f}+{no_bid:.4f}={sum_bids:.4f} <= cost "
            f"{mark.cost_per_contract:.4f}",
            0.0,
        )

    # Sizing: take the smaller of (yes top-of-book size, no top-of-book size,
    # contracts still held). Selling more than top-of-book size on either leg
    # would walk the book and lose the zero-slippage guarantee.
    raw_size = min(
        mark.yes_leg.best_bid_size,
        mark.no_leg.best_bid_size,
        mark.contracts_remaining,
    )

    if raw_size < cfg.partial_unwind_min_size:
        return (
            "WATCH",
            f"profitable unwind sized {raw_size:.2f} below min "
            f"{cfg.partial_unwind_min_size:.2f} — wait for thicker book",
            raw_size,
        )

    # Slippage gate: previously max_slippage_pct was declared on ExitConfig
    # but never read. _build_mark already computes mark.slippage_pct (avg
    # shortfall vwap-vs-bid across legs). If slippage on this unwind would
    # exceed the configured cap, downgrade to WATCH — better to wait for a
    # cleaner book than to pay the spread on a thin unwind.
    if mark.slippage_pct > cfg.max_slippage_pct:
        return (
            "WATCH",
            f"slippage {mark.slippage_pct:.1%} > max {cfg.max_slippage_pct:.1%} "
            f"— book too thin to unwind cleanly",
            raw_size,
        )

    # Fee gate: exit fees on selling `raw_size` contracts on both legs at the
    # current bids. If we don't clear those fees, holding to resolution is
    # strictly better — we already paid entry fees, exit fees on a barely-
    # profitable unwind just compound the drag.
    exit_fees = 0.0
    if fee_cfg is not None and mark.buy_yes is not None and mark.buy_no is not None:
        exit_fees = compute_unwind_fees(
            mark.buy_yes, mark.buy_no, yes_bid, no_bid, raw_size, fee_cfg,
        )
    gross_realized = gross_profit_per_contract * raw_size
    net_realized = gross_realized - exit_fees
    if net_realized <= 0:
        return (
            "WATCH",
            f"top-of-book {yes_bid:.4f}+{no_bid:.4f}={sum_bids:.4f} clears "
            f"cost but exit fees ${exit_fees:.2f} on {raw_size:.1f} contracts "
            f"swallow the gross ${gross_realized:.2f}",
            raw_size,
        )

    return (
        "PARTIAL_UNWIND",
        f"top-of-book {yes_bid:.4f}+{no_bid:.4f}={sum_bids:.4f} > "
        f"cost {mark.cost_per_contract:.4f}, sell {raw_size:.2f} contracts "
        f"net ${net_realized:.2f} (gross ${gross_realized:.2f} - fees ${exit_fees:.2f})",
        raw_size,
    )


async def monitor_open_positions(
    db: Database,
    kalshi: KalshiClient,
    poly: PolymarketClient,
    cfg: ExitConfig,
    dry_run: bool = True,
    fee_cfg: dict | None = None,
    max_concurrent: int = 8,
) -> dict:
    """Mark-to-market every open paper trade. Returns summary for logging.

    Book fetches are parallelized via asyncio.gather with a concurrency bound
    so we don't blow rate limits. Sequential per-trade looping took ~30s on
    193 trades; concurrent brings the same workload to ~3-5s, fast enough
    to run on a tight (~15s) hot loop.
    """
    summary = {
        "n_open": 0,
        "n_marked": 0,
        "partial_unwinds": 0,
        "fully_closed": 0,
        "watches": 0,
        "holds": 0,
        "skipped": 0,
        "realized_this_cycle": 0.0,
        # Per-reason HOLD tally so the live log explains WHY we're not exiting.
        # Without this, "HOLD=193" for hours looks like a bug; with it we see
        # whether it's missing books vs market hasn't moved vs fees-swallow.
        "hold_missing_book": 0,
        "hold_market_not_moved": 0,
        "hold_near_resolution": 0,
        "hold_other": 0,
    }
    if not cfg.enabled:
        return summary

    open_trades = await db.list_open_paper_trades()
    summary["n_open"] = len(open_trades)
    if not open_trades:
        return summary

    # Parallel mark construction: each _build_mark fetches 2 orderbooks
    # independently. Bound concurrency so we don't hammer rate limits.
    sem = asyncio.Semaphore(max_concurrent)

    async def _bounded_build(t: dict) -> Optional[TradeMark]:
        async with sem:
            try:
                return await _build_mark(t, kalshi, poly)
            except Exception as e:
                log.error("monitor: trade %s build_mark error: %s", t.get("id"), e, exc_info=True)
                return None

    marks = await asyncio.gather(*(_bounded_build(t) for t in open_trades))

    # Decision/persistence loop is fast and stays serial — DB writes shouldn't
    # interleave, and ordering (oldest-first) is preserved.
    for trade, mark in zip(open_trades, marks):
        try:
            if mark is None:
                summary["skipped"] += 1
                continue
            recommendation, reason, unwind_size = _decide(mark, cfg, fee_cfg)
            mark.recommendation = recommendation
            mark.reason = reason

            partial_realized = 0.0
            if recommendation == "PARTIAL_UNWIND" and unwind_size > 0:
                gross_per_contract = (
                    mark.yes_leg.best_bid + mark.no_leg.best_bid
                    - mark.cost_per_contract
                )
                gross_realized = gross_per_contract * unwind_size
                exit_fees = (
                    compute_unwind_fees(
                        mark.buy_yes, mark.buy_no,
                        mark.yes_leg.best_bid, mark.no_leg.best_bid,
                        unwind_size, fee_cfg or {},
                    ) if fee_cfg is not None else 0.0
                )
                # Realized = NET of exit fees so partial_realized_usd accumulates
                # the post-fee dollars actually banked. Entry fees are subtracted
                # once at full close (apply_partial_unwind).
                partial_realized = round(gross_realized - exit_fees, 4)
                mark.partial_unwind_size = unwind_size
                mark.partial_unwind_realized = partial_realized

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
                "partial_unwind_size": (
                    round(mark.partial_unwind_size, 4)
                    if mark.partial_unwind_size > 0 else None
                ),
                "partial_unwind_realized_usd": (
                    partial_realized if mark.partial_unwind_size > 0 else None
                ),
            })
            summary["n_marked"] += 1

            if recommendation == "PARTIAL_UNWIND":
                summary["partial_unwinds"] += 1
                summary["realized_this_cycle"] += partial_realized
                log.info(
                    "PARTIAL_UNWIND trade #%d: %s | sold %.1f@$%.4f → realized $%.2f (cum partial=$%.2f) | %s",
                    trade["id"], trade["pair_id"][:60],
                    unwind_size,
                    mark.yes_leg.best_bid + mark.no_leg.best_bid,
                    partial_realized,
                    float(trade.get("partial_realized_usd") or 0) + partial_realized,
                    reason,
                )
                if dry_run:
                    result = await db.apply_partial_unwind(
                        trade["id"], unwind_size, partial_realized,
                    )
                    if result["fully_closed"]:
                        summary["fully_closed"] += 1
                        log.info(
                            "CLOSED trade #%d: %s | total realized $%.2f",
                            trade["id"], trade["pair_id"][:60],
                            result["partial_realized_usd"],
                        )
                        await db.add_pair_cooldown(
                            trade["pair_id"], f"paper-closed: realized ${result['partial_realized_usd']:.2f}",
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
                # Categorize HOLD reason for live-log breakdown
                r = (reason or "").lower()
                if "missing bid book" in r or "zero best-bid" in r:
                    summary["hold_missing_book"] += 1
                elif "<= cost" in r:
                    summary["hold_market_not_moved"] += 1
                elif "resolves in" in r and "hold for full" in r:
                    summary["hold_near_resolution"] += 1
                else:
                    summary["hold_other"] += 1
        except Exception as e:
            log.error("monitor: trade %s error: %s", trade.get("id"), e, exc_info=True)
            summary["skipped"] += 1

    return summary


async def _build_mark(
    trade: dict, kalshi: KalshiClient, poly: PolymarketClient,
) -> Optional[TradeMark]:
    yes_platform = trade["yes_platform"]
    yes_ticker = trade["yes_ticker"]
    yes_contracts_orig = float(trade["yes_contracts"] or 0)
    yes_paid = float(trade["yes_observed_price"] or 0)
    yes_size_usd = float(trade["yes_size_usd"] or 0)

    no_platform = trade["no_platform"]
    no_ticker = trade["no_ticker"]
    no_contracts_orig = float(trade["no_contracts"] or 0)
    no_paid = float(trade["no_observed_price"] or 0)
    no_size_usd = float(trade["no_size_usd"] or 0)

    if yes_contracts_orig <= 0 or no_contracts_orig <= 0:
        return None

    # Reject any trade with missing/zero entry prices. cost_per_contract
    # would be 0 → gross_profit would be a fabricated win on every cycle
    # (cycle keeps firing PARTIAL_UNWIND until contracts_remaining hits 0,
    # producing a 'closed' row with garbage realized profit). This was a
    # likely contributor to the 159 status='closed' realized=NULL trades.
    if yes_paid <= 0 or no_paid <= 0:
        log.warning(
            "monitor: trade #%s skipped — entry price missing "
            "(yes_paid=%.4f no_paid=%.4f)",
            trade.get("id"), yes_paid, no_paid,
        )
        return None

    # contracts_remaining tracks live position size; fall back to original
    # for legacy rows backfilled with NULL → original by the migration.
    contracts_remaining = float(
        trade.get("contracts_remaining")
        if trade.get("contracts_remaining") is not None
        else yes_contracts_orig
    )
    if contracts_remaining <= 0:
        return None

    yes_token = trade.get("yes_token")
    no_token = trade.get("no_token")

    # Fetch bid books for whichever side each leg lives on. Walk to
    # contracts_remaining (not original) since that's our actual live size.
    if yes_platform == "kalshi":
        yes_mark = await _bid_mark_kalshi(kalshi, yes_ticker, "yes", contracts_remaining)
    elif yes_platform == "polymarket" and yes_token:
        yes_mark = await _bid_mark_polymarket(poly, yes_token, contracts_remaining)
    else:
        # Polymarket leg without a stored token (older row from before the
        # token-capture migration) — can't price the unwind, fall through
        # as "book unavailable" so monitor refuses to recommend exit.
        yes_mark = LegMark(0.0, 0.0, 0.0, 0.0, contracts_remaining, False)

    if no_platform == "kalshi":
        no_mark = await _bid_mark_kalshi(kalshi, no_ticker, "no", contracts_remaining)
    elif no_platform == "polymarket" and no_token:
        no_mark = await _bid_mark_polymarket(poly, no_token, contracts_remaining)
    else:
        no_mark = LegMark(0.0, 0.0, 0.0, 0.0, contracts_remaining, False)

    # Cost basis: per the original entry, prorated to remaining contracts.
    # The actual paid-per-contract is fixed at entry; remaining cost basis
    # = remaining × cost_per_contract.
    cost_per_contract = yes_paid + no_paid
    cost_basis = contracts_remaining * cost_per_contract
    unwind_value = (
        yes_mark.vwap_bid * yes_mark.fill_contracts
        + no_mark.vwap_bid * no_mark.fill_contracts
    )
    # Locked payout: at resolution one leg pays $1/contract for whichever side
    # wins, on the contracts still held.
    locked_payout = contracts_remaining * 1.0
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

    # Reconstruct fee-relevant leg dicts from the stored paper_trade row so
    # the unwind-fee calculator can pick the right per-platform / category
    # rate without re-fetching market metadata. category is best-effort —
    # legacy rows may be NULL, in which case fees.py falls back to default.
    buy_yes_skel = {
        "platform": yes_platform,
        "category": trade.get("yes_category", ""),
        "yes_price": yes_paid,
    }
    buy_no_skel = {
        "platform": no_platform,
        "category": trade.get("no_category", ""),
        "no_price": no_paid,
    }

    return TradeMark(
        paper_trade_id=int(trade["id"]),
        yes_leg=yes_mark,
        no_leg=no_mark,
        cost_basis=cost_basis,
        cost_per_contract=cost_per_contract,
        contracts_remaining=contracts_remaining,
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
        buy_yes=buy_yes_skel,
        buy_no=buy_no_skel,
    )
