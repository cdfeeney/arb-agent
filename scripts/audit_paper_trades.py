"""Audit paper_trades on the droplet for structurally bad pairs.

Pulls every (id, yes_platform, yes_ticker, no_platform, no_ticker, yes_question,
no_question, status) row, identifies the Polymarket side, fetches the live
Gamma record for that market id, and classifies:

  * neg_risk_sub        — negRisk=true AND non-empty groupItemTitle
                          (sub-outcome of multi-outcome basket; cannot pair
                           with Kalshi YES/NO binary; root cause of #395)
  * neg_risk_only       — negRisk=true AND empty groupItemTitle
                          (suspicious; standalone negRisk markets are rare)
  * group_only          — non-empty groupItemTitle but negRisk=false
                          (e.g. recurring monthly series; usually OK but flag)
  * binary              — neg_risk=false AND empty groupItemTitle (good)
  * fetch_failed        — Gamma returned no record (poly market deleted/etc)

Run from local repo root with ARB_DROPLET_SSH_PASS set:
    python scripts/audit_paper_trades.py --status open
    python scripts/audit_paper_trades.py --all > audit.json
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Iterable
from urllib.request import urlopen, Request

# Add scripts/ to sys.path so we can import the SSH helper.
sys.path.insert(0, __file__.rsplit("/", 1)[0] if "/" in __file__ else ".")
sys.path.insert(0, __file__.rsplit("\\", 1)[0] if "\\" in __file__ else ".")
from _ssh_helper import run as ssh_run  # noqa: E402


GAMMA = "https://gamma-api.polymarket.com/markets/{id}"
DB = "/root/arb-agent/data/opportunities.db"


def fetch_paper_trades(status_filter: str | None) -> list[dict]:
    """Pull paper_trades rows from the droplet. Returns list of dicts."""
    where = ""
    if status_filter and status_filter != "all":
        where = f"WHERE status='{status_filter}'"
    sql = (
        "SELECT id, status, yes_platform, yes_ticker, no_platform, no_ticker, "
        "yes_question, no_question, detected_at FROM paper_trades "
        f"{where} ORDER BY id;"
    )
    # -separator '|' and no headers for stable parsing.
    cmd = f"sqlite3 -separator '|' {DB}"
    rc, out, err = ssh_run(cmd, stdin_data=sql + "\n")
    if rc != 0:
        raise RuntimeError(f"sqlite3 failed rc={rc}: {err}")

    rows = []
    for line in out.strip().split("\n"):
        if not line.strip():
            continue
        parts = line.split("|")
        if len(parts) < 9:
            continue
        rows.append({
            "id": int(parts[0]),
            "status": parts[1],
            "yes_platform": parts[2],
            "yes_ticker": parts[3],
            "no_platform": parts[4],
            "no_ticker": parts[5],
            "yes_question": parts[6],
            "no_question": parts[7],
            "detected_at": parts[8],
        })
    return rows


def poly_id_for(row: dict) -> str | None:
    if row["yes_platform"] == "polymarket":
        return row["yes_ticker"]
    if row["no_platform"] == "polymarket":
        return row["no_ticker"]
    return None


def fetch_gamma_market(market_id: str) -> dict | None:
    url = GAMMA.format(id=market_id)
    req = Request(url, headers={"User-Agent": "arb-agent-audit/1.0"})
    try:
        with urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data if isinstance(data, dict) else None
    except Exception as e:
        print(f"  gamma fetch failed for {market_id}: {e}", file=sys.stderr)
        return None


def classify(market: dict | None) -> str:
    if market is None:
        return "fetch_failed"
    neg_risk = bool(market.get("negRisk"))
    group = market.get("groupItemTitle") or ""
    if neg_risk and group:
        return "neg_risk_sub"
    if neg_risk:
        return "neg_risk_only"
    if group:
        return "group_only"
    return "binary"


def audit(rows: Iterable[dict]) -> list[dict]:
    out = []
    for r in rows:
        pid = poly_id_for(r)
        if pid is None:
            r["classification"] = "no_polymarket_side"
            out.append(r)
            continue
        market = fetch_gamma_market(pid)
        cls = classify(market)
        r["classification"] = cls
        r["poly_id"] = pid
        if market:
            r["poly_neg_risk"] = bool(market.get("negRisk"))
            r["poly_group_item_title"] = market.get("groupItemTitle") or ""
            r["poly_slug"] = market.get("slug")
            evs = market.get("events")
            if isinstance(evs, list) and evs and isinstance(evs[0], dict):
                r["poly_event_slug"] = evs[0].get("slug")
        out.append(r)
        # Be polite to Gamma.
        time.sleep(0.05)
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--status",
        default="open",
        help="status filter (open, paper_archived, archived, closed, all)",
    )
    p.add_argument("--json", action="store_true", help="dump JSON instead of table")
    args = p.parse_args()

    rows = fetch_paper_trades(args.status)
    print(f"Fetched {len(rows)} paper_trades with status={args.status}", file=sys.stderr)
    audited = audit(rows)

    if args.json:
        print(json.dumps(audited, indent=2, default=str))
        return

    # Table summary
    counts: dict[str, int] = {}
    for r in audited:
        counts[r["classification"]] = counts.get(r["classification"], 0) + 1
    print("\n=== Classification counts ===")
    for k, v in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(f"  {k:<22}  {v}")

    suspect = [r for r in audited if r["classification"] in ("neg_risk_sub", "neg_risk_only", "group_only")]
    if suspect:
        print(f"\n=== Suspect rows ({len(suspect)}) ===")
        for r in suspect:
            print(
                f"  id={r['id']:>5}  status={r['status']:<14}  "
                f"cls={r['classification']:<14}  "
                f"poly_id={r.get('poly_id'):<10}  "
                f"group='{r.get('poly_group_item_title','')[:40]}'  "
                f"yes_q='{r['yes_question'][:50]}'"
            )


if __name__ == "__main__":
    main()
