# Arb Agent — Full Build Plan

## What It Does
Polls Kalshi and Polymarket every 30 seconds, finds correlated prediction markets across both platforms, detects when their combined prices imply a profit (implied_sum < 0.95), sizes a position using configurable rules (Kelly + caps), deduplicates alerts via SQLite, and surfaces opportunities in a rich terminal table + optional Slack.

---

## Project Structure

```
arb-agent/
├── config.yaml              ← ALL tunable thresholds (no hardcoded numbers in code)
├── requirements.txt
├── main.py                  ← async entry point
├── .env.example
├── src/
│   ├── config.py            ← loads config.yaml, overrides from env vars
│   ├── clients/
│   │   ├── kalshi.py        ← authenticated REST client, full market pagination
│   │   └── polymarket.py    ← Gamma API client, no auth needed
│   ├── engine/
│   │   ├── normalizer.py    ← raw API → {platform, question, yes_price, no_price, volume, closes_at, url}
│   │   ├── matcher.py       ← rapidfuzz fuzzy matching + expiry proximity filter
│   │   ├── arb_detector.py  ← core arb logic: both bet directions, profit_pct
│   │   └── sizing.py        ← position sizing rules engine (Kelly + 4 caps)
│   ├── db/
│   │   └── store.py         ← SQLite via aiosqlite: dedup + opportunity history
│   ├── alerts/
│   │   └── notifier.py      ← rich terminal table + Slack webhook
│   ├── promotions/
│   │   └── tracker.py       ← free bet / deposit match / odds boost EV calculators
│   └── agent/
│       └── poller.py        ← asyncio polling loop, orchestrates everything
└── data/
    └── opportunities.db     ← gitignored, auto-created
```

---

## Build Phases

### Phase 1 — Foundation (DONE)
- [x] Project scaffold, all __init__.py files
- [x] config.yaml with all thresholds
- [x] requirements.txt
- [x] main.py async entry point
- [x] src/config.py (load + env override)

### Phase 2 — API Clients (DONE)
- [x] KalshiClient — Bearer auth, paginated GET /markets, semaphore rate limiting
- [x] PolymarketClient — Gamma API, offset pagination, no auth

### Phase 3 — Normalization (DONE)
- [x] normalize_kalshi() — cents→0-1 price, mid bid/ask, dateutil parse
- [x] normalize_polymarket() — outcomePrices JSON array, slug URL

### Phase 4 — Matching Engine (DONE)
- [x] _preprocess() — lowercase, strip punct, remove stopwords
- [x] match_markets() — token_sort_ratio ≥ threshold AND expiry proximity check

### Phase 5 — Arb Detection (DONE)
- [x] detect_arb() — both directions, skip near-expiry, return profit_pct + pair_id

### Phase 6 — Position Sizing Rules (DONE)
- [x] size_position() — Kelly → fractional Kelly → bankroll cap → liquidity cap → max_bet → min_bet
- [x] Sizing transparency: reports which rule was the binding constraint

### Phase 7 — Persistence (DONE)
- [x] Database.init() — SQLite schema, index on (pair_id, seen_at)
- [x] seen_recently() — dedup window check
- [x] save_opportunity() — full JSON blob for post-analysis

### Phase 8 — Alerting (DONE)
- [x] alert_terminal() — rich table with all fields
- [x] alert_slack() — optional webhook, silent failure

### Phase 9 — Promotions Module (DONE)
- [x] calculate_free_bet_arb() — SNR free bet hedging math
- [x] apply_active_promos() — apply configured promos to each opportunity

### Phase 10 — Polling Agent (DONE)
- [x] PollingAgent.run() — infinite async loop
- [x] _poll_once() — concurrent fetch, filter, match, detect, size, alert, save

---

## TODO / Next Steps

### Immediate (before first live run)
- [ ] Register on Kalshi, generate API key, add to .env
- [ ] `pip install -r requirements.txt`
- [ ] Test with `python main.py` — watch for auth errors, pagination, price parsing
- [ ] Tune `similarity_threshold` — start at 80, raise if too many false pairs
- [ ] Tune `min_profit_pct` — 2% is conservative; lower to 1% to see more candidates

### Phase 11 — Embedding-based matching (optional upgrade)
- [ ] Add `text-embedding-3-small` based semantic matching as fallback
- [ ] Add config flag `matching.use_embeddings: false` to toggle
- [ ] Cache embeddings to avoid re-computing every poll

### Phase 12 — Additional platforms
- [ ] PredictIt client (requires auth, limited API)
- [ ] Manifold Markets client (open API)
- [ ] Betfair Exchange (requires account + API key)

### Phase 13 — Dashboard
- [ ] Rich live dashboard showing all open opportunities in a table
- [ ] Refresh in-place instead of appending new tables

### Phase 14 — Automated execution (advanced)
- [ ] Kalshi order placement via POST /orders
- [ ] Polymarket CLOB order placement
- [ ] Risk management: global exposure limits, per-platform limits

---

## Key Config Knobs

| Parameter | Default | Effect |
|-----------|---------|--------|
| `similarity_threshold` | 85 | Lower = more pairs found, more false positives |
| `min_profit_pct` | 0.02 | Lower = more opportunities but tighter margin |
| `kelly_fraction` | 0.25 | Higher = bigger bets, more variance |
| `min_volume` | 5000 | Lower = more illiquid markets included |
| `interval_seconds` | 30 | Lower = faster alerts, more API calls |
| `dedup_window_minutes` | 60 | Higher = fewer repeat alerts |

---

## Arb Math Reference

```
# Standard cross-platform arb
implied_sum = yes_price_A + no_price_B
profit_pct  = 1 - implied_sum   (if implied_sum < 1)

# Example:
# Kalshi YES = 0.44, Polymarket NO = 0.44
# implied_sum = 0.88 → 12% gross profit
# After fees ~1-2% each side: ~8-10% net

# Free bet SNR arb
win_profit = free_bet * (1/yes_price - 1)
hedge_cost = win_profit * no_price_elsewhere
locked_profit = win_profit - hedge_cost
```
