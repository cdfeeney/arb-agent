# Correlated Lag Detection — Design

## Thesis

When BTC (or another underlying) moves significantly, prediction markets that
reference that underlying lag the spot price by 10-60 seconds. During that
window, the market price is stale and tradeable: a buyer can take the lagged
side knowing the market will reprice toward the new spot.

**This is a directional bet, not an arbitrage.** Unlike cross-platform arb,
there is no guaranteed payout — only an expected mean-reversion. The signal
must be sized accordingly (smaller than arb positions).

## Scope of v1 (this build)

- **Underlying**: BTC/USD only. ETH/SOL/etc. follow same template later.
- **Markets**: Kalshi crypto-category markets that explicitly reference BTC
  price in the question. (Polymarket has fewer such markets; defer to v2.)
- **Signal**: BTC moved >X% in the last N seconds AND target market hasn't
  repriced to match.
- **Output**: rows in a new `lag_signals` table with platform/ticker/direction
  /implied edge. NO order placement. Paper-trail style for next morning's
  analysis.
- **Process model**: BTC feed runs as a long-lived asyncio task alongside the
  existing 30s arb poll loop. Lag detector runs once per arb cycle.

## What's NOT in v1

- Polymarket crypto markets (Gamma rate limits + no live WS feed yet)
- Other underlyings (ETH, oil, SPX) — same pattern, different feed
- Backtest framework — needs historical Binance data + historical Kalshi
  snapshots which we don't have. Live paper trading is the v1 validation.
- Order placement
- Dynamic sizing (Kelly, etc.)
- Sub-30s polling for crypto markets specifically (would catch faster signals
  but adds complexity; do this in v2 if signal is real)

## Data flow

```
Binance WS  ──►  BTCFeed (background task, ring buffer of last 600s of ticks)
                                              │
                                              ▼
PollingAgent._poll_once() ───►  [existing arb path, untouched]
                          │
                          └───►  lag_detector.scan(crypto_markets, btc_feed)
                                              │
                                              ▼
                                  for each crypto market:
                                    - get current and N-seconds-ago BTC price
                                    - get current and N-seconds-ago market mid
                                    - compute expected market move from BTC move
                                    - compare to actual market move
                                    - if mismatch > threshold, emit signal
                                              │
                                              ▼
                                    save to lag_signals table
                                              │
                                              ▼
                                    log signal with URL (manual verify next AM)
```

## Crypto market identification

Two filters, both must hold:
1. Kalshi market `event_ticker` starts with `KXBTC` or `KXETH` etc.
   (the prefix list is in `LAG_CRYPTO_TICKER_PREFIXES`)
2. Question contains BTC/Bitcoin/ETH/Ethereum/etc. (catches edge cases
   where the ticker prefix is generic)

False positives are fine — the signal threshold filters them. False
*negatives* (missing real crypto markets) is what we're guarding against.

## Signal model (v1, deliberately simple)

For a binary market "BTC > $X by date Y", a 1% spot move doesn't translate
to a 1% probability shift — it depends on time-to-expiry, current price vs
strike, and implied volatility. A proper model is Black-Scholes-on-binary.

For v1 we use a **directional heuristic**: if BTC moves significantly
in either direction within a short window, the market YES probability should
move in the SAME direction (positive correlation for "BTC > X" type markets).
We're not estimating magnitude — we're flagging "market hasn't moved at all
while BTC did." If the market price is roughly flat (<0.5pp change) AND BTC
moved >`btc_threshold_pct` (default 2%) in the last `window_seconds`
(default 60), emit a signal.

Direction:
- BTC up + market flat → BUY YES (market should reprice up)
- BTC down + market flat → BUY NO (market should reprice down)

This is intentionally crude. v2 introduces a proper sensitivity model.

## Schema: `lag_signals`

```sql
CREATE TABLE lag_signals (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    detected_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Target market
    market_platform     TEXT NOT NULL,
    market_ticker       TEXT NOT NULL,
    market_event_ticker TEXT,
    market_question     TEXT,
    market_url          TEXT,
    market_closes_at    TIMESTAMP,

    -- BTC observation window
    underlying          TEXT NOT NULL,        -- "BTC", "ETH", etc.
    btc_price_t0        REAL,                 -- price at start of window
    btc_price_t1        REAL,                 -- price now
    btc_pct_change      REAL,                 -- (t1-t0)/t0
    window_seconds      INTEGER,

    -- Market observation
    market_price_t0     REAL,                 -- mid price at start of window
                                              -- (NULL on first sighting; populated
                                              -- once we have history)
    market_price_t1     REAL,                 -- mid price now
    market_pp_change    REAL,                 -- (t1-t0) in percentage points

    -- Signal
    direction           TEXT,                 -- BUY_YES | BUY_NO
    signal_strength     REAL,                 -- |btc_pct_change| / max(|market_pp_change|, 0.001)

    -- Resolution (filled in later for paper-trade analysis)
    market_price_t2     REAL,                 -- mid price 60s after detection
    market_repriced     INTEGER,              -- 1 if market moved in expected direction
    revert_seconds      INTEGER,              -- time until market moved >= predicted

    status              TEXT DEFAULT 'open'   -- open | observed | expired
);
CREATE INDEX idx_lag_status ON lag_signals(status, detected_at);
CREATE INDEX idx_lag_market ON lag_signals(market_ticker, detected_at);
```

## Configuration (added to `config.yaml`)

```yaml
lag:
  enabled: true
  underlying: "BTC"
  feed:
    source: "binance"   # only option in v1
    symbol: "btcusdt"
    reconnect_seconds: 5
  detection:
    window_seconds: 60
    btc_threshold_pct: 2.0   # |BTC change| must exceed this to consider signal
    market_flat_threshold_pp: 0.5   # market mid moved less than this = "flat"
    min_market_volume: 500
  ticker_prefixes: ["KXBTC", "KXBTCD", "KXETH"]
  question_keywords: ["bitcoin", "btc", "ethereum", "eth"]
```

## Files added / modified

NEW:
- `src/clients/btc_feed.py` — Binance WS client + ring buffer
- `src/engine/lag_detector.py` — filter + signal logic
- `LAG_DESIGN.md` (this file)

MODIFIED:
- `src/db/store.py` — add `lag_signals` table and helpers
- `src/agent/poller.py` — start BTC feed task; call lag detector each cycle
- `config.yaml` — add `lag` section
- `requirements.txt` — add `websockets` for the BTC feed

## v1 success criteria (next-morning check)

- BTC feed has been receiving ticks for >12 hours with <1% downtime in log
- `lag_signals` table has >0 rows (or honest 0 if BTC was flat overnight)
- Manual spot-check: open the URL of the top signal by `signal_strength`, verify
  on Kalshi that the market price was indeed lagging BTC at `detected_at`
- For at least one signal, the resolution column shows whether the market
  repriced within 60s (this requires the next polling cycle to populate
  `market_price_t2`)

## v2 ideas (not now)

- Proper Black-Scholes-on-binary sensitivity model (use time-to-expiry,
  current price vs strike, implied vol from market)
- Sub-30s polling on identified crypto markets
- ETH, SPX, oil underlyings
- Polymarket crypto markets via CLOB WS subscription
- Backtest framework using Binance historical kline data + saved market snapshots
- Order placement gated by paper-trade success metric (e.g. >60% of signals
  see market move in predicted direction within 90s)
