# Cross-Platform Arbitrage Strategy

Goal: maximize expected profit at the lowest sustainable risk, given a $10,000 bankroll split across Kalshi + Polymarket. No category is excluded *a priori* — each is rated on edge persistence, liquidity, execution risk, and resolution risk.

---

## TL;DR — what to focus the agent on

1. **US federal politics (long-dated, binary)** — primary profit driver. Both platforms compete for the same flows; mispricings persist for hours.
2. **Fed / macro thresholds** — highest safety, thinnest edges, but extremely repeatable. Good for "always-on" base flow.
3. **Crypto round-number milestones** — Polymarket-heavy, Kalshi opportunistic. Mid-volatility, 1-4% edges common near key levels.
4. **Sports — season-long outright winners only** — Kalshi's sports expansion (2025) created fresh Polymarket parity gaps.
5. **Geopolitics + AI milestones** — opportunistic. Lower volume but occasional 5-10% edges around news.

**Avoid** (already filtered or to-be-filtered): multi-outcome events, live in-game markets, 15-min/hourly contracts, events resolving in <24h, anything with divergent resolution sources.

---

## Realistic profit math

> **These ranges are priors, not measurements.** They reflect what cross-platform binary arb operations *typically* report at this scale, but we have zero data of our own yet. Replace with measured paper-trading numbers before sizing real capital.

With **$10k bankroll, quarter-Kelly sizing, 2% min net edge**, the prior expectation is:
- Average bet size: $200–500 per opp (capped by liquidity_cap_pct = 10%)
- Average net edge captured: 1.5–3% after fees + slippage
- Opportunity flow: 2–8 valid arbs/day once filters are stable
- Base case: $10–50/day → $300–1,500/month
- Bull case (election/Fed/championship week): $200–1,000/day for short bursts
- Bear case: $0 weeks when markets don't diverge

**These are anchors to test against, not promises.** Paper-trading is the gate.

The structural framing IS measured: cross-platform arb is a *grind*, not a lottery. The capital is the limit, not the opportunity count. To 10x profit you 10x the bankroll, not the polling rate.

**What kills the EV**: false matches (LLM solves), partial fills (need atomic execution), drift between leg fills (need fast execution), fees (already modeled).

---

## Market categories ranked

### 1. US federal politics — long-dated binary
**Examples**: "Will a Democrat win the 2028 presidency?", "Will Republicans control the Senate after 2026 midterms?", "Will Trump approval > 45% on date X?"

- **Liquidity**: $$$ on both platforms; Polymarket dominates volume but Kalshi has competitive depth on majors.
- **Edge size**: 1–3% baseline, 3–8% near news/debates.
- **Edge persistence**: hours-to-days. Polymarket's crypto-native LPs and Kalshi's USD retail price differently — gap stays open.
- **Risk**: low resolution risk on majors; both platforms reference AP/major news sources.
- **Action**: this is the bread-and-butter. Highest weight.

### 2. Fed / macroeconomic thresholds
**Examples**: "Will the Fed cut rates at the December 2025 FOMC meeting?", "Will CPI YoY > 3% in next print?", "Will GDP growth < 2% in next release?"

- **Liquidity**: $$ on both; Kalshi marketed heavily on macro.
- **Edge size**: 0.5–2% — thin but very stable.
- **Edge persistence**: days. Same publicly-watched data sources.
- **Risk**: lowest of any category. Both platforms cite the same Fed/BLS/BEA sources.
- **Action**: enable always — a steady source of small base flow.

### 3. Crypto round-number milestones
**Examples**: "Will BTC > $120k by date X?", "Will ETH > $5k by date X?"

- **Liquidity**: $$$ on Polymarket, $ on Kalshi (Kalshi has crypto but smaller).
- **Edge size**: 1–4% near key levels; can balloon to 8% on volatility spikes.
- **Edge persistence**: short — minutes to hours. Crypto-native bots arb fast on Polymarket.
- **Risk**: medium — different time-zone cutoffs are a common trap (ET vs UTC). LLM verifier should catch this.
- **Action**: enable, but tighter min edge (3%+) given execution risk.

### 4. Sports — season-long outright only
**Examples**: "Will the Chiefs win Super Bowl LXII?", "Will Caitlin Clark win MVP?"

- **Liquidity**: $$ growing on both. Kalshi's 2025 sports expansion created Polymarket parity gaps.
- **Edge size**: 1–4% on majors, occasionally larger on niche.
- **Edge persistence**: days; sports books arb less aggressively across these two.
- **Risk**: low resolution risk; medium price-drift risk during games.
- **Action**: enable for outright winners. **Hard exclude** any in-game / live / quarterly markets.

### 5. Geopolitics
**Examples**: "Will a Ukraine-Russia ceasefire be signed by date X?", "Will sanctions on country X be lifted in 2026?"

- **Liquidity**: $ — small books on both.
- **Edge size**: 2–10% on news.
- **Edge persistence**: medium.
- **Risk**: HIGH resolution risk — "ceasefire" definitions can differ between platforms.
- **Action**: enable but rely heavily on LLM verification — these are exactly the markets where two platforms phrase the resolution differently.

### 6. AI / tech milestones
**Examples**: "Will OpenAI release GPT-5 by date X?", "Will Anthropic IPO in 2026?"

- **Liquidity**: $ — thin.
- **Edge size**: 2–15% on news, near zero otherwise.
- **Edge persistence**: hours-to-days.
- **Risk**: medium — definitions can be slippery ("released" vs "announced").
- **Action**: opportunistic. Won't move bankroll much but worth scanning.

### 7. Pop culture / awards
**Examples**: Oscars, Grammys, Person of the Year.

- **Liquidity**: $ — tiny except event week.
- **Edge size**: 1–5% baseline, can spike to 10% on idiosyncratic flows.
- **Action**: enable, no special handling.

---

## What we will NOT touch

- **Multi-outcome events** (KXFEDDISSENT-style): already filtered via `filter_binary_kalshi`.
- **Live / in-game markets**: Kalshi doesn't really do these; Polymarket does. Even if they did parity, latency makes us a guaranteed loser to colocated bots.
- **Sub-24h markets**: not enough time to fill, observe, react if something goes wrong. Already filtered via `min_hours_to_close`.
- **15-min / hourly markets**: noise, no real arb edge after fees.
- **Markets where one platform has < $5k volume**: already filtered via `min_volume`.
- **Politician-specific dissent / vote-by-name questions**: structurally not arbitrageable as a binary against a broader outcome.
- **Very-different-resolution-source pairs**: LLM verifier filters these.

---

## Operational risk discipline (non-negotiable)

These are the rules that distinguish a profitable arb operation from one that randomly walks to zero:

1. **Atomic execution or no execution.** Both legs must fill at acceptable prices, or roll back. A naked single leg is just gambling.
2. **Liquidity > edge.** A 5% edge on a market with $1k depth is worse than a 1.5% edge on a $100k market — the small one will move against you while you fill it. `liquidity_cap_pct = 10%` enforces this.
3. **Quarter-Kelly maximum.** Full-Kelly is theoretically optimal for repeated bets but assumes perfect edge estimation. We're estimating with noise — quarter-Kelly is the standard discount.
4. **Bankroll cap per opp = 5%.** No single opportunity, however juicy-looking, gets more than 5% of the $10k. Recovers from any single bad outcome.
5. **Min net edge = 2% AFTER fees.** Anything tighter gets eaten by slippage and gas.
6. **Daily loss kill-switch.** If realized P&L < -3% of bankroll on the day, the agent halts and pings you. Not yet implemented — add before going live.
7. **Per-platform exposure cap.** Don't let either Kalshi or Polymarket hold > 60% of total open exposure. Platform risk is real (insolvency, regulator action, contract dispute).

---

## Where the bias-free analysis lands

The user's instruction was *"don't limit profit due to my bias."* The honest answer:

- **The biggest profit opportunities are outside cross-platform binary arbs entirely** — they're in market-making, intra-platform multi-outcome arbs (buying all NOs in a 12-outcome event for < $1.00 total), and event-driven bets. Those require different infrastructure than this agent.
- **Within cross-platform arbs**, no category is being artificially excluded. We're filtering only on three real dimensions: (1) is it actually binary, (2) is it actually the same event, (3) is there enough liquidity to fill. Everything else is fair game.
- **Higher-EV variants of this same agent** would expand to: more platforms (PredictIt, Manifold, Limitless), websocket-based monitoring, and order-book-level fill simulation.

If you want one of those next, those are real next-phase upgrades. But none of them changes the fundamental: cross-platform arb is a capital-bound grind, and our $10k bankroll is the binding constraint — not our market selection.

---

## Recommended config tuning (next deploy)

Given the analysis above, suggested overrides to `config.yaml`:

```yaml
filters:
  min_volume: 5000          # keep — a real liquidity floor
  min_hours_to_close: 24    # keep
  max_days_to_close: 14     # KEEP at 14. Earlier debugging found longer-dated Kalshi markets
                            # come back unquoted (no bid/ask), generating noise but no signal.
                            # Only widen this AFTER re-testing whether the unquoted-market issue
                            # is platform-wide or was specific to a market segment we already filter.
  min_profit_pct: 0.02      # keep — anything tighter gets eaten

matching:
  similarity_threshold: 65  # already set — broad net, LLM filters
  expiry_proximity_hours: 72

llm:
  enabled: true
  max_pairs_per_cycle: 100  # was 50 — at our current pair counts we should never hit this anyway
  cache_hours: 24
```

Add later (post paper-trading):
- `risk.daily_loss_kill_pct: 0.03`
- `risk.platform_exposure_cap_pct: 0.60`
