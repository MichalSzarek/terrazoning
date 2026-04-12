---
name: trading-review
description: Active verification of MATHS trading logic invariants. Auto-injected when trading-critical files are modified.
globs:
  - src/agents/**/*.py
  - src/risk/**/*.py
  - src/execution/**/*.py
  - src/services/trading_service.py
  - src/services/crypto_trading_service.py
  - src/services/position_monitor.py
  - src/data/**/*.py
---

# Trading Review — Domain Invariant Checklist

This skill provides the 8-category domain invariant reference for MATHS trading logic. When trading-critical files are modified, remind the developer to run `/trading-review` before committing.

## 8 Domain Invariant Categories

### 1. Risk Gate Integrity
The `VetoGate` in `src/risk/veto_gate.py` enforces 9 hard rules. None may be weakened or removed:

| Rule | Invariant |
|------|-----------|
| Max position size | 2% of portfolio per trade |
| Daily loss limit | 5% max daily drawdown |
| VIX veto | VIX > 35 vetoes ALL shorts |
| Concentration | Max 20% in single sector |
| Drawdown halt | 10% portfolio drawdown halts trading |
| Cash reserve | Minimum 30% cash buffer |
| Correlation | Max 3 correlated positions |
| After-hours | No new positions outside market hours |
| Margin | Sufficient margin before entry |

- `daily_pnl` must default to `0.0` (safe — assumes no loss, does NOT skip check)
- Squeeze gate is skipped when `squeeze_input is None` (data unavailable ≠ veto)

### 2. Order Safety
All orders must be **limit orders**. Market orders are forbidden.

- Equity shorts use **bracket orders**: SELL LIMIT (parent) + BUY STOP SL (child) + BUY LIMIT TP (child)
- SL for shorts is a BUY STOP **above** entry price (covers unlimited upside risk)
- TP for shorts is a BUY LIMIT **below** entry price
- `OrderType.MARKET` or `MKT` must never appear in `src/execution/`

### 3. Position Lifecycle
- `opened_at` defaults to `datetime.now(UTC)` — never `None`
- `stop_loss=None` must be handled gracefully (no crash, log warning)
- `atr_at_entry` must propagate from entry through monitoring to exit
- Double-close prevention: check position exists before closing
- State machine: IDLE → SCANNING → ANALYZING → PENDING_EXECUTION → EXECUTED → MONITORING → CLOSING → CLOSED

### 4. Concurrency & Races
- `_get_shorted_symbols()` must run at scan-time to filter out symbols with open positions
- **Known gap:** No execution-time distributed lock — acknowledged, not yet implemented
- Monitor cycle overlap: only one monitor cycle runs at a time (cron interval > cycle duration)

### 5. Data Quality & Staleness
- Borrow fee data freshness: must be < 300 seconds old
- `data_quality` should be validated as proper enum (`HIGH`/`MEDIUM`/`LOW`), not arbitrary string
- **Known gap:** `check_data_freshness` method exists but is not called in the production path
- All external API calls use `tenacity` retries with exponential backoff

### 6. Conviction & Scoring
- 4-way weights: Technical 30% + Fundamentals 30% + Macro 20% + Sentiment 20% = 100%
- When `fund_low=True`: fundamentals score falls back to neutral 5.0
- **Known gap:** When `sent_low=True`: sentiment score does NOT fall back to 5.0 (asymmetric behavior)
- `LOW_CONTEXT_CAP` = 5.0 when both fundamentals AND sentiment have low context
- EXTREME volatility regime caps final conviction at 3.0
- Hysteresis: conviction changes require minimum delta to flip signal

### 7. Crypto-Specific
- `InvalidCryptoShortError` guard: crypto pipeline must reject SHORT signals
- 30% max crypto exposure cap — **Known gap:** defaults to 0 existing exposure (always passes)
- Fear & Greed Index > 80 should veto (extreme greed)
- Crypto trading hours enforcement (24/7 but with maintenance windows)
- Crypto uses LONG/WATCH/HOLD signals (never SHORT)

### 8. Data Pipeline Integrity
- **Known gap:** yfinance client has no HTTP 429 retry logic
- `fast_info` attribute is not used — relies on `info` dict (slower but more complete)
- Twelve Data client retries on 200+error response but **not** HTTP 429
- `has_data` gate checks presence of key fields but excludes `current_ratio` and `roe`
- D/E (Debt-to-Equity) normalization boundary: values above 10.0 are capped

## When This Skill Activates

This skill is contextually loaded when files matching the glob patterns above are being worked on. It serves as reference material for the `/trading-review` slash command and as a passive reminder of domain invariants during development.

## Action Required

When you see this skill loaded, remind the developer:

> Trading-critical files detected. Run `/trading-review` before committing to verify domain invariants.
