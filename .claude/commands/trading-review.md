# /trading-review — Active Trading Logic Verification

You are performing a **self-challenge review** of trading-critical code changes. You must **actively verify** safety — not rubber-stamp a checklist. Every category requires **evidence** (file:line references or command output).

---

## Phase 1: Automated Scans (must ALL pass)

Run each scan. If ANY scan finds violations, that category is **auto-FAIL** — you cannot override.

### Scan 1: Market Orders
```bash
grep -rn "MARKET\|MKT\|market_order\|OrderType.MARKET" src/execution/ src/risk/ 2>/dev/null || echo "PASS: No market orders found"
```
**FAIL if:** Any match found in execution or risk layers.

### Scan 2: print() in src/
```bash
grep -rn "print(" src/ --include="*.py" | grep -v "structlog\|logger\|#.*print\|\".*print\|'.*print\|test\|__repr__\|__str__" || echo "PASS: No print() found"
```
**FAIL if:** Any non-logging print() found.

### Scan 3: Bare except
```bash
grep -rn "except:" src/ --include="*.py" | grep -v "# noqa" || echo "PASS: No bare except found"
```
**FAIL if:** Any bare `except:` without `# noqa`.

### Scan 4: pip usage
```bash
grep -rn "pip install\|pip freeze" scripts/ Makefile Dockerfile docker-compose.yml 2>/dev/null || echo "PASS: No pip usage found"
```
**FAIL if:** Any pip usage in build/deploy files.

### Scan 5: Domain Invariant Tests
```bash
uv run pytest tests/test_4way_debater.py tests/test_fundamentals_agent.py tests/test_sentiment_agent.py tests/test_equity_pre_check.py tests/test_squeeze_risk.py tests/test_borrow_gate.py tests/test_earnings_gate.py -v --tb=short 2>&1 | tail -20
```
**FAIL if:** Any test fails. Report which tests failed.

---

## Phase 2: Domain Verification (8 Categories)

For each category, **read the relevant files** and answer the verification questions with `file:line` evidence. "Looks good" is NOT acceptable — cite specific lines.

### Category 1: Risk Gate Integrity
- Read `src/risk/veto_gate.py` — are all 9 veto rules intact and unmodified?
- Does `daily_pnl` default to `0.0` (safe default)?
- Is squeeze gate skipped when `squeeze_input is None`?
- Is VIX > 35 an absolute veto?

### Category 2: Order Safety
- Grep results from Scan 1 confirm no MARKET orders
- Read `src/execution/ibkr_client.py` — verify bracket order structure: SELL LIMIT parent + BUY STOP (SL) child + BUY LIMIT (TP) child
- For shorts: is SL direction correct (BUY STOP above entry)?

### Category 3: Position Lifecycle
- Read position loading code — does `opened_at` default to `now()`?
- Is `stop_loss=None` handled without crash?
- Is `atr_at_entry` propagated through the lifecycle?
- Is double-close prevented?

### Category 4: Concurrency & Races
- Read scan-time filter — does `_get_shorted_symbols()` run before scanning?
- Acknowledge known gap: no execution-time distributed lock
- Is monitor cycle overlap prevented?

### Category 5: Data Quality & Staleness
- Read borrow freshness check — is threshold < 300 seconds?
- Is `data_quality` validated as proper enum or plain string?
- Acknowledge known gap: `check_data_freshness` exists but is not called in production path

### Category 6: Conviction & Scoring
- Verify 4-way weights sum to 100% (Tech 30 + Fund 30 + Macro 20 + Sent 20)
- Check neutral 5.0 fallback when `fund_low=True`
- Acknowledge known gap: `sent_low=True` has NO 5.0 neutral fallback (asymmetric)
- `LOW_CONTEXT_CAP` = 5.0 when both fundamentals and sentiment are missing
- EXTREME vol regime caps conviction at 3.0

### Category 7: Crypto-Specific
- Verify `InvalidCryptoShortError` guard exists
- Check 30% exposure cap — note it defaults to 0 existing exposure (gap)
- F&G > 80 should veto
- Trading hours enforcement present

### Category 8: Data Pipeline Integrity
- Acknowledge known gap: yfinance has no 429 retry logic
- `fast_info` is not used (potential staleness)
- Twelve Data retries on 200+error but not HTTP 429 (gap)
- `has_data` gate excludes `current_ratio` and `roe`
- D/E normalization boundary at 10.0

---

## Phase 3: Report

Output a structured report in this exact format:

```
## /trading-review Report

**Date:** <today>
**Branch:** <current branch>
**Files changed:** <list from git diff --name-only>

### Phase 1: Automated Scans
| Scan | Result |
|------|--------|
| Market Orders | PASS/FAIL |
| print() | PASS/FAIL |
| Bare except | PASS/FAIL |
| pip usage | PASS/FAIL |
| Domain Tests | PASS/FAIL (X passed, Y failed) |

### Phase 2: Domain Verification
| # | Category | Result | Evidence |
|---|----------|--------|----------|
| 1 | Risk Gate Integrity | PASS/FAIL | <file:line references> |
| 2 | Order Safety | PASS/FAIL | <file:line references> |
| 3 | Position Lifecycle | PASS/FAIL | <file:line references> |
| 4 | Concurrency & Races | PASS/FAIL/KNOWN_GAP | <evidence + gaps acknowledged> |
| 5 | Data Quality & Staleness | PASS/FAIL/KNOWN_GAP | <evidence + gaps acknowledged> |
| 6 | Conviction & Scoring | PASS/FAIL/KNOWN_GAP | <evidence + gaps acknowledged> |
| 7 | Crypto-Specific | PASS/FAIL/KNOWN_GAP | <evidence + gaps acknowledged> |
| 8 | Data Pipeline Integrity | PASS/FAIL/KNOWN_GAP | <evidence + gaps acknowledged> |

### Known Gaps Acknowledged
- [ ] Asymmetric neutral fallback: `sent_low=True` has no 5.0 fallback
- [ ] No execution-time distributed lock
- [ ] `check_data_freshness` not called in production
- [ ] yfinance no 429 retry
- [ ] Twelve Data no HTTP 429 retry
- [ ] Crypto exposure defaults to 0

### Overall: PASS / FAIL
<If FAIL: list blocking issues that must be fixed>
```

---

## Rules

1. **No shortcuts.** Run every scan. Read every file. Cite every line.
2. **Auto-FAIL is final.** If an automated scan fails, the category fails. Period.
3. **Known gaps must be acknowledged**, not hidden. Mark them as `KNOWN_GAP`.
4. **New gaps discovered** during review must be added to the report.
5. If overall is FAIL, list exactly what must be fixed before the code can be committed.
