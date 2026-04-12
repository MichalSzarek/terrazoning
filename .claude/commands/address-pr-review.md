# /address-pr-review — Address PR Review Comments

You are addressing inline code review comments on the current PR. You must **critically evaluate** each comment — never blindly accept suggestions. Use the `superpowers:receiving-code-review` skill philosophy: verify technical accuracy, check against CLAUDE.md rules, and decline with reasons when appropriate.

**Arguments:** $ARGUMENTS (optional: PR number, e.g. `/address-pr-review 31`)

---

## Phase 1: Discovery

### Step 1a: Detect the PR

If `$ARGUMENTS` contains a PR number, use that. Otherwise auto-detect from current branch:

```bash
gh pr view --json number,url,headRefName
```

If no PR found, stop and tell the user: "No PR found for the current branch. Push your branch and create a PR first, or pass a PR number: `/address-pr-review <number>`"

### Step 1b: Get repo owner/name

```bash
gh repo view --json owner,name --jq '"\(.owner.login)/\(.name)"'
```

### Step 1c: Fetch inline review comments

```bash
gh api repos/{owner}/{repo}/pulls/{number}/comments --paginate
```

### Step 1d: Filter comments

- **Keep** only top-level comments (`in_reply_to_id` is null/absent) — these are the review items
- **Skip** reply threads (they are responses to comments, not new review items)
- **Skip** comments from the PR author (self-comments)
- If zero actionable comments remain, report "No unaddressed review comments found" and stop

---

## Phase 2: Triage (per comment)

Invoke the `superpowers:receiving-code-review` skill mindset. For each comment:

### Step 2a: Read context

Read the file at `path`, lines around `line` (±20 lines) to understand the code in question.

### Step 2b: Parse suggestion blocks

GitHub/Copilot comments may contain suggestion blocks:
````
```suggestion
replacement code here
```
````
Extract these — they are proposed code replacements.

### Step 2c: Evaluate — ACCEPT or DECLINE

**Auto-ACCEPT criteria:**
- Genuine bug fix (null check, off-by-one, logic error)
- Type safety improvement (missing type hint, wrong type)
- Naming inconsistency fix
- Missing error handling at system boundaries
- Aligns with CLAUDE.md code style rules (Section 6)
- Fixes a real security issue

**Auto-DECLINE criteria:**
- Adds `print()` instead of structlog (CLAUDE.md Section 6, 13)
- Removes type hints (CLAUDE.md Section 6)
- Suggests `pip` instead of `uv` (CLAUDE.md Section 2, 13)
- Pure style preference with no functional benefit
- Factually wrong about what the code does
- Breaks domain invariants (risk gates, order safety)
- Over-engineering: adds unnecessary abstractions, feature flags, or configurability (CLAUDE.md "avoid over-engineering")
- Suggests bare `except:` (CLAUDE.md Section 6)

**Manual evaluation (read more context):**
- Refactoring suggestions — check if the change is genuinely better
- Performance suggestions — verify the claim
- Architecture suggestions — check against CLAUDE.md Section 7
- Suggestions that touch trading-critical code — extra scrutiny

### Step 2d: Record triage decision

For each comment, record:
- `id`: GitHub comment ID
- `path`: file path
- `line`: line number
- `body`: comment text (truncated)
- `action`: ACCEPT or DECLINE
- `reason`: specific technical reason (not "looks good" or "disagree")
- `suggestion`: extracted suggestion code (if any)

---

## Phase 3: Summary & Confirmation

Present the triage table to the user:

```
## PR Review Triage — PR #{number}

| # | File | Line | Action | Reason |
|---|------|------|--------|--------|
| 1 | src/foo.py | 42 | ACCEPT | Genuine bug: missing null check before .get() |
| 2 | src/bar.py | 17 | DECLINE | Style-only: variable naming preference, current name follows project convention |
| 3 | ... | ... | ... | ... |

**Summary:** {X} ACCEPT, {Y} DECLINE out of {total} comments.

Shall I proceed with implementing the accepted fixes?
```

**Wait for user confirmation before proceeding.** Use AskUserQuestion if needed.

---

## Phase 4: Implement Fixes

For each ACCEPTED comment (in file order to minimize conflicts):

### Step 4a: Apply the fix

- If a `suggestion` block exists and was accepted, apply it directly via Edit
- Otherwise, implement the fix based on the comment's intent
- Keep changes minimal — fix exactly what was requested, nothing more

### Step 4b: Lint touched files

```bash
uv run ruff check --fix {path} && uv run ruff format {path}
```

### Step 4c: Run relevant tests

```bash
uv run pytest tests/test_{module}.py -v
```

Determine `{module}` from the file path (e.g., `src/agents/debater.py` → `tests/test_4way_debater.py`).

### Step 4d: Trading review check

If ANY accepted fix touches trading-critical files (`src/agents/`, `src/risk/`, `src/execution/`, `src/services/trading_service*`, `src/services/position_monitor*`, `src/data/`), you MUST run `/trading-review` and it must PASS before proceeding.

---

## Phase 5: Commit & Push

### Step 5a: Stage and commit

Stage only the files that were modified by accepted fixes:

```bash
git add {file1} {file2} ...
git commit -m "address PR review: {X} accepted, {Y} declined

Accepted:
- {path}: {reason}
...

Declined:
- {path}: {reason}
...

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

### Step 5b: Push

```bash
git push
```

Record the commit SHA from the push output.

---

## Phase 6: Reply on GitHub

For each comment, post a reply on the PR:

### ACCEPT replies

```bash
gh api repos/{owner}/{repo}/pulls/comments/{id}/replies -X POST -f body="Fixed in {sha}.

{brief description of what was changed}"
```

### DECLINE replies

```bash
gh api repos/{owner}/{repo}/pulls/comments/{id}/replies -X POST -f body="Declining this suggestion.

**Reason:** {specific technical reason}

{CLAUDE.md reference if applicable, e.g. 'Per project rules (CLAUDE.md §6): structlog is required, not print().'}"
```

---

## Rules

1. **Never blindly accept.** Every suggestion must be evaluated on technical merit. Copilot and reviewers can be wrong.
2. **Decline with reasons.** A declined suggestion must have a specific, technical justification — not just "I disagree."
3. **CLAUDE.md is authoritative.** If a suggestion contradicts CLAUDE.md rules, decline it and cite the section.
4. **Single commit.** All accepted fixes go in one commit, not per-comment.
5. **Tests must pass.** If tests fail after applying fixes, fix the tests or reconsider the acceptance.
6. **No scope creep.** Only address what the review comments ask for. Don't refactor surrounding code.
7. **Trading-critical = extra scrutiny.** Any fix touching trading code requires `/trading-review` to pass.
