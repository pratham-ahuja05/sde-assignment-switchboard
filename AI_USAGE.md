# AI Usage Disclosure

> Draft written with the AI's help from what actually happened in the session. **Review it and
> rewrite it in your own words before submitting.** Fill in the "_fill in_" parts yourself.

## Tools used
- Claude Code (Anthropic, Claude Opus model), in VS Code.

## What I used them for
- Reading the assignment and the mock provider's source, and comparing assignment A with B.
- Proposing the architecture: a DB-backed state machine, derived per-attempt idempotency keys,
  write-ahead intents, reconciliation of unknown outcomes.
- Writing most of the first-draft code in `solution/switchboard/`, the tests in `solution/tests/`,
  and the first drafts of `SUBMISSION.md` and this file.
- Building the chaos harness, the crash points, the self-audit and the dashboard.
- Diagnosing the first run's violations (profiling the provider's per-webhook httpx client).

## What I wrote or decided myself
_Fill in honestly. For example: choosing B, which stretch goals to keep, decisions you pushed back
on, parts you rewrote, and things you changed after reading the code._

## Where the AI was wrong or unhelpful
- **It assumed localhost latency was negligible.** The first end-to-end run had 10
  `outside_calling_window` violations. The window check used a fixed 20 sim-second guard. Calls
  "lasting" 50 sim-minutes in the ledger (the mock caps them at 6.5) showed the provider itself
  was stalling. Profiling found ~450 ms per `httpx.AsyncClient()` construction on this machine.
  Fixes: a latency-aware window guard in the engine, and a 1-certificate `SSL_CERT_FILE` for the
  mock in the chaos harness on Windows.
- **Releasing an unplaced intent pushed the lead to the back of the queue.** The first version
  set `eligible_at = now` even with zero backoff. A lead that lost its intent to a crash was
  overtaken by fresh leads. Caught by `test_crash_between_intent_and_post_never_double_dials`;
  fixed so only a real backoff (503) reorders.
- Small things: the tests first used relative imports without a package `__init__`, and a
  draft exhaustion test was too tangled to trust, so it was rewritten on a single-lead fixture.

## How I verified AI-generated code
- `pytest`: 58 tests on the pure scheduling/policy code and on engine failure scenarios, using a
  fake clock and a scriptable fake provider.
- End-to-end `chaos` runs against the real mock: hard kills plus injected crash points, checked
  three ways (`/admin/report` violations, `final_state.csv` diffed against `/admin/leads`, and the
  independent `audit` of R1-R9).
- _Fill in: what you read line by line, what you changed by hand, what you tested live._
