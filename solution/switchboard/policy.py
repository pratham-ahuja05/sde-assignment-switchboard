"""Pure business decisions. No I/O. Everything the rules R1-R9 decide lives here, so it can be
tested exhaustively with a fake clock.

Two functions matter:
  * `after_result`  - a call ended: what happens to the lead next?            (R4-R9)
  * `plan_dispatch` - it is time `now`: which leads do we dial right now?    (R1-R3, R6)
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Iterable, Optional

from .config import Campaign, Tenant
from .schedule import is_open, next_open

TERMINAL = frozenset({"interested", "not_interested", "wrong_number", "dnd"})

# Lead priority classes (R6). Lower dials first.
CALLBACK, RETRY, FRESH = 0, 1, 2
PRIORITY_NAME = {CALLBACK: "callback", RETRY: "retry", FRESH: "fresh"}

# Lead states.
READY, DIALING, ACTIVE = "ready", "dialing", "active"
COMPLETED, EXHAUSTED, DND_BLOCKED = "completed", "exhausted", "dnd_blocked"
FINAL = frozenset({COMPLETED, EXHAUSTED, DND_BLOCKED})


@dataclass(frozen=True)
class Decision:
    state: str
    priority: Optional[int] = None
    eligible_at: Optional[datetime] = None   # earliest instant the next attempt may START
    not_before: Optional[datetime] = None    # the raw rule time (callback_at / ended_at + backoff)


def after_result(*, attempts: int, campaign: Campaign, status: str, disposition: Optional[str],
                 callback_at: Optional[datetime], ended_at: datetime, phone_is_dnd: bool,
                 retry_minutes: dict[str, int], end_guard_s: float = 0.0) -> Decision:
    """Decide a lead's next state after one of its calls ended.

    `attempts` already includes the call that just ended.
    Final-state precedence is R9's: completed > exhausted > dnd_blocked.
    """
    if status == "completed" and disposition in TERMINAL:
        return Decision(COMPLETED)                                   # R7
    if attempts >= campaign.max_attempts:
        return Decision(EXHAUSTED)                                   # R4
    if phone_is_dnd:
        return Decision(DND_BLOCKED)                                 # R8

    if status == "completed" and disposition == "callback_requested" and callback_at is not None:
        not_before, prio = callback_at, CALLBACK                     # R6
    else:
        # no_answer / busy / failed per R5. Anything unexpected (a completed call with no or an
        # unknown disposition) is treated like the most conservative retry rule, never dropped.
        minutes = retry_minutes.get(status, max(retry_minutes.values()))
        not_before, prio = ended_at + timedelta(minutes=minutes), RETRY
    return Decision(READY, prio, next_open(not_before, campaign.effective, end_guard_s), not_before)


@dataclass(frozen=True)
class Candidate:
    campaign_id: str
    lead_id: str
    tenant_id: str
    phone: str
    priority: int
    eligible_at: datetime
    seq: int

    def sort_key(self):
        # R6 ordering first, then whoever has waited longest, then file order (which interleaves
        # campaigns fairly), then ids for total determinism.
        return (self.priority, self.eligible_at, self.seq, self.campaign_id, self.lead_id)


@dataclass
class Plan:
    picks: list[Candidate] = field(default_factory=list)
    held_for_callbacks: Counter = field(default_factory=Counter)  # tenant -> slots held back


def plan_dispatch(now: datetime, ready: Iterable[Candidate], *, campaigns: dict[str, Campaign],
                  tenants: dict[str, Tenant], active_by_tenant: dict[str, int], busy_phones: set[str],
                  dnd_phones: set[str], end_guard_s: float = 0.0,
                  callback_lookahead: timedelta = timedelta(0)) -> Plan:
    """Choose which ready leads to dial at `now`.

    `active_by_tenant` / `busy_phones` must count every call that MIGHT be live at the provider:
    accepted calls whose result we have not seen, and intents whose outcome we don't know yet.
    Over-counting only costs throughput; under-counting would be a violation.

    Callback slot holding: if a callback for tenant T becomes due within `callback_lookahead`,
    we stop starting retries and fresh calls that would use up T's last free slots. A call lasts
    a few simulated minutes, so that is exactly how far ahead a slot has to be kept free for the
    callback to start on time (R6: "as soon as possible").
    """
    ready = list(ready)
    used = Counter(active_by_tenant)
    phones = set(busy_phones)
    horizon = now + callback_lookahead

    upcoming = Counter()
    for c in ready:
        if c.priority == CALLBACK and now < c.eligible_at <= horizon and c.phone not in dnd_phones:
            upcoming[c.tenant_id] += 1

    plan = Plan()
    due = sorted((c for c in ready if c.eligible_at <= now), key=Candidate.sort_key)
    for c in due:
        camp = campaigns[c.campaign_id]
        if not is_open(now, camp.effective, end_guard_s):            # R1
            continue
        if c.phone in dnd_phones or c.phone in phones:               # R8, R3
            continue
        cap = tenants[c.tenant_id].max_concurrent
        hold = 0 if c.priority == CALLBACK else min(upcoming[c.tenant_id], cap)
        if used[c.tenant_id] + hold >= cap:                          # R2 (+ callback holding)
            if hold and used[c.tenant_id] < cap:
                plan.held_for_callbacks[c.tenant_id] = hold
            continue
        plan.picks.append(c)
        used[c.tenant_id] += 1
        phones.add(c.phone)
    return plan
