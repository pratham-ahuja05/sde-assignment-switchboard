"""`audit`: prove R1-R9 from our own ledger, without asking the provider.

This is deliberately a *second* implementation of the rules. It doesn't import policy.py or
schedule.py and only reads the facts the provider reported (each call's started_at / ended_at /
status / disposition). If the dispatcher's logic had a bug, this would have to share the same bug
to miss it.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from .config import Config
from .store import Store, dt

IST = timezone(timedelta(hours=5, minutes=30))
TERMINAL = {"interested", "not_interested", "wrong_number", "dnd"}
RULES = {
    "R1": "calls start only inside campaign + regulatory window",
    "R2": "tenant concurrency never above the limit",
    "R3": "never two live calls to one phone",
    "R4": "at most max_attempts accepted calls per lead",
    "R5": "retry gaps respected (60 / 60 / 15 min)",
    "R6": "callbacks never early; lateness measured",
    "R7": "no call after a terminal disposition",
    "R8": "no call to a phone after it said dnd",
    "R9": "every lead final, with the right precedence",
}


@dataclass
class AuditResult:
    findings: dict[str, list[str]] = field(default_factory=lambda: {r: [] for r in RULES})
    stats: dict = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not any(self.findings.values())

    def render(self) -> str:
        lines = ["", "  Switchboard self-audit (independent re-check of our own ledger)", "  " + "-" * 66]
        for r, desc in RULES.items():
            f = self.findings[r]
            lines.append(f"  {'PASS' if not f else 'FAIL'}  {r}  {desc}" + (f"  ({len(f)} issues)" if f else ""))
            lines += [f"          - {x}" for x in f[:5]]
        lines.append("  " + "-" * 66)
        for k, v in self.stats.items():
            lines.append(f"  {k:<28} {v}")
        return "\n".join(lines)


def _in_window(t: datetime, start, end, days) -> bool:
    lt = t.astimezone(IST)
    hm = (lt.hour, lt.minute, lt.second + lt.microsecond / 1e6)
    return lt.weekday() in days and (start.hour, start.minute, 0) <= hm < (end.hour, end.minute, 0)


def _next_open(t: datetime, start, end, days) -> datetime:
    lt = t.astimezone(IST)
    for d in range(8):
        day = lt + timedelta(days=d)
        if day.weekday() not in days:
            continue
        s = day.replace(hour=start.hour, minute=start.minute, second=0, microsecond=0)
        e = day.replace(hour=end.hour, minute=end.minute, second=0, microsecond=0)
        if d == 0 and lt >= e:
            continue
        return max(s, lt) if d == 0 else s
    return lt


def _max_overlap(intervals: list[tuple[datetime, datetime]]) -> int:
    ev = sorted([(s, 1) for s, _ in intervals] + [(e, -1) for _, e in intervals], key=lambda x: (x[0], x[1]))
    cur = best = 0
    for _, d in ev:
        cur += d
        best = max(best, cur)
    return best


def audit(cfg: Config, store: Store) -> AuditResult:
    res = AuditResult()
    F = res.findings
    calls = [dict(r) for r in store.db.execute(
        "SELECT * FROM attempts WHERE state IN ('accepted','ended') ORDER BY campaign_id, lead_id, attempt_no")]
    ended = [c for c in calls if c["state"] == "ended"]
    for c in ended:
        c["start"], c["end"] = dt(c["started_at"]), dt(c["ended_at"])

    by_lead: dict[tuple, list[dict]] = defaultdict(list)
    for c in ended:
        by_lead[(c["campaign_id"], c["lead_id"])].append(c)

    # R1
    reg = cfg.regulatory
    for c in ended:
        camp = cfg.campaigns[c["campaign_id"]]
        w = camp.window
        if not (_in_window(c["start"], w.start, w.end, w.days) and _in_window(c["start"], reg.start, reg.end, reg.days)):
            F["R1"].append(f"{c['request_id']} started {c['start'].astimezone(IST):%a %H:%M:%S}")

    # R2 / R3
    by_tenant, by_phone = defaultdict(list), defaultdict(list)
    for c in ended:
        by_tenant[c["tenant_id"]].append((c["start"], c["end"]))
        by_phone[c["phone"]].append((c["start"], c["end"]))
    peak = {}
    for t, iv in by_tenant.items():
        peak[t] = _max_overlap(iv)
        if peak[t] > cfg.tenants[t].max_concurrent:
            F["R2"].append(f"{t} peaked at {peak[t]} > {cfg.tenants[t].max_concurrent}")
    for p, iv in by_phone.items():
        if _max_overlap(iv) > 1:
            F["R3"].append(f"phone ...{p[-4:]} had overlapping calls")

    # R4-R8, per lead in attempt order
    dnd_at = {}
    for c in ended:
        if c["disposition"] == "dnd":
            dnd_at[c["phone"]] = min(dnd_at.get(c["phone"], c["end"]), c["end"])
    late = []
    for key, cs in by_lead.items():
        camp = cfg.campaigns[key[0]]
        n_all = sum(1 for c in calls if (c["campaign_id"], c["lead_id"]) == key)
        if n_all > camp.max_attempts:
            F["R4"].append(f"{key[0]}/{key[1]} has {n_all} accepted calls")
        cs.sort(key=lambda c: c["start"])
        for prev, nxt in zip(cs, cs[1:]):
            tag = f"{key[0]}/{key[1]} a{nxt['attempt_no']}"
            if prev["disposition"] in TERMINAL:
                F["R7"].append(f"{tag} dialled after {prev['disposition']}")
            if prev["status"] in cfg.retry_minutes:
                gap = prev["end"] + timedelta(minutes=cfg.retry_minutes[prev["status"]])
                if nxt["start"] < gap:
                    F["R5"].append(f"{tag} {(gap - nxt['start']).total_seconds():.0f}s early after {prev['status']}")
            if prev["disposition"] == "callback_requested" and prev["callback_at"]:
                cb = dt(prev["callback_at"])
                if nxt["start"] < cb:
                    F["R6"].append(f"{tag} {(cb - nxt['start']).total_seconds():.0f}s before callback_at")
                due = _next_open(cb, camp.window.start, camp.window.end, camp.window.days)
                late.append(max(0.0, (nxt["start"] - due).total_seconds() / 60))
        for c in cs:
            if c["phone"] in dnd_at and c["start"] > dnd_at[c["phone"]]:
                F["R8"].append(f"{key[0]}/{key[1]} a{c['attempt_no']} called after the phone said dnd")

    # R9: recompute every lead's final state from the facts and compare with what we recorded
    final_counts = defaultdict(int)
    for lead in store.all_leads():
        key = (lead["campaign_id"], lead["lead_id"])
        cs = by_lead.get(key, [])
        n = sum(1 for c in calls if (c["campaign_id"], c["lead_id"]) == key)
        live = any(c["state"] == "accepted" for c in calls if (c["campaign_id"], c["lead_id"]) == key)
        if any(c["disposition"] in TERMINAL for c in cs):
            truth = "completed"
        elif n >= cfg.campaigns[key[0]].max_attempts and not live:
            truth = "exhausted"
        elif lead["phone"] in dnd_at and not live:
            truth = "dnd_blocked"
        else:
            truth = "incomplete"
        final_counts[lead["state"]] += 1
        if truth != lead["state"]:
            F["R9"].append(f"{key[0]}/{key[1]} recorded {lead['state']}, facts say {truth}")

    late.sort()
    j = store.journal_counts()
    res.stats = {
        "calls accepted": len(calls),
        "peak concurrency": dict(sorted(peak.items())),
        "final states": dict(sorted(final_counts.items())),
        "callbacks honoured": len(late),
        "callback lateness p50/max": (f"{late[len(late) // 2]:.1f} / {late[-1]:.1f} min" if late else "n/a"),
        "process boots": j.get("boot", 0),
        "ambiguous 504s reconciled": j.get("ambiguous", 0),
        "503s retried": j.get("unavailable_503", 0),
        "duplicate results ignored": j.get("duplicate_result", 0),
        "results recovered by poll": j.get("recovered_by_poll", 0),
        "late placements detected": j.get("late_placement", 0),
        "provider rejections": j.get("provider_rejected", 0),
    }
    return res
