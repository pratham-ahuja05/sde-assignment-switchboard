"""Static configuration: campaigns, tenants, windows and leads, loaded from `fixtures/`.

Everything here is immutable. Runtime state lives in the database (see store.py).
"""
from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass
from datetime import time

DAY_NAMES = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]


def _hm(s: str) -> time:
    h, m = s.split(":")
    return time(int(h), int(m))


@dataclass(frozen=True)
class Window:
    """A daily calling window `[start, end)` in IST on a set of weekdays (0 = Monday)."""
    start: time
    end: time
    days: frozenset[int]

    def intersect(self, other: "Window") -> "Window":
        return Window(max(self.start, other.start), min(self.end, other.end), self.days & other.days)


ALL_DAYS = frozenset(range(7))


@dataclass(frozen=True)
class Tenant:
    tenant_id: str
    max_concurrent: int


@dataclass(frozen=True)
class Campaign:
    campaign_id: str
    tenant_id: str
    window: Window          # the campaign's own window
    effective: Window       # campaign window ∩ regulatory window: the only time we START calls
    max_attempts: int


@dataclass(frozen=True)
class Lead:
    campaign_id: str
    lead_id: str
    phone: str
    seq: int                # position in its file; used for fair interleaving across campaigns


@dataclass(frozen=True)
class Config:
    tenants: dict[str, Tenant]
    campaigns: dict[str, Campaign]
    leads: list[Lead]
    retry_minutes: dict[str, int]   # call status -> minutes to wait (no_answer, busy, failed)
    webhook_secret: str
    regulatory: Window


def load(fixtures_dir: str) -> Config:
    with open(os.path.join(fixtures_dir, "campaigns.json"), encoding="utf-8") as f:
        raw = json.load(f)

    reg = Window(_hm(raw["regulatory_window"]["start"]), _hm(raw["regulatory_window"]["end"]), ALL_DAYS)
    tenants = {t["tenant_id"]: Tenant(t["tenant_id"], int(t["max_concurrent_calls"])) for t in raw["tenants"]}

    campaigns: dict[str, Campaign] = {}
    leads: list[Lead] = []
    for c in raw["campaigns"]:
        w = c["window"]
        win = Window(_hm(w["start"]), _hm(w["end"]), frozenset(DAY_NAMES.index(d.lower()) for d in w["days"]))
        campaigns[c["campaign_id"]] = Campaign(
            campaign_id=c["campaign_id"], tenant_id=c["tenant_id"], window=win,
            effective=win.intersect(reg), max_attempts=int(c["max_attempts"]),
        )
        # Data minimisation: we read lead_id and phone only. customer_name is PII we never need,
        # so it never enters memory beyond the csv reader or touches our database.
        with open(os.path.join(fixtures_dir, c["leads_file"]), encoding="utf-8", newline="") as f:
            for i, row in enumerate(csv.DictReader(f)):
                leads.append(Lead(c["campaign_id"], row["lead_id"].strip(), row["phone"].strip(), i))

    rp = raw["retry_policy"]
    retry = {"no_answer": int(rp["no_answer_minutes"]), "busy": int(rp["busy_minutes"]),
             "failed": int(rp["failed_minutes"])}
    return Config(tenants, campaigns, leads, retry, raw["webhook_secret"], reg)


def mask_phone(phone: str) -> str:
    """Phones never appear in logs or the dashboard in full."""
    return phone[:3] + "*****" + phone[-4:] if len(phone) > 7 else "***"
