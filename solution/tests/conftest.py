"""Shared fixtures. Nothing here talks to the mock provider: time is a FakeClock and the provider
is an in-memory fake with scriptable failures."""
from __future__ import annotations

import hashlib
import hmac
import itertools
import json
import os
from collections import Counter, defaultdict, deque
from datetime import datetime, time, timedelta

import pytest

from solution.switchboard.clock import FakeClock
from solution.switchboard.config import ALL_DAYS, Campaign, Config, Lead, Tenant, Window, load
from solution.switchboard.engine import Engine
from solution.switchboard.provider import ACCEPTED, UNAVAILABLE, UNKNOWN, PlaceOutcome
from solution.switchboard.schedule import IST
from solution.switchboard.store import Store

FIXTURES = os.path.join(os.path.dirname(__file__), "..", "..", "assignment-b-workflow", "fixtures")
REG = Window(time(9), time(21), ALL_DAYS)
WEEKDAYS = frozenset(range(5))


def ist(day: int, hh: int, mm: int = 0, ss: int = 0) -> datetime:
    """`day` is a November 2026 date. 12 Nov 2026 is a Thursday."""
    return datetime(2026, 11, day, hh, mm, ss, tzinfo=IST)


def campaign(cid: str, tenant: str, start=time(10), end=time(19), days=frozenset(range(6)), max_attempts=3):
    w = Window(start, end, days)
    return Campaign(cid, tenant, w, w.intersect(REG), max_attempts)


def make_config(leads: list[tuple[str, str, str]], cap: int = 2, campaigns: list[Campaign] | None = None) -> Config:
    campaigns = campaigns or [campaign("c_a", "t1"), campaign("c_b", "t1", time(11), time(20, 30), ALL_DAYS)]
    tenants = {c.tenant_id: Tenant(c.tenant_id, cap) for c in campaigns}
    return Config(tenants, {c.campaign_id: c for c in campaigns},
                  [Lead(c, l, p, i) for i, (c, l, p) in enumerate(leads)],
                  {"no_answer": 60, "busy": 60, "failed": 15}, "whsec_test", REG)


@pytest.fixture
def real_config():
    return load(FIXTURES)


class FakeCall:
    def __init__(self, sid, payload, created):
        self.sid, self.p, self.created, self.result = sid, payload, created, None

    def public(self):
        d = {"call_sid": self.sid, "request_id": self.p["request_id"], "campaign_id": self.p["campaign_id"],
             "lead_id": self.p["lead_id"], "state": "ended" if self.result else "active"}
        if self.result:
            d["result"] = self.result
        return d


class FakeProvider:
    """In-memory provider. `script[suffix]` queues behaviours for the request_id ending in `suffix`
    (e.g. "L1-a1"): "503", "504_unplaced", "504_placed", "conn_refused". Default: accept."""

    def __init__(self, clock: FakeClock):
        self.clock = clock
        self.by_rid: dict[str, FakeCall] = {}
        self.posts = Counter()
        self.script = defaultdict(deque)
        self.ids = itertools.count(1)
        self.live_phones: Counter = Counter()

    def _behaviour(self, rid):
        for suffix, q in self.script.items():
            if rid.endswith(suffix) and q:
                return q.popleft()
        return "ok"

    def placed_for(self, lead_id: str) -> int:
        return sum(1 for c in self.by_rid.values() if c.p["lead_id"] == lead_id)

    async def place_call(self, payload):
        rid = payload["request_id"]
        self.posts[rid] += 1
        if rid in self.by_rid:
            return PlaceOutcome(ACCEPTED, 200, self.by_rid[rid].public())
        b = self._behaviour(rid)
        if b == "503":
            return PlaceOutcome(UNAVAILABLE, 503)
        if b == "conn_refused":
            return PlaceOutcome(UNAVAILABLE, error="ConnectError")
        if b == "504_unplaced":
            return PlaceOutcome(UNKNOWN, 504)
        call = self.create(payload)
        if b == "504_placed":
            return PlaceOutcome(UNKNOWN, 504)
        return PlaceOutcome(ACCEPTED, 201, call.public())

    def create(self, payload) -> FakeCall:
        """The provider placing a call (also used by tests to simulate a POST that landed
        while our process was dying)."""
        assert self.live_phones[payload["phone"]] == 0, "R3 violated: phone already live"
        call = FakeCall(f"CA{next(self.ids):06d}", payload, self.clock.now())
        self.by_rid[payload["request_id"]] = call
        self.live_phones[payload["phone"]] += 1
        return call

    def end(self, rid: str, status: str, disposition=None, callback_at=None, after=timedelta(minutes=3)) -> dict:
        call = self.by_rid[rid]
        ended = call.created + after
        call.result = {"event": "call.completed", "call_sid": call.sid, "request_id": rid,
                       "campaign_id": call.p["campaign_id"], "lead_id": call.p["lead_id"],
                       "phone": call.p["phone"], "status": status, "disposition": disposition,
                       "callback_at": callback_at.isoformat() if callback_at else None,
                       "started_at": call.created.isoformat(), "ended_at": ended.isoformat(), "duration_sec": 60}
        self.live_phones[call.p["phone"]] -= 1
        return call.result

    async def find_by_request(self, rid):
        c = self.by_rid.get(rid)
        return c.public() if c else None

    async def get_call(self, sid):
        for c in self.by_rid.values():
            if c.sid == sid:
                return c.public()
        return None


def sign(secret: str, body: bytes) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


class World:
    """An engine wired to a fake clock, a fake provider and a real SQLite file."""

    def __init__(self, tmp_path, cfg: Config, start: datetime):
        self.cfg, self.db_path = cfg, str(tmp_path / "sb.db")
        self.clock = FakeClock(start)
        self.provider = FakeProvider(self.clock)
        self.engine = self.boot()

    def boot(self) -> Engine:
        """Start a (new) process: fresh Engine + Store objects on the same database file.
        Calling it again simulates kill -9 + restart: nothing in memory survives."""
        self.store = Store(self.db_path)
        eng = Engine(self.cfg, self.store, self.provider, self.clock, callback_url="http://test/wh")
        eng.bootstrap("test")
        self.engine = eng
        return eng

    async def tick_and_place(self) -> list[str]:
        rids = self.engine.tick()
        for r in rids:
            await self.engine.place(r)
        return rids

    def lead(self, cid, lid):
        return self.store.lead(cid, lid)

    def body(self, result: dict) -> bytes:
        return json.dumps(result).encode()


@pytest.fixture
def world_factory(tmp_path):
    return lambda cfg, start: World(tmp_path, cfg, start)
