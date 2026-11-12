"""The dispatcher engine: the only code that changes lead state.

How it stays safe when the process dies at any point, in three ideas:

1. **One idempotency key per attempt, derived and not random.**
   request_id = sb-<run>-<campaign>-<lead>-a<attempt_no>, where attempt_no = accepted calls + 1.
   The key is a pure function of durable state, so every process that ever decides to make
   attempt #2 for a lead produces the same key. The provider places at most one call per key,
   so at most one call can exist for each attempt, however many times we crash and retry.

2. **Write-ahead intent.** Before any network I/O the key is committed as an `intent`, together
   with the tenant slot and the phone it reserves, in one BEGIN IMMEDIATE transaction. An intent
   counts as a live call for concurrency (R2) and same-phone (R3) checks until we learn otherwise.
   Over-reserving costs throughput; under-reserving would be a violation, so we over-reserve.

3. **Unknown is a state, not a guess.** After a 504, a timeout or a crash, the intent stays
   `intent` until the provider tells us what happened (GET /v1/calls?request_id=...). Only a 404
   from the provider, at least `reconcile_after` after the intent was written (so no request can
   still be in flight), releases it. A result webhook for an attempt we thought unplaced is still
   accepted: evidence beats assumptions.

Results (webhook or poll) are applied idempotently: an attempt moves to `ended` exactly once and
later copies are journaled as duplicates and ignored.
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from collections import deque
from datetime import datetime, timedelta
from typing import Optional

import httpx

from . import policy
from .clock import SimClock, FakeClock
from .config import Config, mask_phone
from .crash import NO_CRASH, Crasher
from .policy import Candidate, after_result, plan_dispatch
from .provider import ACCEPTED, REJECTED, UNAVAILABLE, UNKNOWN, HttpProvider
from .schedule import is_open, next_open, window_close
from .store import Store, dt, ts


def request_id(run_tag: str, campaign_id: str, lead_id: str, attempt_no: int) -> str:
    return f"sb-{run_tag}-{campaign_id}-{lead_id}-a{attempt_no}"


def _parse(s: Optional[str]) -> Optional[datetime]:
    return datetime.fromisoformat(s) if s else None


class Engine:
    def __init__(self, cfg: Config, store: Store, provider: HttpProvider, clock: SimClock | FakeClock,
                 *, callback_url: str, end_guard_s: float = 20.0,
                 callback_lookahead: timedelta = timedelta(minutes=7),
                 reconcile_after_real_s: float = 1.5, probe_after: timedelta = timedelta(minutes=1),
                 probe_every_real_s: float = 1.0, crash: Crasher = NO_CRASH):
        self.cfg, self.store, self.provider, self.clock = cfg, store, provider, clock
        self.callback_url = callback_url
        self.base_end_guard_s = end_guard_s
        self.latency_sim: deque[float] = deque(maxlen=64)   # recent POST round trips, in sim seconds
        self.callback_lookahead = callback_lookahead
        self.reconcile_after = clock.sim(reconcile_after_real_s)
        self.probe_after = probe_after
        self.probe_every = clock.sim(probe_every_real_s)
        self.crash = crash
        self.inflight: set[str] = set()     # request_ids this process is POSTing right now
        self.wake = asyncio.Event()         # set whenever something may have freed capacity
        self.last_plan = policy.Plan()

    # ================================================================== lifecycle
    @property
    def end_guard_s(self) -> float:
        """Latency-aware window guard (R1).

        The provider judges the window at the moment our POST *arrives*, not when we decided to
        send it. At 600x, 100 ms of real latency is 60 simulated seconds; a provider event-loop
        stall of 400 ms is 4 simulated minutes. So we never start a call closer to window close
        than twice the slowest recent round trip (capped at 15 min, never below the base guard).
        Costs a few minutes of dialling at close; buys R1 even on a slow or stalled provider.
        """
        worst = max(self.latency_sim, default=0.0)
        return max(self.base_end_guard_s, min(900.0, 2.0 * worst))

    def observe_latency(self, real_seconds: float):
        self.latency_sim.append(real_seconds * self.clock.speed)

    @property
    def run_tag(self) -> str:
        return self.store.meta("run_tag")

    def bootstrap(self, run_tag: str):
        """Load leads on first start. On a restart this only records the boot."""
        now = self.clock.now()
        with self.store.tx() as db:
            if self.store.meta("run_tag") is None:
                self.store.set_meta("run_tag", run_tag)
                self.store.set_meta("provider_epoch", self.clock.epoch_key)
                for ld in self.cfg.leads:
                    camp = self.cfg.campaigns[ld.campaign_id]
                    db.execute(
                        "INSERT OR IGNORE INTO leads(campaign_id,lead_id,tenant_id,phone,seq,state,priority,"
                        "eligible_at) VALUES(?,?,?,?,?,'ready',?,?)",
                        (ld.campaign_id, ld.lead_id, camp.tenant_id, ld.phone, ld.seq, policy.FRESH,
                         ts(next_open(now, camp.effective, self.end_guard_s))))
                self.store.journal("run_started", now, leads=len(self.cfg.leads))
            live = len(self.store.attempts_in("intent", "accepted"))
            self.store.journal("boot", now, unresolved_attempts=live)

    def done(self) -> bool:
        return self.store.unfinished() == 0

    # ================================================================== dispatch
    def tick(self) -> list[str]:
        """Decide who to dial now and durably reserve their slots. Returns request_ids to POST."""
        now = self.clock.now()
        rids: list[str] = []
        with self.store.tx() as db:
            rows = {(r["campaign_id"], r["lead_id"]): r for r in self.store.ready_leads()}
            if not rows:
                return rids
            cands = [Candidate(r["campaign_id"], r["lead_id"], r["tenant_id"], r["phone"], r["priority"],
                               dt(r["eligible_at"]), r["seq"]) for r in rows.values()]
            plan = plan_dispatch(now, cands, campaigns=self.cfg.campaigns, tenants=self.cfg.tenants,
                                 active_by_tenant=self.store.live_by_tenant(),
                                 busy_phones=self.store.busy_phones(), dnd_phones=self.store.dnd_phones(),
                                 end_guard_s=self.end_guard_s, callback_lookahead=self.callback_lookahead)
            self.last_plan = plan
            for c in plan.picks:
                lead = rows[(c.campaign_id, c.lead_id)]
                n = lead["attempts"] + 1
                rid = request_id(self.run_tag, c.campaign_id, c.lead_id, n)
                cur = db.execute(
                    "INSERT INTO attempts(request_id,campaign_id,lead_id,attempt_no,tenant_id,phone,priority,"
                    "state,intent_at,owner_pid) VALUES(?,?,?,?,?,?,?,'intent',?,?) "
                    "ON CONFLICT(request_id) DO UPDATE SET state='intent', intent_at=excluded.intent_at, "
                    "priority=excluded.priority, owner_pid=excluded.owner_pid "
                    "WHERE attempts.state IN ('not_placed','rejected')",
                    (rid, c.campaign_id, c.lead_id, n, c.tenant_id, c.phone, c.priority, ts(now), os.getpid()))
                if cur.rowcount != 1:   # a live/ended attempt already owns this key: never re-dial it
                    self.store.journal("anomaly", now, c.campaign_id, c.lead_id, rid,
                                       what="ready lead whose next key is already live; skipped")
                    continue
                db.execute("UPDATE leads SET state='dialing', current_rid=? WHERE campaign_id=? AND lead_id=?",
                           (rid, c.campaign_id, c.lead_id))
                detail = {"attempt": n, "priority": policy.PRIORITY_NAME[c.priority]}
                if c.priority == policy.CALLBACK:
                    due = next_open(dt(lead["not_before"]), self.cfg.campaigns[c.campaign_id].window)
                    detail["late_min"] = round(max(0.0, (now - due).total_seconds() / 60), 2)
                self.store.journal("intent", now, c.campaign_id, c.lead_id, rid, **detail)
                rids.append(rid)
        if rids:
            self.crash("after_intent_commit")
        return rids

    async def place(self, rid: str):
        """POST one intent. Safe to call any number of times for the same rid."""
        att = self.store.attempt(rid)
        if att is None or att["state"] != "intent" or rid in self.inflight:
            return
        self.inflight.add(rid)
        try:
            with self.store.tx() as db:
                db.execute("UPDATE attempts SET posts=posts+1 WHERE request_id=?", (rid,))
            t0 = time.perf_counter()
            out = await self.provider.place_call({
                "request_id": rid, "campaign_id": att["campaign_id"], "lead_id": att["lead_id"],
                "phone": att["phone"], "callback_url": self.callback_url})
        finally:
            self.inflight.discard(rid)
        self.crash("after_provider_reply")
        if out.kind != UNKNOWN:      # the provider's 504 path sleeps on purpose; that isn't latency
            self.observe_latency(time.perf_counter() - t0)

        if out.kind == ACCEPTED:
            self._on_found(rid, out.body or {}, via=f"post_{out.status_code}")
        elif out.kind == UNAVAILABLE:
            self._release(rid, "not_placed", f"unavailable_{out.status_code or out.error}", self.clock.sim(0.2))
        elif out.kind == REJECTED:
            # A rule violation. Should never happen; if it does, back off and make it loud.
            self._release(rid, "rejected", f"rejected_{out.status_code}", self.clock.sim(1.0),
                          body=out.body)
        else:
            with self.store.tx():
                self.store.journal("ambiguous", self.clock.now(), att["campaign_id"], att["lead_id"], rid,
                                   status=out.status_code, error=out.error)
            await self.reconcile_intent(rid, force=True)

    async def reconcile_intent(self, rid: str, force: bool = False):
        """Ask the provider whether an intent became a call. `force` skips the in-flight age guard
        and is only used right after our own POST returned, when nothing of ours is in flight."""
        att = self.store.attempt(rid)
        if att is None or att["state"] != "intent" or rid in self.inflight:
            return
        if not force and self.clock.now() - dt(att["intent_at"]) < self.reconcile_after:
            return
        try:
            found = await self.provider.find_by_request(rid)
        except httpx.HTTPError:
            return                                   # couldn't ask; the watchdog asks again later
        self.crash("mid_reconcile")
        if found is not None:
            self._on_found(rid, found, via="reconcile")
        else:
            self._release(rid, "not_placed", "reconciled_not_placed", timedelta(0))

    # ================================================================== state transitions
    def _on_found(self, rid: str, call: dict, via: str):
        now = self.clock.now()
        with self.store.tx():
            self._accept(rid, call.get("call_sid"), now, via)
            if call.get("state") == "ended" and call.get("result"):
                self.handle_result(call["result"], source="poll")

    def _accept(self, rid: str, sid: Optional[str], now: datetime, via: str) -> bool:
        db = self.store.db
        before = self.store.attempt(rid)
        cur = db.execute("UPDATE attempts SET state='accepted', call_sid=COALESCE(?,call_sid), accepted_at=? "
                         "WHERE request_id=? AND state IN ('intent','not_placed','rejected')", (sid, ts(now), rid))
        if cur.rowcount == 0:
            return False
        db.execute("UPDATE leads SET attempts=attempts+1, current_rid=?, "
                   "state=CASE WHEN state IN ('ready','dialing') THEN 'active' ELSE state END "
                   "WHERE campaign_id=? AND lead_id=?", (rid, before["campaign_id"], before["lead_id"]))
        self.store.journal("accepted", now, before["campaign_id"], before["lead_id"], rid, via=via, sid=sid,
                           attempt=before["attempt_no"])
        if before["state"] != "intent":
            self.store.journal("late_placement", now, before["campaign_id"], before["lead_id"], rid,
                               was=before["state"])
        return True

    def _release(self, rid: str, new_state: str, reason: str, backoff: timedelta, **detail):
        now = self.clock.now()
        with self.store.tx() as db:
            att = self.store.attempt(rid)
            cur = db.execute("UPDATE attempts SET state=? WHERE request_id=? AND state='intent'", (new_state, rid))
            if cur.rowcount == 0:
                return
            # A zero-backoff release (the provider confirmed "never placed") keeps the lead's place
            # in the queue; a backoff (503) deliberately moves it behind leads that are due now.
            db.execute("UPDATE leads SET state='ready', current_rid=NULL, "
                       "eligible_at=CASE WHEN ? > 0 THEN MAX(eligible_at, ?) ELSE eligible_at END "
                       "WHERE campaign_id=? AND lead_id=? AND state='dialing'",
                       (backoff.total_seconds(), ts(now + backoff), att["campaign_id"], att["lead_id"]))
            self.store.journal(reason if new_state != "rejected" else "provider_rejected", now,
                               att["campaign_id"], att["lead_id"], rid, **detail)
        self.wake.set()

    def handle_result(self, result: dict, source: str) -> str:
        """Apply a call result (from a webhook or a poll). Idempotent. Returns applied|duplicate|unknown."""
        now = self.clock.now()
        with self.store.tx() as db:
            rid = result.get("request_id")
            att = self.store.attempt(rid) if rid else None
            if att is None and result.get("call_sid"):
                att = self.store.attempt_by_sid(result["call_sid"])
            if att is None:
                self.store.journal("unknown_result", now, result.get("campaign_id"), result.get("lead_id"), rid,
                                   source=source)
                return "unknown"
            rid = att["request_id"]
            key = (att["campaign_id"], att["lead_id"])
            if att["state"] == "ended":
                self.store.journal("duplicate_result", now, *key, rid, source=source)
                return "duplicate"
            if att["state"] != "accepted":            # result before our 201 / for an "unplaced" intent
                self._accept(rid, result.get("call_sid"), now, via=f"result_{source}")

            status, disp = result.get("status"), result.get("disposition")
            ended = _parse(result.get("ended_at")) or now
            cb = _parse(result.get("callback_at"))
            db.execute("UPDATE attempts SET state='ended', started_at=?, ended_at=?, status=?, disposition=?, "
                       "callback_at=?, result_source=? WHERE request_id=?",
                       (ts(_parse(result.get("started_at"))), ts(ended), status, disp, ts(cb), source, rid))
            if disp == "dnd":
                db.execute("INSERT OR IGNORE INTO dnd_phones(phone,at,request_id) VALUES(?,?,?)",
                           (att["phone"], ts(ended), rid))
            lead = self.store.lead(*key)
            camp = self.cfg.campaigns[att["campaign_id"]]
            d = after_result(attempts=lead["attempts"], campaign=camp, status=status, disposition=disp,
                             callback_at=cb, ended_at=ended, phone_is_dnd=att["phone"] in self.store.dnd_phones(),
                             retry_minutes=self.cfg.retry_minutes, end_guard_s=self.end_guard_s)
            final = d.state in policy.FINAL
            db.execute("UPDATE leads SET state=?, priority=COALESCE(?,priority), eligible_at=COALESCE(?,eligible_at),"
                       " not_before=?, current_rid=NULL, last_status=?, last_disposition=?, last_ended_at=?,"
                       " finalized_at=? WHERE campaign_id=? AND lead_id=?",
                       (d.state, d.priority, ts(d.eligible_at), ts(d.not_before), status, disp, ts(ended),
                        ts(now) if final else None, *key))
            self.store.journal("result", now, *key, rid, source=source, status=status, disposition=disp,
                               attempt=att["attempt_no"], next=d.state,
                               next_kind=policy.PRIORITY_NAME.get(d.priority) if not final else None,
                               eligible_at=d.eligible_at.isoformat() if d.eligible_at else None)
            if source == "poll":
                self.store.journal("recovered_by_poll", now, *key, rid)
            if disp == "dnd":                                           # R8: block the phone everywhere
                for sib in db.execute("SELECT campaign_id, lead_id FROM leads WHERE phone=? AND state='ready'",
                                      (att["phone"],)).fetchall():
                    db.execute("UPDATE leads SET state='dnd_blocked', finalized_at=? WHERE campaign_id=? "
                               "AND lead_id=?", (ts(now), sib["campaign_id"], sib["lead_id"]))
                    self.store.journal("dnd_blocked", now, sib["campaign_id"], sib["lead_id"], None,
                                       because=rid, phone=mask_phone(att["phone"]))
        self.wake.set()
        return "applied"

    # ================================================================== recovery
    async def watchdog(self):
        """Resolve unknown intents and recover lost webhooks. Runs every few hundred ms."""
        now = self.clock.now()
        intents = [a["request_id"] for a in self.store.attempts_in("intent")
                   if a["request_id"] not in self.inflight and now - dt(a["intent_at"]) >= self.reconcile_after]
        probes = []
        for a in self.store.attempts_in("accepted"):
            if not a["call_sid"] or now - dt(a["accepted_at"]) < self.probe_after:
                continue
            if a["last_probe_at"] and now - dt(a["last_probe_at"]) < self.probe_every:
                continue
            probes.append(a)
        if probes:
            with self.store.tx() as db:
                db.executemany("UPDATE attempts SET last_probe_at=? WHERE request_id=?",
                               [(ts(now), a["request_id"]) for a in probes])
        await asyncio.gather(*(self.reconcile_intent(r) for r in intents), *(self._probe(a) for a in probes))

    async def _probe(self, att):
        try:
            call = await self.provider.get_call(att["call_sid"])
        except httpx.HTTPError:
            return
        if call and call.get("state") == "ended" and call.get("result"):
            self.handle_result(call["result"], source="poll")

    # ================================================================== observability
    def status(self) -> dict:
        now = self.clock.now()
        st = self.store
        counts: dict = {}
        for r in st.db.execute("SELECT campaign_id, state, COUNT(*) n, SUM(attempts) a FROM leads "
                               "GROUP BY campaign_id, state"):
            c = counts.setdefault(r["campaign_id"], {"states": {}, "attempts": 0})
            c["states"][r["state"]] = r["n"]
            c["attempts"] += r["a"] or 0
        live = st.live_by_tenant()
        campaigns = []
        for cid, camp in sorted(self.cfg.campaigns.items()):
            c = counts.get(cid, {"states": {}, "attempts": 0})
            open_ = is_open(now, camp.effective, self.end_guard_s)
            closes = window_close(now, camp.effective)
            campaigns.append({
                "campaign_id": cid, "tenant_id": camp.tenant_id, "states": c["states"], "attempts": c["attempts"],
                "total": sum(c["states"].values()), "window_open": open_,
                "closes_at": closes.isoformat() if closes else None,
                "next_open": None if open_ else next_open(now, camp.effective, self.end_guard_s).isoformat()})
        upcoming = [{"campaign_id": r["campaign_id"], "lead_id": r["lead_id"], "due": dt(r["eligible_at"]).isoformat(),
                     "in_min": round((dt(r["eligible_at"]) - now).total_seconds() / 60, 1)}
                    for r in st.db.execute("SELECT * FROM leads WHERE state='ready' AND priority=0 "
                                           "ORDER BY eligible_at LIMIT 8")]
        lates = [json.loads(r["detail"])["late_min"] for r in st.db.execute(
            "SELECT detail FROM journal WHERE kind='intent' AND detail LIKE '%late_min%'")]
        return {
            "sim_now": now.isoformat(), "speed": self.clock.speed, "run_tag": self.run_tag,
            "done": self.done(), "end_guard_s": round(self.end_guard_s, 1),
            "tenants": [{"tenant_id": t.tenant_id, "live": live.get(t.tenant_id, 0), "max": t.max_concurrent,
                         "held_for_callbacks": self.last_plan.held_for_callbacks.get(t.tenant_id, 0)}
                        for t in self.cfg.tenants.values()],
            "campaigns": campaigns,
            "callbacks": {"upcoming": upcoming, "dialed": len(lates),
                          "late_p50_min": sorted(lates)[len(lates) // 2] if lates else None,
                          "late_max_min": max(lates) if lates else None},
            "counters": st.journal_counts(),
            "recent": [{"seq": r["seq"], "sim_at": dt(r["sim_at"]).isoformat(), "pid": r["pid"], "kind": r["kind"],
                        "lead": f"{r['campaign_id']}/{r['lead_id']}" if r["lead_id"] else None,
                        "detail": r["detail"]} for r in st.recent_journal(40)],
        }
