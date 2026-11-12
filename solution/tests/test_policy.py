"""Pure decisions: what happens after a result (R4-R9), and who gets dialled next (R1-R3, R6)."""
from datetime import time, timedelta

import pytest

from solution.switchboard.config import ALL_DAYS, Tenant
from solution.switchboard.policy import (CALLBACK, COMPLETED, DND_BLOCKED, EXHAUSTED, FRESH, READY, RETRY,
                                         Candidate, after_result, plan_dispatch)

from .conftest import campaign, ist

RETRY_MIN = {"no_answer": 60, "busy": 60, "failed": 15}
REN = campaign("c_ren", "acme", time(10), time(19), frozenset(range(6)))
FB = campaign("c_fb", "acme", time(11), time(20, 30), ALL_DAYS)
ZEN = campaign("c_zen", "zen", time(9, 30), time(18), frozenset(range(5)))
CAMPS = {c.campaign_id: c for c in (REN, FB, ZEN)}


def decide(status, disposition=None, attempts=1, ended=ist(12, 12), callback_at=None, dnd=False, camp=REN):
    return after_result(attempts=attempts, campaign=camp, status=status, disposition=disposition,
                        callback_at=callback_at, ended_at=ended, phone_is_dnd=dnd, retry_minutes=RETRY_MIN)


# ------------------------------------------------------------------ after_result
@pytest.mark.parametrize("disp", ["interested", "not_interested", "wrong_number", "dnd"])
def test_terminal_dispositions_complete_the_lead(disp):
    assert decide("completed", disp).state == COMPLETED


def test_completed_beats_exhausted_and_dnd():
    assert decide("completed", "interested", attempts=3, dnd=True).state == COMPLETED


def test_exhausted_beats_dnd_blocked():
    assert decide("no_answer", attempts=3, dnd=True).state == EXHAUSTED


def test_dnd_phone_blocks_a_lead_with_attempts_left():
    assert decide("busy", attempts=1, dnd=True).state == DND_BLOCKED


@pytest.mark.parametrize("status,minutes", [("no_answer", 60), ("busy", 60), ("failed", 15)])
def test_retry_waits_from_ended_at(status, minutes):
    d = decide(status, ended=ist(12, 12, 7, 30))
    assert (d.state, d.priority) == (READY, RETRY)
    assert d.eligible_at == ist(12, 12, 7, 30) + timedelta(minutes=minutes)


def test_retry_that_lands_after_close_moves_to_next_opening():
    d = decide("no_answer", ended=ist(12, 18, 30))           # +60 = 19:30, renewals closed
    assert d.eligible_at == ist(13, 10)
    assert d.not_before == ist(12, 19, 30)


def test_callback_inside_window_is_due_exactly_at_callback_at():
    d = decide("completed", "callback_requested", callback_at=ist(12, 14, 15))
    assert (d.state, d.priority, d.eligible_at) == (READY, CALLBACK, ist(12, 14, 15))


def test_callback_outside_window_is_due_at_next_opening():
    d = decide("completed", "callback_requested", callback_at=ist(13, 20, 0), camp=ZEN)  # Fri 20:00
    assert d.eligible_at == ist(16, 9, 30)                                               # Mon open


def test_callback_on_last_attempt_still_exhausts():
    assert decide("completed", "callback_requested", attempts=3, callback_at=ist(12, 14)).state == EXHAUSTED


def test_unexpected_result_shape_is_retried_conservatively_not_dropped():
    d = decide("completed", None)
    assert d.state == READY and d.eligible_at == ist(12, 13)   # the longest retry gap (60 min)


# ------------------------------------------------------------------ plan_dispatch
TENANTS = {"acme": Tenant("acme", 2), "zen": Tenant("zen", 1)}


def cand(lead, prio=FRESH, camp="c_ren", phone=None, due=ist(12, 12), seq=0):
    return Candidate(camp, lead, CAMPS[camp].tenant_id, phone or f"+91{abs(hash(lead)) % 10**10:010d}",
                     prio, due, seq)


def plan(now, ready, active=None, busy=(), dnd=(), lookahead=timedelta(0), tenants=TENANTS):
    return plan_dispatch(now, ready, campaigns=CAMPS, tenants=tenants, active_by_tenant=active or {},
                         busy_phones=set(busy), dnd_phones=set(dnd), callback_lookahead=lookahead)


def ids(p):
    return [c.lead_id for c in p.picks]


def test_priority_callback_then_retry_then_fresh():
    ready = [cand("fresh", FRESH, seq=0), cand("retry", RETRY, seq=5), cand("cb", CALLBACK, seq=9)]
    one = {"acme": Tenant("acme", 1)}
    assert ids(plan(ist(12, 12), ready, tenants=one)) == ["cb"]
    assert ids(plan(ist(12, 12), ready)) == ["cb", "retry"]


def test_within_a_class_longest_waiting_goes_first():
    ready = [cand("late", RETRY, due=ist(12, 11, 50)), cand("early", RETRY, due=ist(12, 11, 0))]
    assert ids(plan(ist(12, 12), ready))[0] == "early"


def test_tenant_limit_is_shared_across_campaigns():
    ready = [cand("r1", camp="c_ren", seq=0), cand("f1", camp="c_fb", seq=0), cand("r2", camp="c_ren", seq=1)]
    assert len(plan(ist(12, 12), ready).picks) == 2
    assert plan(ist(12, 12), ready, active={"acme": 1}).picks.__len__() == 1
    assert plan(ist(12, 12), ready, active={"acme": 2}).picks == []


def test_tenants_are_independent():
    ready = [cand("r1"), cand("z1", camp="c_zen")]
    assert sorted(ids(plan(ist(12, 12), ready, active={"acme": 2}))) == ["z1"]


def test_same_phone_never_dialled_twice_even_across_campaigns():
    p = "+919999900000"
    ready = [cand("r1", phone=p), cand("f1", camp="c_fb", phone=p)]
    assert len(plan(ist(12, 12), ready).picks) == 1
    assert plan(ist(12, 12), ready, busy=[p]).picks == []


def test_dnd_phone_is_never_dialled():
    assert plan(ist(12, 12), [cand("r1", phone="+919999900001")], dnd=["+919999900001"]).picks == []


def test_not_yet_due_is_not_dialled():
    assert plan(ist(12, 12), [cand("r1", due=ist(12, 12, 0, 1))]).picks == []


def test_closed_campaign_window_is_skipped_but_open_one_is_used():
    # 19:30 Thu: renewals closed (10-19), feedback open (11-20:30)
    ready = [cand("r1", camp="c_ren"), cand("f1", camp="c_fb")]
    assert ids(plan(ist(12, 19, 30), ready)) == ["f1"]


def test_slot_is_held_for_a_callback_about_to_fall_due():
    ready = [cand("fresh", FRESH), cand("cb", CALLBACK, due=ist(12, 12, 4))]
    p = plan(ist(12, 12), ready, active={"acme": 1}, lookahead=timedelta(minutes=7))
    assert p.picks == [] and p.held_for_callbacks["acme"] == 1
    # without the look-ahead the fresh lead takes the last slot and the callback would wait
    assert ids(plan(ist(12, 12), ready, active={"acme": 1})) == ["fresh"]
    # and when the callback is due it goes straight in
    assert ids(plan(ist(12, 12, 4), ready, active={"acme": 1}, lookahead=timedelta(minutes=7))) == ["cb"]


def test_far_future_callback_does_not_hold_slots():
    ready = [cand("fresh"), cand("cb", CALLBACK, due=ist(12, 15))]
    assert ids(plan(ist(12, 12), ready, active={"acme": 1}, lookahead=timedelta(minutes=7))) == ["fresh"]
