"""Engine behaviour under the failures the provider throws at us, and under process death.

Each test answers one "what if my process dies / the network lies right here?" question.
"""
import asyncio
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

from solution.switchboard.app import create_app

from .conftest import ist, make_config, sign

P1, P2, P3 = "+919800000001", "+919800000002", "+919800000003"


def run(coro):
    return asyncio.run(coro)


@pytest.fixture
def w(world_factory):
    cfg = make_config([("c_a", "L1", P1), ("c_a", "L2", P2), ("c_b", "L3", P3)], cap=2)
    return world_factory(cfg, ist(12, 12))


@pytest.fixture
def solo(world_factory):
    return world_factory(make_config([("c_a", "L1", P1)], cap=2), ist(12, 12))


def only_rid(w, lead):
    rows = w.store.db.execute("SELECT request_id FROM attempts WHERE lead_id=? ORDER BY attempt_no DESC", (lead,))
    return rows.fetchone()["request_id"]


def test_happy_path_reserves_slots_and_respects_tenant_cap(w):
    rids = run(w.tick_and_place())
    assert len(rids) == 2                                        # cap 2 shared by c_a and c_b
    assert w.store.live_by_tenant() == {"t1": 2}
    assert run(w.tick_and_place()) == []                        # nothing more until a call ends


def test_request_id_is_deterministic_per_attempt(w):
    run(w.tick_and_place())
    assert only_rid(w, "L1") == "sb-test-c_a-L1-a1"


def test_503_releases_and_retries_with_the_same_key(solo):
    w = solo
    w.provider.script["L1-a1"].append("503")
    run(w.tick_and_place())
    assert w.lead("c_a", "L1")["state"] == "ready"
    w.clock.advance(minutes=5)
    run(w.tick_and_place())
    rid = "sb-test-c_a-L1-a1"
    assert w.provider.posts[rid] == 2 and w.provider.placed_for("L1") == 1
    assert w.lead("c_a", "L1")["attempts"] == 1                 # R4: the 503 didn't count


def test_ambiguous_504_that_was_placed_is_adopted_not_redialled(w):
    w.provider.script["L1-a1"].append("504_placed")
    run(w.tick_and_place())
    lead = w.lead("c_a", "L1")
    assert lead["state"] == "active" and lead["attempts"] == 1
    assert w.provider.placed_for("L1") == 1
    assert w.store.journal_counts()["ambiguous"] >= 1


def test_ambiguous_504_that_was_not_placed_is_released_then_same_key_reused(w):
    w.provider.script["L1-a1"].append("504_unplaced")
    run(w.tick_and_place())
    assert w.lead("c_a", "L1")["state"] == "ready"
    run(w.tick_and_place())
    assert w.provider.placed_for("L1") == 1
    assert w.provider.posts["sb-test-c_a-L1-a1"] == 2


def test_connection_refused_is_treated_as_not_placed(w):
    w.provider.script["L1-a1"].append("conn_refused")
    run(w.tick_and_place())
    assert w.lead("c_a", "L1")["state"] == "ready" and w.lead("c_a", "L1")["attempts"] == 0


def test_webhook_before_the_201_is_read(w):
    rids = w.engine.tick()
    rid = next(r for r in rids if "L1" in r)
    call = w.provider.create({"request_id": rid, "campaign_id": "c_a", "lead_id": "L1", "phone": P1})
    result = w.provider.end(rid, "no_answer")
    assert w.engine.handle_result(result, "webhook") == "applied"   # attempt was still an `intent`
    run(w.engine.place(rid))                                        # now the POST "returns"; no-op
    lead = w.lead("c_a", "L1")
    assert lead["attempts"] == 1 and lead["state"] == "ready" and lead["last_status"] == "no_answer"
    assert call.sid == w.store.attempt(rid)["call_sid"]


def test_duplicate_webhook_is_harmless(w):
    run(w.tick_and_place())
    rid = only_rid(w, "L1")
    res = w.provider.end(rid, "busy")
    assert w.engine.handle_result(res, "webhook") == "applied"
    before = dict(w.lead("c_a", "L1"))
    assert w.engine.handle_result(res, "webhook") == "duplicate"
    assert dict(w.lead("c_a", "L1")) == before


def test_lost_webhook_is_recovered_by_the_watchdog(w):
    run(w.tick_and_place())
    rid = only_rid(w, "L1")
    w.provider.end(rid, "completed", "interested")                  # ...and the webhook never arrives
    w.clock.advance(minutes=10)
    run(w.engine.watchdog())
    lead = w.lead("c_a", "L1")
    assert lead["state"] == "completed" and lead["last_disposition"] == "interested"
    assert w.store.journal_counts()["recovered_by_poll"] == 1


def test_crash_between_intent_and_post_never_double_dials(w):
    rids = w.engine.tick()                                          # intents durable...
    assert rids
    w.boot()                                                        # ...process dies before POSTing
    assert w.store.live_by_tenant() == {"t1": 2}                    # slots stay reserved across the crash
    w.clock.advance(minutes=20)                                     # past the in-flight guard
    run(w.engine.watchdog())                                        # provider says 404 -> release
    assert w.store.live_by_tenant() == {}
    run(w.tick_and_place())
    for lead in ("L1", "L2"):
        assert w.provider.placed_for(lead) == 1


def test_crash_after_provider_accepted_but_before_we_recorded_it(w):
    rids = w.engine.tick()
    rid = next(r for r in rids if "L1" in r)
    w.provider.create({"request_id": rid, "campaign_id": "c_a", "lead_id": "L1", "phone": P1})  # POST landed
    w.boot()                                                        # we died before reading the 201
    w.clock.advance(minutes=20)
    run(w.engine.watchdog())
    lead = w.lead("c_a", "L1")
    assert lead["state"] == "active" and lead["attempts"] == 1
    run(w.tick_and_place())
    assert w.provider.placed_for("L1") == 1                         # adopted, not re-dialled


def test_young_intent_is_not_reconciled_while_its_post_may_be_in_flight(w):
    w.engine.tick()
    w.boot()
    run(w.engine.watchdog())                                        # no time has passed
    assert w.store.attempts_in("intent")                            # still unknown, still reserved


def test_retry_gap_is_respected_end_to_end(solo):
    w = solo
    run(w.tick_and_place())
    w.engine.handle_result(w.provider.end(only_rid(w, "L1"), "no_answer", after=timedelta(minutes=2)), "webhook")
    ended = ist(12, 12, 2)
    w.clock.set(ended + timedelta(minutes=59, seconds=59))
    assert w.engine.tick() == []
    w.clock.set(ended + timedelta(minutes=60))
    assert w.engine.tick() == ["sb-test-c_a-L1-a2"]


def test_callback_is_dialled_at_callback_time_with_priority(solo):
    w = solo
    run(w.tick_and_place())
    cb = ist(12, 15, 30)
    w.engine.handle_result(w.provider.end(only_rid(w, "L1"), "completed", "callback_requested", callback_at=cb),
                           "webhook")
    w.clock.set(cb - timedelta(seconds=1))
    assert w.engine.tick() == []
    w.clock.set(cb)
    assert w.engine.tick() == ["sb-test-c_a-L1-a2"]
    assert w.store.journal_counts()["intent"] == 2


def test_lead_runs_to_exhaustion_after_three_attempts(solo):
    w = solo
    for n in (1, 2, 3):
        assert run(w.tick_and_place()) == [f"sb-test-c_a-L1-a{n}"]
        w.engine.handle_result(w.provider.end(f"sb-test-c_a-L1-a{n}", "failed", after=timedelta(minutes=1)),
                               "webhook")
        w.clock.advance(minutes=20)
    lead = w.lead("c_a", "L1")
    assert (lead["state"], lead["attempts"], lead["last_status"]) == ("exhausted", 3, "failed")
    assert w.engine.done()
    assert run(w.tick_and_place()) == []


def test_dnd_blocks_the_same_phone_in_another_campaign(world_factory):
    cfg = make_config([("c_a", "L1", P1), ("c_b", "L9", P1)], cap=2)
    w = world_factory(cfg, ist(12, 12))
    rids = run(w.tick_and_place())
    assert len(rids) == 1                                           # R3: same phone, one at a time
    w.engine.handle_result(w.provider.end(rids[0], "completed", "dnd"), "webhook")
    states = {r["lead_id"]: r["state"] for r in w.store.all_leads()}
    assert sorted(states.values()) == ["completed", "dnd_blocked"]
    w.clock.advance(hours=3)
    assert w.engine.tick() == []


def test_webhook_signature_is_verified(w):
    run(w.tick_and_place())
    res = w.provider.end(only_rid(w, "L1"), "busy")
    body = w.body(res)
    client = TestClient(create_app(w.engine))
    bad = client.post("/webhooks/telephony", content=body, headers={"X-AU-Signature": "00" * 32})
    assert bad.status_code == 401
    missing = client.post("/webhooks/telephony", content=body)
    assert missing.status_code == 401
    tampered = body.replace(b"busy", b"completed")
    assert client.post("/webhooks/telephony", content=tampered,
                       headers={"X-AU-Signature": sign("whsec_test", body)}).status_code == 401
    ok = client.post("/webhooks/telephony", content=body, headers={"X-AU-Signature": sign("whsec_test", body)})
    assert ok.status_code == 200 and ok.json()["outcome"] == "applied"
    dup = client.post("/webhooks/telephony", content=body, headers={"X-AU-Signature": sign("whsec_test", body)})
    assert dup.status_code == 200 and dup.json()["outcome"] == "duplicate"


def test_journal_is_append_only(w):
    run(w.tick_and_place())
    import sqlite3
    with pytest.raises(sqlite3.DatabaseError):
        w.store.db.execute("UPDATE journal SET kind='x'")
    with pytest.raises(sqlite3.DatabaseError):
        w.store.db.execute("DELETE FROM journal")
