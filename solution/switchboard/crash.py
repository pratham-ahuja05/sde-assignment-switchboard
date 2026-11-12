"""Deterministic crash injection.

The assignment's hint: "ask, at every line between 'pick a lead' and 'the provider has accepted
the call': what happens if my process dies right here?" Instead of only answering in prose, each
of those lines is a named crash point. Setting

    SWITCHBOARD_CRASH="after_intent_commit:0.1,after_provider_reply:0.1"

makes the process hard-exit (os._exit: no cleanup, no flush, no finally blocks, the same thing
kill -9 does) with that probability whenever execution reaches the point. The chaos harness uses
this to land kills exactly on the dangerous lines, not just wherever a random timer fires.
"""
from __future__ import annotations

import os
import random
import sys

POINTS = {
    "after_intent_commit":    "intent + slot reservation durable, POST not yet sent",
    "after_provider_reply":   "provider answered (maybe accepted), answer not yet recorded",
    "after_webhook_verified": "signed result received, not yet applied",
    "after_result_commit":    "result applied, 2xx not yet returned (provider will redeliver)",
    "mid_reconcile":          "asked the provider about an unknown intent, answer not recorded",
}

EXIT_CODE = 137  # what a shell reports for SIGKILL; the chaos harness treats it as "restart me"


class Crasher:
    def __init__(self, spec: str | None = None, seed: int | None = None):
        self.p: dict[str, float] = {}
        for part in filter(None, (spec or "").split(",")):
            name, _, prob = part.partition(":")
            if name.strip() not in POINTS:
                raise ValueError(f"unknown crash point {name!r}; known: {', '.join(POINTS)}")
            self.p[name.strip()] = float(prob or 1.0)
        self.rng = random.Random(seed)

    @classmethod
    def from_env(cls) -> "Crasher":
        seed = os.environ.get("SWITCHBOARD_CRASH_SEED")
        return cls(os.environ.get("SWITCHBOARD_CRASH"), int(seed) if seed else None)

    def __call__(self, point: str):
        p = self.p.get(point)
        if p and self.rng.random() < p:
            sys.stderr.write(f"\n[crash] injected hard exit at {point}: {POINTS[point]}\n")
            sys.stderr.flush()
            os._exit(EXIT_CODE)


NO_CRASH = Crasher()
