"""Simulated time. Business rules never read the machine clock: they read one of these."""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone


class SimClock:
    """Mirrors the provider's clock locally: sim_now = sim_epoch + (wall - real_epoch) * speed.

    Computing locally (rather than polling /v1/clock) means no network call on the hot path.
    `offset` absorbs any disagreement measured at sync time (only relevant if the provider runs on
    another machine with a skewed wall clock).
    """

    def __init__(self, sim_epoch: datetime, real_epoch: float, speed: float, offset_s: float = 0.0):
        self.sim_epoch = sim_epoch
        self.real_epoch = real_epoch
        self.speed = speed
        self.offset = timedelta(seconds=offset_s)

    @classmethod
    def from_provider(cls, payload: dict, rtt_s: float = 0.0) -> "SimClock":
        c = cls(datetime.fromisoformat(payload["sim_epoch"]), float(payload["real_epoch"]),
                float(payload["speed"]))
        # The server read its clock roughly half a round trip ago.
        server_now = datetime.fromisoformat(payload["sim_now"]) + timedelta(seconds=rtt_s / 2 * c.speed)
        drift = (server_now - c.now()).total_seconds()
        if abs(drift) > 30:  # >50 ms real at 600x: a genuinely different wall clock, correct for it
            c.offset = timedelta(seconds=drift)
        return c

    def now(self) -> datetime:
        return self.sim_epoch + timedelta(seconds=(time.time() - self.real_epoch) * self.speed) + self.offset

    def sim(self, real_seconds: float) -> timedelta:
        """How much simulated time passes in `real_seconds` of wall time."""
        return timedelta(seconds=real_seconds * self.speed)

    @property
    def epoch_key(self) -> str:
        """Identifies one lifetime of the provider. A restarted provider gets a new key."""
        return f"{self.sim_epoch.isoformat()}@{self.real_epoch:.3f}"


class FakeClock:
    """For tests: time only moves when the test says so."""

    def __init__(self, start: datetime, speed: float = 600.0):
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        self.t = start
        self.speed = speed
        self.epoch_key = "fake"

    def now(self) -> datetime:
        return self.t

    def advance(self, **kw) -> datetime:
        self.t += timedelta(**kw)
        return self.t

    def set(self, t: datetime):
        self.t = t

    def sim(self, real_seconds: float) -> timedelta:
        return timedelta(seconds=real_seconds * self.speed)
