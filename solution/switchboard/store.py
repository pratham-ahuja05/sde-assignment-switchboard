"""SQLite persistence.

Three kinds of data:
  * `leads`     - the current state of every lead (a projection; small, indexed, mutable)
  * `attempts`  - one row per (lead, attempt_no): the write-ahead record of every dial we intend,
                  keyed by the idempotency key we send to the provider
  * `journal`   - an append-only log of every decision and observation. UPDATE and DELETE are
                  blocked by triggers. The dashboard, the recovery counters and `audit` read it.

Every multi-statement change runs in `BEGIN IMMEDIATE`, which takes SQLite's write lock up
front. So a decision ("these slots are free, reserve them") and its effect ("slots reserved")
are one atomic step even if a second process shares the file.
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS leads (
    campaign_id      TEXT    NOT NULL,
    lead_id          TEXT    NOT NULL,
    tenant_id        TEXT    NOT NULL,
    phone            TEXT    NOT NULL,
    seq              INTEGER NOT NULL,
    state            TEXT    NOT NULL CHECK (state IN
                       ('ready','dialing','active','completed','exhausted','dnd_blocked')),
    priority         INTEGER NOT NULL DEFAULT 2,
    eligible_at      REAL    NOT NULL,       -- sim epoch seconds: earliest next START
    not_before       REAL,                   -- the raw rule time behind eligible_at
    attempts         INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    current_rid      TEXT,
    last_status      TEXT,
    last_disposition TEXT,
    last_ended_at    REAL,
    finalized_at     REAL,
    PRIMARY KEY (campaign_id, lead_id)
);
CREATE INDEX IF NOT EXISTS leads_by_state ON leads (state, eligible_at);
CREATE INDEX IF NOT EXISTS leads_by_phone ON leads (phone);

CREATE TABLE IF NOT EXISTS attempts (
    request_id    TEXT    PRIMARY KEY,       -- the idempotency key; deterministic, see engine.py
    campaign_id   TEXT    NOT NULL,
    lead_id       TEXT    NOT NULL,
    attempt_no    INTEGER NOT NULL,
    tenant_id     TEXT    NOT NULL,
    phone         TEXT    NOT NULL,
    priority      INTEGER NOT NULL,
    state         TEXT    NOT NULL CHECK (state IN
                    ('intent','accepted','ended','not_placed','rejected')),
    intent_at     REAL    NOT NULL,
    posts         INTEGER NOT NULL DEFAULT 0, -- how many times we sent this request_id
    owner_pid     INTEGER,
    call_sid      TEXT UNIQUE,
    accepted_at   REAL,
    started_at    REAL,
    ended_at      REAL,
    status        TEXT,
    disposition   TEXT,
    callback_at   REAL,
    result_source TEXT,                      -- webhook | poll | replay
    last_probe_at REAL,
    UNIQUE (campaign_id, lead_id, attempt_no)
);
CREATE INDEX IF NOT EXISTS attempts_by_state ON attempts (state);

CREATE TABLE IF NOT EXISTS dnd_phones (
    phone      TEXT PRIMARY KEY,
    at         REAL NOT NULL,
    request_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS journal (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    sim_at      REAL    NOT NULL,
    wall_at     REAL    NOT NULL,
    pid         INTEGER NOT NULL,
    kind        TEXT    NOT NULL,
    campaign_id TEXT,
    lead_id     TEXT,
    request_id  TEXT,
    detail      TEXT
);
CREATE INDEX IF NOT EXISTS journal_by_kind ON journal (kind);

CREATE TRIGGER IF NOT EXISTS journal_no_update BEFORE UPDATE ON journal
BEGIN SELECT RAISE(ABORT, 'journal is append-only'); END;
CREATE TRIGGER IF NOT EXISTS journal_no_delete BEFORE DELETE ON journal
BEGIN SELECT RAISE(ABORT, 'journal is append-only'); END;
"""


def ts(t: Optional[datetime]) -> Optional[float]:
    return None if t is None else t.timestamp()


def dt(x: Optional[float]) -> Optional[datetime]:
    return None if x is None else datetime.fromtimestamp(x, tz=timezone.utc)


class Store:
    def __init__(self, path: str):
        self.path = path
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.db = sqlite3.connect(path, isolation_level=None, check_same_thread=False, timeout=30)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        # NORMAL is durable against process death (kill -9): a committed transaction is in the WAL
        # file before COMMIT returns. Only an OS crash / power loss could drop the last commits.
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript(SCHEMA)
        self._in_tx = False

    def close(self):
        self.db.close()

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        if self._in_tx:                         # nested use joins the outer transaction
            yield self.db
            return
        self.db.execute("BEGIN IMMEDIATE")
        self._in_tx = True
        try:
            yield self.db
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
        finally:
            self._in_tx = False

    # ------------------------------------------------------------------ meta
    def meta(self, key: str) -> Optional[str]:
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row["value"] if row else None

    def set_meta(self, key: str, value: str):
        self.db.execute("INSERT INTO meta(key,value) VALUES(?,?) "
                        "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))

    # ------------------------------------------------------------------ journal
    def journal(self, kind: str, sim_now: datetime, campaign_id: str | None = None,
                lead_id: str | None = None, request_id: str | None = None, **detail):
        self.db.execute(
            "INSERT INTO journal(sim_at,wall_at,pid,kind,campaign_id,lead_id,request_id,detail) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (ts(sim_now), time.time(), os.getpid(), kind, campaign_id, lead_id, request_id,
             json.dumps(detail, default=str) if detail else None))

    def journal_counts(self) -> dict[str, int]:
        return {r["kind"]: r["n"] for r in
                self.db.execute("SELECT kind, COUNT(*) n FROM journal GROUP BY kind")}

    def recent_journal(self, limit: int = 40) -> list[sqlite3.Row]:
        return self.db.execute("SELECT * FROM journal ORDER BY seq DESC LIMIT ?", (limit,)).fetchall()

    # ------------------------------------------------------------------ reads
    def lead(self, campaign_id: str, lead_id: str) -> sqlite3.Row:
        return self.db.execute("SELECT * FROM leads WHERE campaign_id=? AND lead_id=?",
                               (campaign_id, lead_id)).fetchone()

    def attempt(self, request_id: str) -> Optional[sqlite3.Row]:
        return self.db.execute("SELECT * FROM attempts WHERE request_id=?", (request_id,)).fetchone()

    def attempt_by_sid(self, call_sid: str) -> Optional[sqlite3.Row]:
        return self.db.execute("SELECT * FROM attempts WHERE call_sid=?", (call_sid,)).fetchone()

    def attempts_in(self, *states: str) -> list[sqlite3.Row]:
        q = f"SELECT * FROM attempts WHERE state IN ({','.join('?' * len(states))})"
        return self.db.execute(q, states).fetchall()

    def ready_leads(self) -> list[sqlite3.Row]:
        return self.db.execute("SELECT * FROM leads WHERE state='ready'").fetchall()

    def live_by_tenant(self) -> dict[str, int]:
        """Calls that might be live at the provider: accepted-without-result, and unresolved intents."""
        return {r["tenant_id"]: r["n"] for r in self.db.execute(
            "SELECT tenant_id, COUNT(*) n FROM attempts WHERE state IN ('intent','accepted') "
            "GROUP BY tenant_id")}

    def busy_phones(self) -> set[str]:
        return {r["phone"] for r in self.db.execute(
            "SELECT DISTINCT phone FROM attempts WHERE state IN ('intent','accepted')")}

    def dnd_phones(self) -> set[str]:
        return {r["phone"] for r in self.db.execute("SELECT phone FROM dnd_phones")}

    def all_leads(self) -> list[sqlite3.Row]:
        return self.db.execute("SELECT * FROM leads ORDER BY campaign_id, lead_id").fetchall()

    def unfinished(self) -> int:
        return self.db.execute(
            "SELECT (SELECT COUNT(*) FROM leads WHERE state NOT IN ('completed','exhausted','dnd_blocked'))"
            " + (SELECT COUNT(*) FROM attempts WHERE state IN ('intent','accepted'))").fetchone()[0]
