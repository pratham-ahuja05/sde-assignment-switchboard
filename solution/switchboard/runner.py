"""`run`: wire everything together and drive the engine until every lead is final."""
from __future__ import annotations

import asyncio
import hashlib
import os
import sys
import time
from dataclasses import dataclass

import uvicorn

from . import config as config_mod
from .app import create_app
from .clock import SimClock
from .crash import Crasher
from .engine import Engine
from .provider import HttpProvider
from .report import diff_against_provider, final_rows, write_final_state
from .store import Store


@dataclass
class RunOptions:
    fixtures: str
    provider: str
    db: str
    out: str
    host: str = "127.0.0.1"
    port: int = 8200
    verify: bool = True
    linger: bool = False
    quiet: bool = False


def log(msg: str):
    print(msg, flush=True)


async def wait_for_provider(provider: HttpProvider, tries: int = 120) -> tuple[dict, float]:
    for i in range(tries):
        try:
            t0 = time.time()
            payload = await provider.clock()
            return payload, time.time() - t0
        except Exception as e:  # noqa: BLE001 - any failure here just means "not up yet"
            if i == 0:
                log(f"[switchboard] waiting for provider at {provider.base} ({type(e).__name__})")
            await asyncio.sleep(0.5)
    raise SystemExit(f"provider at {provider.base} never came up")


def open_fenced_store(path: str, epoch_key: str) -> Store:
    """Epoch fencing: our database describes one lifetime of the provider. If the provider was
    restarted (a new epoch), the old state is meaningless there, so archive it and start fresh,
    never mixing two runs' state. Restarting *our* process keeps the epoch and resumes."""
    store = Store(path)
    old = store.meta("provider_epoch")
    if old is not None and old != epoch_key:
        store.close()
        stale = f"{path}.stale-{int(time.time())}"
        for suffix in ("", "-wal", "-shm"):
            if os.path.exists(path + suffix):
                os.replace(path + suffix, stale + suffix)
        log(f"[switchboard] provider epoch changed ({old} -> {epoch_key}); archived old state to {stale}")
        store = Store(path)
    return store


async def run(opts: RunOptions) -> int:
    cfg = config_mod.load(opts.fixtures)
    provider = HttpProvider(opts.provider)
    payload, rtt = await wait_for_provider(provider)
    clock = SimClock.from_provider(payload, rtt)
    store = open_fenced_store(opts.db, clock.epoch_key)
    resumed = store.meta("run_tag") is not None
    run_tag = hashlib.sha1(clock.epoch_key.encode()).hexdigest()[:8]

    engine = Engine(cfg, store, provider, clock, callback_url=f"http://{opts.host}:{opts.port}/webhooks/telephony",
                    crash=Crasher.from_env())
    engine.observe_latency(rtt)          # seed the latency-aware window guard before the first dial
    engine.bootstrap(run_tag)
    unresolved = len(store.attempts_in("intent", "accepted"))
    log(f"[switchboard] pid={os.getpid()} run={engine.run_tag} {'RESUMED' if resumed else 'fresh run'}"
        f" sim_now={clock.now().isoformat(timespec='seconds')} speed={clock.speed:g}x"
        + (f" unresolved_attempts={unresolved} (reconciling)" if resumed else ""))

    # The webhook receiver must be listening before the first dial, or early results bounce.
    server = uvicorn.Server(uvicorn.Config(create_app(engine), host=opts.host, port=opts.port,
                                           log_level="warning", lifespan="off"))
    srv = asyncio.create_task(server.serve())
    while not server.started:
        if srv.done():
            raise SystemExit(f"could not start webhook server on {opts.host}:{opts.port}")
        await asyncio.sleep(0.02)
    log(f"[switchboard] dashboard  http://{opts.host}:{opts.port}/   status  /status")

    tasks: set[asyncio.Task] = set()
    watchdog: asyncio.Task | None = None
    last_wd = last_log = 0.0

    def spawn(coro):
        t = asyncio.create_task(coro)
        tasks.add(t)
        t.add_done_callback(tasks.discard)

    while True:
        for rid in engine.tick():
            spawn(engine.place(rid))
        wall = time.time()
        if wall - last_wd > 0.25 and (watchdog is None or watchdog.done()):
            watchdog = asyncio.create_task(engine.watchdog())
            last_wd = wall
        if not opts.quiet and wall - last_log > 3:
            log(progress_line(engine))
            last_log = wall
        if not tasks and engine.done():
            break
        engine.wake.clear()
        try:
            await asyncio.wait_for(engine.wake.wait(), timeout=0.02)
        except asyncio.TimeoutError:
            pass
    if watchdog:
        await watchdog

    n = write_final_state(store, opts.out)
    log(progress_line(engine))
    log(f"[switchboard] all leads final; wrote {n} rows to {opts.out}")
    rc = 0
    if opts.verify:
        rc = await verify(provider, store)
    if opts.linger:
        log("[switchboard] --linger: dashboard stays up; Ctrl+C to exit")
        await srv
    server.should_exit = True
    await srv
    await provider.aclose()
    return rc


def progress_line(engine: Engine) -> str:
    s = engine.status()
    parts = []
    for c in s["campaigns"]:
        st = c["states"]
        fin = sum(st.get(k, 0) for k in ("completed", "exhausted", "dnd_blocked"))
        parts.append(f"{c['campaign_id']} {fin}/{c['total']}{'' if c['window_open'] else ' (closed)'}")
    live = " ".join(f"{t['tenant_id']}={t['live']}/{t['max']}" for t in s["tenants"])
    k = s["counters"]
    return (f"[{s['sim_now'][5:16].replace('T', ' ')} IST] " + " | ".join(parts) + f" | live {live}"
            f" | 504s {k.get('ambiguous', 0)} dupWH {k.get('duplicate_result', 0)}"
            f" pollRecovered {k.get('recovered_by_poll', 0)} boots {k.get('boot', 0)}")


async def verify(provider: HttpProvider, store: Store) -> int:
    try:
        report = await provider.admin_report()
        expected = await provider.admin_leads()
    except Exception as e:  # noqa: BLE001
        log(f"[verify] provider admin endpoints unavailable: {e}")
        return 0
    diffs = diff_against_provider(final_rows(store), expected)
    log(f"[verify] provider violation_count = {report['violation_count']} {report['violations_by_type'] or ''}")
    log(f"[verify] max concurrency seen      = {report['max_concurrency_seen']}")
    log(f"[verify] callback lateness (min)   = {report['callback_lateness_minutes']}")
    log(f"[verify] webhooks                  = {report['webhooks']}")
    log(f"[verify] final_state.csv vs /admin/leads: "
        + ("MATCH (all rows)" if not diffs else f"{len(diffs)} differences"))
    for d in diffs[:20]:
        log(f"         {d}")
    return 0 if (report["violation_count"] == 0 and not diffs) else 1


def main(opts: RunOptions) -> int:
    try:
        return asyncio.run(run(opts))
    except KeyboardInterrupt:
        print("\n[switchboard] interrupted; state is durable, run again to resume", file=sys.stderr)
        return 130
